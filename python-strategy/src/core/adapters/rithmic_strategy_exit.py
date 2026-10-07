"""Authoritative single-strategy Rithmic position exit owner."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from logging import Logger
from typing import Any, cast

from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.adapters.rithmic_recovery import (
    load_rithmic_recovery_snapshot,
    rithmic_order_may_be_working,
)
from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)
from src.core.adapters.rithmic_order_observation import (
    project_rithmic_order_snapshot,
)
from src.core.execution import ExecutionEngine, ExitDecision
from src.core.interfaces.exchange import ExchangeError
from src.core.models import OrderStatus, Position, Signal, SignalType


class RithmicStrategyExitService:
    """Cancel owned orders, exit one full position, and verify remote flat."""

    def __init__(
        self,
        *,
        adapter: RithmicExchangeAdapter,
        execution_engine: ExecutionEngine,
        account_service: Any,
        profile: str,
        account_id: str | None,
        operation_gate: RithmicOrderEventLifecycleGate,
        stop_order_event_stream: Callable[..., bool],
        assert_leadership: Callable[[], None],
        restart_order_stream: Callable[[], None],
        lockdown: Callable[[str], None],
        logger: Logger,
    ) -> None:
        if not profile or not account_id:
            raise ValueError("rithmic strategy exit requires account identity")
        self.adapter = adapter
        self.execution_engine = execution_engine
        self.account_service = account_service
        self.profile = profile
        self.account_id = account_id
        self.operation_gate = operation_gate
        self.stop_order_event_stream = stop_order_event_stream
        self.assert_leadership = assert_leadership
        self.restart_order_stream = restart_order_stream
        self.lockdown = lockdown
        self.logger = logger

    def execute(
        self,
        signal: Signal,
        decision: ExitDecision,
    ) -> dict[str, object]:
        if signal.type not in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            raise ValueError("rithmic_strategy_exit_requires_exit_signal")
        if (
            decision.position_quantity is None
            or decision.quantity != decision.position_quantity
        ):
            raise RuntimeError("rithmic_partial_strategy_exit_unsupported")

        return self.operation_gate.run(
            self._execute_validated,
            signal,
            decision.position_quantity,
        )

    def _execute_validated(
        self,
        signal: Signal,
        position_quantity: Decimal,
    ) -> dict[str, object]:
        order_event_stopped = False
        operation_failed = False
        outcome: dict[str, object] | None = None
        cancelled_orders = 0
        try:
            if not self.stop_order_event_stream(timeout=30.0):
                raise RuntimeError("rithmic_strategy_exit_event_stream_stop_timeout")
            order_event_stopped = True

            self.assert_leadership()
            initial_snapshot = self._load_snapshot()
            initial_reconciliation = self._reconcile(initial_snapshot)
            if initial_reconciliation.get("auto_resume_safe") is not True:
                raise RuntimeError(
                    "rithmic_strategy_exit_preflight_reconciliation_blocked"
                )

            cancellation_targets, new_orders = self._cancellation_targets(
                signal,
                initial_snapshot,
            )
            for order in new_orders:
                self.assert_leadership()
                self.execution_engine.order_manager.fail_order(order, "strategy_exit")
                cancelled_orders += 1

            post_cancel_snapshot = initial_snapshot
            post_cancel_reconciliation = initial_reconciliation
            positions: list[Position]
            remote_position: Position | None
            if cancellation_targets:
                self.adapter.start_order_event_stream()
                cancel_error: ExchangeError | None = None
                terminal_targets: list[tuple[Any, Any]] = []
                for order, remote in cancellation_targets:
                    self.assert_leadership()
                    try:
                        cancelled = self.adapter.cancel_order(
                            cast(str, remote.basket_id),
                            order.product_id,
                            order_type=order.type,
                        )
                    except ExchangeError as error:
                        cancel_error = error
                        terminal_targets.append((order, remote))
                        break
                    if not cancelled:
                        raise RuntimeError(
                            f"rithmic_strategy_exit_cancel_failed:order_id={order.id}"
                        )
                    cancelled_orders += 1
                    terminal_targets.append((order, remote))

                try:
                    post_cancel_snapshot = self._load_snapshot()
                    post_cancel_reconciliation = self._reconcile(post_cancel_snapshot)
                    self._validate_post_cancel_snapshot(
                        post_cancel_snapshot,
                        post_cancel_reconciliation,
                        terminal_targets,
                    )
                    positions = self.adapter.positions_from_ledger_snapshot(
                        post_cancel_snapshot
                    )
                    remote_position = self._validate_exit_position(
                        signal,
                        position_quantity,
                        positions,
                    )
                except Exception as verification_error:
                    if cancel_error is not None:
                        raise cancel_error from verification_error
                    raise
            else:
                self._validate_post_cancel_snapshot(
                    initial_snapshot,
                    initial_reconciliation,
                    [],
                )
                positions = self.adapter.positions_from_ledger_snapshot(
                    initial_snapshot
                )
                remote_position = self._validate_exit_position(
                    signal,
                    position_quantity,
                    positions,
                )
            if remote_position is not None:
                self.assert_leadership()
                self.adapter.start_order_event_stream()
                self.execution_engine.exit_authoritative_position(
                    signal.product_id,
                    account_id=self.adapter.account_id,
                )
                snapshot = self._load_snapshot()
                reconciliation = self._reconcile(snapshot)
                self._validate_post_cancel_snapshot(snapshot, reconciliation, [])
                remaining_positions = self.adapter.positions_from_ledger_snapshot(
                    snapshot
                )
                self.assert_leadership()
                target_position = next(
                    (
                        p
                        for p in remaining_positions
                        if p.product_id == signal.product_id
                    ),
                    None,
                )
                if target_position is not None:
                    raise RuntimeError("rithmic_strategy_exit_flat_not_verified")
                self._publish_positions(remaining_positions)
            else:
                self.assert_leadership()
                self._publish_positions(positions)
            outcome = {
                "status": "verified_flat",
                "cancelled_orders": cancelled_orders,
                "product_id": signal.product_id,
            }
        except Exception as error:
            operation_failed = True
            self.lockdown(
                f"rithmic_strategy_exit_requires_reconciliation:{type(error).__name__}"
            )
            raise
        finally:
            if order_event_stopped:
                try:
                    self.assert_leadership()
                    self.restart_order_stream()
                except Exception:
                    self.adapter.close()
                    self.logger.exception(
                        "Rithmic strategy exit failed to restart order event stream"
                    )
                    self.lockdown("rithmic_strategy_exit_order_stream_restart_failed")
                    if not operation_failed:
                        raise RuntimeError(
                            "rithmic_strategy_exit_order_stream_restart_failed"
                        )
        if outcome is None:
            raise RuntimeError("rithmic_strategy_exit_outcome_missing")
        return outcome

    @staticmethod
    def _validate_exit_position(
        signal: Signal,
        position_quantity: Decimal,
        positions: list[Position],
    ) -> Position | None:
        remote_position = next(
            (
                position
                for position in positions
                if position.product_id == signal.product_id
            ),
            None,
        )
        if remote_position is not None:
            remote_side = str(
                getattr(remote_position.side, "value", remote_position.side)
            ).upper()
            expected_side = "LONG" if signal.type == SignalType.EXIT_LONG else "SHORT"
            if (
                remote_side != expected_side
                or remote_position.quantity > position_quantity
            ):
                raise RuntimeError(
                    f"rithmic_strategy_exit_position_drift:expected_side={expected_side} "
                    f"remote_side={remote_side} expected_quantity={position_quantity} "
                    f"remote_quantity={remote_position.quantity}"
                )
        return remote_position

    def _cancellation_targets(
        self,
        signal: Signal,
        snapshot: object,
    ) -> tuple[list[tuple[Any, Any]], list[Any]]:
        active_statuses = {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
        active_orders: list[Any] = []
        for (
            order_record
        ) in self.execution_engine.order_manager.repo.list_orders_by_statuses(
            active_statuses
        ):
            order: Any = order_record
            if (
                order.strategy_id == signal.strategy_id
                and order.product_id == signal.product_id
                and str(getattr(order, "exchange_id", "RITHMIC")).casefold()
                == "rithmic"
                and getattr(order, "account_profile", self.profile) == self.profile
                and getattr(order, "account_id", self.account_id) == self.account_id
            ):
                active_orders.append(order)
        cancellation_targets: list[tuple[Any, Any]] = []
        new_orders: list[Any] = []
        for order in active_orders:
            if order.status == OrderStatus.NEW.value:
                new_orders.append(order)
                continue
            basket_id = str(getattr(order, "exchange_order_id", None) or "").strip()
            if not basket_id:
                raise RuntimeError(
                    f"rithmic_strategy_exit_cancel_identity_missing:order_id={order.id}"
                )
            remote_matches = [
                remote
                for remote in getattr(snapshot, "orders", ())
                if str(getattr(remote, "basket_id", "")) == basket_id
            ]
            if len(remote_matches) != 1:
                raise RuntimeError(
                    f"rithmic_strategy_exit_cancel_identity_ambiguous:order_id={order.id}"
                )
            remote = remote_matches[0]
            projected = project_rithmic_order_snapshot(
                remote,
                account_id=self.account_id,
            )
            if projected.exchange_order_id != basket_id:
                raise RuntimeError(
                    f"rithmic_strategy_exit_cancel_identity_mismatch:order_id={order.id}"
                )
            if not rithmic_order_may_be_working(remote):
                continue
            cancellation_targets.append((order, remote))
        return cancellation_targets, new_orders

    def _validate_post_cancel_snapshot(
        self,
        snapshot: object,
        reconciliation: dict[str, object],
        terminal_targets: list[tuple[Any, Any]],
    ) -> None:
        if reconciliation.get("auto_resume_safe") is not True:
            raise RuntimeError(
                "rithmic_strategy_exit_post_cancel_reconciliation_blocked"
            )
        if any(
            rithmic_order_may_be_working(order)
            for order in getattr(snapshot, "orders", ())
        ):
            raise RuntimeError("rithmic_strategy_exit_working_orders_remain")
        for order, remote in terminal_targets:
            basket_id = str(remote.basket_id)
            matches = [
                current
                for current in getattr(snapshot, "orders", ())
                if str(getattr(current, "basket_id", "")) == basket_id
            ]
            if len(matches) > 1 or (
                matches and rithmic_order_may_be_working(matches[0])
            ):
                raise RuntimeError(
                    f"rithmic_strategy_exit_cancel_not_terminal:order_id={order.id}"
                )
            reconciled_order = self.execution_engine.order_manager.repo.get_order(
                order.id
            )
            if reconciled_order is None or reconciled_order.status not in {
                OrderStatus.FILLED.value,
                OrderStatus.CANCELLED.value,
                OrderStatus.FAILED.value,
                OrderStatus.LIQUIDATED.value,
            }:
                raise RuntimeError(
                    f"rithmic_strategy_exit_cancel_not_terminal:order_id={order.id}"
                )

    def _load_snapshot(self):
        self.adapter.close()
        recoverable_orders = [
            order
            for order in self.execution_engine.list_recoverable_client_orders()
            if str(order.exchange_id).lower() == "rithmic"
        ]
        return load_rithmic_recovery_snapshot(
            self.profile,
            self.account_id,
            recoverable_orders,
            int(self.execution_engine.clock.now()),
        )

    def _reconcile(self, snapshot) -> dict[str, Any]:
        return self.execution_engine.reconcile_owned_orders(
            snapshot_loader=lambda *_args, **_kwargs: snapshot,
        )

    def _publish_positions(self, positions: list[Position]) -> None:
        self.account_service.replace_positions_for_products(
            positions,
            self.adapter.configured_product_ids,
            timestamp_ms=int(self.execution_engine.clock.now() * 1000),
        )
