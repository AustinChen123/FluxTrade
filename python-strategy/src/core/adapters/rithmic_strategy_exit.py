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
from src.core.execution import ExecutionEngine, ExitDecision
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
        verified = False
        order_event_stopped = False
        operation_failed = False
        outcome: dict[str, object] | None = None
        cancelled_orders = 0
        try:
            if not self.stop_order_event_stream(timeout=30.0):
                raise RuntimeError("rithmic_strategy_exit_event_stream_stop_timeout")
            order_event_stopped = True

            self.assert_leadership()
            self.adapter.start_order_event_stream()
            cancelled_orders = self._cancel_active_orders(signal)

            snapshot = self._load_snapshot()
            if any(rithmic_order_may_be_working(order) for order in snapshot.orders):
                raise RuntimeError("rithmic_strategy_exit_working_orders_remain")
            positions = self.adapter.positions_from_ledger_snapshot(snapshot)
            reconciliation = self._reconcile(snapshot)
            if reconciliation.get("auto_resume_safe") is not True:
                raise RuntimeError(
                    "rithmic_strategy_exit_preflight_reconciliation_blocked"
                )
            self.assert_leadership()
            self._publish_positions(positions)
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
                expected_side = (
                    "LONG" if signal.type == SignalType.EXIT_LONG else "SHORT"
                )
                if (
                    remote_side != expected_side
                    or remote_position.quantity > position_quantity
                ):
                    raise RuntimeError(
                        "rithmic_strategy_exit_position_drift:"
                        f"expected_side={expected_side} remote_side={remote_side} "
                        f"expected_quantity={position_quantity} "
                        f"remote_quantity={remote_position.quantity}"
                    )
                self.assert_leadership()
                self.adapter.start_order_event_stream()
                self.execution_engine.exit_authoritative_position(
                    signal.product_id,
                    account_id=self.adapter.account_id,
                )

            for _attempt in range(6):
                self.assert_leadership()
                snapshot = self._load_snapshot()
                remaining_positions = self.adapter.positions_from_ledger_snapshot(
                    snapshot
                )
                if any(
                    rithmic_order_may_be_working(order) for order in snapshot.orders
                ):
                    continue
                reconciliation = self._reconcile(snapshot)
                self.assert_leadership()
                self._publish_positions(remaining_positions)
                target_position = next(
                    (
                        position
                        for position in remaining_positions
                        if position.product_id == signal.product_id
                    ),
                    None,
                )
                if (
                    target_position is None
                    and reconciliation.get("auto_resume_safe") is True
                ):
                    verified = True
                    outcome = {
                        "status": "verified_flat",
                        "cancelled_orders": cancelled_orders,
                        "product_id": signal.product_id,
                    }
                    break
            if not verified:
                raise RuntimeError("rithmic_strategy_exit_flat_not_verified")
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

    def _cancel_active_orders(self, signal: Signal) -> int:
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
            ):
                active_orders.append(order)
        cancelled_orders = 0
        for order in active_orders:
            self.assert_leadership()
            if order.status == OrderStatus.NEW.value:
                self.execution_engine.order_manager.fail_order(
                    order,
                    "strategy_exit",
                )
                cancelled_orders += 1
                continue
            if not order.client_order_id:
                raise RuntimeError(
                    f"rithmic_strategy_exit_cancel_identity_missing:order_id={order.id}"
                )
            remote = self.adapter.get_order_by_client_id(
                order.client_order_id,
                order.product_id,
                order_type=order.type,
            )
            if remote is None:
                raise RuntimeError(
                    f"rithmic_strategy_exit_cancel_lookup_missing:order_id={order.id}"
                )
            if remote.status in {"filled", "cancelled", "rejected"}:
                continue
            self.assert_leadership()
            if not self.adapter.cancel_order(
                cast(str, remote.exchange_order_id),
                order.product_id,
                order_type=order.type,
            ):
                raise RuntimeError(
                    f"rithmic_strategy_exit_cancel_failed:order_id={order.id}"
                )
            cancelled_orders += 1
        return cancelled_orders

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
