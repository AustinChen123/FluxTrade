"""Authoritative Rithmic net-position reduction for portfolio sleeves."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.adapters.rithmic_recovery import (
    load_rithmic_recovery_snapshot,
    rithmic_order_may_be_working,
)
from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)
from src.core.execution import ExecutionEngine, ExitDecision
from src.core.models import Candlestick, OrderStatus, Signal, SignalType


class _PortfolioExitOperationFailure(Exception):
    """Carry a failed gated operation to post-gate compensation scheduling."""

    def __init__(
        self,
        error: Exception,
        reason: str,
        *,
        compensation_required: bool,
    ) -> None:
        super().__init__(reason)
        self.error = error
        self.reason = reason
        self.compensation_required = compensation_required


class RithmicPortfolioExitService:
    """Safely reduce one sleeve against a venue-level product net position."""

    def __init__(
        self,
        *,
        adapter: RithmicExchangeAdapter,
        execution_engine: ExecutionEngine,
        account_service: Any,
        profile: str,
        account_id: str,
        operation_gate: RithmicOrderEventLifecycleGate,
        stop_order_event_stream: Callable[..., bool],
        assert_leadership: Callable[[], None],
        restart_order_stream: Callable[[], None],
        lockdown: Callable[[str], None],
        schedule_emergency_flatten: Callable[[str], None],
        portfolio_id_for_sleeve: Callable[[str], str | None],
    ) -> None:
        if not profile or not account_id:
            raise ValueError("rithmic portfolio exit requires account identity")
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
        self.schedule_emergency_flatten = schedule_emergency_flatten
        self.portfolio_id_for_sleeve = portfolio_id_for_sleeve

    def execute(
        self,
        signal: Signal,
        decision: ExitDecision,
        candle: Candlestick | None,
    ) -> dict[str, object]:
        if signal.type not in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            raise ValueError("rithmic_portfolio_exit_requires_exit_signal")
        if (
            decision.position_quantity is None
            or decision.quantity != decision.position_quantity
        ):
            raise RuntimeError("rithmic_portfolio_partial_sleeve_exit_unsupported")
        exit_quantity = decision.quantity
        if exit_quantity is None:
            raise RuntimeError("rithmic_portfolio_exit_quantity_missing")

        def execute_validated() -> dict[str, object]:
            try:
                return self._execute_validated(
                    signal,
                    decision,
                    candle,
                    exit_quantity,
                )
            except _PortfolioExitOperationFailure as failure:
                if failure.compensation_required:
                    try:
                        self.schedule_emergency_flatten(failure.reason)
                    except Exception as compensation_error:
                        self.lockdown(
                            "rithmic_portfolio_exit_compensation_schedule_failed:"
                            f"{type(compensation_error).__name__}"
                        )
                        raise RuntimeError(
                            "rithmic_portfolio_exit_compensation_schedule_failed"
                        ) from compensation_error
                raise failure.error

        return self.operation_gate.run(execute_validated)

    def _execute_validated(
        self,
        signal: Signal,
        decision: ExitDecision,
        candle: Candlestick | None,
        exit_quantity: Decimal,
    ) -> dict[str, object]:
        operation_failed = False
        order_event_stopped = False
        compensation_required = False
        outcome: dict[str, object] | None = None

        def mark_compensation_required() -> None:
            nonlocal compensation_required
            compensation_required = True

        try:
            if not self.stop_order_event_stream(timeout=30.0):
                raise RuntimeError("rithmic_portfolio_exit_event_stream_stop_timeout")
            order_event_stopped = True
            expected_side = (
                "LONG"
                if signal.type == SignalType.EXIT_LONG
                else "SHORT"
            )
            self._verified_preflight_position(
                signal,
                expected_side=expected_side,
                exit_quantity=exit_quantity,
            )
            self.assert_leadership()
            self.adapter.start_order_event_stream()
            cancelled_orders, cancelled_identities = self._cancel_strategy_orders(
                signal.strategy_id,
                signal.product_id,
                mark_compensation_required=mark_compensation_required,
            )
            _, remote_quantity = self._verified_preflight_position(
                signal,
                expected_side=expected_side,
                exit_quantity=exit_quantity,
                cancelled_identities=cancelled_identities,
            )
            expected_remaining_quantity = remote_quantity - exit_quantity
            self.assert_leadership()
            self.adapter.start_order_event_stream()
            compensation_required = True
            order_id = self.execution_engine.submit_verified_net_reduction(
                signal,
                decision,
                candle=candle,
                preflight_remote_quantity=remote_quantity,
            )

            for _attempt in range(6):
                self.assert_leadership()
                snapshot = self._load_snapshot()
                reconciliation = self._reconcile(snapshot)
                remaining_position = self._position_for_product(
                    snapshot,
                    signal.product_id,
                )
                remaining_quantity = (
                    Decimal("0")
                    if remaining_position is None
                    else Decimal(str(remaining_position.quantity))
                )
                remaining_side = (
                    None
                    if remaining_position is None
                    else str(
                        getattr(
                            remaining_position.side,
                            "value",
                            remaining_position.side,
                        )
                    ).upper()
                )
                local_sleeve_position = (
                    self.account_service.get_position_for_exit(
                        signal.strategy_id,
                        signal.product_id,
                    )
                )
                if (
                    reconciliation.get("auto_resume_safe") is True
                    and remaining_quantity == expected_remaining_quantity
                    and (
                        remaining_quantity == 0
                        or remaining_side == expected_side
                    )
                    and local_sleeve_position is None
                ):
                    compensation_required = False
                    self.execution_engine.record_verified_net_reduction(
                        signal,
                        order_id,
                        remaining_remote_quantity=remaining_quantity,
                    )
                    outcome = {
                        "status": "verified_portfolio_reduction",
                        "portfolio_id": self.portfolio_id_for_sleeve(
                            signal.strategy_id
                        ),
                        "strategy_id": signal.strategy_id,
                        "product_id": signal.product_id,
                        "order_id": order_id,
                        "cancelled_orders": cancelled_orders,
                        "preflight_remote_quantity": str(remote_quantity),
                        "remaining_remote_quantity": str(remaining_quantity),
                    }
                    break
            if outcome is None:
                raise RuntimeError(
                    "rithmic_portfolio_exit_reduction_not_verified"
                )
        except Exception as error:
            operation_failed = True
            reason = (
                f"rithmic_portfolio_exit_requires_reconciliation:{type(error).__name__}"
            )
            self.lockdown(reason)
            raise _PortfolioExitOperationFailure(
                error,
                reason,
                compensation_required=compensation_required,
            ) from error
        finally:
            if order_event_stopped:
                try:
                    self.assert_leadership()
                    self.restart_order_stream()
                except Exception:
                    self.adapter.close()
                    self.lockdown("rithmic_portfolio_exit_order_stream_restart_failed")
                    if not operation_failed:
                        raise RuntimeError(
                            "rithmic_portfolio_exit_order_stream_restart_failed"
                        )
        return outcome

    def _verified_preflight_position(
        self,
        signal: Signal,
        *,
        expected_side: str,
        exit_quantity: Decimal,
        cancelled_identities: (
            set[tuple[str | None, str | None]] | None
        ) = None,
    ) -> tuple[object, Decimal]:
        identities = cancelled_identities or set()
        for _attempt in range(6):
            self.assert_leadership()
            snapshot = self._load_snapshot()
            reconciliation = self._reconcile(snapshot)
            if (
                reconciliation.get("auto_resume_safe") is not True
                or self._identities_still_working(snapshot, identities)
            ):
                continue
            position = self._position_for_product(
                snapshot,
                signal.product_id,
            )
            if position is None:
                raise RuntimeError(
                    "rithmic_portfolio_exit_remote_position_missing"
                )
            remote_side = str(
                getattr(position.side, "value", position.side)
            ).upper()
            remote_quantity = Decimal(str(position.quantity))
            if (
                remote_side != expected_side
                or remote_quantity < exit_quantity
            ):
                raise RuntimeError(
                    "rithmic_portfolio_exit_position_drift:"
                    f"expected_side={expected_side} remote_side={remote_side} "
                    f"exit_quantity={exit_quantity} "
                    f"remote_quantity={remote_quantity}"
                )
            self._assert_local_position_unchanged(
                signal,
                expected_side=expected_side,
                exit_quantity=exit_quantity,
            )
            return position, remote_quantity
        raise RuntimeError(
            "rithmic_portfolio_exit_preflight_reconciliation_blocked"
        )

    def _assert_local_position_unchanged(
        self,
        signal: Signal,
        *,
        expected_side: str,
        exit_quantity: Decimal,
    ) -> None:
        position = self.account_service.get_position_for_exit(
            signal.strategy_id,
            signal.product_id,
        )
        if position is None:
            raise RuntimeError("rithmic_portfolio_exit_local_position_changed")
        side = str(
            getattr(position.side, "value", position.side)
        ).upper()
        quantity = Decimal(str(position.quantity))
        if (
            side != expected_side
            or not quantity.is_finite()
            or quantity != exit_quantity
        ):
            raise RuntimeError("rithmic_portfolio_exit_local_position_changed")

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

    def _reconcile(self, snapshot) -> dict[str, object]:
        return self.execution_engine.reconcile_owned_orders(
            snapshot_loader=lambda *_args, **_kwargs: snapshot,
        )

    def _position_for_product(self, snapshot, product_id: str):
        return next(
            (
                position
                for position in self.adapter.positions_from_ledger_snapshot(
                    snapshot
                )
                if position.product_id == product_id
            ),
            None,
        )

    def _cancel_strategy_orders(
        self,
        strategy_id: str,
        product_id: str,
        *,
        mark_compensation_required: Callable[[], None],
    ) -> tuple[int, set[tuple[str | None, str | None]]]:
        active_statuses = {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
        active_orders: list[Any] = []
        for order_record in (
            self.execution_engine.order_manager.repo.list_orders_by_statuses(
                active_statuses
            )
        ):
            order: Any = order_record
            if (
                order.strategy_id == strategy_id
                and order.product_id == product_id
            ):
                active_orders.append(order)
        cancelled_orders = 0
        identities: set[tuple[str | None, str | None]] = set()
        for order in active_orders:
            identities.add(
                (
                    str(order.exchange_order_id)
                    if getattr(order, "exchange_order_id", None)
                    else None,
                    str(order.client_order_id)
                    if order.client_order_id
                    else None,
                )
            )
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
                    "rithmic_strategy_exit_cancel_identity_missing:"
                    f"order_id={order.id}"
                )
            remote = self.adapter.get_order_by_client_id(
                order.client_order_id,
                order.product_id,
                order_type=order.type,
            )
            if remote is None:
                raise RuntimeError(
                    "rithmic_strategy_exit_cancel_lookup_missing:"
                    f"order_id={order.id}"
                )
            if remote.status in {"filled", "cancelled", "rejected"}:
                continue
            if not remote.exchange_order_id:
                raise RuntimeError(
                    "rithmic_strategy_exit_remote_identity_missing:"
                    f"order_id={order.id}"
                )
            self.assert_leadership()
            mark_compensation_required()
            if not self.adapter.cancel_order(
                remote.exchange_order_id,
                order.product_id,
                order_type=order.type,
            ):
                raise RuntimeError(
                    "rithmic_strategy_exit_cancel_failed:"
                    f"order_id={order.id}"
                )
            cancelled_orders += 1
        return cancelled_orders, identities

    @staticmethod
    def _identities_still_working(
        snapshot,
        identities: set[tuple[str | None, str | None]],
    ) -> bool:
        for remote in snapshot.orders:
            remote_basket = (
                str(remote.basket_id) if remote.basket_id else None
            )
            remote_client = (
                str(remote.client_order_id)
                if remote.client_order_id
                else None
            )
            if not rithmic_order_may_be_working(remote):
                continue
            if any(
                (basket_id is not None and basket_id == remote_basket)
                or (
                    client_order_id is not None
                    and client_order_id == remote_client
                )
                for basket_id, client_order_id in identities
            ):
                return True
        return False
