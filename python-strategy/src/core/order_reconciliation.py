import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, ContextManager, Optional, Protocol, TypedDict, cast

from sqlalchemy.orm import Session

from src.core.audit_service import write_system_event
from src.core.fill_delta import (
    FillDelta,
    FillDeltaState,
    classify_fill_delta,
    snapshot_fill_delta,
)
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderLookupUnsupported,
    IExchangeAdapter,
    OwnedOrderReconciliationContext,
)
from src.core.models import OrderStatus
from src.core.order_event_sync import (
    exchange_snapshot_to_order_event,
    snapshot_fill_fee_rejection,
)


class _ExchangeOrderEventProcessor(Protocol):
    def __call__(
        self,
        event: ExchangeOrderEvent,
        *,
        allow_remote_side_effects: bool = True,
    ) -> dict[str, object]: ...


class _ProtectionRecovery(TypedDict):
    failures: list[dict[str, object]]


class _PricedFill(TypedDict):
    price: Decimal
    quantity: Decimal
    fee: Decimal | None


class OrderReconciler:
    def __init__(
        self,
        *,
        adapter,
        order_manager,
        clock,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]],
        process_exchange_order_event: _ExchangeOrderEventProcessor,
        place_pending_protection_for_filled_entries: Callable[[], dict[str, object]],
        fail_pending_conditionals_for_terminal_entry: Callable[[object], None],
        protective_terminal_without_fill_failure: Callable[[object], dict | None],
        cancel_protective_order_when_sibling_closed: Callable[[object], dict | None],
        cancel_linked_conditional_for_protection_fill: Callable[[object], dict | None],
        local_positions_loader: Callable[[], list[object]] | None = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.adapter = adapter
        adapter_exchange_id = getattr(adapter, "exchange_id", None)
        self._exchange_id = (
            adapter_exchange_id if isinstance(adapter_exchange_id, str) else None
        )
        self.order_manager = order_manager
        self.clock = clock
        self._db_session_factory = db_session_factory
        self.process_exchange_order_event = process_exchange_order_event
        self.place_pending_protection_for_filled_entries = (
            place_pending_protection_for_filled_entries
        )
        self.fail_pending_conditionals_for_terminal_entry = (
            fail_pending_conditionals_for_terminal_entry
        )
        self.protective_terminal_without_fill_failure = (
            protective_terminal_without_fill_failure
        )
        self.cancel_protective_order_when_sibling_closed = (
            cancel_protective_order_when_sibling_closed
        )
        self.cancel_linked_conditional_for_protection_fill = (
            cancel_linked_conditional_for_protection_fill
        )
        self.local_positions_loader = local_positions_loader
        self.logger = logger or logging.getLogger("OrderReconciler")
        self._owned_order_reconciler = (
            adapter.create_owned_order_reconciler(
                OwnedOrderReconciliationContext(
                    list_recoverable_client_orders=self.list_recoverable_client_orders,
                    process_exchange_order_event=self.process_exchange_order_event,
                    now_seconds=lambda: float(self.clock.now()),
                    db_session_factory=self._db_session_factory,
                    local_positions_loader=self.local_positions_loader,
                    logger=self.logger,
                )
            )
            if isinstance(adapter, IExchangeAdapter)
            else None
        )

    def list_recoverable_client_orders(self):
        """Return persisted client orders that need restart reconciliation."""
        statuses = {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
        return self.order_manager.repo.list_client_orders_by_statuses(
            statuses,
            exchange_id=self._exchange_id,
        )

    def record_recoverable_order_scan(self) -> dict:
        """Record a startup scan of client orders that still need reconciliation."""
        if self._db_session_factory is None:
            raise RuntimeError(
                "record_recoverable_order_scan requires db_session_factory"
            )

        orders = self.list_recoverable_client_orders()
        status_counts: dict[str, int] = {}
        for order in orders:
            status_counts[order.status] = status_counts.get(order.status, 0) + 1

        payload = {
            "recoverable_count": len(orders),
            "status_counts": status_counts,
            "orders": [
                {
                    "order_id": order.id,
                    "client_order_id": order.client_order_id,
                    "status": order.status,
                    "strategy_id": order.strategy_id,
                    "product_id": order.product_id,
                    "exchange_order_id": order.exchange_order_id,
                }
                for order in orders
            ],
        }

        with self._db_session_factory() as db:
            try:
                write_system_event(
                    db,
                    event_type="reconcile",
                    event_subtype="startup_recovery_scan",
                    payload=payload,
                )
                db.commit()
            except Exception:
                db.rollback()
                raise

        return payload

    def reconcile_recoverable_client_orders(self) -> dict:
        """Compare recoverable local client orders with exchange snapshots.

        This repairs local order state when the exchange lookup gives enough
        information. It never places replacement orders during startup, with
        one deliberate exception: NEW pending protective orders whose entry
        already has fills are first placements (not replacements) and leaving
        them unplaced keeps a live position naked across restarts.
        """
        if self._db_session_factory is None:
            raise RuntimeError(
                "reconcile_recoverable_client_orders requires db_session_factory"
            )

        orders = self.list_recoverable_client_orders()
        results = []
        result_counts: dict[str, int] = {}
        decision_counts: dict[str, int] = {}

        # Decide skips on the scan snapshot up front: repairing an entry can
        # fail its pending conditionals mid-loop, and a status-based check on
        # the mutated order would stop skipping them.
        pending_protective_ids = {
            order.id for order in orders if self._is_pending_protective_order(order)
        }
        for order in orders:
            if order.id in pending_protective_ids:
                continue
            local_status = order.status
            local_exchange_order_id = order.exchange_order_id
            try:
                snapshot = self.adapter.get_order_by_client_id(
                    order.client_order_id,
                    order.product_id,
                    order_type=order.type,
                )
            except ExchangeOrderLookupUnsupported:
                snapshot = None
                result = "exchange_lookup_unsupported"
            else:
                result = (
                    "exchange_found" if snapshot is not None else "exchange_not_found"
                )
            result_counts[result] = result_counts.get(result, 0) + 1
            if result == "exchange_lookup_unsupported":
                decision = "exchange_unknown"
                repair = self._repair_result(
                    "none",
                    reason="exchange_lookup_unsupported",
                    verification_blocked=True,
                )
            else:
                decision = self._reconcile_decision(
                    order.status, snapshot.status if snapshot else None
                )
                repair = self._repair_reconciled_order(order, decision, snapshot)
            decision_counts[decision] = decision_counts.get(decision, 0) + 1
            unresolved = bool(repair["unresolved"])
            verification_blocked = bool(repair["verification_blocked"])
            results.append(
                {
                    "order_id": order.id,
                    "client_order_id": order.client_order_id,
                    "local_status": local_status,
                    "product_id": order.product_id,
                    "strategy_id": order.strategy_id,
                    "local_exchange_order_id": local_exchange_order_id,
                    "result": result,
                    "decision": decision,
                    "exchange_order_id": snapshot.exchange_order_id
                    if snapshot
                    else None,
                    "exchange_status": snapshot.status if snapshot else None,
                    "repair_action": repair["action"],
                    "repair_reason": repair.get("reason"),
                    "unresolved": unresolved,
                    "verification_blocked": verification_blocked,
                }
            )

        protection_recovery = self.place_pending_protection_for_filled_entries()
        protection_failures = cast(
            _ProtectionRecovery,
            protection_recovery,
        )["failures"]
        reconciliation_unresolved_count = sum(
            1 for result in results if result["unresolved"]
        )
        protection_unresolved_count = len(protection_failures)

        payload = {
            "recoverable_count": len(orders),
            "result_counts": result_counts,
            "decision_counts": decision_counts,
            "unresolved_count": (
                reconciliation_unresolved_count + protection_unresolved_count
            ),
            "reconciliation_unresolved_count": reconciliation_unresolved_count,
            "protection_unresolved_count": protection_unresolved_count,
            "verification_blocked_count": sum(
                1 for result in results if result["verification_blocked"]
            ),
            "results": results,
            "protection_recovery": protection_recovery,
            "skipped_pending_protection_count": len(pending_protective_ids),
        }
        if payload["unresolved_count"] > 0:
            self.logger.error(
                "Startup reconciliation has %s unresolved repair(s)",
                payload["unresolved_count"],
            )
        if payload["verification_blocked_count"] > 0:
            self.logger.warning(
                "Startup reconciliation has %s verification-blocked order(s)",
                payload["verification_blocked_count"],
            )

        with self._db_session_factory() as db:
            try:
                write_system_event(
                    db,
                    event_type="reconcile",
                    event_subtype="startup_exchange_reconcile",
                    payload=payload,
                )
                db.commit()
            except Exception:
                db.rollback()
                raise

        return payload

    def reconcile_owned_orders(self, *, snapshot_loader=None) -> dict[str, object]:
        """Delegate owned-order recovery to the adapter capability."""
        if self._owned_order_reconciler is None:
            raise ExchangeError("owned_order_reconciliation_unsupported")
        return self._owned_order_reconciler.reconcile(snapshot_loader=snapshot_loader)

    def resync_recoverable_order_events(self) -> dict[str, object]:
        """Resync recoverable live orders through the order-event state machine.

        This is intended for disconnect recovery. It converts REST order
        snapshots into ``ExchangeOrderEvent`` instances, then delegates all
        fill/status accounting to ``process_exchange_order_event`` so live
        stream replay and REST catch-up cannot drift into separate semantics.
        """
        orders = self.list_recoverable_client_orders()
        results: list[dict[str, object]] = []
        pending_protective_ids = {
            order.id for order in orders if self._is_pending_protective_order(order)
        }
        for order in orders:
            if order.id in pending_protective_ids:
                continue
            try:
                snapshot = self.adapter.get_order_by_client_id(
                    order.client_order_id,
                    order.product_id,
                    order_type=order.type,
                )
            except ExchangeOrderLookupUnsupported:
                results.append(
                    {
                        "order_id": order.id,
                        "client_order_id": order.client_order_id,
                        "action": "verification_blocked_order_lookup_unsupported",
                        "verification_blocked": True,
                        "unresolved": False,
                    }
                )
                continue
            except ExchangeError as e:
                results.append(
                    {
                        "order_id": order.id,
                        "client_order_id": order.client_order_id,
                        "action": "verification_blocked_order_lookup_failed",
                        "reason": str(e),
                        "verification_blocked": True,
                        "unresolved": False,
                    }
                )
                continue

            if snapshot is None:
                results.append(
                    {
                        "order_id": order.id,
                        "client_order_id": order.client_order_id,
                        "action": "verification_blocked_order_snapshot_missing",
                        "verification_blocked": True,
                        "unresolved": False,
                    }
                )
                continue

            event = self._exchange_snapshot_to_order_event(order.product_id, snapshot)
            result = self.process_exchange_order_event(event)
            result["verification_blocked"] = self._resync_action_verification_blocked(
                str(result["action"])
            )
            result["unresolved"] = str(result["action"]).startswith("unresolved_")
            results.append(result)

        return {
            "recoverable_count": len(orders),
            "applied_count": sum(
                1 for result in results if result["action"] == "applied"
            ),
            "unresolved_count": sum(1 for result in results if result["unresolved"]),
            "verification_blocked_count": sum(
                1 for result in results if result["verification_blocked"]
            ),
            "skipped_pending_protection_count": len(pending_protective_ids),
            "results": results,
        }

    @staticmethod
    def _exchange_snapshot_to_order_event(product_id: str, snapshot):
        return exchange_snapshot_to_order_event(product_id, snapshot)

    @staticmethod
    def _resync_action_verification_blocked(action: str) -> bool:
        return action in {"unknown_order", "unknown_status"}

    def _repair_reconciled_order(
        self, order, decision: str, snapshot
    ) -> dict[str, object]:
        if decision == "local_only":
            self.order_manager.fail_order(
                order, "startup reconciliation: local order not found on exchange"
            )
            self.fail_pending_conditionals_for_terminal_entry(order)
            self._mark_reconciled(order)
            return self._repair_result("marked_failed")

        if snapshot is None:
            return self._repair_result(
                "none",
                reason="exchange_snapshot_unavailable",
                verification_blocked=True,
            )

        if decision == "exchange_unknown":
            return self._repair_result(
                "none",
                reason="exchange_status_unrecognized",
                verification_blocked=True,
            )

        if decision == "exchange_open":
            stale_protective = self.cancel_protective_order_when_sibling_closed(order)
            if stale_protective is not None:
                if stale_protective["cancelled"]:
                    self._mark_reconciled(order)
                    return self._repair_result("cancelled_stale_protective_leg")
                return self._repair_result(
                    "unresolved_linked_conditional_cancel_failed",
                    reason="stale_protective_leg_cancel_failed",
                    unresolved=True,
                )
            partial_repair = self._record_open_snapshot_fill_delta(order, snapshot)
            self.order_manager.mark_submitted(order, snapshot.exchange_order_id)
            if (order.filled_quantity or Decimal("0")) > 0:
                order.status = OrderStatus.PARTIALLY_FILLED.value
                self.order_manager.repo.update_order(order)
            if partial_repair["unresolved"]:
                return partial_repair
            if partial_repair["action"] != "none":
                self._mark_reconciled(order)
                return partial_repair
            self._mark_reconciled(order)
            return self._repair_result("restored_tracking")

        if decision == "exchange_closed":
            normalized_status = snapshot.status.lower()
            terminal_status = self._terminal_order_status(normalized_status)
            if terminal_status is None:
                return self._repair_result("none", reason="decision_not_repairable")

            fill_state, terminal_fill = self._classify_fill_delta(order, snapshot)
            if fill_state == FillDeltaState.LOCAL_OVERSTATED:
                return self._repair_result(
                    "unresolved_terminal_local_fill_exceeds_exchange",
                    reason="exchange_filled_quantity_less_than_local",
                    unresolved=True,
                )

            if fill_state == FillDeltaState.DELTA_UNPRICED:
                return self._repair_result(
                    "unresolved_missing_fill_price",
                    reason="exchange_snapshot_missing_fill_price",
                    unresolved=True,
                )

            if fill_state == FillDeltaState.DELTA_PRICED:
                priced_fill = cast(_PricedFill, terminal_fill)
                fee_rejection = snapshot_fill_fee_rejection(
                    local_filled=order.filled_quantity or Decimal("0"),
                    fill_quantity=priced_fill["quantity"],
                    fee=snapshot.fee,
                    fee_asset=snapshot.fee_asset,
                )
                if fee_rejection is not None:
                    return self._repair_result(
                        fee_rejection,
                        reason=fee_rejection.removeprefix("unresolved_"),
                        unresolved=True,
                    )
                self.order_manager.record_fill_delta(
                    order,
                    priced_fill["price"],
                    priced_fill["quantity"],
                    snapshot.filled_quantity,
                    snapshot.average_price,
                    terminal_status=terminal_status,
                    fee=priced_fill["fee"],
                    fee_asset=snapshot.fee_asset,
                )
                cancel_failure = self.cancel_linked_conditional_for_protection_fill(
                    order
                )
                if cancel_failure is not None:
                    return self._repair_result(
                        "unresolved_linked_conditional_cancel_failed",
                        reason="sibling_cancel_failed_after_reconciled_protective_fill",
                        unresolved=True,
                    )
                self._mark_reconciled(order)
                return self._repair_result(
                    self._filled_terminal_repair_action(terminal_status),
                )

            order.status = terminal_status.value
            self.order_manager.repo.update_order(order)
            if terminal_status in {OrderStatus.CANCELLED, OrderStatus.FAILED}:
                self.fail_pending_conditionals_for_terminal_entry(order)
                protective_failure = self.protective_terminal_without_fill_failure(
                    order
                )
                if protective_failure is not None:
                    self._mark_reconciled(order)
                    return self._repair_result(
                        "unresolved_protective_terminal_without_fill",
                        reason="protective_terminal_without_fill",
                        unresolved=True,
                    )
            self._mark_reconciled(order)
            return self._repair_result(
                self._terminal_repair_action(terminal_status),
                reason=self._terminal_no_delta_reason(
                    normalized_status,
                    fill_state,
                ),
            )

        return self._repair_result("none", reason="decision_not_repairable")

    @staticmethod
    def _repair_result(
        action: str,
        *,
        reason: Optional[str] = None,
        unresolved: bool = False,
        verification_blocked: bool = False,
    ) -> dict[str, object]:
        """Build a reconciliation repair result.

        unresolved means an exchange snapshot proved an order/fill accounting
        mismatch that could not be repaired automatically. verification_blocked
        means reconciliation could not verify exchange state, so operators must
        resolve or explicitly waive the unknown state before production gates.
        """
        return {
            "action": action,
            "reason": reason,
            "unresolved": unresolved,
            "verification_blocked": verification_blocked,
        }

    @staticmethod
    def _terminal_order_status(
        normalized_exchange_status: str,
    ) -> Optional[OrderStatus]:
        if normalized_exchange_status in {"closed", "filled"}:
            return OrderStatus.FILLED
        if normalized_exchange_status in {"canceled", "cancelled"}:
            return OrderStatus.CANCELLED
        if normalized_exchange_status in {"rejected", "expired", "failed"}:
            return OrderStatus.FAILED
        return None

    @staticmethod
    def _terminal_repair_action(status: OrderStatus) -> str:
        if status == OrderStatus.CANCELLED:
            return "marked_cancelled"
        if status == OrderStatus.FAILED:
            return "marked_failed"
        return "marked_filled_without_fill"

    @staticmethod
    def _filled_terminal_repair_action(status: OrderStatus) -> str:
        if status == OrderStatus.CANCELLED:
            return "filled_delta_and_marked_cancelled"
        if status == OrderStatus.FAILED:
            return "filled_delta_and_marked_failed"
        return "filled_from_exchange_snapshot"

    @staticmethod
    def _terminal_no_delta_reason(
        normalized_exchange_status: str,
        fill_state: FillDeltaState,
    ) -> Optional[str]:
        if fill_state == FillDeltaState.CONVERGED:
            return "exchange_fill_already_recorded"
        if normalized_exchange_status in {"closed", "filled"}:
            return "exchange_snapshot_missing_fill_details"
        return None

    @staticmethod
    def _snapshot_fill_delta(order, snapshot) -> Optional[FillDelta]:
        return snapshot_fill_delta(
            local_filled=order.filled_quantity or Decimal("0"),
            local_average_price=order.filled_price,
            cumulative_filled=snapshot.filled_quantity,
            cumulative_average_price=snapshot.average_price,
            cumulative_fee=snapshot.fee,
        )

    @classmethod
    def _classify_fill_delta(
        cls,
        order,
        snapshot,
    ) -> tuple[FillDeltaState, Optional[FillDelta]]:
        return classify_fill_delta(
            local_filled=order.filled_quantity or Decimal("0"),
            local_average_price=order.filled_price,
            cumulative_filled=snapshot.filled_quantity,
            cumulative_average_price=snapshot.average_price,
            cumulative_fee=snapshot.fee,
        )

    def _record_open_snapshot_fill_delta(self, order, snapshot) -> dict[str, object]:
        fill_state, fill_delta = self._classify_fill_delta(order, snapshot)
        if fill_state == FillDeltaState.NO_FILL:
            return self._repair_result("none")
        if fill_state == FillDeltaState.LOCAL_OVERSTATED:
            return self._repair_result(
                "unresolved_open_local_fill_exceeds_exchange",
                reason="exchange_filled_quantity_less_than_local",
                unresolved=True,
            )
        if fill_state == FillDeltaState.CONVERGED:
            return self._repair_result(
                "none",
                reason="exchange_fill_already_recorded",
            )

        if fill_state == FillDeltaState.DELTA_UNPRICED:
            return self._repair_result(
                "unresolved_open_missing_fill_price",
                reason="exchange_snapshot_missing_fill_price",
                unresolved=True,
            )

        priced_fill = cast(_PricedFill, fill_delta)
        fee_rejection = snapshot_fill_fee_rejection(
            local_filled=order.filled_quantity or Decimal("0"),
            fill_quantity=priced_fill["quantity"],
            fee=snapshot.fee,
            fee_asset=snapshot.fee_asset,
        )
        if fee_rejection is not None:
            return self._repair_result(
                fee_rejection,
                reason=fee_rejection.removeprefix("unresolved_"),
                unresolved=True,
            )
        self.order_manager.record_partial_fill(
            order,
            priced_fill["price"],
            priced_fill["quantity"],
            snapshot.filled_quantity,
            snapshot.average_price,
            fee=priced_fill["fee"],
            fee_asset=snapshot.fee_asset,
        )
        return self._repair_result("recorded_partial_fill_and_restored_tracking")

    def _mark_reconciled(self, order) -> None:
        order.last_reconciled_at = datetime.fromtimestamp(
            self.clock.now(), timezone.utc
        )
        self.order_manager.repo.update_order(order)

    @staticmethod
    def _reconcile_decision(local_status: str, exchange_status: Optional[str]) -> str:
        if exchange_status is None:
            if local_status == OrderStatus.NEW.value:
                return "local_only"
            return "exchange_unknown"

        normalized_exchange_status = exchange_status.lower()
        if normalized_exchange_status in {
            "open",
            "new",
            "submitted",
            "partially_filled",
            "submitted_unconfirmed",
        }:
            return "exchange_open"
        if normalized_exchange_status in {
            "closed",
            "filled",
            "canceled",
            "cancelled",
            "rejected",
            "expired",
            "failed",
        }:
            return "exchange_closed"
        return "exchange_unknown"

    @staticmethod
    def _is_pending_protective_order(order) -> bool:
        return (
            order.status == OrderStatus.NEW.value
            and isinstance(order.intent_payload, dict)
            and bool(order.intent_payload.get("pending_entry_order_id"))
        )
