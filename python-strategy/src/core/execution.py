import logging
import time as _time
from datetime import datetime, timezone
from decimal import Decimal
from enum import Enum
from typing import Callable, ContextManager, Optional
from sqlalchemy.orm import Session
from src.core.models import Signal, SignalType, Candlestick, OrderSide, OrderStatus, PositionSide
from src.core.order_manager import OrderManager
from src.core.interfaces.exchange import IExchangeAdapter, ExchangeError, NetworkError
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.interfaces.exchange import ExchangeOrderLookupUnsupported
from src.core.clock import Clock
from src.core.interfaces import IOrderRepository
from src.core.journal import StrategyJournal
from src.core.metrics import ORDERS_TOTAL, EXECUTION_LATENCY
from src.core.audit_service import (
    build_signal_intent_audit,
    write_signal_audit_intent,
    write_signal_audit_outcome,
    write_system_event,
)
from src.core.client_order_id import generate_client_order_id, parse_client_order_id


class FillDeltaState(str, Enum):
    NO_FILL = "no_fill"
    CONVERGED = "converged"
    DELTA_PRICED = "delta_priced"
    DELTA_UNPRICED = "delta_unpriced"
    LOCAL_OVERSTATED = "local_overstated"


class ExecutionEngine:
    def __init__(
        self,
        db_session: Session,
        clock: Clock,
        adapter: IExchangeAdapter,
        order_repository: Optional[IOrderRepository] = None,
        journal: Optional[StrategyJournal] = None,
        is_backtest: Optional[bool] = None,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
        audit_external_orders: bool = False,
    ):
        self.logger = logging.getLogger("ExecutionEngine")
        self.clock = clock
        self._db_session_factory = db_session_factory
        self.audit_external_orders = audit_external_orders
        if order_repository:
            self.order_manager = OrderManager(order_repository, clock, is_backtest=is_backtest)
        else:
            from src.core.repositories import LiveOrderRepository
            self.order_manager = OrderManager(
                LiveOrderRepository(db_session, db_session_factory=db_session_factory),
                clock,
                is_backtest=is_backtest,
            )

        self.default_quantity = Decimal("0.01")
        self.adapter = adapter
        self.journal = journal
        self.logger.info("ExecutionEngine initialized with adapter: %s", type(adapter).__name__)

    def list_recoverable_client_orders(self):
        """Return persisted client orders that need restart reconciliation."""
        statuses = {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
        return self.order_manager.repo.list_client_orders_by_statuses(statuses)

    def record_recoverable_order_scan(self) -> dict:
        """Record a startup scan of client orders that still need reconciliation."""
        if self._db_session_factory is None:
            raise RuntimeError("record_recoverable_order_scan requires db_session_factory")

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
        information. It never places replacement orders during startup.
        """
        if self._db_session_factory is None:
            raise RuntimeError("reconcile_recoverable_client_orders requires db_session_factory")

        orders = self.list_recoverable_client_orders()
        results = []
        result_counts: dict[str, int] = {}
        decision_counts: dict[str, int] = {}

        for order in orders:
            local_status = order.status
            local_exchange_order_id = order.exchange_order_id
            try:
                snapshot = self.adapter.get_order_by_client_id(
                    order.client_order_id,
                    order.product_id,
                )
            except ExchangeOrderLookupUnsupported:
                snapshot = None
                result = "exchange_lookup_unsupported"
            else:
                result = "exchange_found" if snapshot is not None else "exchange_not_found"
            result_counts[result] = result_counts.get(result, 0) + 1
            if result == "exchange_lookup_unsupported":
                decision = "exchange_unknown"
                repair = self._repair_result(
                    "none",
                    reason="exchange_lookup_unsupported",
                    verification_blocked=True,
                )
            else:
                decision = self._reconcile_decision(order.status, snapshot.status if snapshot else None)
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
                    "exchange_order_id": snapshot.exchange_order_id if snapshot else None,
                    "exchange_status": snapshot.status if snapshot else None,
                    "repair_action": repair["action"],
                    "repair_reason": repair.get("reason"),
                    "unresolved": unresolved,
                    "verification_blocked": verification_blocked,
                }
            )

        payload = {
            "recoverable_count": len(orders),
            "result_counts": result_counts,
            "decision_counts": decision_counts,
            "unresolved_count": sum(1 for result in results if result["unresolved"]),
            "verification_blocked_count": sum(
                1 for result in results if result["verification_blocked"]
            ),
            "results": results,
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

    def resync_recoverable_order_events(self) -> dict[str, object]:
        """Resync recoverable live orders through the order-event state machine.

        This is intended for disconnect recovery. It converts REST order
        snapshots into ``ExchangeOrderEvent`` instances, then delegates all
        fill/status accounting to ``process_exchange_order_event`` so live
        stream replay and REST catch-up cannot drift into separate semantics.
        """
        orders = self.list_recoverable_client_orders()
        results: list[dict[str, object]] = []
        for order in orders:
            try:
                snapshot = self.adapter.get_order_by_client_id(
                    order.client_order_id,
                    order.product_id,
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
            "applied_count": sum(1 for result in results if result["action"] == "applied"),
            "unresolved_count": sum(1 for result in results if result["unresolved"]),
            "verification_blocked_count": sum(
                1 for result in results if result["verification_blocked"]
            ),
            "results": results,
        }

    @staticmethod
    def _exchange_snapshot_to_order_event(
        product_id: str,
        snapshot,
    ) -> ExchangeOrderEvent:
        return ExchangeOrderEvent(
            status=snapshot.status,
            product_id=product_id,
            client_order_id=snapshot.client_order_id,
            exchange_order_id=snapshot.exchange_order_id,
            cumulative_filled_quantity=snapshot.filled_quantity,
            cumulative_average_price=snapshot.average_price,
            fee=snapshot.fee,
            raw=snapshot.raw,
        )

    @staticmethod
    def _resync_action_verification_blocked(action: str) -> bool:
        return action in {"unknown_order", "unknown_status"}

    def _repair_reconciled_order(self, order, decision: str, snapshot) -> dict[str, object]:
        if decision == "local_only":
            self.order_manager.fail_order(order, "startup reconciliation: local order not found on exchange")
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
            partial_repair = self._record_open_snapshot_fill_delta(order, snapshot)
            self.order_manager.mark_submitted(order, snapshot.exchange_order_id)
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
                self.order_manager.record_fill_delta(
                    order,
                    terminal_fill["price"],
                    terminal_fill["quantity"],
                    snapshot.filled_quantity,
                    snapshot.average_price,
                    terminal_status=terminal_status,
                    fee=terminal_fill["fee"],
                )
                self._mark_reconciled(order)
                return self._repair_result(
                    self._filled_terminal_repair_action(terminal_status),
                )

            order.status = terminal_status.value
            self.order_manager.repo.update_order(order)
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
    def _terminal_order_status(normalized_exchange_status: str) -> Optional[OrderStatus]:
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

    def _snapshot_fill_delta(self, order, snapshot) -> Optional[dict[str, Optional[Decimal]]]:
        if snapshot.filled_quantity is None or snapshot.filled_quantity <= 0:
            return None
        local_filled_quantity = order.filled_quantity or Decimal("0")
        fill_delta = self._fill_delta_from_cumulative(
            local_filled=local_filled_quantity,
            local_average_price=order.filled_price,
            cumulative_filled=snapshot.filled_quantity,
            cumulative_average_price=snapshot.average_price,
        )
        fill_delta["fee"] = snapshot.fee if local_filled_quantity <= 0 else None
        return fill_delta

    def _classify_fill_delta(
        self,
        order,
        snapshot,
    ) -> tuple[FillDeltaState, Optional[dict[str, Optional[Decimal]]]]:
        fill_delta = self._snapshot_fill_delta(order, snapshot)
        if fill_delta is None:
            return FillDeltaState.NO_FILL, None
        if fill_delta["quantity"] < 0:
            return FillDeltaState.LOCAL_OVERSTATED, fill_delta
        if fill_delta["quantity"] == 0:
            return FillDeltaState.CONVERGED, fill_delta
        if fill_delta["price"] is None:
            return FillDeltaState.DELTA_UNPRICED, fill_delta
        return FillDeltaState.DELTA_PRICED, fill_delta

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

        self.order_manager.record_partial_fill(
            order,
            fill_delta["price"],
            fill_delta["quantity"],
            snapshot.filled_quantity,
            snapshot.average_price,
            fee=fill_delta["fee"],
        )
        return self._repair_result("recorded_partial_fill_and_restored_tracking")

    def _mark_reconciled(self, order) -> None:
        order.last_reconciled_at = datetime.fromtimestamp(self.clock.now(), timezone.utc)
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

    def process_market_data(self, candle: Candlestick):
        """
        Passes market data to the adapter (if applicable) to check for simulated fills.
        """
        fills = self.adapter.on_market_data(candle)

        if fills:
            for fill in fills:
                order = fill['order']
                price = fill['price']
                qty = fill['quantity']
                fee = fill.get('fee')
                fill_type = fill.get('fill_type', 'MARKET')

                self.logger.info("Execution: Adapter fill for %s at %s (fee=%s)", order.id, price, fee)
                self.order_manager.fill_order(
                    order=order,
                    fill_price=price,
                    fill_quantity=qty,
                    fee=fee,
                )

                if self.journal is not None:
                    self._journal_fill(order, price, qty, fee, fill_type, candle)

    def process_exchange_order_event(self, event: ExchangeOrderEvent) -> dict[str, object]:
        """Apply a live exchange order event to local order, trade, and position state."""
        order = self._resolve_order_event_order(event)
        if order is None:
            return {
                "action": "unknown_order",
                "status": event.status,
                "client_order_id": event.client_order_id,
                "exchange_order_id": event.exchange_order_id,
            }

        if event.exchange_order_id and order.exchange_order_id != event.exchange_order_id:
            self.order_manager.update_exchange_order_id(order, event.exchange_order_id)

        event_state = self._classify_exchange_order_event_status(event.status)
        if event_state == "unknown":
            return {
                "action": "unknown_status",
                "order_id": order.id,
                "status": event.status,
            }
        if self._has_non_idempotent_last_fill_only(event):
            return {
                "action": "unresolved_last_fill_without_cumulative_quantity",
                "order_id": order.id,
                "status": event.status,
            }

        fill_delta = self._exchange_order_event_fill_delta(order, event)
        if fill_delta["quantity"] < 0:
            return {
                "action": "unresolved_local_fill_exceeds_exchange",
                "order_id": order.id,
                "status": event.status,
            }
        if fill_delta["quantity"] > 0 and fill_delta["price"] is None:
            return {
                "action": "unresolved_missing_fill_price",
                "order_id": order.id,
                "status": event.status,
            }
        if self._event_fill_exceeds_order_quantity(order, event):
            return {
                "action": "unresolved_exchange_fill_exceeds_order_quantity",
                "order_id": order.id,
                "status": event.status,
            }
        if (
            fill_delta["quantity"] == 0
            and self._requires_terminal_fill_quantity(order, event, event_state)
        ):
            return {
                "action": "unresolved_missing_terminal_fill_quantity",
                "order_id": order.id,
                "status": event.status,
            }
        if self._terminal_event_underfills_order(order, event, event_state, fill_delta):
            return {
                "action": "unresolved_terminal_fill_quantity_below_order_quantity",
                "order_id": order.id,
                "status": event.status,
            }

        if fill_delta["quantity"] > 0:
            terminal_status = self._status_for_exchange_event_fill(event_state)
            cumulative_quantity = event.cumulative_filled_quantity or (
                (order.filled_quantity or Decimal("0")) + fill_delta["quantity"]
            )
            cumulative_average = event.cumulative_average_price or fill_delta["price"]
            self.order_manager.record_fill_delta(
                order,
                fill_delta["price"],
                fill_delta["quantity"],
                cumulative_filled_quantity=cumulative_quantity,
                cumulative_average_price=cumulative_average,
                terminal_status=terminal_status,
                fee=event.fee,
                fee_asset=event.fee_asset,
            )
            if self.journal is not None:
                self._journal_exchange_order_event_fill(
                    order,
                    event,
                    fill_delta["price"],
                    fill_delta["quantity"],
                )
        else:
            self._apply_exchange_order_event_status(order, event_state, event)

        if fill_delta["quantity"] > 0:
            placement_failures = self._place_pending_conditional_orders_for_entry(order)
            if placement_failures:
                self._try_write_conditional_order_event_warning(
                    event_subtype="conditional_order_placement_failed_after_entry_fill",
                    order=order,
                    failures=placement_failures,
                )
                return {
                    "action": "unresolved_conditional_order_placement_failed",
                    "order_id": order.id,
                    "status": event.status,
                    "state": event_state,
                    "fill_quantity": fill_delta["quantity"],
                    "exchange_order_id": order.exchange_order_id,
                    "failures": placement_failures,
                }
            protective_partial_failure = (
                self._protective_partial_fill_requires_resize(order, event_state)
            )
            if protective_partial_failure is not None:
                self._try_write_conditional_order_event_warning(
                    event_subtype="protective_partial_fill_requires_resize",
                    order=order,
                    failures=[protective_partial_failure],
                )
                return {
                    "action": "unresolved_protective_partial_fill_requires_resize",
                    "order_id": order.id,
                    "status": event.status,
                    "state": event_state,
                    "fill_quantity": fill_delta["quantity"],
                    "exchange_order_id": order.exchange_order_id,
                    "failure": protective_partial_failure,
                }
            cancel_failure = self._cancel_linked_conditional_order_for_protection_fill(order)
            if cancel_failure is not None:
                self._try_write_conditional_order_event_warning(
                    event_subtype="linked_conditional_order_cancel_failed",
                    order=order,
                    failures=[cancel_failure],
                )
                return {
                    "action": "unresolved_linked_conditional_cancel_failed",
                    "order_id": order.id,
                    "status": event.status,
                    "state": event_state,
                    "fill_quantity": fill_delta["quantity"],
                    "exchange_order_id": order.exchange_order_id,
                    "failure": cancel_failure,
                }

        return {
            "action": "applied",
            "order_id": order.id,
            "status": event.status,
            "state": event_state,
            "fill_quantity": fill_delta["quantity"],
            "exchange_order_id": order.exchange_order_id,
        }

    def _resolve_order_event_order(self, event: ExchangeOrderEvent):
        if event.client_order_id:
            order = self.order_manager.repo.get_order_by_client_order_id(
                event.client_order_id
            )
            if order is not None:
                return order
        if event.exchange_order_id:
            exchange_id = self._exchange_id_for_order_event(event)
            return self.order_manager.repo.get_order_by_exchange_order_id(
                event.exchange_order_id,
                exchange_id=exchange_id,
                product_id=event.product_id,
            )
        return None

    @staticmethod
    def _exchange_id_for_order_event(event: ExchangeOrderEvent) -> str | None:
        if ":" not in event.product_id:
            return None
        return event.product_id.split(":", 1)[0]

    @staticmethod
    def _classify_exchange_order_event_status(status: str) -> str:
        normalized = (status or "").lower()
        if normalized in {"new", "open", "submitted", "accepted"}:
            return "open"
        if normalized in {"partially_filled", "partial", "partiallyfilled"}:
            return "partial"
        if normalized in {"filled", "closed"}:
            return "filled"
        if normalized in {"canceled", "cancelled"}:
            return "cancelled"
        if normalized in {"rejected"}:
            return "rejected"
        if normalized in {"expired"}:
            return "expired"
        if normalized in {"failed"}:
            return "failed"
        if normalized in {"liquidated", "adl", "force_closed", "forced_liquidation"}:
            return "liquidated"
        return "unknown"

    @staticmethod
    def _terminal_status_for_exchange_event(event_state: str) -> OrderStatus | None:
        return {
            "filled": OrderStatus.FILLED,
            "cancelled": OrderStatus.CANCELLED,
            "rejected": OrderStatus.FAILED,
            "expired": OrderStatus.FAILED,
            "failed": OrderStatus.FAILED,
            "liquidated": OrderStatus.LIQUIDATED,
        }.get(event_state)

    @staticmethod
    def _status_for_exchange_event_fill(event_state: str) -> OrderStatus | None:
        if event_state in {"open", "partial"}:
            return OrderStatus.PARTIALLY_FILLED
        return ExecutionEngine._terminal_status_for_exchange_event(event_state)

    def _exchange_order_event_fill_delta(
        self,
        order,
        event: ExchangeOrderEvent,
    ) -> dict[str, Decimal | None]:
        local_filled = order.filled_quantity or Decimal("0")
        cumulative = event.cumulative_filled_quantity
        if cumulative is None:
            return {"quantity": Decimal("0"), "price": None}

        delta = cumulative - local_filled
        if delta <= 0:
            return {"quantity": delta, "price": None}

        price = None
        if (
            event.last_fill_price is not None
            and event.last_fill_quantity == delta
        ):
            price = event.last_fill_price
        elif event.cumulative_average_price is not None:
            price = self._fill_delta_from_cumulative(
                local_filled=local_filled,
                local_average_price=order.filled_price,
                cumulative_filled=cumulative,
                cumulative_average_price=event.cumulative_average_price,
            )["price"]
        return {"quantity": delta, "price": price}

    @staticmethod
    def _has_non_idempotent_last_fill_only(event: ExchangeOrderEvent) -> bool:
        return (
            event.cumulative_filled_quantity is None
            and event.last_fill_quantity is not None
            and event.last_fill_quantity > 0
        )

    @staticmethod
    def _fill_delta_from_cumulative(
        *,
        local_filled: Decimal,
        local_average_price: Decimal | None,
        cumulative_filled: Decimal,
        cumulative_average_price: Decimal | None,
    ) -> dict[str, Decimal | None]:
        delta = cumulative_filled - local_filled
        if delta <= 0:
            return {"quantity": delta, "price": cumulative_average_price}
        if cumulative_average_price is None:
            return {"quantity": delta, "price": None}
        return {
            "quantity": delta,
            "price": ExecutionEngine._delta_price_from_cumulative_average(
                local_filled=local_filled,
                local_average_price=local_average_price,
                cumulative_filled=cumulative_filled,
                cumulative_average_price=cumulative_average_price,
                delta=delta,
            ),
        }

    @staticmethod
    def _delta_price_from_cumulative_average(
        *,
        local_filled: Decimal,
        local_average_price: Decimal | None,
        cumulative_filled: Decimal,
        cumulative_average_price: Decimal,
        delta: Decimal,
    ) -> Decimal | None:
        if local_filled <= 0:
            return cumulative_average_price
        if local_average_price is None or local_average_price <= 0:
            return None
        cumulative_cost = cumulative_filled * cumulative_average_price
        local_cost = local_filled * local_average_price
        return (cumulative_cost - local_cost) / delta

    @staticmethod
    def _requires_terminal_fill_quantity(
        order,
        event: ExchangeOrderEvent,
        event_state: str,
    ) -> bool:
        if event_state not in {"filled", "liquidated"}:
            return False
        has_fill_quantity = (
            event.cumulative_filled_quantity is not None
            or (
                event.last_fill_quantity is not None
                and event.last_fill_quantity > 0
            )
        )
        if has_fill_quantity:
            return False
        local_filled = order.filled_quantity or Decimal("0")
        order_quantity = order.quantity or Decimal("0")
        return local_filled < order_quantity

    @staticmethod
    def _event_fill_exceeds_order_quantity(order, event: ExchangeOrderEvent) -> bool:
        order_quantity = order.quantity or Decimal("0")
        if order_quantity <= 0 or event.cumulative_filled_quantity is None:
            return False
        return event.cumulative_filled_quantity > order_quantity

    @staticmethod
    def _terminal_event_underfills_order(
        order,
        event: ExchangeOrderEvent,
        event_state: str,
        fill_delta: dict[str, Decimal | None],
    ) -> bool:
        if event_state not in {"filled", "liquidated"}:
            return False
        order_quantity = order.quantity or Decimal("0")
        if order_quantity <= 0:
            return False
        local_filled = order.filled_quantity or Decimal("0")
        effective_filled = event.cumulative_filled_quantity
        if effective_filled is None:
            effective_filled = local_filled + (fill_delta["quantity"] or Decimal("0"))
        return effective_filled < order_quantity

    def _apply_exchange_order_event_status(
        self,
        order,
        event_state: str,
        event: ExchangeOrderEvent,
    ) -> None:
        if event_state == "open":
            if self._has_exchange_order_event_fill_progress(order, event):
                order.status = OrderStatus.PARTIALLY_FILLED.value
                self.order_manager.repo.update_order(order)
            else:
                self.order_manager.mark_submitted(order)
        elif event_state == "partial":
            order.status = OrderStatus.PARTIALLY_FILLED.value
            self.order_manager.repo.update_order(order)
        elif event_state == "filled":
            order.status = OrderStatus.FILLED.value
            self.order_manager.repo.update_order(order)
        elif event_state == "cancelled":
            self.order_manager.mark_cancelled(order)
        elif event_state in {"rejected", "expired", "failed"}:
            self.order_manager.fail_order(order, f"exchange_event_{event_state}")
        elif event_state == "liquidated":
            order.status = OrderStatus.LIQUIDATED.value
            self.order_manager.repo.update_order(order)

    @staticmethod
    def _has_exchange_order_event_fill_progress(
        order,
        event: ExchangeOrderEvent,
    ) -> bool:
        return (
            (order.filled_quantity or Decimal("0")) > 0
            or (event.cumulative_filled_quantity or Decimal("0")) > 0
        )

    def cancel_order(self, order_id: str) -> bool:
        """Cancel a known order through the exchange adapter."""
        order = self.order_manager.repo.get_order(order_id)
        if order is None:
            return False
        if order.status == OrderStatus.CANCELLED.value:
            return True

        client_order_id = getattr(order, "client_order_id", None)
        if client_order_id and self.adapter.cancel_order_by_client_id(
            client_order_id,
            order.product_id,
        ):
            self.order_manager.mark_cancelled(order)
            return True

        exchange_order_id = order.exchange_order_id or order.id
        if not self.adapter.cancel_order(exchange_order_id, order.product_id):
            return False

        self.order_manager.mark_cancelled(order)
        return True

    def execute_signal(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """
        Converts Signal to Order and delegates execution to the Adapter.
        Also places SL/TP/Trailing orders when specified in the signal.
        Returns the Order ID (Internal) if successful.
        """
        if self.audit_external_orders:
            return self._execute_signal_with_audit(signal, candle)
        return self._execute_signal_core(signal, candle)

    def _execute_signal_core(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """Current non-audited signal execution path."""
        side = self._determine_side(signal.type)
        if not side:
            return None

        # Determine Quantity
        qty = self._quantity_for_signal(signal)

        # Determine Order Type and Price
        if signal.price and signal.price > 0:
            order_type = "limit"
            limit_price = signal.price
        elif signal.value:
            order_type = "limit"
            limit_price = signal.value
        else:
            order_type = "market"
            limit_price = None

        # 1. Create Entry Order in DB
        order = self.order_manager.create_order(
            signal=signal,
            side=side,
            order_type=order_type,
            quantity=qty,
            price=limit_price
        )
        self._attach_min_notional_reference_price(order, candle)
        conditional_orders = self._create_conditional_orders(signal, order, qty, candle)
        try:
            self._validate_order_group([order, *conditional_orders])
        except ExchangeError as e:
            self.logger.error("Execution validation failed: %s", e)
            self.order_manager.fail_order(order, str(e))
            for conditional_order in conditional_orders:
                self.order_manager.fail_order(conditional_order, str(e))
            self._record_order_rejection(
                order=order,
                order_type=order_type,
                error=e,
                phase="validation",
            )
            return None

        # 2. Execute via Adapter
        try:
            self.logger.info("Sending Order %s via Adapter...", order.id)
            t0 = _time.monotonic()
            exchange_id = self.adapter.place_order(order)
            EXECUTION_LATENCY.observe(_time.monotonic() - t0)
            self.order_manager.update_exchange_order_id(order, exchange_id)
            self.logger.info("Order Placed. Internal: %s, Exchange: %s", order.id, exchange_id)
            ORDERS_TOTAL.labels(
                order_type=order_type,
                status="placed",
                reason="none",
            ).inc()
        except ExchangeError as e:
            self.logger.error("Execution Failed: %s", e)
            self.order_manager.fail_order(order, str(e))
            for conditional_order in conditional_orders:
                self.order_manager.fail_order(conditional_order, "entry_placement_failed")
            self._record_order_rejection(
                order=order,
                order_type=order_type,
                error=e,
                phase="entry_placement",
            )
            return None

        # 3. Journal: record entry
        if self.journal is not None:
            self.journal.log(
                "entry",
                {
                    "order_id": str(order.id),
                    "side": side,
                    "order_type": order_type,
                    # Post-placement order fields: quantization may have adjusted
                    # the submitted values away from the pre-validation locals.
                    "quantity": str(order.quantity),
                    "price": str(order.price) if order.price else "market",
                    "stop_loss": str(signal.stop_loss) if signal.stop_loss else None,
                    "take_profit": str(signal.take_profit) if signal.take_profit else None,
                    "trailing_distance": str(signal.trailing_distance) if signal.trailing_distance else None,
                },
                timestamp=signal.timestamp,
                trade_id=str(order.id),
            )

        # 4. Place conditional orders (SL/TP/Trailing)
        if conditional_orders:
            self._place_conditional_orders(conditional_orders)

        return order.id

    def _execute_signal_with_audit(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """Fail-stop external execution path with committed intent/outcome audits."""
        if self._db_session_factory is None:
            raise RuntimeError("audit_external_orders requires db_session_factory")

        side = self._determine_side(signal.type)
        if not side:
            return None

        qty = self._quantity_for_signal(signal)
        if signal.price and signal.price > 0:
            order_type = "limit"
            limit_price = signal.price
        elif signal.value:
            order_type = "limit"
            limit_price = signal.value
        else:
            order_type = "market"
            limit_price = None

        client_order_id = self._client_order_id_for_signal(signal)
        existing_order = self.order_manager.repo.get_order_by_client_order_id(client_order_id)
        if existing_order is not None:
            self.logger.info("Order already exists for client_order_id=%s", client_order_id)
            return existing_order.id

        intent_payload = {
            "signal": signal.model_dump(mode="json"),
            "order": {
                "side": side.value,
                "order_type": order_type,
                "quantity": qty,
                "price": limit_price,
                "min_notional_reference_price": candle.close if candle else None,
                "client_order_id": client_order_id,
            },
        }
        order = self.order_manager.create_order(
            signal=signal,
            side=side,
            order_type=order_type,
            quantity=qty,
            price=limit_price,
            client_order_id=client_order_id,
            intent_payload=intent_payload,
        )
        self._attach_min_notional_reference_price(order, candle)
        conditional_orders = self._create_conditional_orders(signal, order, qty, candle)

        with self._db_session_factory() as db:
            audit = build_signal_intent_audit(
                clock=self.clock,
                signal=signal,
                client_order_id=client_order_id,
                intent_payload=intent_payload,
            )
            write_signal_audit_intent(db, audit)

        submit_attempted = False
        try:
            self._validate_order_group([order, *conditional_orders])
            self.order_manager.mark_submitted_unconfirmed(order)
            self.logger.info("Sending Order %s via Adapter...", order.id)
            t0 = _time.monotonic()
            submit_attempted = True
            exchange_id = self.adapter.place_order(order)
            EXECUTION_LATENCY.observe(_time.monotonic() - t0)
            self.order_manager.mark_submitted(order, exchange_id)
            self.logger.info("Order Placed. Internal: %s, Exchange: %s", order.id, exchange_id)
            ORDERS_TOTAL.labels(
                order_type=order_type,
                status="placed",
                reason="none",
            ).inc()
        except ExchangeError as e:
            self.logger.error("Execution Failed: %s", e)
            adoption = self._adopt_order_after_ambiguous_submit_error(
                order,
                e,
                submit_attempted=submit_attempted,
            )
            if adoption["action"] == "adopted":
                exchange_id = str(adoption["exchange_order_id"])
                order.exchange_order_id = exchange_id
                ORDERS_TOTAL.labels(
                    order_type=order_type,
                    status="placed",
                    reason="adopted_after_submit_error",
                ).inc()
            elif adoption.get("terminal"):
                for conditional_order in conditional_orders:
                    self.order_manager.fail_order(
                        conditional_order,
                        "entry_placement_terminal_after_submit_error",
                    )
                ORDERS_TOTAL.labels(
                    order_type=order_type,
                    status="failed",
                    reason="terminal_after_submit_error",
                ).inc()
                with self._db_session_factory() as db:
                    write_signal_audit_outcome(
                        db,
                        audit,
                        order_id=order.id,
                        risk_message=str(e),
                        outcome_payload={
                            "status": "terminal_after_submit_error",
                            "error": str(e),
                            "adoption": adoption,
                        },
                    )
                raise
            elif adoption["verification_blocked"] or adoption.get("unresolved"):
                self._mark_conditional_orders_pending_after_uncertain_submit(
                    entry_order=order,
                    conditional_orders=conditional_orders,
                    adoption=adoption,
                )
                reason = self._record_order_rejection(
                    order=order,
                    order_type=order_type,
                    error=e,
                    phase="audited_execution",
                    write_event=False,
                )
                with self._db_session_factory() as db:
                    self._write_order_rejection_event(
                        db,
                        order=order,
                        order_type=order_type,
                        reason=reason,
                        error=e,
                        phase="audited_execution",
                    )
                    self._write_pending_protection_warning(
                        db,
                        entry_order=order,
                        conditional_orders=conditional_orders,
                        adoption=adoption,
                        error=e,
                    )
                    write_signal_audit_outcome(
                        db,
                        audit,
                        order_id=order.id,
                        risk_message=str(e),
                        outcome_payload={
                            "status": (
                                "unresolved"
                                if adoption.get("unresolved")
                                else "verification_blocked"
                            ),
                            "error": str(e),
                            "adoption": adoption,
                        },
                    )
                raise
            else:
                self.order_manager.fail_order(order, str(e))
                for conditional_order in conditional_orders:
                    self.order_manager.fail_order(conditional_order, str(e))
                reason = self._record_order_rejection(
                    order=order,
                    order_type=order_type,
                    error=e,
                    phase="audited_execution",
                    write_event=False,
                )
                with self._db_session_factory() as db:
                    self._write_order_rejection_event(
                        db,
                        order=order,
                        order_type=order_type,
                        reason=reason,
                        error=e,
                        phase="audited_execution",
                    )
                    write_signal_audit_outcome(
                        db,
                        audit,
                        order_id=order.id,
                        risk_message=str(e),
                        outcome_payload={"status": "failed", "error": str(e)},
                    )
                raise

        with self._db_session_factory() as db:
            write_signal_audit_outcome(
                db,
                audit,
                order_id=order.id,
                risk_message="placed",
                outcome_payload={"status": "placed", "exchange_order_id": exchange_id},
            )

        if self.journal is not None:
            self.journal.log(
                "entry",
                {
                    "order_id": str(order.id),
                    "side": side,
                    "order_type": order_type,
                    # Post-placement order fields: quantization may have adjusted
                    # the submitted values away from the pre-validation locals.
                    "quantity": str(order.quantity),
                    "price": str(order.price) if order.price else "market",
                    "stop_loss": str(signal.stop_loss) if signal.stop_loss else None,
                    "take_profit": str(signal.take_profit) if signal.take_profit else None,
                    "trailing_distance": str(signal.trailing_distance) if signal.trailing_distance else None,
                },
                timestamp=signal.timestamp,
                trade_id=str(order.id),
            )

        return order.id

    def _adopt_order_after_ambiguous_submit_error(
        self,
        order,
        error: ExchangeError,
        *,
        submit_attempted: bool,
    ) -> dict[str, object]:
        if not submit_attempted:
            return {
                "action": "submit_not_attempted",
                "verification_blocked": False,
            }
        if not self._is_ambiguous_submit_error(error):
            return {
                "action": "not_ambiguous",
                "verification_blocked": False,
            }
        if not order.client_order_id:
            return {
                "action": "verification_blocked_missing_client_order_id",
                "verification_blocked": True,
            }
        try:
            snapshot = self.adapter.get_order_by_client_id(
                order.client_order_id,
                order.product_id,
            )
        except ExchangeOrderLookupUnsupported:
            return {
                "action": "verification_blocked_order_lookup_unsupported",
                "verification_blocked": True,
            }
        except ExchangeError as lookup_error:
            return {
                "action": "verification_blocked_order_lookup_failed",
                "reason": str(lookup_error),
                "verification_blocked": True,
            }

        if snapshot is None:
            return {
                "action": "verification_blocked_order_snapshot_missing",
                "verification_blocked": True,
            }

        event_result = self.process_exchange_order_event(
            self._exchange_snapshot_to_order_event(order.product_id, snapshot)
        )
        if event_result["action"] != "applied":
            return {
                "action": event_result["action"],
                "event_result": event_result,
                "verification_blocked": self._resync_action_verification_blocked(
                    str(event_result["action"])
                ),
                "unresolved": str(event_result["action"]).startswith("unresolved_"),
            }
        if event_result.get("state") in {
            "cancelled",
            "rejected",
            "expired",
            "failed",
            "liquidated",
        }:
            return {
                "action": "terminal_after_submit_error",
                "event_result": event_result,
                "exchange_order_id": event_result.get("exchange_order_id")
                or snapshot.exchange_order_id,
                "verification_blocked": False,
                "terminal": True,
            }
        exchange_order_id = event_result.get("exchange_order_id") or snapshot.exchange_order_id
        if exchange_order_id is None:
            return {
                "action": "verification_blocked_order_snapshot_missing_exchange_order_id",
                "event_result": event_result,
                "verification_blocked": True,
            }
        return {
            "action": "adopted",
            "event_result": event_result,
            "exchange_order_id": exchange_order_id,
            "verification_blocked": False,
        }

    @staticmethod
    def _is_ambiguous_submit_error(error: ExchangeError) -> bool:
        return isinstance(error, NetworkError)

    def _write_pending_protection_warning(
        self,
        db: Session,
        *,
        entry_order,
        conditional_orders: list,
        adoption: dict[str, object],
        error: ExchangeError,
    ) -> None:
        if not conditional_orders:
            return
        write_system_event(
            db,
            event_type="system_error",
            event_subtype="protective_orders_pending_after_submit_uncertainty",
            related_strategy_id=entry_order.strategy_id,
            related_order_id=str(entry_order.id),
            payload={
                "entry_order_id": str(entry_order.id),
                "client_order_id": entry_order.client_order_id,
                "product_id": entry_order.product_id,
                "conditional_order_ids": [
                    str(conditional_order.id)
                    for conditional_order in conditional_orders
                ],
                "conditional_order_statuses": {
                    str(conditional_order.id): conditional_order.status
                    for conditional_order in conditional_orders
                },
                "adoption_action": adoption["action"],
                "error": str(error),
                "operator_action": (
                    "entry_submit_outcome_uncertain; verify exchange position "
                    "and place or cancel pending protective orders manually"
                ),
            },
        )

    def _mark_conditional_orders_pending_after_uncertain_submit(
        self,
        *,
        entry_order,
        conditional_orders: list,
        adoption: dict[str, object],
    ) -> None:
        for conditional_order in conditional_orders:
            conditional_order.status = OrderStatus.NEW.value
            conditional_order.exchange_order_id = None
            conditional_order.intent_payload = {
                "pending_entry_order_id": str(entry_order.id),
                "pending_client_order_id": entry_order.client_order_id,
                "pending_reason": "entry_submit_outcome_uncertain",
                "adoption_action": str(adoption["action"]),
            }
            self.order_manager.repo.update_order(conditional_order)

    def _attach_min_notional_reference_price(
        self,
        order,
        candle: Optional[Candlestick],
    ) -> None:
        if candle is not None:
            order.min_notional_reference_price = candle.close

    def _client_order_id_for_signal(self, signal: Signal) -> str:
        client_order_id = (signal.metadata or {}).get("client_order_id")
        if isinstance(client_order_id, str):
            parse_client_order_id(client_order_id)
            return client_order_id
        return generate_client_order_id(
            signal.strategy_id,
            "execution",
            signal.type.value.lower(),
        )

    def _quantity_for_signal(self, signal: Signal) -> Decimal:
        if signal.quantity and signal.quantity > 0:
            return signal.quantity
        if signal.type in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT):
            position = self._position_for_exit_signal(signal)
            if position is not None and position.quantity > 0:
                return position.quantity
        return self.default_quantity

    def _position_for_exit_signal(self, signal: Signal):
        try:
            position = self.adapter.get_position(
                signal.product_id,
                strategy_id=signal.strategy_id,
            )
        except TypeError:
            position = self.adapter.get_position(signal.product_id)
        if position is None:
            return None

        position_side = getattr(position.side, "value", position.side)
        if signal.type == SignalType.EXIT_LONG and position_side == PositionSide.LONG.value:
            return position
        if signal.type == SignalType.EXIT_SHORT and position_side == PositionSide.SHORT.value:
            return position
        return None

    def _create_conditional_orders(
        self,
        signal: Signal,
        entry_order,
        qty: Decimal,
        candle: Optional[Candlestick],
    ) -> list:
        """Create SL/TP/Trailing orders linked via OCO before external placement."""
        # Closing side is opposite of entry
        close_side = OrderSide.SELL if entry_order.side.lower() == "buy" else OrderSide.BUY

        sl_order = None
        tp_order = None
        conditional_orders = []

        # Create SL order
        if signal.stop_loss:
            sl_order = self.order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type="stop_loss",
                quantity=qty,
                trigger_price=signal.stop_loss,
            )
            self._attach_min_notional_reference_price(sl_order, candle)
            conditional_orders.append(sl_order)

        # Create TP order
        if signal.take_profit:
            tp_order = self.order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type="take_profit",
                quantity=qty,
                trigger_price=signal.take_profit,
            )
            self._attach_min_notional_reference_price(tp_order, candle)
            conditional_orders.append(tp_order)

        # Link OCO: SL and TP cancel each other
        if sl_order and tp_order:
            sl_order._linked_order_id = tp_order.id
            tp_order._linked_order_id = sl_order.id

        # Create Trailing Stop order
        if signal.trailing_distance:
            ts_order = self.order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type="trailing_stop",
                quantity=qty,
                trigger_price=signal.stop_loss,
            )
            ts_order._trailing_distance = signal.trailing_distance
            self._attach_min_notional_reference_price(ts_order, candle)
            conditional_orders.append(ts_order)

        for conditional_order in conditional_orders:
            linked_order_id = getattr(conditional_order, "_linked_order_id", None)
            conditional_order.status = OrderStatus.NEW.value
            conditional_order.exchange_order_id = None
            conditional_order.intent_payload = {
                "pending_entry_order_id": str(entry_order.id),
                "linked_order_id": str(linked_order_id) if linked_order_id else None,
                "placement_mode": "place-after-fill",
            }
            self.order_manager.repo.update_order(conditional_order)

        return conditional_orders

    def _place_pending_conditional_orders_for_entry(self, entry_order) -> list[dict]:
        if entry_order.type not in {"market", "limit"}:
            return []
        protected_quantity = entry_order.filled_quantity or Decimal("0")
        if protected_quantity <= 0:
            return []
        related_orders = [
            order
            for order in self.order_manager.repo.list_orders_by_statuses(
                {
                    OrderStatus.NEW.value,
                    OrderStatus.SUBMITTED.value,
                    OrderStatus.PARTIALLY_FILLED.value,
                }
            )
            if isinstance(order.intent_payload, dict)
            and order.intent_payload.get("pending_entry_order_id") == str(entry_order.id)
        ]
        pending = [
            order for order in related_orders if order.status == OrderStatus.NEW.value
        ]
        if not pending:
            underprotected = [
                order
                for order in related_orders
                if (order.quantity or Decimal("0")) < protected_quantity
            ]
            if underprotected:
                return [
                    {
                        "order_id": str(order.id),
                        "order_type": order.type,
                        "reason": "conditional_order_resize_required_after_entry_fill",
                        "current_quantity": str(order.quantity),
                        "required_quantity": str(protected_quantity),
                    }
                    for order in underprotected
                ]
            return []
        for order in pending:
            order.quantity = protected_quantity
            self.order_manager.repo.update_order(order)
        return self._place_conditional_orders(pending)

    @staticmethod
    def _protective_partial_fill_requires_resize(order, event_state: str) -> dict | None:
        if order.type not in {"stop_loss", "take_profit", "trailing_stop"}:
            return None
        if event_state in {"filled", "liquidated"}:
            return None
        if not isinstance(order.intent_payload, dict):
            return None
        linked_order_id = order.intent_payload.get("linked_order_id")
        if not linked_order_id:
            return None
        return {
            "order_id": str(order.id),
            "order_type": order.type,
            "linked_order_id": str(linked_order_id),
            "reason": "protective_partial_fill_requires_resize",
        }

    def _cancel_linked_conditional_order_for_protection_fill(self, order) -> dict | None:
        if order.type not in {"stop_loss", "take_profit", "trailing_stop"}:
            return None
        if not isinstance(order.intent_payload, dict):
            return None
        linked_order_id = order.intent_payload.get("linked_order_id")
        if not linked_order_id:
            return None
        linked_order = self.order_manager.repo.get_order(str(linked_order_id))
        if linked_order is None:
            return None
        if linked_order.status in {
            OrderStatus.CANCELLED.value,
            OrderStatus.FILLED.value,
            OrderStatus.FAILED.value,
            OrderStatus.LIQUIDATED.value,
        }:
            return None
        if self.cancel_order(str(linked_order.id)):
            return None
        return {
            "order_id": str(linked_order.id),
            "order_type": linked_order.type,
            "exchange_order_id": linked_order.exchange_order_id,
            "reason": "cancel_order_returned_false",
        }

    def _validate_order_group(self, orders: list) -> None:
        validate_order = getattr(self.adapter, "validate_order", None)
        if validate_order is None:
            return
        for order in orders:
            validate_order(order)
            self.order_manager.repo.update_order(order)

    def _record_order_rejection(
        self,
        *,
        order,
        order_type: str,
        error: ExchangeError,
        phase: str,
        write_event: bool = True,
    ) -> str:
        reason = self._order_rejection_reason(error)
        ORDERS_TOTAL.labels(
            order_type=order_type,
            status="failed",
            reason=reason,
        ).inc()
        if write_event:
            self._try_write_order_rejection_event(
                order=order,
                order_type=order_type,
                reason=reason,
                error=error,
                phase=phase,
            )
        return reason

    @staticmethod
    def _order_rejection_reason(error: ExchangeError) -> str:
        message = str(error)
        token = message.split(":", 1)[0].strip()
        normalized = "".join(
            char if char.isalnum() else "_"
            for char in token.lower()
        ).strip("_")
        return normalized or "exchange_error"

    def _try_write_order_rejection_event(
        self,
        *,
        order,
        order_type: str,
        reason: str,
        error: ExchangeError,
        phase: str,
    ) -> None:
        if self._db_session_factory is None:
            return
        try:
            with self._db_session_factory() as db:
                self._write_order_rejection_event(
                    db,
                    order=order,
                    order_type=order_type,
                    reason=reason,
                    error=error,
                    phase=phase,
                )
                db.commit()
        except Exception:
            self.logger.exception("Failed to write order rejection system event")

    def _write_order_rejection_event(
        self,
        db: Session,
        *,
        order,
        order_type: str,
        reason: str,
        error: ExchangeError,
        phase: str,
    ) -> None:
        write_system_event(
            db,
            event_type="system_error",
            event_subtype="order_rejected",
            related_strategy_id=order.strategy_id,
            related_order_id=str(order.id),
            payload={
                "order_id": str(order.id),
                "product_id": order.product_id,
                "order_type": order_type,
                "phase": phase,
                "reason": reason,
                "error": str(error),
            },
        )

    def _place_conditional_orders(self, conditional_orders: list) -> list[dict]:
        """Submit prevalidated SL/TP/Trailing orders linked via OCO to each other."""
        failures = []
        for order in conditional_orders:
            try:
                ex_id = self.adapter.place_order(order)
                self.order_manager.mark_submitted(order, ex_id)
            except ExchangeError as e:
                label = {
                    "stop_loss": "SL",
                    "take_profit": "TP",
                    "trailing_stop": "trailing stop",
                }.get(order.type, order.type)
                self.logger.error("Failed to place %s order: %s", label, e)
                failures.append(
                    {
                        "order_id": str(order.id),
                        "order_type": order.type,
                        "reason": str(e),
                    }
                )
        return failures

    def _try_write_conditional_order_event_warning(
        self,
        *,
        event_subtype: str,
        order,
        failures: list[dict],
    ) -> None:
        if self._db_session_factory is None:
            return
        try:
            with self._db_session_factory() as db:
                write_system_event(
                    db,
                    event_type="system_error",
                    event_subtype=event_subtype,
                    related_strategy_id=order.strategy_id,
                    related_order_id=str(order.id),
                    payload={
                        "order_id": str(order.id),
                        "product_id": order.product_id,
                        "failures": failures,
                    },
                )
                db.commit()
        except Exception:
            self.logger.exception("Failed to write conditional order warning event")

    def _journal_fill(self, order, price, qty, fee, fill_type: str, candle: Optional[Candlestick] = None) -> None:
        """Record a fill event to the journal."""
        tag_map = {
            "STOP_LOSS": "sl_hit",
            "TAKE_PROFIT": "tp_hit",
            "TRAILING_STOP": "trailing_hit",
            "MARKET": "fill",
            "LIMIT": "fill",
        }
        tag = tag_map.get(fill_type, "fill")
        ts = candle.timestamp if candle else 0
        self.journal.log(
            tag,
            {
                "order_id": str(order.id),
                "side": order.side,
                "price": str(price),
                "quantity": str(qty),
                "fee": str(fee) if fee else "0",
                "fill_type": fill_type,
            },
            timestamp=ts,
            trade_id=str(order.id),
        )

    def _journal_exchange_order_event_fill(
        self,
        order,
        event: ExchangeOrderEvent,
        fill_price: Decimal,
        fill_quantity: Decimal,
    ) -> None:
        self.journal.log(
            "fill",
            {
                "order_id": str(order.id),
                "side": order.side,
                "signal_price": self._intent_signal_price(order),
                "submitted_price": str(order.price) if order.price else "market",
                "fill_price": str(fill_price),
                "quantity": str(fill_quantity),
                "fee": str(event.fee) if event.fee is not None else "0",
                "fee_asset": event.fee_asset,
                "exchange_order_id": event.exchange_order_id,
                "client_order_id": event.client_order_id,
                "exchange_status": event.status,
            },
            timestamp=event.event_timestamp or int(self.clock.now() * 1000),
            trade_id=str(order.id),
        )

    @staticmethod
    def _intent_signal_price(order) -> str | None:
        intent_payload = getattr(order, "intent_payload", None)
        if not isinstance(intent_payload, dict):
            return None
        order_payload = intent_payload.get("order")
        if not isinstance(order_payload, dict):
            return None
        price = order_payload.get("price")
        return str(price) if price is not None else None

    def _determine_side(self, signal_type: SignalType) -> Optional[OrderSide]:
        if signal_type == SignalType.LONG:
            return OrderSide.BUY
        elif signal_type == SignalType.SHORT:
            return OrderSide.SELL
        elif signal_type == SignalType.EXIT_LONG:
            return OrderSide.SELL
        elif signal_type == SignalType.EXIT_SHORT:
            return OrderSide.BUY
        return None
