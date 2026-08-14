from decimal import Decimal
from typing import Callable, Optional

from src.core.fill_delta import FillDelta, fill_delta_from_cumulative
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.models import OrderStatus


class OrderEventApplier:
    def __init__(
        self,
        *,
        order_manager,
        journal_fill: Optional[
            Callable[[object, ExchangeOrderEvent, Decimal, Decimal], None]
        ],
        fail_pending_conditionals_for_terminal_entry: Callable[[object], None],
        protective_terminal_without_fill_failure: Callable[[object], dict | None],
        write_conditional_warning: Callable[..., None],
        place_pending_conditionals_for_entry: Callable[[object], list[dict]],
        protective_partial_fill_requires_resize: Callable[[object, str], dict | None],
        cancel_linked_conditional_for_protection_fill: Callable[[object], dict | None],
        remote_follow_up_required: Callable[[object, str], bool] | None = None,
    ) -> None:
        self.order_manager = order_manager
        self.journal_fill = journal_fill
        self.fail_pending_conditionals_for_terminal_entry = (
            fail_pending_conditionals_for_terminal_entry
        )
        self.protective_terminal_without_fill_failure = (
            protective_terminal_without_fill_failure
        )
        self.write_conditional_warning = write_conditional_warning
        self.place_pending_conditionals_for_entry = place_pending_conditionals_for_entry
        self.protective_partial_fill_requires_resize = (
            protective_partial_fill_requires_resize
        )
        self.cancel_linked_conditional_for_protection_fill = (
            cancel_linked_conditional_for_protection_fill
        )
        self.remote_follow_up_required = remote_follow_up_required or (
            lambda _order, _event_state: True
        )

    def process_exchange_order_event(
        self,
        event: ExchangeOrderEvent,
        *,
        allow_remote_side_effects: bool = True,
    ) -> dict[str, object]:
        """Apply a live exchange order event to local order, trade, and position state."""
        order = self._resolve_order_event_order(event)
        if order is None:
            return {
                "action": "unknown_order",
                "status": event.status,
                "client_order_id": event.client_order_id,
                "exchange_order_id": event.exchange_order_id,
            }

        if (
            event.exchange_order_id
            and order.exchange_order_id != event.exchange_order_id
        ):
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
        if fill_delta["quantity"] == 0 and self._requires_terminal_fill_quantity(
            order, event, event_state
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

        terminal_failure_state = event_state in {
            "cancelled",
            "rejected",
            "expired",
            "failed",
        }
        if terminal_failure_state and fill_delta["quantity"] == 0:
            # Keep the parent recoverable until zero-fill child cleanup is
            # durable. If the process stops between these operations, startup
            # reconciliation will still query the parent authoritatively.
            self.fail_pending_conditionals_for_terminal_entry(order)

        if fill_delta["quantity"] > 0 and fill_delta["price"] is not None:
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
            if self.journal_fill is not None:
                self.journal_fill(
                    order,
                    event,
                    fill_delta["price"],
                    fill_delta["quantity"],
                )
        else:
            self._apply_exchange_order_event_status(order, event_state, event)

        if terminal_failure_state:
            if fill_delta["quantity"] > 0:
                self.fail_pending_conditionals_for_terminal_entry(order)
            protective_failure = self.protective_terminal_without_fill_failure(order)
            if protective_failure is not None:
                self.write_conditional_warning(
                    event_subtype="protective_order_terminal_without_fill",
                    order=order,
                    failures=[protective_failure],
                )
                return {
                    "action": "unresolved_protective_terminal_without_fill",
                    "order_id": order.id,
                    "status": event.status,
                    "state": event_state,
                    "fill_quantity": fill_delta["quantity"],
                    "exchange_order_id": order.exchange_order_id,
                    "failure": protective_failure,
                }

        # Post-fill actions key off persisted state, not the event delta:
        # a replayed event (crash recovery, resync, duplicate stream message)
        # carries the same cumulative quantity (delta == 0) and must still
        # retry pending protection placement and sibling cancellation.
        if (order.filled_quantity or Decimal("0")) > 0:
            if not allow_remote_side_effects:
                if self.remote_follow_up_required(order, event_state):
                    return {
                        "action": "unresolved_remote_actions_suppressed",
                        "order_id": order.id,
                        "status": event.status,
                        "state": event_state,
                        "fill_quantity": fill_delta["quantity"],
                        "exchange_order_id": order.exchange_order_id,
                    }
                return {
                    "action": "applied",
                    "order_id": order.id,
                    "status": event.status,
                    "state": event_state,
                    "fill_quantity": fill_delta["quantity"],
                    "exchange_order_id": order.exchange_order_id,
                }
            placement_failures = self.place_pending_conditionals_for_entry(order)
            if placement_failures:
                self.write_conditional_warning(
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
            protective_partial_failure = self.protective_partial_fill_requires_resize(
                order,
                event_state,
            )
            if protective_partial_failure is not None:
                self.write_conditional_warning(
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
            cancel_failure = self.cancel_linked_conditional_for_protection_fill(order)
            if cancel_failure is not None:
                self.write_conditional_warning(
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
                if order.product_id != event.product_id:
                    return None
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
        if normalized == "modify_rejected":
            return "modify_rejected"
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

    @classmethod
    def _status_for_exchange_event_fill(cls, event_state: str) -> OrderStatus | None:
        if event_state in {"open", "partial"}:
            return OrderStatus.PARTIALLY_FILLED
        return cls._terminal_status_for_exchange_event(event_state)

    def _exchange_order_event_fill_delta(
        self,
        order,
        event: ExchangeOrderEvent,
    ) -> FillDelta:
        local_filled = order.filled_quantity or Decimal("0")
        cumulative = event.cumulative_filled_quantity
        if cumulative is None:
            return {"quantity": Decimal("0"), "price": None}

        delta = cumulative - local_filled
        if delta <= 0:
            return {"quantity": delta, "price": None}

        price = None
        if event.last_fill_price is not None and event.last_fill_quantity == delta:
            price = event.last_fill_price
        elif event.cumulative_average_price is not None:
            price = fill_delta_from_cumulative(
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
    def _requires_terminal_fill_quantity(
        order,
        event: ExchangeOrderEvent,
        event_state: str,
    ) -> bool:
        if event_state not in {"filled", "liquidated"}:
            return False
        has_fill_quantity = event.cumulative_filled_quantity is not None or (
            event.last_fill_quantity is not None and event.last_fill_quantity > 0
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
        fill_delta: FillDelta,
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
        elif event_state == "modify_rejected":
            # The existing protection remains active when a modify is explicitly
            # rejected, so the order's persisted state must not be downgraded.
            return

    @staticmethod
    def _has_exchange_order_event_fill_progress(
        order,
        event: ExchangeOrderEvent,
    ) -> bool:
        return (order.filled_quantity or Decimal("0")) > 0 or (
            event.cumulative_filled_quantity or Decimal("0")
        ) > 0


def exchange_snapshot_to_order_event(product_id: str, snapshot) -> ExchangeOrderEvent:
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
