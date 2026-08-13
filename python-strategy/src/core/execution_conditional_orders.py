"""Own the venue-neutral conditional-order lifecycle."""

from collections.abc import Callable
from decimal import Decimal
from logging import Logger
from typing import Protocol, cast

from src.core import execution_failure_diagnostics
from src.core.client_order_id import linked_client_order_id
from src.core.conditional_order_intents import (
    conditional_oco_pairs,
    conditional_order_intents,
)
from src.core.execution_ambiguous_submit_adoption import (
    adopt_order_after_ambiguous_submit_error,
    adopt_pending_conditional_order_before_submit,
)
from src.core.execution_submission_gate import ExecutionSubmissionGate
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    IExchangeAdapter,
)
from src.core.metrics import ORDERS_TOTAL
from src.core.models import Candlestick, OrderSide, OrderStatus, Signal
from src.core.order_manager import OrderManager
from src.core.orm_models import Order
from src.core.runtime_capabilities import PendingProtectionFillProcessor


class ConditionalOrder(Protocol):
    id: object
    side: str
    type: str
    product_id: str
    status: str
    quantity: Decimal | None
    filled_quantity: Decimal | None
    client_order_id: str | None
    exchange_order_id: str | None
    intent_payload: dict[str, object] | None


class RecordOrderAck(Protocol):
    def __call__(
        self,
        order: object,
        exchange_order_id: str,
        *,
        order_id: str | None = None,
    ) -> None: ...


class WriteConditionalWarning(Protocol):
    def __call__(
        self,
        *,
        event_subtype: str,
        order: object,
        failures: list[dict[str, object]],
    ) -> None: ...


def _conditional_client_order_id(
    entry_client_order_id: str | None,
    suffix: str,
) -> str | None:
    if not entry_client_order_id:
        return None
    return linked_client_order_id(entry_client_order_id, suffix)


def create_conditional_orders(
    *,
    order_manager: OrderManager,
    signal: Signal,
    entry_order: object,
    quantity: Decimal,
    candle: Candlestick | None,
    attach_min_notional_reference_price: Callable[[object, Candlestick | None], None],
) -> list[ConditionalOrder]:
    entry = cast(ConditionalOrder, entry_order)
    close_side = OrderSide.SELL if entry.side.lower() == "buy" else OrderSide.BUY
    orders: list[ConditionalOrder] = []
    intents = conditional_order_intents(signal)

    for intent in intents:
        order = cast(
            ConditionalOrder,
            order_manager.create_order(
                signal=signal,
                side=close_side,
                order_type=intent.order_type,
                quantity=quantity,
                trigger_price=intent.trigger_price,
                client_order_id=_conditional_client_order_id(
                    entry.client_order_id,
                    intent.client_order_suffix,
                ),
            ),
        )
        if intent.trailing_distance is not None:
            setattr(order, "_trailing_distance", intent.trailing_distance)
        attach_min_notional_reference_price(order, candle)
        orders.append(order)

    for first_index, second_index in conditional_oco_pairs(intents):
        first = orders[first_index]
        second = orders[second_index]
        setattr(first, "_linked_order_id", second.id)
        setattr(second, "_linked_order_id", first.id)

    for order in orders:
        linked_order_id = getattr(order, "_linked_order_id", None)
        order.status = OrderStatus.NEW.value
        order.exchange_order_id = None
        order.intent_payload = {
            "pending_entry_order_id": str(entry.id),
            "linked_order_id": str(linked_order_id) if linked_order_id else None,
            "placement_mode": "place-after-fill",
        }
        order_manager.repo.update_order(cast(Order, order))

    return orders


class ConditionalOrderLifecycleOwner:
    """Create, place, classify, and recover protective conditional orders."""

    def __init__(
        self,
        *,
        order_manager: OrderManager,
        adapter: IExchangeAdapter,
        submission_gate: ExecutionSubmissionGate,
        pending_protection_fill_processor: PendingProtectionFillProcessor,
        process_exchange_order_event: Callable[[ExchangeOrderEvent], dict[str, object]],
        assert_external_operation_allowed: Callable[[], None],
        record_order_ack: RecordOrderAck,
        write_warning: WriteConditionalWarning,
        logger: Logger,
    ) -> None:
        self._order_manager = order_manager
        self._adapter = adapter
        self._submission_gate = submission_gate
        self._pending_protection_fill_processor = pending_protection_fill_processor
        self._process_exchange_order_event = process_exchange_order_event
        self._assert_external_operation_allowed = assert_external_operation_allowed
        self._record_order_ack = record_order_ack
        self._write_warning = write_warning
        self._logger = logger

    def create_orders(
        self,
        *,
        signal: Signal,
        entry_order: object,
        quantity: Decimal,
        candle: Candlestick | None,
        attach_min_notional_reference_price: Callable[
            [object, Candlestick | None], None
        ],
    ) -> list[ConditionalOrder]:
        return create_conditional_orders(
            order_manager=self._order_manager,
            signal=signal,
            entry_order=entry_order,
            quantity=quantity,
            candle=candle,
            attach_min_notional_reference_price=(attach_min_notional_reference_price),
        )

    def place_pending_for_entry(self, entry_order: object) -> list[dict[str, object]]:
        entry = cast(ConditionalOrder, entry_order)
        admission_rejection = self._submission_gate.try_begin_submission()
        if admission_rejection is not None:
            self._logger.warning(
                "Conditional order placement rejected for entry %s: submission gate halted (%s)",
                getattr(entry_order, "id", "?"),
                admission_rejection,
            )
            return [
                {
                    "order_id": str(getattr(entry_order, "id", "?")),
                    "order_type": getattr(entry_order, "type", "?"),
                    "reason": admission_rejection,
                }
            ]
        try:
            return self._place_pending_for_entry(entry)
        finally:
            self._submission_gate.finish_submission()

    def _place_pending_for_entry(
        self,
        entry_order: ConditionalOrder,
    ) -> list[dict[str, object]]:
        if entry_order.type not in {"market", "limit"}:
            return []
        protected_quantity = entry_order.filled_quantity or Decimal("0")
        if protected_quantity <= 0:
            return []
        related_orders = [
            order
            for order in cast(
                list[ConditionalOrder],
                self._order_manager.repo.list_orders_by_statuses(
                    {
                        OrderStatus.NEW.value,
                        OrderStatus.SUBMITTED_UNCONFIRMED.value,
                        OrderStatus.SUBMITTED.value,
                        OrderStatus.PARTIALLY_FILLED.value,
                    }
                ),
            )
            if isinstance(order.intent_payload, dict)
            and order.intent_payload.get("pending_entry_order_id")
            == str(entry_order.id)
        ]
        provider_result = self._pending_protection_fill_processor(
            self._order_manager.repo,
            entry_order,
            related_orders,
        )
        if provider_result is not None:
            return provider_result
        pending = [
            order for order in related_orders if order.status == OrderStatus.NEW.value
        ]
        failures = self._underprotected_failures(
            related_orders,
            protected_quantity,
            pending_statuses={OrderStatus.NEW.value},
        )
        if not pending:
            return failures
        for order in pending:
            order.quantity = protected_quantity
            self._order_manager.repo.update_order(cast(Order, order))
        placement_candidates = []
        for order in pending:
            lookup_failure = adopt_pending_conditional_order_before_submit(
                adapter=self._adapter,
                process_exchange_order_event=self._process_exchange_order_event,
                order=order,
            )
            if lookup_failure is None:
                if order.status == OrderStatus.NEW.value:
                    placement_candidates.append(order)
                continue
            failures.append(lookup_failure)
        if placement_candidates:
            failures.extend(self.place_orders(placement_candidates))
        return failures

    @staticmethod
    def _underprotected_failures(
        related_orders: list[ConditionalOrder],
        protected_quantity: Decimal,
        *,
        pending_statuses: set[str],
    ) -> list[dict[str, object]]:
        return [
            {
                "order_id": str(order.id),
                "order_type": order.type,
                "reason": "conditional_order_resize_required_after_entry_fill",
                "current_quantity": str(order.quantity),
                "required_quantity": str(protected_quantity),
            }
            for order in related_orders
            if order.status not in pending_statuses
            and (order.quantity or Decimal("0")) < protected_quantity
        ]

    def recover_pending_protection(self) -> dict[str, object]:
        pending = [
            order
            for order in cast(
                list[ConditionalOrder],
                self._order_manager.repo.list_orders_by_statuses(
                    {OrderStatus.NEW.value}
                ),
            )
            if isinstance(order.intent_payload, dict)
            and order.intent_payload.get("pending_entry_order_id")
        ]
        entry_ids = {
            str(order.intent_payload["pending_entry_order_id"])
            for order in pending
            if isinstance(order.intent_payload, dict)
        }
        attempted = 0
        failures: list[dict[str, object]] = []
        for entry_id in sorted(entry_ids):
            entry = cast(
                ConditionalOrder | None,
                self._order_manager.repo.get_order(entry_id),
            )
            if entry is None or (entry.filled_quantity or Decimal("0")) <= 0:
                continue
            attempted += 1
            entry_failures = self.place_pending_for_entry(entry)
            if entry_failures:
                failures.extend(entry_failures)
                self.write_warning(
                    event_subtype=(
                        "conditional_order_placement_failed_after_entry_fill"
                    ),
                    order=entry,
                    failures=entry_failures,
                )
        if failures:
            self._logger.error(
                "Pending protection recovery has %s placement failure(s)",
                len(failures),
            )
        return {
            "pending_count": len(pending),
            "entries_attempted": attempted,
            "failures": failures,
        }

    def place_orders(
        self,
        conditional_orders: list[ConditionalOrder],
    ) -> list[dict[str, object]]:
        failures: list[dict[str, object]] = []
        for order in conditional_orders:
            order_id = str(order.id)
            submit_attempted = False
            try:
                if order.client_order_id:
                    self._order_manager.mark_submitted_unconfirmed(cast(Order, order))
                self._assert_external_operation_allowed()
                submit_attempted = True
                exchange_order_id = self._adapter.place_order(cast(Order, order))
                self._record_order_ack(
                    order,
                    exchange_order_id,
                    order_id=order_id,
                )
                ORDERS_TOTAL.labels(
                    order_type=order.type,
                    status="placed",
                    reason="none",
                ).inc()
            except ExchangeError as error:
                label = {
                    "stop_loss": "SL",
                    "take_profit": "TP",
                    "trailing_stop": "trailing stop",
                }.get(order.type, order.type)
                self._logger.error("Failed to place %s order: %s", label, error)
                failures.extend(
                    self._handle_placement_error(
                        order,
                        error,
                        submit_attempted=submit_attempted,
                    )
                )
        return failures

    def _handle_placement_error(
        self,
        order: ConditionalOrder,
        error: ExchangeError,
        *,
        submit_attempted: bool,
    ) -> list[dict[str, object]]:
        adoption = adopt_order_after_ambiguous_submit_error(
            adapter=self._adapter,
            process_exchange_order_event=self._process_exchange_order_event,
            order=order,
            error=error,
            submit_attempted=submit_attempted,
        )
        if (
            submit_attempted
            and adoption["action"] == "verification_blocked_missing_client_order_id"
        ):
            self._order_manager.mark_submitted_unconfirmed(cast(Order, order))
            self._record_failed_metric(
                order.type,
                "verification_blocked_missing_client_order_id",
            )
            return [
                {
                    "order_id": str(order.id),
                    "order_type": order.type,
                    "reason": "verification_blocked_missing_client_order_id",
                    "adoption": adoption,
                    "operator_action": (
                        "conditional_submit_outcome_uncertain_without_client_id; "
                        "verify exchange manually"
                    ),
                }
            ]
        if adoption["action"] == "adopted":
            ORDERS_TOTAL.labels(
                order_type=order.type,
                status="placed",
                reason="adopted_after_submit_error",
            ).inc()
            return []
        if adoption.get("terminal"):
            self._record_failed_metric(order.type, "terminal_after_submit_error")
            return [
                {
                    "order_id": str(order.id),
                    "order_type": order.type,
                    "reason": "terminal_after_submit_error",
                    "adoption": adoption,
                }
            ]
        if adoption.get("verification_blocked") or adoption.get("unresolved"):
            reason = str(adoption["action"])
            self._record_failed_metric(order.type, reason)
            return [
                {
                    "order_id": str(order.id),
                    "order_type": order.type,
                    "reason": reason,
                    "adoption": adoption,
                }
            ]
        self._order_manager.fail_order(cast(Order, order), str(error))
        reason = execution_failure_diagnostics.order_rejection_reason(error)
        self._record_failed_metric(order.type, reason)
        return [
            {
                "order_id": str(order.id),
                "order_type": order.type,
                "reason": str(error),
                "adoption": adoption,
            }
        ]

    @staticmethod
    def _record_failed_metric(order_type: str, reason: str) -> None:
        ORDERS_TOTAL.labels(
            order_type=order_type,
            status="failed",
            reason=reason,
        ).inc()

    def write_warning(
        self,
        *,
        event_subtype: str,
        order: object,
        failures: list[dict[str, object]],
    ) -> None:
        self._write_warning(
            event_subtype=event_subtype,
            order=order,
            failures=failures,
        )
