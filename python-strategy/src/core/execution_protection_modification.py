"""Attached protection modification transaction."""

from collections.abc import Callable
from decimal import Decimal
from typing import ContextManager, Iterable, Protocol, cast

from src.core.clock import Clock
from src.core.interfaces.exchange import ExchangeError, NetworkError
from src.core.models import OrderStatus


class ProtectionOrder(Protocol):
    id: object
    type: str
    trigger_price: Decimal | None
    intent_payload: dict[str, object] | None


class ProtectionModificationRepository(Protocol):
    def get_order(self, order_id: str) -> ProtectionOrder | None: ...

    def list_orders_by_statuses(self, statuses: set[str]) -> list[ProtectionOrder]: ...

    def update_order(self, order: ProtectionOrder) -> None: ...


class ProtectionModificationAdapter(Protocol):
    def modify_protection(
        self,
        order: ProtectionOrder,
        *,
        trigger_price: Decimal,
    ) -> bool: ...


def modify_attached_protection(
    *,
    repository: ProtectionModificationRepository,
    adapter: ProtectionModificationAdapter,
    clock: Clock,
    order_event_lock: ContextManager[bool | None],
    assert_operation_allowed: Callable[[], None],
    halt_for_reconcile: Callable[[], bool],
    entry_order_id: str,
    leg_type: str,
    price: Decimal,
) -> dict[str, str]:
    entry = repository.get_order(str(entry_order_id))
    if entry is None or entry.type not in {"market", "limit"}:
        raise ExchangeError("modify_protection_entry_not_found")
    candidates = [
        order
        for order in repository.list_orders_by_statuses(
            {
                OrderStatus.SUBMITTED_UNCONFIRMED.value,
                OrderStatus.SUBMITTED.value,
                OrderStatus.PARTIALLY_FILLED.value,
            }
        )
        if order.type == leg_type
        and isinstance(order.intent_payload, dict)
        and order.intent_payload.get("pending_entry_order_id") == str(entry.id)
        and order.intent_payload.get("placement_mode") == "attach-at-entry"
    ]
    if len(candidates) != 1:
        raise ExchangeError("modify_protection_leg_identity_ambiguous")
    order = candidates[0]
    with order_event_lock:
        payload = dict(order.intent_payload or {})
        previous_trigger_price = order.trigger_price
        modifications = list(cast(Iterable[object], payload.get("modifications") or []))
        attempt: dict[str, object] = {
            "previous_effective_price": payload.get("effective_price"),
            "requested_price": str(price),
            "started_at_ms": int(clock.now() * 1000),
            "status": "pending",
        }
        modifications.append(attempt)
        payload["modifications"] = modifications
        order.intent_payload = payload
        repository.update_order(order)
        try:
            assert_operation_allowed()
            confirmed = adapter.modify_protection(
                order,
                trigger_price=price,
            )
        except NetworkError:
            halt_for_reconcile()
            attempt["status"] = "ambiguous"
            attempt["finished_at_ms"] = int(clock.now() * 1000)
            order.intent_payload = payload
            repository.update_order(order)
            raise
        except ExchangeError:
            attempt["status"] = "rejected"
            attempt["finished_at_ms"] = int(clock.now() * 1000)
            order.intent_payload = payload
            repository.update_order(order)
            raise
        if not confirmed:
            attempt["status"] = "rejected"
            attempt["finished_at_ms"] = int(clock.now() * 1000)
            order.intent_payload = payload
            repository.update_order(order)
            raise ExchangeError("modify_protection_not_confirmed")
        confirmed_attempt = {
            **attempt,
            "status": "confirmed",
            "finished_at_ms": int(clock.now() * 1000),
        }
        confirmed_payload: dict[str, object] = {
            **payload,
            "requested_price": str(price),
            "expected_effective_price": str(price),
            "effective_price": str(price),
            "price_drift": "0",
            "modification_mode": "absolute",
            "protection_confirmation": "confirmed",
            "modifications": [*modifications[:-1], confirmed_attempt],
        }
        order.trigger_price = price
        order.intent_payload = confirmed_payload
        try:
            repository.update_order(order)
        except Exception:
            order.trigger_price = previous_trigger_price
            order.intent_payload = payload
            halt_for_reconcile()
            raise
    return {
        "entry_order_id": str(entry.id),
        "order_id": str(order.id),
        "leg_type": leg_type,
        "effective_price": str(price),
    }
