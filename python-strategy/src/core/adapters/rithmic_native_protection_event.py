"""Rithmic native-protection order-event identity and confirmation policy."""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import Protocol, cast

from src.core.interfaces import IOrderRepository
from src.core.interfaces.exchange import ExchangeOrderEvent


class _NativeProtectionOrder(Protocol):
    id: str
    type: str
    exchange_order_id: str | None
    trigger_price: Decimal | None
    intent_payload: dict[str, object] | None


def process_native_protection_event(
    repository: IOrderRepository,
    event: ExchangeOrderEvent,
    apply_event: Callable[[], dict[str, object]],
) -> dict[str, object]:
    """Apply one event while preserving Rithmic native-protection identity."""
    identity_failure = _identity_failure(repository, event)
    if identity_failure is not None:
        return identity_failure
    return _verify_confirmation(repository, event, apply_event())


def _identity_failure(
    repository: IOrderRepository,
    event: ExchangeOrderEvent,
) -> dict[str, object] | None:
    if not event.client_order_id:
        return None
    stored_order = repository.get_order_by_client_order_id(event.client_order_id)
    order = cast(_NativeProtectionOrder | None, stored_order)
    payload = dict(getattr(order, "intent_payload", None) or {})
    if (
        order is None
        or order.type not in {"stop_loss", "take_profit"}
        or payload.get("placement_mode") != "attach-at-entry"
    ):
        return None
    raw = event.raw or {}
    expected_parent = payload.get("native_parent_basket_id")
    remote_parent = raw.get("original_basket_id")
    if (
        expected_parent
        and remote_parent is not None
        and str(remote_parent) != str(expected_parent)
    ):
        return {
            "action": "unresolved_native_protection_parent_mismatch",
            "order_id": str(order.id),
            "status": event.status,
        }
    if (
        order.exchange_order_id
        and event.exchange_order_id
        and str(order.exchange_order_id) != str(event.exchange_order_id)
    ):
        return {
            "action": "unresolved_native_protection_basket_mismatch",
            "order_id": str(order.id),
            "status": event.status,
        }
    return None


def _verify_confirmation(
    repository: IOrderRepository,
    event: ExchangeOrderEvent,
    result: dict[str, object],
) -> dict[str, object]:
    if result.get("action") != "applied" or result.get("state") != "open":
        return result
    order_id = result.get("order_id")
    stored_order = repository.get_order(str(order_id)) if order_id else None
    order = cast(_NativeProtectionOrder | None, stored_order)
    payload = dict(getattr(order, "intent_payload", None) or {})
    raw = event.raw or {}
    if (
        order is None
        or order.type not in {"stop_loss", "take_profit"}
        or payload.get("placement_mode") != "attach-at-entry"
        or not raw.get("original_basket_id")
    ):
        return result

    expected_price_type = "stop_market" if order.type == "stop_loss" else "limit"
    if str(raw.get("price_type") or "").lower() != expected_price_type:
        return {
            **result,
            "action": "unresolved_native_protection_price_type_mismatch",
        }
    expected_bracket_type = payload.get("native_bracket_type")
    remote_bracket_type = raw.get("bracket_type")
    if remote_bracket_type and remote_bracket_type != expected_bracket_type:
        return {
            **result,
            "action": "unresolved_native_protection_bracket_type_mismatch",
        }
    raw_price = (
        raw.get("trigger_price") if order.type == "stop_loss" else raw.get("price")
    )
    try:
        remote_price = Decimal(str(raw_price))
    except (InvalidOperation, TypeError, ValueError):
        remote_price = Decimal("NaN")
    if not remote_price.is_finite() or remote_price <= 0:
        return {
            **result,
            "action": "unresolved_native_protection_price_missing",
        }

    payload.update(
        {
            "remote_effective_price": str(remote_price),
            "remote_price_type": expected_price_type,
            "remote_bracket_type": remote_bracket_type,
        }
    )
    expected_raw = payload.get("expected_effective_price")
    if expected_raw is None:
        payload["protection_confirmation"] = "observed_pending_entry_fill"
    else:
        try:
            expected_price = Decimal(str(expected_raw))
        except (InvalidOperation, TypeError, ValueError):
            expected_price = Decimal("NaN")
        if not expected_price.is_finite() or expected_price != remote_price:
            payload["protection_confirmation"] = "conflict"
            order.intent_payload = payload
            repository.update_order(stored_order)
            return {
                **result,
                "action": "unresolved_native_protection_price_mismatch",
                "expected_price": str(expected_raw),
                "remote_price": str(remote_price),
            }
        payload.update(
            {
                "effective_price": str(remote_price),
                "protection_confirmation": "confirmed",
            }
        )
        order.trigger_price = remote_price
    order.intent_payload = payload
    repository.update_order(stored_order)
    return result
