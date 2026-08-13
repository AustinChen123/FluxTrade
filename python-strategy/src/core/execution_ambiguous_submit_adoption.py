"""Classify and adopt orders after an ambiguous submit failure."""

from collections.abc import Callable
from typing import Protocol

from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderLookupUnsupported,
    IExchangeAdapter,
    NetworkError,
)
from src.core.order_event_sync import exchange_snapshot_to_order_event
from src.core.order_reconciliation import OrderReconciler


class SubmittedOrder(Protocol):
    client_order_id: str | None
    product_id: str
    type: str


def adopt_order_after_ambiguous_submit_error(
    *,
    adapter: IExchangeAdapter,
    process_exchange_order_event: Callable[[ExchangeOrderEvent], dict[str, object]],
    order: SubmittedOrder,
    error: ExchangeError,
    submit_attempted: bool,
) -> dict[str, object]:
    if not submit_attempted:
        return {
            "action": "submit_not_attempted",
            "verification_blocked": False,
        }
    if not isinstance(error, NetworkError):
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
        snapshot = adapter.get_order_by_client_id(
            order.client_order_id,
            order.product_id,
            order_type=order.type,
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

    event_result = process_exchange_order_event(
        exchange_snapshot_to_order_event(order.product_id, snapshot)
    )
    if event_result["action"] != "applied":
        action = str(event_result["action"])
        return {
            "action": event_result["action"],
            "event_result": event_result,
            "verification_blocked": (
                OrderReconciler._resync_action_verification_blocked(action)
            ),
            "unresolved": action.startswith("unresolved_"),
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
            "exchange_order_id": (
                event_result.get("exchange_order_id") or snapshot.exchange_order_id
            ),
            "verification_blocked": False,
            "terminal": True,
        }
    exchange_order_id = (
        event_result.get("exchange_order_id") or snapshot.exchange_order_id
    )
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
