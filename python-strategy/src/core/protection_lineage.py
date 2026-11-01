"""Pure validation of persisted protective-order lineage."""

from __future__ import annotations

from typing import Any


def protection_lineage_is_valid(
    *,
    order: Any,
    entry: Any,
    sibling: Any | None,
    event_product_id: object,
) -> bool:
    """Validate persisted child/entry/sibling identity without coercion.

    Missing optional sibling links are allowed. A present link must be a
    non-empty string naming a reciprocal protective sibling. Both entry and
    child sides must be known, with each protective child on the closing side.
    """
    payload = getattr(order, "intent_payload", None)
    entry_payload = getattr(entry, "intent_payload", None)
    if not isinstance(payload, dict) or not (
        entry_payload is None or isinstance(entry_payload, dict)
    ):
        return False
    entry_id = payload.get("pending_entry_order_id")
    if type(entry_id) is not str or not entry_id:
        return False
    if str(getattr(entry, "id", "")) != entry_id:
        return False
    entry_side = _known_order_side(getattr(entry, "side", None))
    child_side = _known_order_side(getattr(order, "side", None))
    if entry_side is None or child_side is None or child_side == entry_side:
        return False
    if (
        getattr(order, "product_id", None) != getattr(entry, "product_id", None)
        or getattr(order, "product_id", None) != event_product_id
        or getattr(order, "exchange_id", None) != getattr(entry, "exchange_id", None)
        or getattr(order, "strategy_id", None) != getattr(entry, "strategy_id", None)
        or getattr(order, "account_profile", None)
        != getattr(entry, "account_profile", None)
        or getattr(order, "account_id", None) != getattr(entry, "account_id", None)
    ):
        return False
    entry_type = getattr(entry, "type", None)
    if type(entry_type) is not str or entry_type not in {"market", "limit"}:
        return False
    if _protection_leg(getattr(order, "type", None)) is None:
        return False

    sibling_link_present = "linked_order_id" in payload
    sibling_id = payload.get("linked_order_id")
    if not sibling_link_present or sibling_id is None:
        return sibling is None
    if type(sibling_id) is not str or not sibling_id:
        return False
    if sibling is None or str(getattr(sibling, "id", "")) != sibling_id:
        return False
    sibling_payload = getattr(sibling, "intent_payload", None)
    sibling_side = _known_order_side(getattr(sibling, "side", None))
    child_leg = _protection_leg(getattr(order, "type", None))
    sibling_leg = _protection_leg(getattr(sibling, "type", None))
    return bool(
        sibling_side is not None
        and sibling_side == child_side
        and child_leg is not None
        and sibling_leg is not None
        and child_leg != sibling_leg
        and getattr(sibling, "id", None) != getattr(order, "id", None)
        and getattr(sibling, "product_id", None) == getattr(order, "product_id", None)
        and getattr(sibling, "exchange_id", None) == getattr(order, "exchange_id", None)
        and getattr(sibling, "strategy_id", None) == getattr(order, "strategy_id", None)
        and getattr(sibling, "account_profile", None)
        == getattr(order, "account_profile", None)
        and getattr(sibling, "account_id", None) == getattr(order, "account_id", None)
        and isinstance(sibling_payload, dict)
        and sibling_payload.get("pending_entry_order_id") == entry_id
        and sibling_payload.get("linked_order_id") == str(getattr(order, "id", ""))
    )


def _known_order_side(value: object) -> str | None:
    raw = getattr(value, "value", value)
    if type(raw) is not str:
        return None
    normalized = raw.lower()
    return normalized if normalized in {"buy", "sell"} else None


def _protection_leg(value: object) -> str | None:
    if type(value) is not str:
        return None
    if value in {"stop_loss", "trailing_stop"}:
        return "SL"
    if value == "take_profit":
        return "TP"
    return None
