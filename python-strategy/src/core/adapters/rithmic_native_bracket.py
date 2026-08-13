"""Pure Rithmic native-bracket policy and projection helpers."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Generic, TypeVar, TypedDict, cast

from src.core.client_order_id import linked_client_order_id
from src.core.interfaces import IOrderRepository
from src.core.interfaces.exchange import ExchangeError
from src.core.product_registry import InstrumentSpec

if TYPE_CHECKING:
    from src.core.orm_models import Order


NativeBracketGroups = dict[str, dict[str, str]]
OrderT = TypeVar("OrderT")


class NativeBracketPlan(TypedDict, Generic[OrderT]):
    entry: OrderT
    stop_ticks: int | None
    target_ticks: int | None
    leg_client_order_ids: dict[str, str]


@dataclass(frozen=True)
class NativeProtectionRequest:
    basket_id: str
    product_id: str
    quantity: str
    leg_type: str
    price: str


def audit_native_bracket_fill(
    repository: IOrderRepository,
    entry_order: object,
    related_orders: Sequence[object],
) -> list[dict[str, object]] | None:
    """Audit attach-at-entry protection after its parent entry fills."""
    native_orders = [
        order
        for order in related_orders
        if (getattr(order, "intent_payload", None) or {}).get("placement_mode")
        == "attach-at-entry"
    ]
    if not native_orders:
        return None
    if len(native_orders) != len(related_orders):
        return [
            {
                "order_id": str(getattr(entry_order, "id", "?")),
                "order_type": getattr(entry_order, "type", "?"),
                "reason": "mixed_native_and_deferred_protection",
            }
        ]

    fill_price = getattr(entry_order, "filled_price", None)
    if fill_price is None or fill_price <= 0:
        return [
            {
                "order_id": str(getattr(entry_order, "id", "?")),
                "order_type": getattr(entry_order, "type", "?"),
                "reason": "native_bracket_entry_fill_price_missing",
            }
        ]

    failures: list[dict[str, object]] = []
    for order in native_orders:
        payload = dict(getattr(order, "intent_payload", None) or {})
        try:
            tick = Decimal(str(payload["price_tick"]))
            distance_ticks = Decimal(str(payload["ticks"]))
            requested_price = Decimal(str(payload["requested_price"]))
        except (KeyError, InvalidOperation, TypeError, ValueError):
            failures.append(
                _fill_audit_failure(order, "native_bracket_audit_metadata_invalid")
            )
            continue
        if (
            not all(
                value.is_finite() for value in (tick, distance_ticks, requested_price)
            )
            or tick <= 0
            or distance_ticks <= 0
            or requested_price <= 0
        ):
            failures.append(
                _fill_audit_failure(order, "native_bracket_audit_metadata_invalid")
            )
            continue

        entry_side = getattr(entry_order, "side", None)
        away_from_entry = (
            getattr(entry_side, "value", entry_side) == "buy"
            and getattr(order, "type", None) == "take_profit"
        ) or (
            getattr(entry_side, "value", entry_side) == "sell"
            and getattr(order, "type", None) == "stop_loss"
        )
        expected_price = (
            fill_price + distance_ticks * tick
            if away_from_entry
            else fill_price - distance_ticks * tick
        )
        drift = (expected_price - requested_price).copy_abs()
        payload.update(
            {
                "actual_entry_fill_price": str(fill_price),
                "expected_effective_price": str(expected_price),
                "price_drift": str(drift),
            }
        )
        remote_raw = payload.get("remote_effective_price")
        try:
            remote_price = Decimal(str(remote_raw))
        except (InvalidOperation, TypeError, ValueError):
            remote_price = None
        if remote_price is None:
            payload["protection_confirmation"] = "pending_remote_event"
        elif not remote_price.is_finite() or remote_price != expected_price:
            payload["protection_confirmation"] = "conflict"
            failures.append(
                _fill_audit_failure(order, "native_bracket_remote_price_mismatch")
            )
        else:
            payload.update(
                {
                    "effective_price": str(remote_price),
                    "protection_confirmation": "confirmed",
                }
            )
            setattr(order, "trigger_price", remote_price)
        setattr(order, "intent_payload", payload)
        repository.update_order(cast("Order", order))
    return failures


def _fill_audit_failure(order: object, reason: str) -> dict[str, object]:
    return {
        "order_id": str(getattr(order, "id", "?")),
        "order_type": getattr(order, "type", "?"),
        "reason": reason,
    }


def supports_native_bracket_group(orders: Sequence[object]) -> bool:
    """Return whether an order group requires Rithmic native protection."""
    return any(
        str(getattr(order, "type", "")).lower()
        in {"stop_loss", "take_profit", "trailing_stop"}
        for order in orders
    )


def build_native_bracket_plan(
    orders: Sequence[OrderT],
    *,
    validate_order: Callable[[OrderT], None],
    get_instrument_spec: Callable[[str], InstrumentSpec],
    order_side: Callable[[OrderT], str],
    persist: bool,
) -> NativeBracketPlan[OrderT]:
    """Validate one native bracket and optionally project durable metadata."""
    entries = [
        order
        for order in orders
        if str(getattr(order, "type", "")).lower() in {"market", "limit"}
    ]
    legs = [order for order in orders if order not in entries]
    if len(entries) != 1 or not legs:
        raise ExchangeError("rithmic_native_bracket_group_invalid")
    entry = entries[0]
    validate_order(entry)
    if Decimal(str(getattr(entry, "quantity", None))) != Decimal("1"):
        raise ExchangeError("rithmic_native_bracket_single_contract_required")

    entry_product_id = str(getattr(entry, "product_id", ""))
    spec = get_instrument_spec(entry_product_id)
    if spec.price_tick is None or spec.price_tick <= 0:
        raise ExchangeError("rithmic_native_bracket_price_tick_required")
    reference_price = (
        Decimal(str(getattr(entry, "price")))
        if getattr(entry, "price", None) is not None
        else _finite_positive_decimal(
            getattr(entry, "min_notional_reference_price", None),
            "rithmic_native_bracket_reference_price_required",
        )
    )
    if reference_price % spec.price_tick != 0:
        raise ExchangeError("rithmic_native_bracket_reference_price_off_tick")

    by_type: dict[str, OrderT] = {}
    ticks: dict[str, int] = {}
    for leg in legs:
        leg_type = str(getattr(leg, "type", "")).lower()
        if leg_type not in {"stop_loss", "take_profit"}:
            raise ExchangeError(
                f"rithmic_native_bracket_leg_unsupported: order_type={leg_type}"
            )
        if leg_type in by_type:
            raise ExchangeError(
                f"rithmic_native_bracket_duplicate_leg: order_type={leg_type}"
            )
        if getattr(leg, "product_id", None) != getattr(entry, "product_id", None):
            raise ExchangeError("rithmic_native_bracket_product_mismatch")
        if Decimal(str(getattr(leg, "quantity", None))) != Decimal("1"):
            raise ExchangeError("rithmic_native_bracket_single_contract_required")
        entry_side = order_side(entry)
        expected_side = "sell" if entry_side == "buy" else "buy"
        if order_side(leg) != expected_side:
            raise ExchangeError("rithmic_native_bracket_close_side_mismatch")
        leg_client_order_id = getattr(leg, "client_order_id", None)
        if not leg_client_order_id:
            raise ExchangeError("rithmic_native_bracket_leg_client_order_id_required")
        suffix = "sl" if leg_type == "stop_loss" else "tp"
        if str(leg_client_order_id) != linked_client_order_id(
            str(getattr(entry, "client_order_id", "")), suffix
        ):
            raise ExchangeError("rithmic_native_bracket_leg_client_order_id_mismatch")
        trigger = _finite_positive_decimal(
            getattr(leg, "trigger_price", None),
            f"rithmic_native_bracket_{leg_type}_price_required",
        )
        _validate_bracket_price_side(
            entry_side=entry_side,
            leg_type=leg_type,
            reference_price=reference_price,
            trigger_price=trigger,
        )
        distance_ticks = (trigger - reference_price).copy_abs() / spec.price_tick
        integral_ticks = distance_ticks.to_integral_value()
        if distance_ticks != integral_ticks:
            raise ExchangeError(f"rithmic_native_bracket_{leg_type}_distance_off_tick")
        if integral_ticks <= 0 or integral_ticks > 2_147_483_647:
            raise ExchangeError(f"rithmic_native_bracket_{leg_type}_ticks_out_of_range")
        by_type[leg_type] = leg
        ticks[leg_type] = int(integral_ticks)

    leg_client_order_ids = {
        leg_type: str(getattr(leg, "client_order_id", ""))
        for leg_type, leg in by_type.items()
    }
    bracket_type = (
        "target_and_stop_static"
        if len(by_type) == 2
        else ("stop_only_static" if "stop_loss" in by_type else "target_only_static")
    )
    if persist:
        native = {
            "placement_mode": "attach-at-entry",
            "bracket_type": bracket_type,
            "reference_price": str(reference_price),
            "price_tick": str(spec.price_tick),
            "legs": {
                leg_type: {
                    "order_id": str(getattr(leg, "id", "")),
                    "client_order_id": str(getattr(leg, "client_order_id", "")),
                    "requested_price": str(getattr(leg, "trigger_price", "")),
                    "ticks": str(ticks[leg_type]),
                }
                for leg_type, leg in by_type.items()
            },
        }
        entry_payload = dict(getattr(entry, "intent_payload", None) or {})
        entry_payload["native_protection"] = native
        setattr(entry, "intent_payload", entry_payload)
        for leg_type, leg in by_type.items():
            leg_payload = dict(getattr(leg, "intent_payload", None) or {})
            leg_payload.update(
                {
                    "placement_mode": "attach-at-entry",
                    "native_leg_type": leg_type,
                    "native_bracket_type": bracket_type,
                    "reference_price": str(reference_price),
                    "requested_price": str(getattr(leg, "trigger_price", "")),
                    "price_tick": str(spec.price_tick),
                    "ticks": str(ticks[leg_type]),
                    "entry_side": order_side(entry),
                    "native_parent_client_order_id": str(
                        getattr(entry, "client_order_id", "")
                    ),
                }
            )
            setattr(leg, "intent_payload", leg_payload)
    return {
        "entry": entry,
        "stop_ticks": ticks.get("stop_loss"),
        "target_ticks": ticks.get("take_profit"),
        "leg_client_order_ids": leg_client_order_ids,
    }


def build_restored_native_bracket_groups(
    orders: Sequence[object],
) -> NativeBracketGroups:
    """Build replay candidates without observing or mutating live adapter state."""
    restored: NativeBracketGroups = {}
    for order in orders:
        payload = getattr(order, "intent_payload", None)
        if (
            not isinstance(payload, dict)
            or payload.get("placement_mode") != "attach-at-entry"
        ):
            continue
        parent_basket_id = payload.get("native_parent_basket_id")
        if not parent_basket_id:
            continue
        parent_client_order_id = payload.get("native_parent_client_order_id")
        leg_type = payload.get("native_leg_type")
        if not parent_client_order_id or leg_type not in {
            "stop_loss",
            "take_profit",
        }:
            raise ExchangeError("rithmic_native_bracket_restore_metadata_invalid")
        group = restored.setdefault(
            str(parent_basket_id),
            {"entry": str(parent_client_order_id)},
        )
        if group["entry"] != str(parent_client_order_id):
            raise ExchangeError("rithmic_native_bracket_restore_metadata_conflict")
        existing = group.get(str(leg_type))
        client_order_id = getattr(order, "client_order_id", None)
        if existing is not None and existing != str(client_order_id):
            raise ExchangeError("rithmic_native_bracket_restore_metadata_conflict")
        if not client_order_id:
            raise ExchangeError("rithmic_native_bracket_restore_metadata_invalid")
        group[str(leg_type)] = str(client_order_id)

    for entry in orders:
        payload = getattr(entry, "intent_payload", None)
        native = payload.get("native_protection") if isinstance(payload, dict) else None
        exchange_order_id = getattr(entry, "exchange_order_id", None)
        client_order_id = getattr(entry, "client_order_id", None)
        if not isinstance(native, dict) or not exchange_order_id:
            continue
        if not client_order_id:
            raise ExchangeError("rithmic_native_bracket_parent_client_order_id_missing")
        legs = native.get("legs")
        if not isinstance(legs, dict) or not legs:
            raise ExchangeError("rithmic_native_bracket_restore_metadata_invalid")
        group = {"entry": str(client_order_id)}
        for leg_type in ("stop_loss", "take_profit"):
            leg = legs.get(leg_type)
            if leg is None:
                continue
            client_order_id = (
                leg.get("client_order_id") if isinstance(leg, dict) else None
            )
            if not client_order_id:
                raise ExchangeError("rithmic_native_bracket_restore_metadata_invalid")
            group[leg_type] = str(client_order_id)
        existing = restored.get(str(exchange_order_id))
        if existing is None:
            restored[str(exchange_order_id)] = group
            continue
        for key, value in group.items():
            if key in existing and existing[key] != value:
                raise ExchangeError("rithmic_native_bracket_restore_metadata_conflict")
            existing[key] = value
    return restored


def merge_native_bracket_groups(
    current_groups: NativeBracketGroups,
    current_parent_ids: set[str],
    restored: NativeBracketGroups,
) -> tuple[NativeBracketGroups, set[str]]:
    """Atomically project replay candidates onto a current locked snapshot."""
    merged = {
        parent_basket_id: dict(group)
        for parent_basket_id, group in current_groups.items()
    }
    for parent_basket_id, group in restored.items():
        existing = merged.setdefault(parent_basket_id, {})
        for key, value in group.items():
            if key in existing and existing[key] != value:
                raise ExchangeError("rithmic_native_bracket_restore_metadata_conflict")
            existing[key] = value
    parent_ids = set(current_parent_ids)
    parent_ids.update(group["entry"] for group in restored.values())
    return merged, parent_ids


def resolve_native_bracket_event_client_order_id(
    *,
    client_order_id: str | None,
    basket_id: str,
    original_basket_id: str | None,
    price_type: str | None,
    groups: NativeBracketGroups,
    parent_ids: set[str],
) -> str | None:
    """Resolve one Rithmic bracket event against a locked state snapshot."""
    if original_basket_id:
        group = groups.get(str(original_basket_id))
        if group is None:
            return None
        if client_order_id not in {None, group["entry"]}:
            raise ExchangeError("rithmic_native_bracket_child_client_id_mismatch")
        leg_type = native_bracket_leg_type(price_type)
        return group.get(leg_type) if leg_type else None
    if basket_id in groups:
        group = groups[basket_id]
        if client_order_id not in {None, group["entry"]}:
            raise ExchangeError("rithmic_native_bracket_parent_client_id_mismatch")
        return group["entry"]
    if client_order_id in parent_ids:
        return None
    return client_order_id


def build_native_protection_request(
    order: object,
    trigger_price: Decimal,
    *,
    get_instrument_spec: Callable[[str], InstrumentSpec],
) -> NativeProtectionRequest:
    """Validate and project one modify-protection request before provider I/O."""
    payload = getattr(order, "intent_payload", None)
    if (
        not isinstance(payload, dict)
        or payload.get("placement_mode") != "attach-at-entry"
    ):
        raise ExchangeError("rithmic_native_protection_identity_required")
    leg_type = str(getattr(order, "type", "")).lower()
    if leg_type not in {"stop_loss", "take_profit"}:
        raise ExchangeError("rithmic_native_protection_leg_unsupported")
    exchange_order_id = getattr(order, "exchange_order_id", None)
    if not exchange_order_id:
        raise ExchangeError("rithmic_native_protection_basket_id_required")
    quantity = getattr(order, "quantity", None)
    if Decimal(str(quantity)) != Decimal("1"):
        raise ExchangeError("rithmic_native_bracket_single_contract_required")
    product_id = str(getattr(order, "product_id", ""))
    spec = get_instrument_spec(product_id)
    if spec.price_tick is None or spec.price_tick <= 0:
        raise ExchangeError("rithmic_native_bracket_price_tick_required")
    price = _finite_positive_decimal(
        trigger_price,
        "rithmic_native_protection_price_required",
    )
    if price % spec.price_tick != 0:
        raise ExchangeError("rithmic_native_protection_price_off_tick")
    reference_price = _finite_positive_decimal(
        payload.get("actual_entry_fill_price") or payload.get("reference_price"),
        "rithmic_native_protection_reference_price_required",
    )
    _validate_bracket_price_side(
        entry_side=str(payload.get("entry_side") or "").lower(),
        leg_type=leg_type,
        reference_price=reference_price,
        trigger_price=price,
    )
    return NativeProtectionRequest(
        basket_id=str(exchange_order_id),
        product_id=product_id,
        quantity=str(quantity),
        leg_type=leg_type,
        price=str(price),
    )


def native_bracket_leg_type(price_type: str | None) -> str | None:
    """Map one Rithmic native price type to its canonical protection leg."""
    normalized = str(price_type or "").lower()
    if normalized in {"stop_market", "stop_limit"}:
        return "stop_loss"
    if normalized in {"limit", "market_if_touched", "limit_if_touched"}:
        return "take_profit"
    return None


def _finite_positive_decimal(value: object, error_code: str) -> Decimal:
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as error:
        raise ExchangeError(error_code) from error
    if not parsed.is_finite() or parsed <= 0:
        raise ExchangeError(error_code)
    return parsed


def _validate_bracket_price_side(
    *,
    entry_side: str,
    leg_type: str,
    reference_price: Decimal,
    trigger_price: Decimal,
) -> None:
    valid = {
        ("buy", "stop_loss"): trigger_price < reference_price,
        ("buy", "take_profit"): trigger_price > reference_price,
        ("sell", "stop_loss"): trigger_price > reference_price,
        ("sell", "take_profit"): trigger_price < reference_price,
    }.get((entry_side, leg_type), False)
    if not valid:
        raise ExchangeError(f"rithmic_native_bracket_{leg_type}_wrong_side")
