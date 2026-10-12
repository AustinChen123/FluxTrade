"""Pure normal-source callback classification and identity helpers."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import Literal, Sequence, cast

from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.decimal_math import canonical_decimal_text


@dataclass(frozen=True, slots=True)
class SourceOrderTransition:
    """One proved source-cache transition; pending evidence has no source state."""

    state: Literal[
        "new", "sent", "live", "partially_filled", "filled", "canceled",
        "failed", "economic_terminal",
    ] | None
    full_callback: bool
    reason_code: str | None = None


@dataclass(frozen=True, slots=True)
class SourceCandidateClassification:
    """Pure disposition and normalized terms for one callback member."""

    disposition: Literal["PLANNED", "FILTERED_SOURCE", "UNRESOLVED_TARGET"]
    reason_code: str | None
    canonical_product_id: str | None
    side: str | None
    price: Decimal | None
    quantity: Decimal | None
    source_client_id: str | None
    source_market_code: int | None
    event_at_ms: int | None
    cancel_alias: str | None
    target_action_id: str | None
    target_local_order_id: str | None
    needs_cancel_target_lookup: bool = False


def classify_source_candidate(
    operation: str,
    member: dict[str, object],
    *,
    event_at_ms: object,
    admitted_products: dict[str, str],
    cancel_targets: Sequence[tuple[str, str]] = (),
) -> SourceCandidateClassification:
    """Classify one source member without ORM, IDs, or external state."""
    product = member.get("instId")
    canonical_product = admitted_products.get(product) if type(product) is str else None
    empty = {
        "canonical_product_id": canonical_product,
        "side": None,
        "price": None,
        "quantity": None,
        "source_client_id": None,
        "source_market_code": None,
        "event_at_ms": None,
        "cancel_alias": None,
        "target_action_id": None,
        "target_local_order_id": None,
    }
    if canonical_product is None:
        return SourceCandidateClassification(
            "FILTERED_SOURCE", "SOURCE_PRODUCT_UNAVAILABLE", **empty
        )
    if operation == "PLACE":
        try:
            price_value = member.get("px")
            quantity_value = member.get("sz")
            if type(price_value) is not str or type(quantity_value) is not str:
                raise ValueError("invalid source decimal")
            price = Decimal(price_value)
            quantity = Decimal(quantity_value)
            side_value = member.get("side")
            client_id = member.get("clOrdId")
            if (
                not price.is_finite() or price <= 0
                or not quantity.is_finite() or quantity <= 0
                or side_value not in {"buy", "sell"}
                or type(client_id) is not str or not client_id
                or type(event_at_ms) is not int
            ):
                raise ValueError("invalid source terms")
        except (InvalidOperation, TypeError, ValueError):
            return SourceCandidateClassification(
                "FILTERED_SOURCE", "SOURCE_TERMS_UNAVAILABLE", **empty
            )
        market_code = member.get("instIdCode")
        if type(market_code) is not int or market_code <= 0:
            return SourceCandidateClassification(
                "FILTERED_SOURCE", "SOURCE_MARKET_UNAVAILABLE", **empty
            )
        return SourceCandidateClassification(
            "PLANNED", None,
            canonical_product,
            cast(str, side_value),
            price,
            quantity,
            cast(str, client_id),
            market_code,
            cast(int, event_at_ms),
            None,
            None,
            None,
        )
    if operation != "CANCEL":
        raise ValueError("SPIDER_SOURCE_OPERATION_INVALID")
    alias = member.get("ordId")
    if type(alias) is not str or not alias:
        return SourceCandidateClassification(
            "UNRESOLVED_TARGET", "SOURCE_CANCEL_TARGET_UNRESOLVED", **empty
        )
    if not cancel_targets:
        return SourceCandidateClassification(
            "UNRESOLVED_TARGET",
            "SOURCE_CANCEL_TARGET_UNRESOLVED",
            **(empty | {"cancel_alias": alias}),
            needs_cancel_target_lookup=True,
        )
    if len(cancel_targets) != 1:
        return SourceCandidateClassification(
            "UNRESOLVED_TARGET",
            "SOURCE_CANCEL_TARGET_UNRESOLVED",
            **(empty | {"cancel_alias": alias}),
        )
    target_action_id, target_local_order_id = cancel_targets[0]
    return SourceCandidateClassification(
        "PLANNED",
        None,
        canonical_product,
        None,
        None,
        None,
        None,
        None,
        None,
        alias,
        target_action_id,
        target_local_order_id,
    )


class SourceClockUnavailable(RuntimeError):
    """The saved source clock cannot safely enter a reset/cooldown path."""


_LIVE_STATUSES = frozenset({"SUBMITTED", "PARTIALLY_FILLED"})
_LEGACY_CLOSED = frozenset({"closed"})
_LEGACY_FAILED = frozenset({"failed"})


def classify_source_order_transition(
    status: object,
    *,
    original_quantity: Decimal,
    cumulative_quantity: Decimal,
    trade_chain_proved: bool,
) -> SourceOrderTransition:
    """The sole status/quantity classifier for admitted financial receipts."""
    pending = SourceOrderTransition(None, False, "SOURCE_STATE_UNAVAILABLE")
    if (
        type(status) is not str
        or not original_quantity.is_finite()
        or not cumulative_quantity.is_finite()
        or original_quantity <= 0
        or cumulative_quantity < 0
        or cumulative_quantity > original_quantity
    ):
        return pending
    quantity = cumulative_quantity
    if status == "NEW" and quantity == 0:
        return SourceOrderTransition("new", False)
    if status == "SUBMITTED_UNCONFIRMED" and quantity == 0:
        return SourceOrderTransition("sent", False)
    if status in _LIVE_STATUSES:
        if quantity == 0:
            return SourceOrderTransition("live", False)
        if quantity < original_quantity:
            return SourceOrderTransition("partially_filled", False)
        if trade_chain_proved:
            return SourceOrderTransition("filled", True)
        return pending
    if status == "FILLED" or status in _LEGACY_CLOSED:
        if quantity == original_quantity and trade_chain_proved:
            return SourceOrderTransition("filled", True)
        return pending
    if status == "CANCELLED" and quantity <= original_quantity:
        return SourceOrderTransition("canceled", False)
    if status == "FAILED" or status in _LEGACY_FAILED:
        return SourceOrderTransition("failed", False)
    if status == "LIQUIDATED":
        return SourceOrderTransition("economic_terminal", False)
    return pending


def full_callback_identity(
    scope: dict[str, str], local_order_id: str, generation: int
) -> str:
    """Return the v1 full-callback idempotency key."""
    value = {
        "domain": "SPIDER_GRID_FULL_CALLBACK_V1",
        "scope": scope,
        "local_order_id": local_order_id,
        "original_generation": generation,
    }
    return sha256(canonical_bytes(value)).hexdigest()


def source_alias(original_source_client_id: str) -> str:
    """Keep the historical source client ID as a host-local alias."""
    if type(original_source_client_id) is not str or not original_source_client_id:
        raise ValueError("SPIDER_SOURCE_ALIAS_INVALID")
    return original_source_client_id


def invoke_saved_source_callback(policy: object, source_input: dict[str, object]) -> None:
    """Invoke only the selected original Policy entrypoint, blocking reset first."""
    if type(source_input) is not dict:
        raise ValueError("SPIDER_SOURCE_INPUT_INVALID")
    original_reset = getattr(policy, "reset_strategy", None)
    if not callable(original_reset):
        raise ValueError("SPIDER_SOURCE_POLICY_INVALID")

    def unavailable(_product: str) -> None:
        raise SourceClockUnavailable("SOURCE_CLOCK_UNAVAILABLE")

    setattr(policy, "reset_strategy", unavailable)
    try:
        kind = source_input.get("type")
        if kind == "ORDERS":
            if set(source_input) != {"type", "order"} or type(source_input.get("order")) is not dict:
                raise ValueError("SPIDER_SOURCE_INPUT_INVALID")
            callback = getattr(policy, "orders", None)
            if not callable(callback):
                raise ValueError("SPIDER_SOURCE_POLICY_INVALID")
            callback(source_input["order"])
        elif kind == "REST_ACK":
            if (
                set(source_input) != {"type", "operation", "entries"}
                or source_input.get("operation") not in {"send", "cancel"}
                or type(source_input.get("entries")) is not list
            ):
                raise ValueError("SPIDER_SOURCE_INPUT_INVALID")
            callback = getattr(policy, "rest_ack", None)
            if not callable(callback):
                raise ValueError("SPIDER_SOURCE_POLICY_INVALID")
            callback(source_input["operation"], source_input["entries"])
        else:
            raise ValueError("SPIDER_SOURCE_INPUT_INVALID")
    finally:
        setattr(policy, "reset_strategy", original_reset)


def decimal_source_text(value: object) -> str:
    """Validate source-facing finite Decimal text without coercing floats."""
    if type(value) is Decimal:
        number = value
    elif type(value) is str:
        try:
            number = Decimal(value)
        except InvalidOperation:
            raise ValueError("SPIDER_SOURCE_DECIMAL_INVALID") from None
    else:
        raise ValueError("SPIDER_SOURCE_DECIMAL_INVALID")
    if not number.is_finite():
        raise ValueError("SPIDER_SOURCE_DECIMAL_INVALID")
    return canonical_decimal_text(number)


def build_orders_source_input(
    *,
    source_alias_id: str,
    source_product_id: str,
    side: str,
    limit_price: Decimal,
    original_quantity: Decimal,
    cumulative_quantity: Decimal,
    state: str,
    fill_price: Decimal | None,
    created_at_ms: int | None,
) -> dict[str, object]:
    """Build the closed Policy.orders shape from immutable source terms."""
    if (
        side not in {"buy", "sell"}
        or state not in {"new", "sent", "live", "partially_filled", "filled", "canceled", "failed"}
        or (created_at_ms is not None and type(created_at_ms) is not int)
        or not limit_price.is_finite()
        or limit_price <= 0
        or not original_quantity.is_finite()
        or original_quantity <= 0
        or not cumulative_quantity.is_finite()
        or cumulative_quantity < 0
        or cumulative_quantity > original_quantity
    ):
        raise ValueError("SPIDER_SOURCE_INPUT_INVALID")
    fill_text = None if fill_price is None else decimal_source_text(fill_price)
    if fill_text is not None and Decimal(fill_text) <= 0:
        raise ValueError("SPIDER_SOURCE_INPUT_INVALID")
    return {
        "type": "ORDERS",
        "order": {
            "ordId": source_alias(source_alias_id),
            "clOrdId": source_alias_id,
            "instId": source_product_id,
            "side": side,
            "px": decimal_source_text(limit_price),
            "sz": decimal_source_text(original_quantity),
            "accFillSz": decimal_source_text(cumulative_quantity),
            "state": state,
            "fillPx": fill_text,
            "cTime": None if created_at_ms is None else str(created_at_ms),
        },
    }


__all__ = [
    "SourceCandidateClassification",
    "SourceOrderTransition",
    "SourceClockUnavailable",
    "classify_source_order_transition",
    "classify_source_candidate",
    "build_orders_source_input",
    "decimal_source_text",
    "full_callback_identity",
    "invoke_saved_source_callback",
    "source_alias",
]
