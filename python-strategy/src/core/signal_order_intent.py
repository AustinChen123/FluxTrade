"""Authoritative Signal-to-order-type classification."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import Literal

from src.core.models import Signal, SignalType


OrderType = Literal["market", "limit"]
PriceSource = Literal["signal.price", "signal.value", "market"]


class InvalidSignalOrderIntent(ValueError):
    """Raised when an explicitly supplied order intent is unsafe to submit."""


@dataclass(frozen=True, slots=True)
class ResolvedOrderIntent:
    """Order type and price selected from the legacy Signal contract."""

    order_type: OrderType
    limit_price: Decimal | None
    price_source: PriceSource

    @property
    def uses_legacy_value_fallback(self) -> bool:
        return self.price_source == "signal.value"


def resolve_signal_order_intent(signal: Signal) -> ResolvedOrderIntent:
    """Resolve order pricing while preserving only valid legacy value fallback."""
    if signal.price is not None:
        _require_positive_finite(signal.price, field="signal.price")
        return ResolvedOrderIntent("limit", signal.price, "signal.price")
    if signal.value is not None:
        _require_positive_finite(signal.value, field="signal.value")
        return ResolvedOrderIntent("limit", signal.value, "signal.value")
    return ResolvedOrderIntent("market", None, "market")


def normalize_signal_quantity(
    signal: Signal,
    *,
    default_entry_quantity: Decimal,
) -> Signal:
    """Return the effective signal quantity used by risk and execution."""
    if signal.type == SignalType.NO_SIGNAL:
        return signal

    quantity = signal.quantity
    if quantity is not None and not quantity.is_finite():
        raise InvalidSignalOrderIntent(
            "invalid_signal_order_intent: signal.quantity must be finite"
        )

    if signal.type not in (SignalType.LONG, SignalType.SHORT):
        return signal
    if quantity is not None and quantity > 0:
        return signal

    _require_positive_finite(
        default_entry_quantity,
        field="default_entry_quantity",
    )
    return signal.model_copy(update={"quantity": default_entry_quantity})


def _require_positive_finite(value: Decimal, *, field: str) -> None:
    if not value.is_finite() or value <= 0:
        raise InvalidSignalOrderIntent(
            f"invalid_signal_order_intent: {field} must be finite and greater than zero"
        )
