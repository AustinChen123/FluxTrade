"""Decimal quantization helpers for hot-path transport boundaries.

Strategies, APIs, reports, and DB-facing code can keep Decimal semantics. This
module is the narrow boundary for converting exchange-grid values into scaled
integer units before crossing a faster transport layer such as PyO3.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, ROUND_UP
from enum import Enum


class RoundingMode(str, Enum):
    """Supported quantization policies for exchange-grid values."""

    NEAREST = "nearest"
    DOWN = "down"
    UP = "up"


_ROUNDING_MAP = {
    RoundingMode.NEAREST: ROUND_HALF_UP,
    RoundingMode.DOWN: ROUND_DOWN,
    RoundingMode.UP: ROUND_UP,
}


@dataclass(frozen=True, slots=True)
class PrecisionSpec:
    """Product precision used by a codec for prices, quantities, and fee rates."""

    price_tick: Decimal
    quantity_step: Decimal
    fee_rate_step: Decimal = Decimal("0.00000001")

    def __post_init__(self) -> None:
        _validate_step(self.price_tick, "price_tick")
        _validate_step(self.quantity_step, "quantity_step")
        _validate_step(self.fee_rate_step, "fee_rate_step")


@dataclass(frozen=True, slots=True)
class PrecisionCodec:
    """Convert Decimal values to integer units and back using a precision spec."""

    spec: PrecisionSpec
    _price_multiplier: Decimal = field(init=False, repr=False)
    _quantity_multiplier: Decimal = field(init=False, repr=False)
    _fee_rate_multiplier: Decimal = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_price_multiplier", Decimal(1) / self.spec.price_tick)
        object.__setattr__(self, "_quantity_multiplier", Decimal(1) / self.spec.quantity_step)
        object.__setattr__(self, "_fee_rate_multiplier", Decimal(1) / self.spec.fee_rate_step)

    def encode_price(
        self,
        value: Decimal | str | int,
        *,
        rounding: RoundingMode = RoundingMode.NEAREST,
    ) -> int:
        return _encode_units(_to_decimal(value), self._price_multiplier, rounding)

    def decode_price(self, units: int) -> Decimal:
        return Decimal(units) * self.spec.price_tick

    def encode_quantity(
        self,
        value: Decimal | str | int,
        *,
        rounding: RoundingMode = RoundingMode.DOWN,
    ) -> int:
        return _encode_units(_to_decimal(value), self._quantity_multiplier, rounding)

    def decode_quantity(self, units: int) -> Decimal:
        return Decimal(units) * self.spec.quantity_step

    def encode_fee_rate(
        self,
        value: Decimal | str | int,
        *,
        rounding: RoundingMode = RoundingMode.NEAREST,
    ) -> int:
        return _encode_units(_to_decimal(value), self._fee_rate_multiplier, rounding)

    def decode_fee_rate(self, units: int) -> Decimal:
        return Decimal(units) * self.spec.fee_rate_step


def _encode_units(value: Decimal, multiplier: Decimal, rounding: RoundingMode) -> int:
    if value < 0:
        raise ValueError("value must be non-negative")
    units = (value * multiplier).to_integral_value(rounding=_ROUNDING_MAP[rounding])
    return int(units)


def _to_decimal(value: Decimal | str | int) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, str):
        return Decimal(value)
    raise TypeError("value must be Decimal, str, or int")


def _validate_step(value: Decimal, field_name: str) -> None:
    if not isinstance(value, Decimal):
        raise TypeError(f"{field_name} must be Decimal")
    if value <= 0:
        raise ValueError(f"{field_name} must be positive")
