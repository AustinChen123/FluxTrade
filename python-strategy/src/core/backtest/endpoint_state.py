"""Canonical, serializable replay endpoint state.

The matcher and persistence layers expose similar state with different field
names and side conventions.  This module owns the single conversion boundary
used by replay runners; source-specific identifiers are intentionally omitted.
"""

from collections.abc import Iterable
from decimal import Decimal
from typing import Any

from pydantic import ConfigDict, Field, computed_field, field_validator, model_validator

from src.core.models import BaseFluxModel, OrderSide, PositionSide


_ORDER_TYPES = frozenset(
    {"MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"}
)
_PROTECTION_ORDER_TYPES = frozenset({"STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"})
_MISSING = object()


def _required_attribute(source: object, *names: str) -> Any:
    for name in names:
        value = getattr(source, name, _MISSING)
        if value is not _MISSING:
            return value
    joined = " or ".join(repr(name) for name in names)
    raise ValueError(
        f"unsupported endpoint-state source {type(source).__name__}: missing {joined}"
    )


def _optional_attribute(source: object, *names: str) -> Any | None:
    for name in names:
        value = getattr(source, name, _MISSING)
        if value is not _MISSING:
            return value
    return None


def _decimal(value: Any, *, field_name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be a decimal value")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (ArithmeticError, TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be a decimal value") from exc
    if not result.is_finite():
        raise ValueError(f"{field_name} must be finite")
    text = format(result, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    if text in {"-0", ""}:
        text = "0"
    return Decimal(text)


def _identity(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value


def _optional_decimal(value: Any | None, *, field_name: str) -> Decimal | None:
    if value is None:
        return None
    return _decimal(value, field_name=field_name)


def _position_side(value: Any) -> PositionSide:
    if isinstance(value, PositionSide):
        return value
    raw = value.value if hasattr(value, "value") else value
    try:
        return PositionSide(str(raw).strip().upper())
    except ValueError as exc:
        raise ValueError(f"unsupported position side: {value!r}") from exc


def _order_type(value: Any) -> str:
    raw = value.value if hasattr(value, "value") else value
    normalized = str(raw).strip().upper().replace(" ", "_")
    if normalized not in _ORDER_TYPES:
        raise ValueError(f"unsupported order type: {value!r}")
    return normalized


def _order_side(value: Any, *, order_type: str) -> OrderSide:
    if isinstance(value, OrderSide):
        return value
    raw = value.value if hasattr(value, "value") else value
    normalized = str(raw).strip().upper()
    if normalized == "BUY":
        return OrderSide.BUY
    if normalized == "SELL":
        return OrderSide.SELL
    if normalized in {"LONG", "SHORT"}:
        position_side = PositionSide(normalized)
        if order_type in _PROTECTION_ORDER_TYPES:
            return OrderSide.closing_side(position_side)
        return OrderSide.from_position_side(position_side)
    raise ValueError(f"unsupported order side: {value!r}")


def _timestamp(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer timestamp")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field_name} must be an integer timestamp") from exc
    if result < 0:
        raise ValueError(f"{field_name} must be non-negative")
    return result


class EndpointPosition(BaseFluxModel):
    """Open position at the end of a replay."""

    model_config = ConfigDict(frozen=True)

    strategy_id: str
    product_id: str
    side: PositionSide
    quantity: Decimal
    average_entry_price: Decimal

    @field_validator("strategy_id", "product_id")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("endpoint identity must be a non-empty string")
        return value

    @field_validator("quantity")
    @classmethod
    def validate_positive_quantity(cls, value: Decimal) -> Decimal:
        if not value.is_finite() or value <= 0:
            raise ValueError("endpoint position quantity must be finite and positive")
        return _decimal(value, field_name="endpoint position quantity")

    @field_validator("average_entry_price")
    @classmethod
    def validate_average_entry_price(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("endpoint average entry price must be finite")
        return _decimal(value, field_name="endpoint average entry price")


class EndpointOrder(BaseFluxModel):
    """Working order at the end of a replay, expressed in action-side terms."""

    model_config = ConfigDict(frozen=True)

    strategy_id: str
    product_id: str
    side: OrderSide
    order_type: str
    quantity: Decimal
    timestamp: int
    price: Decimal | None = None
    trigger_price: Decimal | None = None
    trailing_distance: Decimal | None = None

    @field_validator("strategy_id", "product_id")
    @classmethod
    def validate_identity(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("endpoint identity must be a non-empty string")
        return value

    @field_validator("order_type")
    @classmethod
    def validate_order_type(cls, value: str) -> str:
        return _order_type(value)

    @field_validator("quantity")
    @classmethod
    def validate_quantity(cls, value: Decimal) -> Decimal:
        if not value.is_finite() or value <= 0:
            raise ValueError("endpoint order quantity must be finite and positive")
        return _decimal(value, field_name="endpoint order quantity")

    @field_validator("price", "trigger_price")
    @classmethod
    def validate_optional_price(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and not value.is_finite():
            raise ValueError("endpoint order prices must be finite")
        return (
            _decimal(value, field_name="endpoint order price")
            if value is not None
            else None
        )

    @field_validator("trailing_distance")
    @classmethod
    def validate_trailing_distance(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and (not value.is_finite() or value <= 0):
            raise ValueError("endpoint trailing distance must be finite and positive")
        return (
            _decimal(value, field_name="endpoint trailing distance")
            if value is not None
            else None
        )

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, value: int) -> int:
        if isinstance(value, bool) or value < 0:
            raise ValueError("endpoint order timestamp must be non-negative")
        return value


class ReplayEndpointState(BaseFluxModel):
    """Deterministic state remaining after the final replay candle."""

    model_config = ConfigDict(frozen=True)

    positions: tuple[EndpointPosition, ...] = ()
    working_orders: tuple[EndpointOrder, ...] = ()
    final_mark: Decimal | None = None
    end_timestamp: int | None = None
    halted_early: bool = Field(
        default=False,
        description=(
            "True when a configured halt policy terminated replay processing; "
            "it does not imply that unread input was proven to remain."
        ),
    )

    @field_validator("final_mark")
    @classmethod
    def validate_final_mark(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and not value.is_finite():
            raise ValueError("final_mark must be finite")
        return _decimal(value, field_name="final mark") if value is not None else None

    @field_validator("end_timestamp")
    @classmethod
    def validate_end_timestamp(cls, value: int | None) -> int | None:
        if value is not None and (isinstance(value, bool) or value < 0):
            raise ValueError("end_timestamp must be non-negative")
        return value

    @model_validator(mode="after")
    def validate_final_observation_pair(self) -> "ReplayEndpointState":
        if (self.final_mark is None) != (self.end_timestamp is None):
            raise ValueError("final_mark and end_timestamp must be provided together")
        return self

    @computed_field(return_type=tuple[EndpointOrder, ...])
    @property
    def protection_orders(self) -> tuple[EndpointOrder, ...]:
        return tuple(
            order
            for order in self.working_orders
            if order.order_type in _PROTECTION_ORDER_TYPES
        )


def _normalize_position(source: object) -> EndpointPosition:
    if isinstance(source, EndpointPosition):
        return source
    return EndpointPosition(
        strategy_id=_identity(
            _required_attribute(source, "strategy_id"), field_name="strategy_id"
        ),
        product_id=_identity(
            _required_attribute(source, "product_id"), field_name="product_id"
        ),
        side=_position_side(_required_attribute(source, "side")),
        quantity=_decimal(
            _required_attribute(source, "quantity"), field_name="position quantity"
        ),
        average_entry_price=_decimal(
            _required_attribute(source, "average_entry_price", "entry_price"),
            field_name="average entry price",
        ),
    )


def _normalize_order(source: object) -> EndpointOrder:
    if isinstance(source, EndpointOrder):
        return source
    normalized_type = _order_type(_required_attribute(source, "order_type", "type"))
    price = _optional_decimal(
        _optional_attribute(source, "price"), field_name="order price"
    )
    if (
        normalized_type == "MARKET" or normalized_type in _PROTECTION_ORDER_TYPES
    ) and price == 0:
        price = None
    return EndpointOrder(
        strategy_id=_identity(
            _required_attribute(source, "strategy_id"), field_name="strategy_id"
        ),
        product_id=_identity(
            _required_attribute(source, "product_id"), field_name="product_id"
        ),
        side=_order_side(
            _required_attribute(source, "side"), order_type=normalized_type
        ),
        order_type=normalized_type,
        quantity=_decimal(
            _required_attribute(source, "quantity"), field_name="order quantity"
        ),
        timestamp=_timestamp(
            _required_attribute(source, "timestamp"), field_name="order timestamp"
        ),
        price=price,
        trigger_price=_optional_decimal(
            _optional_attribute(source, "trigger_price"),
            field_name="order trigger price",
        ),
        trailing_distance=_optional_decimal(
            _optional_attribute(source, "trailing_distance", "_trailing_distance"),
            field_name="order trailing distance",
        ),
    )


def _optional_decimal_sort_key(value: Decimal | None) -> tuple[bool, Decimal]:
    return value is None, value if value is not None else Decimal("0")


def _position_sort_key(position: EndpointPosition) -> tuple[Any, ...]:
    return (
        position.strategy_id,
        position.product_id,
        position.side.value,
        position.quantity,
        position.average_entry_price,
    )


def _order_sort_key(order: EndpointOrder) -> tuple[Any, ...]:
    return (
        order.strategy_id,
        order.product_id,
        order.side.value,
        order.order_type,
        order.timestamp,
        _optional_decimal_sort_key(order.price),
        _optional_decimal_sort_key(order.trigger_price),
        _optional_decimal_sort_key(order.trailing_distance),
        order.quantity,
    )


def build_replay_endpoint_state(
    *,
    positions: Iterable[object],
    working_orders: Iterable[object],
    final_mark: Decimal | str | int | None = None,
    end_timestamp: int | None = None,
    halted_early: bool = False,
) -> ReplayEndpointState:
    """Normalize matcher, ORM, or Pydantic state into one deterministic model."""

    normalized_positions = tuple(
        sorted(
            (_normalize_position(item) for item in positions), key=_position_sort_key
        )
    )
    normalized_orders = tuple(
        sorted((_normalize_order(item) for item in working_orders), key=_order_sort_key)
    )
    return ReplayEndpointState(
        positions=normalized_positions,
        working_orders=normalized_orders,
        final_mark=(
            _decimal(final_mark, field_name="final mark")
            if final_mark is not None
            else None
        ),
        end_timestamp=(
            _timestamp(end_timestamp, field_name="end timestamp")
            if end_timestamp is not None
            else None
        ),
        halted_early=halted_early,
    )
