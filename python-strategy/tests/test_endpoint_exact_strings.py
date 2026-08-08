from decimal import Decimal
from types import SimpleNamespace
from typing import SupportsIndex

import pytest
from pydantic import ValidationError

from src.core.backtest.endpoint_state import (
    EndpointOrder,
    EndpointPosition,
    ReplayEndpointState,
    build_replay_endpoint_state,
)
from src.core.models import OrderSide, PositionSide

EndpointModel = type[EndpointPosition] | type[EndpointOrder]
IDENTITY_ERROR = "endpoint identity must be a non-empty string"
ORDER_TYPE_ERROR = "endpoint order type must be a string"
SURROGATE_ORACLE = ("utf-8", "\ud800", 0, 1, "surrogates not allowed")


class HostileString(str):
    calls: list[str]
    impersonated: str

    def __new__(cls, value: str, impersonated: str | None = None) -> "HostileString":
        instance = super().__new__(cls, value)
        instance.calls = []
        instance.impersonated = value if impersonated is None else impersonated
        return instance

    def strip(self, chars: str | None = None) -> str:
        self.calls.append("strip")
        return "visible"

    def __str__(self) -> str:
        self.calls.append("str")
        return self.impersonated

    def upper(self) -> str:
        self.calls.append("upper")
        return self.impersonated

    def replace(self, old: str, new: str, count: SupportsIndex = -1) -> str:
        self.calls.append("replace")
        return self.impersonated

    def encode(self, *args: object, **kwargs: object) -> bytes:
        self.calls.append("encode")
        return b"visible"

    def __hash__(self) -> int:
        self.calls.append("hash")
        return str.__hash__(self.impersonated)

    def __eq__(self, other: object) -> bool:
        self.calls.append("eq")
        return str.__eq__(self.impersonated, other)

    def __repr__(self) -> str:
        self.calls.append("repr")
        return str.__repr__(self)

    def __format__(self, format_spec: str) -> str:
        self.calls.append("format")
        return str.__format__(self, format_spec)


def _position_values() -> dict[str, object]:
    return {
        "strategy_id": "alpha",
        "product_id": "MNQ",
        "side": PositionSide.LONG,
        "quantity": Decimal("1"),
        "average_entry_price": Decimal("100"),
    }


def _order_values() -> dict[str, object]:
    return {
        "strategy_id": "alpha",
        "product_id": "MNQ",
        "side": OrderSide.BUY,
        "order_type": "LIMIT",
        "quantity": Decimal("1"),
        "timestamp": 1,
    }


DIRECT_CASES = [
    (EndpointPosition, "strategy_id"),
    (EndpointPosition, "product_id"),
    (EndpointOrder, "strategy_id"),
    (EndpointOrder, "product_id"),
    (EndpointOrder, "order_type"),
]
BUILDER_CASES = [
    ("position", "strategy_id"),
    ("position", "product_id"),
    ("order", "strategy_id"),
    ("order", "product_id"),
]
NESTED_CASES = [
    (EndpointPosition, "positions", "strategy_id"),
    (EndpointOrder, "working_orders", "product_id"),
    (EndpointOrder, "working_orders", "order_type"),
]


@pytest.mark.parametrize("model,field", DIRECT_CASES)
@pytest.mark.parametrize(
    "raw,impersonated",
    [
        ("alpha", None),
        ("策略🙂", None),
        ("LIMIT", None),
        ("", None),
        (" ", None),
        ("\ud800", None),
        ("PEGGED", "LIMIT"),
    ],
)
def test_direct_fields_reject_str_subclasses_before_hostile_operations(
    model: EndpointModel,
    field: str,
    raw: str,
    impersonated: str | None,
) -> None:
    hostile = HostileString(raw, impersonated)
    values = _position_values() if model is EndpointPosition else _order_values()
    values[field] = hostile
    with pytest.raises(ValidationError) as exc_info:
        model.model_validate(values)
    error = exc_info.value.errors(include_url=False)[0]
    message = ORDER_TYPE_ERROR if field == "order_type" else IDENTITY_ERROR
    context = error.get("ctx")
    assert context is not None
    assert (error["loc"], str(context["error"])) == ((field,), message)
    assert hostile.calls == []


@pytest.mark.parametrize("model,field", DIRECT_CASES[:4])
@pytest.mark.parametrize("value", ["  alpha  ", "  策略-🙂  "])
def test_direct_identities_preserve_exact_base_string_bytes(
    model: EndpointModel, field: str, value: str
) -> None:
    values = _position_values() if model is EndpointPosition else _order_values()
    values[field] = value
    stored = getattr(model.model_validate(values), field)
    assert type(stored) is str and stored.encode() == value.encode()


def test_direct_order_types_preserve_exact_base_strings() -> None:
    for order_type in ("MARKET", "LIMIT", "STOP_LOSS", "TAKE_PROFIT", "TRAILING_STOP"):
        values = {**_order_values(), "order_type": order_type}
        assert EndpointOrder.model_validate(values).order_type == order_type


@pytest.mark.parametrize("model,field", DIRECT_CASES[:4])
def test_lone_surrogate_preserves_complete_unicode_error_oracle(
    model: EndpointModel, field: str
) -> None:
    values = _position_values() if model is EndpointPosition else _order_values()
    values[field] = "\ud800"
    with pytest.raises(ValidationError) as exc_info:
        model.model_validate(values)
    error = exc_info.value.errors(include_url=False)[0]
    context = error.get("ctx")
    assert context is not None
    cause = context["error"]
    assert error["loc"] == (field,)
    assert (error["type"], error["msg"]) == ("value_error", f"Value error, {cause}")
    assert type(cause) is UnicodeEncodeError
    oracle = (cause.encoding, cause.object, cause.start, cause.end, cause.reason)
    assert oracle == SURROGATE_ORACLE


@pytest.mark.parametrize("source_kind,field", BUILDER_CASES)
def test_builder_rejects_identity_subclasses_without_hostile_operations(
    source_kind: str, field: str
) -> None:
    values = _position_values() if source_kind == "position" else _order_values()
    hostile = HostileString("alpha")
    values[field] = hostile
    source = SimpleNamespace(**values)
    with pytest.raises(ValueError, match=rf"^{field} must be a non-empty string$"):
        build_replay_endpoint_state(
            positions=(source,) if source_kind == "position" else (),
            working_orders=(source,) if source_kind == "order" else (),
        )
    assert hostile.calls == []


@pytest.mark.parametrize("kind,field", BUILDER_CASES)
@pytest.mark.parametrize("value", ["  alpha  ", "  策略-🙂  "])
def test_builder_preserves_bytes(kind: str, field: str, value: str) -> None:
    values = _position_values() if kind == "position" else _order_values()
    values[field] = value
    source = SimpleNamespace(**values)
    positions = (source,) if kind == "position" else ()
    orders = (source,) if kind == "order" else ()
    state = build_replay_endpoint_state(positions=positions, working_orders=orders)
    child = state.positions[0] if kind == "position" else state.working_orders[0]
    stored = getattr(child, field)
    assert type(stored) is str and stored.encode() == value.encode()


def test_builder_order_type_normalization_is_unchanged() -> None:
    source = SimpleNamespace(**{**_order_values(), "order_type": " limit "})
    state = build_replay_endpoint_state(positions=(), working_orders=(source,))
    assert state.working_orders[0].order_type == "LIMIT"


@pytest.mark.parametrize(
    "source_kind,field,raw,impersonated,error",
    [
        (
            "position",
            "side",
            "SHORT",
            "LONG",
            "position side must be a PositionSide or string",
        ),
        (
            "order",
            "side",
            "SELL",
            "BUY",
            "order side must be an OrderSide, PositionSide, or string",
        ),
        (
            "order",
            "order_type",
            "PEGGED",
            "LIMIT",
            "order type must be a string",
        ),
    ],
)
def test_builder_rejects_side_and_type_subclasses_before_hostile_operations(
    source_kind: str,
    field: str,
    raw: str,
    impersonated: str,
    error: str,
) -> None:
    values = _position_values() if source_kind == "position" else _order_values()
    hostile = HostileString(raw, impersonated)
    values[field] = hostile
    source = SimpleNamespace(**values)
    with pytest.raises(ValueError, match=rf"^{error}$"):
        build_replay_endpoint_state(
            positions=(source,) if source_kind == "position" else (),
            working_orders=(source,) if source_kind == "order" else (),
        )
    assert hostile.calls == []


@pytest.mark.parametrize(
    "side", [PositionSide.LONG, PositionSide.SHORT, " long ", "SHORT"]
)
def test_builder_position_side_compatibility(side: PositionSide | str) -> None:
    source = SimpleNamespace(**{**_position_values(), "side": side})
    state = build_replay_endpoint_state(positions=(source,), working_orders=())
    expected = (
        side if type(side) is PositionSide else PositionSide(str.upper(str.strip(side)))
    )
    assert state.positions[0].side is expected


@pytest.mark.parametrize(
    "side,expected",
    [
        (OrderSide.BUY, OrderSide.BUY),
        (PositionSide.SHORT, OrderSide.SELL),
        (" buy ", OrderSide.BUY),
        ("SHORT", OrderSide.SELL),
    ],
)
def test_builder_order_side_compatibility(
    side: OrderSide | PositionSide | str, expected: OrderSide
) -> None:
    source = SimpleNamespace(**{**_order_values(), "side": side})
    state = build_replay_endpoint_state(positions=(), working_orders=(source,))
    assert state.working_orders[0].side is expected


@pytest.mark.parametrize(
    "source_kind,field,value,error",
    [
        ("position", "side", "FLAT", "unsupported position side: 'FLAT'"),
        ("order", "side", "HOLD", "unsupported order side: 'HOLD'"),
        ("order", "order_type", "PEGGED", "unsupported order type: 'PEGGED'"),
    ],
)
def test_builder_rejects_invalid_ordinary_side_and_type_strings(
    source_kind: str, field: str, value: str, error: str
) -> None:
    values = _position_values() if source_kind == "position" else _order_values()
    values[field] = value
    source = SimpleNamespace(**values)
    with pytest.raises(ValueError, match=rf"^{error}$"):
        build_replay_endpoint_state(
            positions=(source,) if source_kind == "position" else (),
            working_orders=(source,) if source_kind == "order" else (),
        )


@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
@pytest.mark.parametrize("model,container,field", NESTED_CASES)
def test_nested_corrupted_children_reject_original_subclass_at_exact_location(
    method: str,
    model: EndpointModel,
    container: str,
    field: str,
) -> None:
    values = _position_values() if model is EndpointPosition else _order_values()
    valid = model.model_validate(values)
    hostile = HostileString("LIMIT" if field == "order_type" else "alpha")
    child = (
        valid.model_copy(update={field: hostile})
        if method == "model_copy"
        else model.model_construct(**{**values, field: hostile})
    )
    with pytest.raises(ValidationError) as exc_info:
        ReplayEndpointState.model_validate({container: (child,)})
    assert exc_info.value.errors(include_url=False)[0]["loc"] == (container, 0, field)
    assert getattr(child, field) is hostile and hostile.calls == []
