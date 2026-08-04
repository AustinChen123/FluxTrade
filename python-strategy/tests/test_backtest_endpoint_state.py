from collections.abc import Callable
from decimal import Decimal
import json
from types import SimpleNamespace

import pytest
from pydantic import ValidationError, field_validator

from src.core.backtest.endpoint_state import (
    EndpointOrder,
    EndpointPosition,
    ReplayEndpointState,
    build_replay_endpoint_state,
)
from src.core.models import OrderSide, Position, PositionSide
from src.core.orm_models import Order


@field_validator("quantity", mode="before")
@classmethod
def _accept_position_float(cls, value: object) -> float:
    if not isinstance(value, float):
        raise ValueError("malicious position quantity must be a float")
    return value


@field_validator("quantity", mode="before")
@classmethod
def _accept_order_float(cls, value: object) -> float:
    if not isinstance(value, float):
        raise ValueError("malicious order quantity must be a float")
    return value


MaliciousEndpointPosition = type(
    "MaliciousEndpointPosition",
    (EndpointPosition,),
    {
        "__annotations__": {"quantity": float},
        "validate_positive_quantity": _accept_position_float,
    },
)
MaliciousEndpointOrder = type(
    "MaliciousEndpointOrder",
    (EndpointOrder,),
    {
        "__annotations__": {"quantity": float},
        "validate_quantity": _accept_order_float,
    },
)


class AdversarialTuple(tuple[object, ...]):
    exposed: object

    def __new__(cls, stored: object, exposed: object) -> "AdversarialTuple":
        instance = super().__new__(cls, (stored,))
        instance.exposed = exposed
        return instance

    def __iter__(self):
        return iter((self.exposed,))


def _position_values() -> dict[str, object]:
    return {
        "strategy_id": "alpha",
        "product_id": "RITHMIC:MNQ-202609",
        "side": PositionSide.LONG,
        "quantity": Decimal("1"),
        "average_entry_price": Decimal("21000"),
    }


def _order_values() -> dict[str, object]:
    return {
        "strategy_id": "alpha",
        "product_id": "RITHMIC:MNQ-202609",
        "side": OrderSide.BUY,
        "order_type": "LIMIT",
        "quantity": Decimal("1"),
        "timestamp": 100,
        "price": Decimal("21000"),
        "trigger_price": Decimal("20900"),
        "trailing_distance": Decimal("10"),
    }


def _rust_position() -> SimpleNamespace:
    return SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side="LONG",
        quantity="1",
        entry_price="21000",
    )


def _rust_order() -> SimpleNamespace:
    return SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side="BUY",
        order_type="LIMIT",
        quantity="1",
        timestamp=100,
        price="21000",
        trigger_price="20900",
        trailing_distance="10",
    )


def _orm_order(**overrides: object) -> Order:
    values: dict[str, object] = {
        "id": "random-orm-id",
        "strategy_id": "alpha",
        "product_id": "RITHMIC:MNQ-202609",
        "exchange_id": "simulated",
        "type": "limit",
        "side": "buy",
        "price": Decimal("21000"),
        "trigger_price": None,
        "quantity": Decimal("1"),
        "status": "SUBMITTED",
        "timestamp": 100,
    }
    values.update(overrides)
    return Order(**values)


def test_builder_normalizes_python_orm_and_rust_shapes() -> None:
    python_position = Position(
        strategy_id="beta",
        product_id="RITHMIC:MNQ-202609",
        side=PositionSide.SHORT,
        quantity=Decimal("2"),
        entry_price=Decimal("21010"),
        unrealized_pnl=Decimal("-5"),
    )
    rust_position = SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side="LONG",
        quantity="1",
        entry_price="21000",
    )
    rust_market = SimpleNamespace(
        id="random-rust-id",
        strategy_id="beta",
        product_id="RITHMIC:MNQ-202609",
        side="SHORT",
        order_type="MARKET",
        quantity="2",
        timestamp=102,
        price="0",
        trigger_price=None,
        trailing_distance=None,
    )

    state = build_replay_endpoint_state(
        positions=[python_position, rust_position],
        working_orders=[rust_market, _orm_order()],
        final_mark="21020.25",
        end_timestamp=200,
    )

    assert [(item.strategy_id, item.side) for item in state.positions] == [
        ("alpha", PositionSide.LONG),
        ("beta", PositionSide.SHORT),
    ]
    assert [item.side for item in state.working_orders] == [
        OrderSide.BUY,
        OrderSide.SELL,
    ]
    assert state.working_orders[1].price is None
    assert state.final_mark == Decimal("21020.25")
    assert all(type(item) is EndpointPosition for item in state.positions)
    assert all(type(item) is EndpointOrder for item in state.working_orders)


@pytest.mark.parametrize(
    ("field", "subclass_instance"),
    [
        (
            "positions",
            MaliciousEndpointPosition(**{**_position_values(), "quantity": 1.0}),
        ),
        (
            "working_orders",
            MaliciousEndpointOrder(**{**_order_values(), "quantity": 1.0}),
        ),
    ],
)
def test_parent_rejects_nested_endpoint_subclasses(
    field: str,
    subclass_instance: EndpointPosition | EndpointOrder,
) -> None:
    with pytest.raises(ValidationError, match="exact"):
        ReplayEndpointState.model_validate({field: (subclass_instance,)})


@pytest.mark.parametrize(
    ("positions", "working_orders"),
    [
        (
            (MaliciousEndpointPosition(**{**_position_values(), "quantity": 1.0}),),
            (),
        ),
        (
            (),
            (MaliciousEndpointOrder(**{**_order_values(), "quantity": 1.0}),),
        ),
    ],
)
def test_builder_rejects_endpoint_subclasses_without_reprojection(
    positions: tuple[object, ...], working_orders: tuple[object, ...]
) -> None:
    with pytest.raises(ValueError, match="subclasses are unsupported"):
        build_replay_endpoint_state(
            positions=positions,
            working_orders=working_orders,
        )


@pytest.mark.parametrize(
    ("field", "malicious", "safe"),
    [
        (
            "positions",
            MaliciousEndpointPosition.model_validate(
                {**_position_values(), "quantity": 1.0}
            ),
            EndpointPosition.model_validate(_position_values()),
        ),
        (
            "working_orders",
            MaliciousEndpointOrder.model_validate({**_order_values(), "quantity": 1.0}),
            EndpointOrder.model_validate(_order_values()),
        ),
    ],
)
def test_parent_rejects_tuple_subclass_that_hides_malicious_stored_item(
    field: str,
    malicious: EndpointPosition | EndpointOrder,
    safe: EndpointPosition | EndpointOrder,
) -> None:
    container = AdversarialTuple(malicious, safe)
    assert isinstance(malicious.quantity, float)
    assert container[0] is malicious
    assert tuple(container) == (safe,)

    with pytest.raises(ValidationError) as exc_info:
        ReplayEndpointState.model_validate({field: container})
    assert {error["loc"] for error in exc_info.value.errors()} == {(field,)}


@pytest.mark.parametrize(
    ("protected_side", "closing_side"),
    [("LONG", OrderSide.SELL), ("SHORT", OrderSide.BUY)],
)
def test_rust_conditional_side_is_converted_to_closing_action_side(
    protected_side: str,
    closing_side: OrderSide,
) -> None:
    order = SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side=protected_side,
        order_type="STOP_LOSS",
        quantity="1",
        timestamp=100,
        price="0",
        trigger_price="20900",
        trailing_distance=None,
    )

    state = build_replay_endpoint_state(
        positions=(),
        working_orders=(order,),
        final_mark=Decimal("21000"),
        end_timestamp=200,
    )

    assert state.working_orders[0].side == closing_side


def test_protection_orders_are_derived_and_serialized() -> None:
    market = EndpointOrder(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side=OrderSide.BUY,
        order_type="MARKET",
        quantity=Decimal("1"),
        timestamp=100,
    )
    stop = EndpointOrder(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side=OrderSide.SELL,
        order_type="STOP_LOSS",
        quantity=Decimal("1"),
        timestamp=101,
        trigger_price=Decimal("20900"),
    )
    state = ReplayEndpointState(
        working_orders=(market, stop),
        final_mark=Decimal("21000"),
        end_timestamp=200,
    )

    assert state.protection_orders == (stop,)
    serialized = state.model_dump(mode="json")
    assert serialized["protection_orders"] == [
        {
            "strategy_id": "alpha",
            "product_id": "RITHMIC:MNQ-202609",
            "side": "sell",
            "order_type": "STOP_LOSS",
            "quantity": "1",
            "timestamp": 101,
            "price": None,
            "trigger_price": "20900",
            "trailing_distance": None,
        }
    ]


def test_builder_sorts_semantic_state_and_omits_random_ids() -> None:
    first = _orm_order(
        id="random-a",
        strategy_id="zeta",
        side="sell",
        timestamp=200,
    )
    second = _orm_order(
        id="random-b",
        strategy_id="alpha",
        side="buy",
        timestamp=100,
    )

    forward = build_replay_endpoint_state(
        positions=(),
        working_orders=(first, second),
        final_mark=Decimal("21000"),
        end_timestamp=300,
    )
    reverse = build_replay_endpoint_state(
        positions=(),
        working_orders=(second, first),
        final_mark=Decimal("21000"),
        end_timestamp=300,
    )

    assert forward == reverse
    assert [item.strategy_id for item in forward.working_orders] == ["alpha", "zeta"]
    assert "random-a" not in str(forward.model_dump())
    assert "random-b" not in str(forward.model_dump())


def test_equivalent_decimal_scales_have_identical_json_projection() -> None:
    scaled_position = SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side="LONG",
        quantity="1.00000000",
        entry_price="21000.5000",
    )
    canonical_position = SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side="LONG",
        quantity="1",
        entry_price="21000.5",
    )
    scaled_stop = SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side="LONG",
        order_type="TRAILING_STOP",
        quantity="1.0000",
        timestamp=100,
        price="0.0000",
        trigger_price="20990.5000",
        trailing_distance="10.0000",
    )
    canonical_stop = SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side="LONG",
        order_type="TRAILING_STOP",
        quantity="1",
        timestamp=100,
        price="0",
        trigger_price="20990.5",
        trailing_distance="10",
    )

    scaled = build_replay_endpoint_state(
        positions=(scaled_position,),
        working_orders=(scaled_stop,),
        final_mark="21001.5000",
        end_timestamp=200,
    )
    canonical = build_replay_endpoint_state(
        positions=(canonical_position,),
        working_orders=(canonical_stop,),
        final_mark="21001.5",
        end_timestamp=200,
    )

    assert scaled.model_dump(mode="json") == canonical.model_dump(mode="json")


def test_direct_models_canonicalize_decimal_scale_before_nested_serialization() -> None:
    scaled_position = EndpointPosition.model_validate(
        {
            **_position_values(),
            "quantity": Decimal("1.00"),
            "average_entry_price": Decimal("21000.5000"),
        }
    )
    canonical_position = EndpointPosition.model_validate(
        {**_position_values(), "average_entry_price": Decimal("21000.5")}
    )
    scaled_order = EndpointOrder.model_validate(
        {
            **_order_values(),
            "quantity": Decimal("1.00"),
            "price": Decimal("21000.00"),
            "trigger_price": Decimal("20900.00"),
            "trailing_distance": Decimal("10.00"),
        }
    )
    canonical_order = EndpointOrder.model_validate(_order_values())

    assert scaled_position.model_dump(mode="json") == canonical_position.model_dump(
        mode="json"
    )
    assert scaled_order.model_dump(mode="json") == canonical_order.model_dump(
        mode="json"
    )
    scaled_state = ReplayEndpointState(
        positions=(scaled_position,),
        working_orders=(scaled_order,),
        final_mark=Decimal("21001.5000"),
        end_timestamp=200,
    )
    canonical_state = ReplayEndpointState(
        positions=(canonical_position,),
        working_orders=(canonical_order,),
        final_mark=Decimal("21001.5"),
        end_timestamp=200,
    )
    assert scaled_state.model_dump(mode="json") == canonical_state.model_dump(
        mode="json"
    )

    built = build_replay_endpoint_state(
        positions=(scaled_position,),
        working_orders=(scaled_order,),
        final_mark=Decimal("21001.5000"),
        end_timestamp=200,
    )
    assert built.model_dump(mode="json") == canonical_state.model_dump(mode="json")


def test_empty_endpoint_state_has_no_final_observation() -> None:
    state = build_replay_endpoint_state(positions=(), working_orders=())

    assert state == ReplayEndpointState()
    assert state.positions == ()
    assert state.working_orders == ()
    assert state.protection_orders == ()


def test_endpoint_state_reports_finite_zero_and_negative_prices() -> None:
    position = SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side="LONG",
        quantity="1",
        entry_price="-1",
    )
    stop = SimpleNamespace(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side="LONG",
        order_type="STOP_LOSS",
        quantity="1",
        timestamp=100,
        price="0",
        trigger_price="-2",
        trailing_distance=None,
    )

    state = build_replay_endpoint_state(
        positions=(position,),
        working_orders=(stop,),
        final_mark=Decimal("0"),
        end_timestamp=200,
    )

    assert state.positions[0].average_entry_price == Decimal("-1")
    assert state.working_orders[0].price is None
    assert state.working_orders[0].trigger_price == Decimal("-2")
    assert state.final_mark == Decimal("0")


def test_models_are_frozen_and_validate_invariants() -> None:
    position = EndpointPosition(
        strategy_id="alpha",
        product_id="RITHMIC:MNQ-202609",
        side=PositionSide.LONG,
        quantity=Decimal("1"),
        average_entry_price=Decimal("21000"),
    )

    with pytest.raises(ValidationError, match="frozen"):
        position.quantity = Decimal("2")
    with pytest.raises(ValidationError, match="finite"):
        EndpointPosition(
            strategy_id="alpha",
            product_id="RITHMIC:MNQ-202609",
            side=PositionSide.LONG,
            quantity=Decimal("NaN"),
            average_entry_price=Decimal("21000"),
        )
    with pytest.raises(ValidationError, match="provided together"):
        ReplayEndpointState(final_mark=Decimal("21000"))
    with pytest.raises(ValidationError, match="finite"):
        EndpointOrder(
            strategy_id="alpha",
            product_id="RITHMIC:MNQ-202609",
            side=OrderSide.SELL,
            order_type="STOP_LOSS",
            quantity=Decimal("1"),
            timestamp=100,
            trigger_price=Decimal("NaN"),
        )
    with pytest.raises(ValidationError, match="trailing distance"):
        EndpointOrder(
            strategy_id="alpha",
            product_id="RITHMIC:MNQ-202609",
            side=OrderSide.SELL,
            order_type="TRAILING_STOP",
            quantity=Decimal("1"),
            timestamp=100,
            trailing_distance=Decimal("0"),
        )


@pytest.mark.parametrize(
    ("model", "required", "defaults"),
    [
        (
            EndpointPosition,
            {
                "strategy_id",
                "product_id",
                "side",
                "quantity",
                "average_entry_price",
            },
            {},
        ),
        (
            EndpointOrder,
            {
                "strategy_id",
                "product_id",
                "side",
                "order_type",
                "quantity",
                "timestamp",
            },
            {"price": None, "trigger_price": None, "trailing_distance": None},
        ),
        (
            ReplayEndpointState,
            set(),
            {
                "positions": (),
                "working_orders": (),
                "final_mark": None,
                "end_timestamp": None,
                "halted_early": False,
            },
        ),
    ],
)
def test_endpoint_field_missing_and_default_disposition(
    model: type[EndpointPosition] | type[EndpointOrder] | type[ReplayEndpointState],
    required: set[str],
    defaults: dict[str, object],
) -> None:
    assert {
        name for name, field in model.model_fields.items() if field.is_required()
    } == required
    assert {
        name: field.default
        for name, field in model.model_fields.items()
        if not field.is_required()
    } == defaults


INVALID_MONEY = [0.5, 1, True, Decimal("NaN"), Decimal("Infinity")]
EndpointModel = type[EndpointPosition] | type[EndpointOrder] | type[ReplayEndpointState]


@pytest.mark.parametrize(
    ("model", "values", "field"),
    [
        (EndpointPosition, _position_values, "quantity"),
        (EndpointPosition, _position_values, "average_entry_price"),
        (EndpointOrder, _order_values, "quantity"),
        (EndpointOrder, _order_values, "price"),
        (EndpointOrder, _order_values, "trigger_price"),
        (EndpointOrder, _order_values, "trailing_distance"),
    ],
)
@pytest.mark.parametrize("invalid", INVALID_MONEY)
def test_direct_models_reject_every_noncanonical_money_value(
    model: type[EndpointPosition] | type[EndpointOrder],
    values: Callable[[], dict[str, object]],
    field: str,
    invalid: object,
) -> None:
    model_values = values()
    model_values[field] = invalid
    with pytest.raises(ValidationError):
        model.model_validate(model_values)


@pytest.mark.parametrize("invalid", INVALID_MONEY)
def test_replay_state_rejects_noncanonical_final_mark(invalid: object) -> None:
    with pytest.raises(ValidationError):
        ReplayEndpointState.model_validate(
            {"final_mark": invalid, "end_timestamp": 100}
        )


@pytest.mark.parametrize(
    "values",
    [
        {"positions": [_position_values()]},
        {"positions": (_position_values(),)},
        {"working_orders": [_order_values()]},
        {"working_orders": (_order_values(),)},
        {"end_timestamp": "100", "final_mark": Decimal("21000")},
        {"halted_early": "false"},
        {"unexpected": "value"},
    ],
)
def test_replay_state_rejects_container_mapping_and_scalar_coercion(
    values: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        ReplayEndpointState.model_validate(values)


@pytest.mark.parametrize(
    ("model", "values"),
    [
        (EndpointPosition, {**_position_values(), "side": "LONG"}),
        (EndpointPosition, {**_position_values(), "unexpected": "value"}),
        (EndpointOrder, {**_order_values(), "side": "buy"}),
        (EndpointOrder, {**_order_values(), "order_type": "limit"}),
        (EndpointOrder, {**_order_values(), "timestamp": "100"}),
        (EndpointOrder, {**_order_values(), "timestamp": True}),
        (EndpointOrder, {**_order_values(), "unexpected": "value"}),
    ],
)
def test_nested_models_reject_noncanonical_direct_values(
    model: type[EndpointPosition] | type[EndpointOrder],
    values: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        model.model_validate(values)


STRICT_FALSE_CASES: list[tuple[EndpointModel, dict[str, object], tuple[str, ...]]] = [
    (EndpointPosition, {**_position_values(), "strategy_id": 1}, ("strategy_id",)),
    (EndpointPosition, {**_position_values(), "product_id": 1}, ("product_id",)),
    (EndpointPosition, {**_position_values(), "side": "LONG"}, ("side",)),
    *[
        (EndpointPosition, {**_position_values(), field: value}, (field,))
        for field in ("quantity", "average_entry_price")
        for value in ("1", 1, 1.0, True)
    ],
    (EndpointPosition, {**_position_values(), "extra": 1}, ()),
    (EndpointOrder, {**_order_values(), "strategy_id": 1}, ("strategy_id",)),
    (EndpointOrder, {**_order_values(), "product_id": 1}, ("product_id",)),
    (EndpointOrder, {**_order_values(), "side": "buy"}, ("side",)),
    (EndpointOrder, {**_order_values(), "order_type": 1}, ("order_type",)),
    *[
        (EndpointOrder, {**_order_values(), field: value}, (field,))
        for field in ("quantity", "price", "trigger_price", "trailing_distance")
        for value in ("1", 1, 1.0, True)
    ],
    (EndpointOrder, {**_order_values(), "timestamp": "100"}, ("timestamp",)),
    (EndpointOrder, {**_order_values(), "timestamp": True}, ("timestamp",)),
    (EndpointOrder, {**_order_values(), "extra": 1}, ()),
    (ReplayEndpointState, {"positions": [_position_values()]}, ("positions",)),
    (ReplayEndpointState, {"positions": (_position_values(),)}, ("positions",)),
    (ReplayEndpointState, {"working_orders": [_order_values()]}, ("working_orders",)),
    (ReplayEndpointState, {"working_orders": (_order_values(),)}, ("working_orders",)),
    *[
        (
            ReplayEndpointState,
            {"final_mark": value, "end_timestamp": 100},
            ("final_mark",),
        )
        for value in ("1", 1, 1.0, True)
    ],
    (
        ReplayEndpointState,
        {"final_mark": Decimal("1"), "end_timestamp": "100"},
        ("end_timestamp",),
    ),
    (
        ReplayEndpointState,
        {"final_mark": Decimal("1"), "end_timestamp": True},
        ("end_timestamp",),
    ),
    (ReplayEndpointState, {"halted_early": "false"}, ("halted_early",)),
    (ReplayEndpointState, {"halted_early": 1}, ("halted_early",)),
    (ReplayEndpointState, {"extra": 1}, ()),
]


@pytest.mark.parametrize(("model", "values", "expected_location"), STRICT_FALSE_CASES)
def test_call_time_strict_false_cannot_bypass_raw_canonical_types(
    model: EndpointModel,
    values: dict[str, object],
    expected_location: tuple[str, ...],
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        model.model_validate(values, strict=False, extra="allow")
    assert expected_location in {error["loc"] for error in exc_info.value.errors()}


@pytest.mark.parametrize("with_unknown_attribute", [False, True])
@pytest.mark.parametrize(
    ("model", "source"),
    [
        (EndpointPosition, SimpleNamespace(**_position_values())),
        (EndpointOrder, SimpleNamespace(**_order_values())),
        (
            ReplayEndpointState,
            SimpleNamespace(
                positions=(),
                working_orders=(),
                final_mark=None,
                end_timestamp=None,
                halted_early=False,
            ),
        ),
    ],
)
def test_from_attributes_cannot_bypass_mapping_boundary(
    model: EndpointModel,
    source: SimpleNamespace,
    with_unknown_attribute: bool,
) -> None:
    source = SimpleNamespace(**vars(source))
    if with_unknown_attribute:
        source.unexpected = "value"
    with pytest.raises(ValidationError) as exc_info:
        model.model_validate(source, from_attributes=True)
    assert {error["loc"] for error in exc_info.value.errors()} == {()}


JSON_CASES: list[tuple[EndpointModel, dict[str, object], tuple[str, ...]]] = [
    (EndpointPosition, {**_position_values(), "strategy_id": 1}, ("strategy_id",)),
    (EndpointPosition, {**_position_values(), "product_id": 1}, ("product_id",)),
    (EndpointPosition, _position_values(), ("side",)),
    *[
        (EndpointPosition, {**_position_values(), field: value}, (field,))
        for field in ("quantity", "average_entry_price")
        for value in ("1", 1, 1.0, True)
    ],
    (EndpointPosition, {**_position_values(), "extra": 1}, ()),
    (EndpointOrder, {**_order_values(), "strategy_id": 1}, ("strategy_id",)),
    (EndpointOrder, {**_order_values(), "product_id": 1}, ("product_id",)),
    (EndpointOrder, _order_values(), ("side",)),
    (EndpointOrder, {**_order_values(), "order_type": 1}, ("order_type",)),
    *[
        (EndpointOrder, {**_order_values(), field: value}, (field,))
        for field in ("quantity", "price", "trigger_price", "trailing_distance")
        for value in ("1", 1, 1.0, True)
    ],
    (EndpointOrder, {**_order_values(), "timestamp": "100"}, ("timestamp",)),
    (EndpointOrder, {**_order_values(), "timestamp": True}, ("timestamp",)),
    (EndpointOrder, {**_order_values(), "extra": 1}, ()),
    (ReplayEndpointState, {"positions": []}, ("positions",)),
    (ReplayEndpointState, {"working_orders": []}, ("working_orders",)),
    *[
        (
            ReplayEndpointState,
            {"final_mark": value, "end_timestamp": 100},
            ("final_mark",),
        )
        for value in ("1", 1, 1.0, True)
    ],
    (
        ReplayEndpointState,
        {"final_mark": None, "end_timestamp": "100"},
        ("end_timestamp",),
    ),
    (
        ReplayEndpointState,
        {"final_mark": None, "end_timestamp": True},
        ("end_timestamp",),
    ),
    (ReplayEndpointState, {"halted_early": "false"}, ("halted_early",)),
    (ReplayEndpointState, {"halted_early": 1}, ("halted_early",)),
    (ReplayEndpointState, {"extra": 1}, ()),
]


def _json_ready(values: dict[str, object]) -> str:
    return json.dumps(
        values,
        default=lambda value: value.value if hasattr(value, "value") else str(value),
    )


@pytest.mark.parametrize(("model", "values", "expected_location"), JSON_CASES)
def test_json_validation_cannot_normalize_external_values(
    model: EndpointModel,
    values: dict[str, object],
    expected_location: tuple[str, ...],
) -> None:
    with pytest.raises(ValidationError) as exc_info:
        model.model_validate_json(_json_ready(values), extra="ignore")
    assert expected_location in {error["loc"] for error in exc_info.value.errors()}


@pytest.mark.parametrize("invalid", [0.5, 1, True])
@pytest.mark.parametrize(
    ("source_factory", "field"),
    [
        (_rust_position, "quantity"),
        (_rust_position, "entry_price"),
        (_rust_order, "quantity"),
        (_rust_order, "price"),
        (_rust_order, "trigger_price"),
        (_rust_order, "trailing_distance"),
    ],
)
def test_builder_rejects_noncanonical_source_money(
    source_factory: Callable[[], SimpleNamespace], field: str, invalid: object
) -> None:
    source = source_factory()
    setattr(source, field, invalid)
    if hasattr(source, "entry_price"):
        with pytest.raises(ValueError, match="decimal value"):
            build_replay_endpoint_state(positions=(source,), working_orders=())
    else:
        with pytest.raises(ValueError, match="decimal value"):
            build_replay_endpoint_state(positions=(), working_orders=(source,))


@pytest.mark.parametrize("invalid", [0.5, 1, True])
def test_builder_rejects_noncanonical_final_mark(invalid: object) -> None:
    with pytest.raises(ValueError, match="decimal value"):
        build_replay_endpoint_state(
            positions=(), working_orders=(), final_mark=invalid, end_timestamp=100
        )


@pytest.mark.parametrize(
    "order",
    [
        SimpleNamespace(
            strategy_id="alpha",
            product_id="RITHMIC:MNQ-202609",
            side="FLAT",
            order_type="MARKET",
            quantity="1",
            timestamp=100,
            price="0",
        ),
        SimpleNamespace(
            strategy_id="alpha",
            product_id="RITHMIC:MNQ-202609",
            side="LONG",
            order_type="PEGGED",
            quantity="1",
            timestamp=100,
            price="21000",
        ),
        SimpleNamespace(
            strategy_id="alpha",
            product_id="RITHMIC:MNQ-202609",
            side="LONG",
            quantity="1",
            timestamp=100,
            price="21000",
        ),
        SimpleNamespace(
            strategy_id=None,
            product_id="RITHMIC:MNQ-202609",
            side="LONG",
            order_type="MARKET",
            quantity="1",
            timestamp=100,
            price="0",
        ),
    ],
)
def test_unknown_or_incomplete_order_shape_fails_loud(order: object) -> None:
    with pytest.raises(
        ValueError, match="unsupported endpoint-state source|unsupported|must be"
    ):
        build_replay_endpoint_state(positions=(), working_orders=(order,))
