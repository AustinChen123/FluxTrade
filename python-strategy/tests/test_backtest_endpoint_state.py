from decimal import Decimal
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from src.core.backtest.endpoint_state import (
    EndpointOrder,
    EndpointPosition,
    ReplayEndpointState,
    build_replay_endpoint_state,
)
from src.core.models import OrderSide, Position, PositionSide
from src.core.orm_models import Order


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
