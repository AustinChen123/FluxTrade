from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from src.core.execution_conditional_orders import create_conditional_orders
from src.core.models import Candlestick, OrderSide, OrderStatus, SignalType


@pytest.mark.parametrize(
    ("entry_side", "expected_side"),
    [("buy", OrderSide.SELL), ("sell", OrderSide.BUY)],
)
def test_materializes_all_intents_with_exact_identity_and_oco_persistence(
    signal_factory,
    entry_side,
    expected_side,
):
    signal = signal_factory(
        signal_type=SignalType.LONG,
        stop_loss=Decimal("99.00"),
        take_profit=Decimal("101.00"),
        trailing_distance=Decimal("0.50"),
    )
    entry = SimpleNamespace(
        id="entry-1",
        side=entry_side,
        client_order_id="strategy-worker-entry-1704067200000000000",
    )
    orders = []
    manager = MagicMock()

    def create_order(**kwargs):
        order = SimpleNamespace(
            id=f"order-{len(orders) + 1}",
            type=kwargs["order_type"],
            status=None,
            exchange_order_id="unexpected",
            intent_payload=None,
        )
        orders.append(order)
        return order

    manager.create_order.side_effect = create_order
    attach_reference = MagicMock()
    candle = Candlestick(
        product_id=signal.product_id,
        timeframe=signal.timeframe,
        timestamp=signal.timestamp,
        open=Decimal("100.00"),
        high=Decimal("100.00"),
        low=Decimal("100.00"),
        close=Decimal("100.00"),
        volume=Decimal("1"),
    )

    result = create_conditional_orders(
        repository=manager.repo,
        create_order=manager.create_order,
        signal=signal,
        entry_order=entry,
        quantity=Decimal("2.00"),
        candle=candle,
        attach_min_notional_reference_price=attach_reference,
    )

    assert result == orders
    assert [order.type for order in orders] == [
        "stop_loss",
        "take_profit",
        "trailing_stop",
    ]
    assert manager.create_order.call_args_list == [
        call(
            signal=signal,
            side=expected_side,
            order_type="stop_loss",
            quantity=Decimal("2.00"),
            trigger_price=Decimal("99.00"),
            client_order_id="strategy-worker-sl-1704067200000000000",
        ),
        call(
            signal=signal,
            side=expected_side,
            order_type="take_profit",
            quantity=Decimal("2.00"),
            trigger_price=Decimal("101.00"),
            client_order_id="strategy-worker-tp-1704067200000000000",
        ),
        call(
            signal=signal,
            side=expected_side,
            order_type="trailing_stop",
            quantity=Decimal("2.00"),
            trigger_price=Decimal("99.00"),
            client_order_id="strategy-worker-tr-1704067200000000000",
        ),
    ]
    assert orders[2]._trailing_distance == Decimal("0.50")
    assert not hasattr(orders[0], "_trailing_distance")
    assert not hasattr(orders[1], "_trailing_distance")
    assert orders[0].intent_payload == {
        "pending_entry_order_id": "entry-1",
        "linked_order_id": "order-2",
        "placement_mode": "place-after-fill",
    }
    assert orders[1].intent_payload == {
        "pending_entry_order_id": "entry-1",
        "linked_order_id": "order-1",
        "placement_mode": "place-after-fill",
    }
    assert orders[2].intent_payload == {
        "pending_entry_order_id": "entry-1",
        "linked_order_id": None,
        "placement_mode": "place-after-fill",
    }
    assert {order.status for order in orders} == {OrderStatus.NEW.value}
    assert {order.exchange_order_id for order in orders} == {None}
    assert attach_reference.call_args_list == [
        call(orders[0], candle),
        call(orders[1], candle),
        call(orders[2], candle),
    ]
    assert manager.repo.persist_conditional_order.call_args_list == [
        call(orders[0]),
        call(orders[1]),
        call(orders[2]),
    ]


def test_empty_intents_create_and_persist_nothing(signal_factory):
    signal = signal_factory(signal_type=SignalType.LONG)
    manager = MagicMock()
    attach_reference = MagicMock()

    result = create_conditional_orders(
        repository=manager.repo,
        create_order=manager.create_order,
        signal=signal,
        entry_order=SimpleNamespace(
            id="entry-1",
            side="buy",
            client_order_id=None,
        ),
        quantity=Decimal("1"),
        candle=None,
        attach_min_notional_reference_price=attach_reference,
    )

    assert result == []
    manager.create_order.assert_not_called()
    manager.repo.persist_conditional_order.assert_not_called()
    attach_reference.assert_not_called()


def test_missing_entry_client_id_remains_none(signal_factory):
    signal = signal_factory(
        signal_type=SignalType.LONG,
        stop_loss=Decimal("99"),
    )
    order = SimpleNamespace(
        id="stop-1",
        type="stop_loss",
        status=None,
        exchange_order_id=None,
        intent_payload=None,
    )
    manager = MagicMock()
    manager.create_order.return_value = order

    create_conditional_orders(
        repository=manager.repo,
        create_order=manager.create_order,
        signal=signal,
        entry_order=SimpleNamespace(id="entry-1", side="buy", client_order_id=None),
        quantity=Decimal("1"),
        candle=None,
        attach_min_notional_reference_price=MagicMock(),
    )

    assert manager.create_order.call_args.kwargs["client_order_id"] is None


def test_creation_failure_preserves_partial_rows_without_persistence(signal_factory):
    signal = signal_factory(
        signal_type=SignalType.LONG,
        stop_loss=Decimal("99"),
        take_profit=Decimal("101"),
        trailing_distance=Decimal("0.50"),
    )
    first = SimpleNamespace(id="stop-1", type="stop_loss")
    error = RuntimeError("create sentinel")
    manager = MagicMock()
    manager.create_order.side_effect = [first, error]

    with pytest.raises(RuntimeError) as raised:
        create_conditional_orders(
            repository=manager.repo,
            create_order=manager.create_order,
            signal=signal,
            entry_order=SimpleNamespace(
                id="entry-1",
                side="buy",
                client_order_id="strategy-worker-entry-1704067200000000000",
            ),
            quantity=Decimal("1"),
            candle=None,
            attach_min_notional_reference_price=MagicMock(),
        )

    assert raised.value is error
    assert manager.create_order.call_count == 2
    manager.repo.persist_conditional_order.assert_not_called()
    assert not hasattr(first, "intent_payload")


def test_persistence_failure_preserves_completed_creates_and_stops(signal_factory):
    signal = signal_factory(
        signal_type=SignalType.LONG,
        stop_loss=Decimal("99"),
        take_profit=Decimal("101"),
        trailing_distance=Decimal("0.50"),
    )
    orders = [
        SimpleNamespace(id="stop-1", type="stop_loss"),
        SimpleNamespace(id="target-1", type="take_profit"),
        SimpleNamespace(id="trailing-1", type="trailing_stop"),
    ]
    error = RuntimeError("persist sentinel")
    manager = MagicMock()
    manager.create_order.side_effect = orders
    manager.repo.persist_conditional_order.side_effect = [None, error]

    with pytest.raises(RuntimeError) as raised:
        create_conditional_orders(
            repository=manager.repo,
            create_order=manager.create_order,
            signal=signal,
            entry_order=SimpleNamespace(
                id="entry-1",
                side="buy",
                client_order_id="strategy-worker-entry-1704067200000000000",
            ),
            quantity=Decimal("1"),
            candle=None,
            attach_min_notional_reference_price=MagicMock(),
        )

    assert raised.value is error
    assert manager.create_order.call_count == 3
    assert manager.repo.persist_conditional_order.call_args_list == [
        call(orders[0]),
        call(orders[1]),
    ]
    assert not hasattr(orders[2], "status")
