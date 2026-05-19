"""Invariant tests for order side to matcher position-side conventions."""

from decimal import Decimal

from src.core.adapters.simulated import SimulatedAdapter
from src.core.models import Candlestick, PositionSide


PRODUCT = "BINANCE:BTCUSDT-PERP"
STRATEGY_ID = "position_sign_strategy"
TF = "15m"


def _candle(ts: int, price: str) -> Candlestick:
    value = Decimal(price)
    return Candlestick(
        product_id=PRODUCT,
        timeframe=TF,
        timestamp=ts,
        open=value,
        high=value,
        low=value,
        close=value,
        volume=Decimal("100"),
    )


def _fill_market(adapter, order_factory, *, side: str, quantity: str, ts: int) -> None:
    order = order_factory(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT,
        order_type="market",
        side=side,
        quantity=Decimal(quantity),
        timestamp=ts,
    )

    adapter.place_order(order)
    fills = adapter.on_market_data(_candle(ts + 1, str(50000 + ts)))

    assert len(fills) == 1
    assert fills[0]["order"].id == order.id


def test_order_side_boundary_converts_to_matcher_position_side(order_factory) -> None:
    adapter = SimulatedAdapter()

    buy = order_factory(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT,
        order_type="market",
        side="buy",
    )
    sell = order_factory(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT,
        order_type="market",
        side="sell",
    )
    long_stop = order_factory(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT,
        order_type="stop_loss",
        side="sell",
        trigger_price=Decimal("49000"),
    )
    short_stop = order_factory(
        strategy_id=STRATEGY_ID,
        product_id=PRODUCT,
        order_type="stop_loss",
        side="buy",
        trigger_price=Decimal("51000"),
    )

    assert adapter._to_rust_order(buy).side == PositionSide.LONG
    assert adapter._to_rust_order(sell).side == PositionSide.SHORT
    assert adapter._to_rust_order(long_stop).side == PositionSide.LONG
    assert adapter._to_rust_order(short_stop).side == PositionSide.SHORT


def test_long_position_reduces_and_reverses_with_sell(order_factory) -> None:
    adapter = SimulatedAdapter(Decimal("100000"))

    _fill_market(adapter, order_factory, side="buy", quantity="0.2", ts=1)
    position = adapter.get_position(PRODUCT, strategy_id=STRATEGY_ID)
    assert position is not None
    assert position.side == PositionSide.LONG
    assert position.quantity == Decimal("0.2")

    _fill_market(adapter, order_factory, side="sell", quantity="0.05", ts=2)
    position = adapter.get_position(PRODUCT, strategy_id=STRATEGY_ID)
    assert position is not None
    assert position.side == PositionSide.LONG
    assert position.quantity == Decimal("0.15")

    _fill_market(adapter, order_factory, side="sell", quantity="0.2", ts=3)
    position = adapter.get_position(PRODUCT, strategy_id=STRATEGY_ID)
    assert position is not None
    assert position.side == PositionSide.SHORT
    assert position.quantity == Decimal("0.05")


def test_short_position_reduces_and_reverses_with_buy(order_factory) -> None:
    adapter = SimulatedAdapter(Decimal("100000"))

    _fill_market(adapter, order_factory, side="sell", quantity="0.2", ts=1)
    position = adapter.get_position(PRODUCT, strategy_id=STRATEGY_ID)
    assert position is not None
    assert position.side == PositionSide.SHORT
    assert position.quantity == Decimal("0.2")

    _fill_market(adapter, order_factory, side="buy", quantity="0.05", ts=2)
    position = adapter.get_position(PRODUCT, strategy_id=STRATEGY_ID)
    assert position is not None
    assert position.side == PositionSide.SHORT
    assert position.quantity == Decimal("0.15")

    _fill_market(adapter, order_factory, side="buy", quantity="0.2", ts=3)
    position = adapter.get_position(PRODUCT, strategy_id=STRATEGY_ID)
    assert position is not None
    assert position.side == PositionSide.LONG
    assert position.quantity == Decimal("0.05")
