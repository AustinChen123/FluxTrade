"""Invariant tests for backtest account and Rust matcher position state."""

from decimal import Decimal

from src.core.adapters.simulated import SimulatedAdapter
from src.core.mocks.account_service import BacktestAccountService
from src.core.models import Candlestick


PRODUCT = "BINANCE:BTCUSDT-PERP"
STRATEGY_ID = "invariant_strategy"
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


def _place_market_order(adapter, order_factory, *, side: str, quantity: str, ts: int) -> None:
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

    assert len(fills) == 1, f"expected one fill for {side} {quantity} at ts={ts}"
    assert fills[0]["order"].id == order.id


def _assert_account_matches_rust(account: BacktestAccountService, adapter: SimulatedAdapter) -> None:
    raw_position = adapter._engine.positions.get(f"{STRATEGY_ID}:{PRODUCT}")
    account_position = account.get_position(STRATEGY_ID, PRODUCT)

    if raw_position is None:
        assert account_position is None
        return

    assert account_position is not None
    assert account_position.strategy_id == STRATEGY_ID
    assert account_position.product_id == PRODUCT
    assert account_position.side == raw_position.side
    assert account_position.quantity == Decimal(raw_position.quantity)
    assert account_position.entry_price == Decimal(raw_position.entry_price)
    assert account_position.unrealized_pnl == Decimal(raw_position.unrealized_pnl)


def test_backtest_account_position_matches_rust_after_each_fill(order_factory) -> None:
    adapter = SimulatedAdapter(Decimal("100000"))
    account = BacktestAccountService(adapter=adapter)

    _place_market_order(adapter, order_factory, side="buy", quantity="0.1", ts=1)
    _assert_account_matches_rust(account, adapter)
    assert account.get_position(STRATEGY_ID, PRODUCT).side == "LONG"
    assert account.get_position(STRATEGY_ID, PRODUCT).quantity == Decimal("0.1")

    _place_market_order(adapter, order_factory, side="buy", quantity="0.2", ts=2)
    _assert_account_matches_rust(account, adapter)
    assert account.get_position(STRATEGY_ID, PRODUCT).side == "LONG"
    assert account.get_position(STRATEGY_ID, PRODUCT).quantity == Decimal("0.3")

    _place_market_order(adapter, order_factory, side="sell", quantity="0.1", ts=3)
    _assert_account_matches_rust(account, adapter)
    assert account.get_position(STRATEGY_ID, PRODUCT).side == "LONG"
    assert account.get_position(STRATEGY_ID, PRODUCT).quantity == Decimal("0.2")

    _place_market_order(adapter, order_factory, side="sell", quantity="0.3", ts=4)
    _assert_account_matches_rust(account, adapter)
    assert account.get_position(STRATEGY_ID, PRODUCT).side == "SHORT"
    assert account.get_position(STRATEGY_ID, PRODUCT).quantity == Decimal("0.1")

    _place_market_order(adapter, order_factory, side="buy", quantity="0.1", ts=5)
    _assert_account_matches_rust(account, adapter)
    assert account.get_position(STRATEGY_ID, PRODUCT) is None
