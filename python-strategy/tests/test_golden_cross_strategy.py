from decimal import Decimal

import pytest

from src.core.models import Candlestick, SignalType
from src.strategies.golden_cross import GoldenCrossStrategy


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "15m"


def _candle(index: int, close: str) -> Candlestick:
    price = Decimal(close)
    return Candlestick(
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        timestamp=1_700_000_000_000 + index * 60_000,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("1"),
    )


def test_golden_cross_strategy_emits_market_entry_and_exit():
    strategy = GoldenCrossStrategy(
        strategy_id="golden",
        product_id=PRODUCT_ID,
        short_window=2,
        long_window=3,
        timeframe=TIMEFRAME,
        quantity=Decimal("0.02"),
    )

    signals = [
        strategy.on_candle(_candle(index, close))
        for index, close in enumerate(["5", "4", "3", "6", "1", "1"])
    ]

    assert [signal.type for signal in signals] == [
        SignalType.NO_SIGNAL,
        SignalType.NO_SIGNAL,
        SignalType.NO_SIGNAL,
        SignalType.LONG,
        SignalType.NO_SIGNAL,
        SignalType.EXIT_LONG,
    ]
    assert signals[3].quantity == Decimal("0.02")
    assert signals[3].value is None
    assert signals[3].price is None
    assert signals[3].metadata["sma_short"] == "4.5"
    assert signals[5].quantity == Decimal("0.02")
    assert signals[5].value is None
    assert signals[5].price is None


def test_golden_cross_strategy_ignores_exit_before_entry():
    strategy = GoldenCrossStrategy(
        strategy_id="golden",
        product_id=PRODUCT_ID,
        short_window=2,
        long_window=3,
        timeframe=TIMEFRAME,
        quantity=Decimal("0.02"),
    )

    signals = [
        strategy.on_candle(_candle(index, close))
        for index, close in enumerate(["1", "2", "3", "0"])
    ]

    assert signals[-1].type == SignalType.NO_SIGNAL
    assert signals[-1].quantity is None


def test_golden_cross_strategy_uses_configured_timeframe_requirement():
    strategy = GoldenCrossStrategy(
        strategy_id="golden",
        product_id=PRODUCT_ID,
        short_window=2,
        long_window=3,
        timeframe=TIMEFRAME,
    )

    requirements = strategy.requirements

    assert requirements.product_id == PRODUCT_ID
    assert requirements.timeframe == TIMEFRAME
    assert requirements.lookback_window == 3


def test_golden_cross_strategy_rejects_invalid_parameters():
    with pytest.raises(ValueError, match="short_window must be smaller"):
        GoldenCrossStrategy(
            strategy_id="golden",
            product_id=PRODUCT_ID,
            short_window=3,
            long_window=3,
        )

    with pytest.raises(ValueError, match="quantity must be positive"):
        GoldenCrossStrategy(
            strategy_id="golden",
            product_id=PRODUCT_ID,
            short_window=2,
            long_window=3,
            quantity=Decimal("0"),
        )
