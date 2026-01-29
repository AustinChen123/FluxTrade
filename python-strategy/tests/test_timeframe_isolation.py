"""Tests for Phase 4: Timeframe Channel Isolation.

Covers:
- build_stream_channels() derives correct Redis stream keys from strategy requirements
- Timeframe guard in on_market_data() filters mismatched candles
- Trades are not affected by the timeframe guard
"""

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.core.models import Candlestick, Signal, SignalType, Trade
from src.strategies.base import BaseStrategy, StrategyRequirements


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class FakeStrategy(BaseStrategy):
    """Minimal strategy with configurable requirements."""

    def __init__(self, strategy_id: str, product_id: str, timeframe: str = "15m"):
        super().__init__(strategy_id, product_id)
        self._timeframe = timeframe
        self.candles_received: list[Candlestick] = []
        self.trades_received: list[Trade] = []

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=self._timeframe,
            lookback_window=50,
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        self.candles_received.append(candle)
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=self._timeframe,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )

    def on_trade(self, trade: Trade):
        self.trades_received.append(trade)
        return None


@pytest.fixture
def engine_with_strategy():
    """Create a StrategyEngine with a FakeStrategy, mocking heavy deps."""
    with patch("src.core.engine.redis"), \
         patch("src.core.engine.create_adapter") as mock_create:
        mock_create.return_value = MagicMock()

        from src.core.engine import StrategyEngine

        db = MagicMock()
        clock = MagicMock()
        clock.now.return_value = 1704067200.0

        engine = StrategyEngine(db_session=db, clock=clock)

        def _add(strategy_id, product_id, timeframe):
            strat = FakeStrategy(strategy_id, product_id, timeframe)
            engine.add_strategy(strat)
            return strat

        yield engine, _add


# ---------------------------------------------------------------------------
# build_stream_channels()
# ---------------------------------------------------------------------------


class TestBuildStreamChannels:
    def test_single_strategy(self, engine_with_strategy):
        engine, add = engine_with_strategy
        add("s1", "BINANCE:BTCUSDT-PERP", "15m")

        channels = engine.build_stream_channels()

        assert channels == ["stream:market:binance:btcusdt:15m"]

    def test_multiple_timeframes_same_product(self, engine_with_strategy):
        engine, add = engine_with_strategy
        add("s1", "BINANCE:BTCUSDT-PERP", "15m")
        add("s2", "BINANCE:BTCUSDT-PERP", "5m")

        channels = engine.build_stream_channels()

        assert sorted(channels) == [
            "stream:market:binance:btcusdt:15m",
            "stream:market:binance:btcusdt:5m",
        ]

    def test_different_products(self, engine_with_strategy):
        engine, add = engine_with_strategy
        add("s1", "BINANCE:BTCUSDT-PERP", "1m")
        add("s2", "BINANCE:ETHUSDT-PERP", "15m")

        channels = engine.build_stream_channels()

        assert sorted(channels) == [
            "stream:market:binance:btcusdt:1m",
            "stream:market:binance:ethusdt:15m",
        ]

    def test_no_strategies_returns_empty(self, engine_with_strategy):
        engine, _ = engine_with_strategy
        assert engine.build_stream_channels() == []

    def test_duplicate_timeframes_deduped(self, engine_with_strategy):
        engine, add = engine_with_strategy
        add("s1", "BINANCE:BTCUSDT-PERP", "15m")
        add("s2", "BINANCE:BTCUSDT-PERP", "15m")

        channels = engine.build_stream_channels()

        assert channels == ["stream:market:binance:btcusdt:15m"]


# ---------------------------------------------------------------------------
# Timeframe guard in on_market_data()
# ---------------------------------------------------------------------------


class TestTimeframeGuard:
    def test_matching_timeframe_processed(self, engine_with_strategy):
        engine, add = engine_with_strategy
        strat = add("s1", "BINANCE:BTCUSDT-PERP", "15m")

        candle = Candlestick(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="15m",
            timestamp=1704067200000,
            open=Decimal("42000"), high=Decimal("42500"),
            low=Decimal("41500"), close=Decimal("42200"),
            volume=Decimal("100"),
        )
        engine.on_market_data(candle)

        assert len(strat.candles_received) == 1

    def test_mismatched_timeframe_skipped(self, engine_with_strategy):
        engine, add = engine_with_strategy
        strat = add("s1", "BINANCE:BTCUSDT-PERP", "15m")

        candle_1m = Candlestick(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            open=Decimal("42000"), high=Decimal("42500"),
            low=Decimal("41500"), close=Decimal("42200"),
            volume=Decimal("100"),
        )
        engine.on_market_data(candle_1m)

        assert len(strat.candles_received) == 0

    def test_multiple_strategies_different_tf(self, engine_with_strategy):
        engine, add = engine_with_strategy
        strat_15m = add("s1", "BINANCE:BTCUSDT-PERP", "15m")
        strat_5m = add("s2", "BINANCE:BTCUSDT-PERP", "5m")

        candle_5m = Candlestick(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="5m",
            timestamp=1704067200000,
            open=Decimal("42000"), high=Decimal("42500"),
            low=Decimal("41500"), close=Decimal("42200"),
            volume=Decimal("100"),
        )
        engine.on_market_data(candle_5m)

        assert len(strat_15m.candles_received) == 0
        assert len(strat_5m.candles_received) == 1

    def test_trades_not_filtered_by_timeframe(self, engine_with_strategy):
        engine, add = engine_with_strategy
        strat = add("s1", "BINANCE:BTCUSDT-PERP", "15m")

        trade = Trade(
            id="t1",
            product_id="BINANCE:BTCUSDT-PERP",
            price=Decimal("42000"),
            quantity=Decimal("0.1"),
            side="buy",
            timestamp=1704067200000,
        )
        engine.on_market_data(trade)

        assert len(strat.trades_received) == 1
