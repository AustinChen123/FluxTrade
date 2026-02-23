"""Integration: Stress tests for backtest pipeline.

Requires: compiled fluxtrade_core.so
Validates: large dataset handling, extreme volatility, rapid signals, memory stability.
"""
import pytest
import tracemalloc
from decimal import Decimal
from unittest.mock import patch, MagicMock

from src.core.data_sources.memory import MemoryDataSource
from src.core.backtest_runner import BacktestRunner
from src.core.models import Signal, SignalType
from src.strategies.callable_strategy import CallableStrategy
from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle, make_candle_series

try:
    import fluxtrade_core  # noqa: F401
    HAS_RUST = True
except ImportError:
    HAS_RUST = False

pytestmark = [
    pytest.mark.rust,
    pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled"),
]

INTERVAL_MS = 15 * 60 * 1000


def _run_stress_backtest(strategy, candle_data, mock_session_local, balance=Decimal("10000")):
    mock_session = MagicMock()
    mock_session.query.return_value.filter_by.return_value.all.return_value = []
    mock_session_local.return_value = mock_session

    ds = MemoryDataSource(candle_data)
    runner = BacktestRunner(
        start_time=candle_data[0].timestamp,
        end_time=candle_data[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=balance,
        data_source=ds,
        fee_config={"maker": 0.0002, "taker": 0.0006},
        report_config={"csv_trades": False, "equity_curve": False,
                       "markdown_report": False, "journal": False},
    )
    runner.add_strategy(strategy)
    return runner.run()


def _make_volatile_candles(count: int, volatility_pct: float = 0.20) -> list:
    """Generate candles with extreme volatility (configurable % swings)."""
    candles = []
    price = 50000.0
    start_ts = 1_700_000_000_000

    for i in range(count):
        ts = start_ts + i * INTERVAL_MS
        direction = 1 if i % 2 == 0 else -1
        swing = price * volatility_pct * direction
        close_price = price + swing

        high_price = max(price, close_price) * 1.01
        low_price = min(price, close_price) * 0.99

        candles.append(make_candle(
            timestamp=ts,
            open=Decimal(str(round(price, 2))),
            high=Decimal(str(round(high_price, 2))),
            low=Decimal(str(round(low_price, 2))),
            close=Decimal(str(round(close_price, 2))),
        ))
        price = close_price

    return candles


class TestStressBacktest:

    @patch("src.core.backtest_runner.SessionLocal")
    def test_100k_candles_completes(self, mock_sl):
        """100K candles backtest should complete and produce valid PnL."""
        candle_data = make_candle_series(count=100_000)

        def predict(candle):
            idx = (candle.timestamp - 1_700_000_000_000) // INTERVAL_MS
            if idx % 100 == 50:
                return Signal(
                    strategy_id="stress", product_id=candle.product_id,
                    timeframe=TIMEFRAME, timestamp=candle.timestamp,
                    type=SignalType.LONG, quantity=Decimal("0.001"),
                )
            elif idx % 100 == 0 and idx > 0:
                return Signal(
                    strategy_id="stress", product_id=candle.product_id,
                    timeframe=TIMEFRAME, timestamp=candle.timestamp,
                    type=SignalType.EXIT_LONG, quantity=Decimal("0.001"),
                )
            return None

        strat = CallableStrategy("stress_100k", predict, PRODUCT_ID, TIMEFRAME)
        result = _run_stress_backtest(strat, candle_data, mock_sl)

        assert result is not None
        assert isinstance(result["total_pnl"], Decimal)
        assert result["journal_count"] > 0

    @patch("src.core.backtest_runner.SessionLocal")
    def test_extreme_volatility_no_negative_balance(self, mock_sl):
        """Under +/-20% volatility with small positions, balance stays positive."""
        candle_data = _make_volatile_candles(count=500, volatility_pct=0.20)

        def predict(candle):
            idx = (candle.timestamp - 1_700_000_000_000) // INTERVAL_MS
            if idx % 20 == 5:
                return Signal(
                    strategy_id="vol", product_id=candle.product_id,
                    timeframe=TIMEFRAME, timestamp=candle.timestamp,
                    type=SignalType.LONG, quantity=Decimal("0.001"),
                )
            elif idx % 20 == 15:
                return Signal(
                    strategy_id="vol", product_id=candle.product_id,
                    timeframe=TIMEFRAME, timestamp=candle.timestamp,
                    type=SignalType.EXIT_LONG, quantity=Decimal("0.001"),
                )
            return None

        strat = CallableStrategy("volatile", predict, PRODUCT_ID, TIMEFRAME)
        result = _run_stress_backtest(strat, candle_data, mock_sl, balance=Decimal("10000"))

        assert result is not None
        final_balance = Decimal("10000") + result["total_pnl"]
        assert final_balance > Decimal("0"), f"Balance went negative: {final_balance}"

    @patch("src.core.backtest_runner.SessionLocal")
    def test_rapid_signal_burst(self, mock_sl):
        """50 consecutive candles each with a signal should not crash."""
        candle_data = make_candle_series(count=100)
        call_count = 0

        def predict_burst(candle):
            nonlocal call_count
            call_count += 1
            if call_count <= 50:
                if call_count % 2 == 1:
                    return Signal(
                        strategy_id="burst", product_id=candle.product_id,
                        timeframe=TIMEFRAME, timestamp=candle.timestamp,
                        type=SignalType.LONG, quantity=Decimal("0.001"),
                    )
                else:
                    return Signal(
                        strategy_id="burst", product_id=candle.product_id,
                        timeframe=TIMEFRAME, timestamp=candle.timestamp,
                        type=SignalType.EXIT_LONG, quantity=Decimal("0.001"),
                    )
            return None

        strat = CallableStrategy("burst", predict_burst, PRODUCT_ID, TIMEFRAME)
        result = _run_stress_backtest(strat, candle_data, mock_sl)

        assert result is not None
        assert result["journal_count"] > 0, "Burst signals should produce activity"

    @patch("src.core.backtest_runner.SessionLocal")
    def test_memory_stable_over_large_dataset(self, mock_sl):
        """Memory should not exceed 500MB for 50K candles."""
        candle_data = make_candle_series(count=50_000)

        def predict(candle):
            idx = (candle.timestamp - 1_700_000_000_000) // INTERVAL_MS
            if idx % 200 == 100:
                return Signal(
                    strategy_id="mem", product_id=candle.product_id,
                    timeframe=TIMEFRAME, timestamp=candle.timestamp,
                    type=SignalType.LONG, quantity=Decimal("0.001"),
                )
            elif idx % 200 == 0 and idx > 0:
                return Signal(
                    strategy_id="mem", product_id=candle.product_id,
                    timeframe=TIMEFRAME, timestamp=candle.timestamp,
                    type=SignalType.EXIT_LONG, quantity=Decimal("0.001"),
                )
            return None

        strat = CallableStrategy("memory_test", predict, PRODUCT_ID, TIMEFRAME)

        tracemalloc.start()
        result = _run_stress_backtest(strat, candle_data, mock_sl)
        _, peak_bytes = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        peak_mb = peak_bytes / (1024 * 1024)
        assert result is not None
        assert peak_mb < 500, f"Peak memory {peak_mb:.1f}MB exceeds 500MB limit"
