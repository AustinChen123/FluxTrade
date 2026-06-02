from decimal import Decimal

import pandas as pd
import pytest

from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle_series
from src.core.data_sources.memory import MemoryDataSource
from src.core.golden_cross_fast_fitness import GoldenCrossFastFitnessEvaluator
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.strategies.golden_cross import GoldenCrossStrategy

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False


def _candles_df(candles):
    return pd.DataFrame(
        [
            {
                "timestamp": candle.timestamp,
                "open": float(candle.open),
                "high": float(candle.high),
                "low": float(candle.low),
                "close": float(candle.close),
                "volume": float(candle.volume),
            }
            for candle in candles
        ]
    )


def test_golden_cross_fast_fitness_validates_windows():
    candles = make_candle_series(count=10)
    evaluator = GoldenCrossFastFitnessEvaluator.from_dataframe(_candles_df(candles))

    with pytest.raises(ValueError, match="short_window must be smaller"):
        evaluator.evaluate(short_window=5, long_window=5)


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_golden_cross_fast_fitness_matches_research_runner_core_metrics():
    candles = make_candle_series(count=2_000)
    fee_config = {"maker": 0.0002, "taker": 0.0006}

    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
    )
    runner.add_strategy(
        GoldenCrossStrategy(
            "fast_fitness_parity",
            PRODUCT_ID,
            short_window=20,
            long_window=80,
            timeframe=TIMEFRAME,
            quantity=Decimal("0.01"),
        )
    )
    research_result = runner.run()

    evaluator = GoldenCrossFastFitnessEvaluator.from_dataframe(
        _candles_df(candles),
        initial_balance=Decimal("10000"),
        taker_fee=Decimal("0.0006"),
    )
    fast_result = evaluator.evaluate(
        short_window=20,
        long_window=80,
        quantity=Decimal("0.01"),
    )

    assert fast_result.total_trades == research_result["total_trades"]
    assert fast_result.raw_trade_count == research_result["raw_trade_count"]
    assert abs(fast_result.total_pnl - research_result["total_pnl"]) <= Decimal("0.01")
    assert abs(fast_result.max_drawdown - research_result["max_drawdown"]) <= Decimal("0.01")
