from decimal import Decimal

import pandas as pd
import pytest

from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle_series
from src.core.data_sources.memory import MemoryDataSource
from src.core.golden_cross_fast_fitness import GoldenCrossFastFitnessEvaluator
from src.core.product_registry import FeeModel, InstrumentSpec
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


@pytest.mark.parametrize(
    "quantity",
    [
        Decimal("0"),
        Decimal("-1"),
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
        Decimal("1e10000"),
        Decimal("1e-10000"),
        1,
    ],
)
def test_golden_cross_fast_fitness_rejects_invalid_quantity_before_empty_result(
    quantity,
):
    candles = make_candle_series(count=2)
    evaluator = GoldenCrossFastFitnessEvaluator.from_dataframe(_candles_df(candles))

    with pytest.raises(ValueError, match="quantity must"):
        evaluator.evaluate(
            short_window=1,
            long_window=5,
            quantity=quantity,  # type: ignore[arg-type]
        )


def test_golden_cross_fast_fitness_rejects_non_finite_calculation():
    df = pd.DataFrame(
        {
            "timestamp": [
                1_700_000_000_000,
                1_700_000_300_000,
                1_700_000_600_000,
                1_700_000_900_000,
                1_700_001_200_000,
                1_700_001_500_000,
            ],
            "open": [100, 100, 100, 100, 100, 100],
            "close": [100, 101, 99, 101, 99, 101],
        }
    )
    evaluator = GoldenCrossFastFitnessEvaluator.from_dataframe(
        df,
        taker_fee=Decimal("0.01"),
    )

    with pytest.raises(ValueError, match="non-finite"):
        evaluator.evaluate(
            short_window=1,
            long_window=2,
            quantity=Decimal("1e308"),
        )


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


def test_golden_cross_fast_fitness_uses_instrument_multiplier():
    candles = make_candle_series(count=2_000)
    df = _candles_df(candles)
    spec = InstrumentSpec(
        product_id=PRODUCT_ID,
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        multiplier=Decimal("2"),
    )

    default_result = GoldenCrossFastFitnessEvaluator.from_dataframe(df).evaluate(
        short_window=20,
        long_window=80,
        quantity=Decimal("1"),
    )
    multiplied_result = GoldenCrossFastFitnessEvaluator.from_dataframe(
        df,
        instrument_spec=spec,
    ).evaluate(short_window=20, long_window=80, quantity=Decimal("1"))

    assert multiplied_result.total_pnl == default_result.total_pnl * 2


@pytest.mark.parametrize(
    ("fee_model", "expected"),
    [
        (FeeModel.PERCENTAGE_NOTIONAL, 6.0),
        (FeeModel.PER_CONTRACT, 3.0),
    ],
)
def test_golden_cross_fast_fitness_fee_modes(fee_model, expected):
    candles = make_candle_series(count=10)
    spec = InstrumentSpec(
        product_id=PRODUCT_ID,
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        multiplier=Decimal("2"),
        fee_model=fee_model,
    )
    evaluator = GoldenCrossFastFitnessEvaluator.from_dataframe(
        _candles_df(candles),
        taker_fee=Decimal("1") if fee_model == FeeModel.PER_CONTRACT else Decimal("0.01"),
        instrument_spec=spec,
    )

    assert evaluator._fee(100.0, 3.0) == expected
