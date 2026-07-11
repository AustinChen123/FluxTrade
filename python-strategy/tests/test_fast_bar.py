from decimal import Decimal

import pytest

from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle_series
from src.core.data_sources.memory import MemoryDataSource
from src.core.fast_bar import FastBarReplayRunner, MarketTape, RollingMean, prepare_fast_strategy
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.core.product_registry import InstrumentSpec
from src.strategies.callable_strategy import CallableStrategy
from src.strategies.golden_cross import GoldenCrossStrategy

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False


def test_market_tape_builds_from_memory_data_source():
    candles = make_candle_series(count=5)
    tape = MarketTape.from_data_source(
        MemoryDataSource(candles),
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
    )

    assert len(tape) == 5
    assert tape.product_id == PRODUCT_ID
    assert tape.timeframe == TIMEFRAME
    assert int(tape.timestamps[0]) == candles[0].timestamp
    assert float(tape.close[-1]) == float(candles[-1].close)


def test_market_tape_supports_live_append():
    candles = make_candle_series(count=3)
    tape = MarketTape.empty(product_id=PRODUCT_ID, timeframe=TIMEFRAME, capacity=1)

    for candle in candles:
        tape.append_candle(candle)

    assert len(tape) == 3
    assert tape.capacity >= 3
    assert int(tape.timestamps[0]) == candles[0].timestamp
    assert float(tape.open[2]) == float(candles[2].open)


def test_market_tape_compacts_unused_capacity():
    candle = make_candle_series(count=1)[0]
    tape = MarketTape.empty(product_id=PRODUCT_ID, timeframe=TIMEFRAME, capacity=8)

    tape.append_candle(candle)
    compacted = tape.compact()

    assert len(compacted) == 1
    assert compacted.capacity == 1
    assert int(compacted.timestamps[0]) == candle.timestamp


def test_rolling_mean_tracks_ready_state_and_mean():
    mean = RollingMean(3)

    mean.append(1.0)
    mean.append(2.0)
    assert not mean.ready

    mean.append(3.0)
    assert mean.ready
    assert mean.mean == 2.0

    mean.append(4.0)
    assert mean.mean == 3.0


def test_prepare_fast_strategy_reports_unsupported_strategy():
    strategy = CallableStrategy(
        "callable",
        lambda candle: None,
        PRODUCT_ID,
        TIMEFRAME,
    )

    with pytest.raises(TypeError, match="does not support fast-bar"):
        prepare_fast_strategy(strategy)


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_fast_bar_golden_cross_matches_research_runner_core_metrics():
    candles = make_candle_series(count=2_000)
    fee_config = {"maker": 0.0002, "taker": 0.0006}

    research_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
    )
    research_runner.add_strategy(
        GoldenCrossStrategy(
            "fast_bar_parity",
            PRODUCT_ID,
            short_window=20,
            long_window=80,
            timeframe=TIMEFRAME,
            quantity=Decimal("0.01"),
        )
    )

    tape = MarketTape.from_candles(
        candles,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
    )
    fast_runner = FastBarReplayRunner(
        tape=tape,
        strategy=GoldenCrossStrategy(
            "fast_bar_parity",
            PRODUCT_ID,
            short_window=20,
            long_window=80,
            timeframe=TIMEFRAME,
            quantity=Decimal("0.01"),
        ).prepare_fast(),
        initial_balance=Decimal("10000"),
        maker_fee=Decimal("0.0002"),
        taker_fee=Decimal("0.0006"),
    )

    research_result = research_runner.run()
    fast_result = fast_runner.run()

    assert fast_result["candle_count"] == research_result["candle_count"]
    assert fast_result["raw_trade_count"] == research_result["raw_trade_count"]
    assert fast_result["total_trades"] == research_result["total_trades"]
    assert fast_result["total_pnl"] == research_result["total_pnl"]
    assert fast_result["max_drawdown"] == research_result["max_drawdown"]


@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_fast_bar_uses_instrument_multiplier():
    candles = make_candle_series(count=2_000)
    spec = InstrumentSpec(
        product_id=PRODUCT_ID,
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        multiplier=Decimal("2"),
    )
    tape = MarketTape.from_candles(candles, product_id=PRODUCT_ID, timeframe=TIMEFRAME)
    def strategy():
        return GoldenCrossStrategy(
            "fast_bar_multiplier",
            PRODUCT_ID,
            short_window=20,
            long_window=80,
            timeframe=TIMEFRAME,
            quantity=Decimal("1"),
        ).prepare_fast()

    default_result = FastBarReplayRunner(tape=tape, strategy=strategy()).run()
    multiplied_result = FastBarReplayRunner(
        tape=tape,
        strategy=strategy(),
        instrument_spec=spec,
    ).run()

    assert multiplied_result["total_pnl"] == default_result["total_pnl"] * 2
