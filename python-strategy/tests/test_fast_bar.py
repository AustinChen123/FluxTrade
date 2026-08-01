from decimal import Decimal

import pytest

from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle_series
from src.core.data_sources.memory import MemoryDataSource
from src.core.fast_bar import (
    FastBarReplayRunner,
    InvalidFastBarIntent,
    MarketTape,
    RollingMean,
    SignalIntent,
    _resolve_fast_bar_order_intent,
    prepare_fast_strategy,
)
from src.core.models import OrderSide, PositionSide, SignalType
from src.core.product_registry import InstrumentSpec
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.strategies.callable_strategy import CallableStrategy
from src.strategies.golden_cross import GoldenCrossStrategy

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False


class _ScriptedFastStrategy:
    strategy_id = "scripted_fast"

    def __init__(self, intents: list[SignalIntent | None]) -> None:
        self._intents = iter(intents)

    def on_bar(self, bar):
        del bar
        return next(self._intents, None)


def _fast_runner(*intents: SignalIntent | None) -> FastBarReplayRunner:
    candles = make_candle_series(count=max(1, len(intents)))
    return FastBarReplayRunner(
        tape=MarketTape.from_candles(
            candles,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
        ),
        strategy=_ScriptedFastStrategy(list(intents)),
    )


def _owned_position_for(signal_type: SignalType):
    if signal_type == SignalType.EXIT_LONG:
        return "LONG", Decimal("2")
    if signal_type == SignalType.EXIT_SHORT:
        return "SHORT", Decimal("2")
    return None


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


@pytest.mark.parametrize(
    ("signal_type", "quantity", "owned_position", "expected"),
    [
        (SignalType.LONG, None, None, ("LONG", OrderSide.BUY, Decimal("0.01"))),
        (SignalType.SHORT, None, None, ("SHORT", OrderSide.SELL, Decimal("0.01"))),
        (
            SignalType.LONG,
            Decimal("2"),
            ("SHORT", Decimal("3")),
            ("LONG", OrderSide.BUY, Decimal("2")),
        ),
        (
            SignalType.EXIT_LONG,
            Decimal("2"),
            ("LONG", Decimal("2")),
            ("SHORT", OrderSide.SELL, Decimal("2")),
        ),
        (
            SignalType.EXIT_LONG,
            Decimal("1"),
            ("LONG", Decimal("2")),
            ("SHORT", OrderSide.SELL, Decimal("1")),
        ),
        (
            SignalType.EXIT_SHORT,
            None,
            ("SHORT", Decimal("2")),
            ("LONG", OrderSide.BUY, Decimal("2")),
        ),
        (
            SignalType.EXIT_SHORT,
            Decimal("1"),
            ("SHORT", Decimal("2")),
            ("LONG", OrderSide.BUY, Decimal("1")),
        ),
    ],
)
def test_fast_bar_order_intent_supported_state_matrix(
    signal_type,
    quantity,
    owned_position,
    expected,
):
    intent = SignalIntent(signal_type, quantity=quantity)

    resolved = _resolve_fast_bar_order_intent(
        intent,
        owned_position=owned_position,
    )

    assert resolved == expected


@pytest.mark.parametrize(
    "signal_type",
    [SignalType.LONG, SignalType.SHORT, SignalType.EXIT_LONG, SignalType.EXIT_SHORT],
)
@pytest.mark.parametrize("price", [Decimal("0"), Decimal("100")])
def test_fast_bar_rejects_price_for_every_actionable_signal(signal_type, price):
    intent = SignalIntent(signal_type, price=price)

    with pytest.raises(InvalidFastBarIntent, match="price is unsupported"):
        _resolve_fast_bar_order_intent(
            intent,
            owned_position=_owned_position_for(signal_type),
        )


@pytest.mark.parametrize(
    "signal_type",
    [SignalType.LONG, SignalType.SHORT, SignalType.EXIT_LONG, SignalType.EXIT_SHORT],
)
@pytest.mark.parametrize(
    "quantity",
    [
        Decimal("0"),
        Decimal("-1"),
        Decimal("NaN"),
        Decimal("Infinity"),
        Decimal("-Infinity"),
    ],
)
def test_fast_bar_rejects_invalid_quantity_for_every_actionable_signal(
    signal_type,
    quantity,
):
    intent = SignalIntent(signal_type, quantity=quantity)

    with pytest.raises(InvalidFastBarIntent, match="quantity must"):
        _resolve_fast_bar_order_intent(
            intent,
            owned_position=_owned_position_for(signal_type),
        )


def test_fast_bar_rejects_non_decimal_quantity():
    intent = SignalIntent(SignalType.LONG, quantity=1)  # type: ignore[arg-type]

    with pytest.raises(InvalidFastBarIntent, match="quantity must"):
        _resolve_fast_bar_order_intent(intent, owned_position=None)


def test_fast_bar_rejects_unsupported_signal_type():
    intent = SignalIntent("unsupported")  # type: ignore[arg-type]

    with pytest.raises(InvalidFastBarIntent, match="unsupported signal type"):
        _resolve_fast_bar_order_intent(intent, owned_position=None)


@pytest.mark.parametrize(
    ("signal_type", "owned_position", "quantity", "reason"),
    [
        (SignalType.EXIT_LONG, None, None, "requires an owned position"),
        (SignalType.EXIT_SHORT, None, None, "requires an owned position"),
        (
            SignalType.EXIT_LONG,
            ("SHORT", Decimal("1")),
            None,
            "side mismatch",
        ),
        (
            SignalType.EXIT_SHORT,
            ("LONG", Decimal("1")),
            None,
            "side mismatch",
        ),
        (
            SignalType.EXIT_LONG,
            ("LONG", Decimal("1")),
            Decimal("2"),
            "exceeds owned position",
        ),
        (
            SignalType.EXIT_SHORT,
            ("SHORT", Decimal("1")),
            Decimal("2"),
            "exceeds owned position",
        ),
    ],
)
def test_fast_bar_exit_rejection_state_matrix(
    signal_type,
    owned_position,
    quantity,
    reason,
):
    intent = SignalIntent(signal_type, quantity=quantity)

    with pytest.raises(InvalidFastBarIntent, match=reason):
        _resolve_fast_bar_order_intent(intent, owned_position=owned_position)


@pytest.mark.parametrize(
    "position_quantity",
    [
        Decimal("0"),
        Decimal("-1"),
        Decimal("NaN"),
        Decimal("Infinity"),
        1,
    ],
)
def test_fast_bar_rejects_invalid_owned_position_quantity(position_quantity):
    intent = SignalIntent(SignalType.EXIT_LONG)

    with pytest.raises(InvalidFastBarIntent, match="owned position quantity"):
        _resolve_fast_bar_order_intent(
            intent,
            owned_position=("LONG", position_quantity),  # type: ignore[arg-type]
        )


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_fast_bar_invalid_intent_aborts_without_metrics():
    runner = _fast_runner(
        SignalIntent(SignalType.LONG, price=Decimal("100")),
    )

    with pytest.raises(InvalidFastBarIntent, match="price is unsupported"):
        runner.run()


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_fast_bar_empty_endpoint_state_has_no_final_observation():
    runner = FastBarReplayRunner(
        tape=MarketTape.empty(product_id=PRODUCT_ID, timeframe=TIMEFRAME),
        strategy=_ScriptedFastStrategy([]),
    )

    result = runner.run()

    endpoint = result["endpoint_state"]
    assert endpoint.positions == ()
    assert endpoint.working_orders == ()
    assert endpoint.protection_orders == ()
    assert endpoint.final_mark is None
    assert endpoint.end_timestamp is None
    assert endpoint.halted_early is False


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_fast_bar_final_bar_entry_is_visible_as_working_order():
    runner = _fast_runner(
        SignalIntent(SignalType.LONG, quantity=Decimal("1")),
    )

    result = runner.run()

    endpoint = result["endpoint_state"]
    assert endpoint.positions == ()
    assert len(endpoint.working_orders) == 1
    assert endpoint.working_orders[0].side == OrderSide.BUY
    assert endpoint.working_orders[0].order_type == "MARKET"
    assert endpoint.working_orders[0].price is None
    assert endpoint.protection_orders == ()
    assert endpoint.final_mark == Decimal(str(runner.tape.close[0]))
    assert endpoint.end_timestamp == int(runner.tape.timestamps[0])
    assert endpoint.halted_early is False


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
@pytest.mark.parametrize(
    ("entry_type", "expected_side"),
    [
        (SignalType.LONG, PositionSide.LONG),
        (SignalType.SHORT, PositionSide.SHORT),
    ],
)
def test_fast_bar_open_position_is_visible_at_endpoint(
    entry_type,
    expected_side,
):
    runner = _fast_runner(
        SignalIntent(entry_type, quantity=Decimal("1")),
        None,
    )

    endpoint = runner.run()["endpoint_state"]

    assert len(endpoint.positions) == 1
    assert endpoint.positions[0].side == expected_side
    assert endpoint.positions[0].quantity == Decimal("1")
    assert endpoint.working_orders == ()
    assert endpoint.halted_early is False


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_fast_bar_pending_exit_preserves_position_and_working_order():
    endpoint = _fast_runner(
        SignalIntent(SignalType.LONG, quantity=Decimal("1")),
        SignalIntent(SignalType.EXIT_LONG),
    ).run()["endpoint_state"]

    assert len(endpoint.positions) == 1
    assert endpoint.positions[0].side == PositionSide.LONG
    assert len(endpoint.working_orders) == 1
    assert endpoint.working_orders[0].side == OrderSide.SELL
    assert endpoint.protection_orders == ()


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_fast_bar_completed_exit_is_flat_at_endpoint():
    endpoint = _fast_runner(
        SignalIntent(SignalType.LONG, quantity=Decimal("1")),
        SignalIntent(SignalType.EXIT_LONG),
        None,
    ).run()["endpoint_state"]

    assert endpoint.positions == ()
    assert endpoint.working_orders == ()
    assert endpoint.protection_orders == ()
    assert endpoint.halted_early is False


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
def test_fast_bar_invalid_intent_after_fill_aborts_without_metrics():
    runner = _fast_runner(
        SignalIntent(SignalType.LONG, quantity=Decimal("1")),
        SignalIntent(SignalType.EXIT_LONG, price=Decimal("100")),
    )

    with pytest.raises(InvalidFastBarIntent, match="price is unsupported"):
        runner.run()


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
@pytest.mark.parametrize("signal_type", [SignalType.EXIT_LONG, SignalType.EXIT_SHORT])
def test_fast_bar_flat_exit_aborts_without_metrics(signal_type):
    runner = _fast_runner(SignalIntent(signal_type))

    with pytest.raises(InvalidFastBarIntent, match="requires an owned position"):
        runner.run()


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
@pytest.mark.parametrize("quantity", [Decimal("1e10000"), Decimal("1e-10000")])
def test_fast_bar_rejects_quantity_outside_matcher_decimal_range(quantity):
    runner = _fast_runner(SignalIntent(SignalType.LONG, quantity=quantity))

    with pytest.raises(InvalidFastBarIntent, match="matcher Decimal range"):
        runner.run()


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
@pytest.mark.parametrize(
    ("entry_type", "exit_type", "exit_quantity", "expected_exit_quantity"),
    [
        (SignalType.LONG, SignalType.EXIT_LONG, None, Decimal("2")),
        (SignalType.LONG, SignalType.EXIT_LONG, Decimal("1"), Decimal("1")),
        (SignalType.SHORT, SignalType.EXIT_SHORT, None, Decimal("2")),
        (SignalType.SHORT, SignalType.EXIT_SHORT, Decimal("1"), Decimal("1")),
    ],
)
def test_fast_bar_exit_uses_owned_position_quantity(
    entry_type,
    exit_type,
    exit_quantity,
    expected_exit_quantity,
):
    result = _fast_runner(
        SignalIntent(entry_type, quantity=Decimal("2")),
        SignalIntent(exit_type, quantity=exit_quantity),
        None,
    ).run()

    assert [trade.quantity for trade in result["raw_trades"]] == [
        Decimal("2"),
        expected_exit_quantity,
    ]


@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
@pytest.mark.parametrize(
    ("exit_intent", "reason"),
    [
        (SignalIntent(SignalType.EXIT_SHORT, quantity=Decimal("1")), "side mismatch"),
        (
            SignalIntent(SignalType.EXIT_LONG, quantity=Decimal("2")),
            "exceeds owned position",
        ),
    ],
)
def test_fast_bar_exit_rejects_mismatch_or_oversize(exit_intent, reason):
    runner = _fast_runner(
        SignalIntent(SignalType.LONG, quantity=Decimal("1")),
        exit_intent,
    )

    with pytest.raises(InvalidFastBarIntent, match=reason):
        runner.run()


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

    def fill_digest(result):
        return [
            (trade.timestamp, trade.side, trade.price, trade.quantity, trade.fee)
            for trade in result["raw_trades"]
        ]

    assert fast_result["candle_count"] == research_result["candle_count"]
    assert fill_digest(fast_result) == fill_digest(research_result)
    assert fast_result["raw_trade_count"] == research_result["raw_trade_count"]
    assert fast_result["total_trades"] == research_result["total_trades"]
    assert fast_result["total_pnl"] == research_result["total_pnl"]
    assert fast_result["max_drawdown"] == research_result["max_drawdown"]
    assert fast_result["endpoint_state"] == research_result["endpoint_state"]


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
