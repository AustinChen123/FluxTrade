"""Research backtest path parity checks.

The research runner is intentionally lighter than BacktestRunner, but its
basic fill ordering and fee-aware PnL should stay aligned for simple signal
strategies. These tests protect that contract without asserting speed.
"""

import json
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle, make_candle_series
from src.core.analytics import calculate_metrics
from src.core.backtest_runner import BacktestRunner
from src.core.capital_allocator import CapitalAllocator
from src.core.data_sources.memory import MemoryDataSource
from src.core.models import Candlestick, Signal, SignalType
from src.core.orm_models import (
    BacktestResultSummary,
    BacktestTradeLog,
    Exchange,
    Product,
    SignalAudit,
    Strategy,
)
from src.core.precision import PrecisionCodec, PrecisionSpec
from src.core.product_registry import CapitalModel, InstrumentSpec
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.strategies.callable_strategy import CallableStrategy

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


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


def _sqlite_backtest_session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'research_backtest_parity.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        SignalAudit.__table__,
        BacktestResultSummary.__table__,
        BacktestTradeLog.__table__,
    ]:
        table.create(engine, checkfirst=True)

    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=PRODUCT_ID,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.commit()

    return session_factory


def _signal_factory(
    strategy_id: str,
    candles: list,
    *,
    entry_type: SignalType = SignalType.LONG,
    exit_type: SignalType = SignalType.EXIT_LONG,
    quantity: Decimal = Decimal("0.01"),
):
    def predict(candle):
        index = (candle.timestamp - candles[0].timestamp) // INTERVAL_MS
        if index % 80 == 10:
            return Signal(
                strategy_id=strategy_id,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=entry_type,
                quantity=quantity,
            )
        if index % 80 == 40:
            return Signal(
                strategy_id=strategy_id,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=exit_type,
                quantity=quantity,
            )
        return None

    return predict


def _conditional_signal_factory(
    strategy_id: str,
    first_timestamp: int,
    *,
    entry_type: SignalType,
    stop_loss: Decimal | None,
    take_profit: Decimal | None,
    trailing_distance: Decimal | None,
):
    def predict(candle):
        if candle.timestamp != first_timestamp:
            return None
        return Signal(
            strategy_id=strategy_id,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=entry_type,
            quantity=Decimal("1"),
            stop_loss=stop_loss,
            take_profit=take_profit,
            trailing_distance=trailing_distance,
        )

    return predict


class CapitalLifecycleProbeStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__("capital_lifecycle_probe", PRODUCT_ID)
        self.contexts: list[StrategyContext] = []
        self.decisions: list[str] = []
        self._entered = False
        self._exited = False

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(PRODUCT_ID, TIMEFRAME, 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal | None:
        if context is None:
            raise AssertionError("capital lifecycle test requires context")
        self.contexts.append(context)
        if not self._entered and context.position is None:
            self._entered = True
            self.decisions.append("enter")
            return Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.01"),
            )
        if self._entered and not self._exited and context.position is not None:
            self._exited = True
            self.decisions.append("exit")
            return Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=context.position.quantity,
            )
        return None


class CapitalGatedEntryStrategy(BaseStrategy):
    def __init__(self) -> None:
        super().__init__("capital_gated_entry", PRODUCT_ID)
        self.contexts: list[StrategyContext] = []
        self._sent = False

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(PRODUCT_ID, TIMEFRAME, 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal | None:
        if context is None:
            raise AssertionError("capital gating test requires context")
        self.contexts.append(context)
        if self._sent:
            return None
        self._sent = True
        return Signal(
            strategy_id=self.strategy_id,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            quantity=Decimal("0.01"),
        )


@pytest.mark.smoke
@pytest.mark.parametrize(
    ("entry_type", "exit_type", "multiplier"),
    [
        (SignalType.LONG, SignalType.EXIT_LONG, None),
        (SignalType.SHORT, SignalType.EXIT_SHORT, None),
        (SignalType.LONG, SignalType.EXIT_LONG, Decimal("2")),
        (SignalType.SHORT, SignalType.EXIT_SHORT, Decimal("2")),
    ],
    ids=["long-default", "short-default", "long-futures", "short-futures"],
)
def test_research_backtest_matches_full_runner_core_metrics(
    tmp_path,
    entry_type,
    exit_type,
    multiplier,
):
    session_factory = _sqlite_backtest_session_factory(tmp_path)
    candles = make_candle_series(count=2_000)
    fee_config = {"maker": 0.0002, "taker": 0.0006}
    quantity = Decimal("0.01")
    instrument_spec = (
        None
        if multiplier is None
        else InstrumentSpec(
            product_id=PRODUCT_ID,
            exchange="test",
            symbol="MNQ",
            base="MNQ",
            quote="USD",
            multiplier=multiplier,
        )
    )

    full_runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        max_drawdown_limit=None,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
        report_config={
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        db_session_factory=session_factory,
        instrument_spec=instrument_spec,
    )
    full_runner.add_strategy(
        CallableStrategy(
            "research_parity",
            _signal_factory(
                "research_parity",
                candles,
                entry_type=entry_type,
                exit_type=exit_type,
                quantity=quantity,
            ),
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    research_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
        max_drawdown_limit=None,
        instrument_spec=instrument_spec,
    )
    research_runner.add_strategy(
        CallableStrategy(
            "research_parity",
            _signal_factory(
                "research_parity",
                candles,
                entry_type=entry_type,
                exit_type=exit_type,
                quantity=quantity,
            ),
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    full_result = full_runner.run()
    research_result = research_runner.run()

    with session_factory() as session:
        summary = session.scalars(select(BacktestResultSummary)).one()
        full_trades = session.scalars(select(BacktestTradeLog)).all()

    metrics = json.loads(summary.metrics_json)
    full_closed_trades = calculate_metrics(
        full_trades,
        contract_multiplier=multiplier or Decimal("1"),
    )["closed_trades"]

    assert research_result["candle_count"] == len(candles)
    assert research_result["raw_trade_count"] == 50
    assert research_result["total_trades"] == full_result["total_trades"] == 25
    assert research_result["total_trades"] == metrics["total_trades"]
    assert research_result["total_pnl"] == full_result["total_pnl"]
    assert research_result["profit_factor"] == full_result["profit_factor"]
    assert research_result["closed_trades"] == full_closed_trades


@pytest.mark.smoke
@pytest.mark.parametrize(
    (
        "entry_type",
        "stop_loss",
        "take_profit",
        "trailing_distance",
        "gap_open",
        "high",
        "low",
        "expected_exit",
    ),
    [
        (
            SignalType.LONG,
            Decimal("90"),
            None,
            None,
            None,
            Decimal("105"),
            Decimal("85"),
            Decimal("90"),
        ),
        (
            SignalType.LONG,
            None,
            Decimal("110"),
            None,
            None,
            Decimal("115"),
            Decimal("95"),
            Decimal("110"),
        ),
        (
            SignalType.LONG,
            Decimal("90"),
            Decimal("110"),
            None,
            None,
            Decimal("115"),
            Decimal("85"),
            Decimal("90"),
        ),
        (
            SignalType.LONG,
            None,
            None,
            Decimal("10"),
            None,
            Decimal("115"),
            Decimal("100"),
            Decimal("105"),
        ),
        (
            SignalType.SHORT,
            Decimal("110"),
            None,
            None,
            None,
            Decimal("115"),
            Decimal("95"),
            Decimal("110"),
        ),
        (
            SignalType.SHORT,
            None,
            Decimal("90"),
            None,
            None,
            Decimal("105"),
            Decimal("85"),
            Decimal("90"),
        ),
        (
            SignalType.SHORT,
            Decimal("110"),
            Decimal("90"),
            None,
            None,
            Decimal("115"),
            Decimal("85"),
            Decimal("110"),
        ),
        (
            SignalType.SHORT,
            None,
            None,
            Decimal("10"),
            None,
            Decimal("100"),
            Decimal("85"),
            Decimal("95"),
        ),
        (
            SignalType.LONG,
            Decimal("90"),
            None,
            None,
            Decimal("80"),
            Decimal("100"),
            Decimal("75"),
            Decimal("80"),
        ),
        (
            SignalType.SHORT,
            Decimal("110"),
            None,
            None,
            Decimal("120"),
            Decimal("125"),
            Decimal("100"),
            Decimal("120"),
        ),
    ],
    ids=[
        "long-stop-loss",
        "long-take-profit",
        "long-both-worst-case",
        "long-trailing",
        "short-stop-loss",
        "short-take-profit",
        "short-both-worst-case",
        "short-trailing",
        "long-stop-loss-gap-through",
        "short-stop-loss-gap-through",
    ],
)
def test_research_backtest_matches_full_runner_conditional_orders(
    tmp_path,
    entry_type,
    stop_loss,
    take_profit,
    trailing_distance,
    gap_open,
    high,
    low,
    expected_exit,
):
    start = 1_700_000_000_000
    candles = [
        make_candle(
            start,
            Decimal("100"),
            Decimal("101"),
            Decimal("99"),
            Decimal("100"),
        )
    ]
    if gap_open is not None:
        candles.append(
            make_candle(
                start + INTERVAL_MS,
                Decimal("100"),
                Decimal("105"),
                Decimal("95"),
                Decimal("100"),
            )
        )
    candles.append(
        make_candle(
            start + len(candles) * INTERVAL_MS,
            gap_open or Decimal("100"),
            high,
            low,
            Decimal("100"),
        )
    )
    session_factory = _sqlite_backtest_session_factory(tmp_path)
    fee_config = {
        "maker": Decimal("0.0002"),
        "taker": Decimal("0.0006"),
    }
    signal_factory = _conditional_signal_factory(
        "conditional_parity",
        candles[0].timestamp,
        entry_type=entry_type,
        stop_loss=stop_loss,
        take_profit=take_profit,
        trailing_distance=trailing_distance,
    )

    full_runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        max_drawdown_limit=None,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
        report_config={
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        db_session_factory=session_factory,
    )
    full_runner.add_strategy(
        CallableStrategy(
            "conditional_parity",
            signal_factory,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )
    research_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
    )
    research_runner.add_strategy(
        CallableStrategy(
            "conditional_parity",
            signal_factory,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    full_result = full_runner.run()
    research_result = research_runner.run()

    with session_factory() as session:
        full_trades = session.scalars(select(BacktestTradeLog)).all()
    full_closed_trades = calculate_metrics(full_trades)["closed_trades"]

    assert full_result["candle_count"] == research_result["candle_count"] == len(candles)
    assert full_result["total_trades"] == research_result["total_trades"] == 1
    assert full_result["total_pnl"] == research_result["total_pnl"]
    assert research_result["closed_trades"] == full_closed_trades
    assert research_result["closed_trades"][0].exit_price == expected_exit


@pytest.mark.smoke
def test_conditional_orders_do_not_survive_explicit_exit_or_repeated_entry(
    tmp_path,
):
    start = 1_700_000_000_000
    candles = [
        make_candle(
            start + index * INTERVAL_MS,
            Decimal("100"),
            Decimal("115") if index == 4 else Decimal("101"),
            Decimal("85") if index == 4 else Decimal("99"),
            Decimal("100"),
        )
        for index in range(5)
    ]

    def predict(candle):
        index = (candle.timestamp - start) // INTERVAL_MS
        if index in {0, 3}:
            return Signal(
                strategy_id="conditional_cycles",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("1"),
                stop_loss=Decimal("90"),
                take_profit=Decimal("110"),
            )
        if index == 2:
            return Signal(
                strategy_id="conditional_cycles",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=Decimal("1"),
            )
        return None

    session_factory = _sqlite_backtest_session_factory(tmp_path)
    fee_config = {
        "maker": Decimal("0.0002"),
        "taker": Decimal("0.0006"),
    }
    full_runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        max_drawdown_limit=None,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
        report_config={
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        db_session_factory=session_factory,
    )
    full_runner.add_strategy(
        CallableStrategy(
            "conditional_cycles",
            predict,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )
    research_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
    )
    research_runner.add_strategy(
        CallableStrategy(
            "conditional_cycles",
            predict,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    full_result = full_runner.run()
    research_result = research_runner.run()

    with session_factory() as session:
        full_trades = session.scalars(select(BacktestTradeLog)).all()
    full_closed_trades = calculate_metrics(full_trades)["closed_trades"]

    assert full_result["total_trades"] == research_result["total_trades"] == 2
    assert research_result["raw_trade_count"] == 4
    assert [trade.exit_price for trade in full_closed_trades] == [
        Decimal("100"),
        Decimal("90"),
    ]
    assert research_result["closed_trades"] == full_closed_trades
    assert research_result["total_pnl"] == full_result["total_pnl"]


def test_research_backtest_syncs_capital_lifecycle_after_fills():
    candles = make_candle_series(count=4)
    strategy = CapitalLifecycleProbeStrategy()
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("5000"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 2
    assert strategy.decisions == ["enter", "exit"]
    assert strategy.contexts[0].capital is not None
    assert strategy.contexts[0].capital.used == Decimal("0")
    assert strategy.contexts[1].capital is not None
    assert strategy.contexts[1].capital.used == Decimal("0.01") * candles[1].close
    assert strategy.contexts[1].capital.available == (
        Decimal("5000") - strategy.contexts[1].capital.used
    )
    assert strategy.contexts[2].capital is not None
    assert strategy.contexts[2].capital.used == Decimal("0")
    assert strategy.contexts[2].capital.available == Decimal("5000")
    assert allocator.get_used(strategy.strategy_id) == Decimal("0")


@pytest.mark.parametrize(
    ("spec", "expected_usage"),
    [
        (
            InstrumentSpec(
                product_id=PRODUCT_ID,
                exchange="test",
                symbol="MNQ",
                base="MNQ",
                quote="USD",
                multiplier=Decimal("2"),
                capital_model=CapitalModel.NOTIONAL,
            ),
            lambda candle: Decimal("0.01") * candle.close * Decimal("2"),
        ),
        (
            InstrumentSpec(
                product_id=PRODUCT_ID,
                exchange="test",
                symbol="MNQ",
                base="MNQ",
                quote="USD",
                multiplier=Decimal("2"),
                capital_model=CapitalModel.PER_CONTRACT,
                capital_per_contract=Decimal("2500"),
            ),
            lambda candle: Decimal("25"),
        ),
    ],
)
def test_research_backtest_capital_models_cover_full_lifecycle(spec, expected_usage):
    candles = make_candle_series(count=4)
    strategy = CapitalLifecycleProbeStrategy()
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("5000"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
        instrument_spec=spec,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 2
    assert strategy.contexts[1].capital.used == expected_usage(candles[1])
    assert strategy.contexts[2].capital.used == Decimal("0")
    assert allocator.get_used(strategy.strategy_id) == Decimal("0")


def test_research_backtest_exit_without_quantity_closes_current_position():
    candles = make_candle_series(count=4)

    def predict(candle):
        index = (candle.timestamp - candles[0].timestamp) // INTERVAL_MS
        if index == 0:
            return Signal(
                strategy_id="research_exit_full",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.03"),
            )
        if index == 2:
            return Signal(
                strategy_id="research_exit_full",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=None,
            )
        return None

    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
    )
    runner.add_strategy(
        CallableStrategy(
            "research_exit_full",
            predict,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    result = runner.run()

    assert result["raw_trade_count"] == 2
    assert result["raw_trades"][0].quantity == Decimal("0.03")
    assert result["raw_trades"][1].quantity == Decimal("0.03")
    assert result["total_trades"] == 1


def test_research_backtest_rejects_entry_over_available_capital():
    candles = make_candle_series(count=3)
    strategy = CapitalGatedEntryStrategy()
    allocator = CapitalAllocator(Decimal("100000"))
    allocator.allocate(strategy.strategy_id, Decimal("100"))
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        capital_allocator=allocator,
    )
    runner.add_strategy(strategy)

    result = runner.run()

    assert result["raw_trade_count"] == 0
    assert result["total_trades"] == 0
    assert strategy.contexts[0].capital is not None
    assert strategy.contexts[0].capital.available == Decimal("100")
    assert strategy.contexts[0].latest_rejections == ()
    assert len(strategy.contexts[1].latest_rejections) == 1
    assert "capital_allocation_rejected" in strategy.contexts[1].latest_rejections[0].reason
    assert allocator.get_used(strategy.strategy_id) == Decimal("0")


def test_research_backtest_prepared_scaled_path_matches_decimal_path():
    candles = make_candle_series(count=600)
    fee_config = {"maker": 0.0002, "taker": 0.0006}
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.001"),
        )
    )

    decimal_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
    )
    decimal_runner.add_strategy(
        CallableStrategy(
            "research_scaled_parity",
            _signal_factory("research_scaled_parity", candles),
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    scaled_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
        precision_codec=codec,
    )
    scaled_runner.add_strategy(
        CallableStrategy(
            "research_scaled_parity",
            _signal_factory("research_scaled_parity", candles),
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    decimal_result = decimal_runner.run()
    scaled_result = scaled_runner.run()

    assert scaled_result["candle_count"] == decimal_result["candle_count"] == len(candles)
    assert scaled_result["raw_trade_count"] == decimal_result["raw_trade_count"]
    assert scaled_result["total_trades"] == decimal_result["total_trades"]
    assert scaled_result["total_pnl"] == decimal_result["total_pnl"]
    assert scaled_result["profit_factor"] == decimal_result["profit_factor"]
    assert [
        (trade.side, trade.price, trade.quantity, trade.fee, trade.timestamp)
        for trade in scaled_result["raw_trades"]
    ] == [
        (trade.side, trade.price, trade.quantity, trade.fee, trade.timestamp)
        for trade in decimal_result["raw_trades"]
    ]


def test_research_backtest_reuses_prepared_scaled_candles():
    candles = make_candle_series(count=600)
    fee_config = {"maker": 0.0002, "taker": 0.0006}
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.001"),
        )
    )
    prepared = ResearchBacktestRunner.prepare_scaled_candles(candles, codec)

    decimal_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
    )
    decimal_runner.add_strategy(
        CallableStrategy(
            "research_prepared_scaled",
            _signal_factory("research_prepared_scaled", candles),
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    scaled_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config=fee_config,
        precision_codec=codec,
        prepared_scaled_candles=prepared,
    )
    scaled_runner.add_strategy(
        CallableStrategy(
            "research_prepared_scaled",
            _signal_factory("research_prepared_scaled", candles),
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    decimal_result = decimal_runner.run()
    scaled_result = scaled_runner.run()

    assert scaled_result["total_pnl"] == decimal_result["total_pnl"]
    assert scaled_result["raw_trade_count"] == decimal_result["raw_trade_count"]
    assert [
        (trade.side, trade.price, trade.quantity, trade.fee, trade.timestamp)
        for trade in scaled_result["raw_trades"]
    ] == [
        (trade.side, trade.price, trade.quantity, trade.fee, trade.timestamp)
        for trade in decimal_result["raw_trades"]
    ]


def test_research_backtest_rejects_misaligned_prepared_scaled_candles():
    candles = make_candle_series(count=10)
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.001"),
        )
    )
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        data_source=MemoryDataSource(candles),
        precision_codec=codec,
        prepared_scaled_candles=[],
    )
    runner.add_strategy(
        CallableStrategy(
            "research_misaligned_scaled",
            _signal_factory("research_misaligned_scaled", candles),
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    with pytest.raises(ValueError, match="prepared_scaled_candles length"):
        runner.run()


def test_research_backtest_rejects_out_of_order_prepared_scaled_candles():
    candles = make_candle_series(count=10)
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.001"),
        )
    )
    prepared = ResearchBacktestRunner.prepare_scaled_candles(candles, codec)
    prepared[0], prepared[1] = prepared[1], prepared[0]
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        data_source=MemoryDataSource(candles),
        precision_codec=codec,
        prepared_scaled_candles=prepared,
    )
    runner.add_strategy(
        CallableStrategy(
            "research_out_of_order_scaled",
            _signal_factory("research_out_of_order_scaled", candles),
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    with pytest.raises(ValueError, match="prepared_scaled_candles timestamp"):
        runner.run()
