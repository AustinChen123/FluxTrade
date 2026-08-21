"""Research backtest path parity checks.

The research runner is intentionally lighter than BacktestRunner, but its
basic fill ordering and fee-aware PnL should stay aligned for simple signal
strategies. These tests protect that contract without asserting speed.
"""

import hashlib
import json
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle, make_candle_series
from src.core.analytics import (
    annualized_sharpe_from_moments,
    calculate_metrics,
    utc_daily_return_metrics,
)
from src.core.backtest_runner import BacktestRunner
from src.core.capital_allocator import CapitalAllocator
from src.core.data_sources.memory import MemoryDataSource
from src.core.fast_bar import FastBarReplayRunner, MarketTape, SignalIntent
from src.core.models import (
    Candlestick,
    OrderSide,
    PositionSide,
    Signal,
    SignalType,
)
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
from src.strategies.representative_benchmark import RepresentativeBenchmarkStrategy

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False


def test_utc_daily_sharpe_uses_close_to_close_equity_returns():
    day_ms = 86_400_000
    metrics = utc_daily_return_metrics(
        [
            (0, Decimal("100")),
            (day_ms, Decimal("101")),
            (2 * day_ms, Decimal("103.02")),
        ],
        initial_balance=Decimal("100"),
        start_time=0,
        end_time=2 * day_ms,
    )
    count = metrics["count"]
    sum_ = metrics["sum"]
    sum_squares = metrics["sum_squares"]
    sum_cubes = metrics["sum_cubes"]
    sum_fourth = metrics["sum_fourth"]
    assert isinstance(count, int)
    assert isinstance(sum_, Decimal)
    assert isinstance(sum_squares, Decimal)
    assert isinstance(sum_cubes, Decimal)
    assert isinstance(sum_fourth, Decimal)
    moments: dict[str, Decimal | int] = {
        "count": count,
        "sum": sum_,
        "sum_squares": sum_squares,
        "sum_cubes": sum_cubes,
        "sum_fourth": sum_fourth,
    }

    assert moments == {
        "count": 3,
        "sum": Decimal("0.03"),
        "sum_squares": Decimal("0.0005"),
        "sum_cubes": Decimal("0.000009"),
        "sum_fourth": Decimal("0.00000017"),
    }
    assert annualized_sharpe_from_moments(moments) == Decimal(365).sqrt()


def test_utc_daily_sharpe_carries_leading_internal_and_trailing_days():
    day_ms = 86_400_000
    metrics = utc_daily_return_metrics(
        [
            (day_ms, Decimal("100")),
            (3 * day_ms, Decimal("101")),
        ],
        initial_balance=Decimal("100"),
        start_time=0,
        end_time=4 * day_ms,
    )

    assert metrics["count"] == 5
    assert metrics["sum"] == Decimal("0.01")


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


def _trade_digest(trades) -> str:
    def decimal_text(value) -> str:
        normalized = Decimal(value).normalize()
        return "0" if normalized == 0 else format(normalized, "f")

    rows = []
    for trade in trades:
        side = getattr(trade.side, "value", trade.side)
        rows.append(
            (
                int(trade.timestamp),
                str(side).lower(),
                decimal_text(trade.price),
                decimal_text(trade.quantity),
                decimal_text(trade.fee),
            )
        )
    rows.sort()
    return hashlib.sha256(repr(rows).encode()).hexdigest()


def _net_quantity(trades) -> Decimal:
    quantity = Decimal("0")
    for trade in trades:
        side = getattr(trade.side, "value", trade.side)
        quantity += trade.quantity if str(side).lower() == "buy" else -trade.quantity
    return quantity


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


class OrderIntentParityProbeStrategy(BaseStrategy):
    def __init__(
        self,
        invalid_field: str | None,
        strategy_id: str = "order_intent_parity",
    ) -> None:
        super().__init__(strategy_id, PRODUCT_ID)
        self.invalid_field = invalid_field
        self.contexts: list[StrategyContext] = []
        self.first_timestamp: int | None = None

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(PRODUCT_ID, TIMEFRAME, 1)

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal | None:
        if context is not None:
            self.contexts.append(context)
        if self.first_timestamp is None:
            self.first_timestamp = candle.timestamp
        index = (candle.timestamp - self.first_timestamp) // INTERVAL_MS

        if self.invalid_field is not None and index == 0:
            signal = self._entry_signal(candle)
            return signal.model_copy(update={self.invalid_field: Decimal("0")})

        entry_index = 1 if self.invalid_field is not None else 0
        if index == entry_index:
            return self._entry_signal(candle)
        if index == entry_index + 2:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=Decimal("0.01"),
            )
        return None

    def _entry_signal(self, candle: Candlestick) -> Signal:
        return Signal(
            strategy_id=self.strategy_id,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            value=Decimal("99"),
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
    assert full_result is not None

    with session_factory() as session:
        summary = session.scalars(select(BacktestResultSummary)).one()
        full_trades = session.scalars(select(BacktestTradeLog)).all()

    assert summary.metrics_json is not None
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


@pytest.mark.parametrize(
    (
        "scenario",
        "entry_type",
        "candle_count",
        "expected_position_side",
        "expected_working_orders",
        "expected_protection_orders",
    ),
    [
        ("empty", None, 0, None, 0, 0),
        ("zero-mark", None, 1, None, 0, 0),
        ("negative-mark", None, 1, None, 0, 0),
        ("pending-protected-entry", SignalType.LONG, 1, None, 3, 2),
        ("open-long", SignalType.LONG, 2, PositionSide.LONG, 0, 0),
        ("open-long-trailing", SignalType.LONG, 3, PositionSide.LONG, 1, 1),
        ("open-short", SignalType.SHORT, 2, PositionSide.SHORT, 0, 0),
        ("flat", SignalType.LONG, 3, None, 0, 0),
    ],
)
def test_full_and_research_runners_share_canonical_endpoint_state(
    tmp_path,
    scenario,
    entry_type,
    candle_count,
    expected_position_side,
    expected_working_orders,
    expected_protection_orders,
):
    start = 1_700_000_000_000
    endpoint_price = {
        "zero-mark": Decimal("0"),
        "negative-mark": Decimal("-1"),
    }.get(scenario, Decimal("100"))
    candles = []
    for index in range(candle_count):
        if scenario == "open-long-trailing" and index == 2:
            open_price = Decimal("107")
            high_price = Decimal("110")
            low_price = Decimal("106")
            close_price = Decimal("108")
        else:
            open_price = endpoint_price
            high_price = endpoint_price + Decimal("1")
            low_price = endpoint_price - Decimal("1")
            close_price = endpoint_price
        candles.append(
            make_candle(
                start + index * INTERVAL_MS,
                open_price,
                high_price,
                low_price,
                close_price,
            )
        )

    def predict(candle):
        index = (candle.timestamp - start) // INTERVAL_MS
        if index == 0 and entry_type is not None:
            return Signal(
                strategy_id="endpoint_parity",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=entry_type,
                quantity=Decimal("1"),
                stop_loss=(
                    Decimal("90") if scenario == "pending-protected-entry" else None
                ),
                take_profit=(
                    Decimal("110") if scenario == "pending-protected-entry" else None
                ),
                trailing_distance=(
                    Decimal("5") if scenario == "open-long-trailing" else None
                ),
            )
        if index == 1 and scenario == "flat":
            return Signal(
                strategy_id="endpoint_parity",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=Decimal("1"),
            )
        return None

    end = candles[-1].timestamp if candles else start
    common = {
        "start_time": start,
        "end_time": end,
        "product_id": PRODUCT_ID,
        "timeframe": TIMEFRAME,
        "initial_balance": 10_000,
        "max_drawdown_limit": None,
        "data_source": MemoryDataSource(candles),
    }
    full_runner = BacktestRunner(
        **common,
        report_config={
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        db_session_factory=_sqlite_backtest_session_factory(tmp_path),
    )
    full_runner.add_strategy(
        CallableStrategy(
            "endpoint_parity",
            predict,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )
    research_runner = ResearchBacktestRunner(**common)
    research_runner.add_strategy(
        CallableStrategy(
            "endpoint_parity",
            predict,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    full_result = full_runner.run()
    assert full_result is not None
    full_endpoint = full_result["endpoint_state"]
    research_endpoint = research_runner.run()["endpoint_state"]

    assert full_endpoint.model_dump() == research_endpoint.model_dump()
    assert full_endpoint.halted_early is False
    assert len(full_endpoint.positions) == (expected_position_side is not None)
    assert len(full_endpoint.working_orders) == expected_working_orders
    assert len(full_endpoint.protection_orders) == expected_protection_orders
    if candles:
        assert full_endpoint.final_mark == candles[-1].close
        assert full_endpoint.end_timestamp == candles[-1].timestamp
    else:
        assert full_endpoint.final_mark is None
        assert full_endpoint.end_timestamp is None
    if expected_position_side is not None:
        position = full_endpoint.positions[0]
        assert position.strategy_id == "endpoint_parity"
        assert position.product_id == PRODUCT_ID
        assert position.side == expected_position_side
        assert position.quantity == Decimal("1")
        assert position.average_entry_price == Decimal("100")
    if scenario == "pending-protected-entry":
        assert {order.order_type for order in full_endpoint.working_orders} == {
            "MARKET",
            "STOP_LOSS",
            "TAKE_PROFIT",
        }
        assert {order.side for order in full_endpoint.protection_orders} == {
            OrderSide.SELL
        }
    if scenario == "open-long-trailing":
        trailing = full_endpoint.protection_orders[0]
        assert trailing.order_type == "TRAILING_STOP"
        assert trailing.trigger_price == Decimal("105")
        assert trailing.trailing_distance == Decimal("5")


@pytest.mark.parametrize(
    ("entry_type", "expected_side", "expected_unrealized"),
    [
        (SignalType.LONG, PositionSide.LONG, Decimal("2")),
        (SignalType.SHORT, PositionSide.SHORT, Decimal("-2")),
    ],
)
def test_three_runners_share_open_endpoint_and_mark_to_market(
    tmp_path,
    entry_type,
    expected_side,
    expected_unrealized,
):
    start = 1_700_000_000_000
    candles = [
        make_candle(
            start,
            Decimal("100"),
            Decimal("101"),
            Decimal("99"),
            Decimal("100"),
        ),
        make_candle(
            start + INTERVAL_MS,
            Decimal("99"),
            Decimal("100"),
            Decimal("98"),
            Decimal("99"),
        ),
        make_candle(
            start + 2 * INTERVAL_MS,
            Decimal("101"),
            Decimal("102"),
            Decimal("100"),
            Decimal("101"),
        ),
        make_candle(
            start + 3 * INTERVAL_MS,
            Decimal("102"),
            Decimal("104"),
            Decimal("101"),
            Decimal("103"),
        ),
    ]
    instrument = InstrumentSpec(
        product_id=PRODUCT_ID,
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        multiplier=Decimal("2"),
    )
    taker_fee = Decimal("0.001")

    def predict(candle):
        if candle.timestamp != candles[2].timestamp:
            return None
        return Signal(
            strategy_id="three_runner_endpoint",
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            timestamp=candle.timestamp,
            type=entry_type,
            quantity=Decimal("1"),
        )

    class PreparedStrategy:
        strategy_id = "three_runner_endpoint"

        def on_bar(self, bar):
            if bar.timestamp != candles[2].timestamp:
                return None
            return SignalIntent(entry_type, quantity=Decimal("1"))

    full_runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        max_drawdown_limit=None,
        data_source=MemoryDataSource(candles),
        fee_config={"maker": Decimal("0"), "taker": taker_fee},
        report_config={
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        db_session_factory=_sqlite_backtest_session_factory(tmp_path),
        instrument_spec=instrument,
    )
    full_runner.add_strategy(
        CallableStrategy(
            "three_runner_endpoint",
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
        fee_config={"maker": Decimal("0"), "taker": taker_fee},
        max_drawdown_limit=None,
        instrument_spec=instrument,
    )
    research_runner.add_strategy(
        CallableStrategy(
            "three_runner_endpoint",
            predict,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )
    fast_runner = FastBarReplayRunner(
        tape=MarketTape.from_candles(
            candles,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
        ),
        strategy=PreparedStrategy(),
        initial_balance=Decimal("10000"),
        taker_fee=taker_fee,
        instrument_spec=instrument,
    )

    full_result = full_runner.run()
    research_result = research_runner.run()
    fast_result = fast_runner.run()
    assert full_result is not None
    endpoints = [
        result["endpoint_state"].model_dump(mode="json")
        for result in (full_result, research_result, fast_result)
    ]

    assert endpoints[0] == endpoints[1] == endpoints[2]
    assert len(full_result["endpoint_state"].positions) == 1
    assert full_result["endpoint_state"].positions[0].side == expected_side
    assert full_result["endpoint_state"].working_orders == ()
    expected_entry_fee = candles[3].open * Decimal("2") * taker_fee
    assert (
        full_result["mark_to_market_pnl"]
        == research_result["mark_to_market_pnl"]
        == fast_result["mark_to_market_pnl"]
        == expected_unrealized - expected_entry_fee
    )


@pytest.mark.parametrize("invalid_field", [None, "price", "value"])
def test_full_and_research_runners_share_order_intent_outcomes(
    tmp_path,
    invalid_field,
):
    start = 1_700_000_000_000
    candles = [
        make_candle(
            start + index * INTERVAL_MS,
            Decimal("100"),
            Decimal("101"),
            Decimal("98"),
            Decimal("100"),
        )
        for index in range(5)
    ]
    session_factory = _sqlite_backtest_session_factory(tmp_path)
    report_config = {
        "csv_trades": False,
        "markdown_report": False,
        "equity_curve": False,
        "journal_export": False,
    }
    full_strategy = OrderIntentParityProbeStrategy(invalid_field)
    research_strategy = OrderIntentParityProbeStrategy(invalid_field)

    full_runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        max_drawdown_limit=None,
        data_source=MemoryDataSource(candles),
        report_config=report_config,
        db_session_factory=session_factory,
    )
    full_runner.add_strategy(full_strategy)
    research_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        max_drawdown_limit=None,
        data_source=MemoryDataSource(candles),
    )
    research_runner.add_strategy(research_strategy)

    full_result = full_runner.run()
    research_result = research_runner.run()
    assert full_result is not None

    assert full_result["closed_trade_count"] == 1
    assert research_result["closed_trade_count"] == 1
    assert full_result["total_trades"] == research_result["total_trades"] == 1
    assert full_result["total_pnl"] == research_result["total_pnl"]
    assert research_result["raw_trade_count"] == 2
    expected_rejections = 0 if invalid_field is None else 1
    assert research_result["invalid_order_intent_count"] == expected_rejections
    assert (
        len(research_result["invalid_order_intent_rejections"]) == expected_rejections
    )

    with session_factory() as session:
        full_trades = list(
            session.scalars(
                select(BacktestTradeLog).order_by(
                    BacktestTradeLog.timestamp,
                    BacktestTradeLog.fill_sequence,
                    BacktestTradeLog.id,
                )
            )
        )
        full_rejections = list(
            session.scalars(
                select(SignalAudit).where(SignalAudit.risk_status == "REJECT")
            )
        )
    assert full_trades[0].price == Decimal("99")
    assert research_result["raw_trades"][0].price == Decimal("99")
    assert len(full_rejections) == expected_rejections
    if invalid_field is not None:
        expected_reason = f"signal.{invalid_field}"
        assert full_rejections[0].risk_message is not None
        assert expected_reason in full_rejections[0].risk_message
        assert expected_reason in (
            research_result["invalid_order_intent_rejections"][0].reason
        )
        assert any(
            expected_reason in rejection.reason
            for context in research_strategy.contexts
            for rejection in context.latest_rejections
        )


def test_invalid_order_intent_results_preserve_multistrategy_ownership():
    candles = make_candle_series(count=3)
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        data_source=MemoryDataSource(candles),
    )
    strategy_a = OrderIntentParityProbeStrategy("price", "strategy-a")
    strategy_b = OrderIntentParityProbeStrategy("value", "strategy-b")
    runner.add_strategy(strategy_a)
    runner.add_strategy(strategy_b)

    result = runner.run()

    assert result["invalid_order_intent_count"] == 2
    rejections_by_strategy = {
        rejection.strategy_id: rejection
        for rejection in result["invalid_order_intent_rejections"]
    }
    assert set(rejections_by_strategy) == {"strategy-a", "strategy-b"}
    assert rejections_by_strategy["strategy-a"].product_id == PRODUCT_ID
    assert "signal.price" in rejections_by_strategy["strategy-a"].reason
    assert rejections_by_strategy["strategy-b"].product_id == PRODUCT_ID
    assert "signal.value" in rejections_by_strategy["strategy-b"].reason

    assert strategy_a.contexts[0].latest_rejections == ()
    assert len(strategy_a.contexts[1].latest_rejections) == 1
    assert "signal.price" in strategy_a.contexts[1].latest_rejections[0].reason
    assert strategy_a.contexts[2].latest_rejections == ()

    assert strategy_b.contexts[0].latest_rejections == ()
    assert len(strategy_b.contexts[1].latest_rejections) == 1
    assert "signal.value" in strategy_b.contexts[1].latest_rejections[0].reason
    assert strategy_b.contexts[2].latest_rejections == ()


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
    assert full_result is not None

    with session_factory() as session:
        full_trades = session.scalars(select(BacktestTradeLog)).all()
    full_closed_trades = calculate_metrics(full_trades)["closed_trades"]

    assert (
        full_result["candle_count"] == research_result["candle_count"] == len(candles)
    )
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
    assert full_result is not None

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


@pytest.mark.smoke
def test_protective_fill_then_same_bar_exit_does_not_reverse(tmp_path):
    start = 1_700_000_000_000
    candles = [
        make_candle(
            start, Decimal("100"), Decimal("101"), Decimal("99"), Decimal("100")
        ),
        make_candle(
            start + INTERVAL_MS,
            Decimal("100"),
            Decimal("101"),
            Decimal("99"),
            Decimal("100"),
        ),
        make_candle(
            start + 2 * INTERVAL_MS,
            Decimal("95"),
            Decimal("96"),
            Decimal("85"),
            Decimal("88"),
        ),
        make_candle(
            start + 3 * INTERVAL_MS,
            Decimal("88"),
            Decimal("89"),
            Decimal("87"),
            Decimal("88"),
        ),
    ]

    def predict(candle):
        index = (candle.timestamp - start) // INTERVAL_MS
        if index == 0:
            return Signal(
                strategy_id="protective_exit_probe",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("1"),
                stop_loss=Decimal("90"),
            )
        if index == 2:
            return Signal(
                strategy_id="protective_exit_probe",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=Decimal("1"),
            )
        return None

    fee_config = {"maker": Decimal("0"), "taker": Decimal("0")}
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
        db_session_factory=_sqlite_backtest_session_factory(tmp_path),
    )
    full_runner.add_strategy(
        CallableStrategy(
            "protective_exit_probe",
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
            "protective_exit_probe",
            predict,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    full_result = full_runner.run()
    research_result = research_runner.run()
    assert full_result is not None
    assert full_runner.engine is not None
    full_position = full_runner.engine.execution_engine.adapter.get_position(
        PRODUCT_ID,
        strategy_id="protective_exit_probe",
    )

    assert full_position is None
    assert full_result["total_trades"] == research_result["total_trades"] == 1
    assert full_result["total_pnl"] == research_result["total_pnl"]
    assert research_result["raw_trade_count"] == 2


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
    assert strategy.contexts[1].capital is not None
    assert strategy.contexts[2].capital is not None
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


def test_full_and_research_backtests_cap_exit_to_current_position(tmp_path):
    candles = make_candle_series(count=4)

    def predict(candle):
        index = (candle.timestamp - candles[0].timestamp) // INTERVAL_MS
        if index == 0:
            return Signal(
                strategy_id="research_exit_cap",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.03"),
            )
        if index == 2:
            return Signal(
                strategy_id="research_exit_cap",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=Decimal("1"),
            )
        return None

    session_factory = _sqlite_backtest_session_factory(tmp_path)
    full_runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
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
            "research_exit_cap",
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
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
    )
    research_runner.add_strategy(
        CallableStrategy(
            "research_exit_cap",
            predict,
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    full_runner.run()
    research_result = research_runner.run()
    with session_factory() as session:
        full_trades = session.scalars(select(BacktestTradeLog)).all()

    assert research_result["raw_trade_count"] == 2
    assert [trade.quantity for trade in research_result["raw_trades"]] == [
        Decimal("0.03"),
        Decimal("0.03"),
    ]
    assert [trade.quantity for trade in full_trades] == [
        Decimal("0.03"),
        Decimal("0.03"),
    ]
    assert (
        _net_quantity(research_result["raw_trades"])
        == _net_quantity(full_trades)
        == Decimal("0")
    )


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
    assert (
        "capital_allocation_rejected"
        in strategy.contexts[1].latest_rejections[0].reason
    )
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

    assert (
        scaled_result["candle_count"] == decimal_result["candle_count"] == len(candles)
    )
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


def test_representative_strategy_matches_full_and_prepared_research_paths(tmp_path):
    start = 1_700_000_000_000
    closes = [
        Decimal(value)
        for value in (
            100,
            101,
            102,
            103,
            104,
            105,
            106,
            105,
            104,
            103,
            102,
            101,
            100,
            99,
            98,
            97,
            98,
            99,
            100,
            101,
            102,
            103,
            104,
            103,
            102,
            101,
            100,
            99,
            98,
            97,
        )
    ]
    candles = [
        make_candle(
            start + index * INTERVAL_MS,
            close,
            close + Decimal("0.5"),
            close - Decimal("0.5"),
            close,
            volume=Decimal(100 + index % 4),
        )
        for index, close in enumerate(closes)
    ]
    session_factory = _sqlite_backtest_session_factory(tmp_path)
    instrument = InstrumentSpec(
        product_id=PRODUCT_ID,
        exchange="binance",
        symbol="BTC/USDT:USDT",
        base="BTC",
        quote="USDT",
        multiplier=Decimal("1"),
        quantity_step=Decimal("1"),
        price_tick=Decimal("0.25"),
        capital_model=CapitalModel.PER_CONTRACT,
        capital_per_contract=Decimal("100"),
    )
    assert instrument.price_tick is not None
    assert instrument.quantity_step is not None
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=instrument.price_tick,
            quantity_step=instrument.quantity_step,
        )
    )
    prepared = ResearchBacktestRunner.prepare_scaled_candles(candles, codec)

    def strategy() -> RepresentativeBenchmarkStrategy:
        return RepresentativeBenchmarkStrategy(
            "representative_parity",
            PRODUCT_ID,
            timeframe=TIMEFRAME,
            trend_window=4,
            breakout_window=3,
            atr_window=3,
            rsi_window=3,
            volume_window=3,
            swing_window=1,
            entry_score=2,
            hold_bars=3,
            max_atr_expansion=Decimal("3"),
            quantity=Decimal("1"),
        )

    full_runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        max_drawdown_limit=None,
        data_source=MemoryDataSource(candles),
        fee_config={"maker": Decimal("0"), "taker": Decimal("0")},
        report_config={
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        db_session_factory=session_factory,
        instrument_spec=instrument,
    )
    full_runner.add_strategy(strategy())
    decimal_research_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        data_source=MemoryDataSource(candles),
        fee_config={"maker": Decimal("0"), "taker": Decimal("0")},
        instrument_spec=instrument,
    )
    decimal_research_runner.add_strategy(strategy())
    research_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000,
        data_source=MemoryDataSource(candles),
        fee_config={"maker": Decimal("0"), "taker": Decimal("0")},
        precision_codec=codec,
        prepared_scaled_candles=prepared,
        instrument_spec=instrument,
    )
    research_runner.add_strategy(strategy())

    full_result = full_runner.run()
    decimal_research_result = decimal_research_runner.run()
    research_result = research_runner.run()
    assert full_result is not None

    with session_factory() as session:
        full_trades = session.scalars(select(BacktestTradeLog)).all()
    full_closed_trades = calculate_metrics(full_trades)["closed_trades"]

    assert research_result["total_trades"] > 0
    assert (
        research_result["closed_trades"]
        == decimal_research_result["closed_trades"]
        == full_closed_trades
    )
    assert (
        research_result["total_pnl"]
        == decimal_research_result["total_pnl"]
        == full_result["total_pnl"]
    )
    assert (
        research_result["mark_to_market_pnl"]
        == decimal_research_result["mark_to_market_pnl"]
        == full_result["mark_to_market_pnl"]
    )
    assert (
        research_result["daily_return_moments"]
        == decimal_research_result["daily_return_moments"]
        == full_result["daily_return_moments"]
    )
    assert (
        research_result["annualized_sharpe"]
        == decimal_research_result["annualized_sharpe"]
        == full_result["annualized_sharpe"]
    )
    assert (
        research_result["max_drawdown"]
        == decimal_research_result["max_drawdown"]
        == full_result["max_drawdown"]
    )
    assert (
        research_result["calmar_ratio"]
        == decimal_research_result["calmar_ratio"]
        == full_result["calmar_ratio"]
    )
    assert (
        research_result["max_drawdown_days"]
        == decimal_research_result["max_drawdown_days"]
        == full_result["max_drawdown_days"]
    )
    assert (
        _trade_digest(research_result["raw_trades"])
        == _trade_digest(decimal_research_result["raw_trades"])
        == _trade_digest(full_trades)
    )
    assert (
        _net_quantity(research_result["raw_trades"])
        == _net_quantity(decimal_research_result["raw_trades"])
        == _net_quantity(full_trades)
    )
