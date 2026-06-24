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

from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle_series
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.memory import MemoryDataSource
from src.core.models import Signal, SignalType
from src.core.orm_models import (
    BacktestResultSummary,
    BacktestTradeLog,
    Exchange,
    Product,
    SignalAudit,
    Strategy,
)
from src.core.precision import PrecisionCodec, PrecisionSpec
from src.core.research_backtest_runner import ResearchBacktestRunner
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


def _signal_factory(strategy_id: str, candles: list):
    def predict(candle):
        index = (candle.timestamp - candles[0].timestamp) // INTERVAL_MS
        if index % 80 == 10:
            return Signal(
                strategy_id=strategy_id,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.01"),
            )
        if index % 80 == 40:
            return Signal(
                strategy_id=strategy_id,
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=Decimal("0.01"),
            )
        return None

    return predict


@pytest.mark.smoke
def test_research_backtest_matches_full_runner_core_metrics(tmp_path):
    session_factory = _sqlite_backtest_session_factory(tmp_path)
    candles = make_candle_series(count=2_000)
    fee_config = {"maker": 0.0002, "taker": 0.0006}

    full_runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
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
            "research_parity",
            _signal_factory("research_parity", candles),
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
    )
    research_runner.add_strategy(
        CallableStrategy(
            "research_parity",
            _signal_factory("research_parity", candles),
            PRODUCT_ID,
            TIMEFRAME,
        )
    )

    full_result = full_runner.run()
    research_result = research_runner.run()

    with session_factory() as session:
        summary = session.scalars(select(BacktestResultSummary)).one()

    metrics = json.loads(summary.metrics_json)

    assert research_result["candle_count"] == len(candles)
    assert research_result["raw_trade_count"] == 50
    assert research_result["total_trades"] == full_result["total_trades"] == 25
    assert research_result["total_trades"] == metrics["total_trades"]
    assert research_result["total_pnl"] == full_result["total_pnl"]
    assert research_result["profit_factor"] == full_result["profit_factor"]


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
