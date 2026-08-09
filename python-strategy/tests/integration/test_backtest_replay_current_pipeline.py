"""Current backtest pipeline replay coverage.

This test intentionally follows the post-hardening architecture instead of
older mocked SessionLocal helpers:

MemoryDataSource -> BacktestRunner -> StrategyEngine -> SignalProcessor
-> RiskManager -> ExecutionEngine -> SimulatedAdapter/Rust matcher
-> BacktestOrderRepository -> real SQLite trade log/summary rows.
"""

import json
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, func, select
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
from src.strategies.callable_strategy import CallableStrategy
from src.validation.backtest_capture import capture_signal_batch
from src.validation.trading_outcome import SignalObservation

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


def _sqlite_backtest_session_factory(tmp_path, request: pytest.FixtureRequest):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'current_backtest_replay.db'}",
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
    request.addfinalizer(engine.dispose)
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


@pytest.mark.smoke
def test_current_backtest_replay_persists_trades_and_metrics(
    tmp_path, request: pytest.FixtureRequest
):
    session_factory = _sqlite_backtest_session_factory(tmp_path, request)
    candles = make_candle_series(count=2_000)
    observed_batches: list[tuple[SignalObservation, ...]] = []

    def capture_batch(batch: tuple[Signal, ...]) -> None:
        observed_batches.append(capture_signal_batch(batch))

    def predict(candle):
        index = (candle.timestamp - candles[0].timestamp) // INTERVAL_MS
        if index % 80 == 10:
            return Signal(
                strategy_id="current_replay",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.01"),
            )
        if index % 80 == 40:
            return Signal(
                strategy_id="current_replay",
                product_id=PRODUCT_ID,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.EXIT_LONG,
                quantity=Decimal("0.01"),
            )
        return None

    runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=PRODUCT_ID,
        timeframe=TIMEFRAME,
        initial_balance=10_000.0,
        data_source=MemoryDataSource(candles),
        fee_config={"maker": 0.0002, "taker": 0.0006},
        report_config={
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        db_session_factory=session_factory,
        signal_batch_observer=capture_batch,
    )
    runner.add_strategy(
        CallableStrategy("current_replay", predict, PRODUCT_ID, TIMEFRAME)
    )

    result = runner.run()

    with session_factory() as session:
        summary = session.scalars(select(BacktestResultSummary)).one()
        trade_count = session.scalar(select(func.count()).select_from(BacktestTradeLog))
        audit_count = session.scalar(select(func.count()).select_from(SignalAudit))
        strategy_ids = set(session.scalars(select(BacktestTradeLog.strategy_id)).all())
        fill_sequences = list(
            session.scalars(
                select(BacktestTradeLog.fill_sequence).order_by(
                    BacktestTradeLog.fill_sequence
                )
            ).all()
        )

    assert result is not None
    assert isinstance(summary.metrics_json, str)
    assert isinstance(summary.total_pnl, Decimal)
    assert trade_count is not None
    metrics = json.loads(summary.metrics_json)

    assert trade_count == 50
    assert fill_sequences == list(range(50))
    assert audit_count == 50
    assert strategy_ids == {"current_replay"}
    observed_signals = tuple(signal for batch in observed_batches for signal in batch)
    assert len(observed_batches) == len(candles)
    assert len(observed_signals) == len(candles)
    assert all(type(signal) is SignalObservation for signal in observed_signals)
    assert sum(signal.signal_type != "NO_SIGNAL" for signal in observed_signals) == 50
    metadata_values: set[str] = set()
    for signal in observed_signals:
        assert "client_order_id" in signal.metadata_json
        metadata_values.add(signal.metadata_json)
    assert len(metadata_values) == len(candles)
    assert result["journal_count"] >= trade_count
    assert result["total_trades"] == 25
    assert metrics["total_trades"] == 25
    assert Decimal(str(summary.total_pnl)) == result["total_pnl"]
    assert Decimal(str(summary.total_pnl)) != Decimal("0")
