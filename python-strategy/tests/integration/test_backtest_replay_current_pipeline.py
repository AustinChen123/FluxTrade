"""Current backtest pipeline replay coverage.

This test intentionally follows the post-hardening architecture instead of
older mocked SessionLocal helpers:

MemoryDataSource -> BacktestRunner -> StrategyEngine -> SignalProcessor
-> RiskManager -> ExecutionEngine -> SimulatedAdapter/Rust matcher
-> BacktestOrderRepository -> real SQLite trade log/summary rows.
"""

import json
from decimal import Decimal
from uuid import uuid4

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
from src.validation.backtest_capture import (
    build_normal_backtest_trading_outcome,
    capture_signal_batch,
)
from src.validation.trading_outcome import SignalObservation, TradingOutcome

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
        persisted_fills = tuple(
            {
                "id": trade.id,
                "strategy_id": trade.strategy_id,
                "order_id": trade.order_id,
                "exchange_trade_id": trade.exchange_trade_id,
                "product_id": trade.product_id,
                "side": trade.side,
                "price": trade.price,
                "quantity": trade.quantity,
                "fee": trade.fee,
                "fee_asset": trade.fee_asset,
                "timestamp": trade.timestamp,
                "fill_sequence": trade.fill_sequence,
            }
            for trade in session.scalars(
                select(BacktestTradeLog).order_by(
                    BacktestTradeLog.timestamp,
                    BacktestTradeLog.fill_sequence,
                    BacktestTradeLog.id,
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
    journal = result["journal"]
    assert type(journal) is list
    outcome = build_normal_backtest_trading_outcome(
        signals=observed_signals,
        fills=persisted_fills,
        journal=tuple(journal),
        endpoint_state=result["endpoint_state"],
        initial_balance=runner.initial_balance,
        total_pnl=result["total_pnl"],
    )
    assert type(outcome) is TradingOutcome
    assert len(outcome.signals) == len(candles)
    assert len(outcome.order_observations) == trade_count * 2
    assert len(outcome.fills) == trade_count
    assert len(outcome.journal) == trade_count * 2
    assert not outcome.endpoint_state.positions
    assert not outcome.endpoint_state.working_orders
    assert outcome.financial.fees == sum(
        (fill.fee for fill in outcome.fills),
        start=Decimal("0"),
    )
    assert outcome.financial.realized_pnl == result["total_pnl"]
    assert outcome.financial.equity == runner.initial_balance + result["total_pnl"]
    for index, fill in enumerate(outcome.fills):
        logical_id = f"order-{index:06d}"
        submitted, filled = outcome.order_observations[index * 2 : index * 2 + 2]
        entry_journal, fill_journal = outcome.journal[index * 2 : index * 2 + 2]
        assert submitted.logical_order_id == logical_id
        assert filled.logical_order_id == logical_id
        assert fill.logical_order_id == logical_id
        assert entry_journal.logical_trade_id == logical_id
        assert fill_journal.logical_trade_id == logical_id
        assert (submitted.phase, filled.phase) == ("submitted", "filled")
        assert (submitted.status, filled.status) == ("PLACED", "FILLED")
        assert submitted.side == filled.side == fill.side
    canonical = outcome.canonical_bytes()
    assert outcome.sha256()
    for persisted_fill in persisted_fills:
        assert str(persisted_fill["id"]).encode() not in canonical
        assert str(persisted_fill["order_id"]).encode() not in canonical
    replacement_ids = {str(fill["order_id"]): uuid4().hex for fill in persisted_fills}
    alternate_fills = tuple(
        {
            **fill,
            "id": uuid4().hex,
            "order_id": replacement_ids[str(fill["order_id"])],
        }
        for fill in persisted_fills
    )
    alternate_journal = []
    for row in journal:
        replacement = replacement_ids[str(row["trade_id"])]
        data = {**row["data"], "order_id": replacement}
        alternate_journal.append({**row, "trade_id": replacement, "data": data})
    alternate = build_normal_backtest_trading_outcome(
        signals=observed_signals,
        fills=alternate_fills,
        journal=tuple(alternate_journal),
        endpoint_state=result["endpoint_state"],
        initial_balance=runner.initial_balance,
        total_pnl=result["total_pnl"],
    )
    assert alternate.canonical_bytes() == canonical
    assert alternate.sha256() == outcome.sha256()
    assert alternate.first_difference(outcome) is None
    assert result["journal_count"] >= trade_count
    assert result["total_trades"] == 25
    assert metrics["total_trades"] == 25
    assert Decimal(str(summary.total_pnl)) == result["total_pnl"]
    assert Decimal(str(summary.total_pnl)) != Decimal("0")
