from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

import pytest
from sqlalchemy import select

from src.core.backtest_result_owner import (
    BacktestResultPersistenceOwner,
    _project_result,
)
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.memory import MemoryDataSource
from src.core.orm_models import (
    BacktestClosedTrade,
    BacktestEquitySample,
    BacktestMonthlyReturn,
    BacktestPnlDistribution,
    BacktestResultSummary,
    Exchange,
    Product,
    ResearchDataset,
)
from src.core.product_registry import InstrumentSpec, MarketType
from src.strategies.callable_strategy import CallableStrategy
from test_backtest_result_owner_persistence import (
    _database,
    _identity,
    _row_counts,
)
from test_migrations import fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db
_START = 1_704_067_200_000


def test_native_completed_outcome_persists_exact_projection_without_reports(
    fresh_pg_db: str,
) -> None:
    from integration.conftest import PRODUCT_ID, TIMEFRAME, make_candle_series
    from integration.test_research_backtest_runner import _signal_factory

    engine, session_factory = _database(fresh_pg_db)
    native_balance = Decimal("10000")
    maker_fee, taker_fee = Decimal("0.0002"), Decimal("0.0006")
    candles = make_candle_series(count=81, start_timestamp=_START)
    try:
        with session_factory.begin() as session:
            if session.get(Exchange, "BINANCE") is None:
                session.add(Exchange(id="BINANCE", name="Binance"))
            if session.get(Product, PRODUCT_ID) is None:
                session.add(
                    Product(
                        id=PRODUCT_ID,
                        exchange_id="BINANCE",
                        base_asset="BTC",
                        quote_asset="USDT",
                    )
                )
            session.add(
                ResearchDataset(
                    id="native-dataset",
                    product_id=PRODUCT_ID,
                    timeframe=TIMEFRAME,
                    source="fixture",
                    revision="native-outcome",
                    timestamp_format="epoch_milliseconds",
                    checksum_sha256="c" * 64,
                    roll_policy=None,
                    start_time=candles[0].timestamp,
                    end_time=candles[-1].timestamp,
                    row_count=len(candles),
                    quality_status="validated",
                    lifecycle_state="importing",
                    sealed_at=None,
                    metadata_json="{}",
                )
            )

        instrument = InstrumentSpec(
            PRODUCT_ID,
            "binance",
            "BTC/USDT:USDT",
            "BTC",
            "USDT",
            market_type=MarketType.PERPETUAL,
        )
        runner = BacktestRunner(
            start_time=candles[0].timestamp,
            end_time=candles[-1].timestamp,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            initial_balance=native_balance,
            max_drawdown_limit=None,
            data_source=MemoryDataSource(candles),
            fee_config={"maker": maker_fee, "taker": taker_fee},
            report_config={
                "csv_trades": False,
                "markdown_report": False,
                "equity_curve": False,
                "journal_export": False,
            },
            db_session_factory=session_factory,
            instrument_spec=instrument,
            capture_completed_outcome=True,
        )
        runner.add_strategy(
            CallableStrategy(
                "summary-strategy",
                _signal_factory("summary-strategy", candles),
                PRODUCT_ID,
                TIMEFRAME,
            )
        )
        public_result = runner.run()
        outcome = runner.completed_outcome
        assert public_result is not None and outcome is not None
        assert "completed_outcome" not in public_result
        assert len(outcome.equity_samples) == len(candles)
        assert outcome.closed_trades

        identity = replace(
            _identity(),
            start=candles[0].timestamp,
            end=candles[-1].timestamp,
            initial_balance=native_balance,
            dataset_id="native-dataset",
            dataset_checksum_sha256="c" * 64,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
            currency="USDT",
            instrument=instrument,
            maker_fee=maker_fee,
            taker_fee=taker_fee,
            drawdown_limit=None,
        )
        completed_at = 1_700_000_000_000
        expected = _project_result(identity, outcome, completed_at=completed_at)
        receipt = BacktestResultPersistenceOwner(
            session_factory, clock_ns=lambda: completed_at * 1_000_000
        ).persist(identity, outcome)
        assert _row_counts(engine, identity.job_id) == (
            1,
            len(expected.equity),
            len(expected.closed_trades),
            len(expected.monthly_returns),
            len(expected.pnl_distribution),
        )
        assert (receipt.input_digest, receipt.result_digest) == (
            expected.input_digest,
            expected.result_digest,
        )
        with session_factory() as session:
            summary = session.scalars(
                select(BacktestResultSummary).where(
                    BacktestResultSummary.job_id == identity.job_id
                )
            ).one()
            equity = session.scalars(
                select(BacktestEquitySample)
                .where(BacktestEquitySample.summary_id == summary.id)
                .order_by(BacktestEquitySample.sequence)
            ).all()
            trades = session.scalars(
                select(BacktestClosedTrade)
                .where(BacktestClosedTrade.summary_id == summary.id)
                .order_by(BacktestClosedTrade.sequence)
            ).all()
            monthly = session.scalars(
                select(BacktestMonthlyReturn).where(
                    BacktestMonthlyReturn.summary_id == summary.id
                )
            ).all()
            distribution = session.scalars(
                select(BacktestPnlDistribution)
                .where(BacktestPnlDistribution.summary_id == summary.id)
                .order_by(BacktestPnlDistribution.sequence)
            ).all()
        assert (
            summary.initial_balance,
            summary.total_pnl,
            summary.net_pnl,
            summary.return_pct,
            summary.max_drawdown,
            summary.sharpe,
            summary.sortino,
            summary.calmar,
        ) == tuple(expected.summary)
        assert (summary.job_id, summary.dataset_id) == (
            identity.job_id,
            identity.dataset_id,
        )
        assert [
            (row.sequence, row.timestamp, row.equity, row.drawdown) for row in equity
        ] == [tuple(row) for row in expected.equity]
        assert [
            (
                row.sequence,
                row.entry_time,
                row.exit_time,
                row.side,
                row.quantity,
                row.entry_price,
                row.exit_price,
                row.fee,
                row.pnl,
            )
            for row in trades
        ] == [tuple(row) for row in expected.closed_trades]
        assert [(row.month, row.return_pct) for row in monthly] == [
            tuple(row) for row in expected.monthly_returns
        ]
        assert [
            (row.sequence, row.lower, row.upper, row.count) for row in distribution
        ] == [
            (row.sequence, row.lower, row.upper, row.count)
            for row in expected.pnl_distribution
        ]
    finally:
        engine.dispose()
