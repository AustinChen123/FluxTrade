from __future__ import annotations

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, localcontext

import pytest
import sqlalchemy as sa
from sqlalchemy import event, select
from sqlalchemy.orm import Session, sessionmaker

from src.core.backtest_result_owner import (
    BacktestResultPersistenceOwner,
    BacktestResultRunIdentity,
    _project_result,
)
from src.core.backtest_result_persistence import (
    ClosedTradeSnapshot,
    FullBacktestOutcome,
)
from src.core.models import PositionSide
from src.core.orm_models import (
    BacktestClosedTrade,
    BacktestEquitySample,
    BacktestMonthlyReturn,
    BacktestPnlDistribution,
    BacktestResultSummary,
)
from src.core.product_registry import InstrumentSpec, MarketType
from test_backtest_result_summary_pg import _seed_foreign_keys
from test_migrations import _target_url, _upgrade, fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db

_START = 1_704_067_200_000
_BALANCE = Decimal("100000000000000000000000000000.0000000000000000000000000001")
_EQUITY_LOW = Decimal("99999999999999999999999999999.1234567890123456789012345678")


def _identity(*, job_id: str = "writer-job", dataset_id: str = "summary-dataset"):
    return BacktestResultRunIdentity(
        job_id,
        "summary-strategy",
        "v1",
        "a" * 64,
        dataset_id,
        "0" * 64,
        "RITHMIC:MNQ-CONTINUOUS",
        "1m",
        "USD",
        _START,
        _START + 60_000,
        _BALANCE,
        InstrumentSpec(
            "RITHMIC:MNQ-CONTINUOUS",
            "rithmic",
            "MNQ",
            "MNQ",
            "USD",
            market_type=MarketType.CONTINUOUS_FUTURE,
        ),
        Decimal("0.000123456789012345678901"),
        Decimal("0.000234567890123456789012"),
        Decimal("0.050000000000000000000001"),
        None,
    )


def _outcome() -> FullBacktestOutcome:
    return FullBacktestOutcome(
        _BALANCE,
        Decimal("1.2345678901234567890123456789"),
        Decimal("-9.8765432109876543210987654321"),
        Decimal("0.1234567890123456789012345678"),
        Decimal("1.2345678901234567890123456789"),
        Decimal("-2.345678901234567890123456789"),
        Decimal("3.45678901234567890123456789"),
        ((_START, _BALANCE), (_START + 1, _EQUITY_LOW)),
        (
            ClosedTradeSnapshot(
                _START + 2,
                _START + 3,
                Decimal("100.12345678901234567890123456789"),
                Decimal("99.98765432109876543210987654321"),
                PositionSide.LONG,
                Decimal("0.12345678901234567890123456789"),
                Decimal("-1.2345678901234567890123456789"),
                Decimal("0.000000000000000000000123456789"),
            ),
            ClosedTradeSnapshot(
                _START + 4,
                _START + 5,
                Decimal("99.11111111111111111111111111111"),
                Decimal("100.22222222222222222222222222222"),
                PositionSide.SHORT,
                Decimal("0.98765432109876543210987654321"),
                Decimal("2.345678901234567890123456789"),
                Decimal("0.000000000000000000000987654321"),
            ),
        ),
    )


def _database(db_name: str) -> tuple[sa.Engine, sessionmaker[Session]]:
    _upgrade(db_name, "head")
    engine = sa.create_engine(_target_url(db_name))
    _seed_foreign_keys(engine)
    return engine, sessionmaker(bind=engine)


def _row_counts(engine: sa.Engine, job_id: str) -> tuple[int, int, int, int, int]:
    tables = (
        BacktestResultSummary,
        BacktestEquitySample,
        BacktestClosedTrade,
        BacktestMonthlyReturn,
        BacktestPnlDistribution,
    )
    with engine.connect() as connection:
        summary_id = connection.execute(
            select(BacktestResultSummary.id).where(
                BacktestResultSummary.job_id == job_id
            )
        ).scalar_one_or_none()
        if summary_id is None:
            return (0, 0, 0, 0, 0)
        counts = [1]
        for model in tables[1:]:
            counts.append(
                connection.execute(
                    select(sa.func.count())
                    .select_from(model)
                    .where(model.summary_id == summary_id)
                ).scalar_one()
            )
        return tuple(counts)  # type: ignore[return-value]


def test_writer_commits_exact_summary_and_children_as_one_visible_result(
    fresh_pg_db: str,
) -> None:
    engine, session_factory = _database(fresh_pg_db)
    try:
        identity = _identity()
        outcome = _outcome()
        expected = _project_result(identity, outcome, completed_at=1_700_000_000_999)
        clock_calls: list[int] = []

        def clock_ns() -> int:
            clock_calls.append(1)
            return 1_700_000_000_999_999_999

        visible_before_commit: list[bool] = []

        def after_flush(session: Session, flush_context: object) -> None:
            with engine.connect() as connection:
                count = connection.execute(
                    select(sa.func.count())
                    .select_from(BacktestResultSummary)
                    .where(BacktestResultSummary.job_id == identity.job_id)
                ).scalar_one()
            visible_before_commit.append(count > 0)

        event.listen(Session, "after_flush", after_flush)
        try:
            receipt = BacktestResultPersistenceOwner(
                session_factory, clock_ns=clock_ns
            ).persist(identity, outcome)
        finally:
            event.remove(Session, "after_flush", after_flush)

        assert clock_calls == [1]
        assert visible_before_commit == [False, False]
        assert receipt.job_id == identity.job_id
        assert len(receipt.input_digest) == len(receipt.result_digest) == 64
        with pytest.raises(FrozenInstanceError):
            receipt.job_id = "changed"  # type: ignore[misc]
        assert _row_counts(engine, identity.job_id) == (1, 2, 2, 1, 2)
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
            ).one()
            distribution = session.scalars(
                select(BacktestPnlDistribution)
                .where(BacktestPnlDistribution.summary_id == summary.id)
                .order_by(BacktestPnlDistribution.sequence)
            ).all()

        assert summary.metrics_json is None
        assert (summary.start_time, summary.end_time) == (identity.start, identity.end)
        assert summary.completed_at == 1_700_000_000_999
        assert (summary.input_digest, summary.result_digest) == (
            receipt.input_digest,
            receipt.result_digest,
        )
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
        assert [
            (row.sequence, row.timestamp, row.equity, row.drawdown) for row in equity
        ] == [tuple(row) for row in expected.equity]
        with localcontext() as context:
            context.prec = 100
            expected_drawdown = _BALANCE - _EQUITY_LOW
        assert [row.drawdown for row in equity] == [Decimal("0"), expected_drawdown]
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
        assert monthly.return_pct == expected.monthly_returns[0].return_pct
        assert (monthly.month, monthly.return_pct) == (
            expected.monthly_returns[0].month,
            expected.monthly_returns[0].return_pct,
        )
        assert [
            (row.sequence, row.lower, row.upper, row.count) for row in distribution
        ] == [
            (row.sequence, row.lower, row.upper, row.count)
            for row in expected.pnl_distribution
        ]
        for value in (
            summary.total_pnl,
            summary.net_pnl,
            summary.return_pct,
            summary.initial_balance,
            summary.max_drawdown,
            summary.sharpe,
            summary.sortino,
            summary.calmar,
            equity[1].equity,
            equity[1].drawdown,
            trades[0].quantity,
            trades[0].entry_price,
            trades[0].fee,
            trades[0].pnl,
            monthly.return_pct,
        ):
            assert isinstance(value, Decimal)

        empty_identity = replace(identity, job_id="empty-writer-job")
        empty_outcome = FullBacktestOutcome(
            _BALANCE,
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            Decimal("0"),
            (),
            (),
        )
        BacktestResultPersistenceOwner(session_factory, clock_ns=clock_ns).persist(
            empty_identity, empty_outcome
        )
        assert _row_counts(engine, empty_identity.job_id) == (1, 0, 0, 0, 0)
        assert clock_calls == [1, 1]
    finally:
        engine.dispose()
