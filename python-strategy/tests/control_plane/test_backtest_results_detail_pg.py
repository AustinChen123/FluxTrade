"""Committed PostgreSQL contract for the immutable result detail projection."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

import pytest
from sqlalchemy import event, select, update
from sqlalchemy.orm import Session, sessionmaker

from src.control_plane.backtest_result_http_contract import (
    BacktestResultQuery,
    InvalidBacktestResultHttpRequest,
    TradesCursor,
    TradesCursorBinding,
    format_utc_milliseconds,
    verify_result_cursor,
    wire_decimal,
)
from src.control_plane.models import JobRecord, JobStatus
from src.core.backtest_result_owner import BacktestResultPersistenceOwner
from src.core.orm_models import (
    BacktestClosedTrade,
    BacktestEquitySample,
    BacktestMonthlyReturn,
    BacktestPnlDistribution,
    BacktestResultSummary,
)
from test_backtest_result_owner_persistence import (
    _START,
    _database,
    _identity,
    _outcome,
)
from test_migrations import fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db
_KEY = bytes(range(32))


def _write(factory: sessionmaker[Session], job_id: str, *, zero_trades: bool = False):
    outcome = _outcome()
    if zero_trades:
        outcome = replace(outcome, closed_trades=())
    return BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_999_999_999
    ).persist(_identity(job_id=job_id), outcome)


def _decimal_or_none(value: Decimal | None) -> str | None:
    return None if value is None else wire_decimal(value)


def _trade_wire(job_id: str, row: BacktestClosedTrade) -> dict[str, str]:
    return {
        "id": f"{job_id}:{row.sequence}",
        "entry_time": format_utc_milliseconds(row.entry_time),
        "exit_time": format_utc_milliseconds(row.exit_time),
        "entry_price": wire_decimal(row.entry_price),
        "exit_price": wire_decimal(row.exit_price),
        "side": row.side,
        "quantity": wire_decimal(row.quantity),
        "pnl": wire_decimal(row.pnl),
        "fee": wire_decimal(row.fee),
    }


def test_detail_matches_persisted_rows_and_reads_without_job_state(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import BacktestResultsQueryService

    engine, factory = _database(fresh_pg_db)
    identity = _identity(job_id="detail-job")
    outcome = _outcome()
    receipt = BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_999_999_999
    ).persist(identity, outcome)
    _write(factory, "zero-job", zero_trades=True)
    with factory() as session:
        summary = session.scalars(
            select(BacktestResultSummary).where(
                BacktestResultSummary.job_id == "detail-job"
            )
        ).one()
        equity = session.scalars(
            select(BacktestEquitySample)
            .where(BacktestEquitySample.summary_id == summary.id)
            .order_by(BacktestEquitySample.sequence.asc())
        ).all()
        trades = session.scalars(
            select(BacktestClosedTrade)
            .where(BacktestClosedTrade.summary_id == summary.id)
            .order_by(BacktestClosedTrade.sequence.asc())
        ).all()
        monthly = session.scalars(
            select(BacktestMonthlyReturn)
            .where(BacktestMonthlyReturn.summary_id == summary.id)
            .order_by(BacktestMonthlyReturn.month.asc())
        ).all()
        distribution = session.scalars(
            select(BacktestPnlDistribution)
            .where(BacktestPnlDistribution.summary_id == summary.id)
            .order_by(BacktestPnlDistribution.sequence.asc())
        ).all()
        expected = {
            "job_id": summary.job_id,
            "strategy_id": summary.strategy_id,
            "subject_id": summary.subject_id,
            "dataset_id": summary.dataset_id,
            "product_id": summary.product_id,
            "timeframe": summary.timeframe,
            "started_at": format_utc_milliseconds(summary.start_time),
            "ended_at": format_utc_milliseconds(summary.end_time),
            "currency": summary.currency,
            "metrics": {
                "net_pnl": wire_decimal(cast(Decimal, summary.net_pnl)),
                "return_pct": wire_decimal(cast(Decimal, summary.return_pct)),
                "max_drawdown": wire_decimal(cast(Decimal, summary.max_drawdown)),
                "sharpe": wire_decimal(cast(Decimal, summary.sharpe)),
                "sortino": wire_decimal(cast(Decimal, summary.sortino)),
                "calmar": wire_decimal(cast(Decimal, summary.calmar)),
            },
            "equity": [
                {
                    "timestamp": format_utc_milliseconds(row.timestamp),
                    "equity": wire_decimal(row.equity),
                    "drawdown": wire_decimal(row.drawdown),
                }
                for row in equity
            ],
            "monthly_returns": [
                {"month": row.month, "return_pct": wire_decimal(row.return_pct)}
                for row in monthly
            ],
            "pnl_distribution": [
                {
                    "lower": _decimal_or_none(row.lower),
                    "upper": _decimal_or_none(row.upper),
                    "count": row.count,
                }
                for row in distribution
            ],
            "trade_page": {
                "items": [_trade_wire("detail-job", row) for row in trades],
                "total_count": len(trades),
                "next_cursor": None,
            },
            "input_digest": receipt.input_digest,
            "result_digest": receipt.result_digest,
            "revision": 1,
        }
    assert summary.input_digest == receipt.input_digest
    assert summary.result_digest == receipt.result_digest

    service = BacktestResultsQueryService(factory, cursor_key=_KEY)
    detail = service.get_detail("detail-job")
    assert detail == expected
    assert set(detail) == {
        "job_id",
        "strategy_id",
        "subject_id",
        "dataset_id",
        "product_id",
        "timeframe",
        "started_at",
        "ended_at",
        "currency",
        "metrics",
        "equity",
        "monthly_returns",
        "pnl_distribution",
        "trade_page",
        "input_digest",
        "result_digest",
        "revision",
    }
    assert set(detail["metrics"]) == {
        "net_pnl",
        "return_pct",
        "max_drawdown",
        "sharpe",
        "sortino",
        "calmar",
    }
    assert all(
        set(row) == {"timestamp", "equity", "drawdown"} for row in detail["equity"]
    )
    assert all(set(row) == {"month", "return_pct"} for row in detail["monthly_returns"])
    assert all(
        set(row) == {"lower", "upper", "count"} for row in detail["pnl_distribution"]
    )
    for status in JobStatus:
        job = JobRecord(
            id="detail-job",
            kind="full_strategy_backtest",
            status=status,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
            request={},
        )
        with_job = BacktestResultsQueryService(
            factory, cursor_key=_KEY, job_lookup=lambda _job_id, record=job: record
        )
        assert with_job.get_detail("detail-job") == detail
    zero = service.get_detail("zero-job")
    assert zero["trade_page"] == {
        "items": [],
        "total_count": 0,
        "next_cursor": None,
    }
    assert zero["monthly_returns"] == []
    assert zero["pnl_distribution"] == []
    engine.dispose()


def test_detail_first_hundred_trades_has_count_and_bound_next_cursor(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import BacktestResultsQueryService

    engine, factory = _database(fresh_pg_db)
    base = _outcome()
    template = base.closed_trades[0]
    trades = tuple(
        replace(
            template,
            entry_time=_START + 2 + sequence * 3,
            exit_time=_START + 3 + sequence * 3,
        )
        for sequence in range(101)
    )
    outcome = replace(base, closed_trades=trades)
    receipt = BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_999_999_999
    ).persist(_identity(job_id="large-detail"), outcome)
    service = BacktestResultsQueryService(factory, cursor_key=_KEY)
    detail = service.get_detail("large-detail")
    page = detail["trade_page"]
    assert page["total_count"] == 101
    assert len(page["items"]) == 100
    assert page["items"][0]["id"] == "large-detail:0"
    assert page["items"][-1]["id"] == "large-detail:99"
    assert page["next_cursor"] is not None
    assert verify_result_cursor(
        _KEY,
        page["next_cursor"],
        TradesCursorBinding("large-detail", receipt.result_digest),
    ) == TradesCursor("large-detail", receipt.result_digest, 99)
    assert detail == service.get_detail("large-detail")
    tail = service.list_trades(
        "large-detail", BacktestResultQuery(limit=100, cursor=page["next_cursor"])
    )
    assert [item["id"] for item in tail["items"]] == ["large-detail:100"]
    assert tail["next_cursor"] is None
    engine.dispose()


def test_detail_missing_result_uses_job_presence_and_fixed_dispositions(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import (
        BacktestResultsNotFound,
        BacktestResultsQueryService,
        BacktestResultsUnavailable,
    )

    engine, factory = _database(fresh_pg_db)
    statuses = {
        f"missing-{status.value}": JobRecord(
            id=f"missing-{status.value}",
            kind="full_strategy_backtest",
            status=status,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
            request={},
        )
        for status in JobStatus
    }
    service = BacktestResultsQueryService(
        factory, cursor_key=_KEY, job_lookup=statuses.get
    )
    for job_id in statuses:
        with pytest.raises(BacktestResultsUnavailable, match="result_unavailable"):
            service.get_detail(job_id)
    with pytest.raises(BacktestResultsNotFound, match="result_not_found"):
        service.get_detail("missing-entirely")
    with factory() as session, session.begin():
        session.add(
            BacktestResultSummary(
                strategy_id=_identity().strategy_id,
                start_time=_START,
                end_time=_START + 60_000,
                total_pnl=Decimal("0"),
                metrics_json='{"legacy":"not linked to a job id"}',
            )
        )
    with pytest.raises(BacktestResultsNotFound, match="result_not_found"):
        service.get_detail("legacy-without-link")

    def forbidden_session() -> Session:
        raise AssertionError("invalid decoded job id must be rejected before SQL")

    invalid_service = BacktestResultsQueryService(forbidden_session, cursor_key=_KEY)
    with pytest.raises(InvalidBacktestResultHttpRequest):
        invalid_service.get_detail("bad/job")
    engine.dispose()


def test_detail_backend_failure_is_sanitized_and_closes_owned_session(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import (
        BacktestResultsQueryService,
        BacktestResultsReadUnavailable,
    )

    engine, factory = _database(fresh_pg_db)
    _write(factory, "detail-failure")
    close_count = 0

    class TrackingSession(Session):
        def close(self) -> None:
            nonlocal close_count
            close_count += 1
            super().close()

    tracked_factory = sessionmaker(bind=engine, class_=TrackingSession)
    service = BacktestResultsQueryService(tracked_factory, cursor_key=_KEY)
    service.get_detail("detail-failure")
    assert close_count == 1

    def fail_detail_query(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        if "backtest_equity_sample" in statement:
            raise RuntimeError("private database details")

    event.listen(engine, "before_cursor_execute", fail_detail_query)
    try:
        with pytest.raises(BacktestResultsReadUnavailable) as caught:
            service.get_detail("detail-failure")
    finally:
        event.remove(engine, "before_cursor_execute", fail_detail_query)
    assert str(caught.value) == "browser_result_backend_unavailable"
    assert caught.value.__cause__ is None
    assert close_count == 2
    with factory() as session, session.begin():
        session.execute(
            update(BacktestEquitySample)
            .where(BacktestEquitySample.sequence == 0)
            .values(timestamp=253_402_300_800_000)
        )
    with pytest.raises(BacktestResultsReadUnavailable) as caught:
        service.get_detail("detail-failure")
    assert str(caught.value) == "browser_result_backend_unavailable"
    assert caught.value.__cause__ is None
    assert close_count == 3
    engine.dispose()
