"""Committed PostgreSQL contract for the formal closed-trades projection."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import event, select
from sqlalchemy.orm import Session, sessionmaker

from src.control_plane.backtest_result_http_contract import (
    BacktestResultQuery,
    IndexCursor,
    InvalidBacktestResultHttpRequest,
    TradesCursor,
    TradesCursorBinding,
    encode_result_cursor,
    format_utc_milliseconds,
    verify_result_cursor,
    wire_decimal,
)
from src.control_plane.models import JobRecord, JobStatus
from src.core.backtest_result_owner import BacktestResultPersistenceOwner
from src.core.orm_models import BacktestClosedTrade, BacktestResultSummary
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


def _write(factory: sessionmaker[Session], job_id: str = "trade-job"):
    return BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_999_999_999
    ).persist(_identity(job_id=job_id), _outcome())


def test_trades_pages_match_committed_writer_values_and_bind_cursor(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import BacktestResultsQueryService

    engine, factory = _database(fresh_pg_db)
    outcome = _outcome()
    receipt = BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_999_999_999
    ).persist(_identity(job_id="trade-job"), outcome)
    service = BacktestResultsQueryService(factory, cursor_key=_KEY)
    first = service.list_trades("trade-job", BacktestResultQuery(limit=1))
    with factory() as session:
        persisted = session.scalars(
            select(BacktestClosedTrade)
            .join(BacktestResultSummary)
            .where(BacktestResultSummary.job_id == "trade-job")
            .order_by(BacktestClosedTrade.sequence.asc())
        ).all()
    assert len(persisted) == len(outcome.closed_trades)
    assert [
        (
            row.entry_time,
            row.exit_time,
            row.entry_price,
            row.exit_price,
            row.side,
            row.quantity,
            row.pnl,
            row.fee,
        )
        for row in persisted
    ] == [
        (
            trade.entry_time,
            trade.exit_time,
            trade.entry_price,
            trade.exit_price,
            trade.side,
            trade.quantity,
            trade.pnl,
            trade.fee,
        )
        for trade in outcome.closed_trades
    ]
    trade = outcome.closed_trades[0]
    assert first == {
        "items": [
            {
                "id": "trade-job:0",
                "entry_time": format_utc_milliseconds(trade.entry_time),
                "exit_time": format_utc_milliseconds(trade.exit_time),
                "entry_price": wire_decimal(trade.entry_price),
                "exit_price": wire_decimal(trade.exit_price),
                "side": trade.side.value,
                "quantity": wire_decimal(trade.quantity),
                "pnl": wire_decimal(trade.pnl),
                "fee": wire_decimal(trade.fee),
            }
        ],
        "next_cursor": first["next_cursor"],
        "revision": 1,
    }
    assert first["next_cursor"] is not None
    cursor = first["next_cursor"]
    assert verify_result_cursor(
        _KEY, cursor, TradesCursorBinding("trade-job", receipt.result_digest)
    ) == TradesCursor("trade-job", receipt.result_digest, 0)
    replay = service.list_trades(
        "trade-job", BacktestResultQuery(limit=1, cursor=cursor)
    )
    assert replay == service.list_trades(
        "trade-job", BacktestResultQuery(limit=1, cursor=cursor)
    )
    tail = service.list_trades("trade-job", BacktestResultQuery(limit=2, cursor=cursor))
    second = outcome.closed_trades[1]
    assert tail == {
        "items": [
            {
                "id": "trade-job:1",
                "entry_time": format_utc_milliseconds(second.entry_time),
                "exit_time": format_utc_milliseconds(second.exit_time),
                "entry_price": wire_decimal(second.entry_price),
                "exit_price": wire_decimal(second.exit_price),
                "side": second.side.value,
                "quantity": wire_decimal(second.quantity),
                "pnl": wire_decimal(second.pnl),
                "fee": wire_decimal(second.fee),
            }
        ],
        "next_cursor": None,
        "revision": 1,
    }
    assert tail == service.list_trades(
        "trade-job", BacktestResultQuery(limit=2, cursor=cursor)
    )
    BacktestResultPersistenceOwner(factory).persist(
        _identity(job_id="zero-job"), replace(_outcome(), closed_trades=())
    )
    assert service.list_trades("zero-job", BacktestResultQuery(limit=100)) == {
        "items": [],
        "next_cursor": None,
        "revision": 1,
    }
    status_records = {}
    for status in JobStatus:
        job_id = f"read-{status.value}"
        _write(factory, job_id)
        status_records[job_id] = JobRecord(
            id=job_id,
            kind="full_strategy_backtest",
            status=status,
            created_at=datetime(2026, 1, 1, tzinfo=UTC),
            updated_at=datetime(2026, 1, 1, tzinfo=UTC),
            request={},
        )
    status_service = BacktestResultsQueryService(
        factory, cursor_key=_KEY, job_lookup=status_records.get
    )
    for job_id in status_records:
        page = status_service.list_trades(job_id, BacktestResultQuery(limit=1))
        assert page["items"][0]["id"] == f"{job_id}:0"
    engine.dispose()


def test_trades_rejects_wrong_cursor_bindings_before_child_query(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import BacktestResultsQueryService

    engine, factory = _database(fresh_pg_db)
    receipt = _write(factory)
    service = BacktestResultsQueryService(factory, cursor_key=_KEY)

    def forbidden_session() -> Session:
        raise AssertionError("invalid path/query must be rejected before SQL")

    invalid_service = BacktestResultsQueryService(forbidden_session, cursor_key=_KEY)
    with pytest.raises(InvalidBacktestResultHttpRequest):
        invalid_service.list_trades("bad/job", BacktestResultQuery(limit=1))
    with pytest.raises(InvalidBacktestResultHttpRequest):
        invalid_service.list_trades("trade-job", BacktestResultQuery(limit=True))
    bad_tokens = (
        encode_result_cursor(_KEY, IndexCursor(_START, "trade-job")),
        encode_result_cursor(_KEY, TradesCursor("other-job", receipt.result_digest, 0)),
        encode_result_cursor(_KEY, TradesCursor("trade-job", "a" * 64, 0)),
        encode_result_cursor(
            bytes(32), TradesCursor("trade-job", receipt.result_digest, 0)
        ),
    )
    child_queries: list[str] = []

    def observe_child_query(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        if "backtest_closed_trade" in statement:
            child_queries.append(statement)

    event.listen(engine, "before_cursor_execute", observe_child_query)
    try:
        for token in bad_tokens:
            with pytest.raises(InvalidBacktestResultHttpRequest):
                service.list_trades(
                    "trade-job", BacktestResultQuery(limit=1, cursor=token)
                )
    finally:
        event.remove(engine, "before_cursor_execute", observe_child_query)
    assert child_queries == []
    engine.dispose()


def test_trades_missing_result_uses_only_linked_job_presence(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import (
        BacktestResultsNotFound,
        BacktestResultsQueryService,
        BacktestResultsUnavailable,
    )

    engine, factory = _database(fresh_pg_db)
    with factory() as session, session.begin():
        session.add(
            BacktestResultSummary(
                strategy_id=_identity().strategy_id,
                start_time=_START,
                end_time=_START + 60_000,
                total_pnl=Decimal("0"),
                metrics_json='{"legacy":"no job link"}',
            )
        )
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
            service.list_trades(
                job_id, BacktestResultQuery(limit=100, cursor="malformed")
            )
    with pytest.raises(BacktestResultsNotFound, match="result_not_found"):
        service.list_trades(
            "unlinked-legacy", BacktestResultQuery(limit=100, cursor="malformed")
        )
    engine.dispose()


def test_trades_backend_failure_is_sanitized_and_closes_owned_session(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import (
        BacktestResultsQueryService,
        BacktestResultsReadUnavailable,
    )

    engine, factory = _database(fresh_pg_db)
    _write(factory)
    close_count = 0

    class TrackingSession(Session):
        def close(self) -> None:
            nonlocal close_count
            close_count += 1
            super().close()

    tracked_factory = sessionmaker(bind=engine, class_=TrackingSession)
    service = BacktestResultsQueryService(tracked_factory, cursor_key=_KEY)
    service.list_trades("trade-job", BacktestResultQuery(limit=1))
    assert close_count == 1

    def fail_child_query(
        connection: object,
        cursor: object,
        statement: str,
        parameters: object,
        context: object,
        executemany: bool,
    ) -> None:
        if "backtest_closed_trade" in statement:
            raise RuntimeError("private database details")

    event.listen(engine, "before_cursor_execute", fail_child_query)
    try:
        with pytest.raises(BacktestResultsReadUnavailable) as caught:
            service.list_trades("trade-job", BacktestResultQuery(limit=1))
    finally:
        event.remove(engine, "before_cursor_execute", fail_child_query)
    assert str(caught.value) == "browser_result_backend_unavailable"
    assert caught.value.__cause__ is None
    assert close_count == 2
    engine.dispose()
