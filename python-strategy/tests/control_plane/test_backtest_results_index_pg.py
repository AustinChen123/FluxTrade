"""Committed PostgreSQL contract for the formal backtest-results index."""

from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import event, update
from sqlalchemy.orm import Session, sessionmaker

from src.control_plane.backtest_result_http_contract import (
    BacktestResultQuery,
    IndexCursor,
    IndexCursorBinding,
    InvalidBacktestResultHttpRequest,
    TradesCursor,
    encode_result_cursor,
    format_utc_milliseconds,
    parse_result_query,
    verify_result_cursor,
)
from src.core.backtest_result_owner import BacktestResultPersistenceOwner
from src.core.orm_models import BacktestResultSummary
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


def _index_query(raw: bytes = b""):
    return parse_result_query("index", raw)


def test_index_is_empty_then_pages_only_committed_formal_writer_rows(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import BacktestResultsQueryService

    engine, factory = _database(fresh_pg_db)
    service = BacktestResultsQueryService(factory, cursor_key=bytes(range(32)))
    with factory() as session, session.begin():
        session.add(
            BacktestResultSummary(
                strategy_id="summary-strategy",
                start_time=_START,
                end_time=_START + 60_000,
                total_pnl=Decimal("0"),
                metrics_json='{"legacy":"must not be projected"}',
            )
        )
    assert service.list_index(_index_query()) == {
        "items": [],
        "next_cursor": None,
        "revision": 1,
    }

    visible_during_writer: list[tuple[str, ...]] = []

    def observe_uncommitted(session: Session, flush_context: object) -> None:
        visible_during_writer.append(
            tuple(
                item["job_id"] for item in service.list_index(_index_query())["items"]
            )
        )

    event.listen(Session, "after_flush", observe_uncommitted)
    try:
        BacktestResultPersistenceOwner(
            factory, clock_ns=lambda: 1_700_000_000_999_999_999
        ).persist(_identity(job_id="job-a"), _outcome())
    finally:
        event.remove(Session, "after_flush", observe_uncommitted)
    assert visible_during_writer == [(), ()]
    BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_999_999_999
    ).persist(_identity(job_id="job-b"), _outcome())
    receipt_c = BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_999_999_999
    ).persist(_identity(job_id="job-c"), _outcome())
    BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_998_000_000
    ).persist(_identity(job_id="job-z"), _outcome())

    first = service.list_index(_index_query(b"limit=1"))
    assert [item["job_id"] for item in first["items"]] == ["job-c"]
    assert set(first["items"][0]) == {
        "job_id",
        "subject_id",
        "dataset_id",
        "product_id",
        "timeframe",
        "started_at",
        "ended_at",
        "completed_at",
        "result_digest",
    }
    assert first["items"][0] == {
        "job_id": "job-c",
        "subject_id": "summary-strategy:v1",
        "dataset_id": "summary-dataset",
        "product_id": "RITHMIC:MNQ-CONTINUOUS",
        "timeframe": "1m",
        "started_at": format_utc_milliseconds(_START),
        "ended_at": format_utc_milliseconds(_START + 60_000),
        "completed_at": format_utc_milliseconds(1_700_000_000_999),
        "result_digest": receipt_c.result_digest,
    }
    assert first["next_cursor"] is not None and first["revision"] == 1
    cursor = first["next_cursor"]
    decoded = verify_result_cursor(bytes(range(32)), cursor, IndexCursorBinding())
    assert decoded == IndexCursor(1_700_000_000_999, "job-c")

    replay = service.list_index(_index_query(b"limit=1&cursor=" + cursor.encode()))
    assert replay == service.list_index(
        _index_query(b"limit=1&cursor=" + cursor.encode())
    )
    resized = service.list_index(_index_query(b"limit=2&cursor=" + cursor.encode()))
    assert [item["job_id"] for item in resized["items"]] == ["job-b", "job-a"]
    assert resized["next_cursor"] is not None
    tail = service.list_index(
        _index_query(b"limit=2&cursor=" + resized["next_cursor"].encode())
    )
    assert [item["job_id"] for item in tail["items"]] == ["job-z"]
    assert tail["next_cursor"] is None
    assert [
        item["job_id"] for item in first["items"] + resized["items"] + tail["items"]
    ] == ["job-c", "job-b", "job-a", "job-z"]
    engine.dispose()


def test_index_reader_closes_owned_session_and_sanitizes_backend_failure(
    fresh_pg_db: str,
) -> None:
    from src.control_plane.backtest_results import (
        BacktestResultsQueryService,
        BacktestResultsReadUnavailable,
    )

    engine, factory = _database(fresh_pg_db)

    close_count = 0

    class TrackingSession(Session):
        def close(self) -> None:
            nonlocal close_count
            close_count += 1
            super().close()

    tracked_factory = sessionmaker(bind=engine, class_=TrackingSession)
    BacktestResultsQueryService(tracked_factory, cursor_key=bytes(32)).list_index(
        _index_query()
    )
    assert close_count == 1

    def fail_query(*args: object, **kwargs: object) -> None:
        raise RuntimeError("private database detail")

    event.listen(engine, "before_cursor_execute", fail_query)
    try:
        with pytest.raises(BacktestResultsReadUnavailable) as caught:
            BacktestResultsQueryService(
                tracked_factory, cursor_key=bytes(32)
            ).list_index(_index_query())
    finally:
        event.remove(engine, "before_cursor_execute", fail_query)
    assert str(caught.value) == "browser_result_backend_unavailable"
    assert caught.value.__cause__ is None
    assert close_count == 2

    BacktestResultPersistenceOwner(
        factory, clock_ns=lambda: 1_700_000_000_000_000_000
    ).persist(_identity(job_id="bad-time"), _outcome())
    with factory() as session, session.begin():
        session.execute(
            update(BacktestResultSummary)
            .where(BacktestResultSummary.job_id == "bad-time")
            .values(completed_at=253_402_300_800_000)
        )
    with pytest.raises(BacktestResultsReadUnavailable) as caught:
        BacktestResultsQueryService(tracked_factory, cursor_key=bytes(32)).list_index(
            _index_query()
        )
    assert str(caught.value) == "browser_result_backend_unavailable"
    assert caught.value.__cause__ is None
    assert close_count == 3
    engine.dispose()


def test_index_rejects_foreign_cursor_before_opening_a_session() -> None:
    from src.control_plane.backtest_results import BacktestResultsQueryService

    class ForbiddenFactory:
        def __call__(self) -> Session:
            raise AssertionError("database must not be queried")

    service = BacktestResultsQueryService(ForbiddenFactory(), cursor_key=bytes(32))
    with pytest.raises(InvalidBacktestResultHttpRequest):
        service.list_index(
            BacktestResultQuery(limit=100, cursor="not.a.valid.signed.cursor")
        )
    foreign = encode_result_cursor(bytes(32), TradesCursor("job-c", "a" * 64, 0))
    with pytest.raises(InvalidBacktestResultHttpRequest):
        service.list_index(BacktestResultQuery(limit=100, cursor=foreign))
