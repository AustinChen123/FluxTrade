"""Recording SQL/value contracts, not PostgreSQL lease/concurrency acceptance."""
from contextlib import nullcontext
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql

from src.core.market_data.profiles.jobs import (
    JobConflict, JobIntegrityError, JobSpec, LeaseLost, ProfileIngestJobStore,
)
from src.core.market_data.profiles.publication import CanonicalJsonObject

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
LEASE = timedelta(seconds=30)
SPEC = JobSpec("job", "BINANCE:BTCUSDT-SPOT", 0, 86400000, "g1", "vp-v1", "a" * 64)


def harness() -> tuple[ProfileIngestJobStore, MagicMock, dict[str, Any], list[str]]:
    row: dict[str, Any] = dict(**asdict(SPEC), status="RETRYABLE", attempt=0,
        source_cursor={}, progress_manifest={}, lease_owner=None, lease_expires_at=None,
        retry_after_at=None, last_error_code=None, last_error_detail=None,
        completed_snapshot_id=None, created_at=NOW, updated_at=NOW)
    events: list[str] = []
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = False
    session.scalar.return_value = NOW

    def execute(statement: Any) -> MagicMock:
        compiled = statement.compile(dialect=postgresql.dialect())
        sql = str(compiled)
        events.append(sql)
        if sql.startswith("UPDATE"):
            row.update({key: value for key, value in compiled.params.items() if key in row})
        result = MagicMock()
        result.mappings.return_value.one.return_value = dict(row)
        result.mappings.return_value.one_or_none.return_value = dict(row)
        return result

    session.execute.side_effect = execute
    return ProfileIngestJobStore(lambda: nullcontext(session)), session, row, events


def test_register_is_immutable_and_guards_database_boundary() -> None:
    store, session, row, events = harness()
    row["source_cursor"] = {"id": 9}
    first = store.register(SPEC)
    assert store.register(SPEC) == first and first.source_cursor.thaw() == {"id": 9}
    assert events[0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert "set_config" in events[1]
    assert "ON CONFLICT (id) DO NOTHING" in events[2]
    with pytest.raises(JobConflict):
        store.register(replace(SPEC, config_sha256="b" * 64))
    for dialect, active in (("sqlite", False), ("postgresql", True)):
        session.get_bind.return_value.dialect.name = dialect
        session.in_transaction.return_value = active
        session.execute.reset_mock()
        with pytest.raises(ValueError):
            store.register(SPEC)
        session.execute.assert_not_called()


def test_takeover_checkpoint_retry_and_fail_are_fenced() -> None:
    store, session, row, events = harness()
    row.update(status="RUNNING", attempt=4, lease_owner="old", lease_expires_at=NOW,
               source_cursor={"id": 9}, progress_manifest={"page": 1})
    claim = store.claim_next("worker", LEASE)
    assert claim is not None and claim.attempt == 5 and claim.source_cursor.thaw() == {"id": 9}
    assert claim.progress_manifest.thaw() == {"page": 1} and row["last_error_code"] is None
    candidate = events[2]
    assert "FOR UPDATE SKIP LOCKED" in candidate and "retry_after_at <= clock_timestamp()" in candidate
    assert "lease_expires_at <= clock_timestamp()" in candidate and "ORDER BY" in candidate
    cursor, manifest = CanonicalJsonObject({"id": 10}), CanonicalJsonObject({"page": 2})
    updated = store.checkpoint(claim, cursor, manifest, LEASE)
    assert updated.source_cursor == cursor and updated.progress_manifest == manifest
    assert "attempt =" in events[-1] and "lease_owner =" in events[-1]
    assert "lease_expires_at > clock_timestamp()" in events[-1]
    for stale in (replace(claim, attempt=4), replace(claim, worker_id="old")):
        with pytest.raises(LeaseLost):
            store.fail(stale, "ERROR", "bounded detail")
    session.scalar.return_value = NOW + LEASE
    with pytest.raises(LeaseLost):
        store.retry(claim, timedelta(0), "ERROR", "detail")
    session.scalar.return_value = NOW
    retry = store.retry(claim, timedelta(0), "ERROR", "detail")
    assert retry.status == "RETRYABLE" and retry.lease_owner is None
    newer = store.claim_next("worker", LEASE)
    assert newer is not None and newer.attempt == 6 and newer.source_cursor == cursor
    assert store.fail(newer, "ERROR", "detail").status == "FAILED"


@pytest.mark.parametrize("field,value", [("id", "../bad"), ("window_start_ms", True),
    ("window_end_ms", 86400001), ("config_sha256", "A" * 64), ("algorithm_version", "")])
def test_spec_rejects_invalid_values(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        replace(SPEC, **{field: value})


def test_corrupt_state_and_database_errors_are_not_success() -> None:
    store, session, row, _ = harness()
    row["attempt"] = True
    with pytest.raises(JobIntegrityError, match="^job integrity check failed$"):
        store.get(SPEC.id)
    failure = RuntimeError("database failure")
    session.execute.side_effect = failure
    with pytest.raises(RuntimeError) as caught:
        store.register(SPEC)
    assert caught.value is failure
    for duration in (timedelta(0), timedelta(days=2)):
        with pytest.raises(ValueError, match="invalid job duration"):
            store.claim_next("worker", duration)


def test_expiry_at_update_returns_lease_lost() -> None:
    store, session, _, _ = harness()
    claim = store.claim_next("worker", LEASE)
    assert claim is not None
    def expire(statement: Any, execute: Any = session.execute.side_effect) -> MagicMock:
        result = execute(statement)
        if str(statement).startswith("UPDATE"):
            result.mappings.return_value.one_or_none.return_value = None
        return result
    session.execute.side_effect = expire
    with pytest.raises(LeaseLost):
        store.fail(claim, "ERROR", "detail")


@pytest.mark.parametrize("status", ["RUNNING", "RETRYABLE", "FAILED", "DONE"])
@pytest.mark.parametrize("lease", ["none", "owner", "expiry", "both"])
@pytest.mark.parametrize("completed", [None, "a" * 64, "A" * 64])
@pytest.mark.parametrize("retry", [None, NOW])
@pytest.mark.parametrize("error", ["none", "code", "detail", "both"])
@pytest.mark.parametrize("attempt", [0, 1])
def test_state_matrix(status: str, lease: str, completed: str | None,
                      retry: datetime | None, error: str, attempt: int) -> None:
    store, _, row, _ = harness()
    row.update(status=status, attempt=attempt,
               lease_owner="worker" if lease in ("owner", "both") else None,
               lease_expires_at=NOW if lease in ("expiry", "both") else None,
               completed_snapshot_id=completed, retry_after_at=retry,
               last_error_code="ERROR" if error in ("code", "both") else None,
               last_error_detail="detail" if error in ("detail", "both") else None)
    valid = {
        "RUNNING": attempt == 1 and lease == "both" and completed is None and retry is None and error == "none",
        "RETRYABLE": lease == "none" and completed is None and error in ("none", "both"),
        "FAILED": attempt == 1 and lease == "none" and completed is None and retry is None and error == "both",
        "DONE": attempt == 1 and lease == "none" and completed == "a" * 64 and retry is None and error == "none",
    }[status]
    if valid:
        state = store.get(SPEC.id)
        assert state is not None and state.status == status
    else:
        with pytest.raises(JobIntegrityError, match="^job integrity check failed$"):
            store.get(SPEC.id)


@pytest.mark.parametrize("detail", ["\ud800", "\udfff", "prefix\ud800suffix", "valid \U0001f680"])
def test_error_detail_utf8_boundary(detail: str) -> None:
    store, _, _, _ = harness()
    claim = store.claim_next("worker", LEASE)
    assert claim is not None
    if detail.startswith("valid"):
        assert store.fail(claim, "ERROR", detail).last_error_detail == detail
    else:
        with pytest.raises(ValueError, match="^invalid job error detail$") as caught:
            store.fail(claim, "ERROR", detail)
        assert type(caught.value) is ValueError
