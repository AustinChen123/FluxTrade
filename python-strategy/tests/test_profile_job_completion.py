"""Recording-session completion contracts; no PostgreSQL acceptance claim."""

from dataclasses import asdict, replace
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import DatabaseError

from src.core.market_data.profiles.jobs import JobCompletionError, LeaseLost
from test_profile_jobs import LEASE, NOW, harness
from test_profile_publication import publication


def completion():
    store, session, job, events = harness()
    claim = store.claim_next("worker", LEASE)
    assert claim is not None
    value = publication()
    content = value.content
    snapshot: dict[str, Any] = dict(
        asdict(content),
        id=content.content_sha256,
        content_sha256=content.content_sha256,
        period="1d",
        timezone="UTC",
        revision=1,
        base_volume=content.base_volume,
        quote_volume=content.quote_volume,
        aggregate_count=content.aggregate_count,
        occupied_bins=content.occupied_bins,
        source_manifest=value.source_manifest.thaw(),
        reconciliation=value.reconciliation.thaw(),
        source_available_at=None,
        availability_basis="OBSERVED",
        raw_retention_state="PRESENT",
        quality="VERIFIED",
        computed_at=NOW,
        published_at=NOW,
    )
    execute = session.execute.side_effect

    def routed(statement: Any) -> MagicMock:
        result = execute(statement)
        sql = str(statement)
        if "FROM volume_profile_snapshot" in sql:
            result.mappings.return_value.one_or_none.return_value = dict(snapshot) if snapshot else None
        if "FROM volume_profile_bin" in sql:
            result.mappings.return_value.all.return_value = [asdict(b) for b in content.bins]
        return result

    session.execute.side_effect = routed
    events.clear()
    return store, session, job, events, claim, snapshot


def test_complete_reuses_exact_readback_and_second_fence() -> None:
    store, session, job, events, claim, snapshot = completion()
    result = store.complete(claim, snapshot["id"])
    assert result.status == "DONE" and result.completed_snapshot_id == snapshot["id"]
    assert result.lease_owner is result.lease_expires_at is result.retry_after_at is None
    assert result.last_error_code is result.last_error_detail is None
    assert "FOR UPDATE" in events[2] and "FROM volume_profile_snapshot" in events[3]
    assert "ORDER BY volume_profile_bin.bin_index" in events[4]
    assert all(part in events[5] for part in ("attempt =", "lease_owner =", "lease_expires_at > clock_timestamp()"))
    assert session.begin.return_value.__exit__.call_args.args == (None, None, None)
    assert job["status"] == "DONE"


@pytest.mark.parametrize(
    "damage", ["missing", "quality", "published_at", "base_volume", "metadata", "digest", "identity"]
)
def test_invalid_snapshot_does_not_complete(damage: str) -> None:
    store, session, job, events, claim, snapshot = completion()
    snapshot_id = snapshot["id"]
    if damage == "missing":
        snapshot.clear()
    elif damage == "identity":
        job["grid_id"] = "other"
        claim = replace(claim, spec=replace(claim.spec, grid_id="other"))
    else:
        key, value = {
            "quality": ("quality", "PARTIAL"),
            "published_at": ("published_at", None),
            "base_volume": ("base_volume", 999),
            "metadata": ("source_manifest", []),
            "digest": ("content_sha256", "b" * 64),
        }[damage]
        snapshot[key] = value
    with pytest.raises(JobCompletionError, match="^job completion verification failed$"):
        store.complete(claim, snapshot_id)
    assert job["status"] == "RUNNING" and not any(sql.startswith("UPDATE") for sql in events)
    assert session.begin.return_value.__exit__.call_args.args[0] is JobCompletionError


def test_invalid_id_stale_expired_claim_and_database_failure() -> None:
    store, session, _, events, claim, snapshot = completion()
    for invalid in ("A" * 64, "a" * 63, 1, True):
        with pytest.raises(ValueError, match="invalid completed snapshot ID"):
            store.complete(claim, invalid)  # type: ignore[arg-type]
    assert events == []
    with pytest.raises(LeaseLost):
        store.complete(replace(claim, attempt=claim.attempt + 1), snapshot["id"])
    session.scalar.return_value = NOW + LEASE
    with pytest.raises(LeaseLost):
        store.complete(claim, snapshot["id"])
    failure = DatabaseError("test statement", None, Exception("test failure"))
    session.execute.side_effect = failure
    with pytest.raises(DatabaseError) as caught:
        store.complete(claim, snapshot["id"])
    assert caught.value is failure


@pytest.mark.parametrize(
    "changes",
    [
        {"product_id": "BINANCE:ETHUSDT-SPOT"},
        {"grid_id": "other"},
        {"algorithm_version": "vp-v2"},
        {"window_start_ms": 86400000, "window_end_ms": 172800000},
    ],
)
def test_every_logical_identity_component_must_match(changes: dict[str, Any]) -> None:
    store, _, job, events, claim, snapshot = completion()
    job.update(changes)
    claim = replace(claim, spec=replace(claim.spec, **changes))
    with pytest.raises(JobCompletionError):
        store.complete(claim, snapshot["id"])
    assert job["status"] == "RUNNING" and not any(sql.startswith("UPDATE") for sql in events)


def test_database_readback_exception_is_not_rewritten() -> None:
    store, session, job, _, claim, snapshot = completion()
    execute = session.execute.side_effect
    failure = DatabaseError("test statement", None, Exception("test failure"))

    def broken(statement: Any) -> MagicMock:
        if "FROM volume_profile_bin" in str(statement):
            raise failure
        return execute(statement)

    session.execute.side_effect = broken
    with pytest.raises(DatabaseError) as caught:
        store.complete(claim, snapshot["id"])
    assert caught.value is failure and job["status"] == "RUNNING"


def test_final_done_update_loses_lease_after_snapshot_verification() -> None:
    store, session, _, events, claim, snapshot = completion()
    execute = session.execute.side_effect

    def expired(statement: Any) -> MagicMock:
        result = execute(statement)
        if str(statement).startswith("UPDATE"):
            result.mappings.return_value.one_or_none.return_value = None
        return result

    session.execute.side_effect = expired
    with pytest.raises(LeaseLost):
        store.complete(claim, snapshot["id"])
    assert "ORDER BY volume_profile_bin.bin_index" in events[-2]
    assert events[-1].startswith("UPDATE")
    assert session.begin.return_value.__exit__.call_args.args[0] is LeaseLost
    # Recording fake mutation is not evidence of PostgreSQL rollback.


def test_completion_commit_failure_never_returns_done_result() -> None:
    store, session, _, _, claim, snapshot = completion()
    failure = DatabaseError("commit", None, Exception("test failure"))
    session.begin.return_value.__exit__.side_effect = failure
    with pytest.raises(DatabaseError) as caught:
        store.complete(claim, snapshot["id"])
    assert caught.value is failure
    assert session.begin.return_value.__exit__.call_args.args == (None, None, None)
    # This tests exception propagation only; real rollback needs the PG lane.
