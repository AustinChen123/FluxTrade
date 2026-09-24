"""Binding and recording SQL contracts; true PostgreSQL acceptance is separate."""
from dataclasses import asdict, replace
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles.handoff import _validate_publication_binding, parse_handoff
from src.core.market_data.profiles.jobs import JobCompletionError, RawRetentionResult
from src.core.market_data.profiles.publication import CanonicalJsonObject
from test_profile_handoff import RAW
from test_profile_job_completion import completion


def retention():
    parsed = parse_handoff(RAW)
    store, session, job, events, _, snapshot = completion()
    value = parsed.publication
    job.update(**asdict(parsed.spec), status="DONE", completed_snapshot_id=value.content_sha256,
               lease_owner=None, lease_expires_at=None)
    snapshot.update(**asdict(value.content), id=value.content_sha256, content_sha256=value.content_sha256,
                    source_manifest=value.source_manifest.thaw(), reconciliation=value.reconciliation.thaw(),
                    source_available_at=value.source_available_at, base_volume=value.content.base_volume,
                    quote_volume=value.content.quote_volume, aggregate_count=value.content.aggregate_count,
                    occupied_bins=value.content.occupied_bins)
    original = session.execute.side_effect

    def execute(statement: Any) -> MagicMock:
        sql = str(statement)
        if sql.startswith("UPDATE volume_profile_snapshot"):
            compiled = statement.compile(dialect=postgresql.dialect())
            events.append(str(compiled))
            snapshot.update({k: v for k, v in compiled.params.items() if k in snapshot})
            result = MagicMock()
            result.mappings.return_value.one_or_none.return_value = dict(snapshot)
            return result
        result = original(statement)
        if "FROM volume_profile_bin" in sql:
            result.mappings.return_value.all.return_value = [asdict(b) for b in value.content.bins]
        return result

    session.execute.side_effect = execute
    session.execute.reset_mock()
    events.clear()
    return store, session, job, events, snapshot, parsed


@pytest.mark.parametrize("owner,path,value", [
    ("source_manifest", ("job_id",), "other"), ("source_manifest", ("config_sha256",), "d" * 64),
    ("source_manifest", ("schema_version",), True), ("source_manifest", ("extra",), 1),
    ("source_manifest", ("hours",), []), ("source_manifest", ("hours", 1, "start_ms"), 0),
    ("source_manifest", ("hours", 0, "page_count"), 0),
    ("source_manifest", ("hours", 0, "manifest_sha256"), "bad"),
    ("source_manifest", ("hours", 0, "aggregate_count"), 2),
    ("source_manifest", ("hours", 2, "first_aggregate_id"), 102),
    ("source_manifest", ("hours", 2, "last_aggregate_id"), 102),
    ("source_manifest", ("hours", 1, "last_aggregate_id"), 100),
    ("reconciliation", ("source",), "other"), ("reconciliation", ("interval",), "1h"),
    ("reconciliation", ("expected_base_volume",), "3.0"),
    ("reconciliation", ("actual_quote_volume",), "0"),
    ("reconciliation", ("actual_aggregate_trade_count",), 4),
    ("reconciliation", ("official_constituent_trade_count",), 0),
    ("reconciliation", ("response_sha256",), "bad"), ("reconciliation", ("extra",), 1),
])
def test_binding_rejects_metadata_only_damage(owner: str, path: tuple[Any, ...], value: Any) -> None:
    parsed = parse_handoff(RAW)
    data = getattr(parsed.publication, owner).thaw()
    target = data
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    publication = replace(parsed.publication, **{owner: CanonicalJsonObject(data)})
    with pytest.raises(ValueError, match="^invalid profile handoff$"):
        _validate_publication_binding(parsed.spec, publication, allow_deleted=True)


def test_binding_retention_availability_and_exact_types() -> None:
    parsed = parse_handoff(RAW)
    for state in ("PRESENT", "DELETED", "NOT_STORED"):
        value = replace(parsed.publication, raw_retention_state=state)
        for allow in (False, True):
            if state == "PRESENT" or (allow and state == "DELETED"):
                _validate_publication_binding(parsed.spec, value, allow_deleted=allow)
            else:
                with pytest.raises(ValueError):
                    _validate_publication_binding(parsed.spec, value, allow_deleted=allow)
    for value in (replace(parsed.publication, availability_basis="MODELED"),
                  replace(parsed.publication, source_available_at=None),
                  replace(parsed.publication, source_available_at=datetime(1970, 1, 1, tzinfo=timezone.utc))):
        with pytest.raises(ValueError):
            _validate_publication_binding(parsed.spec, value)
    with pytest.raises(ValueError):
        _validate_publication_binding(replace(parsed.spec, grid_id="other"), parsed.publication)
    with pytest.raises(ValueError):
        _validate_publication_binding(None, parsed.publication)  # type: ignore[arg-type]
    recon = parsed.publication.reconciliation.thaw()
    recon["official_constituent_trade_count"] = 99  # Not aggregate count (2).
    _validate_publication_binding(parsed.spec, replace(parsed.publication, reconciliation=CanonicalJsonObject(recon)))


@pytest.mark.parametrize("available,valid", [
    (datetime(1970, 1, 2, microsecond=1, tzinfo=timezone.utc), False),
    (datetime(1970, 1, 2, microsecond=1000, tzinfo=timezone.utc), True),
    (datetime(9999, 12, 31, 23, 59, 59, 999000, tzinfo=timezone.utc), True),
    (datetime.max.replace(tzinfo=timezone.utc), False),
])
def test_binding_preserves_integer_millisecond_domain(available: datetime, valid: bool) -> None:
    parsed = parse_handoff(RAW)
    value = replace(parsed.publication, source_available_at=available)
    if valid:
        _validate_publication_binding(parsed.spec, value, allow_deleted=True)
    else:
        with pytest.raises(ValueError, match="^invalid profile handoff$"):
            _validate_publication_binding(parsed.spec, value, allow_deleted=True)


@pytest.mark.parametrize("key,value", [("completed_snapshot_id", None), ("attempt", 0),
                                      ("lease_owner", "unexpected"), ("config_sha256", "invalid")])
def test_done_corruption_has_stable_completion_error(key: str, value: Any) -> None:
    store, _, job, events, snapshot, parsed = retention()
    job[key] = value
    for action in (lambda: store.recover_completed_snapshot(parsed.spec),
                   lambda: store.mark_raw_deleted(parsed.spec, snapshot["id"])):
        with pytest.raises(JobCompletionError, match="^job retention verification failed$") as caught:
            action()
        assert type(caught.value) is JobCompletionError
    assert not any(sql.startswith("UPDATE") for sql in events)


def test_read_only_recovery_and_cas_preserve_everything_except_retention() -> None:
    store, session, job, events, snapshot, parsed = retention()
    original_job, original_snapshot = dict(job), dict(snapshot)
    recovered = store.recover_completed_snapshot(parsed.spec)
    assert recovered is not None and recovered.profile.publication == parsed.publication
    assert all(sql.startswith("SELECT") and "FOR UPDATE" not in sql for sql in events)
    events.clear()
    result = store.mark_raw_deleted(parsed.spec, snapshot["id"])
    assert not result.already_deleted and result.completion.profile.publication.raw_retention_state == "DELETED"
    assert "ISOLATION" in events[0] and "set_config" in events[1]
    assert "ingest_job" in events[2] and "FOR UPDATE" in events[2]
    assert "snapshot" in events[3] and "FOR UPDATE" in events[3]
    updates = [call.args[0] for call in session.execute.call_args_list if str(call.args[0]).startswith("UPDATE")]
    assert len(updates) == 1
    sql = str(updates[0])
    assert "SET raw_retention_state=" in sql and "quality =" in sql and "raw_retention_state =" in sql
    assert dict(snapshot) == {**original_snapshot, "raw_retention_state": "DELETED"} and job == original_job
    assert store.mark_raw_deleted(parsed.spec, snapshot["id"]).already_deleted
    assert store.recover_completed_snapshot(parsed.spec) == result.completion
    with pytest.raises(JobCompletionError):
        store.recover_completed(parsed.spec, parsed.publication)
    with pytest.raises(ValueError):
        RawRetentionResult(result.completion, 1)  # type: ignore[arg-type]


@pytest.mark.parametrize("damage", ["spec", "id", "missing", "partial", "digest", "retention", "binding"])
def test_retention_rejects_before_update(damage: str) -> None:
    store, _, _, events, snapshot, parsed = retention()
    spec, identifier = parsed.spec, snapshot["id"]
    if damage == "spec":
        spec = replace(spec, config_sha256="b" * 64)
    elif damage == "id":
        identifier = "e" * 64
    elif damage == "missing":
        snapshot.clear()
    else:
        key, value = {"partial": ("quality", "PARTIAL"), "digest": ("content_sha256", "e" * 64),
                      "retention": ("raw_retention_state", "NOT_STORED"),
                      "binding": ("source_manifest", {})}[damage]
        snapshot[key] = value
    with pytest.raises(JobCompletionError):
        store.mark_raw_deleted(spec, identifier)
    assert not any(sql.startswith("UPDATE") for sql in events)


def test_retention_database_and_commit_failures_propagate() -> None:
    for commit in (False, True):
        store, session, _, _, snapshot, parsed = retention()
        error = DBAPIError("bounded", None, Exception("injected"))
        if commit:
            session.begin.return_value.__exit__.side_effect = error
        else:
            session.execute.side_effect = error
        with pytest.raises(DBAPIError) as caught:
            store.mark_raw_deleted(parsed.spec, snapshot["id"])
        assert caught.value is error  # Recording fake does not prove database rollback.


@pytest.mark.parametrize("damage", ["cas_missing", "revision", "metadata", "job"])
def test_retention_revalidates_returning_and_unchanged_state(damage: str) -> None:
    store, session, job, _, snapshot, parsed = retention()
    execute = session.execute.side_effect

    def changed(statement: Any) -> MagicMock:
        result = execute(statement)
        if str(statement).startswith("UPDATE volume_profile_snapshot"):
            if damage == "cas_missing":
                result.mappings.return_value.one_or_none.return_value = None
            else:
                if damage == "revision":
                    snapshot["revision"] += 1
                elif damage == "metadata":
                    snapshot["source_available_at"] = snapshot["source_available_at"].replace(year=2000)
                else:
                    job["attempt"] += 1
                result.mappings.return_value.one_or_none.return_value = dict(snapshot)
        return result

    session.execute.side_effect = changed
    with pytest.raises(JobCompletionError):
        store.mark_raw_deleted(parsed.spec, snapshot["id"])
    assert session.begin.return_value.__exit__.call_args.args[0] is JobCompletionError
