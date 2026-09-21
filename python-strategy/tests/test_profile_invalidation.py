"""Recording SQL/session contracts only; no true-PG concurrency or rollback claim."""
from contextlib import nullcontext
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles.invalidation import (
    ConfirmedInvalidationRequest, InvalidationAppendResult, InvalidationConflict,
    InvalidationIntegrityError, InvalidationReadTooLarge, InvalidationReferenceError, ProfileInvalidationStore,
)
from src.core.market_data.profiles.repository import TransactionWaitPolicy

REQUEST = ConfirmedInvalidationRequest("event", "a" * 64, "BAD_DATA", "b" * 64, "audit")
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def harness():
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = False
    header = dict(quality="VERIFIED", product_id="BINANCE:BTCUSDT-SPOT", window_start_ms=0,
                  window_end_ms=86400000, grid_id="g1", algorithm_version="vp-v1")
    state: dict[str, Any] = dict(event=None, rows=[], target=header, replacement=dict(header), loser=False)
    calls: list[tuple[str, dict[str, Any]]] = []

    def execute(statement: Any):
        compiled = statement.compile(dialect=postgresql.dialect())
        sql, params = str(compiled), compiled.params
        calls.append((sql, params))
        result = MagicMock()
        if sql.startswith("INSERT"):
            state["event"] = state.get("winner", dict(**asdict(REQUEST), recorded_at=NOW))
            result.mappings.return_value.one_or_none.return_value = None if state["loser"] else state["event"]
        elif "FROM volume_profile_snapshot" in sql:
            result.mappings.return_value.one_or_none.return_value = state["target" if params["id_1"] == REQUEST.snapshot_id else "replacement"]
        else:
            result.mappings.return_value.one_or_none.return_value = state["event"]
            result.mappings.return_value.all.return_value = state["rows"]
        return result

    session.execute.side_effect = execute
    factory = MagicMock(side_effect=lambda: nullcontext(session))
    return ProfileInvalidationStore(factory, TransactionWaitPolicy(11, 22)), session, state, calls, factory


def test_append_order_parameters_header_only_and_idempotency() -> None:
    store, session, _, calls, _ = harness()
    result = store.append_confirmed(REQUEST)
    assert not result.already_present and result.event.recorded_at == NOW
    assert calls[0][0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert calls[1][1] == {"lock_timeout": "11ms", "statement_timeout": "22ms"}
    assert "set_config('TimeZone', 'UTC', true)" in calls[1][0]
    assert "FROM market_data_invalidation" in calls[2][0]
    assert all("FROM volume_profile_snapshot" in sql for sql, _ in calls[3:5])
    assert "ON CONFLICT (event_id) DO NOTHING RETURNING" in calls[5][0]
    assert calls[5][1] == asdict(REQUEST) and "recorded_at" not in calls[5][1]
    assert not any("FOR UPDATE" in sql or "pg_advisory" in sql or "volume_profile_bin" in sql for sql, _ in calls)
    assert session.begin.return_value.__exit__.call_args.args == (None, None, None)
    calls.clear()
    again = store.append_confirmed(REQUEST)
    assert again == InvalidationAppendResult(result.event, True)
    assert len(calls) == 3  # Replay never revalidates reference headers.
    with pytest.raises(FrozenInstanceError):
        setattr(again, "already_present", False)


@pytest.mark.parametrize("field,value", [("event_id", "other"), ("snapshot_id", "c" * 64),
    ("reason_code", "OTHER"), ("replacement_snapshot_id", None), ("source", "other")])
def test_existing_all_five_fields_conflict_and_loser_uses_same_classifier(field: str, value: Any) -> None:
    for loser in (False, True):
        store, _, state, calls, _ = harness()
        event = dict(**asdict(REQUEST), recorded_at=NOW)
        event[field] = value
        state.update(event=None if loser else event, winner=event, loser=loser)
        with pytest.raises(InvalidationConflict):
            store.append_confirmed(REQUEST)
        assert sum(sql.startswith("INSERT") for sql, _ in calls) == int(loser)


def test_conflict_loser_converges_and_missing_winner_is_integrity_error() -> None:
    store, _, state, calls, _ = harness()
    state["loser"] = True
    assert store.append_confirmed(REQUEST).already_present
    assert "FROM market_data_invalidation" in calls[-1][0]
    state.update(event=None, winner=None)
    with pytest.raises(InvalidationIntegrityError):
        store.append_confirmed(REQUEST)


def test_optional_replacement_and_corrupt_content_do_not_prevent_revocation() -> None:
    store, _, state, calls, _ = harness()
    request = replace(REQUEST, replacement_snapshot_id=None)
    state["winner"] = dict(**asdict(request), recorded_at=NOW)
    state["target"].update(content_sha256="broken", base_volume=None, published_at=None)
    result = store.append_confirmed(request)
    assert result.event.replacement_snapshot_id is None
    assert sum("FROM volume_profile_snapshot" in sql for sql, _ in calls) == 1
    assert store.get(request.event_id) == result.event
    for value in (1, None):
        with pytest.raises(ValueError):
            InvalidationAppendResult(result.event, value)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        InvalidationAppendResult(MagicMock(), True)
    with pytest.raises(FrozenInstanceError):
        setattr(request, "source", "other")


@pytest.mark.parametrize("which,key,value", [("target", "quality", "PARTIAL"),
    ("replacement", "quality", "CONFLICT"), ("replacement", "product_id", "other"),
    ("replacement", "window_start_ms", 1), ("replacement", "window_end_ms", 1),
    ("replacement", "grid_id", "other"), ("replacement", "algorithm_version", "other"),
    ("target", "missing", None), ("replacement", "missing", None)])
def test_reference_header_requirements_without_content_verification(which: str, key: str, value: Any) -> None:
    store, _, state, calls, _ = harness()
    if key == "missing":
        state[which] = None
    else:
        state[which][key] = value
    with pytest.raises(InvalidationReferenceError):
        store.append_confirmed(REQUEST)
    assert not any(sql.startswith("INSERT") for sql, _ in calls)


def test_reads_are_read_only_ordered_bounded_and_return_no_partial_list() -> None:
    store, _, state, calls, _ = harness()
    assert store.get(REQUEST.event_id) is None
    get_sql, get_params = calls[-1]
    assert "WHERE market_data_invalidation.event_id = %(event_id_1)s" in get_sql
    assert get_params == {"event_id_1": REQUEST.event_id}
    assert store.list_for_snapshot(REQUEST.snapshot_id) == ()
    list_sql, list_params = calls[-1]
    assert "WHERE market_data_invalidation.snapshot_id = %(snapshot_id_1)s" in list_sql
    assert list_params == {"snapshot_id_1": REQUEST.snapshot_id, "param_1": 101}
    state["rows"] = [dict(**asdict(replace(REQUEST, event_id=name)), recorded_at=NOW) for name in ("A", "a")]
    events = store.list_for_snapshot(REQUEST.snapshot_id, limit=2)
    assert type(events) is tuple and [event.event_id for event in events] == ["A", "a"]
    assert "ORDER BY market_data_invalidation.recorded_at, market_data_invalidation.event_id" in calls[-1][0]
    assert calls[-1][1]["param_1"] == 3
    with pytest.raises(InvalidationReadTooLarge):
        store.list_for_snapshot(REQUEST.snapshot_id, limit=1)
    assert all(sql.startswith(("SELECT", "SET TRANSACTION")) and "FOR UPDATE" not in sql and "pg_advisory" not in sql
               for sql, _ in calls)
    assert all("READ COMMITTED, READ ONLY" in sql for sql, _ in calls if sql.startswith("SET"))


@pytest.mark.parametrize("key,value", [("recorded_at", NOW.replace(tzinfo=None)), ("recorded_at", NOW.replace(microsecond=1)),
    ("event_id", "bad\x00"), ("source", "é"), ("replacement_snapshot_id", REQUEST.snapshot_id)])
def test_corrupt_rows_never_become_missing(key: str, value: Any) -> None:
    store, _, state, _, _ = harness()
    row = dict(**asdict(REQUEST), recorded_at=NOW)
    row[key] = value
    state.update(event=row, rows=[row])
    for action in (lambda: store.get(REQUEST.event_id), lambda: store.list_for_snapshot(REQUEST.snapshot_id),
                   lambda: store.append_confirmed(REQUEST)):
        with pytest.raises(InvalidationIntegrityError, match="^invalid stored invalidation$"):
            action()


def test_exact_inputs_limits_and_session_guard() -> None:
    store, session, _, _, factory = harness()
    class SubInt(int):
        pass
    class SubStr(str):
        pass
    for limit in (True, 0, 1001, 1.0, SubInt(1)):
        with pytest.raises(ValueError):
            store.list_for_snapshot(REQUEST.snapshot_id, limit=limit)  # type: ignore[arg-type]
    for field, value in (("event_id", "e" * 129), ("reason_code", "r" * 65), ("source", SubStr("s")),
                         ("snapshot_id", "A" * 64), ("replacement_snapshot_id", REQUEST.snapshot_id)):
        with pytest.raises(ValueError):
            replace(REQUEST, **{field: value})
    class SubRequest(ConfirmedInvalidationRequest):
        pass
    with pytest.raises(ValueError):
        store.append_confirmed(SubRequest(**asdict(REQUEST)))
    with pytest.raises(ValueError):
        store.get(SubStr("event"))
    with pytest.raises(ValueError):
        ProfileInvalidationStore(factory, MagicMock())
    with pytest.raises(ValueError):
        store.append_confirmed(None)  # type: ignore[arg-type]
    factory.assert_not_called()
    for limit in (1, 1000):
        assert store.list_for_snapshot(REQUEST.snapshot_id, limit=limit) == ()
    for dialect, active in (("sqlite", False), ("postgresql", True)):
        session.get_bind.return_value.dialect.name = dialect
        session.in_transaction.return_value = active
        session.execute.reset_mock()
        with pytest.raises(ValueError):
            store.get(REQUEST.event_id)
        session.execute.assert_not_called()


def test_db_and_commit_ack_unknown_propagate_without_retry() -> None:
    for commit in (False, True):
        store, session, state, _, _ = harness()
        error = DBAPIError("bounded", None, Exception("injected"))
        if commit:
            session.begin.return_value.__exit__.side_effect = error
        else:
            session.execute.side_effect = error
        with pytest.raises(DBAPIError) as caught:
            store.append_confirmed(REQUEST)
        assert caught.value is error
        if commit:
            session.begin.return_value.__exit__.side_effect = None
            assert state["event"] is not None and store.append_confirmed(REQUEST).already_present
            # Fake models ACK loss, not evidence of actual commit or rollback.
