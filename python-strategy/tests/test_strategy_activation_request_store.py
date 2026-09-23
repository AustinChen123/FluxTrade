from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock
from typing import Any, cast
from contextlib import nullcontext

import pytest
from sqlalchemy import PickleType, create_engine, literal, select
from sqlalchemy.engine import RowMapping
from sqlalchemy.dialects.postgresql import dialect

from src.core import strategy_activation_request_store as owner
from src.core.strategy_activation_request_store import (
    ProfileActivationRequestRecord as Record,
    ProfileActivationRequestStatus as Status,
    ProfileActivationRequestIntegrityError as Integrity,
    ProfileActivationRequestValidationError as Validation,
    ProfileActivationRequestConflict as Conflict,
    ProfileActivationRequestStore as Store,
    _hydrate,
    _validate_request_id,
)
from test_profile_activation_request import request

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def test_actual_sqlalchemy_row_mapping_is_accepted():
    engine = create_engine("sqlite://")
    columns = [literal(v, type_=PickleType).label(k) for k, v in row().items()]
    try:
        with engine.connect() as connection:
            result = connection.execute(select(*columns)).mappings().one()
        assert type(result) is RowMapping
        assert _hydrate(result, request().request_id) == Record(
            request(), Status.PENDING, NOW
        )
    finally:
        engine.dispose()


def row() -> dict[str, Any]:
    value = request()
    return dict(
        request_id=value.request_id,
        payload_digest=value.payload_digest,
        canonical_payload=value.canonical_bytes,
        contract_version=1,
        environment=value.intent.key.environment,
        execution_scope_id=value.intent.key.execution_scope_id,
        strategy_id=value.intent.key.strategy_id,
        expected_state_version=0,
        status="PENDING",
        requested_at=NOW,
        terminal_at=None,
        terminal_reason=None,
    )


@pytest.mark.parametrize("status", list(Status))
@pytest.mark.parametrize("binary", [bytes, memoryview])
def test_valid_record_roundtrip(status, binary):
    data = row()
    data.update(
        status=status.value, canonical_payload=binary(data["canonical_payload"])
    )
    if status is not Status.PENDING:
        data.update(terminal_at=NOW, terminal_reason="DONE")
    result = _hydrate(data, data["request_id"])
    assert result == Record(
        request(), status, NOW, data["terminal_at"], data["terminal_reason"]
    )
    with pytest.raises(FrozenInstanceError):
        setattr(result, "status", Status.STALE)
    assert not hasattr(result, "__dict__")


@pytest.mark.parametrize("field", list(row()))
def test_every_column_damage_and_missing_is_sanitized(field):
    for missing in (False, True):
        data = row()
        if missing:
            del data[field]
        else:
            data[field] = "SECRET"
        with pytest.raises(
            Integrity, match="^PROFILE_ACTIVATION_REQUEST_INTEGRITY$"
        ) as caught:
            _hydrate(data, request().request_id)
        assert caught.value.__cause__ is None


class Text(str):
    pass


class Integer(int):
    pass


class Stamp(datetime):
    pass


@pytest.mark.parametrize(
    "field,value",
    [
        ("requested_at", NOW.replace(tzinfo=None)),
        ("requested_at", NOW.replace(microsecond=1)),
        ("requested_at", NOW.replace(tzinfo=timezone(timedelta(hours=1)))),
        ("requested_at", Stamp(2026, 1, 1, tzinfo=timezone.utc)),
        ("request", None),
        ("status", "PENDING"),
        ("status", True),
        ("terminal_at", NOW),
        ("terminal_reason", "DONE"),
    ],
)
def test_record_pending_exact_domain(field, value):
    with pytest.raises(Integrity):
        replace(Record(request(), Status.PENDING, NOW), **{field: value})


@pytest.mark.parametrize(
    "field,value",
    [
        ("terminal_at", None),
        ("terminal_at", NOW - timedelta(milliseconds=1)),
        ("terminal_at", NOW.replace(microsecond=1)),
        ("terminal_at", NOW.replace(tzinfo=None)),
        ("terminal_reason", None),
        ("terminal_reason", ""),
        ("terminal_reason", "bad"),
        ("terminal_reason", "É"),
        ("terminal_reason", "A" * 129),
        ("terminal_reason", Text("DONE")),
    ],
)
def test_terminal_domain(field, value):
    with pytest.raises(Integrity):
        replace(Record(request(), Status.CONSUMED, NOW, NOW, "DONE"), **{field: value})
    assert Record(request(), Status.STALE, NOW, NOW, "A" * 128)


@pytest.mark.parametrize("invalid", [None, True, "A" * 64, "a" * 63, Text("a" * 64)])
def test_request_id_exact_validation(invalid):
    with pytest.raises(Validation):
        _validate_request_id(invalid)


def test_scalar_subclasses_wrong_identity_and_strict_decoder():
    for field, value in (
        ("contract_version", True),
        ("expected_state_version", Integer(0)),
        ("environment", Text("live")),
        ("status", Text("PENDING")),
        ("canonical_payload", b"{}"),
        ("canonical_payload", bytearray(b"{}")),
    ):
        data = row()
        data[field] = value
        with pytest.raises(Integrity):
            _hydrate(data, request().request_id)
    with pytest.raises(Integrity):
        _hydrate(row(), "a" * 64)


def test_memoryview_nbytes_gate_precedes_decoder(monkeypatch):
    data = row()
    data["canonical_payload"] = memoryview(b"x" * 65540).cast("I")
    assert len(data["canonical_payload"]) < 65536 < data["canonical_payload"].nbytes
    decoder = MagicMock(side_effect=AssertionError("must not decode"))
    monkeypatch.setattr(owner.ProfileActivationRequest, "from_canonical_bytes", decoder)
    with pytest.raises(Integrity):
        _hydrate(data, data["request_id"])
    decoder.assert_not_called()


def test_exact_request_and_binary_subclasses():
    class RequestSubclass(owner.ProfileActivationRequest):
        pass

    class Binary(bytes):
        pass

    value = request()
    subclass = RequestSubclass(
        value.actor, value.idempotency_key, value.command, value.intent
    )
    with pytest.raises(Integrity):
        Record(subclass, Status.PENDING, NOW)
    data = row()
    for raw in (Binary(value.canonical_bytes), b"", b"x" * 65537, memoryview(b"")):
        data["canonical_payload"] = raw
        with pytest.raises(Integrity):
            _hydrate(data, value.request_id)


def store_harness():
    sessions, calls, exits = [], [], []
    rows = [row()]

    def fresh():
        session = MagicMock()
        session.get_bind.return_value.dialect.name = "postgresql"
        session.in_transaction.return_value = False

        def execute(statement):
            compiled = statement.compile(dialect=dialect())
            calls.append((str(compiled), compiled.params))
            result = MagicMock()
            result.mappings.return_value.all.return_value = rows
            return result

        session.execute.side_effect = execute
        session.begin.return_value.__exit__.side_effect = lambda *_: exits.append(
            "transaction"
        )
        context = MagicMock()
        context.__enter__.return_value = session
        context.__exit__.side_effect = lambda *_: exits.append("session")
        sessions.append(session)
        return context

    factory = MagicMock(side_effect=fresh)
    return Store(factory), rows, sessions, calls, exits, factory


def test_fresh_readonly_get_confirm_and_missing():
    store, rows, sessions, calls, exits, factory = store_harness()
    rows[0].update(status="CONSUMED", terminal_at=NOW, terminal_reason="DONE")
    record = store.get(request().request_id)
    assert exits == ["transaction", "session"]
    assert record is not None and record.status is Status.CONSUMED
    assert store.confirm(request()) == record
    assert sessions[0] is not sessions[1]
    assert calls[0][0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY"
    assert calls[1][1] == {"lock_timeout": "2000ms", "statement_timeout": "10000ms"}
    assert "set_config('TimeZone', 'UTC', true)" in calls[1][0]
    assert "WHERE strategy_profile_activation_request.request_id =" in calls[2][0]
    assert " OR " not in calls[2][0]
    assert calls[2][1] == {"request_id_1": request().request_id, "param_1": 2}
    rows.clear()
    assert store.get(request().request_id) is None and store.confirm(request()) is None
    assert factory.call_count == 4 and len(calls) == 12
    assert all(sql.startswith(("SET", "SELECT")) for sql, _ in calls)


def test_public_validation_before_session_and_hydration_remapping():
    store, _, _, _, _, factory = store_harness()
    for invalid in (None, True, "SECRET", Text("a" * 64)):
        with pytest.raises(Validation):
            store.get(cast(Any, invalid))
        with pytest.raises(Validation):
            store.confirm(cast(Any, invalid))

    class RequestSubclass(owner.ProfileActivationRequest):
        pass

    value = request()
    with pytest.raises(Validation):
        store.confirm(
            RequestSubclass(
                value.actor, value.idempotency_key, value.command, value.intent
            )
        )
    factory.assert_not_called()
    with pytest.raises(Integrity):
        _hydrate(row(), "SECRET")


def test_valid_conflict_only_after_full_integrity():
    store, rows, *_ = store_harness()
    value = request()
    changed = replace(value, command=type(value.command).RESUME)
    assert changed.command_ref == value.command_ref
    with pytest.raises(Conflict, match="^PROFILE_ACTIVATION_REQUEST_CONFLICT$"):
        store.confirm(changed)
    rows[0]["payload_digest"] = "SECRET"
    with pytest.raises(Integrity):
        store.confirm(changed)
    rows.append(row())
    with pytest.raises(Integrity):
        store.get(value.request_id)


@pytest.mark.parametrize("phase", ["query", "transaction_exit", "session_exit"])
@pytest.mark.parametrize("error", [RuntimeError("SECRET"), KeyboardInterrupt()])
def test_db_and_exit_errors_propagate_without_retry(phase, error):
    store, _, sessions, _, exits, factory = store_harness()
    context = factory.side_effect()
    factory.side_effect = lambda: context
    session = sessions[0]
    if phase == "query":
        execute = session.execute.side_effect
        calls = 0

        def fail_select(statement):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise error
            return execute(statement)

        session.execute.side_effect = fail_select
    elif phase == "transaction_exit":
        session.begin.return_value.__exit__.side_effect = error
    else:
        context.__exit__.side_effect = error
    with pytest.raises(type(error)) as caught:
        store.get(request().request_id)
    assert caught.value is error and factory.call_count == 1
    session.commit.assert_not_called()
    session.rollback.assert_not_called()


@pytest.mark.parametrize("guard", ["dialect", "active"])
def test_fresh_postgres_guard(guard):
    store, _, sessions, calls, _, factory = store_harness()
    factory.side_effect()
    session = sessions[0]
    factory.side_effect = lambda: nullcontext(session)
    if guard == "dialect":
        session.get_bind.return_value.dialect.name = "sqlite"
    else:
        session.in_transaction.return_value = True
    with pytest.raises(Integrity):
        store.get(request().request_id)
    assert calls == []


def admission_harness(
    existing=None, slot=None, version=0, winner=True, pk_loser=None, slot_loser=None
):
    store, _, sessions, calls, exits, factory = store_harness()
    context = factory.side_effect()
    factory.side_effect = lambda: context
    state = dict(ids=0, slots=0)

    def execute(statement):
        compiled = statement.compile(dialect=dialect())
        sql = str(compiled)
        calls.append((sql, compiled.params))
        result = MagicMock()
        if "FOR UPDATE" in sql:
            result.one_or_none.return_value = None if version is None else (version,)
        elif sql.startswith("INSERT"):
            result.mappings.return_value.one_or_none.return_value = (
                row() if winner else None
            )
        elif "FROM strategy_profile_activation_request" in sql:
            name = "slots" if "status =" in sql else "ids"
            selected = (
                (slot if state[name] == 0 else slot_loser)
                if name == "slots"
                else (existing if state[name] == 0 else pk_loser)
            )
            state[name] += 1
            result.mappings.return_value.all.return_value = (
                [] if selected is None else [selected]
            )
        return result

    sessions[0].execute.side_effect = execute
    return store, calls, exits, factory, sessions[0]


def test_admission_order_lock_bind_and_one_insert():
    store, calls, exits, factory, _ = admission_harness()
    value = request()
    assert store.admit(value, current=value.intent).request == value
    assert exits == ["transaction", "session"] and factory.call_count == 1
    assert calls[0][0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert calls[1][1] == {"lock_timeout": "2000ms", "statement_timeout": "10000ms"}
    assert "TimeZone" in calls[1][0]
    assert "FOR UPDATE" in calls[2][0] and calls[2][1] == {
        "strategy_id_1": value.intent.key.strategy_id
    }
    assert "request_id =" in calls[3][0]
    assert calls[4][1] == {
        "environment_1": "live",
        "execution_scope_id_1": "deployment",
        "strategy_id_1": "strategy",
        "status_1": "PENDING",
        "param_1": 2,
    }
    assert "ON CONFLICT DO NOTHING RETURNING" in calls[5][0] and len(calls) == 6
    assert calls[5][1] == {
        k: v
        for k, v in row().items()
        if k not in ("requested_at", "terminal_at", "terminal_reason")
    }


@pytest.mark.parametrize("status", list(Status))
@pytest.mark.parametrize("version", [99, True, -1, 1 << 31])
def test_existing_before_stale_current_and_locked_version(status, version):
    data = row()
    data.update(status=status.value)
    if status is not Status.PENDING:
        data.update(terminal_at=NOW, terminal_reason="DONE")
    store, calls, *_ = admission_harness(existing=data, version=version)
    value = request()
    current = replace(value.intent, expected_state_version=99)
    if version == 99:
        assert store.admit(value, current=current).status is status
    else:
        with pytest.raises(Integrity):
            store.admit(value, current=current)
    assert len(calls) == (4 if version == 99 else 3)


@pytest.mark.parametrize(
    "damage",
    ["current", "version", "missing_state", "conflict", "corrupt"],
)
def test_admission_rejection_never_inserts(damage):
    value = request()
    kwargs: dict[str, Any] = {}
    current = value.intent
    expected = Integrity
    if damage == "current":
        current = replace(
            current, requirements=replace(current.requirements, lookback_window=3)
        )
        expected = owner.ProfileActivationRequestStale
    elif damage in ("version", "missing_state"):
        kwargs["version"] = 1 if damage == "version" else None
        if damage == "version":
            expected = owner.ProfileActivationRequestStale
    else:
        data = row()
        if damage == "conflict":
            changed = replace(value, command=type(value.command).RESUME)
            data.update(
                canonical_payload=changed.canonical_bytes,
                payload_digest=changed.payload_digest,
            )
            expected = Conflict
        elif damage == "corrupt":
            data["payload_digest"] = "SECRET"
        kwargs["existing"] = data
    store, calls, *_ = admission_harness(**kwargs)
    with pytest.raises(expected):
        store.admit(value, current=current)
    assert not any(sql.startswith("INSERT") for sql, _ in calls)


@pytest.mark.parametrize(
    "loser",
    "pk none slot slot_different slot_corrupt early early_different early_corrupt".split(),
)
def test_single_insert_loser_readback(loser):
    data = row()
    if "different" in loser:
        changed = replace(request(), idempotency_key="other")
        data.update(
            request_id=changed.request_id,
            canonical_payload=changed.canonical_bytes,
            payload_digest=changed.payload_digest,
        )
    if "corrupt" in loser:
        data["payload_digest"] = "SECRET"
    store, calls, *_ = admission_harness(
        winner=False,
        slot=data if loser.startswith("early") else None,
        pk_loser=row() if loser == "pk" else None,
        slot_loser=data if loser.startswith("slot") else None,
    )
    value = request()
    if loser in ("pk", "slot", "early"):
        assert store.admit(value, current=value.intent).request == value
    else:
        with pytest.raises(Conflict if "different" in loser else Integrity):
            store.admit(value, current=value.intent)
    assert sum(sql.startswith("INSERT") for sql, _ in calls) == int(
        not loser.startswith("early")
    )
    assert len(calls) == (5 if loser.startswith("early") else 7 if loser == "pk" else 8)


@pytest.mark.parametrize("phase", ["query", "exit"])
@pytest.mark.parametrize("error", [RuntimeError("SECRET"), KeyboardInterrupt()])
def test_admission_error_identity(phase, error):
    store, _, _, factory, session = admission_harness()
    if phase == "query":
        session.execute.side_effect = error
    else:
        session.begin.return_value.__exit__.side_effect = error
    with pytest.raises(type(error)) as caught:
        store.admit(request(), current=request().intent)
    assert caught.value is error and factory.call_count == 1


def test_admission_exact_inputs_before_session():
    store, _, _, _, _, factory = store_harness()
    for args in ((None, request().intent), (request(), None)):
        with pytest.raises(Validation):
            store.admit(cast(Any, args[0]), current=cast(Any, args[1]))
    factory.assert_not_called()
