"""Recording-session contracts; real commit/concurrency evidence lives in migrations tests."""

from contextlib import nullcontext
from dataclasses import FrozenInstanceError, asdict, replace
from datetime import datetime, timezone
from unittest.mock import MagicMock
from typing import Any, cast

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles.decision_input_store import (
    DecisionInputStore,
    DecisionInputRecord,
    DecisionInputConflict,
    DecisionInputIntegrityError,
)
from src.core.market_data.profiles.repository import TransactionWaitPolicy
from test_profile_decision_input import sample

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def row(value=None):
    value = sample() if value is None else value
    return dict(
        **asdict(value.key),
        input_id=value.input_id,
        requirements_digest=value.requirements_digest,
        policy_digest=value.policy_digest,
        input_digest=value.input_digest,
        decision_time_ms=value.decision_time_ms,
        contract_version=1,
        canonical_payload=value.canonical_bytes,
        recorded_at=NOW,
    )


def harness():
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = False
    calls = []
    state: dict[str, Any] = dict(rows=[], winner=row(), loser=False)

    def execute(statement):
        compiled = statement.compile(dialect=dialect())
        sql = str(compiled)
        calls.append((sql, compiled.params))
        result = MagicMock()
        if sql.startswith("INSERT"):
            state["rows"] = [] if state["winner"] is None else [state["winner"]]
            result.mappings.return_value.one_or_none.return_value = (
                None if state["loser"] else state["winner"]
            )
        else:
            result.mappings.return_value.all.return_value = state["rows"]
        return result

    session.execute.side_effect = execute
    factory = MagicMock(side_effect=lambda: nullcontext(session))
    return (
        DecisionInputStore(factory, TransactionWaitPolicy(11, 22)),
        session,
        state,
        calls,
        factory,
    )


def test_winner_readback_readonly_confirm_and_exact_sql():
    store, session, _, calls, factory = harness()
    value = sample()
    record = store.pin(value)
    assert record == DecisionInputRecord(value, NOW, False)
    assert calls[0][0] == "SET TRANSACTION ISOLATION LEVEL READ COMMITTED"
    assert calls[1][1] == {"lock_timeout": "11ms", "statement_timeout": "22ms"}
    assert "set_config('TimeZone', 'UTC', true)" in calls[1][0]
    query, params = calls[2]
    assert "input_id = %(input_id_1)s OR" in query and "LIMIT %(param_1)s" in query
    assert params == {
        "input_id_1": value.input_id,
        "param_1": 2,
        **{f"{key}_1": val for key, val in asdict(value.key).items()},
    }
    for key in asdict(value.key):
        assert f"market_data_decision_input.{key} = %({key}_1)s" in query
    assert "ON CONFLICT DO NOTHING RETURNING" in calls[3][0]
    assert calls[3][1] == {k: v for k, v in row(value).items() if k != "recorded_at"}
    assert session.begin.return_value.__exit__.call_args.args == (None, None, None)
    for read in (
        lambda: store.get(value.key),
        lambda: store.confirm(value),
        lambda: store.pin(value),
    ):
        calls.clear()
        assert read() == replace(record, already_present=True)
        assert len(calls) == 3
    assert factory.call_count == 4
    calls.clear()
    store.confirm(value)
    assert calls[0][0].endswith(", READ ONLY")
    assert not any(
        word in sql
        for sql, _ in calls
        for word in ("INSERT", "UPDATE", "DELETE", "FOR UPDATE", "advisory", "snapshot")
    )
    with pytest.raises(FrozenInstanceError):
        setattr(record, "already_present", True)


def test_loser_missing_and_valid_conflict():
    store, _, state, _, _ = harness()
    state["loser"] = True
    assert store.pin(sample()).already_present
    # Use an empty valid envelope to vary content without changing its key.
    different = replace(
        sample(), requirements=(), context=replace(sample().context, profiles=())
    )
    for method in (store.pin, store.confirm):
        with pytest.raises(DecisionInputConflict, match="^DECISION_INPUT_CONFLICT$"):
            method(different)
    state.update(rows=[], winner=None)
    assert store.get(sample().key) is None and store.confirm(sample()) is None
    with pytest.raises(DecisionInputIntegrityError):
        store.pin(sample())


@pytest.mark.parametrize("field", list(row()))
@pytest.mark.parametrize("method", ["pin", "get", "confirm"])
def test_every_redundant_column_and_payload_corruption(field, method):
    store, _, state, _, _ = harness()
    state["rows"] = [{**row(), field: "SECRET"}]
    with pytest.raises(DecisionInputIntegrityError) as caught:
        getattr(store, method)(sample().key if method == "get" else sample())
    assert (
        str(caught.value) == "DECISION_INPUT_INTEGRITY"
        and caught.value.__cause__ is None
    )


def test_multi_claimants_binary_driver_and_timestamp_domain():
    store, _, state, _, _ = harness()
    state["rows"] = [row(), row()]
    with pytest.raises(DecisionInputIntegrityError):
        store.get(sample().key)
    state["rows"] = [
        {**row(), "canonical_payload": memoryview(sample().canonical_bytes)}
    ]
    record = store.get(sample().key)
    assert record is not None and record.value == sample()
    for stamp in (NOW.replace(tzinfo=None), NOW.replace(microsecond=1), True):
        state["rows"] = [{**row(), "recorded_at": stamp}]
        with pytest.raises(DecisionInputIntegrityError):
            store.get(sample().key)


def test_guards_exceptions_and_no_retry():
    store, session, _, calls, factory = harness()
    for method in (store.pin, store.get, store.confirm):
        with pytest.raises(DecisionInputIntegrityError):
            method(cast(Any, True))
    factory.assert_not_called()
    for attribute, invalid in ((session.in_transaction, True),):
        attribute.return_value = invalid
        with pytest.raises(DecisionInputIntegrityError):
            store.pin(sample())
        assert not calls
    session.in_transaction.return_value = False
    session.get_bind.return_value.dialect.name = "sqlite"
    with pytest.raises(DecisionInputIntegrityError):
        store.pin(sample())
    session.get_bind.return_value.dialect.name = "postgresql"
    error = DBAPIError(None, None, RuntimeError("SECRET"))
    session.execute.side_effect = error
    with pytest.raises(DBAPIError) as caught:
        store.pin(sample())
    assert caught.value is error
    store, session, _, calls, _ = harness()
    session.begin.return_value.__exit__.side_effect = error
    with pytest.raises(DBAPIError) as caught:
        store.pin(sample())
    assert (
        caught.value is error and sum(sql.startswith("INSERT") for sql, _ in calls) == 1
    )


def test_exact_record_types_and_claimant_key_mismatch():
    value = sample()
    for changes in ({"value": True}, {"already_present": 1}, {"recorded_at": None}):
        with pytest.raises(DecisionInputIntegrityError):
            DecisionInputRecord(
                **cast(
                    Any,
                    dict(value=value, recorded_at=NOW, already_present=True) | changes,
                )
            )
    store, _, state, _, _ = harness()
    different = replace(value, key=replace(value.key, strategy_id="other"))
    state["rows"] = [row(different)]
    with pytest.raises(DecisionInputIntegrityError):
        store.get(value.key)


def test_loser_different_valid_content_is_conflict_and_outer_ack_propagates():
    value = sample()
    other = replace(value, requirements=(), context=replace(value.context, profiles=()))
    store, _, state, calls, _ = harness()
    state.update(loser=True, winner=row(other))
    with pytest.raises(DecisionInputConflict):
        store.pin(value)
    assert sum(sql.startswith("INSERT") for sql, _ in calls) == 1
    store, session, _, _, factory = harness()
    error = DBAPIError(None, None, RuntimeError("SECRET"))
    outer = MagicMock()
    outer.__enter__.return_value = session
    outer.__exit__.side_effect = error
    factory.side_effect = lambda: outer
    with pytest.raises(DBAPIError) as caught:
        store.pin(value)
    assert caught.value is error
