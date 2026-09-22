"""Active-transaction recording tests; no mocked durability claims."""

from dataclasses import replace
from datetime import datetime, timezone
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles import decision_application_store as owner
from test_profile_decision_application import batch, applied, key
from test_profile_decision_input import sample
from test_profile_decision_input_store import row as input_row

NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)


def value():
    return batch((key(),), (replace(applied(), input_digest=sample().input_digest),))


def harness():
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = True
    state: dict[str, Any] = dict(
        headers=[], outcomes=[], inputs=[input_row()], loser=False
    )
    calls = []

    def execute(statement, parameters=None):
        compiled = statement.compile(dialect=dialect())
        sql = str(compiled)
        calls.append((sql, compiled.params, parameters))
        result = MagicMock()
        if sql.startswith("INSERT INTO market_data_decision_batch"):
            state["headers"] = [dict(compiled.params, recorded_at=NOW)]
            result.scalar_one_or_none.return_value = (
                None if state["loser"] else compiled.params["batch_digest"]
            )
        elif sql.startswith("INSERT INTO market_data_decision_outcome"):
            state["outcomes"] = parameters
        elif "FROM market_data_decision_batch" in sql:
            result.mappings.return_value.all.return_value = state["headers"]
        elif "FROM market_data_decision_outcome" in sql:
            result.mappings.return_value.all.return_value = state["outcomes"]
        else:
            result.mappings.return_value.all.return_value = state["inputs"]
        return result

    session.execute.side_effect = execute
    return session, state, calls


def read(session):
    return owner.read_decision_batch(session, **owner._identity(value()))


@pytest.mark.parametrize("empty", [False, True])
def test_append_readback_idempotent_and_lifecycle(empty):
    session, state, calls = harness()
    requested = batch() if empty else value()
    assert read(session) is None
    result = owner.append_decision_batch(session, requested)
    assert result == owner.DecisionBatchRecord(requested, NOW, False)
    assert read(session) == replace(result, already_present=True)
    before = len(calls)
    assert owner.append_decision_batch(session, requested).already_present
    assert not any(sql.startswith("INSERT") for sql, _, _ in calls[before:])
    for method in (session.begin, session.commit, session.rollback, session.close):
        method.assert_not_called()
    assert not any("UPDATE" in sql or "advisory" in sql for sql, _, _ in calls)
    assert state["headers"][0]["participant_count"] == int(not empty)
    query, params, _ = calls[0]
    assert "LIMIT %(param_1)s" in query and params["param_1"] == 2
    for name, item in owner._identity(requested).items():
        assert f"market_data_decision_batch.{name} = %({name}_1)s" in query
        assert params[f"{name}_1"] == item
    if not empty:
        input_queries = [
            sql for sql, _, _ in calls if "FROM market_data_decision_input" in sql
        ]
        assert input_queries and all(" OR " in sql for sql in input_queries)


def test_valid_different_batch_conflict_not_corruption():
    session, _, _ = harness()
    owner.append_decision_batch(session, value())
    with pytest.raises(owner.DecisionBatchConflict, match="^DECISION_BATCH_CONFLICT$"):
        owner.append_decision_batch(session, batch())


def test_conflict_loser_reads_complete_winner_without_outcome_writes():
    session, state, calls = harness()
    state.update(loser=True, outcomes=owner._outcomes(value()))
    assert owner.append_decision_batch(session, value()).already_present
    assert (
        sum(
            sql.startswith("INSERT INTO market_data_decision_batch")
            for sql, _, _ in calls
        )
        == 1
    )
    assert not any(
        sql.startswith("INSERT INTO market_data_decision_outcome")
        for sql, _, _ in calls
    )


def test_binary_timestamp_and_duplicate_header_integrity():
    session, state, _ = harness()
    owner.append_decision_batch(session, value())
    state["headers"][0]["canonical_payload"] = memoryview(value().canonical_bytes)
    assert read(session) is not None
    for stamp in (NOW.replace(microsecond=1), NOW.replace(tzinfo=None)):
        state["headers"][0]["recorded_at"] = stamp
        with pytest.raises(owner.DecisionBatchIntegrityError):
            read(session)
    state["headers"][0]["recorded_at"] = NOW
    state["headers"] *= 2
    with pytest.raises(owner.DecisionBatchIntegrityError):
        read(session)


@pytest.mark.parametrize("field", list(owner._header(value())) + ["recorded_at"])
def test_header_claim_corruption(field):
    session, state, _ = harness()
    owner.append_decision_batch(session, value())
    state["headers"][0][field] = "SECRET"
    with pytest.raises(owner.DecisionBatchIntegrityError) as caught:
        read(session)
    assert (
        str(caught.value) == "DECISION_BATCH_INTEGRITY"
        and caught.value.__cause__ is None
    )


@pytest.mark.parametrize("field", list(owner._outcomes(value())[0]))
def test_outcome_claim_corruption(field):
    session, state, _ = harness()
    owner.append_decision_batch(session, value())
    state["outcomes"][0][field] = "SECRET"
    with pytest.raises(owner.DecisionBatchIntegrityError):
        read(session)


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "extra",
        "duplicate",
        "input_missing",
        "input_digest",
        "input_key",
        "input_payload",
    ],
)
def test_set_and_input_integrity(damage):
    session, state, _ = harness()
    owner.append_decision_batch(session, value())
    if damage == "missing":
        state["outcomes"] = []
    elif damage in ("extra", "duplicate"):
        state["outcomes"] *= 2
    elif damage == "input_missing":
        state["inputs"] = []
    else:
        field = {
            "input_digest": "input_digest",
            "input_key": "strategy_id",
            "input_payload": "canonical_payload",
        }[damage]
        state["inputs"][0][field] = "SECRET"
    with pytest.raises(owner.DecisionBatchIntegrityError):
        read(session)


def test_guards_before_sql_and_db_error_identity():
    session, _, calls = harness()
    for invalid in (True, None):
        with pytest.raises(owner.DecisionBatchIntegrityError):
            owner.append_decision_batch(session, cast(Any, invalid))
    too_long = replace(batch(), environment="a" * 65)
    with pytest.raises(owner.DecisionBatchIntegrityError):
        owner.append_decision_batch(session, too_long)
    for pg, active in (("sqlite", True), ("postgresql", False)):
        session.get_bind.return_value.dialect.name = pg
        session.in_transaction.return_value = active
        with pytest.raises(owner.DecisionBatchIntegrityError):
            owner.append_decision_batch(session, batch())
    assert not calls
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = True
    error = DBAPIError(None, None, RuntimeError("SECRET"))
    session.execute.side_effect = error
    with pytest.raises(DBAPIError) as caught:
        owner.append_decision_batch(session, batch())
    assert caught.value is error and session.execute.call_count == 1
