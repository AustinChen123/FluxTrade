from dataclasses import replace
from unittest.mock import MagicMock

import pytest

from src.core.market_data.profiles.bootstrap_seed_store import (
    BootstrapSeedAdmission,
    BootstrapSeedAdmissionError,
)
from test_bootstrap_seed_admission import harness
from test_profile_bootstrap_seed import seed


def run(*, present=False, batch=False, row=None, error=None, error_at=1):
    store, _, connection, _ = harness()
    original = connection.execute.side_effect
    queries = []
    replies = iter((present, batch))

    def execute(sql, params):
        sql = str(sql)
        if "advisory" in sql:
            return original(sql, params)
        queries.append((sql, params))
        if error is not None and len(queries) == error_at:
            raise error
        if "FROM strategy_state WHERE" in sql:
            return MagicMock(mappings=lambda: MagicMock(one_or_none=lambda: row))
        return MagicMock(scalar_one=lambda: next(replies))

    connection.execute.side_effect = execute
    return store, connection, queries


def test_clean_absent_and_exact_sql_scope_and_binds():
    store, connection, queries = run(
        row=dict(status="DISCOVERED", version=0, audit_count=0, ran=False)
    )
    key = seed().key
    with store.initial_admission(key) as handle:
        proof = handle.read_history(key, 60000)
    assert proof.state == "ABSENT" and proof.key is key
    assert proof.boundary_bar_start_ms == 60000
    assert connection.execute.call_count == 5
    sql, params = queries[0]
    for table in (
        "market_data_bootstrap_seed",
        "market_data_decision_input",
        "market_data_decision_outcome",
    ):
        assert f"EXISTS (SELECT 1 FROM {table} WHERE" in sql
    assert sql.count("environment=:environment") == 3
    assert sql.count("execution_scope_id=:execution_scope_id") == 3
    assert sql.count("product_id=:product_id") == 3
    assert sql.count("strategy_id=:strategy_id") == 3
    assert sql.count(") OR EXISTS (") == 2
    assert "JOIN" not in sql and "market_data_application" not in sql
    assert sql.count(") OR EXISTS (") == 2
    assert sql.count("timeframe=:timeframe") == 2
    assert "split_part(trigger_id, ':', 1)=:timeframe" in sql
    assert (
        "LIKE" not in sql and "config_hash" not in sql and "strategy_version" not in sql
    )
    assert "cutover" not in sql and "60000" not in sql
    assert params == dict(
        environment=key.environment,
        execution_scope_id=key.execution_scope_id,
        product_id=key.product_id,
        strategy_id=key.strategy_id,
        timeframe=key.timeframe,
    )
    assert "strategy_id" not in queries[1][0]
    assert (
        "from_status NOT IN ('DISCOVERED','READY','WARNING') OR to_status NOT IN ('DISCOVERED','READY','WARNING')"
        in queries[2][0]
    )
    assert queries[2][1] == {"strategy_id": key.strategy_id}


def test_present_precedes_batch_and_lifecycle():
    store, connection, queries = run(present=True)
    key = replace(seed().key, strategy_version="v2", config_hash="f" * 64)
    with store.initial_admission(key) as handle:
        assert handle.read_history(key, 0).state == "PRESENT"
    assert len(queries) == 1 and connection.execute.call_count == 3


def test_any_batch_is_conservative_unknown_until_bounded_codec_slice():
    store, _, queries = run(batch=True)
    with store.initial_admission(seed().key) as handle:
        assert handle.read_history(seed().key, 0).state == "UNKNOWN"
    assert len(queries) == 2


@pytest.mark.parametrize(
    "status,version,count,ran,expected",
    [
        ("READY", 1, 1, False, "ABSENT"),
        ("WARNING", 2, 2, False, "ABSENT"),
        ("DISCOVERED", 1, 1, False, "UNKNOWN"),
        ("ACTIVE", 0, 0, False, "UNKNOWN"),
        ("RUNNING", 0, 0, False, "UNKNOWN"),
        ("STOPPED", 1, 1, False, "UNKNOWN"),
        ("ERROR", 1, 1, False, "UNKNOWN"),
        ("READY", 1, 1, True, "UNKNOWN"),
        ("READY", 1, 0, False, "UNKNOWN"),
        ("READY", -1, -1, False, "UNKNOWN"),
        ("READY", True, 1, False, "UNKNOWN"),
        ("READY", 1, True, False, "UNKNOWN"),
        ("READY", 0, 0, 0, "UNKNOWN"),
        (None, 0, 0, False, "UNKNOWN"),
    ],
)
def test_lifecycle_complete_audit_matrix(status, version, count, ran, expected):
    store, _, _ = run(
        row=dict(status=status, version=version, audit_count=count, ran=ran)
    )
    with store.initial_admission(seed().key) as handle:
        assert handle.read_history(seed().key, 0).state == expected


@pytest.mark.parametrize("phase", [1, 2, 3])
def test_missing_state_unknown_and_query_error_sanitized(phase):
    store, _, _ = run()
    with store.initial_admission(seed().key) as handle:
        assert handle.read_history(seed().key, 0).state == "UNKNOWN"
    error = RuntimeError("SECRET")
    store, connection, _ = run(error=error, error_at=phase)
    with pytest.raises(BootstrapSeedAdmissionError) as caught:
        with store.initial_admission(seed().key) as handle:
            handle.read_history(seed().key, 0)
    assert str(caught.value) == "BOOTSTRAP_SEED_ADMISSION"
    assert caught.value.__cause__ is None
    assert connection.execute.call_count == phase + 2


def test_construction_replace_and_copy_cannot_extend_lease():
    from copy import copy

    with pytest.raises(BootstrapSeedAdmissionError):
        BootstrapSeedAdmission(seed().key)
    store, _, queries = run()
    with store.initial_admission(seed().key) as handle:
        with pytest.raises(BootstrapSeedAdmissionError):
            replace(handle)
        cloned = copy(handle)
        assert cloned._lease is handle._lease
    for value in (handle, cloned):
        assert not value._active
        with pytest.raises(BootstrapSeedAdmissionError):
            value.read_history(seed().key, 0)
    assert queries == []


@pytest.mark.parametrize("values", [{"present": 1}, {"batch": None}])
def test_membership_result_requires_exact_boolean(values):
    store, _, _ = run(**values)
    with pytest.raises(BootstrapSeedAdmissionError):
        with store.initial_admission(seed().key) as handle:
            handle.read_history(seed().key, 0)


@pytest.mark.parametrize("boundary", [0, 60000, 120000])
def test_boundary_never_filters_history_or_changes_precedence(boundary):
    store, _, queries = run(
        present=True,
        batch=True,
        row=dict(status="ACTIVE", version=1, audit_count=1, ran=True),
    )
    key = replace(seed().key, strategy_version="other", config_hash="e" * 64)
    with store.initial_admission(key) as handle:
        evidence = handle.read_history(key, boundary)
    assert evidence.state == "PRESENT" and evidence.boundary_bar_start_ms == boundary
    assert len(queries) == 1
    sql, binds = queries[0]
    assert set(binds) == {
        "environment",
        "execution_scope_id",
        "strategy_id",
        "product_id",
        "timeframe",
    }
    assert not any(
        word in sql
        for word in (
            "recorded_at",
            "decision_time",
            "cutover",
            "version",
            "config_hash",
            "<",
            ">",
        )
    )


@pytest.mark.parametrize(
    "row",
    [
        {},
        {"status": "READY"},
        dict(status="READY", version=None, audit_count=0, ran=False),
    ],
)
def test_malformed_lifecycle_cannot_prove_absence(row):
    store, _, _ = run(row=row)
    with store.initial_admission(seed().key) as handle:
        if "version" not in row:
            with pytest.raises(BootstrapSeedAdmissionError):
                handle.read_history(seed().key, 0)
        else:
            assert handle.read_history(seed().key, 0).state == "UNKNOWN"
