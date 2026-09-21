"""Recording candidate-read SQL contracts; no true-PostgreSQL acceptance claim."""
import subprocess
import sys
from contextlib import nullcontext
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles import read_repository as reader
from src.core.market_data.profiles.repository import ProfileIntegrityError, TransactionWaitPolicy

DAY = 86400000
ARGS: dict[str, Any] = dict(product_id="BINANCE:BTCUSDT-SPOT", base_grid_id="base", algorithm_version="vp-v1", start_ms=0, end_ms=DAY)
NOW = datetime(2026, 1, 1, microsecond=123456, tzinfo=timezone.utc)


def row(**changes: Any) -> dict[str, Any]:
    return dict(dict(id="a" * 64, revision=1, content_sha256="b" * 64, window_start_ms=0, window_end_ms=DAY,
                     product_id=ARGS["product_id"], grid_id="base", algorithm_version="vp-v1", availability_basis="OBSERVED",
                     computed_at_valid=True, published_at_valid=True, source_available_at_valid=True,
                     computed_at=NOW, published_at=NOW, source_available_at=None, revoked=False), **changes)


def harness(rows: list[dict[str, Any]] | None = None, policy: TransactionWaitPolicy | None = None):
    session = MagicMock()
    session.get_bind.return_value.dialect.name = "postgresql"
    session.in_transaction.return_value = False
    session.execute.return_value.mappings.return_value.all.return_value = [] if rows is None else rows
    factory = MagicMock(side_effect=lambda: nullcontext(session))
    store = reader.ProfileReadRepository(factory) if policy is None else reader.ProfileReadRepository(factory, policy)
    return store, session, factory


def test_exact_transaction_predicates_binds_order_projection_and_revocation() -> None:
    store, session, _ = harness([row()])
    result = store.list_candidates(**ARGS)
    assert type(result) is tuple and result[0].ref.snapshot_id != result[0].ref.content_sha256
    assert result[0].computed_at.microsecond == 123456
    assert session.execute.call_count == 3
    statements = [call.args[0] for call in session.execute.call_args_list]
    assert str(statements[0]) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert statements[1].compile().params == {"lock_timeout": "250ms", "statement_timeout": "1500ms"}
    assert "set_config('TimeZone', 'UTC', true)" in str(statements[1])
    compiled = statements[2].compile(dialect=postgresql.dialect())
    sql = str(compiled)
    assert compiled.params == dict(product_id_1=ARGS["product_id"], grid_id_1="base", algorithm_version_1="vp-v1",
                                   window_start_ms_1=0, window_end_ms_1=DAY, quality_1="VERIFIED", param_1=1001)
    for predicate in ("product_id = %(product_id_1)s", "grid_id = %(grid_id_1)s", "algorithm_version = %(algorithm_version_1)s",
                      "window_start_ms >= %(window_start_ms_1)s", "window_end_ms <= %(window_end_ms_1)s", "quality = %(quality_1)s"):
        assert "volume_profile_snapshot." + predicate in sql
    assert "ORDER BY volume_profile_snapshot.window_start_ms, volume_profile_snapshot.revision, volume_profile_snapshot.id" in sql
    assert "LIMIT %(param_1)s" in sql and "volume_profile_snapshot.revision =" not in sql
    assert "EXISTS (SELECT 1" in sql and "WHERE market_data_invalidation.snapshot_id = volume_profile_snapshot.id" in sql
    assert set(statements[2].selected_columns.keys()) == {
        "id", "revision", "content_sha256", "window_start_ms", "window_end_ms", "product_id", "grid_id", "algorithm_version",
        "availability_basis", "computed_at", "computed_at_valid", "published_at", "published_at_valid",
        "source_available_at", "source_available_at_valid", "revoked"}
    for forbidden in ("source_manifest", "reconciliation", "bin_origin", "bin_step", "base_volume", "quote_volume",
                      "aggregate_count", "occupied_bins", "raw_retention_state", "volume_profile_bin", "ingest_job",
                      " JOIN ", "FOR UPDATE", "pg_advisory"):
        assert forbidden not in sql
    assert session.begin.return_value.__exit__.call_args.args == (None, None, None)


def test_each_timestamp_is_case_guarded_before_driver_materialization() -> None:
    store, session, _ = harness()
    store.list_candidates(**ARGS)
    query = session.execute.call_args.args[0]
    for name in ("computed_at", "published_at", "source_available_at"):
        projection = str(query.selected_columns[name])
        flag = str(query.selected_columns[name + "_valid"])
        for sql in (projection, flag):
            assert f"isfinite(volume_profile_snapshot.{name})" in sql
            assert f"volume_profile_snapshot.{name} >= TIMESTAMPTZ '0001-01-01 00:00:00+00'" in sql
            assert f"volume_profile_snapshot.{name} < TIMESTAMPTZ '10000-01-01 00:00:00+00'" in sql
        assert projection.startswith("CASE WHEN ") and f"THEN volume_profile_snapshot.{name} END" in projection
    assert "source_available_at IS NULL OR" in str(query.selected_columns.source_available_at_valid)


def test_revision_filter_custom_policy_and_empty_incomplete_order_preserved() -> None:
    store, session, _ = harness([], TransactionWaitPolicy(10, 20))
    assert store.list_candidates(**ARGS, revision=2) == ()
    compiled = session.execute.call_args.args[0].compile(dialect=postgresql.dialect())
    assert "volume_profile_snapshot.revision = %(revision_1)s" in str(compiled) and compiled.params["revision_1"] == 2
    assert session.execute.call_args_list[1].args[0].compile().params == {"lock_timeout": "10ms", "statement_timeout": "20ms"}
    store, _, _ = harness([row(revision=1), row(id="c" * 64, revision=2)])
    result = store.list_candidates(**{**ARGS, "end_ms": 7 * DAY})
    assert [item.ref.revision for item in result] == [1, 2]  # Six absent days are not a selection failure.


@pytest.mark.parametrize("field,value", [("product_id", "other"), ("grid_id", "other"), ("algorithm_version", "other"),
    ("window_start_ms", DAY), ("window_end_ms", 2 * DAY), ("id", "A" * 64), ("content_sha256", None),
    ("revision", True), ("revision", 0), ("revision", 1 << 63), ("availability_basis", "bad"), ("revoked", 1),
    ("computed_at", None), ("published_at", None), ("source_available_at", NOW.replace(tzinfo=None)),
    ("computed_at_valid", False), ("published_at_valid", None), ("source_available_at_valid", 1)])
def test_decoy_scope_and_malformed_rows_fail_integrity(field: str, value: Any) -> None:
    store, _, _ = harness([row(**{field: value})])
    with pytest.raises(ProfileIntegrityError, match="^profile integrity check failed$"):
        store.list_candidates(**ARGS)


@pytest.mark.parametrize("name", ["computed_at", "published_at", "source_available_at"])
def test_invalid_sql_timestamp_flags_fail_without_constructing_out_of_range_datetime(name: str) -> None:
    for invalid_database_value in ("infinity", "-infinity", "year 10000", "year 0"):
        store, _, _ = harness([row(**{name: None, name + "_valid": False})])
        with pytest.raises(ProfileIntegrityError):
            store.list_candidates(**ARGS)
        assert invalid_database_value  # SQL CASE maps each invalid timestamp to NULL with false flag.
    for stamp in (datetime(1, 1, 1, tzinfo=timezone.utc), datetime.max.replace(tzinfo=timezone.utc)):
        store, _, _ = harness([row(**{name: stamp})])
        assert getattr(store.list_candidates(**ARGS)[0], name) == stamp


def test_exact_and_one_over_candidate_accounting(monkeypatch: pytest.MonkeyPatch) -> None:
    store, _, _ = harness([row()] * 1000)
    assert len(store.list_candidates(**ARGS)) == 1000
    store, _, _ = harness([row()] * 1001)
    with pytest.raises(reader.ProfileReadTooLarge):
        store.list_candidates(**ARGS)
    monkeypatch.setattr(reader, "MAX_OPERATION_BYTES", 1024)
    store, _, _ = harness([row()])
    assert len(store.list_candidates(**ARGS)) == 1
    store, _, _ = harness([row()] * 2)
    with pytest.raises(reader.ProfileReadTooLarge):
        store.list_candidates(**ARGS)


@pytest.mark.parametrize("field,value", [("product_id", "bad"), ("base_grid_id", "é"), ("algorithm_version", "a" * 33),
    ("start_ms", True), ("start_ms", -DAY), ("end_ms", 0), ("end_ms", DAY + 1), ("end_ms", 91 * DAY),
    ("end_ms", 1 << 63), ("revision", True), ("revision", 0), ("revision", 1 << 63), ("revision", 1.0)])
def test_invalid_inputs_never_open_session(field: str, value: Any) -> None:
    store, _, factory = harness()
    with pytest.raises(ValueError):
        store.list_candidates(**{**ARGS, field: value})
    factory.assert_not_called()


def test_exact_subclasses_multiday_revision_missing_keys_and_revision_decoy() -> None:
    class SubInt(int):
        pass
    class SubStr(str):
        pass
    store, _, factory = harness()
    for changes in (dict(start_ms=SubInt(0)), dict(product_id=SubStr(ARGS["product_id"])), dict(revision=1, end_ms=2 * DAY)):
        with pytest.raises(ValueError):
            store.list_candidates(**{**ARGS, **changes})
    factory.assert_not_called()
    incomplete = row()
    del incomplete["published_at_valid"]
    store, _, _ = harness([incomplete])
    with pytest.raises(ProfileIntegrityError):
        store.list_candidates(**ARGS)
    store, _, _ = harness([row(revision=2)])
    with pytest.raises(ProfileIntegrityError):
        store.list_candidates(**ARGS, revision=1)


def test_session_and_exception_boundaries_without_retry() -> None:
    for dialect, active in (("sqlite", False), ("postgresql", True)):
        store, session, _ = harness()
        session.get_bind.return_value.dialect.name = dialect
        session.in_transaction.return_value = active
        with pytest.raises(ValueError):
            store.list_candidates(**ARGS)
        session.execute.assert_not_called()
    with pytest.raises(ValueError):
        reader.ProfileReadRepository(MagicMock(), MagicMock())
    for commit in (False, True):
        store, session, factory = harness([row()])
        failure = DBAPIError("bounded", None, Exception("injected"))
        if commit:
            session.begin.return_value.__exit__.side_effect = failure
        else:
            session.execute.side_effect = failure
        with pytest.raises(DBAPIError) as caught:
            store.list_candidates(**ARGS)
        assert caught.value is failure and factory.call_count == 1


def test_isolated_import_does_not_load_runtime_owners() -> None:
    guard = "assert not any(n in ('src.main','src.core.engine','src.core.db','src.core.database') or 'rithmic' in n.lower() for n in sys.modules)"
    code = "import sys;import src.core.market_data.profiles.read_repository;" + guard
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)
    # Import is lazy: this probe never calls get_engine or connects to a database.
    rejected = subprocess.run([sys.executable, "-c", "import sys;import src.core.db;" + guard],
                              capture_output=True, timeout=10)
    assert rejected.returncode != 0 and b"AssertionError" in rejected.stderr
