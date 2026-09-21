"""Recording bounded profile-read SQL contracts; no true-PostgreSQL acceptance claim."""
import subprocess
import sys
import json
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles import read_repository as reader
from src.core.market_data.profiles.repository import ProfileIntegrityError, TransactionWaitPolicy
from test_profile_repository import verified_rows

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


def snapshot_harness():
    raw, raw_bins = verified_rows()
    header = dict(raw)
    payload = dict(id=header["id"], source_manifest="{}", reconciliation="{}")
    for name in ("computed_at", "published_at", "source_available_at"):
        header[name + "_valid"] = True
    for name in ("bin_origin", "bin_step", "base_volume", "quote_volume"):
        header[name + "_oversized"] = False
    for name in ("source_manifest", "reconciliation"):
        header[name + "_oversized"] = False
        header[name + "_bytes"] = 2
        del header[name]
    bins = [dict(b, snapshot_id=header["id"]) for b in raw_bins]
    event = dict(event_id="event", snapshot_id=header["id"], reason_code="BAD", source="audit",
                 replacement_snapshot_id=None, recorded_at=NOW.replace(microsecond=123000), recorded_at_valid=True)
    state: dict[str, Any] = dict(header=header, payload=payload, bins=bins, events=[event],
                                bin_counts=dict(count=len(bins), oversized=False), event_count=1)
    store, session, factory = harness()
    phases: list[tuple[str, Any]] = []

    def execute(query: Any):
        result = MagicMock()
        sql = str(query)
        if sql.startswith("SET") or "set_config" in sql:
            return result
        keys = set(query.selected_columns.keys())
        phase = "header" if "source_manifest_bytes" in keys else "payload" if "source_manifest" in keys else "bin_counts" if "count" in keys else "bins" if "bin_index" in keys else "events"
        phases.append((phase, query))
        result.mappings.return_value.one_or_none.return_value = state[phase]
        result.mappings.return_value.one.return_value = state[phase]
        result.mappings.return_value.all.return_value = state[phase]
        return result

    def scalar(query: Any):
        phases.append(("event_count", query))
        return state["event_count"]

    session.execute.side_effect, session.scalar.side_effect = execute, scalar
    return store, session, factory, state, phases, header["id"]


def test_single_read_all_phases_exact_scope_order_limits_and_sole_verifier(monkeypatch: pytest.MonkeyPatch) -> None:
    store, session, _, state, phases, identifier = snapshot_harness()
    verify = MagicMock(wraps=reader._verify_rows)
    monkeypatch.setattr(reader, "_verify_rows", verify)
    result = store.get_verified(identifier)
    assert result is not None and result.invalidations[0].snapshot_id == identifier
    assert result.ref.snapshot_id == result.publication.content_sha256
    assert [name for name, _ in phases] == ["header", "bin_counts", "event_count", "payload", "bins", "events"]
    assert session.begin.call_count == 1 and verify.call_count == 1
    for phase, query in phases:
        compiled = query.compile(dialect=postgresql.dialect())
        sql, params = str(compiled), compiled.params
        table = "volume_profile_snapshot" if phase in ("header", "payload") else "volume_profile_bin" if phase in ("bin_counts", "bins") else "market_data_invalidation"
        key = "id" if table == "volume_profile_snapshot" else "snapshot_id"
        assert f"WHERE {table}.{key} = %({key}_1)s" in sql and params[key + "_1"] == identifier
        assert not any(word in sql for word in ("FOR UPDATE", "pg_advisory", "INSERT", "DELETE", "UPDATE"))
        if phase in ("bin_counts", "bins", "event_count", "events"):
            assert "LIMIT" in sql
            assert (100001 if phase in ("bin_counts", "bins") else 1001 if phase == "event_count" else 10001) in params.values()
        if phase == "bins":
            assert "ORDER BY volume_profile_bin.snapshot_id, volume_profile_bin.bin_index" in sql
        if phase == "events":
            assert "ORDER BY market_data_invalidation.snapshot_id, market_data_invalidation.recorded_at, market_data_invalidation.event_id" in sql
    header_query = phases[0][1]
    assert "source_manifest" not in header_query.selected_columns.keys()
    for name in ("source_manifest", "reconciliation"):
        assert "octet_length(CAST(" in str(header_query.selected_columns[name + "_bytes"])
        assert str(header_query.selected_columns[name + "_bytes"]).startswith("CASE WHEN")
    for name in ("bin_origin", "bin_step", "base_volume", "quote_volume"):
        assert str(header_query.selected_columns[name]).startswith("CASE WHEN")
        assert "octet_length(CAST(" in str(header_query.selected_columns[name + "_oversized"])
        assert 64 in header_query.selected_columns[name].compile().params.values()
        assert 64 in header_query.selected_columns[name + "_oversized"].compile().params.values()
    assert 64 in phases[1][1].compile().params.values()

    def literal_sql(expression: Any) -> str:
        return str(expression.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))

    for name in ("source_manifest", "reconciliation"):
        length = f"octet_length(CAST(volume_profile_snapshot.{name} AS TEXT))"
        assert literal_sql(header_query.selected_columns[name + "_bytes"]) == (
            f"CASE WHEN ({length} <= 131072) THEN {length} ELSE 131073 END")
    for name in ("bin_origin", "bin_step", "base_volume", "quote_volume"):
        column = f"volume_profile_snapshot.{name}"
        length = f"octet_length(CAST({column} AS TEXT))"
        # SQLAlchemy omits ELSE None; SQL CASE's implicit ELSE is NULL.
        assert literal_sql(header_query.selected_columns[name]) == f"CASE WHEN ({length} <= 64) THEN {column} END"
        assert literal_sql(header_query.selected_columns[name + "_oversized"]) == f"{length} > 64"
    bin_sql = literal_sql(phases[1][1])
    for name in ("base_volume", "quote_volume"):
        assert f"octet_length(CAST(volume_profile_bin.{name} AS TEXT)) > 64 AS {name}_oversized" in bin_sql
    assert "coalesce(bool_or(anon_1.base_volume_oversized OR anon_1.quote_volume_oversized), false) AS oversized" in bin_sql
    assert "CAST(volume_profile_snapshot.source_manifest AS TEXT)" in str(phases[3][1])
    assert state["header"]["occupied_bins"] == len(state["bins"])


@pytest.mark.parametrize("quality", [None, "PARTIAL", "CONFLICT"])
def test_missing_or_nonverified_stops_before_payload(quality: str | None) -> None:
    store, _, _, state, phases, identifier = snapshot_harness()
    if quality is None:
        state["header"] = None
    else:
        state["header"]["quality"] = quality
    assert store.get_verified(identifier) is None
    assert [p for p, _ in phases] == ["header"]


@pytest.mark.parametrize("resource", ["metadata", "numeric", "bin_numeric", "bins", "events", "operation"])
def test_preflight_one_over_never_fetches_payload(resource: str, monkeypatch: pytest.MonkeyPatch) -> None:
    store, _, _, state, phases, identifier = snapshot_harness()
    if resource == "metadata":
        state["header"].update(source_manifest_bytes=131073, source_manifest_oversized=True)
    elif resource == "numeric":
        state["header"]["bin_origin_oversized"] = True
    elif resource == "bin_numeric":
        state["bin_counts"]["oversized"] = True
    elif resource == "bins":
        state["bin_counts"]["count"] = 100001
    elif resource == "events":
        state["event_count"] = 1001
    else:
        cost = 1024 + 4 + len(state["bins"]) * 256 + 512
        monkeypatch.setattr(reader, "MAX_OPERATION_BYTES", cost - 1)
    with pytest.raises(reader.ProfileReadTooLarge):
        store.get_verified(identifier)
    assert not {"payload", "bins", "events"} & {p for p, _ in phases}


def test_exact_preflight_limits_and_canonical_metadata_bound(monkeypatch: pytest.MonkeyPatch) -> None:
    for field, maximum in (("bins", 100000), ("events", 1000)):
        store, _, _, state, phases, identifier = snapshot_harness()
        if field == "bins":
            state["bin_counts"]["count"] = maximum
        else:
            state["event_count"] = maximum
        with pytest.raises(ProfileIntegrityError):  # Budget passes; deliberately short payload does not.
            store.get_verified(identifier)
        assert [p for p, _ in phases][-3:] == ["payload", "bins", "events"]
    store, _, _, state, _, identifier = snapshot_harness()
    cost = 1024 + 4 + len(state["bins"]) * 256 + 512
    monkeypatch.setattr(reader, "MAX_OPERATION_BYTES", cost)
    assert store.get_verified(identifier) is not None
    monkeypatch.setattr(reader, "MAX_OPERATION_BYTES", 33554432)
    raw = json.dumps({"x": "a" * (131072 - 9)})
    assert len(raw.encode()) == 131072
    state["header"]["source_manifest_bytes"] = 131072
    state["payload"]["source_manifest"] = raw
    with pytest.raises(ProfileIntegrityError):
        store.get_verified(identifier)


@pytest.mark.parametrize("damage", ["json", "json_list", "header", "numeric", "digest", "totals", "timestamp", "size_flag",
    "bins_scope", "events_scope", "payload_scope", "bin_count", "event_count", "bin_order", "event", "event_timestamp"])
def test_payload_and_header_corruption_is_integrity_not_missing(damage: str) -> None:
    store, _, _, state, _, identifier = snapshot_harness()
    if damage in ("json", "json_list"):
        raw = "xx" if damage == "json" else "[]"
        state["payload"]["source_manifest"] = raw
    elif damage in ("header", "numeric", "digest", "totals", "timestamp", "size_flag"):
        key, value = {"header": ("id", "a" * 64), "numeric": ("bin_origin", Decimal("NaN")),
                      "digest": ("content_sha256", "a" * 64), "totals": ("base_volume", Decimal(99)),
                      "timestamp": ("computed_at_valid", False), "size_flag": ("bin_origin_oversized", None)}[damage]
        state["header"][key] = value
    elif damage.endswith("_scope"):
        owner = damage.split("_")[0]
        if owner == "payload":
            state[owner]["id"] = "d" * 64
        else:
            state[owner][0]["snapshot_id"] = "d" * 64
    elif damage == "bin_count":
        state["bin_counts"]["count"] += 1
    elif damage == "event_count":
        state["event_count"] = 0
    elif damage == "bin_order":
        state["bins"].reverse()
    elif damage == "event":
        state["events"][0]["source"] = "bad!"
    else:
        state["events"][0]["recorded_at_valid"] = False
    with pytest.raises(ProfileIntegrityError, match="^profile integrity check failed$"):
        store.get_verified(identifier)


def test_single_read_validation_db_failure_and_commit_propagation() -> None:
    store, _, factory, _, _, _ = snapshot_harness()
    for identifier in ("A" * 64, "a" * 63, None):
        with pytest.raises(ValueError):
            store.get_verified(identifier)  # type: ignore[arg-type]
    factory.assert_not_called()
    for commit in (False, True):
        store, session, factory, _, _, identifier = snapshot_harness()
        failure = DBAPIError("bounded", None, Exception("injected"))
        if commit:
            session.begin.return_value.__exit__.side_effect = failure
        else:
            session.execute.side_effect = failure
        with pytest.raises(DBAPIError) as caught:
            store.get_verified(identifier)
        assert caught.value is failure and factory.call_count == 1


def test_numeric_boundary_and_independent_operation_event_budget(monkeypatch: pytest.MonkeyPatch) -> None:
    store, _, _, state, phases, identifier = snapshot_harness()
    # PostgreSQL may preserve scale; 64-byte text is allowed without changing exact value.
    for name in ("bin_origin", "bin_step", "base_volume", "quote_volume"):
        text = format(state["header"][name], "f")
        text += "." if "." not in text else ""
        state["header"][name] = Decimal(text + "0" * (64 - len(text)))
    assert store.get_verified(identifier) is not None
    monkeypatch.setattr(reader, "MAX_INVALIDATIONS_PER_OPERATION", 1)
    assert store.get_verified(identifier) is not None
    phases.clear()
    state["event_count"] = 2
    with pytest.raises(reader.ProfileReadTooLarge):
        store.get_verified(identifier)
    assert [name for name, _ in phases] == ["header", "bin_counts", "event_count"]
