"""Recording bounded profile-read SQL contracts; no true-PostgreSQL acceptance claim."""
import subprocess
import sys
import json
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles import read_repository as reader
from src.core.market_data.profiles.repository import ProfileIntegrityError, TransactionWaitPolicy
from src.core.market_data.profiles.read_types import DailyProfileRef, OrderedProfileManifest
from test_profile_repository import verified_rows
from test_profile_publication import publication

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


def manifest_harness(count: int = 2, *, empty: bool = False):
    data: dict[str, Any] = dict(headers=[], summaries=[], payloads=[], bins=[], events=[])
    refs = []
    for day in range(count):
        _, _, _, state, _, _ = snapshot_harness()
        content = replace(publication().content, window_start_ms=day * DAY, window_end_ms=(day + 1) * DAY,
                          bins=() if empty else publication().content.bins)
        identifier = content.content_sha256
        state["header"].update(id=identifier, content_sha256=identifier, window_start_ms=day * DAY,
                               window_end_ms=(day + 1) * DAY, base_volume=content.base_volume, quote_volume=content.quote_volume,
                               aggregate_count=content.aggregate_count, occupied_bins=content.occupied_bins)
        data["headers"].append(state["header"])
        data["payloads"].append(dict(state["payload"], id=identifier))
        data["bins"].extend(dict(b, snapshot_id=identifier) for b in state["bins"] if not empty)
        data["events"].extend(dict(e, snapshot_id=identifier, event_id=f"event{day}") for e in state["events"] if not empty)
        data["summaries"].append(dict(id=identifier, bin_count=0 if empty else len(state["bins"]),
                                      event_count=0 if empty else 1, oversized=False))
        refs.append(DailyProfileRef(identifier, 1, identifier, day * DAY, (day + 1) * DAY))
    data["bins"].sort(key=lambda b: (b["snapshot_id"], b["bin_index"]))
    data["events"].sort(key=lambda e: (e["snapshot_id"], e["recorded_at"], e["event_id"]))
    content = publication().content
    manifest = OrderedProfileManifest(content.product_id, content.grid_id, content.algorithm_version, tuple(refs))
    store, session, factory = harness()
    phases: list[tuple[str, Any]] = []

    def execute(query: Any):
        result = MagicMock()
        if str(query).startswith("SET") or "set_config" in str(query):
            return result
        keys = set(query.selected_columns.keys())
        phase = "headers" if "source_manifest_bytes" in keys else "summaries" if "bin_count" in keys else "payloads" if "source_manifest" in keys else "bins" if "bin_index" in keys else "events"
        phases.append((phase, query))
        if data.get("failure_phase") == phase:
            raise data["failure"]
        result.mappings.return_value.all.return_value = data[phase]
        return result

    session.execute.side_effect = execute
    return store, session, factory, data, phases, manifest


@pytest.mark.parametrize("count,empty", [(1, False), (90, False), (1, True), (90, True)])
def test_manifest_success_one_transaction_and_manifest_order(count: int, empty: bool, monkeypatch: pytest.MonkeyPatch) -> None:
    store, session, factory, data, phases, manifest = manifest_harness(count, empty=empty)
    monkeypatch.setattr(store, "get_verified", MagicMock(side_effect=AssertionError("not per-day")))
    data["headers"].reverse()
    data["payloads"].reverse()
    result = store.get_manifest(manifest)
    assert result is not None and result.manifest == manifest
    assert tuple(day.ref for day in result.days) == manifest.days
    assert all(len(day.invalidations) == (0 if empty else 1) for day in result.days)
    assert [phase for phase, _ in phases] == ["headers", "summaries", "payloads", "bins", "events"]
    assert factory.call_count == session.begin.call_count == 1
    assert str(session.execute.call_args_list[0].args[0]) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
    assert session.execute.call_args_list[1].args[0].compile().params == {"lock_timeout": "250ms", "statement_timeout": "1500ms"}
    assert session.begin.return_value.__exit__.call_args.args == (None, None, None)


def test_manifest_sql_scope_lateral_bounded_probes_and_payload_limits() -> None:
    store, _, _, data, phases, manifest = manifest_harness()
    store.get_manifest(manifest)
    identifiers = [r.snapshot_id for r in manifest.days]
    for phase, query in phases:
        compiled = query.compile(dialect=postgresql.dialect())
        sql = str(compiled)
        assert not any(word in sql for word in ("FOR UPDATE", "INSERT", "UPDATE", "DELETE", "pg_advisory"))
        table = "volume_profile_bin" if phase == "bins" else "market_data_invalidation" if phase == "events" else "volume_profile_snapshot"
        column = "snapshot_id" if phase in ("bins", "events") else "id"
        assert f"WHERE {table}.{column} IN (__[POSTCOMPILE_{column}_1])" in sql
        assert compiled.params[column + "_1"] == identifiers
        assert "LIMIT" in sql
        if phase in ("headers", "payloads"):
            assert query._limit_clause.value == 3
            assert "quality =" not in sql and "product_id =" not in sql
        elif phase in ("bins", "events"):
            assert compiled.params["param_1"] == len(data[phase]) + 1
            suffix = "bin_index" if phase == "bins" else "recorded_at, market_data_invalidation.event_id"
            assert f"ORDER BY {table}.snapshot_id, {table}.{suffix}" in sql
    sql = str(phases[1][1].compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
    assert sql.count("JOIN LATERAL") == 2 and "LIMIT 100001" in sql and "LIMIT 1001" in sql
    assert "volume_profile_bin.snapshot_id = requested.id" in sql
    assert "market_data_invalidation.snapshot_id = requested.id" in sql
    normalized = " ".join(sql.split())
    # Exact inner FROM excludes a locally materialized requested relation: correlation is required.
    assert "FROM volume_profile_bin WHERE volume_profile_bin.snapshot_id = requested.id LIMIT 100001" in normalized
    assert "FROM market_data_invalidation WHERE market_data_invalidation.snapshot_id = requested.id LIMIT 1001" in normalized
    assert "bool_or(anon_1.base_volume OR anon_1.quote_volume)" in sql
    for name in ("base_volume", "quote_volume"):
        assert f"octet_length(CAST(volume_profile_bin.{name} AS TEXT)) > 64 AS {name}" in sql
    assert "count(*) AS bin_count" in sql and "count(*) AS event_count" in sql
    # Both APIs must use the exact same guarded header projection (CASE assertions above).
    assert [str(c) for c in phases[0][1].selected_columns] == [str(c) for c in reader._header_projection()]


@pytest.mark.parametrize("damage", ["missing", "partial", "duplicate", "foreign", "revision", "scope", "digest"])
def test_manifest_header_priority_and_no_payload(damage: str) -> None:
    store, _, _, data, phases, manifest = manifest_harness()
    if damage in ("missing", "partial"):
        data["headers"][0]["computed_at_valid"] = False
        if damage == "missing":
            data["headers"].pop()
        else:
            data["headers"][1]["quality"] = "PARTIAL"
        assert store.get_manifest(manifest) is None
    else:
        if damage == "duplicate":
            data["headers"].append(data["headers"][0])
        elif damage == "foreign":
            data["headers"][0]["id"] = "a" * 64
        else:
            key = {"revision": "revision", "scope": "grid_id", "digest": "content_sha256"}[damage]
            data["headers"][0][key] = 2 if damage == "revision" else "b" * 64
        with pytest.raises(ProfileIntegrityError):
            store.get_manifest(manifest)
    assert [p for p, _ in phases] == ["headers"]


@pytest.mark.parametrize("damage", ["type", "days", "empty", "ref_type", "ref_value", "order", "scope"])
def test_manifest_revalidates_before_session(damage: str) -> None:
    store, _, factory, _, _, manifest = manifest_harness()
    if damage == "type":
        manifest = None
    elif damage == "ref_value":
        object.__setattr__(manifest.days[0], "revision", True)
    elif damage == "scope":
        object.__setattr__(manifest, "base_grid_id", "bad!")
    else:
        value = {"days": list(manifest.days), "empty": (), "ref_type": (None,), "order": tuple(reversed(manifest.days))}[damage]
        object.__setattr__(manifest, "days", value)
    with pytest.raises(ValueError):
        store.get_manifest(manifest)  # type: ignore[arg-type]
    factory.assert_not_called()


@pytest.mark.parametrize("resource", ["bins", "events", "all_events", "bytes", "numeric", "metadata", "bin_numeric"])
def test_manifest_preflight_resource_limits_without_payload(resource: str) -> None:
    store, _, _, data, phases, manifest = manifest_harness(11 if resource == "all_events" else 2)
    if resource in ("bins", "events"):
        name, value = {"bins": ("bin_count", 100001), "events": ("event_count", 1001)}[resource]
        data["summaries"][0][name] = value
    elif resource == "all_events":
        for row in data["summaries"]:
            row["event_count"] = 1000
    elif resource == "bytes":
        for row in data["summaries"]:
            row["bin_count"] = 70000  # Each day fits; combined accounting exceeds 32 MiB.
    elif resource == "bin_numeric":
        data["summaries"][0]["oversized"] = True
    else:
        key = "bin_origin_oversized" if resource == "numeric" else "source_manifest_oversized"
        data["headers"][0][key] = True
    with pytest.raises(reader.ProfileReadTooLarge):
        store.get_manifest(manifest)
    assert not {"payloads", "bins", "events"} & {p for p, _ in phases}


def test_manifest_exact_resource_boundaries_then_payload_consistency(monkeypatch: pytest.MonkeyPatch) -> None:
    for field, maximum, count in (("bin_count", 100000, 1), ("event_count", 1000, 1), ("event_count", 1000, 10)):
        store, _, _, data, phases, manifest = manifest_harness(count)
        for summary in data["summaries"]:
            summary[field] = maximum
        with pytest.raises(ProfileIntegrityError):  # Exact bound passes; short fake payload fails.
            store.get_manifest(manifest)
        assert [p for p, _ in phases][-3:] == ["payloads", "bins", "events"]
    store, _, _, data, _, manifest = manifest_harness()
    exact = len(manifest.days) * (1024 + 4) + len(data["bins"]) * 256 + len(data["events"]) * 512
    monkeypatch.setattr(reader, "MAX_OPERATION_BYTES", exact)
    assert store.get_manifest(manifest) is not None
    monkeypatch.setattr(reader, "MAX_OPERATION_BYTES", exact - 1)
    with pytest.raises(reader.ProfileReadTooLarge):
        store.get_manifest(manifest)


@pytest.mark.parametrize("phase", ["payloads", "bins", "events"])
def test_manifest_payload_one_over_is_too_large(phase: str) -> None:
    store, _, _, data, _, manifest = manifest_harness()
    data[phase].append(data[phase][0])
    with pytest.raises(reader.ProfileReadTooLarge):
        store.get_manifest(manifest)


@pytest.mark.parametrize("phase,damage", [(p, d) for p in ("summaries", "payloads", "bins", "events")
                                         for d in ("missing", "foreign", "duplicate")] + [("bins", "reverse"), ("events", "reverse"),
                                         ("payloads", "json"), ("payloads", "size"), ("bins", "numeric"), ("events", "corrupt")])
def test_manifest_payload_scope_order_count_integrity(phase: str, damage: str) -> None:
    store, _, _, data, _, manifest = manifest_harness()
    rows = data[phase]
    if damage == "missing":
        rows.pop()
    elif damage == "foreign":
        rows[0]["id" if phase in ("summaries", "payloads") else "snapshot_id"] = "a" * 64
    elif damage == "duplicate":
        rows[1] = rows[0]
    elif damage == "reverse":
        rows.reverse()
    elif damage in ("json", "size"):
        rows[0]["source_manifest"] = "xx" if damage == "json" else "{} "
    elif damage == "numeric":
        rows[0]["base_volume"] = Decimal("NaN")
    else:
        rows[0]["reason_code"] = "bad!"
    with pytest.raises(ProfileIntegrityError):
        store.get_manifest(manifest)


@pytest.mark.parametrize("phase", ["headers", "summaries", "payloads", "bins", "events", "commit"])
def test_manifest_database_errors_propagate_once(phase: str) -> None:
    store, session, factory, data, phases, manifest = manifest_harness()
    error = DBAPIError("injected", None, Exception("injected"))
    if phase == "commit":
        session.begin.return_value.__exit__.side_effect = error
    else:
        data.update(failure_phase=phase, failure=error)
    with pytest.raises(DBAPIError) as caught:
        store.get_manifest(manifest)
    assert caught.value is error and factory.call_count == 1
    if phase == "commit":
        assert len(phases) == 5
    else:
        assert phases[-1][0] == phase


def test_manifest_metadata_sql_cap_does_not_relax_canonical_cap() -> None:
    store, _, _, data, phases, manifest = manifest_harness()
    raw = json.dumps({"x": "a" * (131072 - 9)})
    assert len(raw.encode()) == 131072
    data["headers"][0]["source_manifest_bytes"] = 131072
    data["payloads"][0]["source_manifest"] = raw
    with pytest.raises(ProfileIntegrityError):
        store.get_manifest(manifest)
    assert [p for p, _ in phases][-3:] == ["payloads", "bins", "events"]
    phases.clear()
    data["headers"][0]["source_manifest_bytes"] = -1
    with pytest.raises(ProfileIntegrityError):
        store.get_manifest(manifest)
    assert [p for p, _ in phases] == ["headers"]


@pytest.mark.parametrize("field,value", [("bin_count", True), ("bin_count", -1), ("event_count", -1), ("event_count", None), ("oversized", 0)])
def test_manifest_corrupt_summary_flags_fail_before_payload(field: str, value: object) -> None:
    store, _, _, data, phases, manifest = manifest_harness()
    data["summaries"][0][field] = value
    with pytest.raises(ProfileIntegrityError):
        store.get_manifest(manifest)
    assert [p for p, _ in phases] == ["headers", "summaries"]


def test_manifest_event_identity_and_per_day_count_cannot_shift() -> None:
    store, _, _, data, _, manifest = manifest_harness()
    # IDs remain globally sorted by snapshot; duplicated event ID must still fail.
    data["events"][1]["event_id"] = data["events"][0]["event_id"]
    with pytest.raises(ProfileIntegrityError):
        store.get_manifest(manifest)
    store, _, _, data, _, manifest = manifest_harness()
    # Global totals stay equal, but one day's claimed count cannot cover another day.
    data["summaries"][0]["bin_count"] += 1
    data["summaries"][1]["bin_count"] -= 1
    with pytest.raises(ProfileIntegrityError):
        store.get_manifest(manifest)


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("oversize", ["header_bytes", "header_numeric", "summary_count"])
@pytest.mark.parametrize("corrupt", ["identity", "timestamp", "header_flag", "header_size", "summary_count", "summary_flag"])
def test_manifest_integrity_precedes_any_resource_error(reverse: bool, oversize: str, corrupt: str) -> None:
    store, _, _, data, phases, manifest = manifest_harness()
    if oversize == "header_bytes":
        data["headers"][0].update(source_manifest_bytes=131073, source_manifest_oversized=True)
    elif oversize == "header_numeric":
        data["headers"][0]["bin_origin_oversized"] = True
    else:
        data["summaries"][0]["bin_count"] = 100001
    if corrupt == "identity":
        data["headers"][1]["revision"] = 2
    elif corrupt == "timestamp":
        data["headers"][1]["computed_at"] = NOW.replace(tzinfo=None)
    elif corrupt == "header_flag":
        data["headers"][1]["quote_volume_oversized"] = 0
    elif corrupt == "header_size":
        data["headers"][1]["reconciliation_bytes"] = -1
    elif corrupt == "summary_count":
        data["summaries"][1]["event_count"] = -1
    else:
        data["summaries"][1]["oversized"] = None
    if reverse:
        data["headers"].reverse()
        data["summaries"].reverse()
    with pytest.raises(ProfileIntegrityError):
        store.get_manifest(manifest)
    assert not {"payloads", "bins", "events"} & {p for p, _ in phases}
