# pyright: reportArgumentType=false, reportCallIssue=false, reportOptionalSubscript=false
"""Additional P3-v2 write fault and budget tests over real store files."""

import json
from pathlib import Path

import pytest

from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_run_artifacts import canonical_bytes
from spider_run_store_v2_fixtures import empty_v2_writer, p3_v2_finalize_fixture
from spider_run_store_v2_fixtures import callback_row, independent_source_group_wire, source_group_row


def test_v2_e4_failed_publication_stays_running_and_blocks_next_append(
    tmp_path, real_p3_artifacts, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = values[2][0]
    boundary = dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"])
    store.mark_processed(boundary, record_kind=row["record_kind"])
    original_budget = module.artifact_limits.p3_v2_scope_budget
    monkeypatch.setattr(module.artifact_limits, "p3_v2_scope_budget", lambda coverage: original_budget(coverage) | {"J2": 0})
    with pytest.raises(module.SpiderRunStoreError) as limit_error:
        store.append_journal(row)
    assert limit_error.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
    before_failed_publish = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    original_write = module._write

    def fail_failed_status(path, raw, append=False):
        if Path(path).name == ".status.json.tmp":
            raise module.SpiderRunStoreError("ARTIFACT_WRITE_FAILED")
        return original_write(path, raw, append)

    monkeypatch.setattr(module, "_write", fail_failed_status)
    with pytest.raises(module.SpiderRunStoreError) as publish_error:
        store.publish_failure("PERSISTENCE_FAILED", limit_error.value.primary_failure)
    assert publish_error.value.reason == "ARTIFACT_WRITE_FAILED"
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before_failed_publish
    assert b'"state":"RUNNING"' in (store._path / "status.json").read_bytes()
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.append_journal(row)
    assert journal.read_bytes() == b""


@pytest.mark.parametrize("axis", ["refused", "J2", "kind_bytes", "row_count", "material", "ratio"])
def test_v2_append_rejects_each_scope_budget_axis_before_file_publish(
    tmp_path, real_p3_artifacts, axis, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = callback_row(store, values, 4_096)
    original_budget = module.artifact_limits.p3_v2_scope_budget

    def bounded_budget(coverage):
        budget = original_budget(coverage)
        if axis == "refused":
            budget["refused"] = True
        elif axis == "J2":
            budget["J2"] = 0
        elif axis == "kind_bytes":
            budget["kind_budgets"] = dict(budget["kind_budgets"], CALLBACK_RESULT=0)
        elif axis == "row_count":
            budget["row_counts"] = dict(budget["row_counts"], CALLBACK_RESULT=0)
        return budget

    monkeypatch.setattr(module.artifact_limits, "p3_v2_scope_budget", bounded_budget)
    if axis == "material":
        monkeypatch.setattr(module.artifact_limits, "P3_V2_MATERIAL_CEILING", 0)
    elif axis == "ratio":
        monkeypatch.setattr(module.artifact_limits, "P3_V2_EXP_MAX", 0)
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind="CALLBACK_RESULT")
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.append_journal(row)
    assert caught.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
    assert journal.read_bytes() == b"" and store._p3_v2_wire_bytes == 0
    assert store._p3_v2_codec_state == () and store._p3_v2_kind_bytes == {}
    assert store._p3_v2_kind_rows == {} and store._p3_v2_projected_bytes == 0


@pytest.mark.parametrize("delta", [0, 1, -1])
def test_v2_sg_budget_exact_and_plus_one_at_append(tmp_path, real_p3_artifacts, delta, monkeypatch):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = source_group_row(store, values)
    wire = independent_source_group_wire(row)
    limit = len(wire) - delta
    original_budget = module.artifact_limits.p3_v2_scope_budget

    def bounded_budget(coverage):
        return original_budget(coverage) | {"B_SG": limit}

    monkeypatch.setattr(module.artifact_limits, "p3_v2_scope_budget", bounded_budget)
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind="SOURCE_GROUP_RESULT")
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    if delta == 1:
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.append_journal(row)
        assert caught.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
        assert journal.read_bytes() == b"" and store._p3_v2_wire_bytes == 0
    else:
        store.append_journal(row)
        assert journal.read_bytes() == wire
        assert store._p3_v2_kind_bytes["SOURCE_GROUP_RESULT"] == len(wire)


@pytest.mark.parametrize("axis,minus_one", [("table_count", False), ("table_bytes", False),
                                             ("table_count", True), ("table_bytes", True)])
def test_v2_order_table_budget_boundaries_use_wire_definitions(
    tmp_path, real_p3_artifacts, axis, minus_one, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = source_group_row(store, values)
    expected_wire = json.loads(independent_source_group_wire(row))
    definitions = expected_wire.get("order_defs", [])
    assert definitions, "fixture must carry at least one real open-order slot definition"
    definition_bytes = sum(len(canonical_bytes(item)) for item in definitions)
    expected_cap = len(definitions) if axis == "table_count" else definition_bytes
    table_limit = expected_cap - int(minus_one)
    original_budget = module.artifact_limits.p3_v2_scope_budget

    def bounded_budget(coverage):
        budget = original_budget(coverage)
        budget["table_max"] = max(len(definitions), table_limit) if axis == "table_count" and not minus_one else table_limit if axis == "table_count" else 100
        budget["table_bytes_max"] = max(definition_bytes, table_limit) if axis == "table_bytes" and not minus_one else table_limit if axis == "table_bytes" else 100_000
        return budget

    monkeypatch.setattr(module.artifact_limits, "p3_v2_scope_budget", bounded_budget)
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind="SOURCE_GROUP_RESULT")
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    if minus_one:
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.append_journal(row)
        assert caught.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
        assert journal.read_bytes() == b"" and store._p3_v2_codec_state == ()
    else:
        store.append_journal(row)
        assert journal.read_bytes() == independent_source_group_wire(row)
        assert store._p3_v2_definition_bytes == definition_bytes


@pytest.mark.parametrize("definition_size", [4_096, 4_097])
def test_v2_definition_size_ceiling_is_inclusive_with_real_file_io(
    tmp_path, real_p3_artifacts, definition_size, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = source_group_row(store, values)
    phases = ("before", "after")
    originals = []
    for phase in phases:
        path = row["payload"][f"owner_evidence_{phase}"]["open_orders_fact"]["immutable_payload"]["rows"]
        if path:
            originals.append(path[0]["order_id"])
    assert originals
    order = next(
        item for phase in phases
        for item in row["payload"][f"owner_evidence_{phase}"]["open_orders_fact"]["immutable_payload"]["rows"]
    )
    base_size = len(canonical_bytes(order))
    order["order_id"] += "x" * (definition_size - base_size)
    padded_id = order["order_id"]
    for phase in phases:
        rows = row["payload"][f"owner_evidence_{phase}"]["open_orders_fact"]["immutable_payload"]["rows"]
        for item in rows:
            if item["order_id"] == originals[0]:
                item["order_id"] = padded_id
    definition = next(
        item for phase in phases
        for item in row["payload"][f"owner_evidence_{phase}"]["open_orders_fact"]["immutable_payload"]["rows"]
    )
    assert len(canonical_bytes(definition)) == definition_size
    original_budget = module.artifact_limits.p3_v2_scope_budget

    def generous_table(coverage):
        budget = original_budget(coverage)
        budget["table_max"] = 100
        budget["table_bytes_max"] = 100_000
        return budget

    monkeypatch.setattr(module.artifact_limits, "p3_v2_scope_budget", generous_table)
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind="SOURCE_GROUP_RESULT")
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    if definition_size == 4_096:
        store.append_journal(row)
        assert journal.read_bytes().endswith(b"\n")
        assert store._p3_v2_definition_bytes == 4_096
    else:
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.append_journal(row)
        assert caught.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
        assert store._p3_v2_definition_bytes == 0
