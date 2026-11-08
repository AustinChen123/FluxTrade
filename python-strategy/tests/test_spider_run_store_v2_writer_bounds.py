# pyright: reportArgumentType=false, reportCallIssue=false, reportOptionalSubscript=false
"""P3-v2 writer boundary tests using real store files."""

from copy import deepcopy
import pytest

from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_projection_size import canonical_jsonl_row_len
from src.core.backtest.spider_run_artifacts import canonical_bytes
from spider_run_store_v2_fixtures import (
    callback_row, empty_v2_writer, independent_source_group_wire, p3_v2_finalize_fixture,
    source_group_row,
)


ROW_MAX = 1_048_576  # v2 D2.4 inclusive 1 MiB wire-row ceiling.


@pytest.mark.parametrize("delta,admitted", [(-1, True), (0, True), (1, False)])
def test_v2_writer_row_max_is_inclusive_with_real_file_io(
    tmp_path, real_p3_artifacts, delta, admitted, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    original_budget = module.artifact_limits.p3_v2_scope_budget

    def row_budget(coverage):
        budget = original_budget(coverage)
        budget["kind_budgets"] = dict(budget["kind_budgets"], CALLBACK_RESULT=2 * ROW_MAX)
        budget["row_counts"] = dict(budget["row_counts"], CALLBACK_RESULT=100)
        return budget

    monkeypatch.setattr(module.artifact_limits, "p3_v2_scope_budget", row_budget)
    row = callback_row(store, values, ROW_MAX + delta)
    boundary = dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"])
    store.mark_processed(boundary, record_kind=row["record_kind"])
    before_append = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}

    if admitted:
        store.append_journal(row)
        raw = journal.read_bytes()
        assert len(raw) == ROW_MAX + delta
        assert raw.endswith(b"\n")
        assert store._p3_v2_wire_bytes == len(raw)
        assert store._p3_v2_kind_bytes["CALLBACK_RESULT"] == len(raw)
    else:
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.append_journal(row)
        assert caught.value.reason == "PERSISTENCE_FAILED"
        assert caught.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before_append
        assert store._p3_v2_wire_bytes == 0 and store._p3_v2_codec_state == ()
        assert store._p3_v2_kind_bytes == {} and store._p3_v2_kind_rows == {}
        assert store._p3_v2_projected_bytes == store._p3_v2_definition_bytes == 0
        with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
            store.append_journal(row)


@pytest.mark.parametrize("axis,one_byte_over", [
    ("J2", False), ("J2", True),
    ("kind_bytes", False), ("kind_bytes", True),
    ("row_count", False), ("row_count", True),
    ("material", False), ("material", True),
])
def test_v2_writer_cumulative_budget_limits_are_inclusive(
    tmp_path, real_p3_artifacts, axis, one_byte_over, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = callback_row(store, values, 4_096)
    expected_wire = deepcopy(row)
    expected_wire["schema_version"] = "spider_journal_record_v2"
    del expected_wire["configuration_context"]
    del expected_wire["historical_context"]
    expected_wire_bytes = len(canonical_bytes(expected_wire)) + 1
    expected_material_bytes = canonical_jsonl_row_len(row)
    original_budget = module.artifact_limits.p3_v2_scope_budget

    def bounded_budget(coverage):
        budget = original_budget(coverage)
        if axis == "J2":
            budget["J2"] = expected_wire_bytes - int(one_byte_over)
        elif axis == "kind_bytes":
            budget["kind_budgets"] = dict(
                budget["kind_budgets"],
                CALLBACK_RESULT=expected_wire_bytes - int(one_byte_over),
            )
        elif axis == "row_count":
            budget["row_counts"] = dict(budget["row_counts"], CALLBACK_RESULT=1 - int(one_byte_over))
        return budget

    monkeypatch.setattr(module.artifact_limits, "p3_v2_scope_budget", bounded_budget)
    if axis == "material":
        monkeypatch.setattr(module.artifact_limits, "P3_V2_MATERIAL_CEILING", expected_material_bytes - int(one_byte_over))
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind="CALLBACK_RESULT")
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    if one_byte_over:
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.append_journal(row)
        assert caught.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
        assert store._p3_v2_wire_bytes == 0
    else:
        store.append_journal(row)
        assert len(journal.read_bytes()) == expected_wire_bytes
        assert store._p3_v2_wire_bytes == expected_wire_bytes
        assert store._p3_v2_projected_bytes == expected_material_bytes


@pytest.mark.parametrize("one_byte_over", [False, True])
def test_v2_source_group_byte_budget_exact_and_plus_one_real_file_io(
    tmp_path, real_p3_artifacts, one_byte_over, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = source_group_row(store, values)
    expected_raw = independent_source_group_wire(row)
    wire_limit = len(expected_raw) - int(one_byte_over)
    original_budget = module.artifact_limits.p3_v2_scope_budget

    def scoped_budget(coverage):
        return original_budget(coverage) | {"B_SG": wire_limit}

    monkeypatch.setattr(module.artifact_limits, "p3_v2_scope_budget", scoped_budget)
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind="SOURCE_GROUP_RESULT")
    before_append = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    if one_byte_over:
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.append_journal(row)
        assert caught.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before_append
        assert store._p3_v2_kind_bytes == {} and store._p3_v2_wire_bytes == 0
        with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
            store.append_journal(row)
    else:
        store.append_journal(row)
        assert journal.read_bytes() == expected_raw
        assert len(journal.read_bytes()) == wire_limit
        assert store._p3_v2_kind_bytes["SOURCE_GROUP_RESULT"] == wire_limit
