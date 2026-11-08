# pyright: reportArgumentType=false, reportCallIssue=false, reportOptionalSubscript=false
"""P3-v2 writer boundary tests using real store files."""

from copy import deepcopy
import pytest

from src.core.backtest import spider_run_store as module
from src.core.backtest import spider_projection_size as projection_size
from src.core.backtest.spider_projection_size import canonical_jsonl_row_len
from src.core.backtest.spider_run_artifacts import canonical_bytes
from spider_run_store_v2_fixtures import (
    assert_j6_reader_accepts, callback_row, empty_v2_writer,
    independent_source_group_wire, p3_v2_finalize_fixture, source_group_row,
)


ROW_MAX = 1_048_576  # v2 D2.4 inclusive 1 MiB wire-row ceiling.


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_v2_wire_row_max_inclusive_with_real_file_io(
    tmp_path, real_p3_artifacts, delta, monkeypatch,
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
    # Isolate the wire ROW_MAX check; expanded-row acceptance is exercised below
    # with the unmodified shared J6 helper.
    monkeypatch.setattr(module, "projected_jsonl_budget_ok", lambda **kwargs: True)
    row = callback_row(store, values, ROW_MAX + delta)
    expected_v2 = deepcopy(row)
    expected_v2["schema_version"] = "spider_journal_record_v2"
    del expected_v2["configuration_context"]
    del expected_v2["historical_context"]
    assert len(canonical_bytes(row)) + 1 > ROW_MAX
    assert len(canonical_bytes(expected_v2)) + 1 == ROW_MAX + delta
    boundary = dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"])
    store.mark_processed(boundary, record_kind=row["record_kind"])
    before_append = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}

    if delta <= 0:
        store.append_journal(row)
        assert len(journal.read_bytes()) == ROW_MAX + delta
        assert store._p3_v2_wire_bytes == ROW_MAX + delta
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


@pytest.mark.parametrize("delta", [-1, 0, 1])
def test_v2_projected_row_max_is_inclusive_and_j6_reader_closed(
    tmp_path, real_p3_artifacts, delta, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    original_coverage = store._attempt["planned_coverage"]
    store._attempt["planned_coverage"] = [
        dict(item, ordinal=scope * len(original_coverage) + index + 1,
             barrier_id=f"{item['barrier_id']}_{scope}")
        for scope in range(20)
        for index, item in enumerate(original_coverage)
    ]
    normalized = deepcopy(store._attempt)
    normalized["artifact_encoding"] = "artifact_encoding_v1"
    module.validate_artifact(normalized)

    seed = callback_row(store, values, 4_096)
    seed_v2 = deepcopy(seed)
    seed_v2["schema_version"] = "spider_journal_record_v2"
    del seed_v2["configuration_context"]
    del seed_v2["historical_context"]
    expanded_overhead = (len(canonical_bytes(seed)) + 1) - (len(canonical_bytes(seed_v2)) + 1)
    target_projected = ROW_MAX + delta
    row = callback_row(store, values, target_projected - expanded_overhead)
    assert len(canonical_bytes(row)) + 1 == target_projected
    wire_row = deepcopy(row)
    wire_row["schema_version"] = "spider_journal_record_v2"
    del wire_row["configuration_context"]
    del wire_row["historical_context"]
    expected_wire = len(canonical_bytes(wire_row)) + 1
    assert expected_wire < ROW_MAX
    assert len(canonical_bytes(row)) + 1 == target_projected

    store.mark_processed(
        dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]),
        record_kind="CALLBACK_RESULT",
    )
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    if delta == 1:
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.append_journal(row)
        assert caught.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
        assert store._p3_v2_wire_bytes == 0 and store._p3_v2_projected_bytes == 0
        assert store._p3_v2_kind_rows == {} and store._p3_v2_codec_state == ()
        with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
            store.append_journal(row)
    else:
        store.append_journal(row)
        assert len(journal.read_bytes()) == expected_wire
        assert store._p3_v2_projected_bytes == target_projected
        assert_j6_reader_accepts(store, [row])


def test_v2_writer_acceptance_roundtrips_each_real_fixture_row_through_j6_reader(
    tmp_path, real_p3_artifacts,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    empty_v2_writer(store)
    for index, row in enumerate(values[2], start=1):
        store.mark_processed(
            dict(ordinal=index, journal_seq=index, barrier_id=row["barrier_id"]),
            record_kind=row["record_kind"],
        )
        store.append_journal(row)
        assert_j6_reader_accepts(store, values[2][:index])


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
        monkeypatch.setattr(projection_size, "P3_V2_MATERIAL_CEILING", expected_material_bytes - int(one_byte_over))
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
        assert_j6_reader_accepts(store, [row])


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
        assert_j6_reader_accepts(store, [row])
