# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""D5.2 end-to-end slot value matrix through codec and committed reader."""

import copy

import pytest

from spider_journal_codec_test_support import (
    CONFIGURATION,
    HISTORICAL,
    _market,
    _open,
    _row,
    _source_group,
    _working,
    canonical_bytes,
    codec,
    projected_v1_jsonl_row_len,
)
from spider_journal_codec_io_support import (
    ROUND_TRIP_SLOT_CASES,
    _wire_read,
    _wire_read_result,
    _without_context_members,
)
from src.core.backtest import spider_journal_slots_v2 as slot_table
from src.core.backtest import spider_run_native_schema as native_schema


@pytest.mark.parametrize("kind,path,template", ROUND_TRIP_SLOT_CASES)
@pytest.mark.parametrize("shape", ["missing", "null", "empty", "nonempty"])
def test_d52_slot_shape_round_trips_through_codec_reader_and_projection(
    tmp_path, kind, path, template, shape
):
    source = copy.deepcopy(template)
    source["record_kind"] = kind
    parent = source
    for name in path[:-1]:
        parent = parent[name]
    if shape == "missing":
        parent.pop(path[-1])
        expected_value = "<missing>"
    elif shape == "null":
        parent[path[-1]] = None
        expected_value = None
    elif shape == "empty":
        parent[path[-1]] = []
        expected_value = []
    else:
        order = _working() if kind == "HISTORICAL_MARKET_RESULT" else _open()
        parent[path[-1]] = [order]
        expected_value = [order]

    expected = copy.deepcopy(source)
    encoded, _ = codec.encode_p3_v2_row(source)
    read_result = _wire_read_result([encoded], tmp_path)
    projected = read_result.rows[0]

    actual = projected
    expected_slot = expected
    for name in path[:-1]:
        actual = actual[name]
        expected_slot = expected_slot[name]
    if shape == "missing":
        assert path[-1] not in actual
        assert path[-1] not in expected_slot
    else:
        assert actual[path[-1]] == expected_value
        assert expected_slot[path[-1]] == expected_value
    assert canonical_bytes(projected) + b"\n" == canonical_bytes(expected) + b"\n"
    wire_row = _without_context_members([encoded])[0]
    assert read_result.counters["projected_bytes"] == len(canonical_bytes(expected)) + 1
    assert (
        projected_v1_jsonl_row_len(wire_row, (), CONFIGURATION, HISTORICAL)
        == len(canonical_bytes(expected)) + 1
    )


def test_dense_index_uses_prior_state_offset_for_first_new_definition():
    first = _working("prior")
    second = _open("next")
    _, prior = codec.encode_p3_v2_row(_market([first], []))

    encoded, next_state = codec.encode_p3_v2_row(_source_group([], [second]), prior)

    assert encoded["payload"]["owner_evidence_after"]["open_orders_fact"][
        "immutable_payload"
    ]["rows"] == [1]
    assert encoded["order_defs"] == [second]
    assert next_state == (canonical_bytes(first), canonical_bytes(second))


def test_reader_rejects_definitions_without_a_selected_slot(tmp_path):
    row = _row("CALLBACK_RESULT", {"message": "ok"})
    encoded, _ = codec.encode_p3_v2_row(row)
    encoded["order_defs"] = [{"order_id": "unused"}]
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        _wire_read([encoded], tmp_path)


_SNAPSHOT_KINDS = ("MARKET", "EARN", "TRADING", "POSITIONS", "OPEN_ORDERS")
_SNAPSHOT_OUTCOMES = ("SUCCESS", "FAILURE")
_EXPECTED_NO_SLOT_CLASSES = {
    ("CALLBACK_RESULT", None, None),
    *(
        ("SNAPSHOT_FACT", snapshot_kind, outcome)
        for snapshot_kind in _SNAPSHOT_KINDS
        for outcome in _SNAPSHOT_OUTCOMES
        if (snapshot_kind, outcome) != ("OPEN_ORDERS", "SUCCESS")
    ),
}


def _no_slot_rows_from_shared_table():
    assert {kind for kind, _, _ in slot_table.P3_V2_SLOT_TABLE} == {
        "CALLBACK_RESULT",
        "HISTORICAL_MARKET_RESULT",
        "SNAPSHOT_FACT",
        "SOURCE_GROUP_RESULT",
    }
    assert tuple(native_schema._KINDS) == _SNAPSHOT_KINDS
    no_slot_rows = []
    for kind, ordered_slots, selector in slot_table.P3_V2_SLOT_TABLE:
        if kind == "CALLBACK_RESULT":
            if ordered_slots or selector is not None:
                raise AssertionError("CALLBACK_RESULT must remain a no-slot kind")
            no_slot_rows.append((kind, None, None, {"callback": "complete"}))
            continue
        if kind in {"HISTORICAL_MARKET_RESULT", "SOURCE_GROUP_RESULT"}:
            if not ordered_slots or selector is not None:
                raise AssertionError(f"{kind} must retain its unconditional slots")
            continue
        if kind != "SNAPSHOT_FACT":
            raise AssertionError(f"unhandled closed P3 v2 kind: {kind}")
        if len(ordered_slots) != 1 or selector is None:
            raise AssertionError("SNAPSHOT_FACT must have exactly its selected slot")
        for snapshot_kind in _SNAPSHOT_KINDS:
            for outcome in _SNAPSHOT_OUTCOMES:
                if (snapshot_kind, outcome) != selector:
                    no_slot_rows.append(
                        (
                            kind,
                            snapshot_kind,
                            outcome,
                            {
                                "fact": {
                                    "snapshot_kind": snapshot_kind,
                                    "immutable_payload": {"outcome": outcome},
                                }
                            },
                        )
                    )
    actual = {
        (kind, snapshot_kind, outcome)
        for kind, snapshot_kind, outcome, _ in no_slot_rows
    }
    assert actual == _EXPECTED_NO_SLOT_CLASSES
    assert len(no_slot_rows) == len(_EXPECTED_NO_SLOT_CLASSES)
    return tuple(no_slot_rows)


@pytest.mark.parametrize(
    "kind,snapshot_kind,outcome,payload", _no_slot_rows_from_shared_table()
)
@pytest.mark.parametrize(
    "definitions",
    [
        pytest.param([], id="empty-definitions"),
        pytest.param([{"order_id": "unused"}], id="nonempty-definitions"),
        # D5.4 definition size boundary is 4,096 canonical bytes; this item
        # exercises failure precedence before the per-definition size check.
        pytest.param([{"pad": "x" * 4_097}], id="oversized-definition"),
    ],
)
def test_reader_rejects_present_order_defs_for_every_no_slot_class(
    tmp_path, kind, snapshot_kind, outcome, payload, definitions
):
    # Snapshot kind/outcome domain is from spider_run_native_schema.py:_KINDS
    # and snapshot_payload's SUCCESS/FAILURE branches; selector authority is D5.2.
    source = _row(kind, payload)
    encoded, _ = codec.encode_p3_v2_row(source)
    encoded["order_defs"] = definitions
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        _wire_read([encoded], tmp_path)


def test_codec_projection_and_reader_share_one_slot_classifier():
    from src.core.backtest import spider_journal_reader_v2 as reader
    from src.core.backtest import spider_journal_slot_classifier_v2 as classifier
    from src.core.backtest import spider_projection_size as projection

    assert codec.classify_p3_v2_slots is classifier.classify_p3_v2_slots
    assert reader.classify_p3_v2_slots is classifier.classify_p3_v2_slots
    assert projection.classify_p3_v2_slots is classifier.classify_p3_v2_slots


def test_projection_rejects_nonlist_nonnull_slot_as_artifact_mismatch():
    from src.core.backtest.spider_projection_size import projected_v1_jsonl_row_len

    row = _market([], [])
    row["schema_version"] = "spider_journal_record_v2"
    row.pop("configuration_context")
    row.pop("historical_context")
    row["payload"]["request"]["working_orders"] = 0
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        projected_v1_jsonl_row_len(row, (), CONFIGURATION, HISTORICAL)
