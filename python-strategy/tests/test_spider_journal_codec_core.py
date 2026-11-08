# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""J4 codec contract tests."""

from spider_journal_codec_test_support import (
    _market,
    _open,
    _snapshot,
    _source_group,
    _working,
    canonical_bytes,
    codec,
    copy,
    limits,
    slots,
)


def test_callback_codec_smoke_returns_v2_row_and_empty_state():
    row = {
        "schema_version": "spider_journal_record_v1",
        "record_kind": "CALLBACK_RESULT",
        "payload": {"result": "ok"},
    }

    encoded, state = codec.encode_p3_v2_row(row)

    assert encoded == {**row, "schema_version": "spider_journal_record_v2"}
    assert state == ()
    assert row["schema_version"] == "spider_journal_record_v1"


def test_codec_kind_table_matches_j3_reader_closed_kind_set():
    expected = {
        "HISTORICAL_MARKET_RESULT",
        "SNAPSHOT_FACT",
        "CALLBACK_RESULT",
        "SOURCE_GROUP_RESULT",
    }
    assert {kind for kind, _, _ in slots.P3_V2_SLOT_TABLE} == expected
    assert limits.P3_V2_RECORD_KINDS == expected


def test_market_slots_follow_protocol_order_and_assign_dense_indexes():
    first = _working("shared", side="LONG")
    second = _working("shared", side="SHORT")
    source = _market([first, second], [second, first])

    encoded, next_state = codec.encode_p3_v2_row(source)

    assert encoded["schema_version"] == "spider_journal_record_v2"
    assert encoded["payload"]["request"]["working_orders"] == [0, 1]
    assert encoded["payload"]["working_orders_snapshot"] == [1, 0]
    assert encoded["order_defs"] == [first, second]
    assert next_state == (canonical_bytes(first), canonical_bytes(second))
    assert all(
        type(index) is int
        for slot in (
            encoded["payload"]["request"]["working_orders"],
            encoded["payload"]["working_orders_snapshot"],
        )
        for index in slot
    )


def test_duplicate_definitions_are_reused_across_rows_without_mutating_state():
    order = _working()
    encoded_first, state_first = codec.encode_p3_v2_row(_market([order], [order]))
    encoded_second, state_second = codec.encode_p3_v2_row(
        _market([copy.deepcopy(order)], []), state_first
    )

    assert encoded_first["order_defs"] == [order]
    assert "order_defs" not in encoded_second
    assert encoded_second["payload"]["request"]["working_orders"] == [0]
    assert state_second == state_first == (canonical_bytes(order),)


def test_same_order_id_with_different_canonical_bytes_is_a_new_definition():
    one = _working("same-id", remaining_quantity_contracts="1")
    two = _working("same-id", remaining_quantity_contracts="2")

    encoded, state = codec.encode_p3_v2_row(_market([one, two], []))

    assert encoded["payload"]["request"]["working_orders"] == [0, 1]
    assert encoded["order_defs"] == [one, two]
    assert len(state) == 2
    assert state[0] != state[1]


def test_source_group_before_then_after_slots_use_open_order_shape():
    before = _open("before")
    after = _open("after")

    encoded, state = codec.encode_p3_v2_row(_source_group([before], [after]))

    assert encoded["payload"]["owner_evidence_before"]["open_orders_fact"][
        "immutable_payload"
    ]["rows"] == [0]
    assert encoded["payload"]["owner_evidence_after"]["open_orders_fact"][
        "immutable_payload"
    ]["rows"] == [1]
    assert encoded["order_defs"] == [before, after]
    assert state == (canonical_bytes(before), canonical_bytes(after))


def test_snapshot_slot_selected_only_for_open_orders_success():
    opened = _open("snapshot")
    encoded, state = codec.encode_p3_v2_row(_snapshot([opened]))
    failed, failed_state = codec.encode_p3_v2_row(_snapshot(..., outcome="FAILURE"))
    positions, positions_state = codec.encode_p3_v2_row(
        _snapshot([_open("positions")], snapshot_kind="POSITIONS")
    )

    assert encoded["payload"]["fact"]["immutable_payload"]["rows"] == [0]
    assert encoded["order_defs"] == [opened]
    assert state == (canonical_bytes(opened),)
    assert failed == {
        **_snapshot(..., outcome="FAILURE"),
        "schema_version": "spider_journal_record_v2",
    }
    assert failed_state == ()
    assert positions_state == ()
    assert "order_defs" not in positions
    assert positions["payload"]["fact"]["immutable_payload"]["rows"] == [
        _open("positions")
    ]
