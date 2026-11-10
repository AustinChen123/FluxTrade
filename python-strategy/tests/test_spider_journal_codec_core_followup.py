# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""Additional B1 preservation and cross-row tests."""

import pytest

from spider_journal_codec_test_support import (
    _market,
    _open,
    _row,
    _snapshot,
    _source_group,
    _working,
    canonical_bytes,
    codec,
)


def test_null_missing_and_empty_slots_keep_distinct_shapes():
    cases = [
        (
            _market(None, None),
            lambda row: (
                row["payload"]["request"]["working_orders"],
                row["payload"]["working_orders_snapshot"],
            ),
            (None, None),
        ),
        (
            _market([], []),
            lambda row: (
                row["payload"]["request"]["working_orders"],
                row["payload"]["working_orders_snapshot"],
            ),
            ([], []),
        ),
        (
            _market(..., ...),
            lambda row: (
                "working_orders" in row["payload"]["request"],
                "working_orders_snapshot" in row["payload"],
            ),
            (False, False),
        ),
    ]
    for source, get_shape, expected in cases:
        encoded, state = codec.encode_p3_v2_row(source)
        assert get_shape(encoded) == expected
        assert "order_defs" not in encoded
        assert state == ()


def test_callback_row_has_no_slot_and_unknown_delivery_kind_is_rejected():
    callback = _row("CALLBACK_RESULT", {"message": "ok"})
    encoded, state = codec.encode_p3_v2_row(callback)
    assert encoded == {**callback, "schema_version": "spider_journal_record_v2"}
    assert state == ()
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(_row("DELIVERY_ATTEMPT", {}))


def test_state_is_tuple_of_immutable_bytes_and_rejected_calls_do_not_commit():
    valid = _working()
    source = _market([valid], [])
    encoded, proposed = codec.encode_p3_v2_row(source)
    before = proposed

    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(_market([{"bad": "shape"}], []), proposed)

    # Simulate a write failure by declining to adopt the proposed state.
    retry_row, retry_state = codec.encode_p3_v2_row(source)
    assert retry_row == encoded
    assert retry_state == before
    assert type(proposed) is tuple
    assert all(type(item) is bytes for item in proposed)
    assert proposed == (canonical_bytes(valid),)


def test_b1_keeps_context_members_for_the_later_b2_stage():
    source = _market([_working()], [])
    configuration_context = source["configuration_context"]
    historical_context = source["historical_context"]

    encoded, _ = codec.encode_p3_v2_row(source)

    assert encoded["configuration_context"] == configuration_context
    assert encoded["historical_context"] == historical_context
    assert "configuration_context" in encoded
    assert "historical_context" in encoded


@pytest.mark.parametrize(
    "source,slot_path,expected",
    [
        (_market(None, []), ("payload", "request", "working_orders"), None),
        (_market(..., None), ("payload", "working_orders_snapshot"), None),
        (_market([], ...), ("payload", "request", "working_orders"), []),
        (
            _source_group(None, []),
            (
                "payload",
                "owner_evidence_before",
                "open_orders_fact",
                "immutable_payload",
                "rows",
            ),
            None,
        ),
        (_snapshot(None), ("payload", "fact", "immutable_payload", "rows"), None),
        (_snapshot([]), ("payload", "fact", "immutable_payload", "rows"), []),
        (_snapshot(...), ("payload", "fact", "immutable_payload", "rows"), "<missing>"),
    ],
)
def test_slot_null_missing_and_empty_values_are_preserved(source, slot_path, expected):
    encoded, state = codec.encode_p3_v2_row(source)
    current = encoded
    for name in slot_path[:-1]:
        current = current[name]
    actual = current.get(slot_path[-1], "<missing>")

    assert actual == expected
    assert "order_defs" not in encoded
    assert state == ()


def test_valid_snapshot_failure_shape_has_no_slot():
    source = _snapshot(..., outcome="FAILURE")
    encoded, state = codec.encode_p3_v2_row(source)

    assert encoded["payload"]["fact"]["immutable_payload"] == {"outcome": "FAILURE"}
    assert "order_defs" not in encoded
    assert state == ()


def test_legal_twin_rows_with_exact_working_and_open_fields_encode():
    working, opened = _working(), _open()
    working_row = _market([working], [])
    open_row = _source_group([opened], [])

    encoded_working, first_state = codec.encode_p3_v2_row(working_row)
    encoded_open, second_state = codec.encode_p3_v2_row(open_row, first_state)

    assert encoded_working["payload"]["request"]["working_orders"] == [0]
    assert encoded_open["payload"]["owner_evidence_before"]["open_orders_fact"][
        "immutable_payload"
    ]["rows"] == [1]
    assert second_state == (canonical_bytes(working), canonical_bytes(opened))


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
