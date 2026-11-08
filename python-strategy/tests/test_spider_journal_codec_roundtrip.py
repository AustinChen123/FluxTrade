# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""J4 codec contract tests."""

import pytest

from spider_journal_codec_test_support import (
    Path,
    _market,
    _open,
    _snapshot,
    _source_group,
    _working,
    canonical_bytes,
    codec,
    copy,
    decode_canonical,
    hashlib,
)
from spider_journal_codec_io_support import (
    _wire_read,
)


@pytest.mark.parametrize(
    "source,parent_path,field",
    [
        (_market([_working()], []), (), "payload"),
        (_market([_working()], []), ("payload",), "request"),
        (_source_group([_open()], []), ("payload",), "owner_evidence_before"),
        (
            _source_group([_open()], []),
            ("payload", "owner_evidence_before"),
            "open_orders_fact",
        ),
        (
            _source_group([_open()], []),
            ("payload", "owner_evidence_before", "open_orders_fact"),
            "immutable_payload",
        ),
        (_source_group([], [_open()]), ("payload",), "owner_evidence_after"),
        (
            _source_group([], [_open()]),
            ("payload", "owner_evidence_after"),
            "open_orders_fact",
        ),
        (
            _source_group([], [_open()]),
            ("payload", "owner_evidence_after", "open_orders_fact"),
            "immutable_payload",
        ),
        (_snapshot([_open()]), (), "payload"),
        (_snapshot([_open()]), ("payload",), "fact"),
        (
            _snapshot([_open()]),
            ("payload", "fact"),
            "immutable_payload",
        ),
    ],
)
def test_every_selected_slot_parent_field_is_required(source, parent_path, field):
    class DictSubclass(dict):
        pass

    invalid_values = (None, [], 0, True, "wrong", DictSubclass())
    for invalid in ("<missing>", *invalid_values):
        candidate = copy.deepcopy(source)
        parent = candidate
        for name in parent_path:
            parent = parent[name]
        if invalid == "<missing>":
            parent.pop(field, None)
        else:
            parent[field] = invalid
        with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
            codec.encode_p3_v2_row(candidate)


def test_dict_subclass_root_is_not_an_approved_v1_row():
    class DictSubclass(dict):
        pass

    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(
            DictSubclass(
                {
                    "schema_version": "spider_journal_record_v1",
                    "record_kind": "CALLBACK_RESULT",
                    "payload": {},
                }
            )
        )


def test_string_and_immutable_state_subclasses_are_not_exact_wire_types():
    class StringSubclass(str):
        pass

    class TupleSubclass(tuple):
        pass

    class BytesSubclass(bytes):
        pass

    for field in ("schema_version", "record_kind"):
        source = _market([], [])
        original = source[field]
        source[field] = StringSubclass(original)
        with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
            codec.encode_p3_v2_row(source)

    source = _market([], [])
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source, TupleSubclass((b"valid",)))
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source, (BytesSubclass(b"valid"),))


def test_non_success_snapshot_with_rows_does_not_intern_the_rows():
    rows = [_open("failure")]
    source = _snapshot(rows, outcome="FAILURE")

    encoded, state = codec.encode_p3_v2_row(source)

    assert encoded["payload"]["fact"]["immutable_payload"]["rows"] == rows
    assert "order_defs" not in encoded
    assert state == ()


@pytest.mark.parametrize(
    "helper,error_type",
    [
        ("canonical_bytes", TypeError),
        ("canonical_bytes", ValueError),
        ("canonical_bytes", RecursionError),
        ("decode_canonical", TypeError),
        ("decode_canonical", ValueError),
        ("decode_canonical", RecursionError),
    ],
)
def test_canonical_helper_failures_are_classified_as_codec_rejection(
    monkeypatch, helper, error_type
):
    def fail(_value):
        raise error_type("CANONICAL_HELPER_FAILED")

    monkeypatch.setattr(codec, helper, fail)
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(_market([_working()], []))


def test_small_real_f_in_row_round_trips_byte_for_byte_through_j3_reader(tmp_path):
    fixture_path = (
        Path(__file__).parent
        / "fixtures/spider_codec_v2/s0_fin_historical_market_row.json"
    )
    raw_v1 = fixture_path.read_bytes()
    # Provenance: line 87 of S0/fin-a/fin/journal.jsonl, captured before J4;
    # SHA-256 covers the canonical row and its JSONL LF.
    assert hashlib.sha256(raw_v1).hexdigest() == (
        "30461a32cc6062f6be2ec7649a3976c05dff10d29582b604781c8577ac1dbf41"
    )
    assert raw_v1.endswith(b"\n")
    source = decode_canonical(raw_v1[:-1])
    assert type(source) is dict
    source = copy.deepcopy(source)
    contexts = (
        source["configuration_context"],
        source["historical_context"],
    )

    encoded, state = codec.encode_p3_v2_row(source)
    projected = _wire_read([encoded], tmp_path, contexts=contexts)

    assert len(state) == 40
    assert canonical_bytes(projected[0]) + b"\n" == raw_v1
