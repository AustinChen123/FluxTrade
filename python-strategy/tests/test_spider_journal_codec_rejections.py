# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""J4 codec contract tests."""

import pytest

from spider_journal_codec_test_support import (
    OPEN_FIELDS,
    WORKING_FIELDS,
    _market,
    _open,
    _snapshot,
    _source_group,
    _working,
    canonical_bytes,
    codec,
    copy,
)


@pytest.mark.parametrize(
    "field,value",
    [
        ("payload", None),
        ("fact", []),
        ("snapshot_kind", 1),
        ("snapshot_kind", None),
        ("snapshot_kind", "<missing>"),
        ("immutable_payload", None),
        ("outcome", True),
        ("outcome", None),
        ("outcome", "<missing>"),
    ],
)
def test_snapshot_dispatch_fields_are_type_exact(field, value):
    source = _snapshot([])
    fact = source["payload"]["fact"]
    if field in {"payload", "fact"}:
        if field == "payload":
            if value == "<missing>":
                source.pop(field)
            else:
                source[field] = value
        else:
            if value == "<missing>":
                source["payload"].pop(field)
            else:
                source["payload"][field] = value
    elif field == "snapshot_kind":
        if value == "<missing>":
            fact.pop(field)
        else:
            fact[field] = value
    elif field == "immutable_payload":
        if value == "<missing>":
            fact.pop(field)
        else:
            fact[field] = value
    else:
        if value == "<missing>":
            fact["immutable_payload"].pop(field)
        else:
            fact["immutable_payload"][field] = value

    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source)


@pytest.mark.parametrize("field", sorted(WORKING_FIELDS))
def test_every_working_order_field_is_required(field):
    order = _working()
    del order[field]
    source = _market([order], [])
    prior = ()

    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source, prior)
    assert prior == ()


@pytest.mark.parametrize("field", sorted(OPEN_FIELDS))
def test_every_open_order_field_is_required(field):
    order = _open()
    del order[field]
    source = _source_group([order], [])
    prior = ()

    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source, prior)
    assert prior == ()


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


@pytest.mark.parametrize(
    "source",
    [
        _market([_open()], []),
        _source_group([_working()], []),
        _market([_working(extra="not-in-schema")], []),
        _source_group([_open(extra="not-in-schema")], []),
        _market([_working()], [_working(extra="bad-snapshot-shape")]),
    ],
)
def test_wrong_order_class_or_extra_key_is_rejected_with_legal_twin(source):
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source)

    legal = (
        _market([_working()], [])
        if source["record_kind"] == "HISTORICAL_MARKET_RESULT"
        else _source_group([_open()], [])
    )
    encoded, _ = codec.encode_p3_v2_row(legal)
    assert "order_defs" in encoded


@pytest.mark.parametrize("bad_slot", [0, False, {}, "bad", (_working(),)])
def test_slot_container_requires_exact_list_type(bad_slot):
    source = _market(bad_slot, [])
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source)


def test_exact_list_and_dict_subclasses_are_not_accepted_as_order_slots(monkeypatch):
    class ListSubclass(list):
        pass

    class DictSubclass(dict):
        pass

    monkeypatch.setattr(
        codec, "canonical_bytes", lambda value: canonical_bytes(dict(value))
    )

    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(_market(ListSubclass([_working()]), []))
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(_market([DictSubclass(_working())], []))


@pytest.mark.parametrize("kind", [None, [], {}, 0, True, "DELIVERY_ATTEMPT", "UNKNOWN"])
def test_actual_kind_is_type_exact_and_in_closed_v2_kind_table(kind):
    source = _market([], [])
    source["record_kind"] = kind
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source)


@pytest.mark.parametrize("schema", [None, [], {}, 0, True, "spider_journal_record_v2"])
def test_actual_schema_version_is_compared_type_exactly(schema):
    source = _market([], [])
    source["schema_version"] = schema
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source)


def test_schema_and_record_kind_fields_cannot_be_missing():
    for field in ("schema_version", "record_kind"):
        source = _market([], [])
        source.pop(field)
        with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
            codec.encode_p3_v2_row(source)


@pytest.mark.parametrize("state", [None, [], [b"x"], ("not-bytes",), (b"x", b"x")])
def test_state_shape_is_closed_and_invalid_state_is_not_adopted(state):
    source = _market([], [])
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source, state)


def test_order_defs_input_is_not_silently_overwritten():
    source = _market([_working()], [])
    source["order_defs"] = []
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source)


def test_encode_does_not_mutate_input_and_detaches_new_definitions():
    order = _working()
    source = _market([order], [])
    original = copy.deepcopy(source)

    encoded, state = codec.encode_p3_v2_row(source)

    assert source == original
    assert encoded is not source
    assert encoded["payload"] is not source["payload"]
    assert encoded["payload"]["request"] is not source["payload"]["request"]
    assert encoded["order_defs"][0] is not order
    order["order_id"] = "changed-after-encode"
    assert encoded["order_defs"][0]["order_id"] == "w-1"
    assert state == (
        canonical_bytes(original["payload"]["request"]["working_orders"][0]),
    )


def test_non_mapping_root_is_unsupported_before_field_access():
    for source in (None, [], {}, 0, True, "bad"):
        with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
            codec.encode_p3_v2_row(source)


def test_canonical_encoder_failure_becomes_named_codec_rejection(monkeypatch):
    source = _market([_working()], [])

    def fail(_value):
        raise ValueError("CANONICAL_HELPER_FAILED")

    monkeypatch.setattr(codec, "canonical_bytes", fail)
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source)


def test_canonical_decoder_failure_becomes_named_codec_rejection(monkeypatch):
    source = _market([_working()], [])

    def fail(_value):
        raise ValueError("CANONICAL_HELPER_FAILED")

    monkeypatch.setattr(codec, "decode_canonical", fail)
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source)


def test_non_dict_decoded_definition_is_rejected(monkeypatch):
    source = _market([_working()], [])
    monkeypatch.setattr(codec, "decode_canonical", lambda _value: [])
    with pytest.raises(ValueError, match="CODEC_UNSUPPORTED_ROW"):
        codec.encode_p3_v2_row(source)
