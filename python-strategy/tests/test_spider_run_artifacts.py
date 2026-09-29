from copy import deepcopy
from decimal import Decimal, localcontext

import pytest

from src.core.backtest import spider_run_artifacts as a


def attempt() -> dict[str, object]:
    return {
        "schema_version": "spider_attempt_v1", "run_id": "r-1",
        "run_contract_id": "SPIDER_SYNTHETIC_P1_RUN_V1",
        "registration_state": "VALIDATED", "requested_scenario_selector": "bounded.future",
        "registration_failure": None, "profile_id": "SYNTHETIC_MIN_CASH_V1",
        "account_key": {"venue": "venue", "environment": "test", "account": "A"},
        "scenario_plan_id": "not-a-protocol-equality-check",
        "scenario_plan_sha256": "1" * 64, "program_sha256": "2" * 64,
        "native_artifact_sha256": "3" * 64, "policy_source_sha256": "4" * 64,
        "input_contract_hashes": [
            {"name": name, "sha256": "f" * 64}
            for name in ["SCENARIO_PLAN", "PROGRAM", "NATIVE_ARTIFACT", "POLICY_SOURCE_MANIFEST"]
        ],
        "ordering_contract_id": "S_order_v1", "cost_contract_id": "SPIDER_SYNTHETIC_COSTS_V1",
        "funding_exclusion": "SYNTHETIC_P1_NO_FUNDING_INPUT_OR_CLAIM",
        "terminal_policy": "SCHEDULED_MTM", "artifact_encoding": "artifact_encoding_v1",
        "planned_coverage": [{"ordinal": 1, "barrier_id": "SOURCE_GROUP:x", "record_kind": "SOURCE_GROUP_RESULT"}],
    }


NULL_IDENTITIES = (
    "profile_id account_key scenario_plan_id scenario_plan_sha256 program_sha256 "
    "native_artifact_sha256 policy_source_sha256 ordering_contract_id cost_contract_id "
    "funding_exclusion terminal_policy artifact_encoding"
).split()


def rejected() -> dict[str, object]:
    row = attempt()
    row.update(registration_state="REJECTED", registration_failure="UNSUPPORTED_CONFIGURATION",
               input_contract_hashes=[], planned_coverage=[])
    row.update(dict.fromkeys(NULL_IDENTITIES))
    return row


def configuration_context(products=None, config_id="configured-v1"):
    products = ["CFG-FIRST", "CFG-MIDDLE", "CFG-LAST"] if products is None else products
    return {
        "schema_version": "spider_configuration_context_v1",
        "config_id": config_id,
        "configuration_sha256": "a" * 64,
        "products": list(products),
    }


def status() -> dict[str, object]:
    return dict(schema_version="spider_status_v1", run_id="r-1", state="RUNNING",
                processed_boundary=None, persisted_boundary=None, failure_reason=None, primary_failure=None)


@pytest.mark.parametrize("factory", [attempt, rejected, status])
def test_roundtrip_and_every_top_level_field_is_required(factory):
    original = factory()
    before = deepcopy(original)
    assert a.decode_artifact(a.encode_artifact(original)) == original
    assert original == before
    for key in original:
        row = deepcopy(original)
        del row[key]
        with pytest.raises(ValueError):
            a.validate_artifact(row)
    with pytest.raises(ValueError):
        a.validate_artifact({**original, "extra": None})


def test_canonical_decimal_and_order_are_context_independent():
    with localcontext() as context:
        context.prec = 2
        assert a.canonical_bytes({"z": Decimal("1E+4"), "a": [Decimal("-0"), Decimal("123.4500")]}) == b'{"a":["0","123.45"],"z":"10000"}'
    assert a.canonical_bytes({"unicode": "é"}) == b'{"unicode":"\\u00e9"}'
    assert a.decode_canonical(b'{"a":[true,null,-1]}') == {"a": [True, None, -1]}


@pytest.mark.parametrize("value", [1.5, Decimal("NaN"), Decimal("Infinity"), (1,), {1: "x"}, {"x": [1.5]}])
def test_encoder_rejects_non_json_or_nonfinite_values(value):
    with pytest.raises(ValueError):
        a.canonical_bytes(value)


@pytest.mark.parametrize("raw", [
    b'{"a":1,"a":2}', b'{"x":{"a":1,"a":1}}', b'\xef\xbb\xbf{}', b'{ "a":1}',
    b'{"z":1,"a":2}', b'{"a":1.0}', b'{"a":NaN}', b'{"a":Infinity}', b'{"a":-0}',
    b'{"a":"\\u0061"}', b'{"a":"\xc3\xa9"}', b'{}\n', b'[] ', b'\xff',
])
def test_decoder_rejects_noncanonical_or_ambiguous_bytes(raw):
    with pytest.raises(ValueError):
        a.decode_canonical(raw)


@pytest.mark.parametrize("raw", [b'{}', b'{}\n\n', b'\n', b'{}\r\n', b'{}\n{', b'[]\n'])
def test_jsonl_framing_rejects_blank_partial_or_non_object_rows(raw):
    with pytest.raises(ValueError):
        a.decode_jsonl(raw)


def test_jsonl_is_only_a_framing_codec_not_artifact_admission():
    rows: list[object] = [{"arbitrary key": 1}, {}]
    assert a.decode_jsonl(a.encode_jsonl(rows)) == rows
    assert a.decode_jsonl(b"") == []
    with pytest.raises(ValueError):
        a.encode_jsonl([1])
    for raw in [b"", b"{}\n", a.encode_artifact(status()) * 2]:
        with pytest.raises(ValueError):
            a.decode_artifact(raw)


@pytest.mark.parametrize("field", NULL_IDENTITIES)
def test_rejected_cross_population_and_validated_partial_identity_reject(field):
    row = rejected()
    row[field] = attempt()[field]
    with pytest.raises(ValueError):
        a.validate_artifact(row)
    row = attempt()
    row[field] = None
    with pytest.raises(ValueError):
        a.validate_artifact(row)


@pytest.mark.parametrize("field,value", [
    ("registration_failure", None), ("registration_failure", "OTHER"),
    ("profile_id", "UNKNOWN"), ("scenario_plan_sha256", "UNKNOWN"),
    ("input_contract_hashes", [{"name": "SCENARIO_PLAN", "sha256": "f" * 64}]),
    ("planned_coverage", [{"ordinal": 1, "barrier_id": "x", "record_kind": "SOURCE_GROUP_RESULT"}]),
])
def test_rejected_exact_union(field, value):
    row = rejected()
    row[field] = value
    with pytest.raises(ValueError):
        a.validate_artifact(row)


@pytest.mark.parametrize("field,value", [
    ("run_id", "../bad"), ("run_id", "a" * 65), ("run_id", "é"),
    ("requested_scenario_selector", "x/y"), ("requested_scenario_selector", "x" * 129),
    ("registration_state", "UNKNOWN"), ("registration_failure", "UNSUPPORTED_CONFIGURATION"),
    ("profile_id", "UNKNOWN"), ("program_sha256", "F" * 64),
    ("ordering_contract_id", "UNKNOWN"), ("cost_contract_id", "UNKNOWN"),
    ("funding_exclusion", "UNKNOWN"), ("artifact_encoding", "UNKNOWN"),
    ("terminal_policy", "UNKNOWN"), ("account_key", {"venue": "v", "environment": "t"}),
    ("planned_coverage", ()), ("input_contract_hashes", {}),
])
def test_validated_structural_mutations(field, value):
    row = attempt()
    row[field] = value
    with pytest.raises(ValueError):
        a.validate_artifact(row)


def test_hash_list_order_and_closed_entries():
    names = ["SCENARIO_PLAN", "PROGRAM", "NATIVE_ARTIFACT", "POLICY_SOURCE_MANIFEST"]
    variants = [names[::-1], names[:-1], names + names[:1], [names[0]] * 4]
    for order in variants:
        row = attempt()
        row["input_contract_hashes"] = [{"name": name, "sha256": "a" * 64} for name in order]
        with pytest.raises(ValueError):
            a.validate_artifact(row)


@pytest.mark.parametrize("coverage", [
    [{"ordinal": True, "barrier_id": "x", "record_kind": "SOURCE_GROUP_RESULT"}],
    [{"ordinal": 2, "barrier_id": "x", "record_kind": "SOURCE_GROUP_RESULT"}],
    [{"ordinal": 1, "barrier_id": "x", "record_kind": "OTHER"}],
    [{"ordinal": 1, "barrier_id": "x", "record_kind": "SOURCE_GROUP_RESULT", "extra": 0}],
    [{"ordinal": n, "barrier_id": "x", "record_kind": "SOURCE_GROUP_RESULT"} for n in (1, 2)],
])
def test_coverage_structure_order_and_identity_uniqueness(coverage):
    row = attempt()
    row["planned_coverage"] = coverage
    with pytest.raises(ValueError):
        a.validate_artifact(row)


@pytest.mark.parametrize("state", ["RUNNING", "COMPLETE", "FAILED"])
def test_status_failure_state_matrix(state):
    for failure in [None, "PERSISTENCE_FAILED", "UNKNOWN"]:
        for primary in [None, {"kind": "NATIVE", "reason": "NATIVE_INVARIANT"}]:
            row = status()
            row.update(state=state, failure_reason=failure, primary_failure=primary)
            valid = failure == "PERSISTENCE_FAILED" if state == "FAILED" else failure is None and primary is None
            if valid:
                a.validate_artifact(row)
            else:
                with pytest.raises(ValueError):
                    a.validate_artifact(row)


@pytest.mark.parametrize("boundary", [
    {"ordinal": True, "barrier_id": "x", "journal_seq": 1},
    {"ordinal": 0, "barrier_id": "x", "journal_seq": 1},
    {"ordinal": 1, "barrier_id": "x", "journal_seq": -1},
    {"ordinal": 1, "barrier_id": "", "journal_seq": 1},
    {"ordinal": 1, "barrier_id": "x"},
])
def test_bad_boundary_rejects(boundary):
    row = status()
    row["processed_boundary"] = boundary
    with pytest.raises(ValueError):
        a.validate_artifact(row)


@pytest.mark.parametrize("reason", ["", "Exception('oops')", "lowercase", "A" * 129, "A\n"])
def test_primary_reason_is_bounded_code(reason):
    row = status()
    row.update(state="FAILED", failure_reason="NATIVE_FAULT", primary_failure={"kind": "NATIVE", "reason": reason})
    with pytest.raises(ValueError):
        a.validate_artifact(row)


@pytest.mark.parametrize("value", ["-0", "1.0", "1e2", "+1", "01", "NaN", True, 1])
def test_decimal_text_primitive_rejects_noncanonical(value):
    with pytest.raises(ValueError):
        a._decimal_text(value)


def test_scalar_primitives():
    assert a._decimal_text("-0.01") == "-0.01"
    assert a._integer(0) == 0
    assert a._boolean(False) is False
    for value in [True, -1, "1", 1.0]:
        with pytest.raises(ValueError):
            a._integer(value)
    for value in [0, 1, "true", None]:
        with pytest.raises(ValueError):
            a._boolean(value)


@pytest.mark.parametrize("reason", [
    "UNSUPPORTED_CONFIGURATION", "PERSISTENCE_FAILED", "CALLBACK_FAILED", "NATIVE_FAULT",
    "NATIVE_POISONED", "SCHEDULER_FAILED", "ENDPOINT_RECONCILIATION_FAILED",
    "ARTIFACT_WRITE_FAILED", "PUBLICATION_FAILED", "PUBLICATION_DURABILITY_UNKNOWN", "UNEXPECTED_EXCEPTION",
])
def test_every_closed_failure_reason_and_valid_frontier(reason):
    row = status()
    row.update(state="FAILED", failure_reason=reason,
               processed_boundary={"ordinal": 3, "barrier_id": "SOURCE_GROUP:c", "journal_seq": 3},
               persisted_boundary={"ordinal": 2, "barrier_id": "SOURCE_GROUP:b", "journal_seq": 2})
    assert a.decode_artifact(a.encode_artifact(row)) == row


def test_nested_objects_have_no_missing_or_extra_fields():
    cases = [
        ("account_key", {"venue": "v", "environment": "test", "account": "A"}, attempt),
        ("processed_boundary", {"ordinal": 1, "barrier_id": "x", "journal_seq": 1}, status),
    ]
    for field, nested, factory in cases:
        variants = [{**nested, "extra": 1}]
        variants.extend({key: value for key, value in nested.items() if key != absent} for absent in nested)
        for variant in variants:
            row = factory()
            row[field] = variant
            with pytest.raises(ValueError):
                a.validate_artifact(row)
    for malformed in [{"name": "SCENARIO_PLAN"}, {"name": "SCENARIO_PLAN", "sha256": "f" * 64, "extra": 0}]:
        row = attempt()
        row["input_contract_hashes"] = [malformed] + [
            {"name": name, "sha256": "a" * 64}
            for name in ["PROGRAM", "NATIVE_ARTIFACT", "POLICY_SOURCE_MANIFEST"]
        ]
        with pytest.raises(ValueError):
            a.validate_artifact(row)


@pytest.mark.parametrize("primary", [
    {"kind": "OTHER", "reason": "CODE"}, {"kind": "NATIVE"},
    {"kind": "NATIVE", "reason": "CODE", "extra": None}, True,
])
def test_primary_failure_closed_structure(primary):
    row = status()
    row.update(state="FAILED", failure_reason="NATIVE_FAULT", primary_failure=primary)
    with pytest.raises(ValueError):
        a.validate_artifact(row)


def test_nonprotocol_values_do_not_claim_semantic_validation():
    row = attempt()
    row["planned_coverage"] = []
    a.validate_artifact(row)
    for subaccount in [None, "sub"]:
        row["account_key"] = {"venue": "v", "environment": "test", "account": "A", "subaccount": subaccount}
        a.validate_artifact(row)
    for value in [None, [], {"run_id": "r", "schema_version": "spider_completion_v1"}]:
        with pytest.raises(ValueError):
            a.validate_artifact(value)


def test_configuration_context_is_exact_detached_and_immutable():
    source = configuration_context()
    context = a.configuration_context(source)
    assert context.schema_version == "spider_configuration_context_v1"
    assert context.config_id == "configured-v1"
    assert context.configuration_sha256 == "a" * 64
    assert context.products == ("CFG-FIRST", "CFG-MIDDLE", "CFG-LAST")
    source["config_id"] = "mutated"
    source["configuration_sha256"] = "b" * 64
    source["products"][1] = "MUTATED"
    assert context.config_id == "configured-v1"
    assert context.configuration_sha256 == "a" * 64
    assert context.products == ("CFG-FIRST", "CFG-MIDDLE", "CFG-LAST")
    with pytest.raises(AttributeError):
        context.config_id = "changed"


@pytest.mark.parametrize(
    "update",
    [
        {"schema_version": "other"},
        {"config_id": ""},
        {"configuration_sha256": "A" * 64},
        {"configuration_sha256": "a" * 63},
        {"products": []},
        {"products": ["CFG", "CFG"]},
        {"products": ["CFG", ""]},
        {"extra": 1},
    ],
)
def test_configuration_context_rejects_invalid_identity(update):
    with pytest.raises(ValueError):
        a.configuration_context({**configuration_context(), **update})
