"""Causal structure tests, deliberately independent of evidence truth."""

import ast
import hashlib
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_completion_schema as schema
from src.core.backtest.spider_run_artifacts import (
    ConfigurationContext, canonical_bytes, configuration_context, decode_canonical,
    decode_jsonl, encode_jsonl,
)

HASH = "a" * 64
PATHS = ["attempt.json", "status.json", "journal.jsonl", "endpoint.json", "reconciliation.json", "report.jsonl"]
SCHEMAS = ["attempt", "status", "journal_record", "endpoint", "reconciliation", "product_report"]
ORDER = dict(order_id="z", client_order_id="client", product_id="P_A", state="live", side="buy",
             limit_price="2", original_size_contracts="1", cumulative_filled_size_contracts="0", created_at=0)


def report_row(product: str = "P_A") -> dict[str, Any]:
    return dict(schema_version="spider_product_report_v1", run_id="run", product_id=product,
                terminal_reason="SCHEDULED_MTM", position_contracts="0", mark_price=None,
                notional_usd="0", open_orders=[], committed_execution_refs=["SOURCE:中文😀", "LIQUIDATION:a"],
                account_cash="1", account_equity="2", account_available_equity="3",
                account_gross_realized="-1", account_total_fees="0.1", source_evidence_refs=["journal:1"])


CONFIGURED_PRODUCTS = ["CFG-FIRST", "CFG-MIDDLE", "CFG-LAST"]


def configured_context(products=None, config_id="configured-v1") -> dict[str, Any]:
    return {
        "schema_version": "spider_configuration_context_v1",
        "config_id": config_id,
        "configuration_sha256": HASH,
        "products": list(CONFIGURED_PRODUCTS if products is None else products),
    }


def configured_report_row(product: str, context=None) -> dict[str, Any]:
    context = configured_context() if context is None else context
    row = report_row(product)
    row["configuration_context"] = deepcopy(context)
    row["open_orders"] = [dict(ORDER, product_id=product)]
    return row


def configured_report_rows(context=None) -> list[dict[str, Any]]:
    context = configured_context() if context is None else context
    return [configured_report_row(product, context) for product in context["products"]]


def manifest() -> dict[str, Any]:
    return dict(schema_version="spider_completion_v1", run_id="run", state="COMPLETE",
                terminal_reason="SCHEDULED_MTM", endpoint_state_digest=HASH, reconciliation_digest=HASH,
                processed_boundary=dict(ordinal=1, barrier_id="a", journal_seq=1),
                persisted_boundary=dict(ordinal=2, barrier_id="b", journal_seq=3),
                input_contract_hashes=[dict(name=name, sha256=HASH) for name in
                                       ["SCENARIO_PLAN", "PROGRAM", "NATIVE_ARTIFACT", "POLICY_SOURCE_MANIFEST"]],
                planned_coverage=[dict(ordinal=1, barrier_id="a", record_kind="SOURCE_GROUP_RESULT")],
                artifacts=[dict(path=path, schema_version=f"spider_{name}_v1", sha256=HASH,
                                byte_count=0, row_count=99) for path, name in zip(PATHS, SCHEMAS, strict=True)])


def objects(value: Any):
    if isinstance(value, dict):
        yield value
        for item in value.values():
            yield from objects(item)
    elif isinstance(value, list):
        for item in value:
            yield from objects(item)


@pytest.mark.parametrize("ref", ["journal:1", "journal:99", "artifact:report.jsonl#", "artifact:report.jsonl#/0",
                               "artifact:endpoint.json#/中文😀/~0/~1/%25/%zz/#", "artifact:completion.json#/"])
def test_literal_reference_accepts_without_resolution(ref):
    assert schema.evidence_reference(ref) == ref


@pytest.mark.parametrize("suffix", [" ", "\t", "\n", "\u2003", "token \t\n\u2003"])
def test_literal_pointer_whitespace_is_preserved(suffix):
    ref = "artifact:endpoint.json#/" + suffix
    assert schema.evidence_reference(ref).encode("utf-8") == ref.encode("utf-8")


@pytest.mark.parametrize("ref", ["journal:0", "journal:01", "journal:-1", "journal:1.0", "journal:true",
                               "artifact:report.jsonl", "artifact:report.jsonl#x", "artifact:x.json#/",
                               "artifact:../report.jsonl#/", "artifact:report.jsonl#/~", "artifact:report.jsonl#/~2",
                               "artifact:report.jsonl#%2F0", "", None, 1, True])
def test_bad_reference(ref):
    with pytest.raises(ValueError):
        schema.evidence_reference(ref)


@pytest.mark.parametrize("refs", [[], ["journal:1", "journal:1"], ["journal:01"], None])
def test_bad_reference_list(refs):
    with pytest.raises(ValueError):
        schema.evidence_references(refs)


def test_valid_vectors_semantic_mismatch_and_immutability():
    for rows in [[report_row()], [report_row("BTC-USDT-SWAP"), report_row("ETH-USDT-SWAP")]]:
        rows[0]["mark_price"] = "9"  # Flat/projection truth belongs to evidence validation.
        rows[0]["source_evidence_refs"] = ["journal:9", "journal:1"]
        before = deepcopy(rows)
        schema.report(decode_jsonl(encode_jsonl(cast(list[object], rows))))
        assert rows == before
    value = manifest()  # Conflicting boundaries and fabricated counts are legal syntax.
    before = deepcopy(value)
    schema.completion(value)
    assert value == before


@pytest.mark.parametrize("products", [[], ["BTC-USDT-SWAP"], ["ETH-USDT-SWAP", "BTC-USDT-SWAP"],
                                    ["P_A", "P_A"], ["P_A", "BTC-USDT-SWAP", "ETH-USDT-SWAP"]])
def test_invalid_report_vectors(products):
    with pytest.raises(ValueError):
        schema.report([report_row(product) for product in products])


@pytest.mark.parametrize("factory,validate", [(report_row, schema.report_row), (manifest, schema.completion)])
def test_every_object_closed_and_every_required_field(factory, validate):
    value = factory()
    if factory is report_row:
        value["open_orders"] = [deepcopy(ORDER)]
    validate(value)
    for index, original in enumerate(objects(value)):
        for key in original:
            changed = deepcopy(value)
            del list(objects(changed))[index][key]
            with pytest.raises(ValueError):
                validate(changed)
        changed = deepcopy(value)
        list(objects(changed))[index]["extra"] = 1
        with pytest.raises(ValueError):
            validate(changed)


@pytest.mark.parametrize("key", list(report_row()))
def test_report_null_boundaries(key):
    value = report_row()
    value[key] = None
    if key == "mark_price":
        schema.report_row(value)
    else:
        with pytest.raises(ValueError):
            schema.report_row(value)


@pytest.mark.parametrize("bad", [True, 1, "1.0", "-0", "1e2", "NaN"])
@pytest.mark.parametrize("key", ["position_contracts", "mark_price", "notional_usd", "account_cash", "account_equity",
                               "account_available_equity", "account_gross_realized", "account_total_fees"])
def test_every_decimal_field(key, bad):
    value = report_row()
    value[key] = bad
    with pytest.raises(ValueError):
        schema.report_row(value)


@pytest.mark.parametrize("refs", [["SNAPSHOT:x"], ["SOURCE:"], ["SOURCE: x"], ["SOURCE:x", "SOURCE:x"], ["SOURCE"]])
def test_execution_references(refs):
    value = report_row()
    value["committed_execution_refs"] = refs
    with pytest.raises(ValueError):
        schema.report_row(value)


@pytest.mark.parametrize("key,bad", [("schema_version", "spider_report_v1"), ("run_id", "bad id"),
                                   ("product_id", "UNKNOWN"), ("terminal_reason", "UNKNOWN"),
                                   ("open_orders", {}), ("committed_execution_refs", {}),
                                   ("source_evidence_refs", []), ("source_evidence_refs", ["journal:1", "journal:1"])])
def test_report_discriminants_and_lists(key, bad):
    value = report_row()
    value[key] = bad
    with pytest.raises(ValueError):
        schema.report_row(value)


def test_order_rows_order_duplicates_and_deep_scalar():
    value = report_row()
    value["open_orders"] = [dict(ORDER, order_id="a"), dict(ORDER, order_id="z")]
    schema.report_row(value)
    for orders in [value["open_orders"][::-1], [ORDER, ORDER], [dict(ORDER, created_at=True)]]:
        with pytest.raises(ValueError):
            schema.report_row(dict(value, open_orders=orders))


@pytest.mark.parametrize("key", list(manifest()))
def test_manifest_null_boundaries(key):
    value = manifest()
    value[key] = None
    with pytest.raises(ValueError):
        schema.completion(value)


@pytest.mark.parametrize("field,bad", [("path", "../attempt.json"), ("schema_version", "spider_status_v1"),
                                     ("sha256", "A" * 64), ("byte_count", True), ("row_count", -1),
                                     ("row_count", True), ("byte_count", -1), ("byte_count", "1"),
                                     ("row_count", "1"), ("sha256", None)])
def test_manifest_entry_mutation(field, bad):
    value = manifest()
    value["artifacts"][0][field] = bad
    with pytest.raises(ValueError):
        schema.completion(value)


def test_manifest_order_coverage_boundaries_and_scalars():
    changes = [lambda v: v["artifacts"].reverse(), lambda v: v["artifacts"].pop(),
               lambda v: v["artifacts"].append(v["artifacts"][0]), lambda v: v["input_contract_hashes"].reverse(),
               lambda v: v["input_contract_hashes"].pop(), lambda v: v["input_contract_hashes"][0].update(sha256="x"),
               lambda v: v["input_contract_hashes"][0].update(name="UNKNOWN"),
               lambda v: v["planned_coverage"][0].update(ordinal=True),
               lambda v: v["planned_coverage"][0].update(ordinal=2),
               lambda v: v["planned_coverage"][0].update(record_kind="UNKNOWN"),
               lambda v: v["planned_coverage"].append(dict(v["planned_coverage"][0], ordinal=2)),
               lambda v: v["processed_boundary"].update(journal_seq=0),
               lambda v: v["processed_boundary"].update(journal_seq=True),
               lambda v: v["processed_boundary"].update(barrier_id=""),
               lambda v: v["persisted_boundary"].update(ordinal=True),
               lambda v: v.update(state="FAILED"), lambda v: v.update(terminal_reason="UNKNOWN"),
               lambda v: v.update(endpoint_state_digest="x"), lambda v: v.update(reconciliation_digest="x"),
               lambda v: v.update(run_id="bad id"), lambda v: v.update(schema_version="spider_status_v1")]
    for change in changes:
        value = manifest()
        change(value)
        with pytest.raises(ValueError):
            schema.completion(value)


def test_import_boundary():
    tree = ast.parse(Path(schema.__file__).read_text())
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert imports <= {"typing", "src.core.backtest.spider_run_artifacts", "src.core.backtest.spider_run_native_schema"}
    assert {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names} == {"re"}
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and
                   node.func.id in {"open", "eval", "exec", "__import__"} for node in ast.walk(tree))


def test_configured_report_requires_exact_context_products_and_order():
    raw_context = configured_context()
    context = configuration_context(raw_context)
    rows = configured_report_rows(raw_context)
    schema.report(rows, context=context)

    for changed in [
        rows[::-1],
        rows[:-1],
        [rows[0], rows[0], rows[2]],
        [*rows, configured_report_row("CFG-EXTRA", raw_context)],
    ]:
        with pytest.raises(ValueError):
            schema.report(changed, context=context)

    changed = deepcopy(rows)
    del changed[1]["configuration_context"]
    with pytest.raises(ValueError):
        schema.report(changed, context=context)

    changed = deepcopy(rows)
    changed[1]["configuration_context"]["config_id"] = "other-config"
    with pytest.raises(ValueError):
        schema.report(changed, context=context)

    changed = deepcopy(rows)
    changed[1]["product_id"] = "CFG-LAST"
    with pytest.raises(ValueError):
        schema.report(changed, context=context)


def test_configured_report_context_is_detached_and_preserves_product_order():
    source = configured_context(["CFG-LAST", "CFG-MIDDLE", "CFG-FIRST"])
    context = configuration_context(source)
    rows = configured_report_rows(source)
    schema.report(rows, context=context)
    source["products"].reverse()
    assert context.products == ("CFG-LAST", "CFG-MIDDLE", "CFG-FIRST")
    assert [row["product_id"] for row in rows] == [
        "CFG-LAST", "CFG-MIDDLE", "CFG-FIRST"
    ]
    schema.report(rows, context=context)


def test_empty_configured_context_rejects_empty_report():
    context = ConfigurationContext(
        "spider_configuration_context_v1", "configured-v1", HASH, ()
    )
    with pytest.raises(ValueError):
        schema.report([], context=context)


def test_configured_report_validates_nested_orders_against_context():
    context = configuration_context(configured_context())
    rows = configured_report_rows()
    rows[0]["open_orders"][0]["product_id"] = "BTC-USDT-SWAP"
    with pytest.raises(ValueError):
        schema.report(rows, context=context)


def test_configured_report_rows_cannot_be_validated_without_root_context(monkeypatch):
    rows = configured_report_rows()
    with pytest.raises(ValueError):
        schema.report(rows)

    context = configuration_context(configured_context())
    rows = configured_report_rows()
    original = schema.report_row
    monkeypatch.setattr(schema, "report_row", lambda value, **kwargs: original(value))
    with pytest.raises(ValueError):
        schema.report(rows, context=context)


def test_p1_report_domains_reject_context_leak_and_keep_canonical_bytes():
    schema.report([report_row()])
    schema.report([report_row("BTC-USDT-SWAP"), report_row("ETH-USDT-SWAP")])
    leaked = report_row()
    leaked["configuration_context"] = configured_context()
    with pytest.raises(ValueError):
        schema.report([leaked])
    assert hashlib.sha256(canonical_bytes(report_row())).hexdigest() == (
        "bde6f37495c4a4aea5916c8cd82f6f95b7b28f6059101d1376a2aec1a3641bc8"
    )


def test_completion_context_roundtrips_without_changing_manifest_semantics():
    row = manifest()
    row["configuration_context"] = configured_context(
        ["CFG-LAST", "CFG-MIDDLE", "CFG-FIRST"]
    )
    schema.completion(row)
    encoded = canonical_bytes(row)
    assert decode_canonical(encoded) == row
    assert b'"products":["CFG-LAST","CFG-MIDDLE","CFG-FIRST"]' in encoded

    p1_bytes_hash = hashlib.sha256(canonical_bytes(manifest())).hexdigest()
    assert p1_bytes_hash == "e650d5f2d6e1f9508560706441d6563e1b06631725dd955283e5597ccba579a5"
    for invalid in [
        {**row, "configuration_context": None},
        {**row, "configuration_context": {**configured_context(), "products": ()}},
        {**row, "configuration_context": {**configured_context(), "extra": "x"}},
    ]:
        with pytest.raises(ValueError):
            schema.completion(invalid)
