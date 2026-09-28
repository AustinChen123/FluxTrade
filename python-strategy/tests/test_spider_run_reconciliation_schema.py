"""Independent shape mutations, not reconciliation predicate tests."""

import ast
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from src.core.backtest import spider_run_reconciliation_schema as schema
from test_spider_run_completion_schema import report_row

NAMES = ["PLANNED_COVERAGE_COMPLETE", "PROCESSED_EQUALS_PERSISTED", "JOURNAL_CONTIGUOUS",
         "OWNER_IDENTITY_MATCH", "OWNER_DIGEST_MATCH", "ENDPOINT_SNAPSHOT_VERSION_MATCH",
         "NO_UNRESOLVED_QUEUE", "NO_UNRESOLVED_POLL", "NO_UNSUBMITTED_CALLBACK_ACTION",
         "TERMINAL_POLICY_MATCH", "REPORT_PROJECTION_MATCH"]
BARRIER = dict(ordinal=1, barrier_id="a", record_kind="SOURCE_GROUP_RESULT")
BOUNDARY = dict(ordinal=1, barrier_id="a", journal_seq=1)
KEY = dict(visible_at=0, queue_class="SOURCE_GROUP", schedule_sequence=0, stable_id="a")
POLL = dict(poll_id="p", continuation_id="c", status="PAUSED", awaiting="TRADING")
ACTION = dict(delivery_id="d", event_index=1, action_index=0, group_id="g", status="UNSUBMITTED",
              group_result_ref="journal:1", cancel_effect_ref="artifact:endpoint.json#/ ")
TERMINAL = dict(kind="SOURCE_GROUP", stable_id="a", classification="TERMINAL", reason="FAILED")


def fixture() -> dict[str, Any]:
    values = [
        [BARRIER],
        dict(processed_boundary=BOUNDARY, persisted_boundary=dict(BOUNDARY, ordinal=2), last_planned=BARRIER),
        [dict(BARRIER, journal_seq=7)],
        [dict(evidence_ref="journal:1", account_key=dict(venue="v", environment="e", account="a"),
              profile_id="SYNTHETIC_P1_O03_V1", config_id="scenario-v1")],
        [dict(barrier_id="FINAL", expected_owner_sha256="a" * 64, observed_owner_sha256="b" * 64)],
        dict(inspection_account_version=1, endpoint_cutoff=4,
             **{key: dict(captured_account_version=2, snapshot_as_of=3) for key in ["trading", "positions", "open_orders"]}),
        dict(pending_keys=[KEY], remaining_planned_barriers=[BARRIER]),
        [POLL], [ACTION],
        dict(terminal_policy="SCHEDULED_MTM", terminal_reason="O03_NON_ATOMIC_COMPLETE", scheduler_gate="FAILED",
             scheduler_terminal=TERMINAL, owner_gate="RUNNING", owner_lifecycle="RISK_STABLE", remaining_planned_barriers=[BARRIER]),
        dict(report_rows=[report_row()], report_sha256="c" * 64),
    ]
    return dict(schema_version="spider_reconciliation_v1", run_id="run", result="OK",
                checks=[dict(name=name, expected=deepcopy(value), observed=deepcopy(value), result="OK",
                             evidence_refs=["journal:9", "journal:1"]) for name, value in zip(NAMES, values, strict=True)])


def paths(value: Any, prefix: tuple = ()):
    yield prefix, value
    if isinstance(value, dict):
        for key, child in value.items():
            yield from paths(child, (*prefix, key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from paths(child, (*prefix, index))


def at(value: Any, path: tuple) -> Any:
    for key in path:
        value = value[key]
    return value


def rejects(value):
    with pytest.raises(ValueError):
        schema.reconciliation(value)


def test_complete_failed_and_supplied_ok_are_structure_only():
    value = fixture()
    value["checks"][0]["observed"] = [dict(BARRIER, ordinal=8, barrier_id="mismatch")]
    before = deepcopy(value)
    schema.reconciliation(value)
    assert value == before
    value["result"] = "FAILED"
    for check in value["checks"]:
        check["result"] = "FAILED"
    schema.reconciliation(value)
    for malformed in [dict(value, checks=[]), dict(value, checks=value["checks"][:-1])]:
        rejects(malformed)


def test_every_closed_object_and_required_field():
    value = fixture()
    for path, item in paths(value):
        if not isinstance(item, dict):
            continue
        for key in item:
            changed = deepcopy(value)
            del at(changed, path)[key]
            rejects(changed)
        changed = deepcopy(value)
        at(changed, path)["extra"] = True
        rejects(changed)


@pytest.mark.parametrize("side", ["expected", "observed"])
@pytest.mark.parametrize("index", range(11))
def test_every_shape_and_deep_scalar_type(index, side):
    value = fixture()
    original = value["checks"][index][side]
    for path, item in paths(original):
        if isinstance(item, (dict, list)) or item is None:
            continue
        changed = deepcopy(value)
        at(changed["checks"][index][side], path[:-1])[path[-1]] = True if type(item) is int else 0
        rejects(changed)
    for wrong in [None, True, "opaque", {} if isinstance(original, list) else []]:
        changed = deepcopy(value)
        changed["checks"][index][side] = wrong
        rejects(changed)


@pytest.mark.parametrize("side", ["expected", "observed"])
def test_deep_nullability_and_cross_shape_mutations(side):
    value = fixture()
    nullable = {"group_id", "group_result_ref", "cancel_effect_ref", "awaiting", "scheduler_terminal", "mark_price"}
    for index, check in enumerate(value["checks"]):
        for path, item in paths(check[side]):
            if not path or item is None:
                continue
            changed = deepcopy(value)
            at(changed["checks"][index][side], path[:-1])[path[-1]] = None
            if path[-1] in nullable:
                schema.reconciliation(changed)
            else:
                rejects(changed)
        changed = deepcopy(value)
        changed["checks"][index][side] = deepcopy(value["checks"][(index + 1) % 11][side])
        rejects(changed)


@pytest.mark.parametrize("status", ["PAUSED", "IN_PROGRESS", "COMPLETED", "CALLBACK_FAILED"])
@pytest.mark.parametrize("awaiting", [None, "EARN_IF_DUE", "TRADING", "POSITIONS", "OPEN_ORDERS", "LOCAL_ACTIONS"])
def test_poll_matrix_is_not_completion_truth(status, awaiting):
    value = fixture()
    for side in ["expected", "observed"]:
        value["checks"][7][side][0].update(status=status, awaiting=awaiting)
    schema.reconciliation(value)


@pytest.mark.parametrize("side", ["expected", "observed"])
def test_null_exceptions_and_boundaries(side):
    value = fixture()
    action = value["checks"][8][side][0]
    for name in ["group_id", "group_result_ref", "cancel_effect_ref"]:
        action[name] = None
    value["checks"][7][side][0]["awaiting"] = None
    value["checks"][9][side]["scheduler_terminal"] = None
    schema.reconciliation(value)
    for key in ["processed_boundary", "persisted_boundary", "last_planned"]:
        changed = deepcopy(value)
        changed["checks"][1][side][key] = None
        rejects(changed)


@pytest.mark.parametrize("side", ["expected", "observed"])
def test_preserved_coverage_order_and_unique_positive_identities(side):
    value = fixture()
    rows = [dict(BARRIER, ordinal=9, barrier_id="z"), dict(BARRIER, ordinal=3, barrier_id="a")]
    value["checks"][0][side] = rows
    value["checks"][2][side] = [dict(row, journal_seq=seq) for row, seq in zip(rows, [8, 2], strict=True)]
    before = deepcopy(value)
    schema.reconciliation(value)
    assert value == before
    for index, field, bad in [(0, "ordinal", 0), (0, "ordinal", 9), (0, "barrier_id", "z"),
                              (2, "journal_seq", 0), (2, "journal_seq", 8)]:
        changed = deepcopy(value)
        changed["checks"][index][side][1][field] = bad
        rejects(changed)


@pytest.mark.parametrize("side", ["expected", "observed"])
def test_ordered_nested_collections(side):
    value = fixture()
    collections = [(6, ("pending_keys",), [KEY, dict(KEY, queue_class="DELIVERY")]),
                   (6, ("remaining_planned_barriers",), [BARRIER, dict(BARRIER, ordinal=2, barrier_id="b")]),
                   (7, (), [POLL, dict(POLL, poll_id="q", continuation_id="d")]),
                   (8, (), [ACTION, dict(ACTION, delivery_id="e", event_index=0)]),
                   (9, ("remaining_planned_barriers",), [BARRIER, dict(BARRIER, ordinal=2, barrier_id="b")])]
    for index, path, rows in collections:
        for replacement, valid in [(rows, True), (rows[::-1], False), ([rows[0], rows[0]], False)]:
            changed = deepcopy(value)
            if path:
                changed["checks"][index][side][path[0]] = replacement
            else:
                changed["checks"][index][side] = replacement
            if valid:
                schema.reconciliation(changed)
            else:
                rejects(changed)
    for second in [dict(POLL, continuation_id="d"), dict(POLL, poll_id="q")]:
        changed = deepcopy(value)
        changed["checks"][7][side] = [POLL, second]
        rejects(changed)


@pytest.mark.parametrize("side", ["expected", "observed"])
def test_identity_digest_reference_and_enum_mutations(side):
    mutations = [(3, (0, "config_id"), "unknown"), (3, (0, "profile_id"), "unknown"),
                 (3, (0, "evidence_ref"), "journal:01"), (4, (0, "barrier_id"), "not-final"),
                 (4, (0, "expected_owner_sha256"), "A" * 64), (4, (0, "observed_owner_sha256"), "x"),
                 (5, ("endpoint_cutoff",), -1), (7, (0, "status"), "UNKNOWN"),
                 (7, (0, "awaiting"), "UNKNOWN"), (8, (0, "status"), "UNKNOWN"),
                 (8, (0, "cancel_effect_ref"), "artifact:endpoint.json#/~2"),
                 (8, (0, "group_result_ref"), "journal:0"), (9, ("owner_lifecycle",), "UNKNOWN"),
                 (9, ("scheduler_gate",), "UNKNOWN"), (9, ("owner_gate",), "UNKNOWN"),
                 (9, ("terminal_policy",), "UNKNOWN"), (9, ("terminal_reason",), "UNKNOWN"),
                 (9, ("scheduler_terminal", "reason"), "lowercase"), (10, ("report_sha256",), "x"),
                 (10, ("report_rows", 0, "account_cash"), "1.0")]
    for index, path, bad in mutations:
        changed = fixture()
        at(changed["checks"][index][side], path[:-1])[path[-1]] = bad
        rejects(changed)
    for index in [3, 4]:
        changed = fixture()
        changed["checks"][index][side] *= 2
        rejects(changed)


@pytest.mark.parametrize("index", range(11))
def test_check_order_results_and_reference_lists(index):
    for field, bad in [("name", "UNKNOWN"), ("result", "UNKNOWN"), ("evidence_refs", []),
                       ("evidence_refs", ["journal:1", "journal:1"]), ("evidence_refs", ["journal:01"])]:
        value = fixture()
        value["checks"][index][field] = bad
        rejects(value)
    value = fixture()
    value["checks"][index], value["checks"][(index + 1) % 11] = value["checks"][(index + 1) % 11], value["checks"][index]
    rejects(value)


@pytest.mark.parametrize("field,bad", [("schema_version", "unknown"), ("run_id", "bad id"),
                                     ("result", "UNKNOWN"), ("checks", None)])
def test_top_discriminants(field, bad):
    value = fixture()
    value[field] = bad
    rejects(value)


@pytest.mark.parametrize("side", ["expected", "observed"])
@pytest.mark.parametrize("products", [[], ["ETH-USDT-SWAP"], ["ETH-USDT-SWAP", "BTC-USDT-SWAP"],
                                    ["P_A", "P_A"], ["P_A", "BTC-USDT-SWAP"]])
def test_report_vector_invalid_independently(side, products):
    value = fixture()
    value["checks"][10][side]["report_rows"] = [report_row(product) for product in products]
    rejects(value)


@pytest.mark.parametrize("side", ["expected", "observed"])
@pytest.mark.parametrize("products", [["P_A"], ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]])
def test_report_vector_valid_without_cross_side_equality(side, products):
    value = fixture()
    value["checks"][10][side]["report_rows"] = [report_row(product) for product in products]
    before = deepcopy(value)
    schema.reconciliation(value)
    assert value == before


def test_import_boundary():
    tree = ast.parse(Path(schema.__file__).read_text())
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert imports <= {"src.core.backtest", "src.core.backtest.spider_run_artifacts",
                       "src.core.backtest.spider_run_completion_schema"}
    package_names = {alias.name for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)
                     and node.module == "src.core.backtest" for alias in node.names}
    assert package_names == {"spider_run_envelope_schema", "spider_run_native_schema"}
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and
                   node.func.id in {"open", "eval", "exec", "__import__"} for node in ast.walk(tree))
