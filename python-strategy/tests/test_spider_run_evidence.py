"""Pure reconciliation oracle, representable failures and projection errors."""

import ast
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_artifacts as artifacts
from src.core.backtest import spider_run_completion_schema as completion
from src.core.backtest import spider_run_envelope_schema as envelope
from src.core.backtest import spider_run_evidence as evidence
from src.core.backtest.spider_run_reconciliation_schema import reconciliation
from src.core.backtest.spider_scenario_plans import PLAN_IDS, plan_bundle
from test_spider_run_artifacts import attempt as attempt_fixture

RUN = "evidence-test-1"
NAMES = ["PLANNED_COVERAGE_COMPLETE", "PROCESSED_EQUALS_PERSISTED", "JOURNAL_CONTIGUOUS", "OWNER_IDENTITY_MATCH", "OWNER_DIGEST_MATCH",
         "ENDPOINT_SNAPSHOT_VERSION_MATCH", "NO_UNRESOLVED_QUEUE", "NO_UNRESOLVED_POLL", "NO_UNSUBMITTED_CALLBACK_ACTION", "TERMINAL_POLICY_MATCH", "REPORT_PROJECTION_MATCH"]
INITIAL = "artifact:endpoint.json#/initial_owner_evidence"
FINAL = "artifact:endpoint.json#/final_owner_evidence"


def fixture(index=0):
    bundle = cast(dict[str, Any], plan_bundle(PLAN_IDS[index]))
    plan = bundle["plan"]
    attempt = attempt_fixture()
    attempt.update(run_id=RUN, requested_scenario_selector=PLAN_IDS[index], scenario_plan_id=PLAN_IDS[index], profile_id=plan["native_profile"],
                   account_key=plan["account_key"], scenario_plan_sha256=bundle["plan_sha256"], planned_coverage=plan["planned_coverage"], terminal_policy=plan["terminal_policy"])
    for entry, key in zip(cast(list[dict], attempt["input_contract_hashes"]), ["scenario_plan_sha256", "program_sha256", "native_artifact_sha256", "policy_source_sha256"], strict=True):
        entry["sha256"] = attempt[key]
    last = plan["planned_coverage"][-1]
    boundary = dict(ordinal=last["ordinal"], journal_seq=last["ordinal"], barrier_id=last["barrier_id"])
    status = dict(schema_version="spider_status_v1", run_id=RUN, state="COMPLETE", processed_boundary=boundary, persisted_boundary=deepcopy(boundary), failure_reason=None, primary_failure=None)
    for row in [*bundle["journal"], bundle["endpoint"], *bundle["report"]]:
        row["run_id"] = RUN
    return [attempt, status, bundle["journal"], bundle["endpoint"], bundle["report"]]


def validate_inputs(values):
    artifacts.validate_artifact(values[0])
    artifacts.validate_artifact(values[1])
    envelope.journal(values[2])
    envelope.endpoint(values[3])
    completion.report(values[4])


def expected_checks(values):
    attempt, status, journal, endpoint, report = values
    coverage = attempt["planned_coverage"]
    identities, digests = [], []
    sites = [("INITIAL", INITIAL, endpoint["initial_owner_evidence"])]
    for i, row in enumerate(journal):
        if row["record_kind"] == "SOURCE_GROUP_RESULT":
            for phase in ["before", "after"]:
                sites.append((row["barrier_id"] + ":" + phase.upper(), f"artifact:journal.jsonl#/{i}/payload/owner_evidence_{phase}", row["payload"]["owner_evidence_" + phase]))
    sites.append(("FINAL", FINAL, endpoint["final_owner_evidence"]))
    for stage, ref, owner in sites:
        inspection = owner["inspection"]
        identities.append(dict(evidence_ref=ref, account_key=attempt["account_key"], profile_id=attempt["profile_id"], config_id="scenario-v1"))
        digests.append(dict(barrier_id=stage, expected_owner_sha256=inspection["owner_state_digest"], observed_owner_sha256=inspection["owner_state_digest"]))
    final = endpoint["final_owner_evidence"]["inspection"]
    version = final["account_version"]
    cutoff = endpoint["cutoff"]["scheduler_time"]
    shapes = [coverage, dict(processed_boundary=status["processed_boundary"], persisted_boundary=status["persisted_boundary"], last_planned=coverage[-1]),
              [dict(row, journal_seq=row["ordinal"]) for row in coverage], identities, digests,
              dict(inspection_account_version=version, endpoint_cutoff=cutoff, **{key: dict(captured_account_version=version, snapshot_as_of=cutoff) for key in ["trading", "positions", "open_orders"]}),
              dict(pending_keys=[], remaining_planned_barriers=[]), [], [],
              dict(terminal_policy=attempt["terminal_policy"], terminal_reason=attempt["terminal_policy"], scheduler_gate="RUNNING", scheduler_terminal=None,
                   owner_gate="RUNNING", owner_lifecycle=final["lifecycle"], remaining_planned_barriers=[]),
              dict(report_rows=report, report_sha256=sha256(artifacts.canonical_bytes(report)).hexdigest())]
    jrefs = [f"journal:{row['journal_seq']}" for row in journal]
    srefs = [f"journal:{row['journal_seq']}" for row in journal if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    dc = [f"journal:{row['journal_seq']}" for row in journal if row["record_kind"] != "SOURCE_GROUP_RESULT"]
    refs = [["artifact:attempt.json#/planned_coverage", *jrefs], ["artifact:status.json#/processed_boundary", "artifact:status.json#/persisted_boundary", "artifact:attempt.json#/planned_coverage"],
            jrefs, [INITIAL, *srefs, FINAL], [INITIAL, *srefs, FINAL], [FINAL],
            ["artifact:endpoint.json#/scheduler_observation/pending_keys", "artifact:endpoint.json#/remaining_planned_barriers"], ["artifact:endpoint.json#/scheduler_observation/polls"],
            ["artifact:endpoint.json#/scheduler_observation/callback_actions", *dc],
            ["artifact:attempt.json#/terminal_policy", "artifact:endpoint.json#/terminal_reason", "artifact:endpoint.json#/cutoff", "artifact:endpoint.json#/scheduler_observation", FINAL],
            [FINAL, *[f"artifact:report.jsonl#/{i}" for i in range(len(report))]]]
    return [dict(name=name, expected=deepcopy(shape), observed=deepcopy(shape), result="OK", evidence_refs=ref) for name, shape, ref in zip(NAMES, shapes, refs, strict=True)]


@pytest.mark.parametrize("index", range(3))
def test_exact_all_eleven_rows_references_and_immutability(index):
    values = fixture(index)
    validate_inputs(values)
    before = deepcopy(values)
    result = evidence.build_reconciliation(RUN, *values)
    assert result == dict(schema_version="spider_reconciliation_v1", run_id=RUN, result="OK", checks=expected_checks(values))
    reconciliation(result)
    assert values == before
    result["checks"][0]["observed"].clear()
    assert values == before


def at(value: Any, path: tuple) -> Any:
    for key in path:
        value = value[key]
    return value


@pytest.mark.parametrize("index", range(3))
def test_unrepresentable_config_at_every_identity_site(index):
    values = fixture(index)
    sites = [(3, "initial_owner_evidence"), (3, "final_owner_evidence")]
    sites += [(2, i, "payload", phase) for i, row in enumerate(values[2])
              if row["record_kind"] == "SOURCE_GROUP_RESULT"
              for phase in ("owner_evidence_before", "owner_evidence_after")]
    for site in sites:
        mutated = deepcopy(values)
        at(mutated, site)["inspection"]["config_id"] = "other-config"
        validate_inputs(mutated)
        before = deepcopy(mutated)
        with pytest.raises(evidence.ReconciliationProjectionError, match="^ENDPOINT_RECONCILIATION_FAILED$") as error:
            evidence.build_reconciliation(RUN, *mutated)
        assert error.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
        assert mutated == before


@pytest.mark.parametrize("index", range(3))
@pytest.mark.parametrize("field", ["account_key", "profile_id"])
def test_attempt_identity_mismatch_is_representable_failure(index, field):
    values = fixture(index)
    if field == "account_key":
        values[0][field]["account"] = "other-account"
    else:
        values[0][field] = "SYNTHETIC_P1_LIQUIDATION_V1" if index == 0 else "SYNTHETIC_MIN_CASH_V1"
    validate_inputs(values)
    before = deepcopy(values)
    result = evidence.build_reconciliation(RUN, *values)
    assert result["checks"][3]["result"] == "FAILED"
    assert result["result"] == "FAILED"
    reconciliation(result)
    assert values == before


MUTATIONS = [
    ((0, "planned_coverage", 0, "barrier_id"), "SOURCE_GROUP:other", {1}),
    ((2, 0, "barrier_id"), "SOURCE_GROUP:other", {1, 3, 5}),
    ((2, 0, "effective_at"), 999, {3}),
    ((2, 0, "visible_at"), 999, {3}),
    ((2, 0, "account_version_after"), 99, {3}),
    ((2, 0, "scheduler_key", "schedule_sequence"), 99, {3}),
    ((2, 0, "causal_parent_ids"), ["changed"], {3}),
    ((2, 0, "payload", "request", "account_key", "account"), "B", {5}),
    ((2, 0, "payload", "request", "members", 0, "payload", "price"), "1", {5}),
    ((2, 0, "payload", "result", "group_id"), "changed", {5}),
    ((2, 0, "payload", "result", "owner_state_digest"), "f" * 64, {5}),
    ((2, 0, "payload", "owner_evidence_before", "inspection", "cash"), "1", {5}),
    ((2, 0, "payload", "owner_evidence_after", "inspection", "positions_digest"), "f" * 64, {5}),
    ((3, "initial_owner_evidence", "inspection", "account_key", "account"), "B", {4, 5}),
    ((3, "final_owner_evidence", "trading_request", "captured_at"), 999, {5}),
    ((3, "final_owner_evidence", "positions_fact", "captured_account_version"), 99, {5, 6}),
    ((3, "final_owner_evidence", "open_orders_fact", "snapshot_as_of"), 999, {5, 6}),
    ((3, "final_owner_evidence", "trading_fact", "immutable_payload", "equity"), "1", {5}),
    ((3, "final_owner_evidence", "trading_fact", "request_digest"), "f" * 64, {5}),
    ((2, 3, "payload", "delivery", "immutable_payload", "fill_price"), "1", {9}),
    ((2, 3, "payload", "delivery", "account_key", "account"), "B", {9}),
    ((2, 3, "payload", "emission_plan_digest"), "f" * 64, {9}),
    ((2, 4, "payload", "policy_events"), [dict(at_ms=504, kind="alert", reason="total_limit")], {9}),
    ((2, 4, "payload", "outcome"), "CALLBACK_FAILED", {9}),
    ((3, "scheduler_observation", "pending_keys"), [dict(visible_at=999, queue_class="DELIVERY", schedule_sequence=0, stable_id="new")], {7, 10}),
    ((3, "scheduler_observation", "polls"), [dict(poll_id="p", continuation_id="c", status="PAUSED", awaiting=None)], {8, 10}),
    ((3, "scheduler_observation", "records", 0, "classification"), "PENDING", {10}),
    ((3, "scheduler_observation", "last_popped", "visible_at"), 999, {10}),
    ((3, "cutoff", "scheduler_time"), 999, {6, 10}),
    ((3, "terminal_reason"), "O03_NON_ATOMIC_COMPLETE", {10}),
    ((4, 0, "account_cash"), "1", {11}),
    ((4, 0, "source_evidence_refs"), ["journal:2"], {11}),
    ((4, 0, "committed_execution_refs"), [], {11}),
    ((1, "processed_boundary", "ordinal"), 4, {2}),
]


@pytest.mark.parametrize("path,replacement,failed", MUTATIONS)
def test_rehashed_outer_artifacts_do_not_hide_mutations(path, replacement, failed):
    values = fixture()
    parent = at(values, path[:-1])
    parent[path[-1]] = replacement
    validate_inputs(values)
    before = deepcopy(values)
    outer_hashes = [sha256(artifacts.canonical_bytes(value)).hexdigest() for value in values]
    assert all(len(digest) == 64 for digest in outer_hashes)
    result = evidence.build_reconciliation(RUN, *values)
    assert result["result"] == "FAILED"
    assert {i + 1 for i, check in enumerate(result["checks"]) if check["result"] == "FAILED"} == failed
    reconciliation(result)
    assert values == before


@pytest.mark.parametrize("index", range(3))
@pytest.mark.parametrize("phase", ["owner_evidence_before", "owner_evidence_after"])
@pytest.mark.parametrize("path,replacement", [
    (("inspection", "account_version"), 99),
    (("inspection", "owner_state_digest"), "f" * 64),
    (("trading_request", "snapshot_id"), "other"),
    (("positions_request", "captured_at"), 999),
    (("open_orders_request", "account_key", "account"), "other"),
    (("trading_fact", "immutable_payload", "available_equity"), "1"),
    (("positions_fact", "payload_digest"), "f" * 64),
    (("open_orders_fact", "request_digest"), "f" * 64),
])
def test_all_protocols_full_source_owner_vectors(index, phase, path, replacement):
    values = fixture(index)
    owner = values[2][0]["payload"][phase]
    at(owner, path[:-1])[path[-1]] = replacement
    validate_inputs(values)
    result = evidence.build_reconciliation(RUN, *values)
    assert result["checks"][4]["result"] == "FAILED"
    reconciliation(result)


def test_o03_cancel_target_order_and_rejection_with_retained_digests():
    for mutation in ["targets", "rejection"]:
        values = fixture(2)
        payload = values[2][2]["payload"]
        if mutation == "targets":
            payload["request"]["members"][0]["payload"]["targets"].reverse()
        else:
            payload["result"]["rejections"][0]["reason"] = "CAPACITY_EXCEEDED"
        validate_inputs(values)
        assert evidence.build_reconciliation(RUN, *values)["checks"][4]["result"] == "FAILED"


@pytest.mark.parametrize("matches", [0, 1, 2])
def test_callback_matching_complete_result_unique_whole_row(matches):
    values = fixture()
    result = deepcopy(values[2][0]["payload"]["result"])
    if matches == 0:
        result["result_digest"] = "f" * 64
    if matches == 2:
        duplicate = deepcopy(values[2][0])
        duplicate.update(journal_seq=6, barrier_id="SOURCE_GROUP:duplicate")
        values[2].append(duplicate)
    values[3]["scheduler_observation"]["callback_actions"] = [dict(delivery_id="d", event_index=0, action_index=0,
        group_id=result["group_id"], status="SUBMITTED", group_result=result, native_failure=None)]
    validate_inputs(values)
    actual = evidence.build_reconciliation(RUN, *values)
    action = actual["checks"][8]["observed"][0]
    assert action["group_result_ref"] == ("journal:1" if matches == 1 else None)
    assert action["cancel_effect_ref"] is None
    assert actual["checks"][8]["result"] == "FAILED"
    reconciliation(actual)


@pytest.mark.parametrize("status", ["SUBMITTED", "UNSUBMITTED"])
def test_unresolved_nullable_callback_is_failed_not_projection_error(status):
    values = fixture()
    values[3]["scheduler_observation"]["callback_actions"] = [dict(delivery_id="d", event_index=1, action_index=2,
        group_id=None, status=status, group_result=None, native_failure=None)]
    validate_inputs(values)
    result = evidence.build_reconciliation(RUN, *values)
    assert result["checks"][8]["result"] == "FAILED"
    assert result["checks"][8]["observed"][0]["group_result_ref"] is None


@pytest.mark.parametrize("path", [(1, "processed_boundary"), (1, "persisted_boundary"),
                                  (3, "final_owner_evidence", "trading_fact", "captured_account_version"),
                                  (3, "final_owner_evidence", "positions_fact", "captured_account_version"),
                                  (3, "final_owner_evidence", "open_orders_fact", "captured_account_version")])
def test_missing_projection_has_dedicated_error(path):
    values = fixture()
    if len(path) == 2:
        values[1][path[-1]] = None
    else:
        del at(values, path[:-1])[path[-1]]
    validate_inputs(values)
    before = deepcopy(values)
    with pytest.raises(evidence.ReconciliationProjectionError, match="^ENDPOINT_RECONCILIATION_FAILED$") as error:
        evidence.build_reconciliation(RUN, *values)
    assert error.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert values == before


@pytest.mark.parametrize("location", [(0,), (1,), (2, 0), (2, 4), (3,), (4, 0), (4, 1)])
def test_every_run_identity_checked_before_selection(location, monkeypatch):
    values = fixture()
    at(values, location)["run_id"] = "different"
    def forbidden(_):
        raise AssertionError("selector must not run")
    monkeypatch.setattr(evidence, "_plan_bundle", forbidden)
    with pytest.raises(evidence.ReconciliationProjectionError):
        evidence.build_reconciliation(RUN, *values)


@pytest.mark.parametrize("selector", [None, 1, True, "unknown", [PLAN_IDS[0]]])
def test_invalid_plan_resolution(selector):
    values = fixture()
    values[0]["scenario_plan_id"] = selector
    with pytest.raises(evidence.ReconciliationProjectionError, match="^ENDPOINT_RECONCILIATION_FAILED$"):
        evidence.build_reconciliation(RUN, *values)


def test_unrelated_programming_value_error_propagates(monkeypatch):
    def broken(_):
        raise ValueError("programming defect")
    monkeypatch.setattr(evidence, "_plan_bundle", broken)
    with pytest.raises(ValueError, match="^programming defect$") as error:
        evidence.build_reconciliation(RUN, *fixture())
    assert type(error.value) is ValueError


def test_no_reconciliation_authority_and_public_import_boundary():
    assert {name for name in vars(evidence) if not name.startswith("_")} == {"ReconciliationProjectionError", "build_reconciliation"}
    with pytest.raises(TypeError):
        cast(Any, evidence.build_reconciliation)(RUN, *fixture(), reconciliation={"result": "OK"})
    tree = ast.parse(Path(evidence.__file__).read_text())
    assert not any(isinstance(node, ast.Import) for node in ast.walk(tree))
    assert {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)} <= {
        "copy", "hashlib", "typing", "src.core.backtest.spider_run_artifacts", "src.core.backtest.spider_run_completion_schema",
        "src.core.backtest.spider_run_reconciliation_schema", "src.core.backtest.spider_scenario_plans"}
    assert not any(isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id in {"open", "eval", "exec", "__import__"} for node in ast.walk(tree))
