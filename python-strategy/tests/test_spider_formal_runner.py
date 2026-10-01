"""Causal and mutation coverage for the admitted P02 projection boundary."""

from __future__ import annotations

import json
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_formal_runner as formal
from src.core.backtest import spider_scenario_run as run
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_jsonl
from test_spider_scenario_run import historical_input


@pytest.fixture(scope="module")
def admitted_p3_artifacts(tmp_path_factory):
    root = tmp_path_factory.mktemp("admitted-p3-projection")
    directory = _write_run(root, "p4-projection-causality")
    admitted = admit_spider_run(directory)
    assert admitted["decision"] == "ACCEPT"
    return admitted["artifacts"]


def _write_run(root: Path, run_id: str) -> Path:
    _, raw = historical_input(run_id)
    result = run.run_spider_scenario(str(root), run_id, run._P3_SELECTOR, raw)
    assert result == dict(run_id=run_id, outcome="ADMITTED", reason=None)
    directory = root / run_id
    assert admit_spider_run(directory)["decision"] == "ACCEPT"
    return directory


def _replace_jsonl_row(directory: Path, name: str, mutate: Any) -> None:
    path = directory / name
    rows = decode_jsonl(path.read_bytes())
    mutate(rows)
    path.write_bytes(b"".join(canonical_bytes(row) + b"\n" for row in rows))


def test_semantically_identical_p3_runs_project_byte_identically(tmp_path):
    first = _write_run(tmp_path, "p4a")
    second = _write_run(tmp_path, "p4-a-run-id-with-a-distinct-length")
    left_bundle = admit_spider_run(first)["artifacts"]
    right_bundle = admit_spider_run(second)["artifacts"]

    left = formal.project_admitted_spider_run(first)
    right = formal.project_admitted_spider_run(second)

    assert left is not None and right is not None
    assert len("p4a") != len("p4-a-run-id-with-a-distinct-length")
    assert left.canonical_json == right.canonical_json
    assert left.sha256 == right.sha256 == sha256(left.canonical_json).hexdigest()
    assert left_bundle["completion.json"]["run_id"] != right_bundle["completion.json"]["run_id"]
    assert left_bundle["completion.json"]["endpoint_state_digest"] != right_bundle["completion.json"]["endpoint_state_digest"]
    assert left_bundle["completion.json"]["reconciliation_digest"] != right_bundle["completion.json"]["reconciliation_digest"]
    assert [entry["sha256"] for entry in left_bundle["completion.json"]["artifacts"]] != [
        entry["sha256"] for entry in right_bundle["completion.json"]["artifacts"]
    ]
    assert [entry["byte_count"] for entry in left_bundle["completion.json"]["artifacts"]] != [
        entry["byte_count"] for entry in right_bundle["completion.json"]["artifacts"]
    ]
    assert left.value["schema_version"] == "spider_p02_projection_v1"
    assert left.report_rows == right.report_rows
    detached = left.report_rows
    detached[0]["account_cash"] = "tamper"
    assert left.report_rows == left.value["report_rows"]


def test_exact_report_digest_fields_are_replaced_by_normalized_row_digest(tmp_path):
    directory = _write_run(tmp_path, "p4-report-digest")
    projection = formal.project_admitted_spider_run(directory)
    assert projection is not None
    normalized_rows = cast(list[dict[str, object]], projection.value["report_rows"])
    digest = sha256(canonical_bytes(normalized_rows)).hexdigest()
    reconciliation = cast(dict[str, object], projection.value["reconciliation"])
    checks = cast(list[dict[str, object]], reconciliation["checks"])
    report_check = next(row for row in checks if row["name"] == "REPORT_PROJECTION_MATCH")
    for side in ("expected", "observed"):
        evidence = cast(dict[str, object], report_check[side])
        assert evidence["report_sha256"] == digest
        assert evidence["report_rows"] == normalized_rows


@pytest.mark.parametrize(
    ("artifact", "mutate"),
    [
        ("report.jsonl", lambda rows: rows[0].__setitem__("account_cash", "999999")),
        ("journal.jsonl", lambda rows: rows.reverse()),
        ("journal.jsonl", lambda rows: rows[0].__setitem__("journal_seq", 999)),
    ],
)
def test_tampered_report_or_ordered_trace_cannot_project(tmp_path, artifact, mutate):
    directory = _write_run(tmp_path, "p4-tamper")
    _replace_jsonl_row(directory, artifact, mutate)
    assert formal.project_admitted_spider_run(directory) is None


def test_completion_and_reconciliation_tampering_cannot_project(tmp_path):
    directory = _write_run(tmp_path, "p4-completion-tamper")
    completion = json.loads((directory / "completion.json").read_text())
    completion["artifacts"][0]["sha256"] = "0" * 64
    (directory / "completion.json").write_bytes(canonical_bytes(completion) + b"\n")
    assert formal.project_admitted_spider_run(directory) is None


@pytest.mark.parametrize(
    ("artifact", "path", "replacement"),
    [
        ("attempt.json", ("historical_context", "source_sha256"), "0" * 64),
        ("attempt.json", ("configuration_context", "configuration_sha256"), "0" * 64),
        ("attempt.json", ("historical_context", "model_sha256"), "0" * 64),
        ("reconciliation.json", ("result",), "FAILED"),
    ],
)
def test_invalid_source_configuration_model_or_reconciliation_cannot_project(
    tmp_path, artifact, path, replacement,
):
    directory = _write_run(tmp_path, "p4-invalid-input")

    def mutate(rows: list[dict[str, object]]) -> None:
        target = rows[0]
        for component in path[:-1]:
            target = cast(dict[str, object], target[component])
        target[path[-1]] = replacement

    _replace_jsonl_row(
        directory,
        artifact,
        mutate,
    )
    assert formal.project_admitted_spider_run(directory) is None


def test_only_shared_admission_decides_acceptance(monkeypatch, tmp_path):
    calls = []

    def reject(path):
        calls.append(path)
        return {"decision": "REJECT", "reason": "fixture-rejection", "evidence": ()}

    monkeypatch.setattr(formal, "admit_spider_run", reject)
    assert formal.project_admitted_spider_run(tmp_path) is None
    assert calls == [tmp_path]


def test_raw_completion_only_exclusions_are_scoped_to_completion_artifact_metadata(tmp_path):
    directory = _write_run(tmp_path, "p4-exclusions")
    projection = formal.project_admitted_spider_run(directory)
    assert projection is not None
    value = projection.value
    completion = cast(dict[str, object], value["completion"])
    artifacts = cast(list[dict[str, object]], completion["artifacts"])
    assert all("sha256" not in item and "byte_count" not in item for item in artifacts)
    attempt = cast(dict[str, object], value["attempt"])
    assert "program_sha256" in attempt
    assert "scenario_plan_sha256" in attempt
    def has_run_id_key(item: object) -> bool:
        if type(item) is dict:
            return "run_id" in item or any(has_run_id_key(nested) for nested in item.values())
        if type(item) is list:
            return any(has_run_id_key(nested) for nested in item)
        return False

    assert not has_run_id_key(value)


def test_projection_retains_admitted_ordered_p3_semantics(admitted_p3_artifacts):
    artifacts = admitted_p3_artifacts
    value = formal._projection_value(artifacts)

    attempt = deepcopy(artifacts["attempt.json"])
    attempt.pop("run_id")
    status = deepcopy(artifacts["status.json"])
    status.pop("run_id")
    endpoint = deepcopy(artifacts["endpoint.json"])
    endpoint.pop("run_id")
    history = deepcopy(artifacts["historical_input.json"])
    history.pop("run_id")
    reports = deepcopy(artifacts["report.jsonl"])
    for row in reports:
        row.pop("run_id")

    assert value["attempt"] == attempt
    assert value["status"] == status
    assert value["endpoint"] == endpoint
    assert value["historical_input"] == history
    assert value["report_rows"] == reports

    source_journal = artifacts["journal.jsonl"]
    projected_journal = cast(list[dict[str, object]], value["journal"])
    assert len(projected_journal) == len(source_journal) > 1
    assert [row["journal_seq"] for row in projected_journal] == [
        row["journal_seq"] for row in source_journal
    ]
    for projected, source in zip(projected_journal, source_journal, strict=True):
        assert projected["journal_seq"] == source["journal_seq"]
        assert projected["barrier_id"] == source["barrier_id"]
        assert projected["record_kind"] == source["record_kind"]
        assert projected["scheduler_key"] == source["scheduler_key"]
        assert projected["causal_parent_ids"] == source["causal_parent_ids"]
        assert projected["effective_at"] == source["effective_at"]
        assert projected["visible_at"] == source["visible_at"]
        assert projected["account_version_before"] == source["account_version_before"]
        assert projected["account_version_after"] == source["account_version_after"]
        assert projected["payload"] == source["payload"]
    projected_endpoint = cast(dict[str, object], value["endpoint"])
    projected_observation = cast(dict[str, object], projected_endpoint["scheduler_observation"])
    source_observation = cast(dict[str, object], artifacts["endpoint.json"]["scheduler_observation"])
    assert projected_observation["callback_actions"] == source_observation["callback_actions"]
    assert "callback_actions" in projected_observation
    assert projected_observation["pending_keys"] == source_observation["pending_keys"]


def _change_attempt_identity(artifacts):
    artifacts["attempt.json"]["program_sha256"] = "0" * 64


def _change_journal_leaf(artifacts):
    artifacts["journal.jsonl"][0]["effective_at"] += 1


def _remove_journal_row(artifacts):
    artifacts["journal.jsonl"].pop()


def _reorder_journal(artifacts):
    artifacts["journal.jsonl"].reverse()


def _add_journal_row(artifacts):
    artifacts["journal.jsonl"].append(deepcopy(artifacts["journal.jsonl"][-1]))


def _change_report_finance(artifacts):
    artifacts["report.jsonl"][0]["account_cash"] = "1001"


def _change_endpoint_open_order(artifacts):
    rows = artifacts["endpoint.json"]["final_owner_evidence"]["open_orders_fact"]["immutable_payload"]["rows"]
    rows.append({"order_id": "projection-sensitivity-probe"})


def _change_endpoint_callback_action(artifacts):
    actions = artifacts["endpoint.json"]["scheduler_observation"]["callback_actions"]
    actions.append({"projection-sensitivity-probe": "action"})


def _change_completion_state(artifacts):
    artifacts["completion.json"]["terminal_reason"] = "projection-sensitivity-probe"


def _change_status_state(artifacts):
    artifacts["status.json"]["state"] = "projection-sensitivity-probe"


def _change_historical_input(artifacts):
    artifacts["historical_input.json"]["model_id"] = "projection-sensitivity-probe"


def _change_reconciliation_hash(artifacts):
    check = next(
        row for row in artifacts["reconciliation.json"]["checks"]
        if row["name"] == "OWNER_DIGEST_MATCH"
    )
    check["expected"][0]["expected_owner_sha256"] = "0" * 64


@pytest.mark.parametrize(
    "mutate",
    [
        _change_attempt_identity,
        _change_journal_leaf,
        _remove_journal_row,
        _reorder_journal,
        _add_journal_row,
        _change_report_finance,
        _change_endpoint_open_order,
        _change_endpoint_callback_action,
        _change_status_state,
        _change_completion_state,
        _change_historical_input,
        _change_reconciliation_hash,
    ],
    ids=[
        "identity-hash", "journal-leaf", "journal-removal", "journal-reorder",
        "journal-addition", "report-finance", "endpoint-open-order",
        "endpoint-callback-action", "status-state", "completion-state",
        "historical-input", "reconciliation-hash",
    ],
)
def test_projection_bytes_are_sensitive_to_each_retained_semantic_class(
    admitted_p3_artifacts, mutate,
):
    baseline = canonical_bytes(formal._projection_value(admitted_p3_artifacts))
    changed_artifacts = deepcopy(admitted_p3_artifacts)
    mutate(changed_artifacts)
    changed = canonical_bytes(formal._projection_value(changed_artifacts))
    assert changed != baseline
    assert sha256(changed).digest() != sha256(baseline).digest()
