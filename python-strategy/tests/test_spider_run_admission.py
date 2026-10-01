"""Independent bundle framing and causal shared-admission boundaries."""

import ast
import os
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_admission as admission
from src.core.backtest.spider_run_artifacts import canonical_bytes
from test_spider_run_artifacts import configuration_context
from test_spider_run_evidence import RUN, expected_checks, fixture

NAMES = ("attempt.json", "status.json", "journal.jsonl", "endpoint.json", "reconciliation.json", "report.jsonl", "completion.json")
SCHEMAS = ("spider_attempt_v1", "spider_status_v1", "spider_journal_record_v1", "spider_endpoint_v1", "spider_reconciliation_v1", "spider_product_report_v1")
DIAGNOSTIC = ("completion.json", "status.json", "journal.jsonl")
FRONTIER = ("attempt.json", "status.json", "journal.jsonl", "completion.json")


def framed(value, name):
    rows = value if name.endswith(".jsonl") else [value]
    return b"".join(canonical_bytes(row) + b"\n" for row in rows)


def write_bundle(root, values):
    manifest = values["completion.json"]
    manifest["artifacts"] = []
    for name, schema in zip(NAMES[:-1], SCHEMAS, strict=True):
        raw = framed(values[name], name)
        (root / name).write_bytes(raw)
        manifest["artifacts"].append(dict(path=name, schema_version=schema, byte_count=len(raw), sha256=sha256(raw).hexdigest(),
                                          row_count=len(values[name]) if name.endswith(".jsonl") else 1))
    for name, field in [("endpoint.json", "endpoint_state_digest"), ("reconciliation.json", "reconciliation_digest")]:
        manifest[field] = sha256(canonical_bytes(values[name])).hexdigest()
    (root / "completion.json").write_bytes(framed(manifest, "completion.json"))


def bundle(root, index=0) -> dict[str, Any]:
    inputs = fixture(index)
    attempt, status, journal, endpoint, report = inputs
    attempt["policy_source_sha256"] = "eb6ab34d8685fb59e286f5ffda8af24cbecf3e5ab2c729c585ccdca797cac336"
    attempt["input_contract_hashes"][3]["sha256"] = attempt["policy_source_sha256"]
    recon = dict(schema_version="spider_reconciliation_v1", run_id=RUN, result="OK", checks=expected_checks(inputs))
    manifest = dict(schema_version="spider_completion_v1", run_id=RUN, state="COMPLETE", terminal_reason=endpoint["terminal_reason"],
                    input_contract_hashes=deepcopy(attempt["input_contract_hashes"]), planned_coverage=deepcopy(attempt["planned_coverage"]),
                    processed_boundary=deepcopy(status["processed_boundary"]), persisted_boundary=deepcopy(status["persisted_boundary"]))
    values: dict[str, Any] = dict(zip(NAMES, [attempt, status, journal, endpoint, recon, report, manifest], strict=True))
    write_bundle(root, values)
    return values


def rejected(root, reason, evidence):
    assert admission.admit_spider_run(root) == dict(decision="REJECT", reason=reason, evidence=evidence)


@pytest.mark.parametrize("index", range(3))
def test_all_protocols_accept_exact_detached_bytes_without_writes(tmp_path, index, monkeypatch):
    values = bundle(tmp_path, index)
    before = {name: (tmp_path / name).read_bytes() for name in NAMES}
    original_open = os.open
    opens = []
    def read_only(path, flags, *args, **kwargs):
        assert not flags & (os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND)
        opens.append(path)
        return original_open(path, flags, *args, **kwargs)
    monkeypatch.setattr(admission._os, "open", read_only)
    result = admission.admit_spider_run(tmp_path)
    assert result == dict(decision="ACCEPT", artifacts=values)
    assert tuple(result["artifacts"]) == NAMES
    result["artifacts"]["endpoint.json"]["final_owner_evidence"]["inspection"]["cash"] = "999"
    assert admission.admit_spider_run(tmp_path) == dict(decision="ACCEPT", artifacts=values)
    assert {name: (tmp_path / name).read_bytes() for name in NAMES} == before
    assert set(opens) == {str(tmp_path), *NAMES}


@pytest.mark.parametrize("value", [None, 1, b"bytes", "", "bad\0path"])
def test_invalid_path_types_and_strings(value):
    rejected(cast(Any, value), "UNSAFE_ARTIFACT_PATH", ("run-directory",))


def test_directory_and_leaf_basic_paths(tmp_path, monkeypatch):
    root = tmp_path / "not-the-run-id"
    root.mkdir()
    bundle(root)
    link = tmp_path / "link"
    link.symlink_to(root, target_is_directory=True)
    rejected(link, "UNSAFE_ARTIFACT_PATH", ("run-directory",))
    rejected(root / "completion.json", "UNSAFE_ARTIFACT_PATH", ("run-directory",))
    rejected(tmp_path / "missing", "UNSAFE_ARTIFACT_PATH", ("run-directory",))
    parent = tmp_path / "parent"
    parent.symlink_to(tmp_path, target_is_directory=True)
    assert admission.admit_spider_run(parent / root.name)["decision"] == "ACCEPT"
    monkeypatch.chdir(tmp_path)
    assert admission.admit_spider_run(root.name)["decision"] == "ACCEPT"


def test_p1_completion_context_leak_is_cross_artifact_failure(tmp_path):
    values = bundle(tmp_path)
    values["completion.json"]["configuration_context"] = configuration_context()
    write_bundle(tmp_path, values)
    rejected(tmp_path, "ENDPOINT_RECONCILIATION_FAILED", NAMES)


def test_selected_v2_is_unavailable_after_existing_preselection_checks(tmp_path):
    values = bundle(tmp_path)
    values["attempt.json"]["scenario_plan_id"] = "SPIDER_P2_CONFIGURED_SCALE_V1"
    values["attempt.json"]["requested_scenario_selector"] = "SPIDER_P2_CONFIGURED_SCALE_V1"
    write_bundle(tmp_path, values)
    rejected(tmp_path, "ENDPOINT_RECONCILIATION_FAILED", NAMES)


@pytest.mark.parametrize("name", ["completion.json", "endpoint.json"])
@pytest.mark.parametrize("kind", ["symlink", "directory"])
def test_final_leaf_rejection(tmp_path, name, kind):
    bundle(tmp_path)
    leaf = tmp_path / name
    leaf.unlink()
    if kind == "symlink":
        leaf.symlink_to(tmp_path / "missing-target")
    else:
        leaf.mkdir()
    rejected(tmp_path, "UNSAFE_ARTIFACT_PATH", (name,))


def test_manifest_precedes_unsafe_later_artifact(tmp_path):
    bundle(tmp_path)
    (tmp_path / "completion.json").write_bytes(b"{}\n")
    (tmp_path / "report.jsonl").unlink()
    (tmp_path / "report.jsonl").mkdir()
    rejected(tmp_path, "INVALID_MANIFEST", ("completion.json",))


@pytest.mark.parametrize("persisted", [0, 2])
@pytest.mark.parametrize("matching", [True, False])
def test_absent_completion_diagnostic_and_only_needed_opens(tmp_path, persisted, matching, monkeypatch):
    values = bundle(tmp_path)
    status = values["status.json"]
    rows = values["journal.jsonl"][:persisted]
    status.update(state="FAILED", failure_reason="PERSISTENCE_FAILED", processed_boundary=dict(ordinal=4, journal_seq=4, barrier_id="processed"),
                  persisted_boundary=dict(ordinal=persisted, journal_seq=persisted, barrier_id=rows[-1]["barrier_id"]) if rows else None)
    if not matching:
        status["failure_reason"] = "CALLBACK_FAILED"
    (tmp_path / "status.json").write_bytes(framed(status, "status.json"))
    (tmp_path / "journal.jsonl").write_bytes(framed(rows, "journal.jsonl"))
    (tmp_path / "completion.json").unlink()
    original = os.open
    opened = []
    def observe(path, flags, *args, **kwargs):
        opened.append(path)
        return original(path, flags, *args, **kwargs)
    monkeypatch.setattr(admission._os, "open", observe)
    rejected(tmp_path, "INCOMPLETE_PERSISTENCE" if matching else "MISSING_COMPLETE_MANIFEST", DIAGNOSTIC)
    assert opened == [str(tmp_path), "status.json", "journal.jsonl"]


@pytest.mark.parametrize("mutation", ["bytes", "sha256", "byte_count", "row_count", "semantic_digest", "oversize", "oversize_journal"])
def test_actual_file_metadata_is_not_trusted(tmp_path, mutation):
    values = bundle(tmp_path)
    manifest = values["completion.json"]
    if mutation == "bytes":
        (tmp_path / "endpoint.json").write_bytes(b"{}\n")
    elif mutation == "oversize":
        (tmp_path / "endpoint.json").write_bytes(b" " * (16_777_216 + 1))
    elif mutation == "oversize_journal":
        (tmp_path / "journal.jsonl").write_bytes(b" " * (16_777_216 + 1))
    elif mutation == "semantic_digest":
        manifest["endpoint_state_digest"] = "f" * 64
    else:
        manifest["artifacts"][3][mutation] = "f" * 64 if mutation == "sha256" else 0
    (tmp_path / "completion.json").write_bytes(framed(manifest, "completion.json"))
    rejected(tmp_path, "ARTIFACT_MISMATCH", ("journal.jsonl",) if mutation == "oversize_journal" else ("endpoint.json",))


def test_valid_p3_manifest_widens_only_journal_capture(tmp_path, monkeypatch):
    from src.core.backtest.spider_historical_input import historical_context_for_input
    from test_spider_scenario_run import historical_input

    historical, _ = historical_input("limit-routing")
    values = bundle(tmp_path)
    manifest = values["completion.json"]
    manifest.update(
        terminal_reason="MTM_PRESERVE_OPEN_V1",
        configuration_context=configuration_context(),
        historical_context=historical_context_for_input(historical),
        planned_coverage=[dict(ordinal=1, barrier_id="P3_MARKET_1", record_kind="HISTORICAL_MARKET_STEP")],
        processed_boundary=dict(ordinal=1, journal_seq=1, barrier_id="P3_MARKET_1"),
        persisted_boundary=dict(ordinal=1, journal_seq=1, barrier_id="P3_MARKET_1"),
    )
    manifest["artifacts"].append(dict(
        path="historical_input.json", schema_version="spider_historical_research_run_v1",
        sha256="f" * 64, byte_count=1, row_count=1,
    ))
    (tmp_path / "completion.json").write_bytes(framed(manifest, "completion.json"))
    admission._completion(manifest)

    original_capture = admission._capture
    limits = {}

    def observe_capture(directory, name, **kwargs):
        limits[name] = kwargs.get("limit", 16_777_216)
        return original_capture(directory, name, **kwargs)

    monkeypatch.setattr(admission, "_capture", observe_capture)
    admitted = admission.admit_spider_run(tmp_path)
    assert admitted == dict(decision="REJECT", reason="ARTIFACT_MISMATCH", evidence=("historical_input.json",))
    assert limits == {
        "completion.json": 16_777_216,
        "attempt.json": 16_777_216,
        "status.json": 16_777_216,
        "journal.jsonl": 33_554_432,
        "endpoint.json": 16_777_216,
        "reconciliation.json": 16_777_216,
        "report.jsonl": 16_777_216,
        "historical_input.json": 16_777_216,
    }


def test_invalid_historical_manifest_rejects_before_artifact_capture(tmp_path, monkeypatch):
    values = bundle(tmp_path)
    values["completion.json"]["historical_context"] = []
    (tmp_path / "completion.json").write_bytes(framed(values["completion.json"], "completion.json"))
    original_capture = admission._capture
    captures = []

    def observe_capture(directory, name, **kwargs):
        captures.append((name, kwargs.get("limit", 16_777_216)))
        return original_capture(directory, name, **kwargs)

    monkeypatch.setattr(admission, "_capture", observe_capture)
    rejected(tmp_path, "INVALID_MANIFEST", ("completion.json",))
    assert captures == [("completion.json", 16_777_216)]


def test_capture_honors_explicit_limit(tmp_path):
    path = tmp_path / "journal.jsonl"
    path.write_bytes(b"12345")
    directory = os.open(tmp_path, os.O_RDONLY)
    try:
        assert admission._capture(directory, path.name, limit=5) == b"12345"
        with pytest.raises(admission._Oversize):
            admission._capture(directory, path.name, limit=4)
    finally:
        os.close(directory)


@pytest.mark.parametrize("mutation,reason,tokens", [
    ("run", "RUN_ID_MISMATCH", ("report.jsonl",)),
    ("schema", "UNSUPPORTED_SCHEMA", ("endpoint.json",)),
    ("nested", "UNSUPPORTED_SCHEMA", ("endpoint.json",)),
    ("frontier", "INCOMPLETE_PERSISTENCE", FRONTIER),
    ("selector", "ENDPOINT_RECONCILIATION_FAILED", NAMES),
    ("plan_hash", "ENDPOINT_RECONCILIATION_FAILED", NAMES),
    ("policy", "ENDPOINT_RECONCILIATION_FAILED", NAMES),
    ("input_hash", "ENDPOINT_RECONCILIATION_FAILED", NAMES),
    ("completion_hash", "ENDPOINT_RECONCILIATION_FAILED", NAMES),
    ("terminal", "ENDPOINT_RECONCILIATION_FAILED", NAMES),
    ("supplied", "ENDPOINT_RECONCILIATION_FAILED", NAMES),
    ("money", "ENDPOINT_RECONCILIATION_FAILED", NAMES),
    ("projection", "ENDPOINT_RECONCILIATION_FAILED", NAMES),
])
def test_rehashed_causal_mutations(tmp_path, mutation, reason, tokens):
    values = bundle(tmp_path)
    attempt, manifest = values["attempt.json"], values["completion.json"]
    if mutation == "run":
        values["report.jsonl"][1]["run_id"] = "different"
    elif mutation == "schema":
        values["endpoint.json"]["schema_version"] = "future"
    elif mutation == "nested":
        values["endpoint.json"]["final_owner_evidence"]["inspection"]["cash"] = "1.0"
    elif mutation == "frontier":
        values["status.json"]["processed_boundary"]["ordinal"] = 4
    elif mutation == "selector":
        attempt["requested_scenario_selector"] = "unknown"
    elif mutation == "plan_hash":
        attempt["scenario_plan_sha256"] = attempt["input_contract_hashes"][0]["sha256"] = "f" * 64
        manifest["input_contract_hashes"] = deepcopy(attempt["input_contract_hashes"])
    elif mutation == "policy":
        attempt["policy_source_sha256"] = attempt["input_contract_hashes"][3]["sha256"] = "f" * 64
        manifest["input_contract_hashes"] = deepcopy(attempt["input_contract_hashes"])
    elif mutation == "input_hash":
        attempt["input_contract_hashes"][1]["sha256"] = "f" * 64
    elif mutation == "completion_hash":
        manifest["input_contract_hashes"][1]["sha256"] = "f" * 64
    elif mutation == "terminal":
        manifest["terminal_reason"] = "O03_NON_ATOMIC_COMPLETE"
    elif mutation == "supplied":
        values["reconciliation.json"]["checks"][10]["observed"]["report_sha256"] = "f" * 64
    elif mutation == "money":
        values["endpoint.json"]["final_owner_evidence"]["inspection"]["cash"] = "999"
    else:
        del values["endpoint.json"]["final_owner_evidence"]["trading_fact"]["captured_account_version"]
    write_bundle(tmp_path, values)
    rejected(tmp_path, reason, tokens)


@pytest.mark.parametrize("mutation", ["middle-missing", "middle-hash", "middle-money"])
def test_configured_report_schema_failures_keep_unsupported_schema(tmp_path, mutation):
    values = bundle(tmp_path)
    attempt, report = values["attempt.json"], values["report.jsonl"]
    context = configuration_context(products=[row["product_id"] for row in report])
    attempt.update(run_contract_id="SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1",
                   profile_id="SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1",
                   funding_exclusion="SYNTHETIC_P2_NO_FUNDING_INPUT_OR_CLAIM",
                   configuration_context=deepcopy(context))
    for row in report:
        row["configuration_context"] = deepcopy(context)
    if mutation == "middle-missing":
        del report[1]["configuration_context"]
    elif mutation == "middle-hash":
        report[1]["configuration_context"]["configuration_sha256"] = "b" * 64
    else:
        report[1]["account_cash"] = "1.0"
    write_bundle(tmp_path, values)
    rejected(tmp_path, "UNSUPPORTED_SCHEMA", ("report.jsonl",))


@pytest.mark.parametrize("site", ["_decode_jsonl", "_completion", "_report", "_plan_bundle", "_build"])
@pytest.mark.parametrize("error", [ValueError("programming"), RuntimeError("programming"), AssertionError("programming")])
def test_programming_errors_propagate_and_directory_closes(tmp_path, monkeypatch, site, error):
    bundle(tmp_path)
    descriptors = []
    original = os.open
    def opened(*args, **kwargs):
        descriptor = original(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor
    def broken(*args):
        raise error
    monkeypatch.setattr(admission._os, "open", opened)
    monkeypatch.setattr(admission, site, broken)
    with pytest.raises(type(error), match="programming"):
        admission.admit_spider_run(tmp_path)
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_import_boundary():
    tree = ast.parse(Path(admission.__file__).read_text())
    assert {node.name for item in ast.walk(tree) if isinstance(item, ast.Import) for node in item.names} == {"os", "re", "stat"}
    assert {item.module for item in ast.walk(tree) if isinstance(item, ast.ImportFrom)} <= {
        "hashlib", "json", "typing", "src.core.backtest.spider_run_artifacts", "src.core.backtest.spider_run_completion_schema",
        "src.core.backtest.spider_historical_input",
        "src.core.backtest.spider_run_envelope_schema", "src.core.backtest.spider_run_evidence",
        "src.core.backtest.spider_run_reconciliation_schema", "src.core.backtest.spider_scenario_plans"}
