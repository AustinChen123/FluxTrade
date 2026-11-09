"""Independent bundle framing and causal shared-admission boundaries."""

import ast
import os
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_admission as admission
from src.core.backtest.spider_artifact_limits import DEFAULT_LIMIT, LEG32
from src.core.backtest.spider_run_artifacts import canonical_bytes
from test_spider_run_artifacts import configuration_context
from test_spider_run_evidence import RUN, expected_checks, fixture

NAMES = ("attempt.json", "status.json", "journal.jsonl", "endpoint.json", "reconciliation.json", "report.jsonl", "completion.json")
SCHEMAS = ("spider_attempt_v1", "spider_status_v1", "spider_journal_record_v1", "spider_endpoint_v1", "spider_reconciliation_v1", "spider_product_report_v1")
DIAGNOSTIC = ("completion.json", "status.json", "journal.jsonl")
FRONTIER = ("attempt.json", "status.json", "journal.jsonl", "completion.json")

# Outcomes were run against a HEAD export at 5ec93e46; J1 changes only the
# P3 journal over-limit reason in the single oversize-only case.
_HEAD_FAILURES = {
    **{case: ("MISSING_COMPLETE_MANIFEST", DIAGNOSTIC) for case in (
        "status_oserror", "journal_oserror", "status_oversize", "invalid_journal",
        "wrong_status_schema", "paid_status_validation",
        *(f"hostile_{kind}_{journal}" for kind in ("deep", "integer")
          for journal in ("missing", "symlink", "oversize")),
        "hostile_16_32_deep", "hostile_16_32_integer",
    )},
    "completion_oserror": ("INVALID_MANIFEST", ("completion.json",)),
    "completion_oversize": ("ARTIFACT_MISMATCH", ("completion.json",)),
    "unknown_family": ("INVALID_MANIFEST", ("completion.json",)),
    "unknown_encoding": ("INVALID_MANIFEST", ("completion.json",)),
    "artifact_oserror": ("ARTIFACT_MISMATCH", ("endpoint.json",)),
    "artifact_oversize": ("ARTIFACT_MISMATCH", ("endpoint.json",)),
    "artifact_unsafe": ("UNSAFE_ARTIFACT_PATH", ("endpoint.json",)),
    "p1_journal_oversize_bad_endpoint": ("ARTIFACT_MISMATCH", ("journal.jsonl", "endpoint.json")),
    "p3_journal_missing": ("ARTIFACT_MISMATCH", ("journal.jsonl",)),
    "p3_journal_unreadable": ("ARTIFACT_MISMATCH", ("journal.jsonl",)),
    "p3_journal_oversize_only": ("ARTIFACT_MISMATCH", ("journal.jsonl",)),
    "p3_journal_oversize_bad_endpoint": ("ARTIFACT_MISMATCH", ("journal.jsonl", "endpoint.json")),
}


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
    v2_dispatch = admission._admit_p3_v2
    v2_calls = []
    monkeypatch.setattr(admission, "_admit_p3_v2", lambda *args: v2_calls.append(args))
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
    assert v2_calls == []
    monkeypatch.setattr(admission, "_admit_p3_v2", v2_dispatch)
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
    assert opened == [str(tmp_path), "attempt.json", "status.json", "journal.jsonl"]


@pytest.mark.parametrize(("shape", "primary", "matched", "expected"), [
    ("E1", {"kind": "PERSISTENCE", "reason": "ARTIFACT_LIMIT_EXCEEDED"}, True, "ARTIFACT_LIMIT_EXCEEDED"),
    ("E5", {"kind": "PERSISTENCE", "reason": "ARTIFACT_LIMIT_EXCEEDED"}, True, "ARTIFACT_LIMIT_EXCEEDED"),
    ("E1", {"kind": "PERSISTENCE", "reason": "ARTIFACT_WRITE_FAILED"}, True, "INCOMPLETE_PERSISTENCE"),
    ("E5", {"kind": "PERSISTENCE", "reason": "ARTIFACT_WRITE_FAILED"}, True, "MISSING_COMPLETE_MANIFEST"),
    ("E1", {"kind": "PERSISTENCE", "reason": "ARTIFACT_LIMIT_EXCEEDED"}, False, "MISSING_COMPLETE_MANIFEST"),
    ("RUNNING_LIMIT_PRIMARY", {"kind": "PERSISTENCE", "reason": "ARTIFACT_LIMIT_EXCEEDED"}, True, "MISSING_COMPLETE_MANIFEST"),
], ids=["legacy-E1-limit", "legacy-E5-limit", "legacy-E1-other-primary",
       "legacy-E5-other-primary", "legacy-E1-unmatched", "legacy-RUNNING-limit-primary"])
def test_diagnose_routes_matching_legacy_limit_primary(tmp_path, shape, primary, matched, expected):
    values = bundle(tmp_path)
    status = values["status.json"]
    rows = values["journal.jsonl"]
    frontier = (dict(ordinal=len(rows), journal_seq=len(rows), barrier_id=rows[-1]["barrier_id"])
                if rows else None)
    status.update(
        state="RUNNING" if shape.startswith("RUNNING") else "FAILED",
        failure_reason=None if shape.startswith("RUNNING") else "PERSISTENCE_FAILED",
        primary_failure=None if shape == "RUNNING" else primary,
        processed_boundary=(dict(ordinal=len(rows) + 1, journal_seq=len(rows) + 1, barrier_id="processed")
                            if shape == "E1" else frontier),
        persisted_boundary=frontier,
    )
    if not matched and rows:
        rows[-1]["run_id"] = "other-run"
    (tmp_path / "status.json").write_bytes(framed(status, "status.json"))
    (tmp_path / "journal.jsonl").write_bytes(framed(rows, "journal.jsonl"))
    (tmp_path / "completion.json").unlink()
    assert (tmp_path / "status.json").stat().st_size < DEFAULT_LIMIT
    assert (tmp_path / "journal.jsonl").stat().st_size < DEFAULT_LIMIT
    directory = os.open(tmp_path, os.O_RDONLY)
    try:
        result = admission._diagnose(directory)
    finally:
        os.close(directory)
    assert result == dict(decision="REJECT", reason=expected, evidence=DIAGNOSTIC)


def test_limit_refusal_requires_failed_state_and_exact_primary():
    base = dict(state="FAILED", failure_reason="PERSISTENCE_FAILED",
                primary_failure={"kind": "PERSISTENCE", "reason": "ARTIFACT_LIMIT_EXCEEDED"})
    assert admission._limit_refusal(base)
    assert not admission._limit_refusal({**base, "state": "RUNNING"})
    assert not admission._limit_refusal({**base, "primary_failure": {
        "kind": "PERSISTENCE", "reason": "ARTIFACT_LIMIT_EXCEEDED", "extra": 1,
    }})


@pytest.mark.parametrize("mutation", ["bytes", "sha256", "byte_count", "row_count", "semantic_digest", "oversize", "oversize_journal"])
def test_actual_file_metadata_is_not_trusted(tmp_path, mutation):
    values = bundle(tmp_path)
    manifest = values["completion.json"]
    if mutation == "bytes":
        (tmp_path / "endpoint.json").write_bytes(b"{}\n")
    elif mutation == "oversize":
        (tmp_path / "endpoint.json").write_bytes(b" " * (DEFAULT_LIMIT + 1))
    elif mutation == "oversize_journal":
        (tmp_path / "journal.jsonl").write_bytes(b" " * (DEFAULT_LIMIT + 1))
    elif mutation == "semantic_digest":
        manifest["endpoint_state_digest"] = "f" * 64
    else:
        manifest["artifacts"][3][mutation] = "f" * 64 if mutation == "sha256" else 0
    (tmp_path / "completion.json").write_bytes(framed(manifest, "completion.json"))
    rejected(tmp_path, "ARTIFACT_MISMATCH", ("journal.jsonl",) if mutation == "oversize_journal" else ("endpoint.json",))


def _p3_manifest_for_admission(tmp_path, *, include_input=False):
    from src.core.backtest.spider_historical_input import historical_context_for_input
    from test_spider_scenario_run import historical_input

    historical, encoded = historical_input("limit-routing")
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
    if include_input:
        (tmp_path / "historical_input.json").write_bytes(encoded)
        manifest["artifacts"][-1].update(sha256=sha256(encoded).hexdigest(), byte_count=len(encoded))
    (tmp_path / "completion.json").write_bytes(framed(manifest, "completion.json"))
    admission._completion(manifest)
    return manifest


@pytest.mark.parametrize("unsafe_kind", [None, "hardlink", "symlink", "directory"])
def test_p3_oversize_keeps_unsafe_precedence(tmp_path, unsafe_kind):
    _p3_manifest_for_admission(tmp_path)
    source = tmp_path / "input-source.json"
    source.write_bytes(b"{}")
    historical = tmp_path / "historical_input.json"
    if unsafe_kind == "hardlink":
        os.link(source, historical)
    elif unsafe_kind == "symlink":
        historical.symlink_to(source)
    elif unsafe_kind == "directory":
        historical.mkdir()
    with (tmp_path / "journal.jsonl").open("wb") as stream:
        stream.truncate(LEG32 + 1)
    result = admission.admit_spider_run(tmp_path)
    if unsafe_kind is None:
        assert result == dict(decision="REJECT", reason="ARTIFACT_MISMATCH", evidence=("journal.jsonl", "historical_input.json"))
    else:
        assert result == dict(decision="REJECT", reason="UNSAFE_ARTIFACT_PATH", evidence=("historical_input.json",))


@pytest.mark.parametrize("case", tuple(_HEAD_FAILURES))
def test_public_admission_matches_head_failure_matrix(tmp_path, monkeypatch, case):
    values = bundle(tmp_path)
    target = {
        "status_oserror": "status.json", "journal_oserror": "journal.jsonl",
        "completion_oserror": "completion.json", "artifact_oserror": "endpoint.json",
        "p3_journal_unreadable": "journal.jsonl",
    }.get(case)
    if (case.startswith("hostile_") and not case.startswith("hostile_16_32_")) or case in {
        "status_oserror", "journal_oserror", "status_oversize", "invalid_journal",
        "wrong_status_schema", "paid_status_validation",
    }:
        (tmp_path / "completion.json").unlink()
    if case.startswith("hostile_"):
        if case.startswith("hostile_16_32_"):
            kind = case.removeprefix("hostile_16_32_")
            (tmp_path / "completion.json").unlink()
            status = values["status.json"]
            (tmp_path / "status.json").write_bytes(framed(status, "status.json"))
            hostile = (b'{"nested":' + b"[" * 2000 + b"0" + b"]" * 2000 + b"}\n"
                       if kind.endswith("deep") else b'{"n":' + b"1" * 4301 + b"}\n")
            (tmp_path / "journal.jsonl").write_bytes(
                b" " * (DEFAULT_LIMIT + 1 - len(hostile)) + hostile,
            )
        else:
            kind, journal_state = case.removeprefix("hostile_").split("_", 1)
            status = (b'{"nested":' + b"[" * 2000 + b"0" + b"]" * 2000 + b"}\n"
                      if kind == "deep" else b'{"n":' + b"1" * 4301 + b"}\n")
            (tmp_path / "status.json").write_bytes(status)
            journal_path = tmp_path / "journal.jsonl"
            if journal_state == "missing":
                journal_path.unlink()
            elif journal_state == "symlink":
                journal_path.unlink()
                journal_path.symlink_to(tmp_path / "missing")
            else:
                journal_path.write_bytes(b"x" * (DEFAULT_LIMIT + 1))
    elif case == "status_oversize":
        (tmp_path / "status.json").write_bytes(b"x" * (DEFAULT_LIMIT + 1))
    elif case == "invalid_journal":
        (tmp_path / "journal.jsonl").write_bytes(b"{invalid}\n")
    elif case == "wrong_status_schema":
        (tmp_path / "status.json").write_bytes(framed({"schema_version": "unknown_status_v9"}, "status.json"))
    elif case == "paid_status_validation":
        (tmp_path / "status.json").write_bytes(framed(
            {"schema_version": admission._EXECUTION_SCHEMA["status.json"]}, "status.json",
        ))
    elif case == "completion_oversize":
        (tmp_path / "completion.json").write_bytes(b"x" * (DEFAULT_LIMIT + 1))
    elif case in {"unknown_family", "unknown_encoding"}:
        manifest = values["completion.json"]
        if case == "unknown_family":
            manifest["schema_version"] = "unknown_completion_v9"
        write_bundle(tmp_path, values)
        if case == "unknown_encoding":
            values["completion.json"]["artifacts"][2]["schema_version"] = "unknown_journal_v9"
            (tmp_path / "completion.json").write_bytes(framed(values["completion.json"], "completion.json"))
    elif case == "artifact_oversize":
        (tmp_path / "endpoint.json").write_bytes(b"x" * (DEFAULT_LIMIT + 1))
    elif case == "artifact_unsafe":
        endpoint = tmp_path / "endpoint.json"
        endpoint.unlink()
        endpoint.symlink_to(tmp_path / "missing")
    elif case.startswith("p3_journal_"):
        _p3_manifest_for_admission(tmp_path, include_input=True)
        journal_path = tmp_path / "journal.jsonl"
        if case == "p3_journal_missing":
            journal_path.unlink()
        elif case in {"p3_journal_oversize_only", "p3_journal_oversize_bad_endpoint"}:
            with journal_path.open("wb") as stream:
                stream.truncate(LEG32 + 1)
            if case.endswith("bad_endpoint"):
                (tmp_path / "endpoint.json").write_bytes(b"{}\n")
    elif case == "p1_journal_oversize_bad_endpoint":
        with (tmp_path / "journal.jsonl").open("wb") as stream:
            stream.truncate(DEFAULT_LIMIT + 1)
        (tmp_path / "endpoint.json").write_bytes(b"{}\n")

    capture, capture_names = admission._capture, []

    def observe_capture(directory, name, **kwargs):
        capture_names.append(name)
        if name == target and target is not None:
            raise PermissionError("matrix fault")
        return capture(directory, name, **kwargs)

    monkeypatch.setattr(admission, "_capture", observe_capture)
    reason, evidence = _HEAD_FAILURES[case]
    if case == "p3_journal_oversize_only" and os.environ.get("J1_HEAD_ORACLE") != "1":
        reason = "ARTIFACT_LIMIT_EXCEEDED"
    assert admission.admit_spider_run(tmp_path) == dict(decision="REJECT", reason=reason, evidence=evidence)
    if case in {"unknown_family", "unknown_encoding"}:
        assert capture_names == ["completion.json"]


@pytest.mark.parametrize("family,flags", [
    ("paid_execution", {}), ("paid_floating", {"floating_exit": True}),
    ("paid_gross", {"gross_execution": True}),
])
def test_paid_limits_route_through_production_capture(monkeypatch, family, flags):
    names = (admission._GROSS_NAMES if flags.get("gross_execution") else
             admission._FLOATING_NAMES if flags.get("floating_exit") else admission._EXECUTION_NAMES)
    calls = {}
    monkeypatch.setattr(admission, "_regular", lambda *args: None)

    def capture(directory, name, **kwargs):
        calls[name] = kwargs["limit"]
        raise admission._Oversize()

    monkeypatch.setattr(admission, "_capture", capture)
    result = admission._admit_execution(0, {}, b"{}\n", **flags)
    assert result["reason"] == "ARTIFACT_MISMATCH"
    assert calls == {
        name: LEG32 if name == "journal.jsonl" or (family == "paid_gross" and name == "report.jsonl") else DEFAULT_LIMIT
        for name in names
    }


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
        "src.core.backtest.spider_run_reconciliation_schema", "src.core.backtest.spider_scenario_plans",
        "src.core.backtest.spider_run_execution_schema", "src.core.backtest.spider_run_execution_evidence",
        "src.core.backtest.spider_artifact_limits", "src.core.backtest.spider_journal_reader_v2"}


@pytest.fixture(scope="module")
def p3_v2_admission_bundle(tmp_path_factory, real_p3_artifacts):
    from spider_run_store_v2_fixtures import p3_v2_finalize_fixture

    root = tmp_path_factory.mktemp("j10-p3-v2-admission")
    store, values, reconciliation, _ = p3_v2_finalize_fixture(root, real_p3_artifacts)
    assert store.finalize(values[3], reconciliation, values[4]) == "COMPLETE_PUBLISHED"
    return store._path


def _copy_v2_admission_bundle(source, target):
    import shutil

    destination = target / source.name
    shutil.copytree(source, destination)
    return destination


def test_admit_dispatch_accepts_v2_bundle_with_retained_attempt_bytes(tmp_path, p3_v2_admission_bundle):
    from src.core.backtest.spider_run_admission import admit_spider_run

    run_dir = _copy_v2_admission_bundle(p3_v2_admission_bundle, tmp_path)
    result = admit_spider_run(run_dir)
    assert result["decision"] == "ACCEPT", result
    assert result["artifacts"]["attempt.json"]["artifact_encoding"] == "artifact_encoding_v2"
    assert result["artifacts"]["journal.jsonl"]


def test_admit_v2_attempt_is_captured_once(tmp_path, p3_v2_admission_bundle, monkeypatch):
    from src.core.backtest import spider_run_admission as admission
    run_dir = _copy_v2_admission_bundle(p3_v2_admission_bundle, tmp_path)
    original = admission._capture
    attempt_reads = []
    def count_attempt(directory, name, **kwargs):
        if name == "attempt.json":
            attempt_reads.append(name)
        return original(directory, name, **kwargs)

    monkeypatch.setattr(admission, "_capture", count_attempt)
    result = admission.admit_spider_run(run_dir)
    assert result["decision"] == "ACCEPT", result
    assert attempt_reads == ["attempt.json"]


def test_admit_manifest_v2_rejects_v1_attempt_encoding(tmp_path, p3_v2_admission_bundle):
    from src.core.backtest.spider_run_admission import admit_spider_run

    run_dir = _copy_v2_admission_bundle(p3_v2_admission_bundle, tmp_path)
    attempt_path = run_dir / "attempt.json"
    attempt = admission._decode(attempt_path.read_bytes(), "attempt.json")
    attempt["artifact_encoding"] = "artifact_encoding_v1"
    attempt_raw = canonical_bytes(attempt) + b"\n"
    attempt_path.write_bytes(attempt_raw)
    completion_path = run_dir / "completion.json"
    manifest = admission._decode(completion_path.read_bytes(), "completion.json")
    attempt_entry = next(entry for entry in manifest["artifacts"] if entry["path"] == "attempt.json")
    attempt_entry["sha256"] = sha256(attempt_raw).hexdigest()
    attempt_entry["byte_count"] = len(attempt_raw)
    completion_path.write_bytes(canonical_bytes(manifest) + b"\n")
    result = admit_spider_run(run_dir)
    assert result == {
        "decision": "REJECT", "reason": "ARTIFACT_MISMATCH", "evidence": ("attempt.json",),
    }


def test_admit_v2_rejects_journal_symlink(tmp_path, p3_v2_admission_bundle):
    run_dir = _copy_v2_admission_bundle(p3_v2_admission_bundle, tmp_path)
    journal = run_dir / "journal.jsonl"
    outside = tmp_path / "outside-journal.jsonl"
    outside.write_bytes(journal.read_bytes())
    journal.unlink()
    journal.symlink_to(outside)
    assert admission.admit_spider_run(run_dir) == {
        "decision": "REJECT", "reason": "UNSAFE_ARTIFACT_PATH", "evidence": ("journal.jsonl",),
    }


def test_admit_v2_rejects_symlink_swapped_after_regular_check(
    tmp_path, p3_v2_admission_bundle, monkeypatch,
):
    run_dir = _copy_v2_admission_bundle(p3_v2_admission_bundle, tmp_path)
    journal = run_dir / "journal.jsonl"
    outside = tmp_path / "outside-journal.jsonl"
    outside.write_bytes(journal.read_bytes())
    original_regular = admission._regular
    swapped = []

    def regular_then_swap(directory, name):
        original_regular(directory, name)
        if name == "journal.jsonl" and not swapped:
            journal.unlink()
            journal.symlink_to(outside)
            swapped.append(name)

    monkeypatch.setattr(admission, "_regular", regular_then_swap)
    result = admission.admit_spider_run(run_dir)
    assert swapped == ["journal.jsonl"]
    assert result["decision"] == "REJECT"
    assert (result["reason"], result["evidence"]) in {
        ("UNSAFE_ARTIFACT_PATH", ("journal.jsonl",)),
        ("ARTIFACT_MISMATCH", ("journal.jsonl",)),
    }


def test_admit_v2_rejects_fifo_journal_without_blocking(tmp_path, p3_v2_admission_bundle):
    run_dir = _copy_v2_admission_bundle(p3_v2_admission_bundle, tmp_path)
    journal = run_dir / "journal.jsonl"
    journal.unlink()
    os.mkfifo(journal)
    assert admission.admit_spider_run(run_dir) == {
        "decision": "REJECT", "reason": "UNSAFE_ARTIFACT_PATH", "evidence": ("journal.jsonl",),
    }


@pytest.fixture(scope="module")
def p3_v2_diagnosis_bundle(p3_v2_admission_bundle):
    return p3_v2_admission_bundle


# Independent expected rows from JOURNAL_AB_DESIGN_v3 T1.4/T3.3 and J9's
# diagnostic classification contract; do not derive these from the reader.
_V2_DIAGNOSIS_EXPECTED = {
    "E1-limit": "ARTIFACT_LIMIT_EXCEEDED",
    "E5-limit": "ARTIFACT_LIMIT_EXCEEDED",
    "E1-other-primary": "INCOMPLETE_PERSISTENCE",
    "RUNNING-frontier": "INCOMPLETE_PERSISTENCE",
    "journal-unmatched": "MISSING_COMPLETE_MANIFEST",
}


def _prepare_v2_diagnosis(source, target, cell):
    run_dir = _copy_v2_admission_bundle(source, target)
    (run_dir / "completion.json").unlink()
    status = admission._decode((run_dir / "status.json").read_bytes(), "status.json")
    rows = admission._decode((run_dir / "journal.jsonl").read_bytes(), "journal.jsonl")
    frontier = (
        dict(ordinal=len(rows), journal_seq=len(rows), barrier_id=rows[-1]["barrier_id"])
        if rows else None
    )
    if cell == "journal-unmatched":
        rows[-1]["run_id"] = "another-valid-run"
        (run_dir / "journal.jsonl").write_bytes(framed(rows, "journal.jsonl"))
    running = cell == "RUNNING-frontier"
    status.update(
        state="RUNNING" if running else "FAILED",
        failure_reason=None if running else "PERSISTENCE_FAILED",
        primary_failure=(
            None if running else
            {"kind": "PERSISTENCE", "reason": "ARTIFACT_LIMIT_EXCEEDED"}
            if cell in ("E1-limit", "E5-limit") else
            {"kind": "PERSISTENCE", "reason": "ARTIFACT_WRITE_FAILED"}
        ),
        persisted_boundary=frontier,
        processed_boundary=(
            dict(ordinal=len(rows) + 1, journal_seq=len(rows) + 1, barrier_id="unpersisted")
            if cell in ("E1-limit", "E1-other-primary", "RUNNING-frontier") else frontier
        ),
    )
    (run_dir / "status.json").write_bytes(framed(status, "status.json"))
    return run_dir


@pytest.mark.parametrize(
    "cell", tuple(_V2_DIAGNOSIS_EXPECTED), ids=tuple(_V2_DIAGNOSIS_EXPECTED),
)
def test_v2_diagnosis_decision_cells(tmp_path, p3_v2_diagnosis_bundle, cell):
    run_dir = _prepare_v2_diagnosis(p3_v2_diagnosis_bundle, tmp_path, cell)
    directory = os.open(run_dir, os.O_RDONLY)
    try:
        result = admission._diagnose(directory)
    finally:
        os.close(directory)
    assert result == dict(
        decision="REJECT", reason=_V2_DIAGNOSIS_EXPECTED[cell], evidence=DIAGNOSTIC,
    )


def test_v2_diagnosis_v1_attempt_uses_existing_path(tmp_path, p3_v2_diagnosis_bundle, monkeypatch):
    run_dir = _prepare_v2_diagnosis(p3_v2_diagnosis_bundle, tmp_path, "E1-limit")
    attempt = admission._decode((run_dir / "attempt.json").read_bytes(), "attempt.json")
    attempt["artifact_encoding"] = "artifact_encoding_v1"
    (run_dir / "attempt.json").write_bytes(framed(attempt, "attempt.json"))
    calls = []
    monkeypatch.setattr(admission, "_diagnose_p3_v2", lambda *args: calls.append(args))
    directory = os.open(run_dir, os.O_RDONLY)
    try:
        result = admission._diagnose(directory)
    finally:
        os.close(directory)
    assert calls == []
    assert result["reason"] == "MISSING_COMPLETE_MANIFEST"


@pytest.mark.parametrize("attempt_kind", ["symlink", "oversize"], ids=["unreadable", "oversize"])
def test_v2_diagnosis_attempt_capture_failure_uses_existing_result(
    tmp_path, p3_v2_diagnosis_bundle, attempt_kind,
):
    run_dir = _prepare_v2_diagnosis(p3_v2_diagnosis_bundle, tmp_path, "E1-limit")
    attempt_path = run_dir / "attempt.json"
    if attempt_kind == "symlink":
        attempt_path.unlink()
        attempt_path.symlink_to(tmp_path / "outside-missing-attempt")
    else:
        attempt_path.write_bytes(b"x" * (DEFAULT_LIMIT + 1))
    directory = os.open(run_dir, os.O_RDONLY)
    try:
        result = admission._diagnose(directory)
    finally:
        os.close(directory)
    assert result["reason"] == "MISSING_COMPLETE_MANIFEST"


def test_v2_diagnosis_never_follows_journal_symlink(tmp_path, p3_v2_diagnosis_bundle):
    run_dir = _prepare_v2_diagnosis(p3_v2_diagnosis_bundle, tmp_path, "E1-limit")
    journal = run_dir / "journal.jsonl"
    outside = tmp_path / "outside-journal.jsonl"
    outside.write_bytes(journal.read_bytes())
    journal.unlink()
    journal.symlink_to(outside)
    directory = os.open(run_dir, os.O_RDONLY)
    try:
        result = admission._diagnose(directory)
    finally:
        os.close(directory)
    assert result == dict(
        decision="REJECT", reason="MISSING_COMPLETE_MANIFEST", evidence=DIAGNOSTIC,
    )


def test_v2_diagnosis_reader_receives_directory_fd_and_leaf(
    tmp_path, p3_v2_diagnosis_bundle, monkeypatch,
):
    run_dir = _prepare_v2_diagnosis(p3_v2_diagnosis_bundle, tmp_path, "E1-limit")
    original = admission._read_p3_v2_journal_diagnosis_at
    calls = []

    def capture_arguments(directory, name, **kwargs):
        calls.append((directory, name))
        return original(directory, name, **kwargs)

    monkeypatch.setattr(admission, "_read_p3_v2_journal_diagnosis_at", capture_arguments)
    directory = os.open(run_dir, os.O_RDONLY)
    try:
        result = admission._diagnose(directory)
    finally:
        os.close(directory)
    assert result["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
    assert calls == [(directory, "journal.jsonl")]


@pytest.fixture(scope="module")
def p3_v2_admission_values(p3_v2_admission_bundle):
    result = admission.admit_spider_run(p3_v2_admission_bundle)
    assert result["decision"] == "ACCEPT", result
    return result["artifacts"]


# Independent expected outcomes from JOURNAL_AB_DESIGN_v3 T1.1/T1.4 and the
# existing S3 manifest-binding contract; expected values are not computed by
# admission or the reader under test.
_J10_ITEM3_EXPECTED = {
    "attempt-swap-retained": ("ACCEPT", None),
    "mixed-v1-manifest-v2-journal": ("REJECT", "UNSUPPORTED_SCHEMA"),
    "mixed-v2-manifest-v1-journal": ("REJECT", "ARTIFACT_MISMATCH"),
    "v2-attempt-no-v2-artifacts": ("REJECT", "UNSUPPORTED_SCHEMA"),
    "journal-sha256-mismatch": ("REJECT", "ARTIFACT_MISMATCH"),
    "journal-byte-count-mismatch": ("REJECT", "ARTIFACT_MISMATCH"),
    "journal-row-count-mismatch": ("REJECT", "ARTIFACT_MISMATCH"),
    "journal-reader-schema-version-mismatch": ("REJECT", "ARTIFACT_MISMATCH"),
    "attempt-sha256-mismatch": ("REJECT", "ARTIFACT_MISMATCH"),
    "endpoint-digest-mismatch": ("REJECT", "ARTIFACT_MISMATCH"),
    "reconciliation-digest-mismatch": ("REJECT", "ARTIFACT_MISMATCH"),
}


def _rewrite_v2_manifest(run_dir, change):
    path = run_dir / "completion.json"
    manifest = admission._decode(path.read_bytes(), "completion.json")
    change(manifest)
    path.write_bytes(framed(manifest, "completion.json"))
    return manifest


def _journal_entry(manifest):
    return next(entry for entry in manifest["artifacts"] if entry["path"] == "journal.jsonl")


def _make_j10_item3_cell(run_dir, cell, p3_v2_admission_values):
    if cell == "attempt-swap-retained":
        return
    if cell == "mixed-v1-manifest-v2-journal":
        attempt_path = run_dir / "attempt.json"
        attempt = admission._decode(attempt_path.read_bytes(), "attempt.json")
        attempt["artifact_encoding"] = "artifact_encoding_v1"
        attempt_raw = framed(attempt, "attempt.json")
        attempt_path.write_bytes(attempt_raw)
        def mixed_manifest(manifest):
            _journal_entry(manifest)["schema_version"] = "spider_journal_record_v1"
            attempt_entry = next(item for item in manifest["artifacts"] if item["path"] == "attempt.json")
            attempt_entry.update(sha256=sha256(attempt_raw).hexdigest(), byte_count=len(attempt_raw))
        _rewrite_v2_manifest(run_dir, mixed_manifest)
    elif cell in ("mixed-v2-manifest-v1-journal", "v2-attempt-no-v2-artifacts"):
        if cell == "v2-attempt-no-v2-artifacts":
            _rewrite_v2_manifest(
                run_dir, lambda manifest: _journal_entry(manifest).__setitem__(
                    "schema_version", "spider_journal_record_v1",
                ),
            )
        raw = framed(p3_v2_admission_values["journal.jsonl"], "journal.jsonl")
        (run_dir / "journal.jsonl").write_bytes(raw)
        def metadata(manifest):
            entry = _journal_entry(manifest)
            entry.update(sha256=sha256(raw).hexdigest(), byte_count=len(raw),
                         row_count=raw.count(b"\n"))
        _rewrite_v2_manifest(run_dir, metadata)
    elif cell in (
        "journal-sha256-mismatch", "journal-byte-count-mismatch",
        "journal-row-count-mismatch",
    ):
        field = {
            "journal-sha256-mismatch": "sha256",
            "journal-byte-count-mismatch": "byte_count",
            "journal-row-count-mismatch": "row_count",
        }[cell]
        def mismatch(manifest):
            entry = _journal_entry(manifest)
            entry[field] = "f" * 64 if field == "sha256" else entry[field] + 1
        _rewrite_v2_manifest(run_dir, mismatch)
    elif cell == "attempt-sha256-mismatch":
        def mismatch(manifest):
            entry = next(item for item in manifest["artifacts"] if item["path"] == "attempt.json")
            entry["sha256"] = "f" * 64
        _rewrite_v2_manifest(run_dir, mismatch)
    elif cell in ("endpoint-digest-mismatch", "reconciliation-digest-mismatch"):
        field = "endpoint_state_digest" if cell == "endpoint-digest-mismatch" else "reconciliation_digest"
        _rewrite_v2_manifest(run_dir, lambda manifest: manifest.__setitem__(field, "f" * 64))


@pytest.mark.parametrize(
    "cell", tuple(_J10_ITEM3_EXPECTED), ids=tuple(_J10_ITEM3_EXPECTED),
)
def test_j10_item3_real_admission_matrix(
    tmp_path, p3_v2_admission_bundle, p3_v2_admission_values, cell, monkeypatch,
):
    run_dir = _copy_v2_admission_bundle(p3_v2_admission_bundle, tmp_path)
    _make_j10_item3_cell(run_dir, cell, p3_v2_admission_values)
    read_attempts = []
    original_capture = admission._capture
    def capture_once(directory, name, **kwargs):
        if name == "attempt.json":
            read_attempts.append(name)
        captured = original_capture(directory, name, **kwargs)
        if cell == "attempt-swap-retained" and name == "attempt.json":
            replacement = admission._decode(captured, "attempt.json")
            replacement["artifact_encoding"] = "artifact_encoding_v1"
            (run_dir / "attempt.json").write_bytes(framed(replacement, "attempt.json"))
        return captured

    journal_parses = []
    original_decode = admission._decode
    def observe_decode(raw, name):
        if name == "journal.jsonl":
            journal_parses.append("admission")
        return original_decode(raw, name)
    from src.core.backtest import spider_journal_reader_v2 as reader
    original_wire_decode = reader._decode_wire_row
    def observe_wire_decode(line):
        journal_parses.append("v2-reader")
        return original_wire_decode(line)
    monkeypatch.setattr(admission, "_capture", capture_once)
    monkeypatch.setattr(admission, "_decode", observe_decode)
    monkeypatch.setattr(reader, "_decode_wire_row", observe_wire_decode)
    if cell == "journal-reader-schema-version-mismatch":
        from src.core.backtest import spider_journal_buffer_v2 as journal_buffer
        monkeypatch.setattr(journal_buffer, "_V2_SCHEMA", "spider_journal_record_v1")
    v2_calls = []
    original_admit_v2 = admission._admit_p3_v2
    def observe_v2_dispatch(*args, **kwargs):
        v2_calls.append(args)
        return original_admit_v2(*args, **kwargs)
    monkeypatch.setattr(admission, "_admit_p3_v2", observe_v2_dispatch)
    result = admission.admit_spider_run(run_dir)
    expected_decision, expected_reason = _J10_ITEM3_EXPECTED[cell]
    assert result["decision"] == expected_decision
    if expected_reason is not None:
        assert result["reason"] == expected_reason
    if cell == "attempt-swap-retained":
        assert result["artifacts"]["attempt.json"]["artifact_encoding"] == "artifact_encoding_v2"
        assert (run_dir / "attempt.json").read_bytes() != framed(
            result["artifacts"]["attempt.json"], "attempt.json",
        )
        assert read_attempts == ["attempt.json"]
    if cell in {"mixed-v1-manifest-v2-journal", "v2-attempt-no-v2-artifacts"}:
        assert v2_calls == []
    else:
        assert len(v2_calls) == 1
    if cell in {
        "journal-sha256-mismatch", "journal-byte-count-mismatch",
        "journal-row-count-mismatch", "journal-reader-schema-version-mismatch",
    }:
        assert journal_parses == []
