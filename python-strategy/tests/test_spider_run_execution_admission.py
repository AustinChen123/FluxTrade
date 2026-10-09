"""Actual-native admission tests for the closed paid Spider artifact family."""

from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_admission as admission
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical, decode_jsonl, encode_jsonl
from src.core.backtest.spider_run_store import SpiderRunStore
from test_spider_run_execution_evidence import (
    _bundle, _initial_capture_failure_bundle, _liquidation_request,
)
from test_spider_run_execution_store import _candidate, _start_store
from test_spider_paid_endpoint_native import _seeded_two_product_run

_EXECUTION_NAMES = (
    "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
    "paid_endpoint.json", "report.jsonl", "reconciliation.json", "completion.json",
)


def _finalized(tmp_path: Path, *, mode: str, monkeypatch=None):
    if mode == "flat":
        artifacts, facts = _bundle()
    elif mode == "incomplete":
        request = _seeded_two_product_run()
        request = replace(request, policy_end_ms=request.policy_end_ms + 30_000)
        artifacts, facts = _bundle(request)
    elif mode == "insolvent":
        artifacts, facts = _bundle(_liquidation_request("50"))
    elif mode == "liquidated_flat":
        artifacts, facts = _bundle(_liquidation_request("99.003"))
    elif mode == "failed":
        artifacts, facts = _bundle(failure_mode="capture_failure", monkeypatch=monkeypatch)
    elif mode == "zero_journal_failed":
        artifacts, facts, _ = _initial_capture_failure_bundle(monkeypatch)
    else:
        raise AssertionError(mode)
    store, _ = _start_store(tmp_path, artifacts)
    endpoint, reconciliation, report = _candidate(store, artifacts, facts)
    assert store.finalize(endpoint, reconciliation, report) == "COMPLETE_PUBLISHED"
    return store, artifacts, facts


def _refresh_metadata(directory: Path, name: str) -> None:
    manifest_path = directory / "completion.json"
    manifest = cast(dict[str, Any], decode_jsonl(manifest_path.read_bytes())[0])
    raw = (directory / name).read_bytes()
    entry = next(row for row in manifest["artifacts"] if row["path"] == name)
    entry["byte_count"] = len(raw)
    entry["sha256"] = sha256(raw).hexdigest()
    entry["row_count"] = len(decode_jsonl(raw)) if name.endswith(".jsonl") else 1
    manifest_path.write_bytes(encode_jsonl([manifest]))


@pytest.mark.parametrize(
    ("mode", "status", "scope"),
    [
        ("flat", "PAID_FLAT", "COMPLETE_RUN"),
        ("incomplete", "INCOMPLETE", "COMPLETE_RUN"),
        ("insolvent", "LIQUIDATED_INSOLVENT", "COMPLETE_RUN"),
        ("liquidated_flat", "LIQUIDATED_FLAT", "COMPLETE_RUN"),
        ("failed", "FAILED", "FAILED_PREFIX"),
        ("zero_journal_failed", "FAILED", "FAILED_PREFIX"),
    ],
)
def test_actual_native_paid_families_admit_all_endpoint_statuses_read_only(
    tmp_path, monkeypatch, mode, status, scope,
):
    store, _, _ = _finalized(tmp_path, mode=mode, monkeypatch=monkeypatch)
    before = {name: (store._path / name).read_bytes() for name in _EXECUTION_NAMES}

    result = admission.admit_spider_run(store._path)

    assert result["decision"] == "ACCEPT"
    artifacts = result["artifacts"]
    assert tuple(artifacts) == _EXECUTION_NAMES
    assert artifacts["paid_endpoint.json"]["status"] == status
    assert artifacts["completion.json"]["endpoint_status"] == status
    assert artifacts["completion.json"]["evidence_scope"] == scope
    assert type(artifacts["execution_input.json"]) is dict
    assert {name: (store._path / name).read_bytes() for name in _EXECUTION_NAMES} == before


def test_paid_admission_is_detached_and_preserves_exact_wrapper_and_dto_bytes(tmp_path):
    store, source, _ = _finalized(tmp_path, mode="flat")
    result = admission.admit_spider_run(store._path)
    result["artifacts"]["attempt.json"]["run_id"] = "detached-change"
    result["artifacts"]["paid_endpoint.json"]["status"] = "FAILED"

    fresh = admission.admit_spider_run(store._path)

    assert fresh["decision"] == "ACCEPT"
    assert fresh["artifacts"]["attempt.json"]["run_id"] != "detached-change"
    assert (store._path / "execution_input.json").read_bytes() == source["execution_input.json"]
    assert (store._path / "paid_endpoint.json").read_bytes() == source["paid_endpoint.json"]


@pytest.mark.parametrize(
    ("mutation", "reason", "evidence"),
    [
        ("bytes", "ARTIFACT_MISMATCH", ("paid_endpoint.json",)),
        ("schema", "UNSUPPORTED_SCHEMA", ("attempt.json",)),
        ("wrapper", "UNSUPPORTED_SCHEMA", ("execution_input.json",)),
        ("run_id", "RUN_ID_MISMATCH", ("attempt.json",)),
        ("predicate", "ENDPOINT_RECONCILIATION_FAILED", _EXECUTION_NAMES),
        ("frontier", "INCOMPLETE_PERSISTENCE", admission._FRONTIER),
    ],
)
def test_paid_admission_rejects_single_artifact_mutations_before_accept(
    tmp_path, mutation, reason, evidence,
):
    store, _, _ = _finalized(tmp_path, mode="flat")
    directory = store._path
    if mutation == "bytes":
        endpoint_path = directory / "paid_endpoint.json"
        endpoint_path.write_bytes(endpoint_path.read_bytes() + b" ")
    elif mutation == "schema":
        attempt_path = directory / "attempt.json"
        attempt = decode_jsonl(attempt_path.read_bytes())[0]
        attempt["schema_version"] = "unknown_paid_attempt_v9"
        attempt_path.write_bytes(encode_jsonl([attempt]))
        _refresh_metadata(directory, "attempt.json")
    elif mutation == "wrapper":
        wrapper_path = directory / "execution_input.json"
        wrapper_path.write_bytes(wrapper_path.read_bytes() + b"\n")
        _refresh_metadata(directory, "execution_input.json")
    elif mutation == "run_id":
        attempt_path = directory / "attempt.json"
        attempt = decode_jsonl(attempt_path.read_bytes())[0]
        attempt["run_id"] = "different-run"
        attempt_path.write_bytes(encode_jsonl([attempt]))
        _refresh_metadata(directory, "attempt.json")
    elif mutation == "predicate":
        report_path = directory / "report.jsonl"
        report = decode_jsonl(report_path.read_bytes())[0]
        report["source_evidence_refs"] = []
        report_path.write_bytes(encode_jsonl([report]))
        _refresh_metadata(directory, "report.jsonl")
    else:
        status_path = directory / "status.json"
        status = cast(dict[str, Any], decode_jsonl(status_path.read_bytes())[0])
        boundary = cast(dict[str, int], status["processed_boundary"])
        boundary["ordinal"] += 1
        boundary["journal_seq"] += 1
        status_path.write_bytes(encode_jsonl([status]))
        _refresh_metadata(directory, "status.json")

    assert admission.admit_spider_run(directory) == dict(
        decision="REJECT", reason=reason, evidence=evidence,
    )


@pytest.mark.parametrize("mutation", ["generation", "schema", "missing", "nested_type"])
def test_canonical_invalid_execution_wrapper_is_unsupported_schema(tmp_path, mutation):
    store, _, _ = _finalized(tmp_path, mode="flat")
    wrapper_path = store._path / "execution_input.json"
    wrapper = cast(dict[str, Any], decode_canonical(wrapper_path.read_bytes()))
    if mutation == "generation":
        wrapper["generation"] = 1
    elif mutation == "schema":
        wrapper["schema_version"] = "spider_execution_input_v2"
    elif mutation == "missing":
        del wrapper["generation"]
    else:
        wrapper["historical_input"] = []
    wrapper_path.write_bytes(canonical_bytes(wrapper))
    _refresh_metadata(store._path, "execution_input.json")

    assert admission.admit_spider_run(store._path) == dict(
        decision="REJECT", reason="UNSUPPORTED_SCHEMA", evidence=("execution_input.json",),
    )


@pytest.mark.parametrize("error", [ValueError("unexpected internal error"), RuntimeError("internal error")])
def test_unexpected_execution_context_faults_propagate_after_descriptor_close(
    tmp_path, monkeypatch, error,
):
    store, _, _ = _finalized(tmp_path, mode="flat")
    descriptors = []
    original_open = admission._os.open

    def tracked_open(*args, **kwargs):
        descriptor = original_open(*args, **kwargs)
        descriptors.append(descriptor)
        return descriptor

    monkeypatch.setattr(admission._os, "open", tracked_open)
    monkeypatch.setattr(admission, "_execution_context_from_input", lambda _raw: (_ for _ in ()).throw(error))

    with pytest.raises(type(error), match=str(error)):
        admission.admit_spider_run(store._path)
    for descriptor in descriptors:
        with pytest.raises(OSError):
            admission._os.fstat(descriptor)


def test_paid_completion_manifest_is_read_first_and_closed(tmp_path):
    store, _, _ = _finalized(tmp_path, mode="flat")
    completion = store._path / "completion.json"
    completion.write_bytes(b"{}\n")
    report = store._path / "report.jsonl"
    report.unlink()
    report.mkdir()

    assert admission.admit_spider_run(store._path) == dict(
        decision="REJECT", reason="INVALID_MANIFEST", evidence=("completion.json",),
    )


@pytest.mark.parametrize("leaf_kind", ["symlink", "hardlink"])
def test_paid_wrapper_requires_safe_exclusive_leaf(tmp_path, leaf_kind):
    store, _, _ = _finalized(tmp_path, mode="flat")
    wrapper = store._path / "execution_input.json"
    original = wrapper.read_bytes()
    if leaf_kind == "symlink":
        wrapper.unlink()
        target = tmp_path / "outside-wrapper.json"
        target.write_bytes(original)
        wrapper.symlink_to(target)
    else:
        duplicate = tmp_path / "outside-wrapper-link.json"
        duplicate.hardlink_to(wrapper)

    assert admission.admit_spider_run(store._path) == dict(
        decision="REJECT", reason="UNSAFE_ARTIFACT_PATH", evidence=("execution_input.json",),
    )


def test_absent_paid_manifest_diagnoses_only_a_real_processed_journal_gap(tmp_path):
    artifacts, facts = _bundle()
    attempt = cast(dict[str, Any], decode_jsonl(artifacts["attempt.json"])[0])
    writer = SpiderRunStore.create(str(tmp_path), facts["run_id"])
    writer.register(attempt, execution_input=artifacts["execution_input.json"])
    first = cast(dict[str, Any], decode_jsonl(artifacts["journal.jsonl"])[0])
    writer.mark_processed(
        dict(ordinal=1, journal_seq=1, barrier_id=first["barrier_id"]),
        record_kind=first["record_kind"],
    )

    assert admission.admit_spider_run(writer._path) == dict(
        decision="REJECT", reason="INCOMPLETE_PERSISTENCE", evidence=admission._DIAGNOSTIC,
    )
    assert not (writer._path / "completion.json").exists()


def test_absent_zero_event_paid_failure_does_not_masquerade_as_barrier_gap(tmp_path, monkeypatch):
    artifacts, facts, _ = _initial_capture_failure_bundle(monkeypatch)
    attempt = cast(dict[str, Any], decode_jsonl(artifacts["attempt.json"])[0])
    writer = SpiderRunStore.create(str(tmp_path), facts["run_id"])
    writer.register(attempt, execution_input=artifacts["execution_input.json"])

    assert admission.admit_spider_run(writer._path) == dict(
        decision="REJECT", reason="MISSING_COMPLETE_MANIFEST", evidence=admission._DIAGNOSTIC,
    )
