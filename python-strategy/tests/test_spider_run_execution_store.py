"""Causal durability tests for paid Spider artifacts in the existing store."""

from copy import deepcopy
from dataclasses import replace
from pathlib import Path
import sys
from typing import Any, cast

import pytest

from src.core.backtest import spider_historical_input as historical_input_module
from src.core.backtest import spider_execution_input as execution_input_module
from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_jsonl
from src.core.backtest.spider_run_execution_evidence import build_execution_reconciliation
from test_spider_run_execution_evidence import _bundle, _initial_capture_failure_bundle, _liquidation_request
from test_spider_historical_input import _immutable_subclass
from test_spider_paid_endpoint_native import _seeded_two_product_run


def _start_store(tmp_path: Path, artifacts: dict[str, bytes]):
    attempt = cast(dict[str, Any], decode_jsonl(artifacts["attempt.json"])[0])
    run_id = attempt["run_id"]
    store = module.SpiderRunStore.create(str(tmp_path), run_id)
    assert store.register(attempt, execution_input=artifacts["execution_input.json"]) == "NATIVE_CONSTRUCTION_ALLOWED"
    rows = cast(list[dict[str, Any]], decode_jsonl(artifacts["journal.jsonl"]))
    for row in rows:
        boundary = dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"], barrier_id=row["barrier_id"])
        store.mark_processed(boundary, record_kind=row["record_kind"])
        store.append_journal(row)
    return store, rows


def _candidate(store, artifacts: dict[str, bytes], facts: dict[str, Any]):
    raw_files = {name: (store._path / name).read_bytes()
                 for name in ("attempt.json", "status.json", "journal.jsonl", "execution_input.json")}
    raw_files["paid_endpoint.json"] = artifacts["paid_endpoint.json"]
    raw_files["report.jsonl"] = artifacts["report.jsonl"]
    reconciliation = build_execution_reconciliation(facts["run_id"], raw_files)
    assert reconciliation["result"] == "OK", reconciliation["checks"]
    report = cast(list[dict[str, Any]], decode_jsonl(artifacts["report.jsonl"]))
    return decode_jsonl(artifacts["paid_endpoint.json"])[0], reconciliation, report


def test_actual_native_paid_bundle_publishes_closed_family_and_completion(tmp_path):
    artifacts, facts = _bundle()
    assert facts["paid"]["status"] == "PAID_FLAT"
    store, rows = _start_store(tmp_path, artifacts)
    endpoint, reconciliation, report = _candidate(store, artifacts, facts)
    assert store.finalize(endpoint, reconciliation, report) == "COMPLETE_PUBLISHED"
    assert store._state == "FROZEN"
    assert {path.name for path in store._path.iterdir()} == {
        "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
        "paid_endpoint.json", "report.jsonl", "reconciliation.json", "completion.json",
    }
    assert (store._path / "execution_input.json").read_bytes() == artifacts["execution_input.json"]
    assert (store._path / "paid_endpoint.json").read_bytes() == artifacts["paid_endpoint.json"]
    assert (store._path / "report.jsonl").read_bytes() == artifacts["report.jsonl"]
    assert (store._path / "reconciliation.json").read_bytes() == canonical_bytes(reconciliation) + b"\n"
    assert not (store._path / "historical_input.json").exists()
    completion = cast(dict[str, Any], decode_jsonl((store._path / "completion.json").read_bytes())[0])
    assert completion["endpoint_status"] == "PAID_FLAT"
    assert completion["evidence_scope"] == "COMPLETE_RUN"
    assert [item["row_count"] for item in completion["artifacts"]][2] == len(rows)


def test_paid_registration_validates_once_and_rejects_stale_token(tmp_path, monkeypatch):
    artifacts, _ = _bundle()
    attempt = cast(dict[str, Any], decode_jsonl(artifacts["attempt.json"])[0])
    original_validator = historical_input_module.validate_historical_input
    original_factory = historical_input_module._validate_historical_input_once
    validator_calls = 0
    all_calls = 0
    tokens = []

    def validate(run):
        nonlocal validator_calls
        validator_calls += 1
        return original_validator(run)

    def make_token(run):
        token = original_factory(run)
        tokens.append(token)
        return token

    def count_all_aliases(frame, event, _arg):
        nonlocal all_calls
        if event == "call" and frame.f_code is original_validator.__code__:
            all_calls += 1

    monkeypatch.setattr(historical_input_module, "validate_historical_input", validate)
    monkeypatch.setattr(historical_input_module, "_validate_historical_input_once", make_token)
    monkeypatch.setattr(execution_input_module, "_validate_historical_input_once", make_token)
    store = module.SpiderRunStore.create(str(tmp_path), attempt["run_id"])
    raw = artifacts["execution_input.json"]
    sys.setprofile(count_all_aliases)
    try:
        assert store.register(attempt, execution_input=raw) == "NATIVE_CONSTRUCTION_ALLOWED"
    finally:
        sys.setprofile(None)
    assert validator_calls == len(tokens) == all_calls == 1
    assert (store._path / "execution_input.json").read_bytes() == raw

    current = execution_input_module.decode_spider_execution_input(raw)
    changed = replace(current, historical_input=replace(
        current.historical_input, run_id="fresh-paid-registration",
    ))
    changed_attempt = deepcopy(attempt)
    changed_attempt["run_id"] = changed.historical_input.run_id
    changed_raw = execution_input_module.encode_spider_execution_input(changed)
    changed_hash = execution_input_module.validate_spider_execution_input(changed)
    changed_store = module.SpiderRunStore.create(str(tmp_path), changed.historical_input.run_id)
    monkeypatch.setattr(
        module, "_decode_spider_execution_input_validated",
        lambda _: (changed, tokens[0], changed_hash),
    )
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        changed_store.register(changed_attempt, execution_input=changed_raw)
    assert changed_store._state == "CREATED" and list(changed_store._path.iterdir()) == []


def test_paid_registration_falls_back_for_unapproved_historical_tree(tmp_path, monkeypatch):
    artifacts, _ = _bundle()
    attempt = cast(dict[str, Any], decode_jsonl(artifacts["attempt.json"])[0])
    original_validator = historical_input_module.validate_historical_input
    alias_calls = 0
    all_calls = 0
    def count_validation(run):
        nonlocal alias_calls
        alias_calls += 1
        return original_validator(run)

    def count_all_aliases(frame, event, _arg):
        nonlocal all_calls
        if event == "call" and frame.f_code is original_validator.__code__:
            all_calls += 1

    monkeypatch.setattr(historical_input_module, "validate_historical_input", count_validation)
    def reject_tree(*_args):
        return False

    monkeypatch.setattr(execution_input_module, "_is_approved_historical_input_tree", reject_tree)
    monkeypatch.setattr(historical_input_module, "_is_approved_historical_input_tree", reject_tree)
    store = module.SpiderRunStore.create(str(tmp_path), attempt["run_id"])
    raw = artifacts["execution_input.json"]
    sys.setprofile(count_all_aliases)
    try:
        assert store.register(attempt, execution_input=raw) == "NATIVE_CONSTRUCTION_ALLOWED"
    finally:
        sys.setprofile(None)
    assert all_calls == 14
    assert alias_calls == 8
    assert (store._path / "execution_input.json").read_bytes() == raw


def test_non_exact_manifest_fallback_preserves_head_exception_chain(monkeypatch):
    artifacts, _ = _bundle()
    original_decoder = historical_input_module._decode_manifest

    def non_exact_manifest(value):
        return _immutable_subclass(original_decoder(value))

    def error_chain(error):
        if error is None:
            return None
        return (
            type(error).__module__, type(error).__qualname__, error.args,
            error_chain(error.__cause__), error_chain(error.__context__),
            error.__suppress_context__,
        )

    monkeypatch.setattr(historical_input_module, "_decode_manifest", non_exact_manifest)
    with pytest.raises(Exception) as head_error:
        execution_input_module.decode_spider_execution_input(artifacts["execution_input.json"])
    monkeypatch.setattr(execution_input_module, "_is_approved_historical_input_tree", lambda _value: True)
    with pytest.raises(Exception) as validated_error:
        execution_input_module._decode_spider_execution_input_validated(
            artifacts["execution_input.json"],
        )
    assert error_chain(validated_error.value) == error_chain(head_error.value)
    assert validated_error.value.__context__ is None


@pytest.mark.parametrize("offset_ms", [30_000, 59_000])
def test_actual_native_incomplete_endpoint_is_finalized_as_incomplete(tmp_path, offset_ms):
    request = _seeded_two_product_run()
    request = replace(request, policy_end_ms=request.policy_end_ms + offset_ms)
    artifacts, facts = _bundle(request)
    assert facts["paid"]["status"] == "INCOMPLETE"
    store, _ = _start_store(tmp_path, artifacts)
    endpoint, reconciliation, report = _candidate(store, artifacts, facts)
    assert store.finalize(endpoint, reconciliation, report) == "COMPLETE_PUBLISHED"
    saved_raw = (store._path / "paid_endpoint.json").read_bytes()
    completion = decode_jsonl((store._path / "completion.json").read_bytes())[0]
    assert saved_raw == artifacts["paid_endpoint.json"]
    assert completion["endpoint_status"] == "INCOMPLETE"
    assert completion["evidence_scope"] == "COMPLETE_RUN"


def test_actual_native_initial_capture_failure_finalizes_zero_journal_prefix(tmp_path, monkeypatch):
    artifacts, facts, endpoint = _initial_capture_failure_bundle(monkeypatch)
    assert endpoint["status"] == "FAILED"
    store, rows = _start_store(tmp_path, artifacts)
    assert rows == []
    endpoint, reconciliation, report = _candidate(store, artifacts, facts)
    assert store.finalize(endpoint, reconciliation, report) == "COMPLETE_PUBLISHED"
    assert (store._path / "journal.jsonl").read_bytes() == b""
    assert store._processed is store._persisted is None
    completion = decode_jsonl((store._path / "completion.json").read_bytes())[0]
    assert completion["endpoint_status"] == "FAILED"
    assert completion["evidence_scope"] == "FAILED_PREFIX"
    assert completion["processed_boundary"] is completion["persisted_boundary"] is None


@pytest.mark.parametrize("failure_mode", ["source_fault", "binding_lookup", "callback_ack", "capture_failure"])
def test_actual_native_failed_prefix_can_be_finalized_without_changing_failure(tmp_path, monkeypatch, failure_mode):
    request = _seeded_two_product_run(with_working_order=failure_mode == "callback_ack")
    artifacts, facts = _bundle(request, failure_mode=failure_mode, monkeypatch=monkeypatch)
    assert facts["paid"]["status"] == "FAILED"
    store, _ = _start_store(tmp_path, artifacts)
    endpoint, reconciliation, report = _candidate(store, artifacts, facts)
    assert store.finalize(endpoint, reconciliation, report) == "COMPLETE_PUBLISHED"
    assert (store._path / "paid_endpoint.json").read_bytes() == artifacts["paid_endpoint.json"]
    completion = decode_jsonl((store._path / "completion.json").read_bytes())[0]
    assert completion["endpoint_status"] == "FAILED"
    assert completion["evidence_scope"] == "FAILED_PREFIX"


@pytest.mark.parametrize("cash,status", [("50", "LIQUIDATED_INSOLVENT"), ("99.003", "LIQUIDATED_FLAT")])
def test_actual_native_liquidation_status_is_persisted_truthfully(tmp_path, cash, status):
    artifacts, facts = _bundle(_liquidation_request(cash))
    assert facts["paid"]["status"] == status
    store, _ = _start_store(tmp_path, artifacts)
    endpoint, reconciliation, report = _candidate(store, artifacts, facts)
    assert store.finalize(endpoint, reconciliation, report) == "COMPLETE_PUBLISHED"
    completion = decode_jsonl((store._path / "completion.json").read_bytes())[0]
    assert completion["endpoint_status"] == status
    assert completion["evidence_scope"] == "COMPLETE_RUN"


@pytest.mark.parametrize("mutation", ["wrapper", "attempt_id", "hash", "configuration", "history", "execution_context", "mixed_input"])
def test_paid_registration_rejects_mismatched_identity_before_any_artifact_publish(tmp_path, mutation):
    artifacts, facts = _bundle()
    attempt = deepcopy(facts["attempt"])
    wrapper = artifacts["execution_input.json"]
    historical_input = None
    if mutation == "wrapper":
        wrapper += b"\n"
    elif mutation == "attempt_id":
        attempt["run_id"] = "different-run"
    elif mutation == "hash":
        attempt["input_contract_hashes"][0]["sha256"] = "f" * 64
    elif mutation == "configuration":
        attempt["configuration_context"]["config_id"] = "other-config"
    elif mutation == "history":
        attempt["historical_context"]["source_sha256"] = "f" * 64
    elif mutation == "execution_context":
        attempt["execution_context"]["policy_end_ms"] += 1
    else:
        historical_input = b"{}"
    store = module.SpiderRunStore.create(str(tmp_path), facts["run_id"])
    with pytest.raises((ValueError, KeyError), match="INVALID_ARTIFACT"):
        store.register(attempt, historical_input=historical_input, execution_input=wrapper)
    assert list(store._path.iterdir()) == []
    assert store._state == "CREATED"


def test_paid_barrier_rejects_delivery_attempt_and_retains_uncommitted_prefix(tmp_path):
    artifacts, facts = _bundle()
    store, _ = _start_store(tmp_path, {**artifacts, "journal.jsonl": b""})
    bad = decode_jsonl(artifacts["journal.jsonl"])[0]
    bad["record_kind"] = "DELIVERY_ATTEMPT"
    boundary = dict(ordinal=1, journal_seq=1, barrier_id=bad["barrier_id"])
    with pytest.raises((ValueError, RuntimeError)):
        store.mark_processed(boundary, record_kind="DELIVERY_ATTEMPT")
    assert store._processed is store._persisted is None
    assert (store._path / "journal.jsonl").read_bytes() == b""


def test_processed_without_append_cannot_finalize_or_create_completion(tmp_path):
    artifacts, facts = _bundle()
    store, _ = _start_store(tmp_path, {**artifacts, "journal.jsonl": b""})
    first = cast(dict[str, Any], decode_jsonl(artifacts["journal.jsonl"])[0])
    store.mark_processed(
        dict(ordinal=1, journal_seq=1, barrier_id=first["barrier_id"]),
        record_kind=first["record_kind"],
    )
    endpoint = cast(dict[str, Any], decode_jsonl(artifacts["paid_endpoint.json"])[0])
    report = cast(list[dict[str, Any]], decode_jsonl(artifacts["report.jsonl"]))
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.finalize(endpoint, {}, report)
    assert not (store._path / "completion.json").exists()


def test_paid_diagnostic_uses_no_clobber_publication_and_never_completes(tmp_path):
    artifacts, facts = _bundle(failure_mode="capture_failure")
    store, _ = _start_store(tmp_path, artifacts)
    store.publish_failure("SCHEDULER_FAILED", dict(kind="SCHEDULER", reason="CAPTURE_FAILED"))
    endpoint = cast(dict[str, Any], decode_jsonl(artifacts["paid_endpoint.json"])[0])
    store.publish_paid_diagnostic(endpoint)
    raw = (store._path / "paid_endpoint.json").read_bytes()
    assert decode_jsonl(raw)[0] == endpoint
    with pytest.raises(module.SpiderRunStoreError):
        store.publish_paid_diagnostic(endpoint)
    assert (store._path / "paid_endpoint.json").read_bytes() == raw
    assert not (store._path / "completion.json").exists()


def test_paid_append_failure_keeps_prefix_without_completion(tmp_path, monkeypatch):
    artifacts, facts = _bundle()
    store = module.SpiderRunStore.create(str(tmp_path), facts["run_id"])
    store.register(facts["attempt"], execution_input=artifacts["execution_input.json"])
    row = cast(dict[str, Any], decode_jsonl(artifacts["journal.jsonl"])[0])
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind=row["record_kind"])
    original = module._write

    def fail_append(path, raw, append=False):
        if append:
            raise module.SpiderRunStoreError("ARTIFACT_WRITE_FAILED")
        return original(path, raw, append)

    monkeypatch.setattr(module, "_write", fail_append)
    with pytest.raises(module.SpiderRunStoreError):
        store.append_journal(row)
    assert store._barrier_failed
    assert (store._path / "journal.jsonl").read_bytes() == b""
    assert not (store._path / "completion.json").exists()
