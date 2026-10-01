"""Causal filesystem durability gates using independent detached fixtures."""

import ast
import os
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any, Protocol, cast

import pytest

from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import (
    canonical_bytes,
    decode_jsonl,
    validate_artifact,
)
from src.core.backtest.spider_historical_input import (
    decode_p2_configuration, encode_historical_run_input,
    historical_context_for_input, historical_planned_coverage,
)
from src.core.backtest.spider_run_completion_schema import report as validate_report
from src.core.backtest.spider_run_envelope_schema import endpoint as validate_endpoint, journal as validate_journal
from src.core.backtest.spider_run_reconciliation_schema import reconciliation as validate_reconciliation
from test_spider_run_artifacts import configured_attempt, configuration_context, historical_attempt, rejected
from test_spider_run_completion_schema import configured_report_rows
from test_spider_run_envelope_schema import configured_endpoint, configured_group_record
from test_spider_run_envelope_schema import record
from test_spider_run_reconciliation_schema import configured_fixture as configured_reconciliation_fixture
from test_spider_run_evidence import RUN, expected_checks, fixture
from test_spider_historical_input import _valid_run


class _ReportValidationBoundary(Protocol):
    def __call__(self, value: object, *, context: object | None = None) -> None: ...


def prepared(tmp_path, index=0):
    values = fixture(index)
    values[0]["policy_source_sha256"] = "eb6ab34d8685fb59e286f5ffda8af24cbecf3e5ab2c729c585ccdca797cac336"
    values[0]["input_contract_hashes"][3]["sha256"] = values[0]["policy_source_sha256"]
    recon: dict[str, Any] = dict(schema_version="spider_reconciliation_v1", run_id=RUN, result="OK", checks=expected_checks(values))
    return module.SpiderRunStore.create(str(tmp_path), RUN), values, recon


def boundary(row):
    return dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"], barrier_id=row["barrier_id"])


def historical_registration():
    run = _valid_run()
    raw = encode_historical_run_input(run)
    configuration = decode_p2_configuration(
        run.configuration_bytes, run.configuration_sha256, run.ordered_products
    )
    config_context = dict(
        schema_version="spider_configuration_context_v1",
        config_id=configuration["config_id"],
        configuration_sha256=run.configuration_sha256,
        products=list(run.ordered_products),
    )
    history_context = historical_context_for_input(run)
    attempt = historical_attempt()
    attempt.update(
        run_id=run.run_id,
        account_key={"venue": "SPIDER_HISTORICAL_RESEARCH", "environment": "RESEARCH_ONLY",
                     "account": run.account_key},
        scenario_plan_sha256=history_context["historical_input_sha256"],
        configuration_context=config_context,
        historical_context=history_context,
        planned_coverage=historical_planned_coverage(run),
    )
    attempt["policy_source_sha256"] = run.policy_source_sha256
    for item, key in zip(
        cast(list[dict[str, object]], attempt["input_contract_hashes"]),
        ("scenario_plan_sha256", "program_sha256", "native_artifact_sha256", "policy_source_sha256"),
        strict=True,
    ):
        item["sha256"] = attempt[key]
    return run, raw, attempt


def ready(store, values):
    assert store.register(values[0]) == "NATIVE_CONSTRUCTION_ALLOWED"
    for row in values[2]:
        store.mark_processed(boundary(row))
        store.append_journal(row)
        status = decode_jsonl((store._path / "status.json").read_bytes())[0]
        assert status["state"] == "RUNNING"
        assert status["processed_boundary"] == status["persisted_boundary"] == boundary(row)


def configured_ready(tmp_path):
    store = module.SpiderRunStore.create(str(tmp_path), "r-1")
    attempt = configured_attempt()
    attempt.update(run_id="r-1", planned_coverage=[
        {"ordinal": 1, "barrier_id": "barrier-1", "record_kind": "SOURCE_GROUP_RESULT"}
    ])
    assert store.register(attempt) == "NATIVE_CONSTRUCTION_ALLOWED"
    row = configured_group_record("CFG-FIRST")
    row.update(run_id="r-1", barrier_id="barrier-1")
    store.mark_processed(boundary(row))
    store.append_journal(row)
    status = decode_jsonl((store._path / "status.json").read_bytes())[0]
    endpoint = configured_endpoint("CFG-FIRST")
    endpoint["run_id"] = "r-1"
    endpoint["cutoff"]["persisted_boundary"] = boundary(row)
    reconciliation_value = configured_reconciliation_fixture()
    reconciliation_value["run_id"] = "r-1"
    report = configured_report_rows()
    for entry in report:
        entry["run_id"] = "r-1"
    validate_artifact(attempt)
    validate_artifact(status)
    validate_journal([row])
    validate_endpoint(endpoint)
    validate_reconciliation(reconciliation_value)
    cast(_ReportValidationBoundary, validate_report)(
        report, context=attempt["configuration_context"]
    )
    return store, attempt, status, [row], endpoint, reconciliation_value, report


@pytest.mark.parametrize("index", range(3))
def test_full_publication_and_actual_complete_status_metadata(tmp_path, index):
    store, values, recon = prepared(tmp_path, index)
    before = deepcopy(values)
    ready(store, values)
    root = tmp_path / RUN
    running = (root / "status.json").read_bytes()
    assert store.finalize(values[3], recon, values[4]) == "COMPLETE_PUBLISHED"
    result = admit_spider_run(root)
    assert result["decision"] == "ACCEPT"
    complete = (root / "status.json").read_bytes()
    metadata = result["artifacts"]["completion.json"]["artifacts"][1]
    assert metadata["sha256"] == sha256(complete).hexdigest() != sha256(running).hexdigest()
    assert metadata["byte_count"] == len(complete) and metadata["row_count"] == 1
    assert values == before and store._state == "FROZEN"
    with pytest.raises(RuntimeError):
        store.publish_failure("PERSISTENCE_FAILED", None)


def test_registration_order_and_empty_journal_durability(tmp_path, monkeypatch):
    events = []
    original_sync, original_write, original_replace, original_link = module._sync, module._write, os.replace, os.link
    def sync(path):
        events.append(("dir", Path(path).name))
        original_sync(path)
    def write(path, raw, append=False):
        original_write(path, raw, append)
        events.append(("file", Path(path).name))
    def link(src, dst):
        original_link(src, dst)
        events.append(("link", Path(dst).name))
    def replace(src, dst):
        original_replace(src, dst)
        events.append(("replace", Path(dst).name))
    monkeypatch.setattr(module, "_sync", sync)
    monkeypatch.setattr(module, "_write", write)
    monkeypatch.setattr(module.os, "link", link)
    monkeypatch.setattr(module.os, "replace", replace)
    store, values, _ = prepared(tmp_path)
    assert store.register(values[0]) == "NATIVE_CONSTRUCTION_ALLOWED"
    assert events == [("dir", tmp_path.name), ("file", ".attempt.json.tmp"), ("link", "attempt.json"), ("dir", RUN),
                      ("file", ".status.json.tmp"), ("replace", "status.json"), ("dir", RUN), ("file", "journal.jsonl"), ("dir", RUN)]
    assert (tmp_path / RUN / "journal.jsonl").read_bytes() == b""


def test_rejected_registration_and_invalid_orders(tmp_path):
    store, values, _ = prepared(tmp_path)
    with pytest.raises(RuntimeError):
        store.publish_failure("PERSISTENCE_FAILED", None)
    with pytest.raises(RuntimeError):
        store.mark_processed(boundary(values[2][0]))
    attempt = rejected()
    attempt["run_id"] = RUN
    assert store.register(attempt) == "REGISTRATION_REJECTED"
    assert store._state == "FAILED" and not (tmp_path / RUN / "journal.jsonl").exists()
    assert decode_jsonl((tmp_path / RUN / "status.json").read_bytes())[0]["failure_reason"] == "UNSUPPORTED_CONFIGURATION"
    with pytest.raises(RuntimeError):
        store.register(attempt)


def test_mark_before_after_capture_failure_is_memory_only(tmp_path):
    store, values, _ = prepared(tmp_path)
    store.register(values[0])
    root = tmp_path / RUN
    before = (root / "status.json").read_bytes()
    store.mark_processed(boundary(values[2][0]))
    assert (root / "status.json").read_bytes() == before and (root / "journal.jsonl").read_bytes() == b""
    with pytest.raises(RuntimeError):
        store.mark_processed(boundary(values[2][1]))
    store.publish_failure("PERSISTENCE_FAILED", None)
    status = decode_jsonl((root / "status.json").read_bytes())[0]
    assert status["processed_boundary"] == boundary(values[2][0]) and status["persisted_boundary"] is None
    assert admit_spider_run(root)["reason"] == "INCOMPLETE_PERSISTENCE"


@pytest.mark.parametrize("stage", ["write", "short", "flush", "fsync", "publish", "directory"])
def test_one_failure_per_publication_primitive(tmp_path, monkeypatch, stage):
    store, values, _ = prepared(tmp_path)
    original = module._open
    class Stream:
        def __init__(self, *args, **kwargs):
            self.inner = original(*args, **kwargs)
        def __enter__(self):
            return self
        def __exit__(self, *args):
            self.inner.close()
        def fileno(self):
            return self.inner.fileno()
        def write(self, raw):
            if stage == "write":
                raise OSError("injected")
            return len(raw) - 1 if stage == "short" else self.inner.write(raw)
        def flush(self):
            if stage == "flush":
                raise OSError("injected")
            self.inner.flush()
    def fail(*args):
        raise OSError("injected")
    def directory_failure(_):
        raise module.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN")
    monkeypatch.setattr(module, "_open", Stream)
    if stage == "fsync":
        monkeypatch.setattr(module.os, "fsync", fail)
    elif stage == "publish":
        monkeypatch.setattr(module.os, "link", fail)
    elif stage == "directory":
        monkeypatch.setattr(module, "_sync", directory_failure)
    reason = "PUBLICATION_FAILED" if stage == "publish" else "PUBLICATION_DURABILITY_UNKNOWN" if stage == "directory" else "ARTIFACT_WRITE_FAILED"
    with pytest.raises(module.SpiderRunStoreError) as error:
        store.register(values[0])
    assert error.value.reason == reason
    assert not (tmp_path / RUN / "journal.jsonl").exists() and not (tmp_path / RUN / "completion.json").exists()


@pytest.mark.parametrize("status_fails", [False, True])
def test_empty_journal_directory_failure_no_token_and_failure_seam(tmp_path, monkeypatch, status_fails):
    store, values, _ = prepared(tmp_path)
    original_sync, original_publish = module._sync, store._publish
    def sync(path):
        if (Path(path) / "journal.jsonl").exists():
            raise module.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN")
        original_sync(path)
    def publish(name, raw):
        if name == "status.json" and b'"FAILED"' in raw:
            if status_fails:
                raise module.SpiderRunStoreError("PUBLICATION_FAILED")
            monkeypatch.setattr(module, "_sync", original_sync)
        original_publish(name, raw)
    monkeypatch.setattr(module, "_sync", sync)
    monkeypatch.setattr(store, "_publish", publish)
    with pytest.raises(module.SpiderRunStoreError) as error:
        store.register(values[0])
    assert error.value.reason == ("PUBLICATION_FAILED" if status_fails else "PERSISTENCE_FAILED")
    assert error.value.requested_failure_reason == "PERSISTENCE_FAILED"
    assert error.value.primary_failure == dict(kind="PERSISTENCE", reason="PUBLICATION_DURABILITY_UNKNOWN")


def test_append_status_failure_stops_next_barrier_and_preserves_primary(tmp_path, monkeypatch):
    store, values, _ = prepared(tmp_path)
    store.register(values[0])
    store.mark_processed(boundary(values[2][0]))
    def fail(*args):
        raise module.SpiderRunStoreError("PUBLICATION_FAILED")
    monkeypatch.setattr(store, "_publish", fail)
    with pytest.raises(module.SpiderRunStoreError) as error:
        store.append_journal(values[2][0])
    assert error.value.reason == error.value.requested_failure_reason == "PERSISTENCE_FAILED"
    assert error.value.primary_failure == dict(kind="PERSISTENCE", reason="PUBLICATION_FAILED")
    with pytest.raises(RuntimeError):
        store.mark_processed(boundary(values[2][1]))
    primary = dict(kind="CALLBACK", reason="POLICY_FAILED")
    with pytest.raises(module.SpiderRunStoreError) as failure:
        store.publish_failure("CALLBACK_FAILED", primary)
    assert failure.value.reason == "PUBLICATION_FAILED" and failure.value.requested_failure_reason == "CALLBACK_FAILED"
    assert failure.value.primary_failure == primary


def test_first_append_failure_retains_zero_persisted_frontier(tmp_path, monkeypatch):
    store, values, _ = prepared(tmp_path)
    store.register(values[0])
    store.mark_processed(boundary(values[2][0]))
    original = module._write
    def fail(path, raw, append=False):
        if append:
            raise module.SpiderRunStoreError("ARTIFACT_WRITE_FAILED")
        original(path, raw, append)
    monkeypatch.setattr(module, "_write", fail)
    with pytest.raises(module.SpiderRunStoreError) as error:
        store.append_journal(values[2][0])
    assert error.value.primary_failure == dict(kind="PERSISTENCE", reason="ARTIFACT_WRITE_FAILED")
    store.publish_failure(error.value.reason, error.value.primary_failure)
    assert store._persisted is None
    assert admit_spider_run(tmp_path / RUN)["reason"] == "INCOMPLETE_PERSISTENCE"


def test_context_mismatch_append_writes_neither_journal_nor_status(tmp_path):
    store = module.SpiderRunStore.create(str(tmp_path), "r-1")
    attempt = configured_attempt()
    attempt["planned_coverage"] = [
        {"ordinal": 1, "barrier_id": "barrier-1", "record_kind": "SOURCE_GROUP_RESULT"}
    ]
    store.register(attempt)
    assert decode_jsonl((store._path / "status.json").read_bytes())[0]["configuration_context"] == attempt["configuration_context"]
    row = configured_group_record("CFG-MIDDLE")
    row.update(run_id="r-1", barrier_id="barrier-1")
    wrong_context = configuration_context(config_id="configured-v1")
    wrong_context["configuration_sha256"] = "b" * 64

    def replace_context(value):
        if type(value) is dict:
            if "configuration_context" in value:
                value["configuration_context"] = deepcopy(wrong_context)
            for nested in value.values():
                replace_context(nested)
        elif type(value) is list:
            for nested in value:
                replace_context(nested)

    replace_context(row)
    target = boundary(row)
    store.mark_processed(target)
    journal_path, status_path = store._path / "journal.jsonl", store._path / "status.json"
    before_journal, before_status = journal_path.read_bytes(), status_path.read_bytes()
    with pytest.raises(module.SpiderRunStoreError) as error:
        store.append_journal(row)
    assert error.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert journal_path.read_bytes() == before_journal
    assert status_path.read_bytes() == before_status
    assert store._processed == target and store._persisted is None


def test_historical_registration_persists_input_and_dynamic_journal_frontier(tmp_path):
    run, raw, attempt = historical_registration()
    store = module.SpiderRunStore.create(str(tmp_path), run.run_id)
    assert store.register(attempt, historical_input=raw) == "NATIVE_CONSTRUCTION_ALLOWED"
    assert (store._path / "historical_input.json").read_bytes() == raw
    assert (store._path / "historical_input.json").read_bytes().startswith(b"{")
    expected_config = attempt["configuration_context"]
    expected_history = attempt["historical_context"]
    running = decode_jsonl((store._path / "status.json").read_bytes())[0]
    assert (running["configuration_context"], running["historical_context"]) == (expected_config, expected_history)
    assert admit_spider_run(store._path) == dict(
        decision="REJECT", reason="MISSING_COMPLETE_MANIFEST",
        evidence=("completion.json", "status.json", "journal.jsonl"),
    )

    rows = []
    for sequence in (1, 2):
        row = record("CALLBACK_RESULT")
        row.update(run_id=attempt["run_id"], journal_seq=sequence, barrier_id=f"dynamic-{sequence}",
                   configuration_context=deepcopy(expected_config), historical_context=deepcopy(expected_history))
        rows.append(row)
    malformed = dict(ordinal=1, journal_seq=2, barrier_id="malformed-frontier")
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.mark_processed(malformed, record_kind="CALLBACK_RESULT")
    assert store._processed is store._processed_kind is None
    assert store._persisted is None
    store.mark_processed(boundary(rows[0]), record_kind="CALLBACK_RESULT")
    store.append_journal(rows[0])
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.mark_processed(dict(ordinal=3, journal_seq=3, barrier_id="skip"), record_kind="CALLBACK_RESULT")
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.mark_processed(dict(ordinal=2, journal_seq=2, barrier_id="dynamic-1"), record_kind="CALLBACK_RESULT")
    store.mark_processed(boundary(rows[1]), record_kind="CALLBACK_RESULT")
    mismatched = record("DELIVERY_ATTEMPT")
    mismatched.update(run_id=attempt["run_id"], journal_seq=2, barrier_id="dynamic-2",
                      configuration_context=deepcopy(expected_config), historical_context=deepcopy(expected_history))
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.append_journal(mismatched)
    store.append_journal(rows[1])
    persisted, status = (decode_jsonl((store._path / name).read_bytes())[0] for name in ("journal.jsonl", "status.json"))
    assert decode_jsonl((store._path / "journal.jsonl").read_bytes()) == rows
    assert (persisted["configuration_context"], persisted["historical_context"]) == (expected_config, expected_history)
    assert status["processed_boundary"] == status["persisted_boundary"] == boundary(rows[1])
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        store.finalize({}, {}, [])
    assert not any((store._path / name).exists() for name in ("endpoint.json", "reconciliation.json", "report.jsonl", "completion.json"))


@pytest.mark.parametrize("case", ["missing_input", "bad_input_bytes", "wrong_context", "wrong_coverage", "wrong_run_id"])
def test_historical_registration_rejects_input_or_attempt_mismatch_before_attempt_publish(tmp_path, case):
    run, raw, attempt = historical_registration()
    if case == "wrong_context":
        historical = attempt["historical_context"]
        assert isinstance(historical, dict)
        historical["source_sha256"] = "f" * 64
    elif case == "wrong_coverage":
        coverage = attempt["planned_coverage"]
        assert isinstance(coverage, list)
        coverage.pop()
    elif case == "wrong_run_id":
        attempt["run_id"] = "other-run"
    store = module.SpiderRunStore.create(str(tmp_path), run.run_id)
    supplied = None if case == "missing_input" else raw + b" " if case == "bad_input_bytes" else raw
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        store.register(attempt, historical_input=supplied)
    assert not (store._path / "attempt.json").exists()
    assert not (store._path / "status.json").exists()
    assert not (store._path / "historical_input.json").exists()


@pytest.mark.parametrize("case", ["policy_source", "hash_0", "hash_1", "hash_2", "hash_3"])
def test_historical_registration_rejects_identity_mismatch_without_publishing(tmp_path, case):
    run, raw, attempt = historical_registration()
    if case == "policy_source":
        attempt["policy_source_sha256"] = "f" * 64
        cast(list[dict[str, object]], attempt["input_contract_hashes"])[3]["sha256"] = "f" * 64
    else:
        index = int(case[-1])
        cast(list[dict[str, object]], attempt["input_contract_hashes"])[index]["sha256"] = "f" * 64
    store = module.SpiderRunStore.create(str(tmp_path), run.run_id)
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        store.register(attempt, historical_input=raw)
    assert store._state == "CREATED"
    assert store._attempt == {}
    assert store._processed is store._persisted is None
    assert list(store._path.iterdir()) == []


def test_p1_p2_registration_rejects_historical_input_bytes(tmp_path):
    store, values, _ = prepared(tmp_path)
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        store.register(values[0], historical_input=b"{}")
    assert not (store._path / "attempt.json").exists()


def test_historical_dynamic_append_rejects_context_mismatch_without_persisting(tmp_path):
    run, raw, attempt = historical_registration()
    store = module.SpiderRunStore.create(str(tmp_path), run.run_id)
    store.register(attempt, historical_input=raw)
    row = record("CALLBACK_RESULT")
    row.update(run_id=run.run_id, journal_seq=1, barrier_id="dynamic-1",
               configuration_context=deepcopy(attempt["configuration_context"]),
               historical_context=deepcopy(attempt["historical_context"]))
    row["historical_context"]["source_sha256"] = "f" * 64
    store.mark_processed(boundary(row), record_kind="CALLBACK_RESULT")
    journal_path, status_path = store._path / "journal.jsonl", store._path / "status.json"
    before = journal_path.read_bytes(), status_path.read_bytes()
    with pytest.raises(module.SpiderRunStoreError, match="ENDPOINT_RECONCILIATION_FAILED"):
        store.append_journal(row)
    assert (journal_path.read_bytes(), status_path.read_bytes()) == before
    assert store._persisted is None


def test_configured_report_validation_uses_candidate_context_for_reread(tmp_path):
    store, attempt, _, _, _, _, _ = configured_ready(tmp_path)
    candidate = configuration_context()
    candidate["configuration_sha256"] = "b" * 64
    rows = configured_report_rows(candidate)
    for row in rows:
        row["run_id"] = "r-1"
    path = store._path / "report.jsonl"
    path.write_bytes(b"".join(canonical_bytes(row) + b"\n" for row in rows))
    reread, _ = store._reread("report.jsonl", module._report_validator(attempt))
    assert reread == rows


@pytest.mark.parametrize("mutation", ["hash", "order"])
def test_finalize_valid_candidate_report_mismatch_reaches_context_chain(tmp_path, monkeypatch, mutation):
    store, attempt, _, _, endpoint, recon, _ = configured_ready(tmp_path)
    candidate = cast(dict[str, object], deepcopy(attempt["configuration_context"]))
    if mutation == "hash":
        candidate["configuration_sha256"] = "b" * 64
    else:
        cast(list[str], candidate["products"]).reverse()
    report = configured_report_rows(candidate)
    for row in report:
        row["run_id"] = "r-1"
    original = module._context_chain
    seen = []

    def observe(attempt_value, *artifacts):
        seen.append(artifacts[-1])
        return original(attempt_value, *artifacts)

    monkeypatch.setattr(module, "_context_chain", observe)
    with pytest.raises(module.SpiderRunStoreError) as error:
        store.finalize(endpoint, recon, report)
    assert error.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert seen == [report]
    assert not (store._path / "endpoint.json").exists()


def test_configured_context_control_reaches_unavailable_selector(tmp_path, monkeypatch):
    store, attempt, _, _, endpoint, recon, report = configured_ready(tmp_path)
    from src.core.backtest import spider_run_evidence as evidence

    original = evidence._plan_bundle
    selected = []

    def observe(plan_id):
        selected.append(plan_id)
        return original(plan_id)

    monkeypatch.setattr(evidence, "_plan_bundle", observe)
    with pytest.raises(module.SpiderRunStoreError) as error:
        store.finalize(endpoint, recon, report)
    assert error.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert selected == [attempt["scenario_plan_id"]]
    assert (store._path / "endpoint.json").exists()
    assert not (store._path / "completion.json").exists()


@pytest.mark.parametrize("report", [[None], ["row"], [{}]])
def test_configured_malformed_report_is_invalid_artifact_before_context_chain(tmp_path, report):
    store, _, _, _, endpoint, recon, _ = configured_ready(tmp_path)
    with pytest.raises(ValueError, match="^INVALID_ARTIFACT$"):
        store.finalize(endpoint, recon, report)


@pytest.mark.parametrize("gate", [1, 2, 3])
def test_finalize_rejects_at_each_context_chain_gate(tmp_path, monkeypatch, gate):
    store, values, recon = prepared(tmp_path)
    ready(store, values)
    original = module._context_chain
    calls = 0

    def reject_selected(*args):
        nonlocal calls
        calls += 1
        if calls == gate:
            raise module.ReconciliationProjectionError()
        return original(*args)

    monkeypatch.setattr(module, "_context_chain", reject_selected)
    with pytest.raises(module.SpiderRunStoreError) as error:
        store.finalize(values[3], recon, values[4])
    root = tmp_path / RUN
    assert error.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert calls == gate
    assert (root / "endpoint.json").exists() is (gate >= 2)
    assert (b'"COMPLETE"' in (root / "status.json").read_bytes()) is (gate == 3)
    assert not (root / "completion.json").exists()


def test_root_directory_fsync_failure_has_no_artifact_authority(tmp_path, monkeypatch):
    def fail(_):
        raise OSError("injected")
    monkeypatch.setattr(module.os, "fsync", fail)
    with pytest.raises(module.SpiderRunStoreError) as error:
        module.SpiderRunStore.create(str(tmp_path), RUN)
    assert error.value.reason == "PUBLICATION_DURABILITY_UNKNOWN"
    assert list((tmp_path / RUN).iterdir()) == []


@pytest.mark.parametrize("collision", ["attempt.json", ".attempt.json.tmp"])
def test_collision_and_stale_temp_never_clobbered(tmp_path, collision):
    store, values, _ = prepared(tmp_path)
    target = tmp_path / RUN / collision
    target.write_bytes(b"preserved")
    with pytest.raises(module.SpiderRunStoreError):
        store.register(values[0])
    assert target.read_bytes() == b"preserved"
    with pytest.raises(module.SpiderRunStoreError):
        module.SpiderRunStore.create(str(tmp_path), RUN)


@pytest.mark.parametrize("fault", ["io", "decode", "evidence", "complete-status", "completion-dir"])
def test_finalize_rereads_and_completion_visibility_freezes(tmp_path, monkeypatch, fault):
    store, values, recon = prepared(tmp_path)
    ready(store, values)
    original_read, original_sync = Path.read_bytes, module._sync
    def read(path):
        if path.name == "attempt.json" and fault == "io":
            raise OSError("injected")
        if path.name == "attempt.json" and fault == "decode":
            return b"not-json\n"
        raw = original_read(path)
        if path.name == "status.json" and fault == "complete-status" and b'"COMPLETE"' in raw:
            return b"{}\n"
        return raw
    def sync(path):
        if fault == "completion-dir" and (Path(path) / "completion.json").exists():
            raise module.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN")
        original_sync(path)
    if fault == "evidence":
        recon["checks"][10]["observed"]["report_sha256"] = "f" * 64
    monkeypatch.setattr(Path, "read_bytes", read)
    monkeypatch.setattr(module, "_sync", sync)
    with pytest.raises(module.SpiderRunStoreError) as error:
        store.finalize(values[3], recon, values[4])
    expected = "PUBLICATION_FAILED" if fault == "io" else "PUBLICATION_DURABILITY_UNKNOWN" if fault == "completion-dir" else "ENDPOINT_RECONCILIATION_FAILED"
    assert error.value.reason == expected
    assert (tmp_path / RUN / "completion.json").exists() == (fault == "completion-dir")
    if fault == "completion-dir":
        assert store._state == "FROZEN"
        with pytest.raises(RuntimeError):
            store.publish_failure("PUBLICATION_FAILED", None)


def test_programming_error_and_import_boundary(tmp_path, monkeypatch):
    store, values, recon = prepared(tmp_path)
    ready(store, values)
    def broken(*args):
        raise RuntimeError("programming")
    monkeypatch.setattr(module, "build_reconciliation", broken)
    with pytest.raises(RuntimeError, match="programming"):
        store.finalize(values[3], recon, values[4])
    tree = ast.parse(Path(module.__file__).read_text())
    imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
    assert not any(any(word in (name or "") for word in ("plans", "codec", "scheduler", "policy", "admission")) for name in imports)
