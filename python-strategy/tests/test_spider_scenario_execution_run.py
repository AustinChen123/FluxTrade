"""Actual-native paid scenario orchestration and same-store admission tests."""

from dataclasses import replace
from hashlib import sha256
import inspect
import os
from pathlib import Path
import stat
from typing import Any, cast

import pytest

from src.core.backtest import spider_scenario_run as scenario
from src.core.backtest import spider_run_admission as _admission
from src.core.backtest import spider_run_store as _store
from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_execution_input import encode_spider_execution_input
from src.core.backtest.spider_historical_input import encode_historical_run_input
from test_spider_execution_input import _input
from test_spider_paid_endpoint_native import _seeded_two_product_run
from test_spider_run_execution_evidence import _liquidation_request


def _request(run_id: str, *, mode: str = "flat"):
    if mode == "insolvent":
        request = _liquidation_request("50")
    elif mode == "liquidated_flat":
        request = _liquidation_request("99.003")
    else:
        request = _seeded_two_product_run()
    run = replace(request.historical_input, run_id=run_id)
    request = _input(run)
    if mode == "incomplete":
        request = replace(request, policy_end_ms=request.policy_end_ms + 30_000)
    return request, encode_spider_execution_input(request)


def _run(tmp_path: Path, run_id: str, *, mode: str = "flat", **kwargs):
    request, raw = _request(run_id, mode=mode)
    result = scenario.run_spider_scenario(
        str(tmp_path), run_id, scenario._PAID_SELECTOR, execution_input=raw, **kwargs,
    )
    return request, raw, result


@pytest.mark.parametrize(
    ("mode", "status"),
    [
        ("flat", "PAID_FLAT"),
        ("incomplete", "INCOMPLETE"),
        ("insolvent", "LIQUIDATED_INSOLVENT"),
        ("liquidated_flat", "LIQUIDATED_FLAT"),
    ],
)
def test_paid_scenario_uses_one_native_composition_store_and_admission(tmp_path, monkeypatch, mode, status):
    request, raw = _request("scenario-" + mode, mode=mode)
    original_apply = wire.ScenarioCodec.apply_group
    applied = []

    def counted_apply(codec, group):
        applied.append(group["group_id"])
        return original_apply(codec, group)

    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", counted_apply)
    result = scenario.run_spider_scenario(
        str(tmp_path), request.historical_input.run_id, scenario._PAID_SELECTOR,
        execution_input=raw,
    )

    assert result == dict(
        run_id=request.historical_input.run_id, outcome="ADMITTED", reason=None,
        endpoint_status=status,
    )
    directory = tmp_path / request.historical_input.run_id
    admission = scenario._admit(directory)
    assert admission["decision"] == "ACCEPT"
    artifacts = admission["artifacts"]
    assert tuple(artifacts) == _admission._EXECUTION_NAMES + ("completion.json",)
    assert artifacts["execution_input.json"] == scenario._decode(raw)
    assert (directory / "execution_input.json").read_bytes() == raw
    assert not (directory / "historical_input.json").exists()
    attempt = artifacts["attempt.json"]
    assert attempt["program_sha256"] == _program_hash()
    assert attempt["native_artifact_sha256"] == sha256(
        Path(wire.loaded_native_artifact_path()).read_bytes()
    ).hexdigest()
    assert attempt["policy_source_sha256"] == request.historical_input.policy_source_sha256
    assert attempt["scenario_plan_sha256"] != attempt["historical_context"]["historical_input_sha256"]
    groups = [row for row in artifacts["journal.jsonl"] if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    assert len(applied) == len(groups)
    assert len(applied) == len(set(applied))
    for row in groups:
        payload = row["payload"]
        effective = payload["request"]["group_effective_at"]
        before, after = payload["owner_evidence_before"], payload["owner_evidence_after"]
        assert before["cutoff"] == after["cutoff"] == effective
        assert before["inspection"]["account_version"] == payload["result"]["account_version_before"]
        assert after["inspection"]["account_version"] == payload["result"]["account_version_after"]
    completion = artifacts["completion.json"]
    assert completion["state"] == "COMPLETE"
    assert completion["endpoint_status"] == status


def _program_hash() -> str:
    digest = sha256()
    assert len(scenario._PAID_PROGRAM) == len(set(scenario._PAID_PROGRAM))
    for name in sorted(scenario._PAID_PROGRAM):
        encoded = name.encode()
        digest.update(len(encoded).to_bytes(8, "big") + encoded
                      + sha256((scenario._ROOT / name).read_bytes()).digest())
    return digest.hexdigest()


@pytest.mark.parametrize(
    "case", ["wrong_selector", "mixed_inputs", "missing_input", "malformed_input", "nested_run_id"],
)
def test_paid_input_rejections_happen_before_store_or_owner(tmp_path, monkeypatch, case):
    run_id = "preowner-" + case
    request, raw = _request(run_id)
    selector = scenario._PAID_SELECTOR
    historical = None
    supplied = raw
    if case == "wrong_selector":
        selector = scenario._P3_SELECTOR
    elif case == "mixed_inputs":
        historical = encode_historical_run_input(request.historical_input)
    elif case == "missing_input":
        supplied = None
    elif case == "malformed_input":
        supplied = raw + b"\n"
    elif case == "nested_run_id":
        wrong_run = replace(request.historical_input, run_id="other-run")
        supplied = encode_spider_execution_input(_input(wrong_run))

    monkeypatch.setattr(scenario._Store, "create", lambda *_: pytest.fail("store created before paid admission"))
    monkeypatch.setattr(
        scenario._ReplayComposition, "_from_execution_run",
        classmethod(lambda *_: pytest.fail("native owner constructed before paid admission")),
    )
    result = scenario.run_spider_scenario(
        str(tmp_path), run_id, selector, historical, execution_input=supplied,
    )
    assert result == dict(
        run_id=run_id, outcome="REJECTED", reason="UNSUPPORTED_CONFIGURATION", endpoint_status=None,
    )
    assert not (tmp_path / run_id).exists()


def test_initial_native_capture_failure_is_attempted_once_and_finalized_as_failed_prefix(tmp_path, monkeypatch):
    original_capture = wire.ScenarioCodec.capture_snapshot
    failed = [False]

    def fail_first(codec, request):
        if not failed[0]:
            failed[0] = True
            raise RuntimeError("initial snapshot failure")
        return original_capture(codec, request)

    monkeypatch.setattr(wire.ScenarioCodec, "capture_snapshot", fail_first)
    request, raw, result = _run(tmp_path, "initial-failed")

    assert failed[0]
    assert result == dict(
        run_id=request.historical_input.run_id, outcome="ADMITTED", reason="EVIDENCE_FAILED",
        endpoint_status="FAILED",
    )
    directory = tmp_path / request.historical_input.run_id
    artifacts = scenario._admit(directory)["artifacts"]
    assert artifacts["journal.jsonl"] == []
    assert artifacts["status.json"]["processed_boundary"] is None
    assert artifacts["report.jsonl"][0]["initial_owner_evidence"] is None
    assert artifacts["paid_endpoint.json"]["failure"]["evidence_error"]["reason"] == "initial snapshot failure"
    assert (directory / "execution_input.json").read_bytes() == raw


def test_final_native_capture_failure_is_admitted_as_truthful_failed_prefix(tmp_path, monkeypatch):
    original_capture = wire.ScenarioCodec.capture_snapshot
    failed = [False]

    def fail_paid_final(codec, request):
        if request["snapshot_id"].startswith("PAID_FINAL_") and not failed[0]:
            failed[0] = True
            raise RuntimeError("final snapshot failure")
        return original_capture(codec, request)

    monkeypatch.setattr(wire.ScenarioCodec, "capture_snapshot", fail_paid_final)
    request, _, result = _run(tmp_path, "final-failed")

    assert failed[0]
    assert result["outcome"] == "ADMITTED"
    assert result["endpoint_status"] == "FAILED"
    artifacts = scenario._admit(tmp_path / request.historical_input.run_id)["artifacts"]
    assert artifacts["paid_endpoint.json"]["failure"]["evidence_error"]["reason"] == "final snapshot failure"
    assert artifacts["completion.json"]["evidence_scope"] == "FAILED_PREFIX"


def test_paid_same_run_id_is_no_clobber_and_rejection_has_exact_four_fields(tmp_path):
    request, raw = _request("paid-no-clobber")
    first = scenario.run_spider_scenario(
        str(tmp_path), request.historical_input.run_id, scenario._PAID_SELECTOR,
        execution_input=raw,
    )
    before = {path.name: path.read_bytes() for path in (tmp_path / request.historical_input.run_id).iterdir()}
    second = scenario.run_spider_scenario(
        str(tmp_path), request.historical_input.run_id, scenario._PAID_SELECTOR,
        execution_input=raw,
    )
    assert first["outcome"] == "ADMITTED"
    assert second == dict(
        run_id=request.historical_input.run_id, outcome="FAILED", reason="PUBLICATION_FAILED",
        endpoint_status=None,
    )
    assert {path.name: path.read_bytes() for path in (tmp_path / request.historical_input.run_id).iterdir()} == before


def test_old_scenario_selectors_keep_three_field_result_and_no_paid_input(tmp_path):
    result = scenario.run_spider_scenario(str(tmp_path), "old-selector", "UNKNOWN_SELECTOR")
    assert tuple(result) == ("run_id", "outcome", "reason")


@pytest.mark.parametrize(
    ("reason", "error"),
    [
        (True, _store.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN")),
        (True, _store.SpiderRunStoreError(
            "PERSISTENCE_FAILED", "PERSISTENCE_FAILED",
            dict(kind="PERSISTENCE", reason="PUBLICATION_DURABILITY_UNKNOWN"),
        )),
        (False, ValueError("PUBLICATION_DURABILITY_UNKNOWN")),
    ],
)
def test_paid_durability_unknown_recognizes_only_typed_publication_uncertainty(reason, error):
    assert scenario._paid_durability_unknown(error) is reason


def test_paid_append_directory_fsync_uncertainty_stops_without_more_publication_or_dispatch(
    tmp_path, monkeypatch,
):
    original_fsync = os.fsync
    original_publish = _store.SpiderRunStore._publish
    original_apply = wire.ScenarioCodec.apply_group
    hit = []
    publications = []
    applications = []

    def counted_publish(store, name, raw):
        publications.append(name)
        return original_publish(store, name, raw)

    def counted_apply(codec, group):
        applications.append(group["group_id"])
        return original_apply(codec, group)

    def uncertain_directory_sync(descriptor):
        frame = inspect.currentframe()
        while frame is not None and frame.f_code.co_name != "append_journal":
            frame = frame.f_back
        if (not hit and frame is not None and stat.S_ISDIR(os.fstat(descriptor).st_mode)):
            hit.append((len(publications), len(applications)))
            raise OSError("review append directory sync failure")
        return original_fsync(descriptor)

    monkeypatch.setattr(_store.SpiderRunStore, "_publish", counted_publish)
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", counted_apply)
    monkeypatch.setattr(os, "fsync", uncertain_directory_sync)
    request, _, result = _run(tmp_path, "paid-append-sync-unknown")

    assert hit
    assert result == dict(
        run_id=request.historical_input.run_id, outcome="DURABILITY_UNKNOWN",
        reason="PUBLICATION_DURABILITY_UNKNOWN", endpoint_status=None,
    )
    assert (len(publications), len(applications)) == hit[0]
    directory = tmp_path / request.historical_input.run_id
    assert not (directory / "completion.json").exists()
    assert scenario._admit(directory)["decision"] != "ACCEPT"


@pytest.mark.parametrize(
    ("phase", "expected_applications"),
    [("BEFORE", 0), ("AFTER", 1)],
)
def test_paid_source_capture_failure_preserves_original_cause_and_native_prefix(
    tmp_path, monkeypatch, phase, expected_applications,
):
    original_capture = wire.ScenarioCodec.capture_snapshot
    original_apply = wire.ScenarioCodec.apply_group
    applications = []
    failed = []

    def fail_source_capture(codec, request):
        if request["snapshot_id"].startswith(f"PAID_SOURCE_0_{phase}_TRADING") and not failed:
            failed.append(True)
            raise RuntimeError(f"Source {phase.lower()}: a/b; ExactCase")
        return original_capture(codec, request)

    def count_apply(codec, group):
        applications.append(group["group_id"])
        return original_apply(codec, group)

    monkeypatch.setattr(wire.ScenarioCodec, "capture_snapshot", fail_source_capture)
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", count_apply)
    request, _, result = _run(tmp_path, f"paid-source-{phase.lower()}-failure")

    assert failed and len(applications) == expected_applications
    assert result["outcome"] == "FAILED"
    assert result["reason"] == "PERSISTENCE_FAILED"
    directory = tmp_path / request.historical_input.run_id
    status_raw = (directory / "status.json").read_bytes()
    status = cast(dict[str, Any], scenario._decode(status_raw[:-1]))
    assert status["state"] == "FAILED"
    assert status["failure_reason"] == "PERSISTENCE_FAILED"
    assert status["primary_failure"] == dict(
        kind="PERSISTENCE", reason="SOURCE_EVIDENCE_CAPTURE_FAILED",
    )
    assert not (directory / "completion.json").exists()
    assert scenario._admit(directory)["decision"] != "ACCEPT"
    endpoint = cast(dict[str, Any], scenario._decode((directory / "paid_endpoint.json").read_bytes()[:-1]))
    assert endpoint["failure"]["primary"] == dict(
        kind="RuntimeError", reason=f"Source {phase.lower()}: a/b; ExactCase", native_failure=None,
    )
    assert endpoint["failure"]["evidence_error"] is None
    if phase == "AFTER":
        artifacts = [cast(dict[str, Any], scenario._decode(line))
                     for line in (directory / "journal.jsonl").read_bytes().splitlines()]
        assert applications and status["persisted_boundary"] is not None
        assert all(row["record_kind"] != "SOURCE_GROUP_RESULT" for row in artifacts)
        assert [action["classification"] for action in endpoint["actions"]] == ["COMMITTED", "PENDING"]
        assert endpoint["pending_obligations"]["positions"]


def test_paid_committed_group_with_missing_native_binding_retains_terminal_prefix(tmp_path, monkeypatch):
    monkeypatch.setattr(wire.ScenarioCodec, "historical_order_binding", lambda *_: None)
    request, raw = _request("paid-missing-native-binding")
    result = scenario.run_spider_scenario(
        str(tmp_path), request.historical_input.run_id, scenario._PAID_SELECTOR,
        execution_input=raw,
    )

    assert result == dict(
        run_id=request.historical_input.run_id, outcome="ADMITTED",
        reason="DEPENDENCY_BINDING_CONFLICT", endpoint_status="FAILED",
    )
    directory = tmp_path / request.historical_input.run_id
    artifacts = scenario._admit(directory)["artifacts"]
    groups = [row for row in artifacts["journal.jsonl"] if row["record_kind"] == "SOURCE_GROUP_RESULT"]
    assert groups and groups[0]["payload"]["result"]["classification"] == "COMMITTED"
    assert artifacts["paid_endpoint.json"]["failure"]["primary"]["reason"] == "DEPENDENCY_BINDING_CONFLICT"


def test_paid_ordinary_journal_write_failure_is_unadmitted_without_completion(tmp_path, monkeypatch):
    original_write = _store._write
    applications = []
    original_apply = wire.ScenarioCodec.apply_group

    def fail_journal(path, raw, append=False):
        if path.name == "journal.jsonl" and append:
            raise _store.SpiderRunStoreError("PUBLICATION_FAILED")
        return original_write(path, raw, append)

    def count_apply(codec, group):
        applications.append(group["group_id"])
        return original_apply(codec, group)

    monkeypatch.setattr(_store, "_write", fail_journal)
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", count_apply)
    request, _, result = _run(tmp_path, "paid-journal-write-failed")

    assert result["outcome"] == "FAILED"
    assert result["reason"] == "PERSISTENCE_FAILED"
    assert applications == []
    directory = tmp_path / request.historical_input.run_id
    assert not (directory / "completion.json").exists()
    assert scenario._admit(directory)["decision"] != "ACCEPT"


@pytest.mark.parametrize(("phase", "expected_applications"), [("BEFORE", 0), ("AFTER", 1)])
def test_paid_source_diagnostic_survives_one_status_temp_open_failure_without_retry(
    tmp_path, monkeypatch, phase, expected_applications,
):
    original_capture = wire.ScenarioCodec.capture_snapshot
    original_apply = wire.ScenarioCodec.apply_group
    original_open = os.open
    capture_failed = []
    failed_status_open = []
    applications = []

    def fail_source_capture(codec, request):
        if request["snapshot_id"].startswith(f"PAID_SOURCE_0_{phase}_TRADING") and not capture_failed:
            capture_failed.append(True)
            raise RuntimeError(f"Source {phase.lower()}: lost status; ExactCase")
        return original_capture(codec, request)

    def fail_one_status_temp_open(path, flags, *args, **kwargs):
        if (Path(path).name == ".status.json.tmp" and capture_failed and not failed_status_open):
            failed_status_open.append(True)
            raise OSError("one-time status temp open failure")
        return original_open(path, flags, *args, **kwargs)

    def count_apply(codec, group):
        applications.append(group["group_id"])
        return original_apply(codec, group)

    monkeypatch.setattr(wire.ScenarioCodec, "capture_snapshot", fail_source_capture)
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", count_apply)
    monkeypatch.setattr(os, "open", fail_one_status_temp_open)
    request, _, result = _run(tmp_path, f"paid-status-open-{phase.lower()}")

    assert capture_failed and failed_status_open == [True]
    assert len(applications) == expected_applications
    assert result["outcome"] == "FAILED"
    assert result["reason"] == "PERSISTENCE_FAILED"
    directory = tmp_path / request.historical_input.run_id
    assert not (directory / "completion.json").exists()
    assert scenario._admit(directory)["decision"] != "ACCEPT"
    diagnostic = cast(
        dict[str, Any], scenario._decode((directory / "paid_endpoint.json").read_bytes()[:-1]),
    )
    assert diagnostic["failure"]["primary"] == dict(
        kind="RuntimeError", reason=f"Source {phase.lower()}: lost status; ExactCase", native_failure=None,
    )
