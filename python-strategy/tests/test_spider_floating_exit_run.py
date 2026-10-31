"""Durable, admitted execution of the closed source-derived exit candidate."""

from __future__ import annotations

from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_store, spider_scenario_run
from src.core.backtest.spider_floating_exit_input import encode_spider_floating_exit_input
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import (
    canonical_bytes,
    decode_canonical,
    decode_jsonl,
    encode_jsonl,
    execution_context,
    floating_exit_context,
)
from src.core.backtest.spider_run_execution_evidence import build_execution_reconciliation
from src.core.backtest.spider_run_execution_schema import (
    _FLOATING_SCHEMA,
    execution_context_from_input,
    floating_exit_context_from_input,
    validate_execution_artifact,
)
from src.core.backtest.spider_execution_input import encode_spider_execution_input
from src.core.backtest.spider_scenario_run import run_spider_scenario
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_floating_exit_host import (
    FIXED_REMAINING_COST_TP_V1,
    PRODUCTS,
    _candidate,
    _reprice_cut,
    _seeded_candidate_run,
    _FIFTEEN,
    D,
)
from test_spider_historical_input import _initial_account_state


_SELECTOR = "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1"


@pytest.mark.slow
def test_candidate_native_exit_is_persisted_and_readmitted_with_closed_family(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    run = _seeded_candidate_run(minutes=22)
    cutoff = run.range_start_ms + _FIFTEEN
    run = _reprice_cut(run, cutoff, ratios={PRODUCTS[0]: D("1.04")})
    request = _candidate(run, FIXED_REMAINING_COST_TP_V1,
                         cutoff=run.range_start_ms + 20 * 60_000)
    raw_input = encode_spider_floating_exit_input(request)
    reconciliations = []
    rebuild = spider_scenario_run._build_execution_reconciliation

    def capture_reconciliation(*args, **kwargs):
        raw_artifacts = cast(dict[str, bytes], args[1])
        captured = tmp_path / "captured-before-reconciliation"
        captured.mkdir(exist_ok=True)
        for name, raw in raw_artifacts.items():
            (captured / name).write_bytes(raw)
        result = rebuild(*args, **kwargs)
        reconciliations.append(result)
        return result

    monkeypatch.setattr(spider_scenario_run, "_build_execution_reconciliation", capture_reconciliation)

    result = run_spider_scenario(
        str(tmp_path), run.run_id, _SELECTOR, floating_exit_input=raw_input,
    )

    assert set(result) == {"run_id", "outcome", "reason", "endpoint_status"}
    assert result["run_id"] == run.run_id
    assert reconciliations and reconciliations[0]["result"] == "OK", [
        row["name"] for row in reconciliations[0]["checks"] if row["result"] != "OK"
    ]
    assert result["outcome"] == "ADMITTED" and result["reason"] is None
    assert result["endpoint_status"] in ("PAID_FLAT", "INCOMPLETE")
    run_path = tmp_path / run.run_id
    admitted = admit_spider_run(run_path)
    assert admitted["decision"] == "ACCEPT"
    artifacts = cast(dict[str, Any], admitted["artifacts"])
    for name, schema_version in _FLOATING_SCHEMA.items():
        artifact = artifacts[name]
        row = artifact[0] if name.endswith(".jsonl") else artifact
        assert row["schema_version"] == schema_version
    attempt = artifacts["attempt.json"]
    assert attempt["run_contract_id"] == _SELECTOR
    assert attempt["terminal_policy"] == "PAID_CLOSE_RESEARCH_V1"
    assert attempt["execution_context"]["observation_projection_id"] == (
        "CHAT_BAYES_CLOSED15M_SOURCE14_V1"
    )
    report = artifacts["report.jsonl"][0]
    observation = report["floating_observation"]
    assert observation["decision_batches"]
    assert observation["product_exits"][0]["product_id"] == PRODUCTS[0]
    assert observation["product_exits"][0]["status"] == "EXITED"
    assert D(artifacts["paid_endpoint.json"]["owner_evidence"]["inspection"]["total_fees"]) > D(0)

    raw_artifacts = {
        name: (run_path / name).read_bytes()
        for name in (
            "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
            "paid_endpoint.json", "report.jsonl", "reconciliation.json", "completion.json",
        )
    }
    assert build_execution_reconciliation(run.run_id, raw_artifacts)["result"] == "OK"

    def changed_json(files: dict[str, bytes], name: str, mutate) -> dict[str, bytes]:
        changed = dict(files)
        row = cast(dict[str, Any], decode_canonical(changed[name].removesuffix(b"\n")))
        mutate(row)
        changed[name] = canonical_bytes(row) + b"\n"
        return changed

    def changed_report(files: dict[str, bytes], mutate) -> dict[str, bytes]:
        changed = dict(files)
        rows = decode_jsonl(changed["report.jsonl"])
        mutate(cast(dict[str, Any], rows[0]))
        changed["report.jsonl"] = encode_jsonl(cast(list[object], rows))
        return changed

    null_observation = changed_report(
        raw_artifacts, lambda row: row.__setitem__("floating_observation", None),
    )
    with pytest.raises(ValueError):
        build_execution_reconciliation(run.run_id, {
            name: null_observation[name] for name in (
                "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
                "paid_endpoint.json", "report.jsonl",
            )
        })

    def assert_check_failed(files: dict[str, bytes], check_name: str) -> None:
        semantic_files = {
            name: files[name] for name in (
                "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
                "paid_endpoint.json", "report.jsonl",
            )
        }
        result = build_execution_reconciliation(run.run_id, semantic_files)
        assert result["result"] == "FAILED"
        assert next(row for row in result["checks"] if row["name"] == check_name)["result"] == "FAILED"

    assert_check_failed(changed_json(
        raw_artifacts, "paid_endpoint.json",
        lambda row: row["actions"].reverse(),
    ), "ACTION_BINDINGS")
    assert_check_failed(changed_json(
        raw_artifacts, "paid_endpoint.json",
        lambda row: row["actions"][0].__setitem__("action_id", "unbound-action"),
    ), "ACTION_BINDINGS")
    assert_check_failed(changed_json(
        raw_artifacts, "attempt.json",
        lambda row: row.__setitem__("run_id", "candidate-other-run"),
    ), "INPUT_IDENTITY")
    assert_check_failed(changed_report(raw_artifacts, lambda row: row["floating_observation"][
        "decision_batches"][0].__setitem__("observed_at", row["floating_observation"][
            "decision_batches"][0]["observed_at"] + 1)), "REPORT_PROJECTION")
    assert_check_failed(changed_report(raw_artifacts, lambda row: row["floating_observation"][
        "decision_batches"][0].__setitem__("valuation_context_id", "0" * 64)), "REPORT_PROJECTION")
    assert_check_failed(changed_report(raw_artifacts, lambda row: row["floating_observation"][
        "decision_batches"][0].__setitem__("lifecycle", "AWAITING_CANCEL_EFFECTIVE")), "REPORT_PROJECTION")
    assert_check_failed(changed_report(raw_artifacts, lambda row: row["floating_observation"][
        "decision_batches"][0]["rows"][0].__setitem__(
            "gross", str(D(row["floating_observation"]["decision_batches"][0]["rows"][0]["gross"]) + D(1)))),
        "REPORT_PROJECTION")
    assert_check_failed(changed_report(raw_artifacts, lambda row: row["floating_observation"][
        "product_exits"][0].__setitem__("status", "INCOMPLETE")), "REPORT_PROJECTION")
    assert_check_failed(changed_report(raw_artifacts, lambda row: row["floating_observation"][
        "product_exits"][0].__setitem__(
            "completed_at", row["floating_observation"]["product_exits"][0]["completed_at"] + 1)),
        "REPORT_PROJECTION")
    assert_check_failed(changed_report(raw_artifacts, lambda row: row.__setitem__(
        "source_evidence_refs", [])), "REPORT_PROJECTION")

    candidate_context = cast(dict[str, Any], decode_canonical(
        raw_artifacts["attempt.json"].removesuffix(b"\n"),
    ))[
        "execution_context"]
    candidate_validated = floating_exit_context(candidate_context)
    assert validate_execution_artifact(
        "attempt.json", raw_artifacts["attempt.json"], execution_context=candidate_validated,
    )
    original_context = execution_context(execution_context_from_input(
        encode_spider_execution_input(request.execution_input),
    ))
    with pytest.raises(ValueError):
        validate_execution_artifact(
            "attempt.json", raw_artifacts["attempt.json"], execution_context=original_context,
        )

    repeated = run_spider_scenario(
        str(tmp_path), run.run_id, _SELECTOR, floating_exit_input=raw_input,
    )
    assert repeated == dict(
        run_id=run.run_id, outcome="FAILED", reason="PUBLICATION_FAILED", endpoint_status=None,
    )

    assert_check_failed(changed_report(raw_artifacts, lambda row: row["floating_observation"][
        "decision_batches"][0]["rows"][0].__setitem__("reason", None)), "REPORT_PROJECTION")


def test_candidate_rejects_mixed_or_invalid_entry_before_run_directory(tmp_path: Path):
    run = _seeded_candidate_run(minutes=22)
    request = _candidate(run, FIXED_REMAINING_COST_TP_V1,
                         cutoff=run.range_start_ms + 20 * 60_000)
    raw_input = encode_spider_floating_exit_input(request)

    mismatched = run_spider_scenario(
        str(tmp_path), run.run_id, "SPIDER_PAID_RESEARCH_RUN_V1", floating_exit_input=raw_input,
    )
    assert mismatched["outcome"] == "REJECTED"
    assert not (tmp_path / run.run_id).exists()

    mixed = run_spider_scenario(
        str(tmp_path), run.run_id, _SELECTOR,
        execution_input=b"{}", floating_exit_input=raw_input,
    )
    assert mixed["outcome"] == "REJECTED"
    assert not (tmp_path / run.run_id).exists()

    malformed = run_spider_scenario(
        str(tmp_path), run.run_id, _SELECTOR, floating_exit_input=raw_input + b"\n",
    )
    assert malformed["outcome"] == "REJECTED"
    assert not (tmp_path / run.run_id).exists()


def test_candidate_generic_failure_keeps_diagnostic_chain_and_durable_unknown_priority(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture,
):
    run = _seeded_candidate_run(minutes=22)
    request = _candidate(run, FIXED_REMAINING_COST_TP_V1,
                         cutoff=run.range_start_ms + 20 * 60_000)
    raw_input = encode_spider_floating_exit_input(request)

    def fail_composition(cls, candidate):
        raise RuntimeError("candidate composition diagnostic marker")

    monkeypatch.setattr(
        spider_scenario_run._ReplayComposition,
        "_from_floating_exit_run",
        classmethod(fail_composition),
    )
    failed = run_spider_scenario(str(tmp_path), run.run_id, _SELECTOR, floating_exit_input=raw_input)
    assert failed["outcome"] == "FAILED" and failed["reason"] == "UNEXPECTED_EXCEPTION"
    assert "candidate composition diagnostic marker" in caplog.text
    failure_path = tmp_path / run.run_id
    status = cast(dict[str, Any], decode_canonical(
        (failure_path / "status.json").read_bytes().removesuffix(b"\n"),
    ))
    assert status["state"] == "FAILED" and status["failure_reason"] == "UNEXPECTED_EXCEPTION"
    assert not (failure_path / "completion.json").exists()

    second_root = tmp_path / "durability"
    second_root.mkdir()
    original_publish = spider_run_store.SpiderRunStore._publish

    def unknown_after_attempt(self, name, raw):
        if name == "attempt.json":
            original_publish(self, name, raw)
            raise spider_run_store.SpiderRunStoreError(
                "PUBLICATION_DURABILITY_UNKNOWN", "PERSISTENCE_FAILED",
            )
        original_publish(self, name, raw)

    monkeypatch.setattr(spider_run_store.SpiderRunStore, "_publish", unknown_after_attempt)
    unknown = run_spider_scenario(str(second_root), run.run_id, _SELECTOR, floating_exit_input=raw_input)
    assert unknown["outcome"] == "DURABILITY_UNKNOWN"
    assert unknown["reason"] == "PUBLICATION_DURABILITY_UNKNOWN"
    assert not (second_root / run.run_id / "completion.json").exists()


@pytest.mark.slow
def test_failed_terminal_cache_preserves_verified_batch_and_product_prefix(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    run = _seeded_candidate_run(minutes=37)
    config = cast(dict[str, Any], decode_canonical(run.configuration_bytes))
    positions = [
        dict(
            product_id=product, side="LONG", quantity_contracts="1",
            lots=[dict(seed_execution_id=f"FLOATING-SEED-{index}", seed_sequence=index,
                       strategy_id=run.strategy_identity, quantity_contracts="1",
                       entry_price=str(D(100 + index)))],
        )
        for index, product in enumerate(PRODUCTS[:2])
    ]
    config["positions"] = positions
    configuration_bytes = canonical_bytes(config)
    run = replace(
        run,
        configuration_bytes=configuration_bytes,
        configuration_sha256=sha256(configuration_bytes).hexdigest(),
        initial_account_state=_initial_account_state(cash="10000", positions=positions),
    )
    first_cutoff = run.range_start_ms + _FIFTEEN
    second_cutoff = run.range_start_ms + 30 * 60_000
    run = _reprice_cut(run, first_cutoff, ratios={PRODUCTS[1]: D("1.04")})
    run = _reprice_cut(run, second_cutoff, ratios={PRODUCTS[0]: D("1.04")})
    request = _candidate(run, FIXED_REMAINING_COST_TP_V1,
                         cutoff=run.range_start_ms + 35 * 60_000)
    raw_input = encode_spider_floating_exit_input(request)
    replay = spider_scenario_run._ReplayComposition
    original_factory = replay._from_floating_exit_run
    reconciliations = []
    original_reconcile = spider_scenario_run._build_execution_reconciliation

    def capture_reconciliation(*args, **kwargs):
        raw_files = cast(dict[str, bytes], args[1])
        captured = tmp_path / "terminal-prefix-before-reconciliation"
        captured.mkdir(exist_ok=True)
        for name, raw in raw_files.items():
            (captured / name).write_bytes(raw)
        reconciliation = original_reconcile(*args, **kwargs)
        reconciliations.append(reconciliation)
        return reconciliation

    monkeypatch.setattr(
        spider_scenario_run, "_build_execution_reconciliation", capture_reconciliation,
    )

    def stop_after_candidate_request(cls, candidate):
        composition = original_factory(candidate)
        endpoint = composition._paid_endpoint
        assert endpoint is not None
        original_request = endpoint.request_product_exit
        request_count = 0

        def fail_second_product_request(composition, product_id, reason):
            nonlocal request_count
            request_count += 1
            if request_count == 2:
                raise spider_scenario_run._ScheduleError("INJECTED_POLICY_REQUEST_FAILURE")
            return original_request(composition, product_id, reason)

        endpoint.request_product_exit = fail_second_product_request
        return composition

    monkeypatch.setattr(replay, "_from_floating_exit_run", classmethod(stop_after_candidate_request))
    result = run_spider_scenario(
        str(tmp_path), run.run_id, _SELECTOR, floating_exit_input=raw_input,
    )
    assert reconciliations and reconciliations[0]["result"] == "OK", [
        row["name"] for row in reconciliations[0]["checks"] if row["result"] != "OK"
    ]
    assert result["outcome"] == "ADMITTED"
    assert result["endpoint_status"] == "FAILED"
    admitted = admit_spider_run(tmp_path / run.run_id)
    assert admitted["decision"] == "ACCEPT"
    report_rows = cast(dict[str, Any], admitted["artifacts"])["report.jsonl"]
    report = cast(dict[str, Any], report_rows[0])
    assert report["scheduler_observation"]["terminal"]["kind"] == "HISTORICAL_MARKET_CACHE"
    observation = report["floating_observation"]
    assert len(observation["decision_batches"]) == 2
    assert [row["product_id"] for row in observation["product_exits"]] == [PRODUCTS[1]]
    assert [row["product_id"] for row in observation["decision_batches"][0]["rows"]
            if row["active"] and row["reason"] is not None] == [PRODUCTS[1]]
    assert [row["product_id"] for row in observation["decision_batches"][1]["rows"]
            if row["active"] and row["reason"] is not None] == [PRODUCTS[0]]
    captured = tmp_path / "terminal-prefix-before-reconciliation"
    semantic_files = {
        name: (captured / name).read_bytes() for name in (
            "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
            "paid_endpoint.json", "report.jsonl",
        )
    }
    report_rows = decode_jsonl(semantic_files["report.jsonl"])
    report_row = cast(dict[str, Any], report_rows[0])
    scheduler = cast(dict[str, Any], report_row["scheduler_observation"])
    current_time = scheduler["current_time"]
    future_pending_times = [
        row["key"]["visible_at"] for row in scheduler["records"]
        if row["classification"] == "PENDING" and row["key"]["visible_at"] > current_time
    ]
    assert future_pending_times
    completed_at = report_row["floating_observation"]["product_exits"][0]["completed_at"]
    assert completed_at is not None
    future_rows = decode_jsonl(semantic_files["report.jsonl"])
    future_report = cast(dict[str, Any], future_rows[0])
    future_report["floating_observation"]["product_exits"][0]["completed_at"] = min(future_pending_times)
    future_files = dict(semantic_files)
    future_files["report.jsonl"] = encode_jsonl(cast(list[object], future_rows))
    future_completion = build_execution_reconciliation(run.run_id, future_files)
    assert future_completion["result"] == "FAILED"
    assert next(row for row in future_completion["checks"]
                if row["name"] == "REPORT_PROJECTION")["result"] == "FAILED"
    report_row["floating_observation"]["product_exits"].clear()
    semantic_files["report.jsonl"] = encode_jsonl(cast(list[object], report_rows))
    dropped_prior_operation = build_execution_reconciliation(run.run_id, semantic_files)
    assert dropped_prior_operation["result"] == "FAILED"
    assert next(row for row in dropped_prior_operation["checks"]
                if row["name"] == "REPORT_PROJECTION")["result"] == "FAILED"


@pytest.mark.slow
def test_initial_candidate_failure_allows_only_zero_event_null_observation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
):
    run = _seeded_candidate_run()
    request = _candidate(run)
    raw_input = encode_spider_floating_exit_input(request)
    candidate_context = floating_exit_context_from_input(raw_input)
    candidate_hash = candidate_context["execution_input_sha256"]
    original_factory = _ReplayComposition._from_floating_exit_run.__func__

    def fail_initial_capture(cls, candidate_request):
        composition = original_factory(cls, candidate_request)

        def fail_capture(*_args, **_kwargs):
            raise RuntimeError("injected initial owner capture failure")

        composition.capture_owner_evidence = fail_capture
        return composition

    monkeypatch.setattr(
        _ReplayComposition, "_from_floating_exit_run", classmethod(fail_initial_capture),
    )
    result = run_spider_scenario(
        str(tmp_path), run.run_id, _SELECTOR, floating_exit_input=raw_input,
    )
    assert result == dict(
        run_id=run.run_id, outcome="ADMITTED", reason="EVIDENCE_FAILED", endpoint_status="FAILED",
    )
    admitted = admit_spider_run(tmp_path / run.run_id)
    assert admitted["decision"] == "ACCEPT"
    artifacts = cast(dict[str, Any], admitted["artifacts"])
    report = cast(dict[str, Any], artifacts["report.jsonl"][0])
    endpoint = cast(dict[str, Any], artifacts["paid_endpoint.json"])
    assert report["floating_observation"] == dict(
        schema_version="spider_floating_exit_observation_v1",
        run_contract_sha256=candidate_hash, decision_batches=[], product_exits=[],
    )
    assert endpoint["status"] == "FAILED" and endpoint["owner_evidence"] is None
    assert artifacts["journal.jsonl"] == []

    raw_files = {
        name: (tmp_path / run.run_id / name).read_bytes()
        for name in (
            "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
            "paid_endpoint.json", "report.jsonl", "reconciliation.json", "completion.json",
        )
    }
    assert build_execution_reconciliation(run.run_id, raw_files)["result"] == "OK"
    null_observation = decode_jsonl(raw_files["report.jsonl"])
    cast(dict[str, Any], null_observation[0])["floating_observation"] = None
    raw_files["report.jsonl"] = encode_jsonl(cast(list[object], null_observation))
    assert build_execution_reconciliation(run.run_id, {
        name: raw_files[name] for name in (
            "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
            "paid_endpoint.json", "report.jsonl",
        )
    })["result"] == "OK"
