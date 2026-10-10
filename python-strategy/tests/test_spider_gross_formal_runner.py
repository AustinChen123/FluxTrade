"""Actual-native formal orchestration for the closed gross execution family."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
import json
from typing import Any, cast

import pytest

from examples import run_spider_scenario_replay as cli
from src.core import backtest_runner as full
from src.core import research_backtest_runner as research
from src.core.backtest import spider_formal_runner as formal
from src.core.backtest import spider_scenario_run as scenario
from src.core.backtest.spider_gross_execution_input import encode_spider_gross_execution_input
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest import spider_run_store
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition, _ScheduleError
from test_spider_gross_execution_input import _over_cap_gross_input
from test_spider_gross_execution_evidence import _native_terminal_request
from test_spider_execution_formal_runner import _expected_projection as _expected_paid_projection


def _for_run(run_id: str):
    request = _over_cap_gross_input(policy_end_offset_ms=30_000)
    history = replace(request.execution_input.historical_input, run_id=run_id)
    execution = replace(request.execution_input, historical_input=history)
    return replace(request, execution_input=execution)


def test_three_gross_entrances_share_actual_native_projection(tmp_path, capsys):
    run_ids = ("gross-cli-run", "gross-research-distinct-length", "gross-full-third-length")
    assert len({len(value) for value in run_ids}) == len(run_ids)
    results: list[formal.SpiderGrossExecutionFormalResult] = []

    input_path = tmp_path / "gross-input.json"
    raw = encode_spider_gross_execution_input(_for_run(run_ids[0]))
    input_path.write_bytes(raw)
    assert cli.main([
        "--output-root", str(tmp_path), "--run-id", run_ids[0],
        "--scenario-selector", scenario._GROSS_SELECTOR,
        "--gross-execution-input", str(input_path),
    ]) == 1
    output = capsys.readouterr()
    assert output.err == ""
    cli_result = json.loads(output.out)
    assert cli_result == dict(
        run_id=run_ids[0], outcome="ADMITTED", reason=None, endpoint_status="INCOMPLETE",
    )
    research_raw = encode_spider_gross_execution_input(_for_run(run_ids[1]))
    full_raw = encode_spider_gross_execution_input(_for_run(run_ids[2]))
    results.append(research.ResearchBacktestRunner.run_spider_gross_execution(
        output_root=str(tmp_path), run_id=run_ids[1], gross_execution_input=research_raw,
    ))
    results.append(full.BacktestRunner.run_spider_gross_execution(
        output_root=str(tmp_path), run_id=run_ids[2], gross_execution_input=full_raw,
    ))
    assert [result.runner_kind for result in results] == ["research", "full"]
    assert all(result.schema_version == "spider_gross_execution_formal_result_v1" for result in results)
    assert all(result.outcome == "ADMITTED" and result.admission == "ACCEPT" for result in results)
    assert all(result.endpoint_status == "INCOMPLETE" for result in results)
    projections = [formal._gross_execution_projection_from_admitted_artifacts(
        admit_spider_run(tmp_path / run_id)["artifacts"],
    ) for run_id in run_ids]
    expected = _expected_paid_projection(admit_spider_run(tmp_path / run_ids[0])["artifacts"])
    expected["schema_version"] = "spider_gross_execution_projection_v1"
    expected_json = canonical_bytes(expected)
    assert projections[0].canonical_json == projections[1].canonical_json == projections[2].canonical_json == expected_json
    assert results[0].projection is not None and results[0].projection.canonical_json == projections[1].canonical_json
    assert results[1].projection is not None and results[1].projection.canonical_json == projections[2].canonical_json
    detached = results[0].report_rows
    assert detached is not None
    detached_gross = cast(dict[str, Any], detached[0]["gross_observation"])
    cast(list[object], detached_gross["observations"]).clear()
    detached_again = results[0].report_rows
    assert detached_again is not None
    assert cast(dict[str, Any], detached_again[0]["gross_observation"])["observations"]

    artifacts = admit_spider_run(tmp_path / run_ids[0])["artifacts"]
    endpoint = cast(dict[str, Any], artifacts["paid_endpoint.json"])
    assert endpoint["schema_version"] == "spider_gross_paid_endpoint_result_v1"
    assert "gross_response" not in endpoint
    report = cast(dict[str, Any], cast(list[object], artifacts["report.jsonl"])[0])
    assert report["schema_version"] == "spider_gross_execution_report_v1"
    assert report["gross_observation"]["schema_version"] == "spider_gross_response_observation_v2"
    assert len(report["gross_observation"]["episodes"]) == 1
    endpoint_actions = cast(list[dict[str, Any]], endpoint["actions"])
    reduction_order_ids = {
        action["order_id"] for action in endpoint_actions
        if action.get("role") == "GROSS_REDUCTION" and action.get("order_id") is not None
    }
    assert len(reduction_order_ids) >= 2
    market_results = [
        cast(dict[str, Any], row["payload"]["result"])
        for row in cast(list[dict[str, Any]], artifacts["journal.jsonl"])
        if row["record_kind"] == "HISTORICAL_MARKET_RESULT"
    ]
    fills = [(product["product_id"], fill) for result in market_results
             for product in result["products"] for fill in product["fills"]
             if fill["order_id"] in reduction_order_ids]
    assert len({fill["order_id"] for _, fill in fills}) >= 2
    assert len({product_id for product_id, _ in fills}) >= 2
    assert {fill["source_event_id"] for _, fill in fills}
    filled_market_row = next(row for row in cast(list[dict[str, Any]], artifacts["journal.jsonl"])
                             if row["record_kind"] == "HISTORICAL_MARKET_RESULT" and any(
        fill["order_id"] in reduction_order_ids
        for product in row["payload"]["result"]["products"] for fill in product["fills"]
    ))
    assert Decimal(filled_market_row["payload"]["owner_inspection_after"]["total_fees"]) > 0
    assert canonical_bytes(projections[0].value) == projections[0].canonical_json
    artifact_names = (
        "attempt.json", "status.json", "journal.jsonl", "execution_input.json",
        "paid_endpoint.json", "report.jsonl", "reconciliation.json", "completion.json",
    )
    prior_bytes = {name: (tmp_path / run_ids[0] / name).read_bytes() for name in artifact_names}
    duplicate = full.BacktestRunner.run_spider_gross_execution(
        output_root=str(tmp_path), run_id=run_ids[0], gross_execution_input=raw,
    )
    assert duplicate.outcome == "FAILED" and duplicate.projection is None
    assert prior_bytes == {name: (tmp_path / run_ids[0] / name).read_bytes() for name in artifact_names}


@pytest.mark.parametrize("bad_input", [b"", b"{}"])
def test_invalid_gross_input_is_rejected_before_store_creation(tmp_path, bad_input):
    result = scenario.run_spider_scenario(
        str(tmp_path), "invalid-gross", scenario._GROSS_SELECTOR,
        gross_execution_input=bad_input,
    )
    assert result["outcome"] == "REJECTED"
    assert not (tmp_path / "invalid-gross").exists()


def test_gross_selector_and_input_are_mutually_exclusive_before_store(tmp_path):
    raw = encode_spider_gross_execution_input(_for_run("mixed-gross"))
    result = scenario.run_spider_scenario(
        str(tmp_path), "mixed-gross", scenario._PAID_SELECTOR,
        execution_input=b"{}", gross_execution_input=raw,
    )
    assert result["outcome"] == "REJECTED"
    assert not (tmp_path / "mixed-gross").exists()


def test_gross_nested_run_id_and_selector_are_checked_before_store(tmp_path):
    raw = encode_spider_gross_execution_input(_for_run("nested-other"))
    mismatch = scenario.run_spider_scenario(
        str(tmp_path), "invocation-id", scenario._GROSS_SELECTOR, gross_execution_input=raw,
    )
    wrong_selector = scenario.run_spider_scenario(
        str(tmp_path), "nested-other", scenario._PAID_SELECTOR, gross_execution_input=raw,
    )
    assert mismatch["outcome"] == wrong_selector["outcome"] == "REJECTED"
    assert not (tmp_path / "invocation-id").exists()
    assert not (tmp_path / "nested-other").exists()


@pytest.mark.parametrize(("cash", "expected"), [
    ("50", "LIQUIDATED_INSOLVENT"), ("99.003", "LIQUIDATED_FLAT"),
])
def test_gross_formal_runner_preserves_native_economic_terminals(tmp_path, cash, expected):
    run_id = f"gross-terminal-{expected.lower()}"
    request = _native_terminal_request(cash)
    history = replace(request.execution_input.historical_input, run_id=run_id)
    request = replace(request, execution_input=replace(request.execution_input, historical_input=history))
    result = full.BacktestRunner.run_spider_gross_execution(
        output_root=str(tmp_path), run_id=run_id,
        gross_execution_input=encode_spider_gross_execution_input(request),
    )
    assert result.outcome == "ADMITTED" and result.admission == "ACCEPT"
    assert result.endpoint_status == expected
    assert result.projection is not None
    report = result.report_rows
    assert report is not None
    gross = cast(dict[str, Any], report[0]["gross_observation"])
    assert gross["current_state"] == expected
    assert gross["final_obligation_state"]["native_state"]["lifecycle"] == expected


def test_gross_formal_ownerless_initial_failure_keeps_null_observation(tmp_path, monkeypatch):
    original = _ReplayComposition._from_gross_response_run

    def fail_initial_capture(request):
        composition = original(request)

        def fail(*args, **kwargs):
            raise RuntimeError("injected owner capture failure")

        composition.capture_owner_evidence = fail
        return composition

    monkeypatch.setattr(_ReplayComposition, "_from_gross_response_run", fail_initial_capture)
    run_id = "gross-ownerless-prefix"
    result = full.BacktestRunner.run_spider_gross_execution(
        output_root=str(tmp_path), run_id=run_id,
        gross_execution_input=encode_spider_gross_execution_input(_for_run(run_id)),
    )
    assert result.outcome == "ADMITTED" and result.admission == "ACCEPT"
    assert result.endpoint_status == "FAILED"
    assert result.projection is not None
    assert cast(dict[str, Any], result.projection.value["paid_endpoint"])["status"] == "FAILED"
    assert result.report_rows is not None
    ownerless_rows = result.report_rows
    assert ownerless_rows is not None
    assert ownerless_rows[0]["gross_observation"] is None


def test_gross_formal_typed_source_scheduler_failure_is_recorded(tmp_path, monkeypatch):
    original = _ReplayComposition._from_gross_response_run

    def fail_source_group(request):
        composition = original(request)
        inner = composition._codec

        class FailedSourceCodec:
            def __getattr__(self, name):
                return getattr(inner, name)

            def apply_gross_response_group(self, group, *, restricted):
                raise _ScheduleError("EXECUTION_FAILURE")

        cast(Any, composition)._codec = FailedSourceCodec()
        return composition

    monkeypatch.setattr(_ReplayComposition, "_from_gross_response_run", fail_source_group)
    run_id = "gross-typed-source-failure"
    result = full.BacktestRunner.run_spider_gross_execution(
        output_root=str(tmp_path), run_id=run_id,
        gross_execution_input=encode_spider_gross_execution_input(_for_run(run_id)),
    )
    assert result.endpoint_status == "FAILED"
    assert result.outcome == "ADMITTED" and result.admission == "ACCEPT"
    assert result.projection is not None
    value = result.projection.value
    endpoint = cast(dict[str, Any], value["paid_endpoint"])
    completion = cast(dict[str, Any], value["completion"])
    assert endpoint["status"] == "FAILED"
    assert cast(dict[str, Any], endpoint["failure"])["primary"]["reason"] == "EXECUTION_FAILURE"
    assert completion["evidence_scope"] == "FAILED_PREFIX"
    assert result.report_rows is not None
    source_failure_rows = result.report_rows
    assert source_failure_rows is not None
    gross_failure = cast(dict[str, Any], source_failure_rows[0]["gross_observation"])
    assert gross_failure["current_state"] == "FAILED"
    assert gross_failure["observations"]
    assert gross_failure["final_obligation_state"] is not None


def test_gross_unexpected_invocation_failure_is_recorded_without_paid_rethrow(tmp_path, monkeypatch):
    def fail_dispatch(self, until):
        raise RuntimeError("injected gross invocation failure")

    monkeypatch.setattr(_ReplayComposition, "_dispatch_due", fail_dispatch)
    run_id = "gross-unexpected-invocation"
    result = full.BacktestRunner.run_spider_gross_execution(
        output_root=str(tmp_path), run_id=run_id,
        gross_execution_input=encode_spider_gross_execution_input(_for_run(run_id)),
    )
    assert result.outcome == "FAILED"
    assert result.reason == "UNEXPECTED_EXCEPTION"
    assert result.projection is None and result.report_rows is None
    status = json.loads((tmp_path / run_id / "status.json").read_text())
    assert status["state"] == "FAILED"


def test_gross_completion_directory_durability_is_unknown_not_admitted(tmp_path, monkeypatch):
    original_sync = spider_run_store._sync

    def fail_after_completion(path):
        if (tmp_path / "gross-durability" / "completion.json").exists():
            raise spider_run_store.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN")
        original_sync(path)

    monkeypatch.setattr(spider_run_store, "_sync", fail_after_completion)
    run_id = "gross-durability"
    result = full.BacktestRunner.run_spider_gross_execution(
        output_root=str(tmp_path), run_id=run_id,
        gross_execution_input=encode_spider_gross_execution_input(_for_run(run_id)),
    )
    assert result.outcome == "DURABILITY_UNKNOWN"
    assert result.reason == "PUBLICATION_DURABILITY_UNKNOWN"
    assert result.projection is None and result.report_rows is None


def test_gross_program_manifest_closes_current_family_once():
    assert len(scenario._GROSS_PROGRAM) == len(set(scenario._GROSS_PROGRAM))
    for path in (
        "python-strategy/src/core/backtest/spider_gross_execution_input.py",
        "python-strategy/src/core/backtest/spider_gross_response.py",
        "python-strategy/src/core/backtest/spider_gross_observation.py",
        "python-strategy/src/core/backtest/spider_gross_artifact_schema.py",
        "python-strategy/src/core/backtest/spider_run_execution_schema.py",
        "python-strategy/src/core/backtest/spider_run_execution_evidence.py",
        "python-strategy/src/core/backtest/spider_gross_execution_evidence.py",
        "python-strategy/src/core/backtest/spider_formal_runner.py",
        "python-strategy/src/core/backtest_runner.py",
        "python-strategy/src/core/research_backtest_runner.py",
        "python-strategy/examples/run_spider_scenario_replay.py",
    ):
        assert scenario._GROSS_PROGRAM.count(path) == 1
    assert not any("bayes" in path.lower() or "source14" in path.lower() for path in scenario._GROSS_PROGRAM)


@pytest.mark.parametrize(("outcome", "status", "expected"), [
    ("ADMITTED", "PAID_FLAT", 0), ("ADMITTED", "LIQUIDATED_FLAT", 0),
    ("ADMITTED", "LIQUIDATED_INSOLVENT", 0), ("ADMITTED", "INCOMPLETE", 1),
    ("ADMITTED", "FAILED", 3), ("REJECTED", None, 2),
    ("FAILED", None, 3), ("DURABILITY_UNKNOWN", None, 3),
])
def test_gross_cli_exit_code_follows_formal_outcome(tmp_path, monkeypatch, capsys, outcome, status, expected):
    monkeypatch.setattr(scenario, "run_spider_scenario", lambda *args, **kwargs: {
        "run_id": "cli-status", "outcome": outcome, "reason": None,
        "endpoint_status": status,
    })
    assert cli.main([
        "--output-root", str(tmp_path), "--run-id", "cli-status",
        "--scenario-selector", scenario._GROSS_SELECTOR,
        "--gross-execution-input", "missing-input.json",
    ]) == expected
    assert json.loads(capsys.readouterr().out)["outcome"] == outcome
