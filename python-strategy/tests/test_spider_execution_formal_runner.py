"""Formal projection and runner/CLI transport for admitted paid execution runs."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
import json
from typing import cast

import pytest

from examples import run_spider_scenario_replay as cli
from src.core import backtest_runner as full
from src.core import research_backtest_runner as research
from src.core.backtest import spider_formal_runner as formal
from src.core.backtest import spider_scenario_run as scenario
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest.spider_execution_input import encode_spider_execution_input
from src.core.backtest.synthetic_scenario_codec import ScenarioCodec
from test_spider_scenario_execution_run import _request


def _has_run_id(value: object) -> bool:
    if type(value) is dict:
        return "run_id" in value or any(_has_run_id(item) for item in value.values())
    if type(value) is list:
        return any(_has_run_id(item) for item in value)
    return False


def _expected_projection(artifacts: dict[str, object]) -> dict[str, object]:
    def without_run_id(value: object) -> object:
        if type(value) is dict:
            return {key: without_run_id(item) for key, item in value.items() if key != "run_id"}
        if type(value) is list:
            return [without_run_id(item) for item in value]
        return value

    endpoint = cast(dict[str, object], without_run_id(artifacts["paid_endpoint.json"]))
    reconciliation = cast(dict[str, object], without_run_id(artifacts["reconciliation.json"]))
    completion = cast(dict[str, object], without_run_id(artifacts["completion.json"]))
    for entry in cast(list[dict[str, object]], completion["artifacts"]):
        entry.pop("sha256")
        entry.pop("byte_count")
    completion["endpoint_state_digest"] = endpoint
    completion["reconciliation_digest"] = reconciliation
    return {
        "schema_version": "spider_execution_projection_v1",
        "attempt": without_run_id(artifacts["attempt.json"]),
        "status": without_run_id(artifacts["status.json"]),
        "journal": without_run_id(artifacts["journal.jsonl"]),
        "execution_input": without_run_id(artifacts["execution_input.json"]),
        "paid_endpoint": endpoint,
        "report_rows": without_run_id(artifacts["report.jsonl"]),
        "reconciliation": reconciliation,
        "completion": completion,
    }


@pytest.mark.parametrize(
    "model_id",
    ["OHLC4_OPEN_HIGH_LOW_CLOSE_V1", "OHLC4_OPEN_LOW_HIGH_CLOSE_V1"],
)
def test_three_paid_entrances_produce_identical_projection_and_detached_rows(
    tmp_path, capsys, model_id,
):
    suffix = "hl" if model_id == "OHLC4_OPEN_HIGH_LOW_CLOSE_V1" else "lh"
    cli_id = f"formal-cli-{suffix}"
    research_id = f"formal-research-distinct-length-{suffix}"
    full_id = f"formal-full-third-distinct-length-{suffix}"
    assert len({len(cli_id), len(research_id), len(full_id)}) == 3

    cli_request, _ = _request(cli_id)
    cli_request = replace(
        cli_request,
        historical_input=replace(cli_request.historical_input, model_id=model_id),
    )
    cli_raw = encode_spider_execution_input(cli_request)
    input_path = tmp_path / "execution-input.json"
    input_path.write_bytes(cli_raw)
    assert cli.main([
        "--output-root", str(tmp_path), "--run-id", cli_id,
        "--scenario-selector", scenario._PAID_SELECTOR,
        "--execution-input", str(input_path),
    ]) == 0
    cli_output = capsys.readouterr()
    assert cli_output.err == ""
    assert json.loads(cli_output.out) == dict(
        run_id=cli_id, outcome="ADMITTED", reason=None, endpoint_status="PAID_FLAT",
    )

    research_request, _ = _request(research_id)
    research_request = replace(
        research_request,
        historical_input=replace(research_request.historical_input, model_id=model_id),
    )
    research_raw = encode_spider_execution_input(research_request)
    full_request, _ = _request(full_id)
    full_request = replace(
        full_request,
        historical_input=replace(full_request.historical_input, model_id=model_id),
    )
    full_raw = encode_spider_execution_input(full_request)
    research_result = research.ResearchBacktestRunner.run_spider_execution(
        output_root=str(tmp_path), run_id=research_id, execution_input=research_raw,
    )
    full_result = full.BacktestRunner.run_spider_execution(
        output_root=str(tmp_path), run_id=full_id, execution_input=full_raw,
    )

    assert research_request.policy_id == full_request.policy_id == cli_request.policy_id
    assert research_request.historical_input.model_id == full_request.historical_input.model_id == model_id
    assert research_result.runner_kind == "research" and full_result.runner_kind == "full"
    for result in (research_result, full_result):
        assert result.schema_version == "spider_execution_formal_result_v1"
        assert (result.outcome, result.reason, result.admission, result.endpoint_status) == (
            "ADMITTED", None, "ACCEPT", "PAID_FLAT",
        )
        assert result.projection is not None
    research_projection = research_result.projection
    full_projection = full_result.projection
    assert research_projection is not None and full_projection is not None
    assert research_projection.canonical_json == full_projection.canonical_json
    assert research_projection.sha256 == full_projection.sha256 == sha256(
        research_projection.canonical_json,
    ).hexdigest()

    cli_artifacts = admit_spider_run(tmp_path / cli_id)["artifacts"]
    expected = _expected_projection(cli_artifacts)
    cli_projection_raw = canonical_bytes(expected)
    assert research_projection.canonical_json == cli_projection_raw
    assert research_projection.value == expected
    assert not _has_run_id(research_projection.value)
    research_artifacts = admit_spider_run(tmp_path / research_id)["artifacts"]
    assert research_result.report_rows == research_artifacts["report.jsonl"]
    full_artifacts = admit_spider_run(tmp_path / full_id)["artifacts"]
    assert full_result.report_rows == full_artifacts["report.jsonl"]
    original = deepcopy(full_result.report_rows)
    assert original is not None
    detached = full_result.report_rows
    assert detached is not None
    detached[0]["account_cash"] = "tampered detached copy"
    assert full_result.report_rows == original


@pytest.mark.parametrize(
    ("mode", "status"),
    [
        ("flat", "PAID_FLAT"),
        ("incomplete", "INCOMPLETE"),
        ("liquidated_flat", "LIQUIDATED_FLAT"),
        ("insolvent", "LIQUIDATED_INSOLVENT"),
    ],
)
def test_paid_runner_retains_endpoint_statuses(tmp_path, mode, status):
    request, raw = _request("formal-status-" + mode, mode=mode)
    result = research.ResearchBacktestRunner.run_spider_execution(
        output_root=str(tmp_path), run_id=request.historical_input.run_id,
        execution_input=raw,
    )
    assert result.outcome == "ADMITTED"
    assert result.endpoint_status == status
    assert result.projection is not None


def test_genuine_native_failure_is_admitted_and_projected_truthfully(tmp_path, monkeypatch):
    original = ScenarioCodec.capture_snapshot
    failed = [False]

    def fail_initial(codec, request):
        if not failed[0]:
            failed[0] = True
            raise RuntimeError("formal initial snapshot failure")
        return original(codec, request)

    monkeypatch.setattr(ScenarioCodec, "capture_snapshot", fail_initial)
    request, raw = _request("formal-native-failed")
    result = full.BacktestRunner.run_spider_execution(
        output_root=str(tmp_path), run_id=request.historical_input.run_id,
        execution_input=raw,
    )
    assert failed[0]
    assert result.outcome == "ADMITTED" and result.admission == "ACCEPT"
    assert result.endpoint_status == "FAILED" and result.projection is not None
    assert result.reason == "EVIDENCE_FAILED"
    assert cast(dict[str, object], result.projection.value["paid_endpoint"])["status"] == "FAILED"


@pytest.mark.parametrize(
    ("outcome", "status", "code"),
    [
        ("ADMITTED", "PAID_FLAT", 0),
        ("ADMITTED", "INCOMPLETE", 1),
        ("ADMITTED", "LIQUIDATED_FLAT", 0),
        ("ADMITTED", "LIQUIDATED_INSOLVENT", 0),
        ("ADMITTED", "FAILED", 3),
        ("REJECTED", None, 2),
        ("FAILED", "FAILED", 3),
        ("DURABILITY_UNKNOWN", None, 3),
    ],
)
def test_paid_cli_exit_codes_and_exact_transport_output(
    monkeypatch, tmp_path, capsys, outcome, status, code,
):
    calls = []
    scenario_result = dict(
        run_id="cli-result", outcome=outcome,
        reason=None if outcome == "ADMITTED" else "FIXTURE_REASON",
        endpoint_status=status,
    )
    monkeypatch.setattr(
        scenario, "run_spider_scenario",
        lambda *args, **kwargs: (calls.append((args, kwargs)), scenario_result)[1],
    )
    input_path = tmp_path / "raw-wrapper.json"
    input_path.write_bytes(b"opaque exact bytes\n")
    assert cli.main([
        "--output-root", str(tmp_path), "--run-id", "cli-result",
        "--scenario-selector", scenario._PAID_SELECTOR,
        "--execution-input", str(input_path),
    ]) == code
    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == scenario_result
    assert calls == [(
        (str(tmp_path), "cli-result", scenario._PAID_SELECTOR),
        {"execution_input": b"opaque exact bytes\n"},
    )]


def test_execution_input_and_historical_input_are_mutually_exclusive(tmp_path, monkeypatch):
    monkeypatch.setattr(scenario, "run_spider_scenario", lambda *_args, **_kwargs: pytest.fail("must not run"))
    with pytest.raises(SystemExit) as raised:
        cli.main([
            "--output-root", str(tmp_path), "--run-id", "exclusive",
            "--scenario-selector", scenario._PAID_SELECTOR,
            "--historical-input", "old.json", "--execution-input", "paid.json",
        ])
    assert raised.value.code == 2


def test_paid_runner_rejection_and_durability_unknown_have_no_projection(monkeypatch, tmp_path):
    for outcome, status in (("REJECTED", None), ("FAILED", "FAILED"), ("DURABILITY_UNKNOWN", None)):
        monkeypatch.setattr(
            formal.spider_scenario_run, "run_spider_scenario",
            lambda *_args, _outcome=outcome, _status=status, **_kwargs: dict(
                run_id="formal-error", outcome=_outcome, reason="FIXTURE_REASON",
                endpoint_status=_status,
            ),
        )
        monkeypatch.setattr(formal, "admit_spider_run", lambda *_: pytest.fail("must not admit"))
        result = formal._invoke_spider_execution(
            output_root=str(tmp_path), run_id="formal-error", execution_input=b"bytes",
            runner_kind="full",
        )
        assert result.outcome == outcome and result.endpoint_status == status
        assert result.projection is None and result.admission is None and result.report_rows is None


def test_paid_program_hash_covers_new_entry_sources_once():
    sources = {
        "python-strategy/src/core/backtest/spider_formal_runner.py",
        "python-strategy/src/core/backtest_runner.py",
        "python-strategy/src/core/research_backtest_runner.py",
        "python-strategy/examples/run_spider_scenario_replay.py",
    }
    assert sources <= set(scenario._PAID_PROGRAM)
    assert len(scenario._PAID_PROGRAM) == len(set(scenario._PAID_PROGRAM))
    assert not sources.intersection(scenario._PROGRAM)
    assert not sources.intersection(scenario._P3_PROGRAM)
