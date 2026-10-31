"""Formal transport for admitted source-derived floating-exit runs."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal
from hashlib import sha256
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, cast

import pytest

from examples import run_spider_scenario_replay as cli
from src.core import backtest_runner as full
from src.core import research_backtest_runner as research
from src.core.backtest import spider_formal_runner as formal
from src.core.backtest import spider_scenario_run as scenario
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest.spider_floating_exit_input import encode_spider_floating_exit_input
from src.core.backtest.spider_floating_exit_policy import (
    BAYES_REMAINING_COST_RISK_V1,
    FIXED_REMAINING_COST_TP_V1,
)
from test_spider_floating_exit_host import (
    PRODUCTS,
    _candidate,
    _reprice_cut,
    _seeded_candidate_run,
    _FIFTEEN,
)


_ROOT = Path(__file__).parents[1]
_CLI = _ROOT / "examples/run_spider_scenario_replay.py"
_SELECTOR = "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1"


def _candidate_input(run_id: str, decision_policy: str) -> bytes:
    run = replace(_seeded_candidate_run(minutes=22), run_id=run_id)
    cutoff = run.range_start_ms + _FIFTEEN
    if decision_policy == FIXED_REMAINING_COST_TP_V1:
        run = _reprice_cut(run, cutoff, ratios={PRODUCTS[0]: Decimal("1.04")})
    else:
        run = _reprice_cut(run, cutoff, ratios={product: Decimal("0.8") for product in PRODUCTS})
    request = _candidate(
        run, decision_policy, cutoff=run.range_start_ms + 20 * 60_000,
    )
    return encode_spider_floating_exit_input(request)


def _process_env() -> dict[str, str]:
    pythonpath = [str(_ROOT), str(_ROOT / "src")]
    inherited = os.environ.get("PYTHONPATH")
    if inherited:
        pythonpath.append(inherited)
    return dict(os.environ, PYTHONPATH=os.pathsep.join(pythonpath))


def _without_run_id(value: object) -> object:
    if type(value) is dict:
        return {
            key: _without_run_id(item)
            for key, item in cast(dict[str, object], value).items()
            if key != "run_id"
        }
    if type(value) is list:
        return [_without_run_id(item) for item in cast(list[object], value)]
    return value


def _expected_projection(artifacts: dict[str, Any]) -> dict[str, object]:
    endpoint = cast(dict[str, object], _without_run_id(artifacts["paid_endpoint.json"]))
    reconciliation = cast(dict[str, object], _without_run_id(artifacts["reconciliation.json"]))
    completion = cast(dict[str, object], _without_run_id(artifacts["completion.json"]))
    metadata = completion["artifacts"]
    assert type(metadata) is list
    for entry in cast(list[dict[str, object]], metadata):
        entry.pop("sha256")
        entry.pop("byte_count")
    completion["endpoint_state_digest"] = endpoint
    completion["reconciliation_digest"] = reconciliation
    return {
        "schema_version": "spider_floating_exit_projection_v1",
        "attempt": _without_run_id(artifacts["attempt.json"]),
        "status": _without_run_id(artifacts["status.json"]),
        "journal": _without_run_id(artifacts["journal.jsonl"]),
        "execution_input": _without_run_id(artifacts["execution_input.json"]),
        "paid_endpoint": endpoint,
        "report_rows": _without_run_id(artifacts["report.jsonl"]),
        "reconciliation": reconciliation,
        "completion": completion,
    }


# ~613s locally; GitHub runners are 2-3x slower. Speedup is a tracked follow-up.
@pytest.mark.timeout(3600)
@pytest.mark.slow
def test_fixed_candidate_projection_matches_full_research_and_cli(tmp_path):
    names = ("floating-cli", "floating-full-long-id", "floating-research-id")
    assert len({len(name) for name in names}) == 3
    projections = []
    results = []

    cli_id = names[0]
    cli_input = _candidate_input(cli_id, FIXED_REMAINING_COST_TP_V1)
    input_path = tmp_path / "floating-exit-input.json"
    input_path.write_bytes(cli_input)
    process = subprocess.run(
        [sys.executable, str(_CLI), "--output-root", str(tmp_path), "--run-id", cli_id,
         "--scenario-selector", _SELECTOR, "--floating-exit-input", str(input_path)],
        cwd=_ROOT, env=_process_env(), capture_output=True, text=True,
        encoding="utf-8", timeout=1800,
    )
    assert process.stderr == ""
    cli_result = json.loads(process.stdout)
    assert set(cli_result) == {"run_id", "outcome", "reason", "endpoint_status"}
    assert cli_result["run_id"] == cli_id and cli_result["outcome"] == "ADMITTED"
    assert cli_result["endpoint_status"] in ("PAID_FLAT", "INCOMPLETE")
    assert process.returncode == (1 if cli_result["endpoint_status"] == "INCOMPLETE" else 0)
    cli_artifacts = cast(dict[str, Any], admit_spider_run(tmp_path / cli_id)["artifacts"])
    cli_endpoint = cast(dict[str, Any], cli_artifacts["paid_endpoint.json"])
    cli_report = cast(list[dict[str, Any]], cli_artifacts["report.jsonl"])[0]
    cli_observation = cast(dict[str, Any], cli_report["floating_observation"])
    assert any(
        row["product_id"] == PRODUCTS[0] and row["reason"] == "TP"
        for batch in cli_observation["decision_batches"] for row in batch["rows"]
    )
    assert any(
        row["product_id"] == PRODUCTS[0] and row["reason"] == "TP" and row["status"] == "EXITED"
        for row in cli_observation["product_exits"]
    )
    assert Decimal(cli_endpoint["owner_evidence"]["inspection"]["total_fees"]) > 0
    projections.append(_expected_projection(cli_artifacts))

    for runner, name, kind in (
        (full.BacktestRunner, names[1], "full"),
        (research.ResearchBacktestRunner, names[2], "research"),
    ):
        raw = _candidate_input(name, FIXED_REMAINING_COST_TP_V1)
        result = runner.run_spider_floating_exit(
            output_root=str(tmp_path), run_id=name, floating_exit_input=raw,
        )
        assert result.schema_version == "spider_floating_exit_formal_result_v1"
        assert result.runner_kind == kind
        assert (result.outcome, result.admission) == ("ADMITTED", "ACCEPT")
        assert result.endpoint_status == cli_result["endpoint_status"]
        assert result.projection is not None
        assert result.projection.value["schema_version"] == "spider_floating_exit_projection_v1"
        assert set(result.projection.value) == {
            "schema_version", "attempt", "status", "journal", "execution_input",
            "paid_endpoint", "report_rows", "reconciliation", "completion",
        }
        artifacts = cast(dict[str, Any], admit_spider_run(tmp_path / name)["artifacts"])
        endpoint = cast(dict[str, Any], artifacts["paid_endpoint.json"])
        report = cast(list[dict[str, Any]], artifacts["report.jsonl"])[0]
        observation = cast(dict[str, Any], report["floating_observation"])
        assert any(
            row["product_id"] == PRODUCTS[0] and row["reason"] == "TP"
            for batch in observation["decision_batches"] for row in batch["rows"]
        )
        assert any(
            row["product_id"] == PRODUCTS[0] and row["reason"] == "TP" and row["status"] == "EXITED"
            for row in observation["product_exits"]
        )
        assert Decimal(endpoint["owner_evidence"]["inspection"]["total_fees"]) > 0
        expected = _expected_projection(artifacts)
        assert result.projection.canonical_json == canonical_bytes(expected)
        assert result.projection.value == expected
        assert result.projection.sha256 == sha256(result.projection.canonical_json).hexdigest()
        assert result.report_rows == artifacts["report.jsonl"]
        projections.append(result.projection.value)
        results.append(result)

    assert projections[0] == projections[1] == projections[2]
    for result in results:
        assert result.projection is not None
        detached = result.projection.report_rows
        before = deepcopy(detached)
        detached[0]["run_id"] = "mutated-detached-copy"
        assert result.projection.report_rows == before
        rows = result.report_rows
        assert rows is not None
        rows[0]["run_id"] = "mutated-formal-copy"
        assert result.report_rows != rows


@pytest.mark.slow
def test_bayes_candidate_formal_result_retains_real_native_exit_and_fee(tmp_path):
    run_id = "floating-bayes-native"
    raw = _candidate_input(run_id, BAYES_REMAINING_COST_RISK_V1)
    result = research.ResearchBacktestRunner.run_spider_floating_exit(
        output_root=str(tmp_path), run_id=run_id, floating_exit_input=raw,
    )
    assert result.outcome == "ADMITTED" and result.admission == "ACCEPT"
    assert result.projection is not None
    value = result.projection.value
    assert value["schema_version"] == "spider_floating_exit_projection_v1"
    endpoint = cast(dict[str, Any], value["paid_endpoint"])
    assert Decimal(endpoint["owner_evidence"]["inspection"]["total_fees"]) > 0
    report = cast(list[dict[str, Any]], value["report_rows"])[0]
    observation = cast(dict[str, Any], report["floating_observation"])
    assert any(
        row["product_id"] == PRODUCTS[0] and row["reason"] == "RISK"
        for batch in observation["decision_batches"] for row in batch["rows"]
    )
    assert any(
        row["product_id"] == PRODUCTS[0] and row["reason"] == "RISK" and row["status"] == "EXITED"
        for row in observation["product_exits"]
    )


@pytest.mark.parametrize(
    ("outcome", "status", "exit_code"),
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
def test_candidate_cli_transports_fixed_result_and_exit_code(
    tmp_path, capsys, monkeypatch, outcome, status, exit_code,
):
    calls = []
    response = dict(
        run_id="candidate-cli-result", outcome=outcome,
        reason=None if outcome == "ADMITTED" else "FIXTURE_REASON",
        endpoint_status=status,
    )
    monkeypatch.setattr(
        scenario, "run_spider_scenario",
        lambda *args, **kwargs: (calls.append((args, kwargs)), response)[1],
    )
    input_path = tmp_path / "candidate-input.json"
    input_path.write_bytes(b"exact candidate bytes\n")
    assert cli.main([
        "--output-root", str(tmp_path), "--run-id", "candidate-cli-result",
        "--scenario-selector", _SELECTOR,
        "--floating-exit-input", str(input_path),
    ]) == exit_code
    captured = capsys.readouterr()
    assert captured.err == ""
    assert json.loads(captured.out) == response
    assert calls == [((str(tmp_path), "candidate-cli-result", _SELECTOR), {
        "floating_exit_input": b"exact candidate bytes\n",
    })]


def test_candidate_formal_rejection_is_pre_owner_and_keeps_null_status(tmp_path, monkeypatch):
    monkeypatch.setattr(
        scenario, "run_spider_scenario",
        lambda *_args, **_kwargs: dict(
            run_id="invalid-candidate", outcome="REJECTED",
            reason="UNSUPPORTED_CONFIGURATION", endpoint_status=None,
        ),
    )
    monkeypatch.setattr(formal, "admit_spider_run", lambda *_: pytest.fail("rejection must not admit"))
    result = full.BacktestRunner.run_spider_floating_exit(
        output_root=str(tmp_path), run_id="invalid-candidate", floating_exit_input=b"bad bytes",
    )
    assert result.schema_version == "spider_floating_exit_formal_result_v1"
    assert (result.outcome, result.reason, result.admission, result.endpoint_status) == (
        "REJECTED", "UNSUPPORTED_CONFIGURATION", None, None,
    )
    assert result.projection is None and result.report_rows is None


@pytest.mark.parametrize(
    ("outcome", "status"),
    [("REJECTED", None), ("FAILED", "FAILED"), ("DURABILITY_UNKNOWN", None)],
)
def test_candidate_formal_failure_transport_does_not_attempt_admission(
    tmp_path, monkeypatch, outcome, status,
):
    monkeypatch.setattr(
        scenario, "run_spider_scenario",
        lambda *_args, **_kwargs: dict(
            run_id="candidate-formal-failure", outcome=outcome,
            reason="FIXTURE_REASON", endpoint_status=status,
        ),
    )
    monkeypatch.setattr(formal, "admit_spider_run", lambda *_: pytest.fail("must not admit"))
    result = research.ResearchBacktestRunner.run_spider_floating_exit(
        output_root=str(tmp_path), run_id="candidate-formal-failure", floating_exit_input=b"bytes",
    )
    assert (result.outcome, result.endpoint_status, result.reason) == (
        outcome, status, "FIXTURE_REASON",
    )
    assert result.admission is None and result.projection is None and result.report_rows is None


def test_floating_program_manifest_covers_transport_sources_once():
    expected = {
        "python-strategy/src/core/backtest/spider_formal_runner.py",
        "python-strategy/src/core/backtest_runner.py",
        "python-strategy/src/core/research_backtest_runner.py",
        "python-strategy/examples/run_spider_scenario_replay.py",
    }
    assert expected <= set(scenario._FLOATING_PROGRAM)
    assert len(scenario._FLOATING_PROGRAM) == len(set(scenario._FLOATING_PROGRAM))
    assert not expected.intersection(scenario._PROGRAM)
    assert not expected.intersection(scenario._P3_PROGRAM)


def test_candidate_cli_input_is_mutually_exclusive_with_old_families(tmp_path, monkeypatch):
    monkeypatch.setattr(
        scenario, "run_spider_scenario",
        lambda *_args, **_kwargs: pytest.fail("argument parser must reject mixed input families"),
    )
    with pytest.raises(SystemExit) as raised:
        cli.main([
            "--output-root", str(tmp_path), "--run-id", "mixed-candidate",
            "--scenario-selector", _SELECTOR,
            "--execution-input", "paid.json", "--floating-exit-input", "candidate.json",
        ])
    assert raised.value.code == 2
