"""Frozen H12 acceptance matrix for all Spider formal entry points."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
import json
from typing import Any, cast

from examples import run_spider_scenario_replay as cli
from src.core.backtest import spider_formal_runner as formal
from src.core.backtest.spider_historical_input import (
    encode_historical_run_input,
    validate_historical_input,
)
from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest_runner import BacktestRunner
from src.core.research_backtest_runner import ResearchBacktestRunner
from test_spider_historical_input import (
    ANSWERS,
    INPUTS,
)
from test_spider_historical_oracle import _oracle_run

_INPUTS_SHA256 = "f0ef34464e3ccefd4cc7ea21485698e3f6398a0f323354bb7ac41156e4c3dd51"
_ANSWERS_SHA256 = "30f412b0c1ddd1ae6ac779cb6c821efbef96f410f751781add33bc02991b1d5e"
_MATRIX = (
    (
        "H02", "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
        "87eccb0a0f2077b8968a5be038dbaef9abb1a6caf3b78c579a0ce2e7efad6748",
        "high-low",
    ),
    (
        "H09", "OHLC4_OPEN_HIGH_LOW_CLOSE_V1",
        "13f1f7f904777d482a69b46b5515ba91ea7c65a4471da1555381a07e283a6252",
        "high-low",
    ),
    (
        "H09", "OHLC4_OPEN_LOW_HIGH_CLOSE_V1",
        "f6682a54d8bc1694387d0655e3d79ce05c9e1b0a3dbaef1ce44eecf292f9d2a2",
        "low-high",
    ),
)
_ENTRIES = ("cli", "research", "full")


def _executions(projection: formal.SpiderP02Projection) -> list[tuple[str, str, str]]:
    result = []
    value = cast(dict[str, Any], projection.value)
    for row in cast(list[dict[str, Any]], value["journal"]):
        if row["record_kind"] != "HISTORICAL_MARKET_RESULT":
            continue
        for product in row["payload"]["result"]["products"]:
            result.extend(
                (fill["order_id"], fill["quantity_contracts"], fill["price"])
                for fill in product["fills"]
            )
    return result


def _assert_h02_coverage(projection: formal.SpiderP02Projection) -> None:
    value = projection.value
    journal = cast(list[dict[str, Any]], cast(dict[str, Any], value)["journal"])
    market_rows = [row for row in journal if row["record_kind"] == "HISTORICAL_MARKET_RESULT"]
    assert [
        (
            row["payload"]["result"]["raw_time_ms"],
            [
                (fill["order_id"], fill["quantity_contracts"], fill["price"])
                for product in row["payload"]["result"]["products"]
                for fill in product["fills"]
            ],
            row["payload"]["owner_inspection_after"]["cash"],
            row["payload"]["owner_inspection_after"]["total_fees"],
        )
        for row in market_rows
    ] == [
        (1_790_640_000_000, [("H02-A-LONG", "1", "100")], "999.9", "0.1"),
        (1_790_640_020_000, [("H02-A-LONG", "1", "100")], "999.8", "0.2"),
    ]
    # Duplicate-fact idempotency lives in the independent P3 oracle test; the
    # strict P3 DTO/P02 journal does not persist that test-only injected delivery.
    final_callbacks = [
        row["payload"] for row in journal
        if row["record_kind"] == "CALLBACK_RESULT" and row["payload"]["actions"]
    ]
    assert len(final_callbacks) == 1
    assert [event["kind"] for event in final_callbacks[0]["policy_events"]] == ["cancel", "send"]
    assert len(final_callbacks[0]["actions"]) == 3

    endpoint = cast(dict[str, Any], cast(dict[str, Any], value)["endpoint"])
    observation = cast(dict[str, Any], endpoint["scheduler_observation"])
    assert len(observation["callback_actions"]) == 3
    final_owner = cast(dict[str, Any], endpoint["final_owner_evidence"])
    open_orders = cast(dict[str, Any], final_owner["open_orders_fact"])
    orders = cast(dict[str, Any], open_orders["immutable_payload"])["rows"]
    assert len(orders) == 3
    assert len(observation["pending_keys"]) == 1
    assert sum(poll["status"] == "IN_PROGRESS" for poll in observation["polls"]) == 1


def _assert_h09_path(projection: formal.SpiderP02Projection, model_id: str) -> None:
    high_low = model_id == "OHLC4_OPEN_HIGH_LOW_CLOSE_V1"
    expected = (
        [("H09-SHORT", "1", "105"), ("H09-LONG", "1", "95")]
        if high_low else
        [("H09-LONG", "1", "95"), ("H09-SHORT", "1", "105")]
    )
    assert _executions(projection) == expected
    value = cast(dict[str, Any], projection.value)
    endpoint = cast(dict[str, Any], value["endpoint"])
    endpoint = cast(dict[str, Any], endpoint["final_owner_evidence"])
    assert endpoint["inspection"]["cash"] == "1009.8"
    assert endpoint["inspection"]["total_fees"] == "0.2"
    assert endpoint["trading_fact"]["immutable_payload"]["equity"] == "1009.8"


def test_frozen_h12_formal_acceptance_matrix(tmp_path, capsys):
    assert sha256(INPUTS.read_bytes()).hexdigest() == _INPUTS_SHA256
    assert sha256(ANSWERS.read_bytes()).hexdigest() == _ANSWERS_SHA256
    runs_root = tmp_path / "runs"
    inputs_root = tmp_path / "inputs"
    runs_root.mkdir()
    inputs_root.mkdir()

    for case, model_id, expected_hash, path_label in _MATRIX:
        _, oracle_input = _oracle_run(case, model_id=model_id)
        assert oracle_input.model_id == model_id
        assert validate_historical_input(oracle_input) == expected_hash
        projections: dict[str, list[formal.SpiderP02Projection]] = {
            entry: [] for entry in _ENTRIES
        }
        run_ids: set[str] = set()

        for entry in _ENTRIES:
            repeat_run_ids = tuple(
                f"{case.lower()}-{path_label}-{entry}-{suffix}"
                for suffix in ("x", "longer-repeat")
            )
            assert len(set(repeat_run_ids)) == 2
            assert len({len(run_id) for run_id in repeat_run_ids}) == 2
            for run_id in repeat_run_ids:
                assert run_id not in run_ids
                run_ids.add(run_id)
                run_input = replace(oracle_input, run_id=run_id)
                assert replace(run_input, run_id=oracle_input.run_id) == oracle_input
                assert validate_historical_input(run_input) == expected_hash
                raw_input = encode_historical_run_input(run_input)

                if entry == "cli":
                    input_path = inputs_root / f"{run_id}.json"
                    input_path.write_bytes(raw_input)
                    exit_code = cli.main([
                        "--output-root", str(runs_root), "--run-id", run_id,
                        "--scenario-selector", "SPIDER_HISTORICAL_RESEARCH_RUN_V1",
                        "--historical-input", str(input_path),
                    ])
                    captured = capsys.readouterr()
                    assert exit_code == 0 and captured.err == ""
                    assert captured.out == json.dumps(
                        dict(run_id=run_id, outcome="ADMITTED", reason=None),
                        sort_keys=True, separators=(",", ":"),
                    ) + "\n"
                    projection = formal.project_admitted_spider_run(runs_root / run_id)
                    assert projection is not None
                elif entry == "research":
                    result = ResearchBacktestRunner.run_spider_historical(
                        output_root=str(runs_root), run_id=run_id, historical_input=raw_input,
                    )
                    assert result.runner_kind == "research"
                    assert result.outcome == "ADMITTED" and result.projection is not None
                    projection = result.projection
                else:
                    result = BacktestRunner.run_spider_historical(
                        output_root=str(runs_root), run_id=run_id, historical_input=raw_input,
                    )
                    assert result.runner_kind == "full"
                    assert result.outcome == "ADMITTED" and result.projection is not None
                    projection = result.projection
                    admitted = admit_spider_run(runs_root / run_id)
                    assert admitted["decision"] == "ACCEPT"
                    original_rows = admitted["artifacts"]["report.jsonl"]
                    assert result.report_rows == original_rows
                    assert all(row["run_id"] == run_id for row in result.report_rows or [])
                    normalized_rows = deepcopy(original_rows)
                    for row in normalized_rows:
                        row.pop("run_id")
                    assert projection.report_rows == normalized_rows
                    detached_rows = result.report_rows
                    assert detached_rows is not None
                    detached_rows[0]["run_id"] = "mutated-copy"
                    assert result.report_rows == original_rows
                    assert admitted["artifacts"]["report.jsonl"] == original_rows

                assert projection.sha256 == sha256(projection.canonical_json).hexdigest()
                projections[entry].append(projection)

        all_projections = [projection for entry in _ENTRIES for projection in projections[entry]]
        assert len(all_projections) == 6
        assert len({item.canonical_json for item in all_projections}) == 1
        assert len({item.sha256 for item in all_projections}) == 1
        for entry in _ENTRIES:
            assert projections[entry][0].canonical_json == projections[entry][1].canonical_json
            assert projections[entry][0].sha256 == projections[entry][1].sha256

        if case == "H02":
            _assert_h02_coverage(projections["full"][0])
        else:
            _assert_h09_path(projections["full"][0], model_id)
