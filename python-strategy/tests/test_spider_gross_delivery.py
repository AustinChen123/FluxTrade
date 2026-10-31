"""Causal native-delivery tests for gross-reduction execution facts."""

from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from typing import Any, cast

import pytest

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.backtest.synthetic_scenario_replay import _CallbackPrefix, _ReplayComposition
from test_spider_gross_execution_input import _over_cap_gross_input
from test_spider_historical_input import _policy_cache


def _dispatch_to_reduction_notice(composition: _ReplayComposition, endpoint: Any):
    """Run actual scheduled events until native has emitted a reduction notice."""
    while True:
        notices = [
            record
            for record in composition._records.values()
            if record["item"].get("kind") == "DELIVERY"
            and record["item"]["delivery"].get("payload_kind") == "EXECUTION_FACT"
            and endpoint.delivery_roles.get(record["item"]["delivery"]["delivery_id"])
            == "GROSS_REDUCTION"
        ]
        if notices:
            return min(notices, key=lambda record: record["key"])
        assert composition._queue
        result = composition._dispatch_due(composition._queue[0][0])
        assert result["classification"] == "SUCCESS", (
            result.get("reason"), result.get("stable_id"), result,
        )


def _duplicate_execution_occurrence(composition: _ReplayComposition, record: dict[str, Any]):
    original = record["item"]["delivery"]
    duplicate = composition._codec.build_delivery(
        cast(wire.Projection, dict(
            schema_version="delivery_projection_v1",
            reference=dict(namespace="SOURCE", fact_id=original["source_fact_id"]),
            payload_kind="EXECUTION_FACT",
            occurrence_index=original["occurrence_index"] + 1,
            schedule_sequence=original["schedule_sequence"] + 1,
            visible_at=original["visible_at"],
        ))
    )
    assert duplicate["delivery_id"] != original["delivery_id"]
    assert canonical_bytes(duplicate["immutable_payload"]) == canonical_bytes(
        original["immutable_payload"]
    )
    return duplicate


def _financial_snapshot(composition: _ReplayComposition) -> tuple[Any, ...]:
    state = composition._codec.inspect_state()
    gross = composition._codec.gross_response_projection()
    return (
        state["cash"], state["total_fees"], state["gross_realized"],
        gross["owner_state_digest"],
        tuple((row["product_id"], row["signed_contracts"], row["gross"])
              for row in gross["products"]),
        tuple((row["order_id"], row["remaining_contracts"], row["status"])
              for row in gross["orders"]),
    )


def _gross_delivery_cut(composition: _ReplayComposition, endpoint: Any) -> tuple[Any, ...]:
    assert endpoint.gross_response is not None
    return (
        _financial_snapshot(composition),
        deepcopy(endpoint.gross_response.rounds),
        deepcopy(endpoint.actions),
        deepcopy((composition._policy.rows, composition._policy.last_capital,
                  composition._policy.events)),
        frozenset(composition._p3_execution_notice_delivered_facts),
    )


@pytest.mark.parametrize("source_running", [False, True])
def test_actual_gross_reduction_notice_duplicate_is_consumer_deduplicated(source_running, monkeypatch):
    request = _over_cap_gross_input(
        policy_end_offset_ms=150_000, settlement_extension_ms=180_000,
    )
    if source_running:
        execution = request.execution_input
        history = execution.historical_input
        history = replace(history, initial_policy_cache=_policy_cache(running=True))
        request = replace(request, execution_input=replace(execution, historical_input=history))
    history = request.execution_input.historical_input
    source_cache = cast(dict[str, Any], decode_canonical(history.initial_policy_cache))
    assert source_cache["running"] is source_running
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    evidence = []
    composition._evidence_callback = lambda kind, key, item: evidence.append(
        (kind, key, deepcopy(item))
    )

    notice = _dispatch_to_reduction_notice(composition, endpoint)
    delivery = notice["item"]["delivery"]
    action = next(
        row for row in endpoint.actions
        if row["role"] == "GROSS_REDUCTION"
        and row["product_id"] == delivery["immutable_payload"]["product_id"]
        and row["classification"] == "COMMITTED"
    )
    assert action["role"] == "GROSS_REDUCTION"
    assert delivery["immutable_payload"]["state"] in {"partial", "filled"}
    assert D(delivery["immutable_payload"]["cumulative_filled_size_contracts"]) > 0
    duplicate = _duplicate_execution_occurrence(composition, notice)

    assert composition._enqueue(dict(kind="DELIVERY", delivery=duplicate))["classification"] == "PENDING"
    product_id = delivery["immutable_payload"]["product_id"]
    assert composition._policy.running is source_running
    assert composition._paid_source_control_enabled(delivery["visible_at"])
    assert composition._paid_source_control_enabled(delivery["visible_at"], product_id)
    duplicate_cut = {}
    original_deliver = composition._deliver_queued

    def measure_duplicate(record):
        candidate = record["item"].get("delivery", {})
        if candidate.get("delivery_id") == duplicate["delivery_id"]:
            duplicate_cut["key"] = record["key"]
            duplicate_cut["before"] = _gross_delivery_cut(composition, endpoint)
            result = original_deliver(record)
            duplicate_cut["after"] = _gross_delivery_cut(composition, endpoint)
            return result
        return original_deliver(record)

    monkeypatch.setattr(composition, "_deliver_queued", measure_duplicate)
    result = composition._dispatch_due(notice["key"][0])
    assert result["classification"] == "SUCCESS", result
    duplicate_record = composition._records[(2, duplicate["delivery_id"])]
    assert duplicate_cut["key"] == duplicate_record["key"]
    assert duplicate_record["result"]["classification"] == "SUCCESS"
    assert duplicate_record["result"]["events"] == []
    assert any(
        kind == "CALLBACK_RESULT" and item["delivery_id"] == duplicate["delivery_id"]
        and item["outcome"] == "CONSUMER_DEDUPLICATED"
        for kind, _, item in evidence
    )
    assert duplicate_cut["after"] == duplicate_cut["before"]
    # The original native fact was consumed; its duplicate must neither replay
    # it nor erase later same-time native progress in the scheduler.
    assert delivery["source_fact_id"] in duplicate_cut["before"][4]
    assert delivery["source_fact_id"] in duplicate_cut["after"][4]
    same_time_native_notices = [
        row for row in composition._records.values()
        if row["item"].get("kind") == "DELIVERY"
        and row["item"]["delivery"].get("payload_kind") == "EXECUTION_FACT"
        and endpoint.delivery_roles.get(row["item"]["delivery"]["delivery_id"])
        == "GROSS_REDUCTION"
        and row["item"]["delivery"]["delivery_id"] != duplicate["delivery_id"]
        and row["key"][0] == duplicate_record["key"][0]
    ]
    assert same_time_native_notices
    assert all(row.get("result", {}).get("classification") == "SUCCESS"
               for row in same_time_native_notices)
    final_cut = _gross_delivery_cut(composition, endpoint)
    assert len(final_cut[1]) >= len(duplicate_cut["after"][1])
    assert {row["action_id"] for row in duplicate_cut["after"][2]} <= {
        row["action_id"] for row in final_cut[2]
    }
    assert duplicate["delivery_id"] not in endpoint.pending_delivery_ids


def test_actual_gross_reduction_notice_callback_fault_retains_financial_fact(monkeypatch):
    request = _over_cap_gross_input(
        policy_end_offset_ms=150_000, settlement_extension_ms=180_000,
    )
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    notice = _dispatch_to_reduction_notice(composition, endpoint)
    delivery = notice["item"]["delivery"]
    delivery_id = delivery["delivery_id"]
    action = next(
        row for row in endpoint.actions
        if row["role"] == "GROSS_REDUCTION"
        and row["product_id"] == delivery["immutable_payload"]["product_id"]
        and row["classification"] == "COMMITTED"
    )
    assert action["role"] == "GROSS_REDUCTION"
    assert delivery["immutable_payload"]["state"] in {"partial", "filled"}
    assert D(delivery["immutable_payload"]["cumulative_filled_size_contracts"]) > 0
    before = _financial_snapshot(composition)
    rounds_before = len(endpoint.gross_response.rounds)
    original_capture = composition._capture_callback
    failures = []

    def fail_reduction_notice(candidate):
        if candidate["delivery_id"] == delivery_id:
            failures.append(candidate["delivery_id"])
            return _CallbackPrefix((), RuntimeError("injected gross reduction notice callback failure"))
        return original_capture(candidate)

    monkeypatch.setattr(composition, "_capture_callback", fail_reduction_notice)
    result = composition._dispatch_due(notice["key"][0])
    assert result["classification"] == "TERMINAL"
    assert result["reason"] == "CALLBACK_FAILED"
    assert failures == [delivery_id]
    assert _financial_snapshot(composition) == before
    assert delivery_id in endpoint.pending_delivery_ids
    assert endpoint.delivery_roles[delivery_id] == "GROSS_REDUCTION"
    assert action in endpoint.actions
    assert action["action_id"] == endpoint.gross_response.rounds[-1]["action_ids"][0]
    assert action["order_id"] == delivery["immutable_payload"]["order_id"]
    assert len(endpoint.gross_response.rounds) == rounds_before
    terminal_result = endpoint.result(composition)
    assert terminal_result["status"] == "FAILED"
    assert terminal_result["gross_response"]["current_state"] == "FAILED"
    terminal = next(record for record in composition._records.values()
                    if record["key"] == composition._last_popped)
    assert terminal["item"]["delivery"]["delivery_id"] == delivery_id
    assert terminal["item"]["delivery"]["immutable_payload"] == delivery["immutable_payload"]
    assert terminal["result"]["reason"] == "CALLBACK_FAILED"
