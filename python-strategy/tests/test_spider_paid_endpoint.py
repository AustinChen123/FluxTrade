"""Cutoff, poll, and evidence-outlet tests for the native paid endpoint."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from typing import Any, cast

import pytest

from src.core.backtest.synthetic_scenario_replay import _ReplayComposition, _ScheduleError
from src.core.backtest.spider_paid_endpoint import PaidEndpoint, _paid_flat
from src.core.backtest.spider_run_artifacts import decode_canonical
from test_spider_paid_endpoint_native import _seeded_two_product_run
from test_spider_execution_input import _input
from test_spider_historical_input import (
    _initial_account_state,
    _p2_configuration,
    _policy_cache,
    _rehashed_run,
    _run_with_configuration,
    _valid_run,
)


def _endpoint(composition: _ReplayComposition) -> PaidEndpoint:
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    return endpoint


def test_started_poll_completes_after_cutoff_without_source_control():
    run = _valid_run()
    policy_end = run.range_start_ms + 5_010
    request = replace(_input(run), policy_end_ms=policy_end)
    composition = _ReplayComposition._from_execution_run(request)
    endpoint = _endpoint(composition)

    assert endpoint.source_enabled(policy_end * 16 + 5)
    assert not endpoint.source_enabled(policy_end * 16 + 6)
    cutoff_visible = policy_end * 16 + 7
    assert composition._dispatch_due(cutoff_visible)["classification"] == "SUCCESS"
    assert endpoint.drain_started
    assert any(record.observation.status == "IN_PROGRESS" for record in composition._polls.values())
    policy_events = deepcopy(composition._policy.events)
    policy_time = composition._policy.now_ms

    dispatch = composition._dispatch_due(run.range_end_ms * 16 + 6)
    assert dispatch["classification"] == "SUCCESS", (dispatch, composition._terminal)
    assert all(record.observation.status == "COMPLETED" for record in composition._polls.values())
    assert composition._policy.events == policy_events
    assert composition._policy.now_ms == policy_time

    post_cut_deliveries = [
        record["item"]["delivery"]
        for record in composition._records.values()
        if record["item"].get("kind") == "DELIVERY"
        and record["item"]["delivery"]["visible_at"] >= policy_end * 16 + 6
    ]
    assert post_cut_deliveries
    assert all(composition._audit.get(delivery["delivery_id"]) == []
               for delivery in post_cut_deliveries)


@pytest.mark.parametrize("inspection_fails", [False, True])
def test_evidence_failure_keeps_failed_outlet_null_and_preserves_inspection(monkeypatch, inspection_fails):
    request = _input()
    composition = _ReplayComposition._from_execution_run(request)

    def fail_capture(*_args):
        raise RuntimeError("final evidence unavailable")

    monkeypatch.setattr(composition, "capture_owner_evidence", fail_capture)
    if inspection_fails:
        def fail_inspection():
            raise RuntimeError("read-only inspection unavailable")
        monkeypatch.setattr(composition, "_paid_inspection", fail_inspection)
    result = _endpoint(composition).result(composition)

    assert result["status"] == "FAILED"
    assert result["failure"] == {
        "primary": {"kind": "OWNER_EVIDENCE", "reason": "EVIDENCE_FAILED", "native_failure": None},
        "evidence_error": {
            "kind": "RuntimeError", "reason": "final evidence unavailable", "native_failure": None,
        },
        "inspection": None if inspection_fails else composition._paid_inspection(),
        "inspection_error": ({"kind": "RuntimeError", "reason": "read-only inspection unavailable",
                              "native_failure": None} if inspection_fails else None),
        "observed_at": composition._current_time,
    }
    assert result["owner_evidence"] is None
    assert result["pending_obligations"]["positions"] is None
    assert result["pending_obligations"]["working_orders"] is None
    assert result["reasons"] == ["EXECUTION_FAILURE"]


def test_scheduler_failure_retains_complete_current_owner_evidence():
    request = _input()
    composition = _ReplayComposition._from_execution_run(request)
    composition._stop_scheduler(_ScheduleError("injected scheduler failure"), "POLL", "poll-id")

    result = _endpoint(composition).result(composition)

    assert result["status"] == "FAILED"
    assert result["owner_evidence"]["cutoff"] == composition._current_time
    assert result["owner_evidence"]["cutoff"] != request.historical_input.range_end_ms * 16 + 6
    assert result["pending_obligations"]["positions"] == []
    assert result["pending_obligations"]["working_orders"] == []
    assert result["failure"] == {
        "primary": {"kind": "POLL", "reason": "injected scheduler failure", "native_failure": None},
        "evidence_error": None,
        "inspection": composition._codec.inspect_state(),
        "inspection_error": None,
        "observed_at": composition._current_time,
    }


def test_initial_paused_poll_without_stages_does_not_block_paid_flat():
    run = replace(_valid_run(), initial_policy_cache=_policy_cache(running=False, paused=True))
    request = replace(_input(run), policy_end_ms=run.range_start_ms + 5_010)
    composition = _ReplayComposition._from_execution_run(request)

    dispatch = composition._dispatch_due(run.range_end_ms * 16 + 6)
    assert dispatch["classification"] == "SUCCESS", (dispatch, composition._terminal)
    assert composition._polls
    assert all(record.observation.status == "PAUSED" for record in composition._polls.values())

    result = _endpoint(composition).result(composition)

    assert result["status"] == "PAID_FLAT"
    assert result["pending_obligations"]["pending_polls"] == []
    assert "POLL_PENDING" not in result["reasons"]


def test_in_progress_poll_with_real_awaiting_stage_remains_pending():
    original = _valid_run()
    run = replace(original, range_end_ms=original.range_start_ms + 5_011)
    request = replace(_input(run), policy_end_ms=run.range_start_ms + 5_010)
    composition = _ReplayComposition._from_execution_run(request)
    dispatch = composition._dispatch_due(run.range_end_ms * 16 + 6)
    assert dispatch["classification"] == "SUCCESS", (dispatch, composition._terminal)

    result = _endpoint(composition).result(composition)

    assert result["status"] == "INCOMPLETE"
    assert result["owner_evidence"]["cutoff"] == run.range_end_ms * 16 + 6
    assert result["pending_obligations"]["pending_polls"]
    assert all(row["status"] == "IN_PROGRESS" and row["awaiting"] is not None
               for row in result["pending_obligations"]["pending_polls"])
    assert "POLL_PENDING" in result["reasons"]


def test_paid_same_callback_client_cancel_binds_only_at_authoritative_resolution():
    run = replace(_valid_run(), initial_policy_cache=_policy_cache(running=False))
    policy_end = run.range_start_ms + 1_001
    request = replace(_input(run), policy_end_ms=policy_end)
    composition = _ReplayComposition._from_execution_run(request)
    endpoint = _endpoint(composition)
    visible_at = (run.range_start_ms + 1_000) * 16 + 6
    product_id = run.ordered_products[0]
    client_id = "PAID-SAME-CALLBACK"
    policy_events = [
        {"kind": "send", "orders": [dict(
            ordType="limit", instId=product_id, clOrdId=client_id,
            side="buy", sz="0.2", px="50",
        )]},
        {"kind": "cancel", "orders": [dict(ordId=client_id, instId=product_id)]},
    ]
    audit_events = [
        {"event": deepcopy(event), "actions": [dict(group_id=None, status="UNSUBMITTED")]}
        for event in policy_events
    ]
    delivery_id = "PAID-SAME-CALLBACK-DELIVERY"
    composition._audit[delivery_id] = audit_events
    record = {"item": {"delivery": {"delivery_id": delivery_id, "visible_at": visible_at}}}
    groups = composition._historical_order_children(record, policy_events, audit_events)
    send_group, cancel_group = [item[0]["group"] for item in groups]
    action = endpoint.action_by_group[cancel_group["group_id"]]
    assert action["order_id"] is None
    assert action["effective_at"] == cancel_group["group_effective_at"]
    composition._admit_set(groups)
    endpoint.start_drain(composition)
    assert endpoint.drain_started
    assert action["order_id"] is None

    acceptance_at = send_group["group_effective_at"]
    assert composition._dispatch_due(acceptance_at)["classification"] == "SUCCESS"
    binding = composition._historical_send_by_client[client_id]["binding"]
    assert binding is not None
    assert action["order_id"] == binding["order_id"]
    assert endpoint.roles_by_order[binding["order_id"]] == "GRID"
    assert binding["product_id"] == product_id
    assert binding["strategy_id"] == request.historical_input.strategy_identity
    assert composition._paid_cancel_pending_orders == {binding["order_id"]}
    cancel_requests = [
        item["item"]["group"] for item in composition._records.values()
        if item["item"].get("kind") == "SOURCE_GROUP"
        and item["item"]["group"]["members"][0]["kind"] == "CANCEL_REQUEST"
    ]
    assert [group["group_id"] for group in cancel_requests] == [cancel_group["group_id"]]
    assert cancel_requests[0]["group_id"] == cancel_group["group_id"]
    assert endpoint.action_by_group[cancel_group["group_id"]]["classification"] == "COMMITTED"


def test_paid_source_cancel_uses_native_result_for_already_filled_seed_order():
    request = _seeded_two_product_run(with_working_order=True, late_fill_order=True)
    run = request.historical_input
    policy_end = run.range_start_ms + 59_000
    request = replace(request, policy_end_ms=policy_end)
    composition = _ReplayComposition._from_execution_run(request)
    endpoint = _endpoint(composition)
    target_id = "PAID-SEED-ORDER-A"
    visible_at = (run.range_start_ms + 50_000) * 16 + 6
    policy_events = [{"kind": "cancel", "orders": [dict(
        ordId=target_id, instId="A-USDT-SWAP", instIdCode=1,
    )]}]
    audit_events = [{"event": deepcopy(policy_events[0]),
                     "actions": [dict(group_id=None, status="UNSUBMITTED")]}]
    delivery_id = "PAID-TERMINAL-CANCEL-DELIVERY"
    composition._audit[delivery_id] = audit_events
    record = {"item": {"delivery": {"delivery_id": delivery_id, "visible_at": visible_at}}}
    (cancel_item, _) = composition._historical_order_children(record, policy_events, audit_events)[0]
    group = cancel_item["group"]
    action = endpoint.action_by_group[group["group_id"]]
    assert action["order_id"] is None
    composition._admit_set([(cancel_item, None)])

    acceptance_at = group["group_effective_at"]
    assert composition._dispatch_due(acceptance_at - 1)["classification"] == "SUCCESS"
    assert target_id not in {row["order_id"] for row in composition._codec.historical_working_orders()}
    assert composition._dispatch_due(acceptance_at)["classification"] == "SUCCESS"

    result = action["result"]
    assert result is not None
    assert action["classification"] in ("COMMITTED", "REJECTED")
    assert composition._terminal is None
    assert action["order_id"] == target_id
    assert target_id not in {row["order_id"] for row in composition._codec.historical_working_orders()}


def test_evidence_failure_retains_last_verified_cutoff_but_not_stale_rows(monkeypatch):
    composition = _ReplayComposition._from_execution_run(_input())
    endpoint = _endpoint(composition)
    old_cutoff = composition._current_time
    last = composition.capture_owner_evidence(
        old_cutoff,
        composition._paid_snapshot_request("TRADING", old_cutoff, "PAID_LAST_TRADING"),
        composition._paid_snapshot_request("POSITIONS", old_cutoff, "PAID_LAST_POSITIONS"),
        composition._paid_snapshot_request("OPEN_ORDERS", old_cutoff, "PAID_LAST_ORDERS"),
    )["owner_evidence"]
    endpoint.last_owner_evidence = last
    endpoint.last_cutoff = old_cutoff

    def fail_capture(*_args):
        raise RuntimeError("later evidence unavailable")

    monkeypatch.setattr(composition, "capture_owner_evidence", fail_capture)
    result = endpoint.result(composition)

    assert result["status"] == "FAILED"
    assert result["owner_evidence"] == last
    assert result["owner_evidence"]["cutoff"] == old_cutoff
    assert result["pending_obligations"]["positions"] is None
    assert result["pending_obligations"]["working_orders"] is None
    assert result["failure"]["evidence_error"]["reason"] == "later evidence unavailable"


def test_pre_cut_generated_send_keeps_original_native_acceptance_after_cutoff():
    run = replace(_valid_run(), initial_policy_cache=_policy_cache(running=False))
    policy_end = run.range_start_ms + 1_000
    request = replace(_input(run), policy_end_ms=policy_end)
    composition = _ReplayComposition._from_execution_run(request)
    visible_at = (policy_end - 1) * 16 + 6
    assert composition._dispatch_due(visible_at)["classification"] == "SUCCESS"

    event = {"kind": "send", "orders": [dict(
        ordType="limit", instId="A-USDT-SWAP", clOrdId="PRE-CUT-GRID",
        side="buy", sz="0.2", px="50",
    )]}
    audit_events = [{"actions": [{"group_id": None, "status": "UNSUBMITTED"}]}]
    record = {"item": {"delivery": {"delivery_id": "PRE-CUT-DELIVERY", "visible_at": visible_at}}}
    pending = composition._historical_order_children(record, [event], audit_events)
    assert len(pending) == 1
    group = pending[0][0]["group"]
    acceptance_at = group["group_effective_at"]
    composition._admit_set(pending)

    dependency = composition._historical_send_dependencies[group["group_id"]]
    assert dependency["state"] == "PENDING"
    endpoint = _endpoint(composition)
    assert endpoint.actions == []

    assert acceptance_at == policy_end * 16 + 3
    assert composition._dispatch_due(acceptance_at - 1)["classification"] == "SUCCESS"
    assert dependency["state"] == "PENDING"
    assert composition._dispatch_due(policy_end * 16 + 7)["classification"] == "SUCCESS"
    assert dependency["state"] == "COMMITTED"
    assert dependency["binding"]["product_id"] == "A-USDT-SWAP"
    assert endpoint.roles_by_order[dependency["binding"]["order_id"]] == "GRID"
    assert any(action["role"] == "DRAIN_CANCEL" for action in endpoint.actions)

    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    assert endpoint.result(composition)["status"] == "PAID_FLAT"


def test_native_liquidation_remains_terminal_without_paid_exit_resurrection():
    run = _valid_run()
    configuration = _p2_configuration()
    configuration["cash"] = "50"
    configuration["positions"] = [dict(
        product_id="A-USDT-SWAP", side="LONG", quantity_contracts="1",
        lots=[dict(seed_execution_id="LIQUIDATION-SEED", seed_sequence=0,
                   strategy_id=run.strategy_identity, quantity_contracts="1", entry_price="100")],
    )]
    run = _run_with_configuration(run, configuration)
    run = replace(run, initial_account_state=_initial_account_state(
        cash="50", positions=configuration["positions"],
    ), initial_policy_cache=_policy_cache(running=False))
    trade = tuple(replace(row, open=D("1"), high=D("1"), low=D("1"), close=D("1"))
                  if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms else row
                  for row in run.trade_bars)
    marks = tuple(replace(row, open=D("1"), high=D("1"), low=D("1"), close=D("1"))
                  if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms else row
                  for row in run.mark_bars)
    run = _rehashed_run(run, trade_rows=trade, mark_rows=marks)
    request = replace(_input(run), policy_end_ms=run.range_start_ms + 5_000)
    composition = _ReplayComposition._from_execution_run(request)

    dispatch = composition._dispatch_due(run.range_end_ms * 16 + 6)
    assert dispatch["classification"] == "SUCCESS", (dispatch, composition._terminal)
    result = _endpoint(composition).result(composition)

    assert result["status"] == "LIQUIDATED_INSOLVENT"
    assert result["reasons"] == ["NATIVE_TERMINAL"]
    assert result["owner_evidence"]["inspection"]["lifecycle"] == "LIQUIDATED_INSOLVENT"
    assert composition._paid_native_terminal == "LIQUIDATED_INSOLVENT"
    assert result["actions"] == []
    assert not any(
        record["item"].get("kind") == "SOURCE_GROUP"
        and record["item"]["group"]["members"][0]["kind"] == "INTENT"
        for record in composition._records.values()
    )


def test_fully_filled_grid_order_before_cancel_effect_does_not_fault_paid_drain():
    request = _seeded_two_product_run(with_working_order=True, late_fill_order=True)
    request = replace(request, policy_end_ms=request.policy_end_ms + 39_000)
    composition = _ReplayComposition._from_execution_run(request)

    assert composition._dispatch_due(request.historical_input.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    endpoint = _endpoint(composition)
    grid_fills = [
        fill
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        for product in record.get("result", {}).get("historical_result", {}).get("products", [])
        for fill in product["fills"]
        if fill["order_id"] == "PAID-SEED-ORDER-A"
    ]
    cancel_effects = [
        record["result"]["group_result"]
        for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
        and record["item"]["group"]["members"][0]["kind"] == "CANCEL_EFFECT"
    ]
    result = endpoint.result(composition)

    assert len(grid_fills) == 1
    assert grid_fills[0]["quantity_contracts"] == D("0.2")
    assert len(cancel_effects) == 1
    assert cancel_effects[0]["classification"] == "COMMITTED"
    assert cancel_effects[0]["rejections"] == []
    assert result["status"] == "INCOMPLETE"
    assert result["failure"] is None
    assert not any(order["order_id"] == "PAID-SEED-ORDER-A"
                   for order in result["pending_obligations"]["working_orders"])


def test_native_partial_exit_keeps_exact_below_minimum_dust_without_resend():
    request = _seeded_two_product_run()
    run = request.historical_input
    configuration = cast(dict[str, Any], decode_canonical(run.configuration_bytes))
    for product in cast(list[dict[str, Any]], configuration["products"]):
        if product["product_id"] == "A-USDT-SWAP":
            for spec in product["specs"]:
                spec["minimum_quantity"] = "0.01"
    before = tuple(replace(spec, minimum_quantity=D("0.01"))
                   if spec.product_id == "A-USDT-SWAP" else spec for spec in run.spec_before)
    after = tuple(replace(spec, minimum_quantity=D("0.01"))
                  if spec.product_id == "A-USDT-SWAP" else spec for spec in run.spec_after)
    run = _run_with_configuration(run, configuration)
    run = replace(run, spec_before=before, spec_after=after)
    run = _rehashed_run(
        run, trade_rows=tuple(replace(row, volume=D("1.999")) for row in run.trade_bars),
    )
    request = _input(run)
    composition = _ReplayComposition._from_execution_run(request)

    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    result = _endpoint(composition).result(composition)
    exits = [action for action in result["actions"]
             if action["role"] == "EXIT" and action["product_id"] == "A-USDT-SWAP"]

    assert result["status"] == "INCOMPLETE"
    assert "RESIDUAL_POSITION" in result["reasons"]
    assert [row["position_contracts"] for row in result["pending_obligations"]["positions"]] == [
        D("0.001"), D("-0.001"),
    ]
    assert len(exits) == 1
    assert exits[0]["classification"] == "COMMITTED"
    assert result["pending_obligations"]["working_orders"][0]["cumulative_filled_size_contracts"] == D("0.999")
    assert len([action for action in result["actions"]
                if action["role"] == "EXIT" and action["product_id"] == "A-USDT-SWAP"]) == 1


def test_native_rejects_paid_exit_for_legal_under_minimum_residual():
    request = _seeded_two_product_run(with_working_order=True)
    run = request.historical_input
    configuration = cast(dict[str, Any], decode_canonical(run.configuration_bytes))
    position = cast(list[dict[str, Any]], configuration["positions"])[0]
    position["quantity_contracts"] = "0.1"
    position["lots"][0]["quantity_contracts"] = "0.1"
    order = cast(list[dict[str, Any]], configuration["orders"])[0]
    order.update(side="SHORT", limit_price="99", original_quantity_contracts="0.1",
                 remaining_quantity_contracts="0.1")
    for product in cast(list[dict[str, Any]], configuration["products"]):
        if product["product_id"] == "A-USDT-SWAP":
            product["specs"][0]["minimum_quantity"] = "0.1"
    before = tuple(replace(spec, minimum_quantity=D("0.1"))
                   if spec.product_id == "A-USDT-SWAP" else spec for spec in run.spec_before)
    after = tuple(replace(spec, minimum_quantity=D("0.1"))
                  if spec.product_id == "A-USDT-SWAP" else spec for spec in run.spec_after)
    run = _run_with_configuration(run, configuration)
    run = replace(run, spec_before=before, spec_after=after,
                  initial_account_state=_initial_account_state(
                      cash="10000", orders=cast(list[dict[str, Any]], configuration["orders"]),
                      positions=cast(list[dict[str, Any]], configuration["positions"]),
                  ))
    run = _rehashed_run(run, trade_rows=tuple(
        replace(row, high=D("100"), low=D("98"), volume=D("0.2"))
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms else row
        for row in run.trade_bars
    ))
    request = replace(_input(run), policy_end_ms=run.range_start_ms + 5_000)
    composition = _ReplayComposition._from_execution_run(request)

    dispatch = composition._dispatch_due(run.range_end_ms * 16 + 6)
    assert dispatch["classification"] == "SUCCESS", (dispatch, composition._terminal)
    result = _endpoint(composition).result(composition)
    a_exits = [action for action in result["actions"]
               if action["role"] == "EXIT" and action["product_id"] == "A-USDT-SWAP"]

    assert result["status"] == "INCOMPLETE"
    assert "EXIT_REJECTED" in result["reasons"]
    assert [row["position_contracts"] for row in result["pending_obligations"]["positions"]
            if row["product_id"] == "A-USDT-SWAP"] == [D("0.05")]
    assert len(a_exits) == 1
    assert a_exits[0]["classification"] == "REJECTED"
    assert result["owner_evidence"]["inspection"]["gate"] == "RUNNING"
    assert result["owner_evidence"]["inspection"]["gate"] == "RUNNING"


def test_paid_flat_classifier_rejects_any_unresolved_action_id():
    assert not _paid_flat([], [], ["pending-action-id"], [], [],
                           pending_cancels=set(), pending_sends=[])
    assert _paid_flat([], [], [], [], [], pending_cancels=set(), pending_sends=[])
