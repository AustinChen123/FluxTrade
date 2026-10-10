"""Causal tests for run-local, per-product native exits."""

from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from typing import Any, cast

import pytest

from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from test_spider_execution_input import _input
from test_spider_historical_input import (
    _initial_account_state,
    _p2_configuration,
    _policy_cache,
    _rehashed_run,
    _run_with_configuration,
    _valid_run,
)
from test_spider_paid_endpoint_native import _require_paid_endpoint, _seeded_two_product_run


def _request_exit(composition, product_id="A-USDT-SWAP", reason="TP"):
    endpoint = _require_paid_endpoint(composition)
    return endpoint.request_product_exit(composition, product_id, reason)


def _active_two_product_request(*, signal_a=True, signal_b=True):
    base = _seeded_two_product_run()
    policy = cast(dict[str, Any], decode_canonical(_policy_cache(running=True)))
    if signal_a:
        policy["rows"][0]["hold上限"] = "0"
    if signal_b:
        policy["rows"][1]["hold下限"] = "0"
    run = replace(
        base.historical_input,
        initial_policy_cache=canonical_bytes(policy),
    )
    return replace(
        _input(run),
        policy_end_ms=run.range_end_ms - 1,
    )


def test_product_exit_closes_only_selected_seeded_position_through_native_exit():
    base = _seeded_two_product_run()
    request = replace(
        base,
        policy_end_ms=base.historical_input.range_end_ms - 1,
    )
    composition = _ReplayComposition._from_execution_run(request)
    start_visible = request.historical_input.range_start_ms * 16 + 6
    assert composition._dispatch_due(start_visible)["classification"] == "SUCCESS"

    initial = _request_exit(composition)
    assert initial["status"] == "EXITING"
    assert initial["reason"] == "TP"
    assert initial["requested_at"] == start_visible
    assert initial["positions"][0]["position_contracts"] == D("1")

    assert composition._dispatch_due(request.historical_input.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    endpoint = _require_paid_endpoint(composition)
    progress = endpoint.product_exit_progress(composition, "A-USDT-SWAP")
    assert progress["status"] == "EXITED"
    assert progress["positions"] == []
    assert progress["working_orders"] == []
    assert progress["completed_at"] is not None
    exit_actions = [
        action for action in endpoint.actions
        if action["product_id"] == "A-USDT-SWAP" and action["role"] == "EXIT"
    ]
    assert len(exit_actions) == 1
    assert exit_actions[0]["classification"] == "COMMITTED"
    exit_payload = next(
        record["item"]["group"]["members"][0]["payload"]
        for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
        and record["item"]["group"].get("group_id") == exit_actions[0]["group_id"]
    )
    assert exit_payload["reduce_only"] is True
    assert exit_payload["quantity_contracts"] == D("1")
    fills = [
        fill
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        and "historical_result" in record.get("result", {})
        for product in record["result"]["historical_result"]["products"]
        for fill in product["fills"]
        if fill["order_id"] == exit_actions[0]["order_id"]
    ]
    assert len(fills) == 1
    assert fills[0]["order_id"] == exit_actions[0]["order_id"]
    assert (fills[0]["quantity_contracts"], fills[0]["price"]) == (D("1"), D("99.9"))
    assert len(fills[0]["execution_id"]) == 64
    result = endpoint.result(composition)
    inspection = result["owner_evidence"]["inspection"]
    trading = result["owner_evidence"]["trading_fact"]["immutable_payload"]
    expected_fee = fills[0]["quantity_contracts"] * fills[0]["price"] * D("0.001")
    expected_realized = fills[0]["quantity_contracts"] * (fills[0]["price"] - D("100"))
    assert inspection["total_fees"] == expected_fee == D("0.0999")
    assert inspection["gross_realized"] == expected_realized == D("-0.1")
    assert trading["equity"] == D("9999.8001")
    frozen_actions = deepcopy(endpoint.actions)
    repeated = _request_exit(composition, reason="RISK")
    assert repeated["status"] == "EXITED"
    assert endpoint.actions == frozen_actions
    assert endpoint.result(composition)["owner_evidence"]["inspection"] == inspection


@pytest.mark.parametrize(
    ("product_id", "reason", "message"),
    [
        ("UNKNOWN-USDT-SWAP", "TP", "UNKNOWN_PRODUCT"),
        ("A-USDT-SWAP", "STOP", "INVALID_EXIT_REASON"),
    ],
)
def test_product_exit_rejects_invalid_request_without_mutation(product_id, reason, message):
    request = _seeded_two_product_run()
    composition = _ReplayComposition._from_execution_run(request)
    before = (
        deepcopy(composition._policy.rows),
        deepcopy(composition._policy.events),
        deepcopy(_require_paid_endpoint(composition).actions),
        tuple(composition._queue),
        composition._codec.inspect_state(),
    )
    with pytest.raises(ValueError, match=message):
        _request_exit(composition, product_id, reason)
    after = (
        deepcopy(composition._policy.rows),
        deepcopy(composition._policy.events),
        deepcopy(_require_paid_endpoint(composition).actions),
        tuple(composition._queue),
        composition._codec.inspect_state(),
    )
    assert after == before


def test_product_exit_rejects_cross_composition_endpoint_without_mutation():
    request_a = _active_two_product_request()
    request_b = _active_two_product_request()
    first = _ReplayComposition._from_execution_run(request_a)
    second = _ReplayComposition._from_execution_run(request_b)
    endpoint = _require_paid_endpoint(first)
    before = (
        deepcopy(first._policy.rows),
        deepcopy(first._policy.events),
        deepcopy(second._policy.rows),
        deepcopy(second._policy.events),
        deepcopy(endpoint.actions),
        tuple(first._queue),
        tuple(second._queue),
        first._codec.inspect_state(),
        second._codec.inspect_state(),
    )
    with pytest.raises(ValueError, match="INVALID_EXECUTION_SCOPE"):
        endpoint.request_product_exit(second, "A-USDT-SWAP", "TP")
    after = (
        deepcopy(first._policy.rows),
        deepcopy(first._policy.events),
        deepcopy(second._policy.rows),
        deepcopy(second._policy.events),
        deepcopy(endpoint.actions),
        tuple(first._queue),
        tuple(second._queue),
        first._codec.inspect_state(),
        second._codec.inspect_state(),
    )
    assert after == before


@pytest.mark.parametrize("boundary", ["seed", "policy_end", "whole_run_drain", "settlement"])
def test_first_product_exit_rejects_outside_visible_policy_window_without_mutation(boundary):
    request = _active_two_product_request()
    run = request.historical_input
    composition = _ReplayComposition._from_execution_run(request)
    if boundary == "policy_end":
        until = request.policy_end_ms * 16 + 6
    elif boundary == "whole_run_drain":
        until = request.policy_end_ms * 16 + 7
    elif boundary == "settlement":
        until = run.range_end_ms * 16 + 6
    else:
        until = None
    if until is not None:
        assert composition._dispatch_due(until)["classification"] == "SUCCESS"

    before = (
        deepcopy(composition._policy.rows),
        deepcopy(composition._policy.events),
        deepcopy(_require_paid_endpoint(composition).actions),
        tuple(composition._queue),
        composition._codec.inspect_state(),
    )
    with pytest.raises(ValueError, match="PRODUCT_EXIT_NOT_AVAILABLE"):
        _request_exit(composition)
    after = (
        deepcopy(composition._policy.rows),
        deepcopy(composition._policy.events),
        deepcopy(_require_paid_endpoint(composition).actions),
        tuple(composition._queue),
        composition._codec.inspect_state(),
    )
    assert after == before


def test_product_exit_accepts_last_visible_phase_before_cutoff():
    request = _active_two_product_request()
    composition = _ReplayComposition._from_execution_run(request)
    visible = request.policy_end_ms * 16 + 5
    assert composition._dispatch_due(visible)["classification"] == "SUCCESS"
    progress = _request_exit(composition)
    assert progress["requested_at"] == visible
    assert progress["reason"] == "TP"


@pytest.mark.parametrize(
    ("cash", "terminal"),
    [("50", "LIQUIDATED_INSOLVENT"), ("99.003", "LIQUIDATED_FLAT")],
)
def test_existing_product_operation_observes_both_native_terminal_outcomes(cash, terminal):
    run = _valid_run()
    configuration = _p2_configuration()
    configuration["cash"] = cash
    configuration["positions"] = [dict(
        product_id="A-USDT-SWAP", side="LONG", quantity_contracts="1",
        lots=[dict(seed_execution_id="PRODUCT-TERMINAL-SEED", seed_sequence=0,
                   strategy_id=run.strategy_identity, quantity_contracts="1", entry_price="100")],
    )]
    run = _run_with_configuration(run, configuration)
    run = replace(
        run,
        initial_account_state=_initial_account_state(
            cash=cash, positions=configuration["positions"],
        ),
        initial_policy_cache=_policy_cache(running=False),
    )
    run = _rehashed_run(
        run,
        trade_rows=tuple(
            replace(row, open=D("1"), high=D("1"), low=D("1"), close=D("1"))
            if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms
            else row
            for row in run.trade_bars
        ),
        mark_rows=tuple(
            replace(row, open=D("1"), high=D("1"), low=D("1"), close=D("1"))
            if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms
            else row
            for row in run.mark_bars
        ),
    )
    request = replace(_input(run), policy_end_ms=run.range_start_ms + 5_000)
    composition = _ReplayComposition._from_execution_run(request)
    assert composition._dispatch_due(run.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    before = _request_exit(composition, "A-USDT-SWAP")
    assert before["status"] == "EXITING"
    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    endpoint = _require_paid_endpoint(composition)
    assert composition._paid_native_terminal == terminal
    actions = deepcopy(endpoint.actions)
    queue = tuple(composition._queue)
    native = composition._codec.inspect_state()
    progress = _request_exit(composition, "A-USDT-SWAP", "RISK")
    assert progress["status"] == terminal
    assert progress["reason"] == "TP"
    assert progress["requested_at"] == before["requested_at"]
    assert progress["positions"] == []
    assert progress["working_orders"] == []
    assert endpoint.actions == actions
    assert tuple(composition._queue) == queue
    assert composition._codec.inspect_state() == native
    with pytest.raises(ValueError, match="PRODUCT_EXIT_NOT_AVAILABLE"):
        _request_exit(composition, "B-USDT-SWAP")
    assert endpoint.actions == actions
    assert tuple(composition._queue) == queue
    assert composition._codec.inspect_state() == native


def test_existing_product_operation_observes_scheduler_failure_without_retry(monkeypatch):
    request = _active_two_product_request()
    run = request.historical_input
    composition = _ReplayComposition._from_execution_run(request)
    assert composition._dispatch_due(run.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    _request_exit(composition)
    endpoint = _require_paid_endpoint(composition)

    def fail_risk_check():
        raise RuntimeError("PRODUCT_EXIT_CALLBACK_FAILURE")

    monkeypatch.setattr(composition._policy, "check_risk", fail_risk_check)
    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "TERMINAL"
    assert composition._terminal is not None
    actions = deepcopy(endpoint.actions)
    queue = tuple(composition._queue)
    progress = _request_exit(composition, reason="RISK")
    assert progress["status"] == "FAILED"
    assert endpoint.actions == actions
    assert tuple(composition._queue) == queue


def test_product_exit_suppresses_only_target_source_orders_and_keeps_first_reason():
    request = _active_two_product_request()
    run = request.historical_input
    composition = _ReplayComposition._from_execution_run(request)
    start_visible = run.range_start_ms * 16 + 6
    assert composition._dispatch_due(start_visible)["classification"] == "SUCCESS"

    first = _request_exit(composition)
    action_count = len(_require_paid_endpoint(composition).actions)
    repeated = _request_exit(composition, reason="RISK")
    assert repeated["reason"] == first["reason"] == "TP"
    assert repeated["requested_at"] == first["requested_at"] == start_visible
    assert len(_require_paid_endpoint(composition).actions) == action_count

    midpoint = (run.range_start_ms + 15_000) * 16 + 6
    assert composition._dispatch_due(midpoint)["classification"] == "SUCCESS"
    b_grid_orders = [
        order for order in composition._codec.historical_working_orders()
        if order["product_id"] == "B-USDT-SWAP"
        and _require_paid_endpoint(composition).roles_by_order.get(order["order_id"]) == "GRID"
    ]
    assert b_grid_orders
    assert _require_paid_endpoint(composition).product_exit_progress(
        composition, "A-USDT-SWAP"
    )["status"] in {"EXITING", "EXITED"}

    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    audit_actions = [
        action
        for events in composition._audit.values()
        for event in events
        for action in event.get("actions", [])
    ]
    assert any(action["status"] == "SUPPRESSED_PRODUCT_EXIT" for action in audit_actions)
    source_groups = [
        record["item"]["group"]
        for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
    ]
    source_products = {
        group["members"][0]["payload"].get("product_id")
        for group in source_groups
        if group["members"][0].get("kind") == "INTENT"
        and composition._paid_role_by_client.get(
            group["members"][0]["payload"].get("client_order_id")
        ) == "GRID"
    }
    assert "A-USDT-SWAP" not in source_products
    assert "B-USDT-SWAP" in source_products
    row_a = composition._policy.row("A-USDT-SWAP")
    row_b = composition._policy.row("B-USDT-SWAP")
    assert row_a is not None and row_a["active"] == "false"
    assert row_b is not None and row_b["active"] == "true"


def test_product_exit_does_not_add_an_active_row_gate_to_original_risk_events():
    request = _active_two_product_request(signal_a=True, signal_b=True)
    policy = cast(
        dict[str, Any], decode_canonical(request.historical_input.initial_policy_cache),
    )
    policy["rows"][1]["active"] = "false"
    run = replace(
        request.historical_input,
        initial_policy_cache=canonical_bytes(policy),
    )
    request = replace(_input(run), policy_end_ms=run.range_end_ms - 1)
    composition = _ReplayComposition._from_execution_run(request)
    assert composition._dispatch_due(run.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    _request_exit(composition, "A-USDT-SWAP")
    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"

    b_source_groups = [
        record for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
        and record["item"]["group"].get("members")
        and record["item"]["group"]["members"][0].get("kind") == "INTENT"
        and record["item"]["group"]["members"][0]["payload"].get("product_id") == "B-USDT-SWAP"
        and record["item"]["group"]["members"][0]["payload"].get("reduce_only") is not True
    ]
    assert b_source_groups
    assert all(record.get("result", {}).get("classification") == "SUCCESS"
               for record in b_source_groups)


@pytest.mark.parametrize("late_fill", [False, True])
def test_product_exit_reuses_native_cancel_barrier_and_preserves_late_fill(late_fill):
    base = _seeded_two_product_run(with_working_order=True, late_fill_order=late_fill)
    run = base.historical_input
    cutoff = run.range_start_ms + 39_001 if late_fill else run.range_end_ms - 1
    request = replace(base, policy_end_ms=cutoff)
    composition = _ReplayComposition._from_execution_run(request)
    request_visible = (cutoff - 1 if late_fill else run.range_start_ms) * 16 + 6
    assert composition._dispatch_due(request_visible)["classification"] == "SUCCESS"

    first = _request_exit(composition)
    assert first["status"] == "EXITING"
    endpoint = _require_paid_endpoint(composition)
    cancels = [
        action for action in endpoint.actions
        if action["product_id"] == "A-USDT-SWAP" and action["role"] == "DRAIN_CANCEL"
    ]
    assert cancels
    assert all(action["order_id"] == "PAID-SEED-ORDER-A" for action in cancels)
    assert composition._paid_cancel_pending_orders == {"PAID-SEED-ORDER-A"}

    if late_fill:
        pre_effect_at = (cutoff + 4_999) * 16 + 15
        assert composition._dispatch_due(pre_effect_at)["classification"] == "SUCCESS"
        cancel_actions = [
            action for action in endpoint.actions
            if action["product_id"] == "A-USDT-SWAP" and action["role"] == "DRAIN_CANCEL"
        ]
        assert [action["classification"] for action in cancel_actions] == ["COMMITTED", "PENDING"]
        assert cancel_actions[1]["parent_action_id"] == cancel_actions[0]["action_id"]
        assert "PAID-SEED-ORDER-A" in composition._paid_cancel_pending_orders
        effect_record = next(
            record for record in composition._records.values()
            if record["item"].get("kind") == "SOURCE_GROUP"
            and record["item"]["group"].get("group_id") == cancel_actions[1]["group_id"]
        )
        assert composition._dispatch_due(effect_record["key"][0])["classification"] == "SUCCESS"
        ack_ids = [
            delivery_id for delivery_id, action in endpoint.cancel_ack_action_by_delivery.items()
            if action["product_id"] == "A-USDT-SWAP"
        ]
        assert len(ack_ids) == 1
        ack_record = next(
            record for record in composition._records.values()
            if record["item"].get("kind") == "DELIVERY"
            and record["item"]["delivery"]["delivery_id"] == ack_ids[0]
        )
        assert endpoint.product_exit_progress(composition, "A-USDT-SWAP")["pending_deliveries"]
        assert composition._dispatch_due(ack_record["key"][0] - 1)["classification"] == "SUCCESS"
        assert endpoint.product_exit_progress(composition, "A-USDT-SWAP")["pending_deliveries"]
        assert composition._dispatch_due(ack_record["key"][0])["classification"] == "SUCCESS"
        assert endpoint.product_exit_progress(composition, "A-USDT-SWAP")["pending_deliveries"] == []

    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    fills = [
        fill
        for record in composition._records.values()
        if isinstance(record.get("result"), dict)
        and isinstance(record["result"].get("historical_result"), dict)
        for product in record["result"]["historical_result"].get("products", [])
        for fill in product.get("fills", [])
        if fill["order_id"] == "PAID-SEED-ORDER-A"
    ]
    assert bool(fills) is late_fill
    endpoint = _require_paid_endpoint(composition)
    exit_actions = [
        action for action in endpoint.actions
        if action["product_id"] == "A-USDT-SWAP" and action["role"] == "EXIT"
    ]
    assert len(exit_actions) == 1
    assert exit_actions[0]["classification"] == "COMMITTED"
    progress = endpoint.product_exit_progress(composition, "A-USDT-SWAP")
    assert progress["status"] == ("INCOMPLETE" if late_fill else "EXITED")
    if late_fill:
        assert fills[0]["quantity_contracts"] == D("0.2")
        assert progress["positions"][0]["position_contracts"] == D("1.2")
        assert len([
            order for order in progress["working_orders"]
            if endpoint.roles_by_order.get(order["order_id"]) == "EXIT"
        ]) == 1


def test_native_exit_rejection_keeps_current_dust_and_does_not_retry():
    run = _seeded_two_product_run(with_working_order=True).historical_input
    configuration = cast(dict[str, Any], decode_canonical(run.configuration_bytes))
    position = cast(list[dict[str, Any]], configuration["positions"])[0]
    position["quantity_contracts"] = "0.1"
    cast(list[dict[str, Any]], position["lots"])[0]["quantity_contracts"] = "0.1"
    order = cast(list[dict[str, Any]], configuration["orders"])[0]
    order.update(
        side="SHORT", limit_price="99", original_quantity_contracts="0.1",
        remaining_quantity_contracts="0.1",
    )
    for product in cast(list[dict[str, Any]], configuration["products"]):
        if product["product_id"] == "A-USDT-SWAP":
            product["specs"][0]["minimum_quantity"] = "0.1"
    before = tuple(
        replace(spec, minimum_quantity=D("0.1"))
        if spec.product_id == "A-USDT-SWAP" else spec
        for spec in run.spec_before
    )
    after = tuple(
        replace(spec, minimum_quantity=D("0.1"))
        if spec.product_id == "A-USDT-SWAP" else spec
        for spec in run.spec_after
    )
    run = _run_with_configuration(run, configuration)
    run = replace(
        run,
        spec_before=before,
        spec_after=after,
        initial_account_state=_initial_account_state(
            cash="10000", orders=cast(list[dict[str, Any]], configuration["orders"]),
            positions=cast(list[dict[str, Any]], configuration["positions"]),
        ),
    )
    run = _rehashed_run(
        run,
        trade_rows=tuple(
            replace(row, high=D("100"), low=D("98"), volume=D("0.2"))
            if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms
            else row
            for row in run.trade_bars
        ),
    )
    request = replace(_input(run), policy_end_ms=run.range_start_ms + 5_000)
    composition = _ReplayComposition._from_execution_run(request)
    assert composition._dispatch_due(run.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    initial = _request_exit(composition)
    assert initial["status"] == "EXITING"
    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"

    endpoint = _require_paid_endpoint(composition)
    progress = endpoint.product_exit_progress(composition, "A-USDT-SWAP")
    exits = [
        action for action in endpoint.actions
        if action["role"] == "EXIT" and action["product_id"] == "A-USDT-SWAP"
    ]
    assert progress["status"] == "INCOMPLETE"
    assert progress["positions"][0]["position_contracts"] == D("0.05")
    assert len(exits) == 1 and exits[0]["classification"] == "REJECTED"
    actions = deepcopy(endpoint.actions)
    repeated = _request_exit(composition, reason="RISK")
    assert repeated["status"] == "INCOMPLETE"
    assert repeated["positions"][0]["position_contracts"] == D("0.05")
    assert endpoint.actions == actions


def test_native_partial_product_exit_keeps_one_order_and_exact_remainder():
    base = _seeded_two_product_run()
    run = _rehashed_run(
        base.historical_input,
        trade_rows=tuple(replace(row, volume=D("0.4")) for row in base.historical_input.trade_bars),
    )
    request = replace(_input(run), policy_end_ms=run.range_end_ms - 1)
    composition = _ReplayComposition._from_execution_run(request)
    assert composition._dispatch_due(run.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    _request_exit(composition)
    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"

    endpoint = _require_paid_endpoint(composition)
    exits = [
        action for action in endpoint.actions
        if action["role"] == "EXIT" and action["product_id"] == "A-USDT-SWAP"
    ]
    fills = [
        fill
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        and "historical_result" in record.get("result", {})
        for product in record["result"]["historical_result"]["products"]
        for fill in product["fills"]
        if exits and fill["order_id"] == exits[0]["order_id"]
    ]
    progress = endpoint.product_exit_progress(composition, "A-USDT-SWAP")
    assert len(exits) == 1 and exits[0]["classification"] == "COMMITTED"
    assert len(fills) == 2
    assert [fill["quantity_contracts"] for fill in fills] == [D("0.1"), D("0.1")]
    assert progress["status"] == "INCOMPLETE"
    assert progress["positions"][0]["position_contracts"] == D("0.8")
    assert len([order for order in progress["working_orders"]
                if endpoint.roles_by_order.get(order["order_id"]) == "EXIT"]) == 1
    actions = deepcopy(endpoint.actions)
    repeated = _request_exit(composition, reason="RISK")
    assert repeated["positions"][0]["position_contracts"] == D("0.8")
    assert endpoint.actions == actions


def test_pending_source_send_keeps_original_acceptance_then_enters_product_cancel_barrier():
    request = _active_two_product_request(signal_a=True, signal_b=False)
    run = request.historical_input
    composition = _ReplayComposition._from_execution_run(request)
    assert composition._dispatch_due(run.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    dependency = None
    while composition._queue:
        assert composition._dispatch_due(composition._queue[0][0])["classification"] == "SUCCESS"
        dependency = next(
            (
                item for item in composition._historical_send_dependencies.values()
                if item["product_id"] == "A-USDT-SWAP" and item["state"] == "PENDING"
            ),
            None,
        )
        if dependency is not None:
            break
    assert dependency is not None
    acceptance_group = next(
        record for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
        and record["item"]["group"].get("group_id") == dependency["group_id"]
    )
    endpoint = _require_paid_endpoint(composition)
    assert acceptance_group["result"]["classification"] == "PENDING"
    progress = _request_exit(composition)
    assert progress["status"] == "EXITING"
    assert dependency["group_id"] in {
        pending["group_id"] for pending in progress["pending_sends"]
    }
    assert all(
        pending["product_id"] == "A-USDT-SWAP"
        for pending in progress["pending_sends"]
    )
    assert not [
        action for action in endpoint.actions
        if action["role"] == "DRAIN_CANCEL" and action["product_id"] == "A-USDT-SWAP"
    ]

    assert composition._dispatch_due(acceptance_group["key"][0])["classification"] == "SUCCESS"
    assert dependency["state"] == "COMMITTED"
    assert dependency["binding"] is not None
    assert endpoint.roles_by_order[dependency["binding"]["order_id"]] == "GRID"
    assert any(
        action["role"] == "DRAIN_CANCEL"
        and action["product_id"] == "A-USDT-SWAP"
        and action["order_id"] == dependency["binding"]["order_id"]
        for action in endpoint.actions
    )
    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    assert endpoint.product_exit_progress(composition, "A-USDT-SWAP")["status"] == "EXITED"


def test_flat_position_stays_exiting_until_its_native_execution_notice_is_delivered():
    base = _seeded_two_product_run()
    request = replace(base, policy_end_ms=base.historical_input.range_end_ms - 1)
    run = request.historical_input
    composition = _ReplayComposition._from_execution_run(request)
    assert composition._dispatch_due(run.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    _request_exit(composition)
    endpoint = _require_paid_endpoint(composition)

    selected_delivery: tuple[str, dict[str, Any]] | None = None
    while composition._queue:
        next_at = composition._queue[0][0]
        assert composition._dispatch_due(next_at)["classification"] == "SUCCESS"
        filled_exit = [
            (delivery_id, record)
            for delivery_id, role in endpoint.delivery_roles.items()
            if role == "EXIT"
            for record in composition._records.values()
            if record["item"].get("kind") == "DELIVERY"
            and record["item"]["delivery"]["delivery_id"] == delivery_id
            and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
        ]
        if filled_exit and endpoint.product_operations["A-USDT-SWAP"]["positions"] == []:
            selected = filled_exit[-1]
            if selected[0] in endpoint.pending_delivery_ids:
                selected_delivery = selected
                break
    assert selected_delivery is not None
    delivery_id, record = selected_delivery
    progress = endpoint.product_exit_progress(composition, "A-USDT-SWAP")
    assert progress["status"] == "EXITING"
    assert progress["positions"] == []
    assert [row["delivery_id"] for row in progress["pending_deliveries"]] == [delivery_id]
    assert composition._audit.get(delivery_id) is None

    delivery_at = record["key"][0]
    assert composition._dispatch_due(delivery_at - 1)["classification"] == "SUCCESS"
    assert endpoint.product_exit_progress(composition, "A-USDT-SWAP")["status"] == "EXITING"
    assert composition._dispatch_due(delivery_at)["classification"] == "SUCCESS"
    assert composition._audit[delivery_id] == []
    assert endpoint.product_exit_progress(composition, "A-USDT-SWAP")["status"] == "EXITED"


def test_one_incomplete_product_does_not_block_the_other_product_exit():
    base = _seeded_two_product_run()
    run = base.historical_input
    trade_rows = tuple(
        replace(row, volume=D("0.01"))
        if row.product_id == "B-USDT-SWAP" and row.bar_open_ms == run.range_start_ms
        else row
        for row in run.trade_bars
    )
    run = _rehashed_run(run, trade_rows=trade_rows)
    request = replace(_input(run), policy_end_ms=run.range_end_ms - 1)
    composition = _ReplayComposition._from_execution_run(request)
    assert composition._dispatch_due(run.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    _request_exit(composition, "A-USDT-SWAP", "TP")
    _request_exit(composition, "B-USDT-SWAP", "RISK")
    endpoint = _require_paid_endpoint(composition)

    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    a_progress = endpoint.product_exit_progress(composition, "A-USDT-SWAP")
    b_progress = endpoint.product_exit_progress(composition, "B-USDT-SWAP")
    assert a_progress["status"] == "EXITED"
    assert a_progress["positions"] == []
    assert b_progress["status"] == "INCOMPLETE"
    assert b_progress["positions"]
    assert len([
        action for action in endpoint.actions
        if action["product_id"] == "B-USDT-SWAP" and action["role"] == "EXIT"
    ]) == 1
    assert any(
        order["product_id"] == "B-USDT-SWAP"
        and endpoint.roles_by_order.get(order["order_id"]) == "EXIT"
        for order in b_progress["working_orders"]
    )
