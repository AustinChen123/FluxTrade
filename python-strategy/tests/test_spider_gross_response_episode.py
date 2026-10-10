"""Causal run-local tests for gross-response barriers and proposal evidence."""

from __future__ import annotations

from dataclasses import replace
from decimal import ROUND_CEILING, Decimal as D
from hashlib import sha256
from typing import Any, cast

import pytest

from src.core.backtest.spider_gross_response import propose_reductions
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_gross_execution_input import _over_cap_gross_input
from test_spider_historical_input import (
    _initial_account_state,
    _policy_cache,
    _rehashed_run,
    _run_with_configuration,
)


def _with_minimums(request, minimums: dict[str, str]):
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    for product in configuration["products"]:
        minimum = minimums.get(product["product_id"])
        if minimum is not None:
            product["specs"][0]["minimum_quantity"] = minimum
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        spec_before=tuple(
            replace(spec, minimum_quantity=D(minimums[spec.product_id]))
            if spec.product_id in minimums else spec
            for spec in history.spec_before
        ),
        spec_after=tuple(
            replace(spec, minimum_quantity=D(minimums[spec.product_id]))
            if spec.product_id in minimums else spec
            for spec in history.spec_after
        ),
    )
    cache = cast(dict[str, Any], decode_canonical(history.initial_policy_cache))
    for market in cache["markets"]:
        if market["product_id"] in minimums:
            market["minSz"] = minimums[market["product_id"]]
    history = replace(history, initial_policy_cache=canonical_bytes(cache))
    return replace(request, execution_input=replace(execution, historical_input=history))


def _with_later_mark(request, value: str, *, offset_ms: int = 60_000):
    execution = request.execution_input
    history = execution.historical_input
    mark_rows = tuple(
        replace(row, open=D(value), high=D(value), low=D(value), close=D(value))
        if row.bar_open_ms == history.range_start_ms + offset_ms else row
        for row in history.mark_bars
    )
    history = _rehashed_run(history, mark_rows=mark_rows)
    return replace(request, execution_input=replace(execution, historical_input=history))


def _dispatch_to_end(request):
    composition = _ReplayComposition._from_gross_response_run(request)
    result = composition._dispatch_due(request.execution_input.historical_input.range_end_ms * 16 + 6)
    assert result["classification"] == "SUCCESS", result
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    return composition, endpoint


def _with_existing_reducing_order(request, quantity: str):
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    order = configuration["orders"][0]
    order.update(
        side="SHORT",
        reduce_only=True,
        original_quantity_contracts=quantity,
        remaining_quantity_contracts=quantity,
    )
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        initial_account_state=canonical_bytes(dict(
            cash=configuration["cash"],
            positions=configuration["positions"],
            orders=configuration["orders"],
        )),
    )
    return replace(request, execution_input=replace(execution, historical_input=history))


def _with_limited_settlement_volume(request, volume: str):
    execution = request.execution_input
    history = execution.historical_input
    rows = tuple(
        replace(row, volume=D(volume))
        for row in history.trade_bars
    )
    history = _rehashed_run(history, trade_rows=rows)
    return replace(request, execution_input=replace(execution, historical_input=history))


def _with_first_market_price(request, product_id: str, value: str):
    execution = request.execution_input
    history = execution.historical_input
    target = history.range_start_ms
    trade_rows = tuple(
        replace(row, open=D(value), high=D(value), low=D(value), close=D(value))
        if row.product_id == product_id and row.bar_open_ms == target else row
        for row in history.trade_bars
    )
    mark_rows = tuple(
        replace(row, open=D(value), high=D(value), low=D(value), close=D(value))
        if row.product_id == product_id and row.bar_open_ms == target else row
        for row in history.mark_bars
    )
    history = _rehashed_run(history, trade_rows=trade_rows, mark_rows=mark_rows)
    return replace(request, execution_input=replace(execution, historical_input=history))


def _with_reversed_pool(request):
    execution = request.execution_input
    history = execution.historical_input
    ordered_products = tuple(reversed(history.ordered_products))
    ordinals = {product: index for index, product in enumerate(ordered_products)}
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["products"] = list(reversed(configuration["products"]))
    configuration["positions"] = list(reversed(configuration["positions"]))
    history = replace(history, ordered_products=ordered_products)
    history = _run_with_configuration(history, configuration)

    trade_rows = tuple(sorted(
        history.trade_bars,
        key=lambda row: (row.bar_open_ms, ordinals[row.product_id]),
    ))
    mark_rows = tuple(sorted(
        history.mark_bars,
        key=lambda row: (row.bar_open_ms, ordinals[row.product_id]),
    ))

    def manifest_for(manifest, rows):
        digest_rows = sorted(rows, key=lambda row: (ordinals[row.product_id], row.source_sequence))
        digest = sha256(b"".join(bytes.fromhex(row.source_row_hash) for row in digest_rows)).hexdigest()
        return replace(
            manifest,
            product_mapping=tuple((product, product) for product in ordered_products),
            canonical_rows_sha256=digest,
        )

    cache = cast(dict[str, Any], decode_canonical(history.initial_policy_cache))
    cache["rows"] = list(reversed(cache["rows"]))
    cache["markets"] = list(reversed(cache["markets"]))
    initial = cast(dict[str, Any], decode_canonical(history.initial_account_state))
    initial["positions"] = list(reversed(initial["positions"]))
    history = replace(
        history,
        trade_bars=trade_rows,
        mark_bars=mark_rows,
        trade_manifest=manifest_for(history.trade_manifest, trade_rows),
        mark_manifest=manifest_for(history.mark_manifest, mark_rows),
        spec_before=tuple(reversed(history.spec_before)),
        spec_after=tuple(reversed(history.spec_after)),
        initial_policy_cache=canonical_bytes(cache),
        initial_account_state=canonical_bytes(initial),
    )
    scope = replace(request.authorization_scope, ordered_products=ordered_products)
    return replace(request, authorization_scope=scope,
                   execution_input=replace(execution, historical_input=history))


def _with_running_source(request):
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["orders"][0]["client_order_id"] = "PAID-SEED-0001"
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        initial_account_state=_initial_account_state(
            cash=configuration["cash"],
            positions=configuration["positions"],
            orders=configuration["orders"],
        ),
    )
    cache = cast(dict[str, Any], decode_canonical(history.initial_policy_cache))
    cache["running"] = True
    history = replace(history, initial_policy_cache=canonical_bytes(cache))
    return replace(request, execution_input=replace(execution, historical_input=history))


def test_gross_cancel_ack_is_a_reduction_barrier_and_proposal_facts_are_captured():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
        with_working_order=True,
    )
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None

    ack_id = None
    while ack_id is None:
        assert composition._queue
        key = composition._queue[0]
        dispatch = composition._dispatch_due(key[0])
        assert dispatch["classification"] == "SUCCESS", dispatch
        ack_id = next(iter(endpoint.cancel_ack_action_by_delivery), None)

    ack_action = endpoint.cancel_ack_action_by_delivery[ack_id]
    assert ack_action["role"] == "GROSS_CANCEL"
    cancel_actions = [action for action in endpoint.actions if action["role"] == "GROSS_CANCEL"]
    assert len(cancel_actions) == 2  # one request and its one effect
    assert cancel_actions[1]["parent_action_id"] == cancel_actions[0]["action_id"]
    assert ack_action is cancel_actions[1]
    ack_record = next(
        record for record in composition._records.values()
        if record["item"].get("kind") == "DELIVERY"
        and record["item"]["delivery"]["delivery_id"] == ack_id
    )
    assert ack_id in endpoint.pending_delivery_ids
    assert ack_record["result"]["classification"] == "PENDING"
    assert not any(action["role"] == "GROSS_REDUCTION" for action in endpoint.actions)

    effect_observation = next(
        observation for observation in reversed(endpoint.gross_response.observations)
        if ack_id in observation["proposal_context"]["operations"]["pending_delivery_ids"]
    )
    captured = effect_observation["proposal_context"]
    assert all(
        captured["scheduler_key"][name] == effect_observation["scheduler_key"][name]
        for name in ("visible_at", "schedule_sequence", "stable_id")
    )
    assert captured["source_cache"]["policy_rows"]
    assert captured["source_cache"]["policy_markets"]
    assert captured["operations"]["pending_deliveries"][0]["cancel_action_id"] == ack_action["action_id"]
    progress = captured["operations"]["gross_response"]
    assert progress == {
        "episode_sequence": 1,
        "state": "RESTRICTED",
        "active": True,
        "rejected_products": [],
        "current_round_sequence": None,
        "current_round_action_ids": [],
    }
    assert any(
        row["delivery"]["delivery_id"] == ack_id
        for row in captured["operations"]["pending_deliveries"]
    )
    frozen = canonical_bytes(captured)

    dispatch = composition._dispatch_due(ack_record["key"][0])
    assert dispatch["classification"] == "SUCCESS", dispatch
    assert ack_id not in endpoint.pending_delivery_ids
    assert sum(action["role"] == "GROSS_REDUCTION" for action in endpoint.actions) == 2

    proposal = endpoint.gross_response.rounds[0]["proposal"]
    assert proposal["proposal_context"]["operations"]["pending_delivery_ids"] == []
    # A later native/account mutation cannot retroactively change this proposal cut.
    endpoint.actions[0]["classification"] = "TEST_ONLY_LATER_MUTATION"
    endpoint.gross_response.active = False
    endpoint.gross_response.rejected_products.add("A-USDT-SWAP")
    endpoint.gross_response.current_round_action_ids.append("LATER-ACTION")
    assert canonical_bytes(captured) == frozen


def test_gross_cancel_ack_callback_failure_preserves_reduction_barrier(monkeypatch):
    from src.core.backtest.synthetic_scenario_replay import _CallbackPrefix

    request = _over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
        with_working_order=True,
    )
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    original_capture = composition._capture_callback
    failed_ids: list[str] = []

    def fail_gross_cancel_ack(delivery):
        delivery_id = delivery["delivery_id"]
        cancel_action = endpoint.cancel_ack_action_by_delivery.get(delivery_id)
        if cancel_action is not None and cancel_action["role"] == "GROSS_CANCEL":
            failed_ids.append(delivery_id)
            return _CallbackPrefix((), RuntimeError("injected gross cancel ACK callback failure"))
        return original_capture(delivery)

    monkeypatch.setattr(composition, "_capture_callback", fail_gross_cancel_ack)
    while True:
        assert composition._queue
        key = composition._queue[0]
        dispatch = composition._dispatch_due(key[0])
        if dispatch["classification"] == "TERMINAL":
            break
        assert dispatch["classification"] == "SUCCESS", dispatch

    assert dispatch["reason"] == "CALLBACK_FAILED"
    assert len(failed_ids) == 1
    failed_id = failed_ids[0]
    assert failed_id in endpoint.pending_delivery_ids
    assert not any(action["role"] == "GROSS_REDUCTION" for action in endpoint.actions)
    terminal = next(
        record for record in composition._records.values()
        if record["key"] == composition._last_popped
    )
    assert terminal["item"]["kind"] == "DELIVERY"
    assert terminal["item"]["delivery"]["delivery_id"] == failed_id
    assert terminal["item"]["delivery"]["payload_kind"] == "TRANSPORT_ACK"
    assert endpoint.cancel_ack_action_by_delivery[failed_id]["role"] == "GROSS_CANCEL"


@pytest.mark.parametrize(("volume", "expected_fill", "barrier"), [(None, D("0.2"), "notice"), (D("0.4"), D("0.1"), "ack")])
def test_gross_response_preserves_seed_fill_before_cancel_barrier(volume, expected_fill, barrier):
    request = _over_cap_gross_input(
        policy_end_offset_ms=30_000,
        with_working_order=True,
        late_fill_order=True,
    )
    request = _with_first_market_price(request, "A-USDT-SWAP", "99")
    if volume is not None:
        request = _with_limited_settlement_volume(request, str(volume))
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    history = request.execution_input.historical_input
    start = history.range_start_ms
    initial_fees = composition._codec.inspect_state()["total_fees"]

    first_step = composition._dispatch_due(start * 16 + 7)
    assert first_step["classification"] == "SUCCESS", first_step
    step = next(
        row for row in composition._records.values()
        if row["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        and row["item"]["node"]["step_index"] == 0
    )
    fills = [
        fill for product in step["result"]["historical_result"]["products"]
        for fill in product["fills"] if fill["order_id"] == "PAID-SEED-ORDER-A"
    ]
    assert len(fills) == 1
    assert fills[0]["quantity_contracts"] == expected_fill
    assert fills[0]["price"] == D("99")
    after_fill = composition._codec.inspect_state()
    assert after_fill["total_fees"] - initial_fees == expected_fill * D("99") * D("0.001")
    assert not any(action["role"] == "GROSS_REDUCTION" for action in endpoint.actions)
    fill_notice = next(
        row for row in composition._records.values()
        if row["item"].get("kind") == "DELIVERY"
        and row["item"]["delivery"]["source_fact_id"] == fills[0]["source_event_id"]
    )

    if barrier == "notice":
        assert composition._dispatch_due(fill_notice["key"][0])["classification"] == "SUCCESS"
        assert any(
            action["role"] == "GROSS_CANCEL" and action["classification"] == "REJECTED"
            for action in endpoint.actions
        )
    else:
        ack_id = None
        while ack_id is None:
            assert composition._queue
            next_key = composition._queue[0]
            result = composition._dispatch_due(next_key[0])
            assert result["classification"] == "SUCCESS", result
            ack_id = next(
                (delivery_id for delivery_id, action in endpoint.cancel_ack_action_by_delivery.items()
                 if action["role"] == "GROSS_CANCEL"),
                None,
            )
        assert ack_id in endpoint.pending_delivery_ids
        assert not any(action["role"] == "GROSS_REDUCTION" for action in endpoint.actions)
        ack_record = next(
            row for row in composition._records.values()
            if row["item"].get("kind") == "DELIVERY"
            and row["item"]["delivery"]["delivery_id"] == ack_id
        )
        assert composition._dispatch_due(ack_record["key"][0])["classification"] == "SUCCESS"
        assert ack_id not in endpoint.pending_delivery_ids

    assert endpoint.gross_response.rounds
    proposal = endpoint.gross_response.rounds[0]["proposal"]
    proposal_cut = next(
        observation for observation in endpoint.gross_response.observations
        if observation["native_state"]["owner_state_digest"]
        == proposal["native_state_digest"]
    )
    assert D(proposal_cut["native_state"]["gross_admission"]["gross"]) > D("200")


def test_gross_rejection_stops_redistribution_but_later_native_recovery_is_allowed():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
    )
    request = _with_minimums(request, {"B-USDT-SWAP": "0.8"})
    request = _with_later_mark(request, "37.5")
    composition, endpoint = _dispatch_to_end(request)
    assert endpoint.gross_response is not None
    result = endpoint.result(composition)

    gross_actions = [action for action in endpoint.actions if action["role"] == "GROSS_REDUCTION"]
    # The first rejected episode stops at one proportional round. The actual
    # lower mark recovers it; a later native re-breach starts a distinct round.
    assert len(gross_actions) == 4
    rejected = [action for action in gross_actions if action["classification"] == "REJECTED"]
    committed = [action for action in gross_actions if action["classification"] == "COMMITTED"]
    assert [action["product_id"] for action in rejected] == ["B-USDT-SWAP", "B-USDT-SWAP"]
    assert [action["product_id"] for action in committed] == ["A-USDT-SWAP", "A-USDT-SWAP"]
    assert rejected[0]["result"]["rejections"]
    assert endpoint.gross_response.rejected_products == {"B-USDT-SWAP"}
    assert endpoint.gross_response.episode_sequence == 2
    assert len(endpoint.gross_response.rounds) == 2
    assert endpoint.gross_response.rounds[0]["action_ids"] == [
        action["action_id"] for action in gross_actions[:2]
    ]
    assert endpoint.gross_response.rounds[1]["action_ids"] == [
        action["action_id"] for action in gross_actions[2:]
    ]
    captured_round_progress = [
        round_row["proposal"]["proposal_context"]["operations"]["gross_response"]
        for round_row in endpoint.gross_response.rounds
    ]
    assert [row["episode_sequence"] for row in captured_round_progress] == [0, 1]
    assert captured_round_progress[0]["state"] == "IDLE"
    assert captured_round_progress[1]["state"] == "RECOVERED"
    assert captured_round_progress[0]["current_round_sequence"] is None
    assert captured_round_progress[1]["current_round_sequence"] == 1
    assert captured_round_progress[1]["current_round_action_ids"] == [
        action["action_id"] for action in gross_actions[:2]
    ]
    later_progress = next(
        row["proposal_context"]["operations"]["gross_response"]
        for row in endpoint.gross_response.observations
        if row["proposal_context"]["operations"]["gross_response"]["current_round_sequence"] == 2
    )
    assert later_progress["episode_sequence"] == 2
    assert later_progress["current_round_action_ids"] == [
        action["action_id"] for action in gross_actions[2:]
    ]
    transitions = [(row["after_state"], row["reason"]) for row in endpoint.gross_response.observations]
    reject_at = next(i for i, item in enumerate(transitions) if item == ("INCOMPLETE", "ACTION_REJECTED"))
    recovered_at = next(i for i, item in enumerate(transitions) if i > reject_at and item == ("RECOVERED", None))
    assert endpoint.gross_response.rounds[0]["observed_at"] < endpoint.gross_response.observations[reject_at]["observed_at"]
    assert endpoint.gross_response.rounds[1]["observed_at"] > endpoint.gross_response.observations[recovered_at]["observed_at"]
    assert result["gross_response"]["current_state"] == "RECOVERED"
    assert result["status"] == "PAID_FLAT"
    assert not any(action["role"] == "EXIT" and action["classification"] == "REJECTED"
                   for action in endpoint.actions)


def test_target_without_native_admission_readiness_waits_without_fake_rejection():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
    )
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["leverage"] = "1"
    for product in configuration["products"]:
        product["tiers"][0]["rows"][0].update(imr="1", max_leverage="1")
    history = _run_with_configuration(history, configuration)
    request = replace(request, execution_input=replace(execution, historical_input=history))

    composition, endpoint = _dispatch_to_end(request)
    assert endpoint.gross_response is not None
    wait = next(
        row for row in endpoint.gross_response.observations
        if row["reason"] == "WAIT_ADMISSION_READINESS"
    )
    admission = wait["native_state"]["gross_admission"]
    assert D(admission["gross"]) <= D("1.5") * D(admission["effective_equity"])
    assert wait["native_state"]["current_admission_state_ready"] is False
    progress = wait["proposal_context"]["operations"]["gross_response"]
    assert progress["active"] is True
    assert progress["rejected_products"] == []
    assert endpoint.gross_response.rejected_products == set()
    assert len(endpoint.gross_response.rounds) == 2
    assert not any(
        action["role"] == "GROSS_REDUCTION" and action["classification"] == "REJECTED"
        for action in endpoint.actions
    )


def test_readiness_wait_does_not_bypass_ordinary_order_cancellation():
    request = _over_cap_gross_input(
        with_working_order=True,
        policy_end_offset_ms=150_000,
        settlement_extension_ms=180_000,
    )
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["leverage"] = "1"
    for product in configuration["products"]:
        product["tiers"][0]["rows"][0].update(imr="1", max_leverage="1")
    history = _run_with_configuration(history, configuration)
    request = replace(request, execution_input=replace(execution, historical_input=history))

    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    run = request.execution_input.historical_input
    result = composition._dispatch_due(run.range_start_ms * 16 + 6)

    assert result["classification"] == "SUCCESS", result
    observation = endpoint.gross_response.observations[-1]
    assert observation["reason"] == "CANCEL_PENDING"
    assert observation["native_state"]["orders"]
    cancellations = [action for action in endpoint.actions if action["role"] == "GROSS_CANCEL"]
    assert len(cancellations) == 1
    assert cancellations[0]["classification"] == "PENDING"
    assert not any(action["role"] == "GROSS_REDUCTION" for action in endpoint.actions)


@pytest.mark.parametrize(
    ("minimum_b", "expect_rejected_product", "expect_post_cut_wait"),
    [("0.8", True, False), ("0.01", False, True)],
)
def test_readiness_priority_and_cutoff_do_not_retry_gross_rounds(
    minimum_b: str, expect_rejected_product: bool, expect_post_cut_wait: bool,
):
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
    )
    if minimum_b != "0.01":
        request = _with_minimums(request, {"B-USDT-SWAP": minimum_b})
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["leverage"] = "1"
    for product in configuration["products"]:
        product["tiers"][0]["rows"][0].update(imr="1", max_leverage="1")
    history = _run_with_configuration(history, configuration)
    request = replace(request, execution_input=replace(execution, historical_input=history))
    request = _with_later_mark(request, "72")

    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    episode = endpoint.gross_response
    cutoff = endpoint.policy_end_ms * 16 + 6
    history = request.execution_input.historical_input
    witness_time = cutoff + 13 if expect_post_cut_wait else history.range_start_ms * 16 + 960_007
    dispatch = composition._dispatch_due(witness_time)
    assert dispatch["classification"] == "SUCCESS", dispatch
    matching = [
        row for row in episode.observations
        if row["native_state"]["current_admission_state_ready"] is False
        and D(row["native_state"]["gross_admission"]["gross"])
        <= D("1.5") * D(row["native_state"]["gross_admission"]["effective_equity"])
        and row["reason"] == "WAIT_ADMISSION_READINESS"
    ]
    assert matching
    wait = matching[-1]
    assert wait["reason"] == "WAIT_ADMISSION_READINESS"
    assert wait["after_state"] == "RESTRICTED"
    current_state = composition._codec.gross_response_projection()
    assert wait["observed_at"] == composition._current_time
    assert wait["scheduler_key"]["visible_at"] == wait["proposal_context"]["scheduler_key"]["visible_at"]
    assert wait["scheduler_key"]["schedule_sequence"] == wait["proposal_context"]["scheduler_key"]["schedule_sequence"]
    assert wait["scheduler_key"]["stable_id"] == wait["proposal_context"]["scheduler_key"]["stable_id"]
    assert composition._last_popped is not None
    assert wait["scheduler_key"]["visible_at"] == composition._last_popped[0]
    assert wait["native_state"]["owner_state_digest"] == current_state["owner_state_digest"]
    assert episode._obligations_clear(endpoint, composition, current_state)
    assert not episode._unverified_orders_to_cancel(current_state)
    if expect_post_cut_wait:
        assert wait["observed_at"] > cutoff
        assert wait["cause"] in ("SOURCE_GROUP", "DELIVERY"), [
            (row["cause"], row["observed_at"], row["reason"])
            for row in matching if row["observed_at"] > cutoff
        ]
        assert all(
            endpoint.roles_by_order.get(order.get("order_id")) == "EXIT"
            for order in wait["native_state"]["orders"]
        )
        assert all(
            endpoint.delivery_roles.get(delivery_id) == "EXIT"
            for delivery_id in wait["proposal_context"]["operations"]["pending_delivery_ids"]
        )
    else:
        assert wait["observed_at"] <= cutoff
    assert wait["proposal_context"]["operations"]["gross_response"]["active"] is True
    assert bool(episode.rejected_products) is expect_rejected_product
    assert episode.rejected_products == (
        {"B-USDT-SWAP"} if expect_rejected_product else set()
    )
    assert episode._obligations_clear(
        endpoint, composition, wait["native_state"],
    )
    assert episode.rounds
    assert all(round_row["observed_at"] < cutoff for round_row in episode.rounds)
    round_action_ids = {
        action_id for round_row in episode.rounds for action_id in round_row["action_ids"]
    }
    assert round_action_ids == {
        action["action_id"] for action in endpoint.actions
        if action["role"] == "GROSS_REDUCTION"
    }
    assert not any(
        action["role"] == "GROSS_REDUCTION"
        and action["effective_at"] >= cutoff
        for action in endpoint.actions
    )
    assert all(round_row["observed_at"] < cutoff for round_row in episode.rounds)
    result = composition._dispatch_due(history.range_end_ms * 16 + 6)
    assert result["classification"] == "SUCCESS", result


def test_distinct_same_time_delivery_keys_each_advance_gross_readiness():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
    )
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["leverage"] = "1"
    for product in configuration["products"]:
        product["tiers"][0]["rows"][0].update(imr="1", max_leverage="1")
    history = _run_with_configuration(history, configuration)
    request = replace(request, execution_input=replace(execution, historical_input=history))
    request = _with_later_mark(request, "72")

    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    episode = endpoint.gross_response
    visible_at = history.range_start_ms * 16 + 960_038
    before = composition._dispatch_due(visible_at - 1)
    assert before["classification"] == "SUCCESS", before
    prior_round_ids = [
        action["action_id"] for action in endpoint.actions
        if action["role"] == "GROSS_REDUCTION"
    ]
    prior_round_count = len(episode.rounds)
    prior_observation_count = len(episode.observations)
    result = composition._dispatch_due(visible_at)
    assert result["classification"] == "SUCCESS", result

    rows = [
        observation for index, observation in enumerate(episode.observations)
        if index >= prior_observation_count
        and observation["cause"] == "DELIVERY"
        and observation["proposal_context"]["scheduler_key"]["visible_at"] == visible_at
    ]
    keys = [
        (row["proposal_context"]["scheduler_key"]["queue_class"],
         row["proposal_context"]["scheduler_key"]["schedule_sequence"],
         row["proposal_context"]["scheduler_key"]["stable_id"])
        for row in rows
    ]
    assert len(rows) >= 2
    assert len(set(keys)) == len(keys)
    assert len({row["native_state"]["owner_state_digest"] for row in rows}) == 1
    assert rows[-1]["reason"] == "WAIT_ADMISSION_READINESS"
    current_state = composition._codec.gross_response_projection()
    assert episode._obligations_clear(endpoint, composition, current_state)
    assert len(episode.rounds) == prior_round_count
    assert [
        action["action_id"] for action in endpoint.actions
        if action["role"] == "GROSS_REDUCTION"
    ] == prior_round_ids


def test_proposal_rows_follow_declared_pool_order_not_native_projection_order():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
    )
    composition = _ReplayComposition._from_gross_response_run(request)
    history = request.execution_input.historical_input
    native_state = composition._codec.gross_response_projection()
    assert [row["product_id"] for row in native_state["products"]] == sorted(history.ordered_products)
    declared_order = tuple(reversed(history.ordered_products))
    proposal = propose_reductions(
        native_state,
        strategy_id=history.strategy_identity,
        ordered_products=declared_order,
    )
    assert [row["product_id"] for row in proposal.rows] == list(declared_order)
    assert [row["product_id"] for row in native_state["products"]] == sorted(history.ordered_products)


def test_reversed_valid_historical_pool_keeps_gross_action_order():
    request = _with_reversed_pool(_over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
    ))
    history = request.execution_input.historical_input
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None

    dispatch = composition._dispatch_due(history.range_start_ms * 16 + 6)
    assert dispatch["classification"] == "SUCCESS", dispatch
    assert endpoint.gross_response.rounds
    round_row = endpoint.gross_response.rounds[0]
    assert [row["product_id"] for row in round_row["proposal"]["rows"]] == list(
        history.ordered_products
    )
    action_ids = round_row["action_ids"]
    actions = [action for action in endpoint.actions if action["action_id"] in action_ids]
    assert [action["product_id"] for action in actions] == list(history.ordered_products)
    proposal_observation = next(
        observation for observation in endpoint.gross_response.observations
        if observation["native_state"]["owner_state_digest"]
        == round_row["proposal"]["native_state_digest"]
    )
    native_products = proposal_observation["native_state"]["products"]
    assert [row["product_id"] for row in native_products] == sorted(history.ordered_products)


def test_zero_gross_product_does_not_receive_minimum_quantity_uplift():
    request = _with_minimums(_over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
    ), {"B-USDT-SWAP": "10"})
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["cash"] = "40"
    positions = [row for row in configuration["positions"] if row["product_id"] == "A-USDT-SWAP"]
    configuration["positions"] = positions
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        initial_account_state=_initial_account_state(
            cash="40", positions=positions, orders=configuration["orders"],
        ),
    )
    request = replace(request, execution_input=replace(execution, historical_input=history))
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None

    dispatch = composition._dispatch_due(history.range_start_ms * 16 + 6)
    assert dispatch["classification"] == "SUCCESS", dispatch
    assert endpoint.gross_response.rounds
    proposal_rows = endpoint.gross_response.rounds[0]["proposal"]["rows"]
    zero_product = next(
        row for row in endpoint.gross_response.observations[0]["native_state"]["products"]
        if row["product_id"] == "B-USDT-SWAP"
    )
    assert D(zero_product["signed_contracts"]) == 0
    assert all(row["product_id"] != "B-USDT-SWAP" for row in proposal_rows)
    assert [action["product_id"] for action in endpoint.actions
            if action["role"] == "GROSS_REDUCTION"] == ["A-USDT-SWAP"]


def test_running_source_grid_intent_is_rejected_by_gross_guard_without_l_mutation():
    request = _over_cap_gross_input(
        policy_end_offset_ms=30_000,
        with_working_order=True,
        late_fill_order=True,
    )
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["cash"] = "109"
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        initial_account_state=_initial_account_state(
            cash="109", positions=configuration["positions"], orders=configuration["orders"],
        ),
    )
    cache = cast(dict[str, Any], decode_canonical(history.initial_policy_cache))
    next(row for row in cache["rows"] if row["name"] == "A-USDT-SWAP")["歩差"] = "0.1"
    history = replace(history, initial_policy_cache=canonical_bytes(cache))
    request = replace(request, execution_input=replace(execution, historical_input=history))
    request = _with_running_source(request)
    request = _with_first_market_price(request, "A-USDT-SWAP", "99")
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    run = request.execution_input.historical_input
    original_rows = [dict(row) for row in composition._policy.rows]
    start = run.range_start_ms * 16 + 6

    source_groups = []
    while composition._queue and composition._queue[0][0] <= start + 5_000 * 16:
        key = composition._queue[0]
        result = composition._dispatch_due(key[0])
        assert result["classification"] == "SUCCESS", result
        for record in composition._records.values():
            item = record["item"]
            if (item.get("kind") == "SOURCE_GROUP" and "group_result" in record["result"]
                    and item["group"]["group_id"] not in endpoint.action_by_group):
                if any(
                    member.get("kind") == "INTENT"
                    and member.get("payload", {}).get("client_order_id") != "PAID-SEED-0001"
                    for member in item["group"].get("members", [])
                ):
                    source_groups.append(record)
        if source_groups:
            break

    assert source_groups
    group_result = source_groups[-1]["result"]["group_result"]
    rejections = group_result.get("rejections", [])
    assert rejections
    members = source_groups[-1]["item"]["group"]["members"]
    assert source_groups[-1]["result"]["gross_group_control"]["restricted"] is True
    assert any(row.get("reason") == "GROSS_CAP_EXCEEDED" for row in rejections)
    assert any(member.get("kind") == "INTENT" for member in members)
    assert composition._policy.running is True
    assert composition._policy.rows == original_rows


def test_running_source_policy_view_excludes_only_gross_reduction_orders(monkeypatch):
    request = _over_cap_gross_input(
        policy_end_offset_ms=150_000, settlement_extension_ms=180_000,
        with_working_order=True,
    )
    request = _with_running_source(request)
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None

    open_order_rows: list[tuple[set[str], set[str]]] = []
    policy_views: list[set[str]] = []
    original_apply = composition._apply_payload
    original_open_orders = composition._policy.open_orders

    def capture_apply(delivery):
        if delivery["payload_kind"] == "OPEN_ORDER_SNAPSHOT":
            raw_ids = {
                row["order_id"]
                for row in delivery["immutable_payload"]["rows"]
            }
            gross_ids = {
                order_id
                for order_id, role in endpoint.roles_by_order.items()
                if role == "GROSS_REDUCTION"
            }
            open_order_rows.append((raw_ids, raw_ids & gross_ids))
        original_apply(delivery)

    def capture_policy_view(rows):
        policy_views.append({row["ordId"] for row in rows})
        original_open_orders(rows)

    monkeypatch.setattr(composition, "_apply_payload", capture_apply)
    monkeypatch.setattr(composition._policy, "open_orders", capture_policy_view)
    result = composition._dispatch_due(
        request.execution_input.historical_input.range_end_ms * 16 + 6
    )

    assert result["classification"] == "SUCCESS", result
    assert len(policy_views) == len(open_order_rows) > 0
    assert any(gross_ids for _, gross_ids in open_order_rows)
    for (raw_ids, excluded_ids), visible_ids in zip(open_order_rows, policy_views, strict=True):
        assert visible_ids == raw_ids - excluded_ids
        assert excluded_ids <= raw_ids
    gross_order_ids = {
        order_id
        for order_id, role in endpoint.roles_by_order.items()
        if role == "GROSS_REDUCTION"
    }
    assert not any(
        member.get("kind") == "CANCEL_REQUEST"
        and member.get("payload", {}).get("targets", [{}])[0].get("target_order_id")
        in gross_order_ids
        for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
        for member in record["item"]["group"].get("members", [])
    )


def test_exact_two_to_one_gross_boundary_does_not_start_response_episode():
    request = _over_cap_gross_input()
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["cash"] = "100"
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        initial_account_state=_initial_account_state(
            cash="100", positions=configuration["positions"], orders=configuration["orders"],
        ),
    )
    request = replace(request, execution_input=replace(execution, historical_input=history))

    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    initial = composition._codec.gross_response_projection()["gross_admission"]
    assert D(initial["gross"]) == D("200")
    assert D(initial["effective_equity"]) == D("100")
    assert D(initial["gross_ratio"]["numerator"]) == D("200")
    assert D(initial["gross_ratio"]["denominator"]) == D("100")

    dispatch = composition._dispatch_due(history.range_start_ms * 16 + 6)

    assert dispatch["classification"] == "SUCCESS"
    assert endpoint.gross_response.observations[-1]["after_state"] == "IDLE"
    assert endpoint.gross_response.observations[-1]["reason"] is None
    assert endpoint.gross_response.rounds == []
    assert not any(action["role"] == "GROSS_REDUCTION" for action in endpoint.actions)


def test_native_product_fees_and_steps_drive_actual_proportional_round():
    request = _over_cap_gross_input(policy_end_offset_ms=30_000)
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    product = next(row for row in configuration["products"] if row["product_id"] == "A-USDT-SWAP")
    product["taker_fee_rate"] = "0.002"
    product["specs"][0]["quantity_step"] = "0.01"
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        spec_before=tuple(
            replace(row, quantity_step=D("0.01")) if row.product_id == "A-USDT-SWAP" else row
            for row in history.spec_before
        ),
        spec_after=tuple(
            replace(row, quantity_step=D("0.01")) if row.product_id == "A-USDT-SWAP" else row
            for row in history.spec_after
        ),
    )
    cache = cast(dict[str, Any], decode_canonical(history.initial_policy_cache))
    market = next(row for row in cache["markets"] if row["product_id"] == "A-USDT-SWAP")
    market["lotSz"] = "0.01"
    history = replace(history, initial_policy_cache=canonical_bytes(cache))
    request = replace(request, execution_input=replace(execution, historical_input=history))

    composition, endpoint = _dispatch_to_end(request)
    assert endpoint.gross_response is not None
    first_round = endpoint.gross_response.rounds[0]
    proposal = first_round["proposal"]
    assert D(proposal["estimated_fee_rate"]) == D("0.0015")
    proposal_rows = {row["product_id"]: row for row in proposal["rows"]}
    assert set(proposal_rows) == {"A-USDT-SWAP", "B-USDT-SWAP"}
    for product_id, step in (("A-USDT-SWAP", D("0.01")), ("B-USDT-SWAP", D("0.001"))):
        row = proposal_rows[product_id]
        raw_quantity = D(row["raw_quantity"])
        expected = (raw_quantity / step).to_integral_value(rounding=ROUND_CEILING) * step
        assert D(row["quantity"]) == expected
    actions = [action for action in endpoint.actions if action["role"] == "GROSS_REDUCTION"]
    first_actions = [action for action in actions if action["action_id"] in first_round["action_ids"]]
    first_group_ids = {action["group_id"] for action in first_actions}
    first_payloads = [
        record["item"]["group"]["members"][0]["payload"]
        for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
        and record["item"]["group"]["group_id"] in first_group_ids
    ]
    assert {row["product_id"] for row in first_payloads} == set(proposal_rows)
    assert {
        row["product_id"]: D(row["quantity_contracts"])
        for row in first_payloads
    } == {product_id: D(row["quantity"]) for product_id, row in proposal_rows.items()}
    assert all(action["classification"] != "REJECTED" for action in first_actions)


def test_minimum_rejection_remains_incomplete_at_cutoff_but_paid_exit_continues():
    request = _over_cap_gross_input(policy_end_offset_ms=30_000)
    request = _with_minimums(request, {"B-USDT-SWAP": "0.8"})
    composition, endpoint = _dispatch_to_end(request)
    assert endpoint.gross_response is not None
    result = endpoint.result(composition)

    reductions = [action for action in endpoint.actions if action["role"] == "GROSS_REDUCTION"]
    assert len(reductions) == 2
    assert [(action["product_id"], action["classification"]) for action in reductions] == [
        ("A-USDT-SWAP", "COMMITTED"), ("B-USDT-SWAP", "REJECTED"),
    ]
    assert reductions[1]["result"]["rejections"]
    assert len(endpoint.gross_response.rounds) == 1
    assert result["gross_response"]["current_state"] == "INCOMPLETE"
    assert result["status"] == "INCOMPLETE"
    exits = [action for action in endpoint.actions if action["role"] == "EXIT"]
    assert {action["product_id"] for action in exits} == {"A-USDT-SWAP", "B-USDT-SWAP"}
    assert endpoint.rejected_exit_products == set()


def test_existing_collective_reducing_order_holds_native_capacity_without_cancel():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
        with_working_order=True,
    )
    request = _with_existing_reducing_order(request, "0.5")
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None

    run = request.execution_input.historical_input
    assert composition._dispatch_due(run.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    observation = endpoint.gross_response.observations[-1]
    native_products = observation["native_state"]["products"]
    product_a = next(row for row in native_products if row["product_id"] == "A-USDT-SWAP")
    assert D(product_a["closing_capacity"]) == D("0.5")
    assert observation["after_state"] == "RESTRICTED"
    assert observation["reason"] == "WAIT_CAPACITY"
    assert observation["proposal"]["state"] == "WAIT_CAPACITY"
    assert observation["proposal"]["rows"][0]["reason"] == "WAIT_CAPACITY"
    assert endpoint.actions == []
    assert [order["order_id"] for order in composition._codec.historical_working_orders()] == [
        "PAID-SEED-ORDER-A",
    ]


def test_partial_gross_reduction_keeps_one_order_per_product_through_cutoff():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000,
        policy_end_offset_ms=150_000,
    )
    request = _with_limited_settlement_volume(request, "0.1")
    composition, endpoint = _dispatch_to_end(request)
    assert endpoint.gross_response is not None
    result = endpoint.result(composition)

    reductions = [action for action in endpoint.actions if action["role"] == "GROSS_REDUCTION"]
    assert len(reductions) == 2
    assert len({action["order_id"] for action in reductions}) == len(reductions)
    assert len(endpoint.gross_response.rounds) == 1
    gross_order_ids = {action["order_id"] for action in reductions}
    fills_by_order: dict[str, list[D]] = {}
    for record in composition._records.values():
        if record["item"].get("kind") != "HISTORICAL_MARKET_STEP":
            continue
        for product in record.get("result", {}).get("historical_result", {}).get("products", []):
            for fill in product["fills"]:
                if fill["order_id"] in gross_order_ids:
                    fills_by_order.setdefault(fill["order_id"], []).append(fill["quantity_contracts"])
    assert set(fills_by_order) == gross_order_ids
    assert all(len(chunks) > 1 for chunks in fills_by_order.values())
    assert result["status"] == "INCOMPLETE"
    assert result["gross_response"]["current_state"] in {"INCOMPLETE", "RECOVERED"}


@pytest.mark.parametrize(
    ("cash", "expected_terminal"),
    [("50", "LIQUIDATED_INSOLVENT"), ("99.003", "LIQUIDATED_FLAT")],
)
def test_native_liquidation_remains_terminal_without_gross_response_resurrection(
    cash: str, expected_terminal: str,
):
    request = _over_cap_gross_input()
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    positions = [dict(
        product_id="A-USDT-SWAP",
        side="LONG",
        quantity_contracts="1",
        lots=[dict(
            seed_execution_id="GROSS-LIQUIDATION-SEED",
            seed_sequence=0,
            strategy_id=history.strategy_identity,
            quantity_contracts="1",
            entry_price="100",
        )],
    )]
    configuration["cash"] = cash
    configuration["positions"] = positions
    configuration["orders"] = []
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        initial_account_state=_initial_account_state(cash=cash, positions=positions, orders=[]),
        initial_policy_cache=_policy_cache(running=False),
    )
    request = replace(request, execution_input=replace(execution, historical_input=history))
    request = _with_first_market_price(request, "A-USDT-SWAP", "1")
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None

    run = request.execution_input.historical_input
    result = composition._dispatch_due(run.range_end_ms * 16 + 6)
    assert result["classification"] == "SUCCESS"
    assert composition._paid_native_terminal == expected_terminal
    assert not endpoint.actions

    final = endpoint.result(composition)
    assert final["status"] == expected_terminal
    assert final["gross_response"]["current_state"] == expected_terminal
    assert not endpoint.gross_response.rounds
