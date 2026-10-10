"""Formal gross report projection remains detached from native run-local facts."""

from __future__ import annotations

from copy import deepcopy
from decimal import Decimal as D
from dataclasses import replace
from typing import Any, cast

import pytest

from src.core.backtest.spider_gross_observation import (
    _overlimit,
    _peak,
    project_gross_observation,
)
from src.core.backtest.synthetic_scenario_replay import _CallbackPrefix, _ReplayComposition
from src.core.backtest.spider_run_artifacts import decode_canonical
from test_spider_gross_delivery import (
    _dispatch_to_reduction_notice,
    _duplicate_execution_occurrence,
)
from test_spider_gross_execution_input import _over_cap_gross_input
from test_spider_historical_input import _run_with_configuration
from test_spider_gross_response_episode import (
    _dispatch_to_end,
    _initial_account_state,
    _policy_cache,
    _with_existing_reducing_order,
    _with_first_market_price,
    _with_later_mark,
    _with_limited_settlement_volume,
    _with_minimums,
)


def test_formal_projection_uses_actual_native_trigger_and_is_detached():
    request = _over_cap_gross_input(policy_end_offset_ms=30_000)
    history = request.execution_input.historical_input
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    result = composition._dispatch_due(history.range_end_ms * 16 + 6)
    assert result["classification"] == "SUCCESS"

    episode = endpoint.gross_response
    facts_before = deepcopy((episode.observations, episode.rounds, endpoint.actions))
    native_projection = composition._codec.gross_response_projection()
    report = project_gross_observation(
        request, episode,
        endpoint_projection={"current_state": episode.state, "reason": episode.reason},
    )

    assert report is not None
    assert set(report) == {
        "schema_version", "run_contract_sha256", "policy_id", "response_parameters",
        "authorization_scope", "observations", "episodes", "peak_ratio",
        "overlimit_intervals", "overlimit_duration_ms", "current_state", "reason",
    }
    assert report["schema_version"] == "spider_gross_response_observation_v1"
    assert report["authorization_scope"]["ordered_products"] == list(history.ordered_products)
    assert report["peak_ratio"]["numerator"] == D("200")
    assert report["peak_ratio"]["denominator"] == D("50")
    assert all(set(row) == {
        "key", "observed_at", "native_state", "before_state", "after_state", "reason",
        "episode_sequence",
    } for row in report["observations"])
    triggers = [row for row in report["observations"]
                if row["before_state"] in ("IDLE", "RECOVERED")
                and row["after_state"] in ("RESTRICTED", "INCOMPLETE")]
    assert len(triggers) == 1
    assert (triggers[0]["key"] is None
            or set(triggers[0]["key"]) == {
                "visible_at", "queue_class", "schedule_sequence", "stable_id",
            })
    assert report["episodes"][0]["trigger_key"] == triggers[0]["key"]
    assert report["episodes"][0]["trigger_state"] == triggers[0]["native_state"]
    assert len(report["episodes"][0]["rounds"]) >= 1
    assert report["episodes"][0]["rounds"][0]["completion_key"] is not None
    assert report["episodes"][0]["rounds"][0]["proposal"]["scope"] == {
        "kind": "RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1",
        "strategy_id": history.strategy_identity,
        "ordered_products": list(history.ordered_products),
    }
    assert len(report["episodes"][0]["rounds"][0]["proposal"]["rows"]) == 2
    assert D(report["episodes"][0]["rounds"][0]["proposal"]["estimated_fee"]) > 0
    assert report["episodes"][0]["rounds"][0]["proposal"]["proposal_context"].keys() == {
        "source_state", "operation_state",
    }
    assert (episode.observations, episode.rounds, endpoint.actions) == facts_before
    assert composition._codec.gross_response_projection() == native_projection

    report_context = report["episodes"][0]["rounds"][0]["proposal"]["proposal_context"]
    report_context["operation_state"].clear()
    report_context["source_state"]["policy_rows"].clear()
    assert episode.observations == facts_before[0]


def test_projection_has_no_ownerless_gross_observation():
    assert project_gross_observation(_over_cap_gross_input(), None) is None


def test_actual_exact_two_boundary_projects_no_episode_or_round():
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
    assert composition._dispatch_due(history.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    accepted = endpoint.result(composition)
    report = project_gross_observation(
        request, endpoint.gross_response, endpoint_projection=accepted["gross_response"],
    )
    assert report is not None
    assert report["episodes"] == []
    assert report["peak_ratio"]["numerator"] == D("200")
    assert report["peak_ratio"]["denominator"] == D("100")


def test_minimum_rejection_at_cutoff_is_reported_without_automatic_retry():
    request = _over_cap_gross_input(policy_end_offset_ms=30_000)
    request = _with_minimums(request, {"B-USDT-SWAP": "0.8"})
    composition, endpoint = _dispatch_to_end(request)
    assert endpoint.gross_response is not None
    accepted = endpoint.result(composition)
    report = project_gross_observation(
        request, endpoint.gross_response, endpoint_projection=accepted,
    )
    assert report is not None
    assert report["current_state"] == accepted["gross_response"]["current_state"]
    rejected = [
        action for action in endpoint.actions
        if action["role"] == "GROSS_REDUCTION" and action["classification"] == "REJECTED"
    ]
    assert len(rejected) == 1
    rounds = [row for episode in report["episodes"] for row in episode["rounds"]]
    assert len(rounds) == 1
    assert rejected[0]["action_id"] in rounds[0]["action_ids"]
    assert rounds[0]["completion_key"] is not None
    assert rounds[0]["reason"] == "ACTION_REJECTED"


def test_actual_pending_reduction_notice_is_not_reported_as_round_completion():
    request = _over_cap_gross_input(
        policy_end_offset_ms=150_000, settlement_extension_ms=180_000,
    )
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    _dispatch_to_reduction_notice(composition, endpoint)

    report = project_gross_observation(
        request, endpoint.gross_response,
        endpoint_projection={
            "current_state": endpoint.gross_response.state,
            "reason": endpoint.gross_response.reason,
        },
    )

    assert report is not None
    rounds = [round_row for episode in report["episodes"] for round_row in episode["rounds"]]
    assert rounds
    assert rounds[-1]["action_ids"]
    assert rounds[-1]["completion_key"] is None
    assert endpoint.pending_delivery_ids


def test_final_cut_uses_explicit_same_owner_operation_and_native_facts():
    request = _over_cap_gross_input(
        policy_end_offset_ms=150_000, settlement_extension_ms=180_000,
    )
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    _dispatch_to_reduction_notice(composition, endpoint)
    episode = endpoint.gross_response
    assert episode.observations
    final_facts = endpoint.gross_response._proposal_context(endpoint, composition)
    native_state = composition._codec.gross_response_projection()
    report = project_gross_observation(
        request, episode,
        endpoint_projection=episode.observation(),
        final_facts={"operations": final_facts["operations"], "native_state": native_state},
    )
    assert report is not None
    remaining = report["episodes"][-1]["remaining_obligations"]
    assert remaining
    assert remaining[0]["native_state"] == native_state
    assert remaining[0]["operation_state"] == final_facts["operations"]


def test_actual_wait_capacity_proposal_is_retained_without_fake_round():
    request = _over_cap_gross_input(
        with_working_order=True, policy_end_offset_ms=150_000,
        settlement_extension_ms=180_000,
    )
    request = _with_existing_reducing_order(request, "0.5")
    history = request.execution_input.historical_input
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    assert composition._dispatch_due(history.range_start_ms * 16 + 6)["classification"] == "SUCCESS"
    projection = endpoint.gross_response.observation()

    report = project_gross_observation(request, endpoint.gross_response, endpoint_projection=projection)

    assert report is not None and len(report["episodes"]) == 1
    attempts = [row for row in report["episodes"][0]["rounds"]
                if row["proposal"]["state"] == "WAIT_CAPACITY"]
    assert len(attempts) == 1
    assert attempts[0]["sequence"] is None
    assert attempts[0]["action_ids"] == []
    assert attempts[0]["completion_key"] is None
    assert D(attempts[0]["proposal"]["estimated_fee"]) > 0


def test_actual_recovery_rebreach_report_keeps_native_transitions_and_global_rounds():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000, policy_end_offset_ms=150_000,
    )
    request = _with_minimums(request, {"B-USDT-SWAP": "0.8"})
    request = _with_later_mark(request, "37.5")
    composition, endpoint = _dispatch_to_end(request)
    assert endpoint.gross_response is not None
    accepted = endpoint.result(composition)

    report = project_gross_observation(
        request, endpoint.gross_response,
        endpoint_projection=accepted,
    )

    assert report is not None and len(report["episodes"]) == 2
    first, second = report["episodes"]
    assert first["recovered_key"] is not None
    assert first["recovery_state"] == next(
        row["native_state"] for row in report["observations"]
        if row["key"] == first["recovered_key"]
    )
    assert second["trigger_key"] is not None
    assert [row["sequence"] for episode in report["episodes"] for row in episode["rounds"]
            if row["sequence"] is not None] == [1, 2]
    assert report["current_state"] == "RECOVERED"
    assert report["reason"] is None
    first_rejection = report["episodes"][0]["rounds"][0]
    assert first_rejection["action_ids"]
    assert first_rejection["completion_key"] is not None
    assert first_rejection["reason"] == "ACTION_REJECTED"


def test_native_readiness_false_is_reported_without_inventing_rejection():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000, policy_end_offset_ms=150_000,
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
    accepted = endpoint.result(composition)
    report = project_gross_observation(
        request, endpoint.gross_response, endpoint_projection=accepted,
    )
    assert report is not None
    assert report["current_state"] == accepted["gross_response"]["current_state"]
    wait = next(row for row in report["observations"]
                if row["reason"] == "WAIT_ADMISSION_READINESS")
    assert wait["native_state"]["current_admission_state_ready"] is False
    assert not any(
        action["role"] == "GROSS_REDUCTION" and action["classification"] == "REJECTED"
        for action in endpoint.actions
    )


def test_actual_duplicate_native_delivery_observations_are_not_deduplicated():
    request = _over_cap_gross_input(
        policy_end_offset_ms=150_000, settlement_extension_ms=180_000,
    )
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    notice = _dispatch_to_reduction_notice(composition, endpoint)
    delivery = notice["item"]["delivery"]
    duplicate = _duplicate_execution_occurrence(composition, notice)
    assert composition._enqueue(dict(kind="DELIVERY", delivery=duplicate))["classification"] == "PENDING"
    duplicate_key = composition._records[(2, duplicate["delivery_id"])]["key"]
    assert composition._dispatch_due(duplicate_key[0])["classification"] == "SUCCESS"
    projection = endpoint.gross_response.observation()

    report = project_gross_observation(request, endpoint.gross_response, endpoint_projection=projection)

    assert report is not None
    source_delivery_checks = sum(
        row.get("cause") == "DELIVERY"
        and row.get("proposal_context", {}).get("scheduler_key", {}).get("visible_at")
        == duplicate_key[0]
        for row in endpoint.gross_response.observations
    )
    report_delivery_checks = sum(
        row["key"] is not None
        and row["key"]["queue_class"] == "DELIVERY"
        and row["key"]["visible_at"] == duplicate_key[0]
        for row in report["observations"]
    )
    assert source_delivery_checks > 0
    assert report_delivery_checks == source_delivery_checks
    assert delivery["delivery_id"] not in endpoint.pending_delivery_ids


def _metric_observation(
    *, observed_at: int, gross: str, equity: str,
    key: dict[str, object] | None, cause: str = "MARKET_STEP",
) -> dict[str, object]:
    return {
        "cause": cause,
        "observed_at": observed_at,
        "scheduler_key": None,
        "native_state": {"gross_admission": {
            "gross": gross, "effective_equity": equity,
        }},
        "proposal_context": {"scheduler_key": key},
    }


def _metric_key(visible_at: int, stable_id: str) -> dict[str, object]:
    return {
        "visible_at": visible_at, "queue_class": "HISTORICAL_MARKET_STEP",
        "schedule_sequence": 0, "stable_id": stable_id,
    }


def test_metric_projection_keeps_null_key_peak_ties_and_raw_millisecond_units():
    first = _metric_observation(
        observed_at=1606, gross="21", equity="10", key=None,
        cause="START_READINESS",
    )
    tied = _metric_observation(
        observed_at=1614, gross="42", equity="20",
        key=_metric_key(1614, "tie"),
    )
    recovered = _metric_observation(
        observed_at=3206, gross="20", equity="10",
        key=_metric_key(3206, "recover"),
    )

    peak = _peak([first, tied])
    assert peak is not None and peak["key"] is None
    intervals, duration = _overlimit([first, tied, recovered], None)
    assert duration == 100
    assert intervals == [{
        "start_key": None, "end_key": _metric_key(3206, "recover"),
        "end_cut": 3206, "open": False,
    }]


def test_metric_projection_open_cut_is_scaled_scheduler_time_and_invalid_order_rejects():
    start = _metric_observation(
        observed_at=1606, gross="21", equity="10",
        key=_metric_key(1606, "start"),
    )
    intervals, duration = _overlimit(
        [start], _metric_key(3206, "settlement-cut"),
    )
    assert duration == 100
    assert intervals[0]["end_cut"] == 3206
    assert intervals[0]["open"] is True

    backwards = _metric_observation(
        observed_at=1590, gross="20", equity="10",
        key=_metric_key(1590, "backwards"),
    )
    with pytest.raises(ValueError, match="INVALID_SCHEDULER_ORDER"):
        _overlimit([start, backwards], None)


def test_nonpositive_equity_closes_observed_overlimit_interval_without_ratio():
    start = _metric_observation(
        observed_at=1606, gross="21", equity="10",
        key=_metric_key(1606, "start"),
    )
    terminal = _metric_observation(
        observed_at=2406, gross="200", equity="0",
        key=_metric_key(2406, "insolvent"),
    )
    peak = _peak([start, terminal])
    intervals, duration = _overlimit([start, terminal], None)
    assert peak == {"numerator": D("21"), "denominator": D("10"),
                    "key": _metric_key(1606, "start")}
    assert duration == 50
    assert intervals == [{
        "start_key": _metric_key(1606, "start"),
        "end_key": _metric_key(2406, "insolvent"),
        "end_cut": 2406,
        "open": False,
    }]


def test_actual_partial_reduction_report_keeps_native_terminal_evidence():
    request = _over_cap_gross_input(
        settlement_extension_ms=180_000, policy_end_offset_ms=150_000,
    )
    request = _with_limited_settlement_volume(request, "0.1")
    composition, endpoint = _dispatch_to_end(request)
    assert endpoint.gross_response is not None
    accepted = endpoint.result(composition)
    report = project_gross_observation(
        request, endpoint.gross_response,
        endpoint_projection=accepted["gross_response"],
    )
    assert report is not None and report["current_state"] in {"INCOMPLETE", "RECOVERED"}
    rounds = [row for episode in report["episodes"] for row in episode["rounds"]]
    assert rounds and rounds[0]["action_ids"]
    assert rounds[0]["completion_key"] is None
    assert any(item["remaining_obligations"] for item in report["episodes"])


def test_true_callback_failure_report_preserves_failed_cut_and_pending_notice(monkeypatch):
    request = _over_cap_gross_input(
        policy_end_offset_ms=150_000, settlement_extension_ms=180_000,
    )
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    notice = _dispatch_to_reduction_notice(composition, endpoint)
    delivery_id = notice["item"]["delivery"]["delivery_id"]
    original_capture = composition._capture_callback

    def fail_notice(candidate):
        if candidate["delivery_id"] == delivery_id:
            return _CallbackPrefix((), RuntimeError("injected report failure"))
        return original_capture(candidate)

    monkeypatch.setattr(composition, "_capture_callback", fail_notice)
    failure = composition._dispatch_due(notice["key"][0])
    assert failure["classification"] == "TERMINAL"
    accepted = endpoint.result(composition)
    final_facts = endpoint.gross_response._proposal_context(endpoint, composition)
    final_native_state = composition._codec.gross_response_projection()
    report = project_gross_observation(
        request, endpoint.gross_response,
        endpoint_projection=accepted["gross_response"],
        final_facts={
            "operations": final_facts["operations"],
            "native_state": final_native_state,
        },
    )
    assert report is not None and report["current_state"] == "FAILED"
    assert report["reason"] == accepted["gross_response"]["reason"]
    assert report["episodes"][0]["rounds"][-1]["completion_key"] is None
    assert report["episodes"][0]["remaining_obligations"]
    assert report["episodes"][0]["remaining_obligations"][0]["operation_state"] == final_facts["operations"]
    assert report["episodes"][0]["remaining_obligations"][0]["native_state"] == final_native_state
    assert endpoint.pending_delivery_ids


@pytest.mark.parametrize("cash, expected", [("50", "LIQUIDATED_INSOLVENT"), ("99.003", "LIQUIDATED_FLAT")])
def test_native_economic_terminal_report_preserves_terminal_state(cash: str, expected: str):
    from test_spider_historical_input import _run_with_configuration

    request = _over_cap_gross_input()
    execution = request.execution_input
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    positions = [dict(
        product_id="A-USDT-SWAP", side="LONG", quantity_contracts="1",
        lots=[dict(seed_execution_id="REPORT-LIQUIDATION-SEED", seed_sequence=0,
                   strategy_id=history.strategy_identity, quantity_contracts="1",
                   entry_price="100")],
    )]
    configuration.update(cash=cash, positions=positions, orders=[])
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
    assert composition._dispatch_due(history.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    accepted = endpoint.result(composition)
    report = project_gross_observation(
        request, endpoint.gross_response, endpoint_projection=accepted["gross_response"],
    )
    assert report is not None and report["current_state"] == expected
    assert report["peak_ratio"] is not None
    assert report["peak_ratio"]["denominator"] > 0
    if expected == "LIQUIDATED_INSOLVENT":
        assert any(
            D(row["native_state"]["gross_admission"]["effective_equity"]) <= 0
            for row in report["observations"]
        )
    assert report["overlimit_intervals"] == []
