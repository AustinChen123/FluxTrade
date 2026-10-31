"""Closed semantic identity for original GRID with gross response."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from typing import Any, cast

import pytest

from src.core.backtest.spider_gross_execution_input import (
    ADMISSION_POLICY_ID,
    NATIVE_PROFILE_ID,
    POLICY_ID,
    RESPONSE_POLICY_ID,
    SCHEMA_VERSION,
    GrossAuthorizationScope,
    GrossResponseParameters,
    SpiderGrossExecutionInput,
    decode_spider_gross_execution_input,
    encode_spider_gross_execution_input,
    validate_spider_gross_execution_input,
)
from src.core.backtest.spider_gross_response import GrossResponseEpisode
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from src.core.backtest.spider_execution_input import validate_spider_execution_input
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from test_spider_paid_endpoint_native import _seeded_two_product_run
from test_spider_historical_input import (
    HistoricalBar,
    _initial_account_state,
    _manifest,
    _rows,
    _run_with_configuration,
)


def _gross_input() -> SpiderGrossExecutionInput:
    execution = _seeded_two_product_run()
    historical = execution.historical_input
    return SpiderGrossExecutionInput(
        SCHEMA_VERSION,
        POLICY_ID,
        NATIVE_PROFILE_ID,
        ADMISSION_POLICY_ID,
        RESPONSE_POLICY_ID,
        GrossResponseParameters(),
        GrossAuthorizationScope(historical.strategy_identity, historical.ordered_products),
        None,
        execution,
    )


def _over_cap_gross_input(
    *, settlement_extension_ms: int = 0, policy_end_offset_ms: int = 0,
    with_working_order: bool = False, late_fill_order: bool = False,
) -> SpiderGrossExecutionInput:
    execution = _seeded_two_product_run(
        with_working_order=with_working_order, late_fill_order=late_fill_order,
    )
    history = execution.historical_input
    configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
    configuration["cash"] = "50"
    positions = cast(list[dict[str, Any]], configuration["positions"])
    orders = cast(list[dict[str, Any]], configuration["orders"])
    history = _run_with_configuration(history, configuration)
    history = replace(
        history,
        initial_account_state=_initial_account_state(cash="50", orders=orders, positions=positions),
    )
    if settlement_extension_ms:
        old_end = history.range_end_ms
        new_end = old_end + settlement_extension_ms
        if settlement_extension_ms % 60_000:
            raise ValueError("settlement extension must use whole minutes")
        trade_additions: list[HistoricalBar] = []
        mark_additions: list[HistoricalBar] = []
        for product_id in history.ordered_products:
            trade_sequence = sum(row.product_id == product_id for row in history.trade_bars)
            mark_sequence = sum(row.product_id == product_id for row in history.mark_bars)
            for offset, bar in enumerate(_rows(product_id, old_end, trade=True)[:settlement_extension_ms // 60_000]):
                sequenced = replace(bar, source_sequence=trade_sequence + offset)
                trade_additions.append(replace(
                    sequenced,
                    source_row_hash=sha256(sequenced.canonical_row(trade=True)).hexdigest(),
                ))
            for offset, bar in enumerate(_rows(product_id, old_end, trade=False)[:settlement_extension_ms // 60_000]):
                sequenced = replace(bar, source_sequence=mark_sequence + offset)
                mark_additions.append(replace(
                    sequenced,
                    source_row_hash=sha256(sequenced.canonical_row(trade=False)).hexdigest(),
                ))
        trade_bars = tuple(sorted(history.trade_bars + tuple(trade_additions),
                                  key=lambda row: (row.bar_open_ms, history.ordered_products.index(row.product_id))))
        mark_bars = tuple(sorted(history.mark_bars + tuple(mark_additions),
                                 key=lambda row: (row.bar_open_ms, history.ordered_products.index(row.product_id))))
        configuration = cast(dict[str, Any], decode_canonical(history.configuration_bytes))
        for product in configuration["products"]:
            product["marks"][0]["valid_to"] = new_end
        history = _run_with_configuration(history, configuration)
        history = replace(
            history,
            range_end_ms=new_end,
            trade_bars=trade_bars,
            mark_bars=mark_bars,
            trade_manifest=_manifest(history.trade_manifest.source_id, trade_bars, trade=True),
            mark_manifest=_manifest(history.mark_manifest.source_id, mark_bars, trade=False),
            spec_after=tuple(replace(spec, effective_at_ms=new_end) for spec in history.spec_after),
        )
    execution = replace(
        execution,
        historical_input=history,
        policy_end_ms=history.range_start_ms + policy_end_offset_ms,
    )
    return SpiderGrossExecutionInput(
        SCHEMA_VERSION,
        POLICY_ID,
        NATIVE_PROFILE_ID,
        ADMISSION_POLICY_ID,
        RESPONSE_POLICY_ID,
        GrossResponseParameters(),
        GrossAuthorizationScope(history.strategy_identity, history.ordered_products),
        None,
        execution,
    )


def test_gross_execution_input_has_exact_closed_identity_and_roundtrips():
    value = _gross_input()
    raw = encode_spider_gross_execution_input(value)
    row = cast(dict[str, Any], decode_canonical(raw))
    assert set(row) == {
        "schema_version", "policy_id", "native_profile_id", "admission_policy_id",
        "response_policy_id", "response_parameters", "authorization_scope", "model_id",
        "execution_input",
    }
    assert set(row["response_parameters"]) == {
        "trigger_cap", "target_cap", "allocation", "quantity_rounding",
    }
    assert set(row["authorization_scope"]) == {"kind", "strategy_id", "ordered_products"}
    assert row["response_parameters"] == {
        "trigger_cap": "2",
        "target_cap": "1.5",
        "allocation": "GROSS_PROPORTIONAL_V1",
        "quantity_rounding": "CEIL_STEP_NO_MINIMUM_UPLIFT_V1",
    }
    assert row["authorization_scope"] == {
        "kind": "RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1",
        "strategy_id": value.authorization_scope.strategy_id,
        "ordered_products": list(value.authorization_scope.ordered_products),
    }
    assert row["model_id"] is None
    assert decode_spider_gross_execution_input(raw) == value
    assert encode_spider_gross_execution_input(decode_spider_gross_execution_input(raw)) == raw
    assert validate_spider_gross_execution_input(value) == sha256(canonical_bytes({
        "schema_version": SCHEMA_VERSION,
        "policy_id": POLICY_ID,
        "native_profile_id": NATIVE_PROFILE_ID,
        "admission_policy_id": ADMISSION_POLICY_ID,
        "response_policy_id": RESPONSE_POLICY_ID,
        "response_parameters": row["response_parameters"],
        "authorization_scope": row["authorization_scope"],
        "model_id": None,
        "execution_input_sha256": validate_spider_execution_input(value.execution_input),
    })).hexdigest()


@pytest.mark.parametrize(
    "mutation",
    ["missing", "extra", "schema", "policy", "trigger", "target", "allocation", "scope_kind", "scope_id", "scope_products", "model"],
)
def test_gross_execution_input_rejects_unknown_or_mismatched_semantics(mutation):
    value = _gross_input()
    row = cast(dict[str, Any], decode_canonical(encode_spider_gross_execution_input(value)))
    if mutation == "missing":
        del row["response_policy_id"]
    elif mutation == "extra":
        row["extra"] = "unsupported"
    elif mutation == "schema":
        row["schema_version"] = "spider_gross_execution_input_v2"
    elif mutation == "policy":
        row["policy_id"] = "OTHER"
    elif mutation == "trigger":
        row["response_parameters"]["trigger_cap"] = "2.0"
    elif mutation == "target":
        row["response_parameters"]["target_cap"] = "1.4"
    elif mutation == "allocation":
        row["response_parameters"]["allocation"] = "GROSS_NETTED_V1"
    elif mutation == "scope_kind":
        row["authorization_scope"]["kind"] = "ALL_ACCOUNT_POSITIONS_V1"
    elif mutation == "scope_id":
        row["authorization_scope"]["strategy_id"] = "another-strategy"
    elif mutation == "scope_products":
        row["authorization_scope"]["ordered_products"].reverse()
    elif mutation == "model":
        row["model_id"] = "UNAPPROVED_MODEL"
    with pytest.raises(ValueError):
        decode_spider_gross_execution_input(canonical_bytes(row))


def test_gross_input_identity_changes_with_nested_paid_semantics():
    value = _gross_input()
    changed_execution = replace(value.execution_input, policy_end_ms=value.execution_input.policy_end_ms + 1)
    changed = replace(value, execution_input=changed_execution)
    assert validate_spider_gross_execution_input(changed) != validate_spider_gross_execution_input(value)
    wrong_scope = replace(
        value,
        authorization_scope=GrossAuthorizationScope(
            value.authorization_scope.strategy_id,
            ("B-USDT-SWAP", "A-USDT-SWAP"),
        ),
    )
    with pytest.raises(ValueError):
        validate_spider_gross_execution_input(wrong_scope)


@pytest.mark.parametrize(
    ("settlement_extension_ms", "policy_end_offset_ms", "expected_status", "expected_episode_state"),
    [
        (0, 0, "PAID_FLAT", "RECOVERED"),
        (0, 30_000, "INCOMPLETE", "INCOMPLETE"),
        (180_000, 150_000, "PAID_FLAT", "RECOVERED"),
    ],
)
def test_gross_response_reductions_and_settlement(
    settlement_extension_ms: int,
    policy_end_offset_ms: int,
    expected_status: str,
    expected_episode_state: str,
):
    request = _over_cap_gross_input(
        settlement_extension_ms=settlement_extension_ms,
        policy_end_offset_ms=policy_end_offset_ms,
    )
    assert validate_spider_gross_execution_input(request)
    composition = _ReplayComposition._from_gross_response_run(request)
    endpoint = composition._paid_endpoint
    assert endpoint is not None and endpoint.gross_response is not None
    run = request.execution_input.historical_input
    dispatch = composition._dispatch_due(run.range_end_ms * 16 + 6)
    terminal_record = composition._records.get((3, composition._last_popped[3])) if composition._last_popped else None
    native_after_failure = composition._codec.gross_response_projection()
    gross_before = native_after_failure["gross_admission"]
    assert isinstance(gross_before, dict)
    assert dispatch["classification"] == "SUCCESS", dict(
        reason=dispatch.get("reason"), native_failure=dispatch.get("native_failure"),
        gross_observed=gross_before.get("gross"), equity=gross_before.get("effective_equity"),
        gate=native_after_failure.get("gate"), lifecycle=native_after_failure.get("lifecycle"),
        actions=[(row["role"], row["product_id"], row["classification"])
            for row in endpoint.actions],
        observations=[(row["before_state"], row["after_state"], row["reason"])
                      for row in endpoint.gross_response.observations],
        market_result=(
            {key: terminal_record.get("result", {}).get(key)
             for key in ("reason", "native_failure", "classification", "failure", "native_result")}
            if terminal_record else None
        ),
        last_popped=composition._last_popped,
    )
    result = endpoint.result(composition)
    assert result["status"] == expected_status
    assert {action["role"] for action in result["actions"]} <= {"GROSS_REDUCTION", "EXIT"}
    assert not any(action["role"] == "GRID" for action in endpoint.actions)
    actions = [action for action in result["actions"] if action["role"] == "GROSS_REDUCTION"]
    committed = [action for action in actions if action["classification"] == "COMMITTED"]
    pending = [action for action in actions if action["classification"] == "PENDING"]
    cutoff = request.execution_input.policy_end_ms * 16 + 6
    assert all(action["effective_at"] < cutoff for action in actions)
    if policy_end_offset_ms == 0:
        assert not actions
        assert {action["product_id"] for action in result["actions"] if action["role"] == "EXIT"} == set(run.ordered_products)
    else:
        assert {action["product_id"] for action in committed} == set(run.ordered_products)
        if settlement_extension_ms:
            assert len(committed) == 4
    if expected_status == "INCOMPLETE":
        assert "EXIT_PENDING" in result["reasons"]
        assert "RESIDUAL_POSITION" in result["reasons"]
        assert not pending
        assert len(committed) == 2
        gross_notice_times = [
            record["key"][0]
            for record in composition._records.values()
            if record["item"].get("kind") == "DELIVERY"
            and endpoint.delivery_roles.get(record["item"]["delivery"]["delivery_id"])
            == "GROSS_REDUCTION"
        ]
        exit_group_ids = {
            action["group_id"] for action in result["actions"] if action["role"] == "EXIT"
        }
        exit_group_times = [
            record["key"][0]
            for record in composition._records.values()
            if record["item"].get("kind") == "SOURCE_GROUP"
            and record["item"]["group"]["group_id"] in exit_group_ids
        ]
        assert gross_notice_times and exit_group_times
        assert max(gross_notice_times) < min(exit_group_times)
    else:
        assert not pending
    reduction_order_ids = {action["order_id"] for action in committed}
    fills = [
        fill
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        for product in record.get("result", {}).get("historical_result", {}).get("products", [])
        for fill in product["fills"]
        if fill["order_id"] in reduction_order_ids
    ]
    if reduction_order_ids:
        assert fills and {fill["order_id"] for fill in fills} == reduction_order_ids
    else:
        assert not fills
    gross_order_ids = {action["order_id"] for action in committed}
    gross_notices = [
        record for record in composition._records.values()
        if record["item"].get("kind") == "DELIVERY"
        and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
        and record["item"]["delivery"]["immutable_payload"]["order_id"]
        in gross_order_ids
    ]
    assert bool(gross_notices) is bool(gross_order_ids)
    assert all(record["result"].get("events") == [] for record in gross_notices)
    reductions_by_group = {action["group_id"]: action for action in committed}
    reduction_groups = [
        record["item"]["group"]["members"][0]["payload"]
        for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
        and record["item"]["group"]["group_id"] in reductions_by_group
        and record.get("result", {}).get("group_result", {}).get("classification") == "COMMITTED"
    ]
    assert {payload["product_id"] for payload in reduction_groups} == {
        action["product_id"] for action in committed
    }
    if policy_end_offset_ms:
        assert {payload["product_id"] for payload in reduction_groups} == set(run.ordered_products)
    assert all(payload["order_type"] == "MARKET" and payload["reduce_only"] is True
               for payload in reduction_groups)
    if reduction_groups:
        assert {payload["product_id"]: payload["side"] for payload in reduction_groups} == {
            "A-USDT-SWAP": "SHORT", "B-USDT-SWAP": "LONG",
        }
    total_fees = D(result["owner_evidence"]["inspection"]["total_fees"])
    assert total_fees > 0
    if policy_end_offset_ms == 0:
        assert total_fees == D("0.2")
    elif settlement_extension_ms == 0:
        assert total_fees == D("0.1252")
        assert "EXIT_PENDING" in result["reasons"]
    else:
        assert total_fees == D("0.2")
    assert result["gross_response"]["current_state"] == expected_episode_state
    assert endpoint.gross_response.observations
    initial_observation = endpoint.gross_response.observations[0]
    initial_gross = D(initial_observation["native_state"]["gross_admission"]["gross"])
    initial_equity = D(initial_observation["native_state"]["gross_admission"]["effective_equity"])
    assert initial_observation["before_state"] == "IDLE"
    assert initial_observation["after_state"] == "RESTRICTED"
    assert initial_gross > D(2) * initial_equity
    if expected_status == "PAID_FLAT":
        recovery = next(
            observation for observation in endpoint.gross_response.observations
            if observation["after_state"] == "RECOVERED"
        )
        recovered_gross = D(recovery["native_state"]["gross_admission"]["gross"])
        recovered_equity = D(recovery["native_state"]["gross_admission"]["effective_equity"])
        assert recovered_gross <= D("1.5") * recovered_equity
        assert recovery["native_state"]["current_admission_state_ready"] is True
        if settlement_extension_ms and policy_end_offset_ms:
            waiting_at_target = next(
                observation for observation in endpoint.gross_response.observations
                if D(observation["native_state"]["gross_admission"]["gross"])
                <= D("1.5") * D(observation["native_state"]["gross_admission"]["effective_equity"])
                and not observation["native_state"]["current_admission_state_ready"]
                and any(
                    action["role"] == "EXIT" and action["classification"] == "PENDING"
                    for action in observation["proposal_context"]["operations"]["actions"]
                )
            )
            assert waiting_at_target["after_state"] == "RECOVERED"
            assert waiting_at_target["reason"] is None
    assert endpoint.contract_hash == GrossResponseEpisode.admit(request).contract_hash
