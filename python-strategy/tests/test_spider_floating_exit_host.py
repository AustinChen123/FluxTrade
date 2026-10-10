"""Causal tests for the source-derived candidate host on the real native replay."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal as D
from hashlib import sha256
from typing import Any, cast

import pytest

from src.core.backtest import spider_bayes_model
from src.core.backtest.spider_bayes_replay import PRODUCTS
from src.core.backtest.spider_execution_input import decode_spider_execution_input
from src.core.backtest.spider_floating_exit_host import SpiderFloatingExitHost
from src.core.backtest.spider_floating_exit_input import (
    BAYES_REMAINING_COST_RISK_V1,
    FIXED_REMAINING_COST_TP_V1,
    POLICY_ID,
    SCHEMA_VERSION,
    SpiderFloatingExitInput,
    decode_spider_floating_exit_input,
    encode_spider_floating_exit_input,
    validate_spider_floating_exit_input,
)
from src.core.backtest.spider_historical_input import HistoricalInputError
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from src.core.backtest.synthetic_scenario_codec import ScenarioCodec, _native
from test_spider_bayes_replay import _manifest, _valid_source14_run
from test_spider_execution_input import _input
from test_spider_historical_input import _initial_account_state, _policy_cache


_MINUTE = 60_000
_FIFTEEN = 15 * _MINUTE


def _seeded_candidate_run(*, minutes: int = 17, position: bool = True):
    run = _valid_source14_run(minutes=minutes)
    config = cast(dict[str, Any], decode_canonical(run.configuration_bytes))
    for product in config["products"]:
        product["taker_fee_rate"] = "0.0006"
    config["cash"] = "10000"
    positions = []
    if position:
        positions = [dict(
            product_id=PRODUCTS[0], side="LONG", quantity_contracts="1",
            lots=[dict(seed_execution_id="FLOATING-SEED-A", seed_sequence=0,
                       strategy_id=run.strategy_identity, quantity_contracts="1",
                       entry_price="100")],
        )]
    config["positions"] = positions
    raw = canonical_bytes(config)
    policy = cast(dict[str, Any], decode_canonical(_policy_cache(PRODUCTS)))
    policy["rows"] = [
        {**row, "active": "true" if row["name"] in PRODUCTS[:2] else "false"}
        for row in policy["rows"]
    ]
    run = replace(
        run,
        run_id="candidate-source14-native",
        configuration_bytes=raw,
        configuration_sha256=sha256(raw).hexdigest(),
        initial_account_state=_initial_account_state(cash="10000", positions=positions),
        initial_policy_cache=canonical_bytes(policy),
    )
    return run


def _candidate(run, decision_policy=FIXED_REMAINING_COST_TP_V1, *, cutoff=None):
    execution = replace(
        _input(run),
        policy_end_ms=cutoff if cutoff is not None else run.range_start_ms + 16 * _MINUTE,
    )
    return SpiderFloatingExitInput(
        SCHEMA_VERSION, POLICY_ID, decision_policy, spider_bayes_model.MODEL_ID, execution,
    )


def _reprice_cut(run, cutoff_ms: int, *, ratios: dict[str, D]):
    trade_rows = []
    mark_rows = []
    for row in run.trade_bars:
        if row.bar_open_ms == cutoff_ms - _MINUTE and row.product_id in ratios:
            price = D(100 + PRODUCTS.index(row.product_id)) * ratios[row.product_id]
            row = replace(row, open=price, high=price, low=price, close=price)
        trade_rows.append(replace(row, source_row_hash=sha256(row.canonical_row(trade=True)).hexdigest()))
    for row in run.mark_bars:
        if row.bar_open_ms == cutoff_ms - _MINUTE and row.product_id in ratios:
            price = D(100 + PRODUCTS.index(row.product_id)) * ratios[row.product_id]
            row = replace(row, open=price, high=price, low=price, close=price)
        mark_rows.append(replace(row, source_row_hash=sha256(row.canonical_row(trade=False)).hexdigest()))
    trades, marks = tuple(trade_rows), tuple(mark_rows)
    return replace(
        run, trade_bars=trades, mark_bars=marks,
        trade_manifest=_manifest(run.trade_manifest, trades, trade=True, products=PRODUCTS),
        mark_manifest=_manifest(run.mark_manifest, marks, trade=False, products=PRODUCTS),
    )


def _advance(composition, until):
    result = composition._dispatch_due(until)
    if result["classification"] != "SUCCESS":
        terminal = composition._terminal or {}
        key = composition._last_popped
        record = composition._records.get((key[1], key[3])) if key is not None else None
        group = record.get("item", {}).get("group") if record is not None else None
        member = group["members"][0] if isinstance(group, dict) else {}
        details = (
            result.get("reason"), composition._last_popped,
            terminal.get("kind"), terminal.get("stable_id"), terminal.get("reason"),
            terminal.get("native_failure"),
            (group.get("group_effective_at"), member.get("stamp"), {
                key: member.get("payload", {}).get(key) for key in (
                    "product_id", "strategy_id", "side", "quantity_contracts", "limit_price",
                    "order_type", "reduce_only", "targets",
                )
            })
            if isinstance(group, dict) else None,
        )
        assert False, details
    return result


def test_floating_input_is_closed_canonical_and_candidate_hash_drives_native_identity():
    request = _candidate(_seeded_candidate_run())
    original = request.execution_input
    raw = encode_spider_floating_exit_input(request)
    decoded = decode_spider_floating_exit_input(raw)
    assert decoded == request
    assert encode_spider_floating_exit_input(decoded) == raw
    with pytest.raises(ValueError):
        decode_spider_execution_input(raw)
    baseline = validate_spider_floating_exit_input(request)
    assert validate_spider_floating_exit_input(replace(request, execution_input=replace(
        original, historical_input=replace(original.historical_input, run_id="other-transport-id"),
    ))) == baseline
    assert validate_spider_floating_exit_input(replace(request, decision_policy_id=BAYES_REMAINING_COST_RISK_V1)) != baseline

    composition = _ReplayComposition._from_floating_exit_run(decoded)
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    assert endpoint.contract_hash == baseline
    order_contract = composition._historical_order_contract
    assert order_contract is not None
    assert order_contract["run_contract_hash"] == baseline

    decoded_value = decode_canonical(raw)
    assert isinstance(decoded_value, dict)
    value = cast(dict[str, object], decoded_value)
    value["extra"] = True
    with pytest.raises(ValueError):
        decode_spider_floating_exit_input(canonical_bytes(value))

    for field in ("schema_version", "policy_id", "decision_policy_id", "model_id"):
        with pytest.raises(ValueError, match="unsupported floating-exit identity"):
            validate_spider_floating_exit_input(replace(request, **{field: []}))


@pytest.mark.parametrize("invalid_profile", ["fee", "source_order"])
def test_candidate_rejects_unsupported_fee_or_source_order_before_native_construction(
    monkeypatch, invalid_profile,
):
    products = tuple(reversed(PRODUCTS)) if invalid_profile == "source_order" else PRODUCTS
    run = _valid_source14_run(minutes=17, products=products)
    if invalid_profile == "fee":
        configuration = cast(dict[str, Any], decode_canonical(run.configuration_bytes))
        cast(list[dict[str, Any]], configuration["products"])[0]["taker_fee_rate"] = "0.0007"
        raw = canonical_bytes(configuration)
        run = replace(run, configuration_bytes=raw, configuration_sha256=sha256(raw).hexdigest())
    request = _candidate(run)

    def reject_native_construction(*args, **kwargs):
        raise AssertionError("native construction must follow candidate admission")

    monkeypatch.setattr(ScenarioCodec, "__init__", reject_native_construction)
    with pytest.raises(HistoricalInputError):
        _ReplayComposition._from_floating_exit_run(request)


@pytest.mark.parametrize("decision_policy", [FIXED_REMAINING_COST_TP_V1, BAYES_REMAINING_COST_RISK_V1])
def test_visible_native_tp_decision_routes_to_existing_reduce_only_exit(decision_policy):
    run = _seeded_candidate_run(minutes=22)
    cutoff = run.range_start_ms + _FIFTEEN
    run = _reprice_cut(run, cutoff, ratios={PRODUCTS[0]: D("1.04")})
    request = _candidate(run, decision_policy, cutoff=run.range_start_ms + 20 * _MINUTE)
    composition = _ReplayComposition._from_floating_exit_run(request)
    host = cast(SpiderFloatingExitHost, composition._floating_exit_host)
    _advance(composition, run.range_start_ms * 16 + 6)
    assert host.batches == []  # Initial prior is not a decision.

    _advance(composition, cutoff * 16 + 6)
    assert len(host.batches) == 1
    batch = host.batches[0]
    assert batch.cutoff_ms == cutoff and batch.visible_at == cutoff * 16 + 6
    assert batch.observed_at == cutoff * 16 + 6
    assert tuple(row.product_id for row in batch.rows) == PRODUCTS
    assert tuple(row.active for row in batch.rows) == (True, True, *(False for _ in PRODUCTS[2:]))
    assert batch.rows[0].reason == "TP"
    assert batch.rows[0].unrealized > 0
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    native_exit_observations: list[tuple[D, D]] = []
    b_working_during_a_exit: list[dict[str, Any]] = []
    original_exit_group = endpoint._exit_group

    def observe_exit_group(composition, product_id, quantity):
        operation = endpoint.product_operations.get(product_id)
        if operation is not None:
            position_rows = operation["positions"]
            assert isinstance(position_rows, list) and len(position_rows) == 1
            native_exit_observations.append((
                position_rows[0]["position_contracts"], quantity,
            ))
            b_working_during_a_exit.extend(
                dict(order) for order in composition._codec.historical_working_orders()
                if order["product_id"] == PRODUCTS[1]
                and endpoint.roles_by_order.get(order["order_id"]) == "GRID"
            )
        return original_exit_group(composition, product_id, quantity)

    endpoint._exit_group = observe_exit_group
    composition._policy.paused = True  # Pausing after the request does not cancel its native exit.
    _advance(composition, run.range_end_ms * 16 + 6)
    result = endpoint.result(composition)
    progress = endpoint.product_exit_progress(composition, PRODUCTS[0])
    assert progress["status"] == "EXITED"
    assert progress["positions"] == []
    assert progress["working_orders"] == []
    assert result["status"] in ("PAID_FLAT", "INCOMPLETE")
    assert result["owner_evidence"]["inspection"]["total_fees"] > 0
    assert not any(row["product_id"] == PRODUCTS[0] for row in result["pending_obligations"]["positions"])
    exit_actions = [
        action for action in endpoint.actions
        if action["role"] == "EXIT" and action["product_id"] == PRODUCTS[0]
    ]
    assert len(exit_actions) == 1
    assert exit_actions[0]["classification"] == "COMMITTED"
    exit_group = next(
        record["item"]["group"] for record in composition._records.values()
        if record["item"].get("kind") == "SOURCE_GROUP"
        and record["item"]["group"].get("group_id") == exit_actions[0]["group_id"]
    )
    exit_payload = exit_group["members"][0]["payload"]
    assert exit_payload["reduce_only"] is True
    assert len(native_exit_observations) == 1
    assert native_exit_observations[0][0] == native_exit_observations[0][1]
    assert exit_payload["quantity_contracts"] == native_exit_observations[0][0]
    fills = [
        fill
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        and "historical_result" in record.get("result", {})
        for product in record["result"]["historical_result"]["products"]
        for fill in product["fills"]
        if fill["order_id"] == exit_actions[0]["order_id"]
    ]
    assert fills
    assert sum((fill["quantity_contracts"] for fill in fills), D(0)) == exit_payload["quantity_contracts"]
    assert all(fill["price"] > D(0) for fill in fills)
    product_configs = {
        product["product_id"]: product
        for product in cast(dict[str, Any], decode_canonical(run.configuration_bytes))["products"]
    }
    native_fills = [
        (product["product_id"], fill, record["result"]["historical_result"]["raw_time_ms"])
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        and "historical_result" in record.get("result", {})
        for product in record["result"]["historical_result"]["products"]
        for fill in product["fills"]
    ]
    expected_native_fees = sum(
        (
            fill["quantity_contracts"] * fill["price"]
            * D(product_configs[product_id]["specs"][0]["contract_value"])
            * D(product_configs[product_id]["specs"][0]["multiplier"])
            * D(product_configs[product_id]["taker_fee_rate"])
            for product_id, fill, _ in native_fills
        ), D(0),
    )
    assert result["owner_evidence"]["inspection"]["total_fees"] == expected_native_fees
    exit_effective_at = exit_group["group_effective_at"]
    assert any(
        product_id == PRODUCTS[0]
        and endpoint.roles_by_order.get(fill["order_id"]) == "GRID"
        and raw_time * 16 < exit_effective_at
        and raw_time >= cutoff
        for product_id, fill, raw_time in native_fills
    )
    assert b_working_during_a_exit


def test_bayes_risk_uses_actual_native_cost_and_closed_source_posterior():
    run = _seeded_candidate_run()
    cutoff = run.range_start_ms + _FIFTEEN
    run = _reprice_cut(run, cutoff, ratios={product: D("0.8") for product in PRODUCTS})
    request = _candidate(run, BAYES_REMAINING_COST_RISK_V1)
    composition = _ReplayComposition._from_floating_exit_run(request)
    _advance(composition, cutoff * 16 + 6)
    host = cast(SpiderFloatingExitHost, composition._floating_exit_host)
    batch = host.batches[0]
    cost = composition._codec.historical_cost_projection()
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    native_a = next(row for row in cost["rows"] if row["product_id"] == PRODUCTS[0])
    assert batch.posterior[2] + batch.posterior[3] > D("0.77")
    assert native_a["signed_base_quantity"] == D("2")
    a_grid_fills = [
        fill
        for record in composition._records.values()
        if record["item"].get("kind") == "HISTORICAL_MARKET_STEP"
        and "historical_result" in record.get("result", {})
        for product in record["result"]["historical_result"]["products"]
        if product["product_id"] == PRODUCTS[0]
        for fill in product["fills"]
        if endpoint.roles_by_order.get(fill["order_id"]) == "GRID"
    ]
    assert a_grid_fills
    assert all(fill["price"] == D("80") for fill in a_grid_fills)
    assert sum((fill["quantity_contracts"] for fill in a_grid_fills), D(0)) == D("1")
    expected_average = (
        D("100") * (D(1) + D("0.0006"))
        + D("80") * (D(1) + D("0.0006"))
    ) / D(2)
    assert native_a["average_open_price"] == expected_average
    assert batch.rows[0].reason == "RISK"
    assert batch.rows[0].unrealized == (
        D("2") * (batch.rows[0].price - expected_average)
        - batch.rows[0].estimated_exit_fee
    )
    _advance(composition, run.range_end_ms * 16 + 6)
    assert any(action["role"] == "EXIT" and action["product_id"] == PRODUCTS[0]
               and action["classification"] == "COMMITTED" for action in endpoint.actions)
    assert endpoint.product_exit_progress(composition, PRODUCTS[0])["status"] == "EXITED"


@pytest.mark.parametrize("cache_change", ["stopped", "paused"])
def test_disabled_candidate_records_empty_active_decision_without_new_exit(cache_change):
    run = _seeded_candidate_run()
    cache = cast(dict[str, Any], decode_canonical(run.initial_policy_cache))
    cache["running" if cache_change == "stopped" else "paused"] = (
        False if cache_change == "stopped" else True
    )
    run = replace(run, initial_policy_cache=canonical_bytes(cache))
    cutoff = run.range_start_ms + _FIFTEEN
    run = _reprice_cut(run, cutoff, ratios={PRODUCTS[0]: D("1.04")})
    composition = _ReplayComposition._from_floating_exit_run(_candidate(run))
    host = cast(SpiderFloatingExitHost, composition._floating_exit_host)
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    _advance(composition, cutoff * 16 + 6)
    batch = host.batches[0]
    assert not any(row.active or row.reason is not None for row in batch.rows)
    assert not [action for action in endpoint.actions if action["role"] == "EXIT"]


def test_candidate_cutoff_at_range_end_leaves_only_the_existing_whole_run_drain():
    run = _seeded_candidate_run(minutes=17, position=False)
    cache = cast(dict[str, Any], decode_canonical(run.initial_policy_cache))
    cache["paused"] = True
    run = replace(run, initial_policy_cache=canonical_bytes(cache))
    request = _candidate(run, cutoff=run.range_start_ms + _FIFTEEN)
    composition = _ReplayComposition._from_floating_exit_run(request)

    _advance(composition, run.range_end_ms * 16 + 7)

    endpoint = composition._paid_endpoint
    assert endpoint is not None
    host = cast(SpiderFloatingExitHost, composition._floating_exit_host)
    assert host.batches == []
    assert endpoint.drain_started is True
    assert endpoint.product_operations == {}
    assert not [action for action in endpoint.actions if action["role"] == "EXIT"]


def test_two_closed_samples_are_exactly_once_detached_and_prefix_causal():
    run = _seeded_candidate_run(minutes=31, position=False)
    cache = cast(dict[str, Any], decode_canonical(run.initial_policy_cache))
    cache["paused"] = True
    run = replace(run, initial_policy_cache=canonical_bytes(cache))
    cutoff = run.range_start_ms + 30 * _MINUTE + 1
    request = _candidate(run, cutoff=cutoff)
    composition = _ReplayComposition._from_floating_exit_run(request)
    _advance(composition, run.range_end_ms * 16 + 7)

    host = cast(SpiderFloatingExitHost, composition._floating_exit_host)
    assert [(batch.cutoff_ms, batch.visible_at) for batch in host.batches] == [
        (run.range_start_ms + _FIFTEEN, (run.range_start_ms + _FIFTEEN) * 16 + 6),
        (run.range_start_ms + 2 * _FIFTEEN, (run.range_start_ms + 2 * _FIFTEEN) * 16 + 6),
    ]
    expected_first = host.batches[0]
    original_observation = host.observation()
    detached = host.observation()
    detached_batches = cast(list[dict[str, Any]], detached["batches"])
    cast(list[dict[str, Any]], detached_batches[0]["rows"])[0]["price"] = "0"
    assert host.observation() == original_observation

    # The changed row is strictly after the first 15-minute close; its prefix
    # decision must equal the same close from the full, unmodified run.
    changed = _reprice_cut(
        run, run.range_start_ms + _FIFTEEN + 2 * _MINUTE,
        ratios={product: D("1.5") for product in PRODUCTS},
    )
    prefix = _ReplayComposition._from_floating_exit_run(_candidate(changed, cutoff=cutoff))
    _advance(prefix, (run.range_start_ms + _FIFTEEN) * 16 + 6)
    prefix_host = cast(SpiderFloatingExitHost, prefix._floating_exit_host)
    assert len(prefix_host.batches) == 1
    assert prefix_host.batches[0] == expected_first

    repeated = _ReplayComposition._from_floating_exit_run(request)
    _advance(repeated, run.range_end_ms * 16 + 7)
    repeated_host = cast(SpiderFloatingExitHost, repeated._floating_exit_host)
    assert repeated_host.observation() == original_observation


def test_cost_projection_failure_at_cache_keeps_native_evidence_and_does_not_retry(monkeypatch):
    run = _seeded_candidate_run()
    cutoff = run.range_start_ms + _FIFTEEN
    run = _reprice_cut(run, cutoff, ratios={product: D("0.8") for product in PRODUCTS})
    composition = _ReplayComposition._from_floating_exit_run(_candidate(run, BAYES_REMAINING_COST_RISK_V1))
    _advance(composition, cutoff * 16 + 5)
    codec = composition._codec
    inspection_before = codec.inspect_state()
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    actions_before = [dict(action) for action in endpoint.actions]
    calls = 0

    def fail_cost_projection(self):
        nonlocal calls
        calls += 1
        raise _native.ScenarioReplayInvariantError("NATIVE_INVARIANT")

    monkeypatch.setattr(ScenarioCodec, "historical_cost_projection", fail_cost_projection)
    failure = composition._dispatch_due(cutoff * 16 + 6)
    assert failure["classification"] == "TERMINAL"
    assert failure["native_failure"] == {"kind": "INVARIANT", "reason": "NATIVE_INVARIANT"}
    assert calls == 1
    host = cast(SpiderFloatingExitHost, composition._floating_exit_host)
    assert host.batches == []
    assert endpoint.actions == actions_before
    assert codec.inspect_state() == inspection_before
    assert composition._dispatch_due(cutoff * 16 + 6)["classification"] == "TERMINAL"
    assert calls == 1
    assert endpoint.result(composition)["status"] == "FAILED"


def test_disabled_candidate_after_native_liquidation_does_not_read_cost_or_reopen(monkeypatch):
    run = _seeded_candidate_run(position=True)
    configuration = cast(dict[str, Any], decode_canonical(run.configuration_bytes))
    configuration["cash"] = "99.003"
    configuration_bytes = canonical_bytes(configuration)
    cache = cast(dict[str, Any], decode_canonical(run.initial_policy_cache))
    cache["running"] = False
    run = replace(
        run,
        configuration_bytes=configuration_bytes,
        configuration_sha256=sha256(configuration_bytes).hexdigest(),
        initial_account_state=_initial_account_state(cash="99.003", positions=configuration["positions"]),
        initial_policy_cache=canonical_bytes(cache),
    )
    trade_rows = []
    mark_rows = []
    for row in run.trade_bars:
        if row.product_id == PRODUCTS[0] and row.bar_open_ms == run.range_start_ms:
            row = replace(row, open=D("1"), high=D("1"), low=D("1"), close=D("1"))
        trade_rows.append(replace(row, source_row_hash=sha256(row.canonical_row(trade=True)).hexdigest()))
    for row in run.mark_bars:
        if row.product_id == PRODUCTS[0] and row.bar_open_ms == run.range_start_ms:
            row = replace(row, open=D("1"), high=D("1"), low=D("1"), close=D("1"))
        mark_rows.append(replace(row, source_row_hash=sha256(row.canonical_row(trade=False)).hexdigest()))
    trades, marks = tuple(trade_rows), tuple(mark_rows)
    run = replace(
        run,
        trade_bars=trades,
        mark_bars=marks,
        trade_manifest=_manifest(run.trade_manifest, trades, trade=True),
        mark_manifest=_manifest(run.mark_manifest, marks, trade=False),
    )
    composition = _ReplayComposition._from_floating_exit_run(_candidate(run))
    calls = 0

    def fail_cost_projection(self):
        nonlocal calls
        calls += 1
        raise AssertionError("native terminal must suppress candidate cost reads")

    monkeypatch.setattr(ScenarioCodec, "historical_cost_projection", fail_cost_projection)
    _advance(composition, run.range_end_ms * 16 + 7)
    endpoint = composition._paid_endpoint
    assert endpoint is not None
    result = endpoint.result(composition)
    assert result["owner_evidence"]["inspection"]["lifecycle"] == "LIQUIDATED_FLAT"
    assert composition._paid_native_terminal == "LIQUIDATED_FLAT"
    assert calls == 0
    host = cast(SpiderFloatingExitHost, composition._floating_exit_host)
    assert host.batches == []
    assert endpoint.product_operations == {}
