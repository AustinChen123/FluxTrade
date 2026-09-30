from copy import deepcopy
from dataclasses import replace
from decimal import Decimal as D
from typing import cast

import pytest

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.spider_historical_input import HistoricalInputError, HistoricalRunInput, validate_historical_input
from src.core.backtest.spider_historical_replay import _ClosedMarketSnapshot
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_historical_input import (
    _p2_configuration,
    _policy_cache,
    _rehashed_run,
    _run_with_configuration,
    _valid_run,
)


def _snapshot_event(composition: _ReplayComposition, index: int = 0) -> dict[str, object]:
    keys = sorted(key for key in composition._records if key[0] == 4)
    return composition._records[keys[index]]["item"]


def _node(run: HistoricalRunInput, open_ms: int, step: int) -> wire.HistoricalNode:
    trade = {(row.product_id, row.bar_open_ms): row for row in run.trade_bars}
    marks = {(row.product_id, row.bar_open_ms): row for row in run.mark_bars}
    bars = []
    for product in run.ordered_products:
        t, m = trade[(product, open_ms)], marks[(product, open_ms)]
        bars.append({
            "product_id": product,
            "trade": {"open": t.open, "high": t.high, "low": t.low, "close": t.close,
                      "volume_contracts": cast(D, t.volume), "confirmed": True,
                      "source_row_hash": t.source_row_hash},
            "mark": {"open": m.open, "high": m.high, "low": m.low, "close": m.close,
                     "confirmed": True, "source_row_hash": m.source_row_hash},
        })
    return {
        "schema_version": "historical_node_v1",
        "model_id": run.model_id,
        "model_version": "1",
        "run_contract_hash": validate_historical_input(run),
        "bar_open_ms": open_ms,
        "bar_duration_ms": 60_000,
        "step_index": step,
        "market_slippage_bps": run.market_slippage_bps,
        "bars": bars,
        "working_orders": [],
    }


def _composition(run: HistoricalRunInput) -> _ReplayComposition:
    return _ReplayComposition._from_historical_run(run)


def _extend_one_minute(run: HistoricalRunInput) -> HistoricalRunInput:
    next_open = run.range_end_ms
    trade, marks = [], []
    for product in run.ordered_products:
        for source, target in ((run.trade_bars, trade), (run.mark_bars, marks)):
            prior = next(row for row in reversed(source) if row.product_id == product)
            target.append(replace(prior, bar_open_ms=next_open, source_sequence=prior.source_sequence + 1))
    run = _rehashed_run(run, trade_rows=(*run.trade_bars, *trade), mark_rows=(*run.mark_bars, *marks))
    return replace(
        run,
        range_end_ms=next_open + 60_000,
        trade_manifest=replace(run.trade_manifest, source_row_count=len(run.trade_bars),
                               normalized_row_count=len(run.trade_bars), last_source_timestamp_ms=next_open),
        mark_manifest=replace(run.mark_manifest, source_row_count=len(run.mark_bars),
                              normalized_row_count=len(run.mark_bars), last_source_timestamp_ms=next_open),
        spec_after=tuple(replace(spec, effective_at_ms=next_open + 60_000) for spec in run.spec_after),
    )


def test_historical_bootstrap_restores_policy_cache_and_keeps_detached_order_position_evidence():
    run = _valid_run()
    cache = _policy_cache(
        running=False,
        paused=True,
        online=False,
        ws_open=True,
        order_id=9,
        capital={"total": "1200", "usdt": "1100", "avail": "900", "earn": "100", "position": "25"},
        rows=[
            {"product_id": "A-USDT-SWAP", "name": "A-USDT-SWAP", "active": "false", "leverage": "3",
             "歩差": "0.02", "單數": "4", "hold上限": "0.8", "hold下限": "-0.8", "hold": "0.25"},
            {"product_id": "B-USDT-SWAP", "name": "B-USDT-SWAP", "active": "true", "leverage": "2",
             "歩差": "0.03", "單數": "5", "hold上限": "0.9", "hold下限": "-0.9", "hold": "-0.5"},
        ],
        markets=[
            {"product_id": "A-USDT-SWAP", "price": "77", "ctVal": "1", "lotSz": "1", "minSz": "1",
             "increment": "1", "ratioHL": "0.2", "state": "live", "instIdCode": 1},
            {"product_id": "B-USDT-SWAP", "price": "88", "ctVal": "1", "lotSz": "1", "minSz": "1",
             "increment": "1", "ratioHL": "0.3", "state": "live", "instIdCode": 2},
        ],
        replies={"A-USDT-SWAP": {"reply": "kept"}},
        orders={"A-USDT-SWAP": {"order_id": "evidence-only"}},
        positions={"B-USDT-SWAP": {"quantity": "evidence-only"}},
        last_filled_price={"A-USDT-SWAP": {"buy": "1.25", "sell": "0"}},
    )
    run = replace(run, initial_policy_cache=cache)
    composition = _composition(run)
    policy = composition._policy
    assert (policy.running, policy.paused, policy.online, policy.ws_open, policy.order_id) == (False, True, False, True, 9)
    assert [row["name"] for row in policy.rows] == list(run.ordered_products)
    assert policy.rows[0]["active"] == "false" and policy.rows[1]["hold"] == "-0.5"
    assert list(policy.markets) == list(run.ordered_products)
    assert policy.markets["A-USDT-SWAP"]["price"] == "77"
    assert policy.capital["total"] == "1200" and policy.replies == {"A-USDT-SWAP": {"reply": "kept"}}
    assert policy.last_filled_price == {"A-USDT-SWAP": {"buy": "1.25", "sell": "0"}}
    assert policy.parameters["defaultN"] == D("1")
    assert composition._historical_initial_cache_evidence == {
        "orders": {"A-USDT-SWAP": {"order_id": "evidence-only"}},
        "positions": {"B-USDT-SWAP": {"quantity": "evidence-only"}},
    }
    assert composition._historical_initial_cache_evidence["orders"] is not cast(dict, decode_cache(run))["orders"]


def decode_cache(run: HistoricalRunInput) -> dict[str, object]:
    from src.core.backtest.spider_run_artifacts import decode_canonical
    return cast(dict[str, object], decode_canonical(run.initial_policy_cache))


def test_historical_account_identity_uses_frozen_research_namespace_only():
    first = _composition(_valid_run())
    second = _composition(replace(_valid_run(), account_key="another-account"))
    expected = {"venue": "SPIDER_HISTORICAL_RESEARCH", "environment": "RESEARCH_ONLY", "account": "account"}
    assert first._codec.inspect_state()["account_key"] == first._account == expected
    assert second._codec.inspect_state()["account_key"] == second._account == {**expected, "account": "another-account"}
    assert first._codec.inspect_state()["account_key"] != second._codec.inspect_state()["account_key"]


def test_invalid_historical_run_constructs_neither_composition_nor_native_owner(monkeypatch):
    composition_calls = []
    owner_calls = []
    original_init = _ReplayComposition.__init__
    original_native = wire._native._SyntheticScenarioReplaySession

    def count_composition(self, *args, **kwargs):
        composition_calls.append(True)
        original_init(self, *args, **kwargs)

    def count_owner(*args, **kwargs):
        owner_calls.append(True)
        return original_native(*args, **kwargs)

    monkeypatch.setattr(_ReplayComposition, "__init__", count_composition)
    monkeypatch.setattr(wire._native, "_SyntheticScenarioReplaySession", count_owner)
    with pytest.raises(HistoricalInputError):
        _composition(replace(_valid_run(), policy_id=""))
    assert composition_calls == [] and owner_calls == []


def _assert_rejected_before_owner(run: HistoricalRunInput, monkeypatch) -> None:
    composition_calls = []
    owner_calls = []
    original_init = _ReplayComposition.__init__
    original_native = wire._native._SyntheticScenarioReplaySession

    def count_composition(self, *args, **kwargs):
        composition_calls.append(True)
        original_init(self, *args, **kwargs)

    def count_owner(*args, **kwargs):
        owner_calls.append(True)
        return original_native(*args, **kwargs)

    monkeypatch.setattr(_ReplayComposition, "__init__", count_composition)
    monkeypatch.setattr(wire._native, "_SyntheticScenarioReplaySession", count_owner)
    with pytest.raises(HistoricalInputError):
        _composition(run)
    assert composition_calls == [] and owner_calls == []


@pytest.mark.parametrize("case", [
    "no_active", "multiple_active", "ends_before_last", "ends_at_last",
    "second_spec_overlaps_during_run",
    "contract_value", "multiplier", "price_tick", "quantity_step", "minimum_quantity",
])
def test_configured_spec_semantics_reject_before_composition_or_native_owner(case, monkeypatch):
    run = _valid_run()
    if case in {"ends_before_last", "ends_at_last"}:
        run = _extend_one_minute(run)
    configuration = _p2_configuration()
    first = configuration["products"][0]["specs"][0]
    start, end = run.range_start_ms, run.range_end_ms
    last_open = end - run.bar_duration_ms
    evidence_product = 1 if case in {"multiplier", "quantity_step"} else 0
    if case == "no_active":
        first["valid_from"] = start + 1
    elif case == "multiple_active":
        configuration["products"][0]["specs"].append({**first, "version": "spec-v2"})
    elif case == "ends_before_last":
        first["valid_to"] = last_open - 1
    elif case == "ends_at_last":
        first["valid_to"] = last_open
    elif case == "second_spec_overlaps_during_run":
        run = _extend_one_minute(run)
        start, end = run.range_start_ms, run.range_end_ms
        last_open = end - run.bar_duration_ms
        configuration["products"][0]["specs"].append({
            **first, "version": "spec-v2", "valid_from": start + run.bar_duration_ms,
        })
    else:
        product = configuration["products"][evidence_product]
        product["specs"][0][case] = "2"
    run = _run_with_configuration(run, configuration)
    _assert_rejected_before_owner(run, monkeypatch)


def test_configured_spec_exact_run_end_boundary_is_valid_and_drives_cache(monkeypatch):
    run = _valid_run()
    configuration = _p2_configuration()
    for product in configuration["products"]:
        first = product["specs"][0]
        first["valid_to"] = run.range_end_ms
        product["specs"].append({**first, "version": "spec-v2", "valid_from": run.range_end_ms,
                                 "valid_to": None})
        product["marks"][0]["valid_to"] = 200_000_000
    run = _run_with_configuration(run, configuration)
    owner_calls = []
    original_native = wire._native._SyntheticScenarioReplaySession

    def count_owner(*args, **kwargs):
        owner_calls.append(True)
        return original_native(*args, **kwargs)

    monkeypatch.setattr(wire._native, "_SyntheticScenarioReplaySession", count_owner)
    composition = _composition(run)
    assert len(owner_calls) == 1
    close_ms = run.range_start_ms + run.bar_duration_ms
    assert composition._dispatch_due(close_ms * 16 + 6)["classification"] == "SUCCESS"
    for market in composition._policy.markets.values():
        assert (market["ctVal"], market["lotSz"], market["minSz"], market["increment"]) == (
            D("1"), D("1"), D("1"), D("1"))


def test_market_cache_uses_exact_1440_closed_bars_and_ignores_future_until_its_close():
    base = _extend_one_minute(_valid_run())
    outside_open = base.warmup_start_ms
    included_open = base.range_start_ms - 86_340_000
    altered = []
    for row in base.trade_bars:
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == outside_open:
            altered.append(replace(row, high=D("1000"), low=D("1")))
        elif row.product_id == "A-USDT-SWAP" and row.bar_open_ms == included_open:
            altered.append(replace(row, high=D("200"), low=D("100")))
        else:
            altered.append(row)
    common = _rehashed_run(base, trade_rows=tuple(altered))
    future_rows = list(common.trade_bars)
    future_index = next(i for i, row in enumerate(future_rows)
                        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == common.range_start_ms + 60_000)
    future_rows[future_index] = replace(future_rows[future_index], high=D("300"), low=D("90"), close=D("150"))
    different_future = _rehashed_run(common, trade_rows=tuple(future_rows))
    left, right = _composition(common), _composition(different_future)
    close_ms = common.range_start_ms + 60_000
    due = close_ms * 16 + 6
    assert left._policy.markets["A-USDT-SWAP"]["price"] == "100"
    assert left._dispatch_due(due - 1)["classification"] == "SUCCESS"
    assert left._policy.markets["A-USDT-SWAP"]["price"] == "100"
    assert left._dispatch_due(due)["classification"] == "SUCCESS"
    assert right._dispatch_due(due)["classification"] == "SUCCESS"
    assert left._policy.markets == right._policy.markets
    assert left._policy.events == right._policy.events == []
    snapshot_left = left._records[(4, f"MARKET_CLOSE_{close_ms}")]["result"]["market_snapshot"]
    snapshot_right = right._records[(4, f"MARKET_CLOSE_{close_ms}")]["result"]["market_snapshot"]
    assert snapshot_left == snapshot_right
    rows = snapshot_left["markets"]
    assert [row["product_id"] for row in rows] == ["A-USDT-SWAP", "B-USDT-SWAP"]
    assert [(row["ctVal"], row["lotSz"], row["minSz"], row["increment"]) for row in rows] == [
        (D("1"), D("1"), D("1"), D("1")), (D("1"), D("1"), D("1"), D("1"))]
    assert rows[0]["ratioHL"] == D("100")
    assert rows[1]["ratioHL"] == D("0")
    assert rows[0]["price"] == D("100")
    later = common.range_end_ms * 16 + 6
    assert left._dispatch_due(later)["classification"] == "SUCCESS"
    assert right._dispatch_due(later)["classification"] == "SUCCESS"
    assert left._policy.markets["A-USDT-SWAP"]["price"] == D("100")
    assert right._policy.markets["A-USDT-SWAP"]["price"] == D("150")
    assert right._policy.markets["A-USDT-SWAP"]["ratioHL"] > left._policy.markets["A-USDT-SWAP"]["ratioHL"]


def test_market_cache_duplicates_conflicts_and_out_of_order_are_terminal_before_policy_mutation():
    composition = _composition(_valid_run())
    item = _snapshot_event(composition)
    before = deepcopy(composition._policy.markets)
    assert composition._enqueue(item)["classification"] == "PENDING"
    assert composition._enqueue(deepcopy(item))["classification"] == "PENDING"
    changed = deepcopy(item)
    snapshot = cast(_ClosedMarketSnapshot, changed["snapshot"])
    changed["snapshot"] = replace(snapshot, markets=(replace(snapshot.markets[0], price=D("99")), *snapshot.markets[1:]))
    conflict = composition._enqueue(changed)
    assert conflict["classification"] == "TERMINAL" and conflict["reason"] == "INVALID_SCHEMA"
    assert composition._policy.markets == before


def test_step3_cache_and_next_open_dispatch_in_frozen_phase_order(monkeypatch):
    run = _extend_one_minute(_valid_run())
    config = _p2_configuration()
    for product in config["products"]:
        product["marks"][0]["valid_to"] = 200_000_000
    run = _run_with_configuration(run, config)
    composition = _composition(run)
    calls = []
    original_step = wire.ScenarioCodec.historical_market_step
    original_market = composition._policy.apply_market

    def tracked_step(codec, node):
        calls.append(("STEP", node["step_index"]))
        return original_step(codec, node)

    def tracked_market(markets):
        calls.append(("CACHE", tuple(markets)))
        original_market(markets)

    monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", tracked_step)
    monkeypatch.setattr(composition._policy, "apply_market", tracked_market)
    close_ms = run.range_start_ms + 60_000
    first_step = {"kind": "HISTORICAL_MARKET_STEP", "schedule_sequence": 0, "stable_id": "CLOSE", "node": _node(run, run.range_start_ms, 3)}
    next_open = {"kind": "HISTORICAL_MARKET_STEP", "schedule_sequence": 1, "stable_id": "OPEN", "node": _node(run, close_ms, 0)}
    assert composition._enqueue(first_step)["classification"] == "PENDING"
    assert composition._enqueue(next_open)["classification"] == "PENDING"
    assert composition._dispatch_due(close_ms * 16 + 7)["classification"] == "SUCCESS"
    assert [entry[0] for entry in calls] == ["STEP", "CACHE", "STEP"]
    assert calls[0] == ("STEP", 3) and calls[2] == ("STEP", 0)
    assert composition._policy.events == []
    assert composition._records[(4, f"MARKET_CLOSE_{close_ms}")]["result"]["market_snapshot"]["close_ms"] == close_ms
