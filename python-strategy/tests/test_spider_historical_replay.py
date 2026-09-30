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


def _assert_rejected_before_owner(run: HistoricalRunInput, monkeypatch, *, message: str | None = None) -> None:
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
    with pytest.raises(HistoricalInputError, match=message):
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
    for product in configuration["products"]:
        product["marks"][0]["valid_to"] = end
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
    message = ("configured product spec differs from run evidence"
               if case in {"contract_value", "multiplier", "price_tick", "quantity_step", "minimum_quantity"}
               else None)
    _assert_rejected_before_owner(run, monkeypatch, message=message)


def test_configured_spec_coherent_baseline_is_accepted():
    composition = _composition(_valid_run())
    assert composition._policy.markets["A-USDT-SWAP"]["increment"] == "0.01"
    assert composition._policy.markets["A-USDT-SWAP"]["lotSz"] == "0.001"


def test_configured_spec_exact_run_end_boundary_is_valid_and_uses_run_cache(monkeypatch):
    run = _valid_run()
    configuration = _p2_configuration()
    for product in configuration["products"]:
        first = product["specs"][0]
        first["valid_to"] = run.range_end_ms
        product["specs"].append({**first, "version": "spec-v2", "valid_from": run.range_end_ms,
                                 "valid_to": None})
        product["marks"][0]["valid_to"] = 200_000_000
    run = _run_with_configuration(run, configuration)
    # Historical timers now perform real Policy polling; keep this cache test
    # isolated from intentional grid-order acceptance.
    run = replace(run, initial_policy_cache=_policy_cache(running=False))
    owner_calls = []
    original_native = wire._native._SyntheticScenarioReplaySession

    def count_owner(*args, **kwargs):
        owner_calls.append(True)
        return original_native(*args, **kwargs)

    monkeypatch.setattr(wire._native, "_SyntheticScenarioReplaySession", count_owner)
    composition = _composition(run)
    assert len(owner_calls) == 1
    assert composition._dispatch_due(run.range_end_ms * 16 + 6)["classification"] == "SUCCESS"
    final_close = composition._records[(3, f"P3_MARKET_{run.range_start_ms}_3")]
    assert final_close["result"]["classification"] == "SUCCESS"
    for market in composition._policy.markets.values():
        assert (market["ctVal"], market["lotSz"], market["minSz"], market["increment"]) == (
            D("1"), D("0.001"), D("0.001"), D("0.01"))


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
    for composition in (left, right):
        for row in composition._policy.rows:
            row["active"] = "false"
    close_ms = common.range_start_ms + 60_000
    due = close_ms * 16 + 6
    assert left._policy.markets["A-USDT-SWAP"]["price"] == "100"
    assert left._dispatch_due(due - 1)["classification"] == "SUCCESS"
    assert left._policy.markets["A-USDT-SWAP"]["price"] == "100"
    assert left._dispatch_due(due)["classification"] == "SUCCESS"
    assert right._dispatch_due(due)["classification"] == "SUCCESS"
    assert left._policy.markets == right._policy.markets
    assert left._policy.events == right._policy.events
    assert all(common.range_start_ms < event["at_ms"] < close_ms
               and (event["at_ms"] - common.range_start_ms) % 5_000 == 0
               for event in left._policy.events)
    snapshot_left = left._records[(4, f"MARKET_CLOSE_{close_ms}")]["result"]["market_snapshot"]
    snapshot_right = right._records[(4, f"MARKET_CLOSE_{close_ms}")]["result"]["market_snapshot"]
    assert snapshot_left == snapshot_right
    rows = snapshot_left["markets"]
    assert [row["product_id"] for row in rows] == ["A-USDT-SWAP", "B-USDT-SWAP"]
    assert [(row["ctVal"], row["lotSz"], row["minSz"], row["increment"]) for row in rows] == [
        (D("1"), D("0.001"), D("0.001"), D("0.01")),
        (D("1"), D("0.001"), D("0.001"), D("0.01"))]
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
    run = replace(run, initial_policy_cache=_policy_cache(running=False))
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
    assert composition._dispatch_due(close_ms * 16 + 7)["classification"] == "SUCCESS"
    assert [entry[0] for entry in calls] == ["STEP", "STEP", "STEP", "STEP", "CACHE", "STEP"]
    assert [entry[1] for entry in calls if entry[0] == "STEP"] == [0, 1, 2, 3, 0]
    assert composition._policy.events
    assert all(run.range_start_ms < event["at_ms"] < close_ms
               and (event["at_ms"] - run.range_start_ms) % 5_000 == 0
               for event in composition._policy.events)
    assert composition._records[(4, f"MARKET_CLOSE_{close_ms}")]["result"]["market_snapshot"]["close_ms"] == close_ms


def test_historical_poll_timers_use_raw_policy_and_effective_scheduler_clocks():
    run = _valid_run()
    composition = _composition(run)
    policy = composition._policy
    assert (policy.now_ms, policy.shared_ms, policy.last_earn_ms, policy.last_market_ms,
            policy.reset_ms, policy.last_capital) == (
        run.range_start_ms, run.range_start_ms - 12_000, run.range_start_ms - 61_000, 0, {}, None)
    timer_records = [record for record in composition._records.values()
                     if record["item"]["kind"] == "HISTORICAL_TIMER"]
    assert [row["raw_time_ms"] for row in timer_records] == list(
        range(run.range_start_ms + 5_000, run.range_end_ms, 5_000))
    first_raw = run.range_start_ms + 5_000
    assert composition._dispatch_due(first_raw * 16 + 9)["classification"] == "SUCCESS"
    first = composition._records[(5, f"P3_TIMER_{first_raw}")]["result"]
    assert first["events"] == ({"at_ms": first_raw, "kind": "request_earn"},)
    assert policy.now_ms == first_raw
    assert composition._current_time == first_raw * 16 + 9
    assert len(composition._continuations) == 1

    earn = composition._records[(1, f"P3_POLL_{first_raw}_EARN")]["item"]
    request = earn["request"]
    assert request["capture_mode"] == "FROZEN_POLL_FIXTURE"
    assert request["fixture_key"] == "P3_POLL_EARN_ZERO"
    assert request["captured_at"] == (first_raw + 1) * 16 + 5
    assert earn["delivery_projection"]["visible_at"] == (first_raw + 5) * 16 + 6
    assert composition._records[(1, request["snapshot_id"])]["key"][0] == request["captured_at"]
    first_poll = composition._polls[f"P3_POLL_{first_raw}"]
    assert [(stage["snapshot_request"]["captured_at"], stage["delivery_projection"]["visible_at"])
            for stage in first_poll.stages] == [
        ((first_raw + 1) * 16 + 5, (first_raw + 5) * 16 + 6),
        ((first_raw + 6) * 16 + 5, (first_raw + 10) * 16 + 6),
        ((first_raw + 11) * 16 + 5, (first_raw + 15) * 16 + 6),
        ((first_raw + 16) * 16 + 5, (first_raw + 20) * 16 + 6),
    ]
    assert composition._dispatch_due(request["captured_at"])["classification"] == "SUCCESS"
    earn_delivery = next(record["item"]["delivery"] for record in composition._records.values()
                         if record["item"]["kind"] == "DELIVERY"
                         and record["item"]["delivery"]["continuation_id"] == f"P3_CONT_{first_raw}"
                         and record["item"]["delivery"]["payload_kind"] == "EARN_SNAPSHOT")
    assert earn_delivery["immutable_payload"] == {"outcome": "SUCCESS", "earn": D(0)}
    assert earn_delivery["snapshot_as_of"] == request["captured_at"]
    delivery_key = next(record["key"][0] for record in composition._records.values()
                        if record["item"]["kind"] == "DELIVERY"
                        and record["item"]["delivery"]["delivery_id"] == earn_delivery["delivery_id"])
    assert delivery_key == earn["delivery_projection"]["visible_at"]

    second_raw = first_raw + 5_000
    assert composition._dispatch_due(second_raw * 16 + 9)["classification"] == "SUCCESS"
    second_poll = composition._polls[f"P3_POLL_{second_raw}"]
    assert [stage["snapshot_request"]["snapshot_kind"] for stage in second_poll.stages] == [
        "TRADING", "POSITIONS", "OPEN_ORDERS"]
    assert [(stage["snapshot_request"]["captured_at"], stage["delivery_projection"]["visible_at"])
            for stage in second_poll.stages] == [
        ((second_raw + 1) * 16 + 5, (second_raw + 5) * 16 + 6),
        ((second_raw + 6) * 16 + 5, (second_raw + 10) * 16 + 6),
        ((second_raw + 11) * 16 + 5, (second_raw + 15) * 16 + 6),
    ]
    assert [stage["snapshot_request"]["capture_mode"] for stage in second_poll.stages] == [
        "OWNER_CURRENT", "OWNER_CURRENT", "OWNER_CURRENT"]
    assert policy.now_ms == second_raw


def test_historical_timer_phase_follows_same_time_market_cache_and_excludes_endpoint():
    run = _extend_one_minute(_valid_run())
    composition = _composition(run)
    same_time = run.range_start_ms + 60_000
    assert composition._dispatch_due(same_time * 16 + 9)["classification"] == "SUCCESS"
    assert composition._records[(4, f"MARKET_CLOSE_{same_time}")]["result"]["classification"] == "SUCCESS"
    timer = composition._records[(5, f"P3_TIMER_{same_time}")]
    assert timer["result"]["classification"] == "SUCCESS"
    assert composition._last_popped == (same_time * 16 + 9, 5, 11, f"P3_TIMER_{same_time}")
    assert len(composition._continuations) == 12

    endpoint = _composition(_valid_run())
    end = run.range_start_ms + 60_000
    assert all(record["item"]["raw_at"] != end for record in endpoint._records.values()
               if record["item"]["kind"] == "HISTORICAL_TIMER")


def test_historical_poll_sends_become_native_groups_only_after_acceptance_tick():
    run = _extend_one_minute(_valid_run())
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    parent_raw = run.range_start_ms + 5_020
    assert composition._dispatch_due(parent_raw * 16 + 6)["classification"] == "SUCCESS"
    actions = [action for events in composition._audit.values() for event in events
               for action in event.get("actions", [])]
    assert actions and all(action["status"] == "UNSUBMITTED" for action in actions)
    groups = [record for record in composition._records.values()
              if record["item"]["kind"] == "SOURCE_GROUP"
              and record["item"]["group"]["ordering_contract_id"] == "HISTORICAL_ORDER_V1"]
    assert len(groups) == 4
    assert len({record["key"][0] for record in groups}) == 1
    accepted_at = (parent_raw + run.order_accept_delay_ms) * 16 + 3
    assert {record["key"][0] for record in groups} == {accepted_at}
    assert [record["key"][2] for record in groups] == list(range(4))
    assert [record["item"]["group"]["members"][0]["payload"]["client_order_id"]
            for record in groups] == [
        order["clOrdId"] for event in composition._policy.events
        if event["kind"] == "send" for order in event["orders"]
    ]
    assert all(record["result"]["classification"] == "PENDING" for record in groups)
    assert all(action["status"] == "UNSUBMITTED" for action in actions)
    orders_before_accept = composition._codec.inspect_state()["orders_digest"]
    assert composition._dispatch_due(accepted_at - 1)["classification"] == "SUCCESS"
    assert composition._codec.inspect_state()["orders_digest"] == orders_before_accept
    assert composition._dispatch_due(accepted_at)["classification"] == "SUCCESS"
    assert all(record["result"]["group_result"]["classification"] == "COMMITTED" for record in groups)
    assert all(action["status"] == "SUBMITTED" for action in actions if action["group_id"] is not None)
    assert composition._codec.inspect_state()["orders_digest"] != orders_before_accept
    assert all(record["key"][3] in {action["group_id"] for action in actions} for record in groups)


def test_historical_chain_uses_frozen_owner_orders_and_commits_one_partial_fill():
    run = _valid_run()
    trade_rows = tuple(
        replace(row, high=D("103"), low=D("98"), volume=D("0.1"))
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == run.range_start_ms else row
        for row in run.trade_bars
    )
    run = _rehashed_run(run, trade_rows=trade_rows)
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    start = run.range_start_ms

    steps = [record for record in composition._records.values()
             if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"]
    assert len(steps) == 1
    first_key = start * 16 + 7
    assert steps[0]["key"][0] == first_key
    assert composition._dispatch_due(first_key)["classification"] == "SUCCESS"
    first_result = steps[0]["result"]["historical_result"]
    assert all(not product["fills"] for product in first_result["products"])
    assert steps[0]["result"]["working_orders_snapshot"] == []

    parent_visible = (start + 5_020) * 16 + 6
    assert composition._dispatch_due(parent_visible)["classification"] == "SUCCESS"
    historical_groups = [record for record in composition._records.values()
                         if record["item"]["kind"] == "SOURCE_GROUP"
                         and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"]
    assert len(historical_groups) == 4
    assert all(record["result"]["classification"] == "PENDING" for record in historical_groups)
    assert composition._dispatch_due(historical_groups[0]["key"][0]) ["classification"] == "SUCCESS"
    assert all(record["result"]["group_result"]["classification"] == "COMMITTED"
               for record in historical_groups)

    step1_key = (start + 20_000) * 16 + 8
    step1 = next(record for record in composition._records.values()
                 if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                 and record["item"]["node"]["step_index"] == 1)
    assert step1["key"][0] == step1_key
    assert step1["item"]["node"]["working_orders"] == []
    orders_before_step1 = composition._codec.inspect_state()["orders_digest"]
    step1_dispatch = composition._dispatch_due(step1_key)
    assert step1_dispatch["classification"] == "SUCCESS", step1_dispatch
    step1_result = step1["result"]["historical_result"]
    assert all(not product["fills"] for product in step1_result["products"])
    assert step1["result"]["working_orders_snapshot"] == []
    assert step1_result["owner_evidence"]["orders_digest"] == orders_before_step1

    step2_key = (start + 40_000) * 16 + 8
    step2 = next(record for record in composition._records.values()
                 if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                 and record["item"]["node"]["step_index"] == 2)
    frozen = step2["item"]["node"]["working_orders"]
    assert len(frozen) == 4
    assert [row["accepted_source_sequence"] for row in frozen] == [0, 1, 2, 3]
    assert composition._dispatch_due(step2_key)["classification"] == "SUCCESS"
    step2_result = step2["result"]["historical_result"]
    fills = [fill for product in step2_result["products"] for fill in product["fills"]]
    assert len(fills) == 1
    fill = fills[0]
    assert fill["quantity_contracts"] == D("0.025")
    selected = next(row for row in frozen if row["order_id"] == fill["order_id"])
    assert selected["side"] == "SHORT" and selected["limit_price"] == D("101")
    assert fill["price"] == D("103")
    assert len(fill["execution_id"]) == 64
    assert fill["source_event_id"]
    assert fill["occurrence_index"] == 0
    notice = next(record for record in composition._records.values()
                  if record["item"]["kind"] == "DELIVERY"
                  and record["item"]["delivery"]["source_fact_id"] == fill["source_event_id"])
    assert notice["key"][0] == (start + 40_002) * 16 + 6
    assert notice["item"]["delivery"]["immutable_payload"]["state"] == "partially_filled"
    owner_before_notice = composition._codec.inspect_state()
    assert composition._dispatch_due(notice["key"][0])["classification"] == "SUCCESS"
    notice_events = composition._audit[notice["item"]["delivery"]["delivery_id"]]
    assert not any(event["event"]["kind"] in ("send", "cancel") for event in notice_events)
    owner_after_notice = composition._codec.inspect_state()
    for field in ("account_version", "cash", "total_fees", "positions_digest", "orders_digest"):
        assert owner_after_notice[field] == owner_before_notice[field]
    policy_events_after_notice = deepcopy(composition._policy.events)
    delivered_notice = notice["item"]["delivery"]
    assert composition._codec.build_delivery(dict(
        schema_version="delivery_projection_v1",
        reference=dict(namespace=delivered_notice["source_namespace"], fact_id=delivered_notice["source_fact_id"]),
        payload_kind=delivered_notice["payload_kind"],
        occurrence_index=delivered_notice["occurrence_index"],
        schedule_sequence=delivered_notice["schedule_sequence"],
        visible_at=delivered_notice["visible_at"],
    )) == delivered_notice
    duplicate = composition._enqueue(dict(kind="DELIVERY", delivery=delivered_notice))
    assert duplicate["classification"] == "SUCCESS"
    assert composition._policy.events == policy_events_after_notice
    owner_after = step2_result["owner_evidence"]
    assert owner_after["account_version"] > first_result["owner_evidence"]["account_version"]
    assert owner_after["orders_digest"] != first_result["owner_evidence"]["orders_digest"]
    assert owner_after["total_fees"] > first_result["owner_evidence"]["total_fees"]
    assert owner_after["cash"] < first_result["owner_evidence"]["cash"]
    assert owner_after["positions_digest"] != first_result["owner_evidence"]["positions_digest"]

    step3 = next(record for record in composition._records.values()
                 if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                 and record["item"]["node"]["step_index"] == 3)
    partial = next(row for row in step3["item"]["node"]["working_orders"]
                   if row["order_id"] == fill["order_id"])
    assert partial["order_version"] == 2
    assert partial["status"] == "PARTIALLY_FILLED"
    assert partial["remaining_quantity_contracts"] == frozen[
        next(index for index, row in enumerate(frozen) if row["order_id"] == fill["order_id"])
    ]["remaining_quantity_contracts"] - fill["quantity_contracts"]
    positions = composition._codec.capture_snapshot(dict(
        schema_version="snapshot_request_v1", account_key=composition._account,
        snapshot_id="P3C5B_POSITIONS_AFTER_FILL", snapshot_kind="POSITIONS",
        capture_mode="OWNER_CURRENT", captured_at=step2_key,
    ))["immutable_payload"]["rows"]
    a_position = next(row for row in positions if row["product_id"] == "A-USDT-SWAP")
    assert a_position["position_contracts"] == -fill["quantity_contracts"]
    step3_key = run.range_end_ms * 16 + 4
    assert composition._dispatch_due(step3_key)["classification"] == "SUCCESS"
    assert step3["result"]["classification"] == "SUCCESS"


def test_historical_full_execution_notice_derives_native_cancel_and_replacements():
    run = _valid_run()
    trade_rows = tuple(
        replace(row, open=D("101"), high=D("101.5"), low=D("100.5"), close=D("101"), volume=D("0.4"))
        if row.product_id == "A-USDT-SWAP" else row
        for row in run.trade_bars
    )
    run = _rehashed_run(run, trade_rows=trade_rows)
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    start = run.range_start_ms
    assert composition._dispatch_due((start + 40_002) * 16 + 6)["classification"] == "SUCCESS"

    full_notices = [record for record in composition._records.values()
                    if record["item"]["kind"] == "DELIVERY"
                    and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
                    and record["item"]["delivery"]["immutable_payload"]["state"] == "filled"
                    and record["result"]["classification"] == "SUCCESS"]
    assert full_notices
    notice = next(record for record in full_notices
                  if record["item"]["delivery"]["immutable_payload"]["limit_price"]
                  != record["item"]["delivery"]["immutable_payload"]["fill_price"])
    fact = notice["item"]["delivery"]["immutable_payload"]
    events = composition._audit[notice["item"]["delivery"]["delivery_id"]]
    sends = [event for event in events if event["event"]["kind"] == "send"]
    cancels = [event for event in events if event["event"]["kind"] == "cancel"]
    assert sends and cancels
    assert sum(bool(composition._audit[record["item"]["delivery"]["delivery_id"]])
               for record in full_notices) == 1
    replacement_prices = [order["px"] for event in sends for order in event["event"]["orders"]]
    assert fact["limit_price"] == D("101") and fact["fill_price"] == D("101.5")
    assert "100" in replacement_prices
    assert str(fact["fill_price"]) not in replacement_prices
    assert all(action["status"] == "UNSUBMITTED" and action["group_id"] is not None
               for event in (*sends, *cancels) for action in event["actions"])

    initial_groups = [record for record in composition._records.values()
                      if record["item"]["kind"] == "SOURCE_GROUP"
                      and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"]
    child_groups = [record for record in initial_groups
                    if record["item"]["group"]["members"][0]["stamp"]["source_sequence"] >= 4]
    child_groups.sort(key=lambda record: record["item"]["group"]["members"][0]["stamp"]["source_sequence"])
    assert [record["item"]["group"]["members"][0]["kind"] for record in child_groups] == [
        "CANCEL_REQUEST", "INTENT", "INTENT",
    ]
    assert [record["item"]["group"]["members"][0]["stamp"]["source_sequence"]
            for record in child_groups] == list(range(4, 7))
    assert all(record["key"][0] == (start + 40_003) * 16 + 3 for record in child_groups)
    assert composition._dispatch_due((start + 40_003) * 16 + 3)["classification"] == "SUCCESS"
    assert all(record["result"]["group_result"]["classification"] == "COMMITTED"
               for record in child_groups)
    assert all(action["status"] == "SUBMITTED"
               and action["group_result"]["classification"] == "COMMITTED"
               for event in (*sends, *cancels) for action in event["actions"])

    requests = [record for record in child_groups
                if record["item"]["group"]["members"][0]["kind"] == "CANCEL_REQUEST"]
    effects = [record for record in composition._records.values()
               if record["item"]["kind"] == "SOURCE_GROUP"
               and record["item"]["group"]["members"][0]["kind"] == "CANCEL_EFFECT"]
    assert len(requests) == len(effects) == 1
    for request in requests:
        request_member = request["item"]["group"]["members"][0]
        request_event = request_member["stamp"]["event_id"]
        target = request_member["payload"]["targets"][0]["target_order_id"]
        matching_effect = next(record for record in effects
                               if record["item"]["group"]["members"][0]["payload"]["effects"][0]["detecting_event_id"]
                               == request_event)
        effect_member = matching_effect["item"]["group"]["members"][0]
        assert matching_effect["key"][0] == (start + 45_003) * 16 + 2
        assert effect_member["stamp"]["causal_parent_ids"] == [request_event]
        assert effect_member["payload"]["effects"] == [dict(
            detecting_event_id=request_event, target_order_id=target, reason="EXPLICIT_SCENARIO")
        ]
        assert target in {order["order_id"] for order in composition._codec.historical_working_orders()}
    assert composition._dispatch_due((start + 45_003) * 16 + 2)["classification"] == "SUCCESS"
    ack_visible = (start + 45_005) * 16 + 6
    ack_records = [record for record in composition._records.values()
                   if record["item"]["kind"] == "DELIVERY"
                   and record["item"]["delivery"]["payload_kind"] == "TRANSPORT_ACK"]
    assert len(ack_records) == 1
    ack_record = ack_records[0]
    ack_delivery = ack_record["item"]["delivery"]
    effect_event = effects[0]["item"]["group"]["members"][0]["stamp"]["event_id"]
    assert ack_record["key"][0] == ack_delivery["visible_at"] == ack_visible
    assert ack_delivery["source_fact_id"] == effect_event
    ack_payload = ack_delivery["immutable_payload"]
    assert set(ack_payload) == {"route", "operation", "client_order_id", "order_id", "code"}
    assert (ack_payload["route"], ack_payload["operation"], ack_payload["order_id"], ack_payload["code"]) == (
        "WS", "CANCEL", target, "0",
    )
    assert isinstance(ack_payload["client_order_id"], str) and ack_payload["client_order_id"]
    state_after_effect = deepcopy(composition._codec.inspect_state())
    policy_events_before_ack = deepcopy(composition._policy.events)
    ack_calls = []
    capture_callback = composition._capture_callback

    def count_ack_callback(delivery):
        if delivery["delivery_id"] == ack_delivery["delivery_id"]:
            ack_calls.append(delivery["delivery_id"])
        return capture_callback(delivery)

    composition._capture_callback = count_ack_callback
    assert composition._dispatch_due(ack_visible - 1)["classification"] == "SUCCESS"
    assert ack_record["result"]["classification"] == "PENDING"
    assert composition._dispatch_due(ack_visible)["classification"] == "SUCCESS"
    assert ack_record["result"]["classification"] == "SUCCESS"
    assert ack_record["result"]["events"] == []
    assert ack_calls == [ack_delivery["delivery_id"]]
    assert composition._codec.inspect_state() == state_after_effect
    assert composition._policy.events == policy_events_before_ack
    ack_audit_after_delivery = deepcopy(composition._audit[ack_delivery["delivery_id"]])
    assert composition._enqueue(dict(kind="DELIVERY", delivery=ack_delivery))["classification"] == "SUCCESS"
    assert composition._policy.events == policy_events_before_ack
    assert composition._codec.inspect_state() == state_after_effect
    assert composition._audit[ack_delivery["delivery_id"]] == ack_audit_after_delivery
    assert ack_calls == [ack_delivery["delivery_id"]]
    assert composition._dispatch_due((start + 45_025) * 16 + 6)["classification"] == "SUCCESS"
    requests = [record for record in composition._records.values()
                if record["item"]["kind"] == "SOURCE_GROUP"
                and record["item"]["group"]["members"][0]["kind"] == "CANCEL_REQUEST"]
    effects = [record for record in composition._records.values()
               if record["item"]["kind"] == "SOURCE_GROUP"
               and record["item"]["group"]["members"][0]["kind"] == "CANCEL_EFFECT"]
    target_requests = [record for record in requests
                       if record["item"]["group"]["members"][0]["payload"]["targets"][0]["target_order_id"]
                       == target]
    target_effects = [record for record in effects
                      if record["item"]["group"]["members"][0]["payload"]["effects"][0]["target_order_id"]
                      == target]
    assert sum(record["result"]["group_result"]["classification"] == "COMMITTED"
               for record in target_requests) == 1
    rejected_repeats = [record for record in target_requests
                        if record["result"]["group_result"]["classification"] == "REJECTED"]
    assert rejected_repeats
    assert all(record["result"]["group_result"]["rejections"][0]["reason"]
               == "CANCEL_REQUEST_TOO_LATE" for record in rejected_repeats)
    assert len(target_effects) == 1
    assert target_effects[0]["result"]["group_result"]["classification"] == "COMMITTED"
    assert target not in {order["order_id"] for order in composition._codec.historical_working_orders()}

    request_effects = {
        effect_record["item"]["group"]["members"][0]["payload"]["effects"][0]["target_order_id"]:
        effect_record
        for effect_record in effects
    }
    pending_repeats = []
    post_effect_repeats = []
    for request_record in requests:
        if request_record["result"]["group_result"]["classification"] != "REJECTED":
            continue
        request_target = request_record["item"]["group"]["members"][0]["payload"]["targets"][0]["target_order_id"]
        if request_target not in request_effects:
            continue
        effect_record = request_effects[request_target]
        (pending_repeats if request_record["key"] < effect_record["key"]
         else post_effect_repeats).append(request_record)
    assert pending_repeats and post_effect_repeats
    policy_events_after_full = deepcopy(composition._policy.events)
    delivered_notice = notice["item"]["delivery"]
    assert composition._codec.build_delivery(dict(
        schema_version="delivery_projection_v1",
        reference=dict(namespace=delivered_notice["source_namespace"], fact_id=delivered_notice["source_fact_id"]),
        payload_kind=delivered_notice["payload_kind"],
        occurrence_index=delivered_notice["occurrence_index"],
        schedule_sequence=delivered_notice["schedule_sequence"],
        visible_at=delivered_notice["visible_at"],
    )) == delivered_notice
    duplicate = composition._enqueue(dict(kind="DELIVERY", delivery=delivered_notice))
    assert duplicate["classification"] == "SUCCESS"
    assert composition._policy.events == policy_events_after_full
    assert composition._dispatch_due((start + 60_000) * 16 + 4)["classification"] == "SUCCESS"
    close_node = next(record for record in composition._records.values()
                      if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"
                      and record["item"]["node"]["bar_open_ms"] == start
                      and record["item"]["node"]["step_index"] == 3)
    assert close_node["result"]["classification"] == "SUCCESS"
    assert target in {order["order_id"] for order in close_node["item"]["node"]["working_orders"]}
    assert all(fill["order_id"] != target
               for product in close_node["result"]["historical_result"]["products"]
               for fill in product["fills"])


def test_historical_cancel_ack_after_endpoint_remains_pending_without_extending_run():
    base = _valid_run()
    start = base.range_start_ms
    trade_rows = tuple(
        replace(row, open=D("101"), high=D("101.5"), low=D("100.5"), close=D("101"), volume=D("0.4"))
        if row.product_id == "A-USDT-SWAP" else row
        for row in base.trade_bars
    )
    run = replace(_rehashed_run(base, trade_rows=trade_rows), range_end_ms=start + 45_004)
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    ack_calls = []
    capture_callback = composition._capture_callback

    def count_ack_callback(delivery):
        if delivery["payload_kind"] == "TRANSPORT_ACK":
            ack_calls.append(delivery["delivery_id"])
        return capture_callback(delivery)

    composition._capture_callback = count_ack_callback
    endpoint = run.range_end_ms * 16 + 6
    assert composition._dispatch_due(endpoint)["classification"] == "SUCCESS"
    effects = [record for record in composition._records.values()
               if record["item"]["kind"] == "SOURCE_GROUP"
               and record["item"]["group"]["members"][0]["kind"] == "CANCEL_EFFECT"
               and record["result"].get("group_result", {}).get("classification") == "COMMITTED"]
    acks = [record for record in composition._records.values()
            if record["item"]["kind"] == "DELIVERY"
            and record["item"]["delivery"]["payload_kind"] == "TRANSPORT_ACK"]
    assert len(effects) == len(acks) == 1
    ack = acks[0]
    assert ack["item"]["delivery"]["source_fact_id"] == effects[0]["item"]["group"]["members"][0]["stamp"]["event_id"]
    assert ack["key"][0] > endpoint == run.range_end_ms * 16 + 6
    assert ack["result"]["classification"] == "PENDING"
    assert ack_calls == []


def test_historical_execution_notice_after_endpoint_remains_pending():
    run = _valid_run()
    trade_rows = tuple(
        replace(row, high=D("103"), low=D("98"), volume=D("1"))
        if row.product_id == "A-USDT-SWAP" else row
        for row in run.trade_bars
    )
    run = _rehashed_run(run, trade_rows=trade_rows)
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    start = run.range_start_ms
    assert composition._dispatch_due((start + 5_020) * 16 + 6)["classification"] == "SUCCESS"
    assert composition._dispatch_due((start + 5_021) * 16 + 3)["classification"] == "SUCCESS"
    for row in composition._policy.rows:
        row["active"] = "false"
    endpoint = run.range_end_ms * 16 + 6
    assert composition._dispatch_due(endpoint)["classification"] == "SUCCESS"
    pending = [record for record in composition._records.values()
               if record["item"]["kind"] == "DELIVERY"
               and record["item"]["delivery"]["payload_kind"] == "EXECUTION_FACT"
               and record["key"][0] > endpoint]
    assert pending
    assert all(record["result"]["classification"] == "PENDING" for record in pending)
    assert all(record["item"]["delivery"]["visible_at"] == run.range_end_ms * 16 + 38
               for record in pending)


def test_historical_chain_runs_final_close_without_endpoint_next_open():
    run = _valid_run()
    composition = _composition(run)
    end = run.range_end_ms
    assert composition._dispatch_due(end * 16 + 6)["classification"] == "SUCCESS"
    steps = [record for record in composition._records.values()
             if record["item"]["kind"] == "HISTORICAL_MARKET_STEP"]
    assert [(record["item"]["node"]["bar_open_ms"], record["item"]["node"]["step_index"])
            for record in steps] == [(run.range_start_ms, 0), (run.range_start_ms, 1),
                                     (run.range_start_ms, 2), (run.range_start_ms, 3)]
    final = steps[-1]
    assert final["key"][0] == end * 16 + 4
    assert final["result"]["classification"] == "SUCCESS"
    assert not any(record["item"]["node"]["bar_open_ms"] == end for record in steps)


def test_private_future_bar_does_not_change_policy_prefix_before_its_close():
    base = _extend_one_minute(_valid_run())
    config = _p2_configuration()
    for product in config["products"]:
        product["marks"][0]["valid_to"] = 200_000_000
    base = _run_with_configuration(base, config)
    changed_rows = tuple(
        replace(row, high=D("120"), low=D("80"), close=D("110"))
        if row.product_id == "A-USDT-SWAP" and row.bar_open_ms == base.range_end_ms - 60_000 else row
        for row in base.trade_bars
    )
    changed = _rehashed_run(base, trade_rows=changed_rows)
    left, right = _composition(base), _composition(changed)
    before_future_close = (base.range_end_ms - 60_000) * 16 + 9
    assert left._dispatch_due(before_future_close)["classification"] == "SUCCESS"
    assert right._dispatch_due(before_future_close)["classification"] == "SUCCESS"
    assert left._policy.events == right._policy.events
    assert left._policy.markets == right._policy.markets
    assert all(event["at_ms"] < base.range_end_ms for event in left._policy.events)


def test_historical_derived_ids_bind_contract_parent_and_ordinal_not_run_id():
    from src.core.backtest.spider_policy_protocol import _historical_child_id

    digest = "a" * 64
    first = _historical_child_id(digest, "delivery-a", "ORDER_GROUP", 0)
    assert first == _historical_child_id(digest, "delivery-a", "ORDER_GROUP", 0)
    assert first != _historical_child_id("b" * 64, "delivery-a", "ORDER_GROUP", 0)
    assert first != _historical_child_id(digest, "delivery-b", "ORDER_GROUP", 0)
    assert first != _historical_child_id(digest, "delivery-a", "ORDER_GROUP", 1)
    with pytest.raises(ValueError):
        _historical_child_id("not-a-hash", "delivery-a", "ORDER_GROUP", 0)
    with pytest.raises(ValueError):
        _historical_child_id(digest, "delivery-a", "ORDER_GROUP", -1)


def test_historical_send_at_endpoint_stays_unsubmitted_without_native_group():
    run = _extend_one_minute(_valid_run())
    composition = _composition(run)
    composition._policy.rows[1]["active"] = "false"
    parent_raw = run.range_start_ms + 5_020
    accepted_raw = parent_raw + run.order_accept_delay_ms
    composition._historical_order_contract["range_end_ms"] = accepted_raw
    assert composition._dispatch_due(parent_raw * 16 + 6)["classification"] == "SUCCESS"
    assert not any(record["item"]["kind"] == "SOURCE_GROUP"
                   and record["item"]["group"].get("ordering_contract_id") == "HISTORICAL_ORDER_V1"
                   for record in composition._records.values())
    actions = [action for events in composition._audit.values() for event in events
               for action in event.get("actions", [])]
    assert actions and all(action["status"] == "UNSUBMITTED" and action["group_id"] is None for action in actions)


def _complete_poll_through_native_deliveries(
    composition: _ReplayComposition, issued_at: int, *, before_final=None,
) -> None:
    poll_plan = composition._historical_poll_plan(issued_at, len(composition._polls))
    started = composition._start_poll(poll_plan, lambda *_: None)
    assert started.observation.status == "IN_PROGRESS"
    poll = composition._polls[poll_plan["poll_id"]]
    while poll.observation.status == "IN_PROGRESS":
        stage = poll.stages[poll.index]
        if poll.index == len(poll.stages) - 1 and before_final is not None:
            before_final()
        request = stage["snapshot_request"]
        fact = composition._codec.capture_snapshot(request)
        projection = deepcopy(stage["delivery_projection"])
        projection["reference"] = fact["reference"]
        delivery = composition._codec.build_delivery(projection)
        composition._resume_poll(delivery)
    assert poll.observation.status == "COMPLETED"


@pytest.mark.parametrize("issued_at,expected_day_ago", [
    (129_540_000, False),  # 11:59 UTC
    (129_600_000, True),   # 12:00 UTC
    (129_660_000, False),  # 12:01 UTC
])
def test_historical_poll_completion_updates_day_ago_only_in_utc_noon_minute(issued_at, expected_day_ago):
    composition = _composition(_valid_run())
    composition._historical_poll_range = (issued_at, issued_at + 1)
    _complete_poll_through_native_deliveries(composition, issued_at)
    assert ("day_ago" in composition._policy.capital) is expected_day_ago
    if expected_day_ago:
        assert composition._policy.capital["day_ago"] == composition._policy.capital["total"]


def test_historical_noon_rollover_refreshes_on_repeated_completed_five_second_ticks():
    composition = _composition(_valid_run())
    first_noon_tick = 129_600_005
    second_noon_tick = first_noon_tick + 5_000
    composition._historical_poll_range = (first_noon_tick, second_noon_tick + 1)
    _complete_poll_through_native_deliveries(composition, first_noon_tick)
    assert composition._policy.capital["day_ago"] == composition._policy.capital["total"]

    _complete_poll_through_native_deliveries(
        composition, second_noon_tick,
        before_final=lambda: composition._policy.capital.__setitem__("total", D("777")),
    )
    assert composition._policy.capital["day_ago"] == D("777")


def test_historical_timer_preserves_poll_start_terminal_and_stops_queue(monkeypatch):
    composition = _composition(_valid_run())
    first_raw = composition._historical_poll_range[0] + 5_000
    original_plan = composition._historical_poll_plan

    def invalid_plan(raw_at, sequence):
        plan = original_plan(raw_at, sequence)
        # A due EARN delivery is visible after this capture; force trading capture
        # to precede that visible boundary so the real poll validator rejects it.
        plan["trading"]["snapshot_request"]["captured_at"] = raw_at
        return plan

    monkeypatch.setattr(composition, "_historical_poll_plan", invalid_plan)
    result = composition._dispatch_due(first_raw * 16 + 9)
    timer = composition._records[(5, f"P3_TIMER_{first_raw}")]
    assert result["classification"] == timer["result"]["classification"] == "TERMINAL"
    assert result["reason"] == timer["result"]["reason"] == "INVALID_SCHEMA"
    assert composition._terminal["classification"] == "TERMINAL"
    assert composition._last_popped == timer["key"]
    later_raw = first_raw + 5_000
    assert (later_raw * 16 + 9, 5, 1, f"P3_TIMER_{later_raw}") in composition._queue
    assert composition._records[(5, f"P3_TIMER_{later_raw}")]["result"]["classification"] == "PENDING"
