from copy import deepcopy
from decimal import Decimal as D
from typing import cast

import pytest

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_synthetic_scenario_codec import _configured_input, _historical_node, _seeded_working_order


ACCOUNT: wire.Account = {"venue": "okx-scenario", "environment": "test", "account": "A"}
PROFILE = "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1"


def _owner(*, with_order: bool = False) -> _ReplayComposition:
    configuration = _configured_input(2)
    if with_order:
        configuration["orders"] = [_seeded_working_order()]
    return _ReplayComposition(PROFILE, ACCOUNT, configuration=configuration)


def _item(node: wire.HistoricalNode, *, sequence: int = 0, stable_id: str = "NODE") -> dict[str, object]:
    return dict(kind="HISTORICAL_MARKET_STEP", schedule_sequence=sequence, stable_id=stable_id, node=node)


def _fill_node() -> wire.HistoricalNode:
    configuration = _configured_input(2)
    return _historical_node(configuration, bar_open_ms=60, volume=D("1"), working_orders=[{
        "order_id": "hist-order", "product_id": "WIRE-Z", "order_version": 0,
        "status": "OPEN", "remaining_quantity_contracts": D("1"), "accepted_at": 50,
        "accepted_source_sequence": 1, "order_kind": "LIMIT", "side": "SHORT",
        "limit_price": D("1"), "risk_cancel_pending": False,
    }])


def test_queued_historical_node_dispatches_once_with_frozen_clock_and_detached_result(monkeypatch):
    owner = _owner()
    node = _historical_node(_configured_input(2), bar_open_ms=60)
    item = _item(node)
    calls = []
    original = wire.ScenarioCodec.historical_market_step

    def counted(codec, request):
        calls.append(deepcopy(request))
        return original(codec, request)

    monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", counted)
    canonical_item = deepcopy(item)
    pending = owner._enqueue(item)
    assert pending == dict(kind="HISTORICAL_MARKET_STEP", stable_id="NODE", classification="PENDING")
    node["bars"].clear()
    assert owner._dispatch_due(967)["classification"] == "SUCCESS"
    assert len(calls) == 1
    assert owner._enqueue(canonical_item)["classification"] == "SUCCESS"
    assert len(calls) == 1
    record = owner._records[(3, "NODE")]
    result = record["result"]["historical_result"]
    assert (result["raw_time_ms"], result["effective_at"]) == (60, 967)
    assert [row["product_id"] for row in result["products"]] == ["WIRE-Z", "WIRE-A"]
    observation = owner._scheduler_observation()
    assert observation["records"][(3, "NODE")]["item"]["kind"] == "HISTORICAL_MARKET_STEP"


def test_exact_duplicate_is_noop_and_conflicting_duplicate_fails_before_native_mutation(monkeypatch):
    owner = _owner()
    node = _historical_node(_configured_input(2), bar_open_ms=60)
    item = _item(node)
    before = owner._codec.inspect_state()
    assert owner._enqueue(item)["classification"] == "PENDING"
    assert owner._enqueue(deepcopy(item))["classification"] == "PENDING"
    changed = deepcopy(item)
    cast(wire.HistoricalNode, changed["node"])["step_index"] = 1
    conflict = owner._enqueue(changed)
    assert conflict["classification"] == "TERMINAL" and conflict["reason"] == "INVALID_SCHEMA"
    assert owner._codec.inspect_state() == before
    assert owner._dispatch_due(1000) == conflict


@pytest.mark.parametrize("change", ["bool_time", "overflow", "unknown_step"])
def test_invalid_clock_rejects_before_queue_or_owner_mutation(change):
    owner = _owner()
    node = _historical_node(_configured_input(2), bar_open_ms=60)
    if change == "bool_time":
        node["bar_open_ms"] = cast(int, True)
    elif change == "overflow":
        node["bar_open_ms"] = 2**59 - 1
        node["step_index"] = 1
    else:
        node["step_index"] = cast(int, 4)
    before = owner._codec.inspect_state()
    rejected = owner._enqueue(_item(node))
    assert rejected["classification"] == "TERMINAL" and rejected["reason"] == "INVALID_SCHEMA"
    assert not owner._queue and not owner._records
    assert owner._codec.inspect_state() == before


def test_out_of_order_node_fails_without_second_native_owner_mutation(monkeypatch):
    owner = _owner()
    calls = []
    original = wire.ScenarioCodec.historical_market_step

    def counted(codec, request):
        calls.append(request["bar_open_ms"])
        return original(codec, request)

    monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", counted)
    first = _item(_historical_node(_configured_input(2), bar_open_ms=60), stable_id="FIRST")
    assert owner._enqueue(first)["classification"] == "PENDING"
    assert owner._dispatch_due(967)["classification"] == "SUCCESS"
    before = owner._codec.inspect_state()
    earlier = _item(_historical_node(_configured_input(2), bar_open_ms=59), stable_id="EARLIER")
    rejected = owner._enqueue(earlier)
    assert rejected["classification"] == "TERMINAL" and rejected["reason"] == "INVALID_SCHEMA"
    assert calls == [60] and owner._codec.inspect_state() == before


def test_scheduler_commits_limit_fill_via_native_owner_once(monkeypatch):
    owner = _owner(with_order=True)
    node = _fill_node()
    calls = []
    original = wire.ScenarioCodec.historical_market_step

    def counted(codec, request):
        calls.append(request)
        return original(codec, request)

    monkeypatch.setattr(wire.ScenarioCodec, "historical_market_step", counted)
    assert owner._enqueue(_item(node))["classification"] == "PENDING"
    assert owner._dispatch_due(967)["classification"] == "SUCCESS"
    assert len(calls) == 1
    result = owner._records[(3, "NODE")]["result"]["historical_result"]
    fills = result["products"][0]["fills"]
    assert (result["raw_time_ms"], result["effective_at"]) == (60, 967)
    assert len(fills) == 1 and fills[0]["order_id"] == "hist-order"
    assert fills[0]["quantity_contracts"] == D("0.5")
    assert fills[0]["price"] == D("1") and fills[0]["execution_id"]
    assert result["owner_evidence"]["total_fees"] == D("0.0005")


def test_scheduler_preserves_native_market_owner_rejection_without_mutation():
    owner = _owner(with_order=True)
    before = owner._codec.inspect_state()
    node = _historical_node(_configured_input(2), bar_open_ms=60, volume=D("1"), working_orders=[{
        "order_id": "hist-order", "product_id": "WIRE-Z", "order_version": 0,
        "status": "OPEN", "remaining_quantity_contracts": D("1"), "accepted_at": 50,
        "accepted_source_sequence": 1, "order_kind": "MARKET", "side": "SHORT",
        "limit_price": None, "risk_cancel_pending": False,
    }])
    assert owner._enqueue(_item(node))["classification"] == "PENDING"
    rejected = owner._dispatch_due(967)
    assert rejected["classification"] == "TERMINAL"
    assert rejected["reason"] == "HISTORICAL_MARKET_OWNER_UNSUPPORTED"
    assert owner._codec.inspect_state() == before
