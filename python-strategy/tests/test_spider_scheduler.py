from copy import deepcopy
from decimal import Decimal as D
from typing import Any, cast

import pytest

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest import spider_policy_protocol as protocol
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from test_spider_emission_pairing import fixture as emission_fixture
from test_spider_poll_continuations import plan as poll_plan

ACCOUNT: wire.Account = {"venue": "okx-scenario", "environment": "test", "account": "A"}


def stage(name="S", at=500, visible=500, sequence=0) -> dict[str, Any]:
    return dict(capture_sequence=sequence, request=dict(schema_version="snapshot_request_v1", account_key=ACCOUNT,
        snapshot_id=name, snapshot_kind="TRADING", capture_mode="FROZEN_POLL_FIXTURE", fixture_key="POLL_Q1_S1", captured_at=at),
        delivery_projection=dict(schema_version="delivery_projection_v1", reference=dict(namespace="SNAPSHOT", fact_id=name),
            payload_kind="TRADING_SNAPSHOT", occurrence_index=0, schedule_sequence=sequence, visible_at=visible))


def materialize(s):
    codec = wire.ScenarioCodec("SYNTHETIC_BTC_ETH_V1", ACCOUNT)
    codec.capture_snapshot(s["request"])
    return codec.build_delivery(s["delivery_projection"])


def empty(delivery):
    return dict(delivery_id=delivery["delivery_id"], expected_policy_events=[], financial_items=[], market_requests=[])


def source(name="G", at=500, sequence=0) -> dict[str, Any]:
    member = dict(kind="INTENT", stamp=dict(event_id=name, effective_at=at, causal_parent_ids=[],
        ordering_contract_id="S_order_v1", scenario_ordinal=60), payload=dict(intent_id=name, client_order_id=name,
        config_id="scenario-v1", product_id="BTC-USDT-SWAP", strategy_id="S", side="LONG", order_type="LIMIT",
        quantity_contracts=D(100000), limit_price=D(50000), reduce_only=False, requested_at=at))
    return dict(kind="SOURCE_GROUP", schedule_sequence=sequence, group=dict(schema_version="scenario_group_v1", group_id=name,
        account_key=ACCOUNT, ordering_contract_id="S_order_v1", group_effective_at=at, declared_member_count=1, members=[member]))


def owner(stages=()):
    plans = {d["delivery_id"]: empty(d) for d in map(materialize, stages)}
    return _ReplayComposition("SYNTHETIC_BTC_ETH_V1", ACCOUNT, plans)


def capture(s) -> dict[str, Any]:
    return dict(kind="SNAPSHOT_CAPTURE", **s)


def enqueue_delivery(o, d, p=None):
    return o._enqueue(dict(kind="DELIVERY", delivery=d), empty(d) if p is None else p)


def test_clock_inclusive_order_new_due_work_and_detachment(monkeypatch):
    s = stage("G")
    o = owner([s])
    calls = []
    apply, snapshot = wire.ScenarioCodec.apply_group, wire.ScenarioCodec.capture_snapshot
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", lambda self, x: (calls.append("source"), apply(self, x))[1])
    monkeypatch.setattr(wire.ScenarioCodec, "capture_snapshot", lambda self, x: (calls.append("capture"), snapshot(self, x))[1])
    assert o._current_time == 500 and o._policy.now_ms == 100000
    assert o._enqueue(capture(s))["classification"] == "PENDING"
    assert o._enqueue(source())["classification"] == "PENDING"
    s["request"]["fixture_key"] = "poison"
    assert o._dispatch_due(500)["classification"] == "SUCCESS"
    assert calls == ["source", "capture"]
    view = o._scheduler_observation()
    assert view["last_popped"][:3] == (500, 2, 0) and not view["pending"]
    assert view["records"][(0, "G")]["result"]["group_result"]["classification"] == "REJECTED"
    assert view["records"][(1, "G")]["result"]["classification"] == "SUCCESS"
    assert o._dispatch_due(900)["classification"] == "SUCCESS" and o._current_time == 900
    assert o._policy.now_ms == 100000
    view["records"].clear()
    assert o._scheduler_observation()["records"]


@pytest.mark.parametrize("kind", ["SOURCE_GROUP", "SNAPSHOT_CAPTURE"])
def test_retained_class_identity_duplicate_before_clock_and_conflict(kind):
    item = source() if kind == "SOURCE_GROUP" else capture(stage())
    o = owner([stage()])
    original = deepcopy(item)
    o._enqueue(item)
    o._dispatch_due(600)
    assert o._enqueue(item)["classification"] == "SUCCESS" and item == original
    changed = deepcopy(item)
    changed["schedule_sequence" if kind == "SOURCE_GROUP" else "capture_sequence"] = 8
    failure = o._enqueue(changed)
    assert failure["reason"] == "INVALID_SCHEMA"
    assert o._enqueue(None) == o._dispatch_due(False) == o._begin_poll(None) == failure


@pytest.mark.parametrize("change", ["sequence", "financial", "past", "last_key", "cutoff", "visible", ["bad"], {"bad": 1}, 7, "UNKNOWN"])
def test_queue_collisions_and_cutoff_fail_before_work(change):
    o = owner()
    o._enqueue(source("guard", 700))
    if change == "sequence":
        o._enqueue(capture(stage("A")))
        failure = o._enqueue(capture(stage("B")))
    elif change == "financial":
        o._enqueue(source())
        failure = o._enqueue(source("other", sequence=1))
    elif change == "past":
        o._dispatch_due(600)
        failure = o._enqueue(source(at=599))
    elif change == "last_key":
        enqueue_delivery(o, materialize(stage()))
        o._dispatch_due(500)
        failure = o._enqueue(capture(stage()))
    elif change == "cutoff":
        failure = o._dispatch_due(True)
    else:
        failure = o._enqueue(capture(stage(at=501, visible=500)) if change == "visible" else dict(kind=change))
    assert failure["reason"] == "INVALID_SCHEMA" and o._policy.events == []
    assert o._enqueue(None) == o._dispatch_due(900) == o._begin_poll(None) == failure and (700, 0, 0, "guard") in o._queue


@pytest.mark.parametrize("completed", [False, True])
@pytest.mark.parametrize("changed", ["payload", "plan"])
def test_delivery_two_digest_conflict_precedes_queue_and_clock(completed, changed):
    o, d = owner(), materialize(stage())
    p = empty(d)
    pending = enqueue_delivery(o, d, p)
    assert enqueue_delivery(o, d, p) == pending
    if completed:
        o._dispatch_due(600)
        assert enqueue_delivery(o, d, p)["classification"] == "SUCCESS"
    before = deepcopy(vars(o._policy))
    d, p = deepcopy(d), deepcopy(p)
    if changed == "payload":
        d["payload_digest"] = "00"*32
    else:
        p["expected_policy_events"] = [dict(kind="request_market", event_digest="00"*32)]
    result = enqueue_delivery(o, d, p)
    assert result["reason"] == "DELIVERY_ID_CONFLICT" and vars(o._policy) == before


def test_capture_is_immutable_and_registry_is_detached():
    s = stage(visible=502)
    d = materialize(s)
    plans = {d["delivery_id"]: empty(d)}
    o = _ReplayComposition("SYNTHETIC_BTC_ETH_V1", ACCOUNT, plans)
    plans.clear()
    o._enqueue(capture(s))
    o._dispatch_due(500)
    stored = deepcopy(o._records[(2, d["delivery_id"])]["item"]["delivery"])
    o._codec.apply_group(source("later", 501)["group"])
    assert o._dispatch_due(502)["classification"] == "SUCCESS"
    assert o._records[(2, d["delivery_id"])]["item"]["delivery"] == stored
    assert o._policy.capital["total"] == D(100)


def test_missing_plan_keeps_native_capture_and_delivery():
    o, s = owner(), stage()
    before = o._codec.inspect_state()
    o._enqueue(capture(s))
    failure = o._dispatch_due(800)
    assert failure["reason"] == "INVALID_SCHEMA" and o._current_time == 500
    fact = o._codec.capture_snapshot(s["request"])
    delivery = o._codec.build_delivery(s["delivery_projection"])
    assert fact["reference"]["fact_id"] == delivery["source_fact_id"] == "S"
    assert o._codec.inspect_state() == before


def batch(o, monkeypatch):
    events, plan = emission_fixture()
    event = events[0]
    event["orders"].append(deepcopy(event["orders"][0]))
    event_digest = protocol._policy_event_digest(event)
    plan["financial_items"][2] = deepcopy(plan["financial_items"][0])
    for i, item in enumerate(plan["financial_items"]):
        item["event_digest"] = event_digest
        item["expected_group"]["group_id"] = f"G{i}"
        item["expected_group"]["group_effective_at"] = 501+i
        item["expected_group"]["members"][0]["stamp"]["effective_at"] = 501+i
    d = materialize(stage())
    plan.update(delivery_id=d["delivery_id"], expected_policy_events=[dict(kind="send", event_digest=event_digest)], market_requests=[])
    def callback(_):
        o._policy.events.append(deepcopy(event))
    monkeypatch.setattr(o, "_apply_payload", callback)
    enqueue_delivery(o, d, plan)
    return d, plan


@pytest.mark.parametrize("failure", ["mapping", "collision", "callback"])
def test_entire_callback_batch_admission_is_atomic(monkeypatch, failure):
    o = owner()
    d, p = batch(o, monkeypatch)
    if failure == "mapping":
        o._records[(2, d["delivery_id"])]["plan"]["financial_items"][2]["expected_group"]["members"][0]["payload"]["client_order_id"] = "wrong"
    elif failure == "collision":
        o._enqueue(source("occupied", 503))
    else:
        def broken(_):
            o._policy.emit("request_market")
            raise RuntimeError("source")
        monkeypatch.setattr(o, "_apply_payload", broken)
    native = o._codec.inspect_state()
    result = o._dispatch_due(600)
    assert result["reason"] == {"mapping": "POLICY_EMISSION_MISMATCH", "collision": "INVALID_SCHEMA", "callback": "CALLBACK_FAILED"}[failure]
    assert all((0, f"G{i}") not in o._records for i in range(3)) and o._codec.inspect_state() == native
    assert o._policy.events and o._current_time == 500
    assert o._dispatch_due(None) == result


@pytest.mark.parametrize("mode", ["native", "fault", "reject"])
def test_partial_multi_action_audit_and_due_only_submission(monkeypatch, mode):
    o, calls = owner(), []
    d, p = batch(o, monkeypatch)
    native_error: Exception = RuntimeError("missing fixture exception")
    try:
        o._codec.build_delivery(cast(wire.Projection, dict(stage()["delivery_projection"], reference=dict(namespace="SOURCE", fact_id="missing"))))
    except Exception as exc:
        native_error = exc
    actual = o._codec.apply_group(source()["group"])
    fault_source = source("fault")
    fault_source["group"]["members"][0]["payload"]["order_type"] = "MARKET"
    del fault_source["group"]["members"][0]["payload"]["limit_price"]
    actual_fault = wire.ScenarioCodec("SYNTHETIC_BTC_ETH_V1", ACCOUNT).apply_group(fault_source["group"])
    def apply(_, group):
        calls.append(group["group_id"])
        if len(calls) == 2 and mode == "native":
            raise native_error
        return actual_fault if len(calls) == 2 and mode == "fault" else actual
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", apply)
    assert o._dispatch_due(500)["classification"] == "SUCCESS" and calls == []
    result = o._dispatch_due(600)
    view = o._scheduler_observation()
    event = view["callbacks"][d["delivery_id"]][0]
    assert mode != "fault" or (result["group_result"] == actual_fault and event["actions"][1]["group_result"] == actual_fault)
    assert calls == (["G0", "G1", "G2"] if mode == "reject" else ["G0", "G1"])
    assert event["disposition"] == ("ALL" if mode == "reject" else "PARTIAL")
    assert [a["status"] for a in event["actions"]] == (["SUBMITTED"]*3 if mode == "reject" else ["SUBMITTED", "SUBMITTED", "UNSUBMITTED"])
    if mode == "native":
        assert "native_failure" in event["actions"][1] and "group_result" not in event["actions"][1]
    if mode != "reject":
        assert o._current_time == 502 and view["pending"] and result["classification"] == "TERMINAL"
        assert enqueue_delivery(o, d, p) == result


def test_poll_handoff_and_whole_derived_set_collision(monkeypatch):
    p = poll_plan()
    stages = [dict(capture_sequence=p[k]["capture_sequence"], request=p[k]["snapshot_request"], delivery_projection=p[k]["delivery_projection"])
              for k in ("trading", "positions", "open_orders")]
    events, emission = emission_fixture()
    emission.update(delivery_id=materialize(stages[0])["delivery_id"], expected_policy_events=emission["expected_policy_events"][:1],
                    financial_items=emission["financial_items"][:2], market_requests=[])
    for i, item in enumerate(emission["financial_items"]):
        group = item["expected_group"]
        group["group_effective_at"] = group["members"][0]["stamp"]["effective_at"] = 100001+i
    plans = {d["delivery_id"]: empty(d) for d in map(materialize, stages)}
    plans[emission["delivery_id"]] = emission
    o = _ReplayComposition("SYNTHETIC_BTC_ETH_V1", ACCOUNT, plans)
    monkeypatch.setattr(o, "_apply_payload", lambda _: o._policy.events.append(deepcopy(events[0])))
    result = o._begin_poll(p)
    assert result["classification"] == "SUCCESS" and o._current_time == 100000 and len(o._queue) == 1
    assert o._begin_poll(p) == result and len(o._queue) == 1
    collision = deepcopy(stages[1])
    collision["request"]["snapshot_id"] = collision["delivery_projection"]["reference"]["fact_id"] = "occupied"
    o._enqueue(capture(collision))
    failed = o._dispatch_due(100000)
    assert failed["reason"] == "INVALID_SCHEMA"
    assert (1, "Q1positions") not in o._records and o._polls["Q1"].index == 1
    assert (0, "G0") not in o._records and (0, "G1") not in o._records


@pytest.mark.parametrize("why", ["earlier", "last_key", "cutoff"])
def test_poll_scheduler_preflight_prevents_mutation(why):
    o = owner()
    p = poll_plan(issued=500)
    if why == "earlier":
        o._enqueue(source(at=500))
        p = poll_plan(issued=501)
    elif why == "last_key":
        d = materialize(stage())
        enqueue_delivery(o, d)
        o._dispatch_due(500)
    else:
        o._dispatch_due(501)
    before = deepcopy(vars(o._policy))
    result = o._begin_poll(p)
    assert result["reason"] == "INVALID_SCHEMA" and vars(o._policy) == before and not o._polls


def test_programmer_errors_are_not_native_or_callback_failures(monkeypatch):
    o = owner()
    def broken(_, __):
        raise RuntimeError("programmer")
    monkeypatch.setattr(wire.ScenarioCodec, "apply_group", broken)
    o._enqueue(source())
    with pytest.raises(RuntimeError, match="^programmer$"):
        o._dispatch_due(500)
    assert o._terminal is None and wire._native_failure(ValueError("INVALID_SCHEMA")) is None


@pytest.mark.parametrize("where", ["capture", "build"])
def test_recognized_native_capture_build_failures_retain_prefix(where):
    o, s = owner(), stage()
    if where == "capture":
        s["request"]["fixture_key"] = "UNKNOWN_FIXTURE"
    else:
        s["delivery_projection"]["payload_kind"] = "EARN_SNAPSHOT"
    native = o._codec.inspect_state()
    o._enqueue(capture(s))
    result = o._dispatch_due(600)
    assert result["classification"] == "TERMINAL" and "native_failure" in result
    assert result["reason"] != "CALLBACK_FAILED" and o._codec.inspect_state() == native
    if where == "build":
        assert o._codec.capture_snapshot(s["request"])["reference"]["fact_id"] == "S"


def test_complete_poll_due_dispatch_and_delivery_duplicate_do_not_repeat_local_actions():
    p = poll_plan()
    stages = [dict(capture_sequence=p[k]["capture_sequence"], request=p[k]["snapshot_request"], delivery_projection=p[k]["delivery_projection"])
              for k in ("trading", "positions", "open_orders")]
    o = owner(stages)
    o._begin_poll(p)
    assert o._dispatch_due(100010)["classification"] == "SUCCESS" and o._polls["Q1"].observation.status == "COMPLETED"
    before = deepcopy(vars(o._policy))
    d = materialize(stages[-1])
    assert enqueue_delivery(o, d)["classification"] == "SUCCESS" and o._begin_poll(p)["status"] == "COMPLETED"
    assert vars(o._policy) == before and not o._queue
