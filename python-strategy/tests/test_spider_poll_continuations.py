from copy import deepcopy
from decimal import Decimal as D
from typing import Any

import pytest

from src.core.backtest import spider_policy_protocol as protocol
from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition

ACCOUNT: wire.Account = {"venue": "okx-scenario", "environment": "test", "account": "A"}


def plan(poll="Q1", issued=100000, earn=False) -> dict[str, Any]:
    result: dict[str, Any] = dict(account_key=deepcopy(ACCOUNT), poll_id=poll, issued_at=issued, continuation_id=poll)
    kinds = [("earn", "EARN", "EARN_SNAPSHOT", "POLL_EARN_FAILURE")] if earn else []
    kinds += [("trading", "TRADING", "TRADING_SNAPSHOT", "POLL_Q1_S1" if poll == "Q1" else "POLL_Q2_S2"),
              ("positions", "POSITIONS", "POSITION_SNAPSHOT", "POLL_POSITIONS_EMPTY"),
              ("open_orders", "OPEN_ORDERS", "OPEN_ORDER_SNAPSHOT", "POLL_OPEN_ORDERS_EMPTY")]
    for i, (name, kind, payload, fixture) in enumerate(kinds):
        result[name] = dict(capture_sequence=i, snapshot_request=dict(schema_version="snapshot_request_v1",
            account_key=deepcopy(ACCOUNT), snapshot_id=poll+name, snapshot_kind=kind, capture_mode="FROZEN_POLL_FIXTURE",
            fixture_key=fixture, captured_at=issued+i*2, continuation_id=poll), delivery_projection=dict(
            schema_version="delivery_projection_v1", reference=dict(namespace="SNAPSHOT", fact_id=poll+name),
            payload_kind=payload, occurrence_index=i, schedule_sequence=i, visible_at=issued+i*2, continuation_id=poll))
    return result


def replay():
    return _ReplayComposition("SYNTHETIC_BTC_ETH_V1", ACCOUNT)


def preflight(issued, stage):
    assert stage["snapshot_request"]["captured_at"] >= issued


def response(owner, stage):
    owner._codec.capture_snapshot(stage["snapshot_request"])
    return owner._codec.build_delivery(stage["delivery_projection"])


def state(owner):
    errors = {id(r.observation.prefix.exception): r.observation.prefix.exception for r in owner._polls.values()}
    return deepcopy((vars(owner._policy), owner._polls, owner._continuations, owner._last_poll_at), errors), owner._codec.inspect_state()


def rejected(owner, call):
    before = state(owner)
    with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
        call()
    assert state(owner) == before


@pytest.mark.parametrize("reverse,total", [(False, D(110)), (True, D(100))])
def test_overlapping_arrival_order_one_shot_and_detachment(reverse, total):
    owner = replay()
    native = owner._codec.inspect_state()
    owner._policy.capital["day_ago"] = D(17)
    p1, p2 = plan(), plan("Q2", 100001)
    first, second = owner._start_poll(p1, preflight), owner._start_poll(p2, preflight)
    assert first.clock_advance == 100000 and second.clock_advance == 100001
    assert first.next_stage == p1["trading"] and second.next_stage == p2["trading"]
    assert first.next_stage is not None
    first.next_stage["snapshot_request"]["snapshot_id"] = "poison"
    assert first.observation.awaiting is not None
    first.observation.awaiting["delivery_projection"]["visible_at"] = -1
    p1["positions"]["snapshot_request"]["snapshot_id"] = "poison"
    completed = []
    for p in ([plan("Q2", 100001), plan()] if reverse else [plan(), plan("Q2", 100001)]):
        current = owner._start_poll(p, lambda *_: pytest.fail("duplicate preflight"))
        assert current.clock_advance is None and current.next_stage is None
        while current.observation.status == "IN_PROGRESS":
            before = deepcopy(current.observation.awaiting)
            delivery = response(owner, before)
            delivery.update(payload_digest="00"*32, snapshot_version=99, snapshot_as_of=-1)
            original = deepcopy(delivery)
            current = owner._resume_poll(delivery)
            assert delivery == original and current.clock_advance is None
            assert current.next_stage == current.observation.awaiting
            rejected(owner, lambda: owner._resume_poll(delivery))
            duplicate = owner._start_poll(p, lambda *_: pytest.fail("duplicate preflight"))
            assert duplicate.observation == current.observation and duplicate.next_stage is None
        completed.append(current)
    assert all(r.observation.status == "COMPLETED" and r.next_stage is None for r in completed)
    assert owner._policy.capital["total"] == total and owner._policy.now_ms == 100001
    assert owner._policy.capital["day_ago"] == D(17)
    assert owner._codec.inspect_state() == native
    rejected(owner, lambda: owner._start_poll(dict(plan("Q3"), continuation_id="Q1"), preflight))


@pytest.mark.parametrize("gap,earn", [(60000, False), (60001, True)])
def test_due_boundary_preflight_order_clocks_and_duplicate(gap, earn):
    owner, calls = replay(), []
    owner._policy.capital["day_ago"] = D(17)
    p = plan(issued=100000+gap, earn=earn)
    before = state(owner)
    original = deepcopy(p)
    def inspect_preflight(issued, stage):
        assert state(owner) == before
        calls.append((issued, deepcopy(stage)))
        stage["snapshot_request"]["snapshot_id"] = "poison"
    result = owner._start_poll(p, inspect_preflight)
    assert p == original
    assert len(calls) == 1 and result.next_stage == p["earn" if earn else "trading"]
    assert owner._policy.last_earn_ms == (p["issued_at"] if earn else 100000)
    assert [e["kind"] for e in result.observation.prefix.events] == (["request_earn"] if earn else [])
    assert owner._policy.now_ms == p["issued_at"] and owner._policy.capital["day_ago"] == D(17)
    owner._policy.last_earn_ms = -999999
    duplicate = owner._start_poll(p, lambda *_: pytest.fail("duplicate preflight"))
    assert duplicate.observation == result.observation and duplicate.clock_advance is duplicate.next_stage is None
    rejected(owner, lambda: owner._start_poll(dict(p, issued_at=p["issued_at"]+1), preflight))


def test_pause_reuse_and_preflight_rejection_are_before_mutation():
    owner = replay()
    owner._policy.paused = True
    p = plan(issued=-100, earn=True)
    before = deepcopy(vars(owner._policy))
    result = owner._start_poll(p, lambda *_: pytest.fail("paused preflight"))
    assert result.observation.status == "PAUSED" and result.clock_advance is result.next_stage is None
    assert vars(owner._policy) == before and owner._last_poll_at is None
    rejected(owner, lambda: owner._start_poll(dict(plan("Q2"), continuation_id="Q1"), preflight))
    owner._policy.paused = False
    assert owner._start_poll(p, preflight) == result
    def reject_preflight(*_):
        raise ValueError("INVALID_SCHEMA")
    rejected(owner, lambda: owner._start_poll(plan("Q2"), reject_preflight))
    owner._start_poll(plan("Q2", 100002), preflight)
    rejected(owner, lambda: owner._start_poll(plan("Q3", 100001), preflight))
    assert owner._start_poll(plan("Q3", 100002), preflight).clock_advance == 100002


@pytest.mark.parametrize("stage,field,value", [
    (None, "account_key", dict(ACCOUNT, account="B")), (None, "issued_at", True),
    ("trading", "account_key", dict(ACCOUNT, account="B")), ("trading", "continuation_id", "other"),
    ("trading", "snapshot_kind", "EARN"), ("trading", "captured_at", 99999),
    ("positions", "captured_at", 100000), ("open_orders", "captured_at", 100002),
])
def test_start_stage_business_and_representation_rejections(stage, field, value):
    owner, p = replay(), plan()
    target = p if stage is None else p[stage]["snapshot_request"]
    target[field] = value
    original = deepcopy(p)
    rejected(owner, lambda: owner._start_poll(p, lambda *_: pytest.fail("invalid preflight")))
    assert p == original


@pytest.mark.parametrize("stage", ["earn", "trading", "positions", "open_orders"])
@pytest.mark.parametrize("field,value", [("payload_kind", "TRANSPORT_ACK"), ("continuation_id", "other"),
    ("reference", dict(namespace="SOURCE", fact_id="Q1trading")), ("reference", dict(namespace="SNAPSHOT", fact_id="wrong")),
    ("visible_at", 99999), ("transport", dict(route="REST", operation="ORDER", client_order_id="C", code="0"))])
def test_invalid_projection_and_due_rules(stage, field, value):
    owner, p = replay(), plan(issued=160001, earn=True)
    p[stage]["delivery_projection"][field] = value
    rejected(owner, lambda: owner._start_poll(p, preflight))
    rejected(owner, lambda: owner._start_poll(plan(earn=True), preflight))
    rejected(owner, lambda: owner._start_poll(plan(issued=160001), preflight))


@pytest.mark.parametrize("field,value", [("account_key", dict(ACCOUNT, account="B")), ("source_namespace", "SOURCE"),
    ("source_fact_id", "other"), ("payload_kind", "POSITION_SNAPSHOT"), ("occurrence_index", 99),
    ("schedule_sequence", 99), ("visible_at", 100001), ("continuation_id", "other")])
def test_every_response_identity_before_mutation(field, value):
    owner = replay()
    result = owner._start_poll(plan(), preflight)
    delivery = response(owner, result.next_stage)
    delivery[field] = value
    original = deepcopy(delivery)
    rejected(owner, lambda: owner._resume_poll(delivery))
    assert delivery == original


@pytest.mark.parametrize("failed", ["earn", "trading", "positions", "open_orders"])
def test_each_failure_cache_is_unchanged_and_completes(failed):
    owner = replay()
    p = plan(issued=160001, earn=True)
    p[failed]["snapshot_request"]["fixture_key"] = "POLL_"+failed.upper()+"_FAILURE"
    current = owner._start_poll(p, preflight)
    native = owner._codec.inspect_state()
    while current.next_stage:
        stage = current.next_stage
        delivery = response(owner, stage)
        before = deepcopy((owner._policy.capital, owner._policy.parameters, owner._policy.replies))
        current = owner._resume_poll(delivery)
        if stage == p[failed]:
            assert (owner._policy.capital, owner._policy.parameters, owner._policy.replies) == before
    assert current.observation.status == "COMPLETED" and owner._codec.inspect_state() == native


@pytest.mark.parametrize("fail_at", ["application", "local", "none"])
def test_callback_prefix_terminal_duplicate_and_local_actions_once(monkeypatch, fail_at):
    owner, calls = replay(), []
    native = owner._codec.inspect_state()
    p = plan()
    current = owner._start_poll(p, preflight)
    while current.next_stage != p["open_orders"]:
        current = owner._resume_poll(response(owner, current.next_stage))
    def apply(_):
        owner._policy.capital["total"] = D(123)
        owner._policy.emit("request_market")
        if fail_at == "application":
            raise RuntimeError("source prefix")
    def leverage():
        calls.append("leverage")
        owner._policy.emit("request_earn")
    def risk():
        calls.append("risk")
        if fail_at == "local":
            raise RuntimeError("local prefix")
    monkeypatch.setattr(owner, "_apply_payload", apply)
    monkeypatch.setattr(owner._policy, "raise_leverage", leverage)
    monkeypatch.setattr(owner._policy, "check_risk", risk)
    owner._policy.running, owner._policy.ws_open = True, False
    delivery = response(owner, current.next_stage)
    result = owner._resume_poll(delivery)
    assert result.observation.status == ("COMPLETED" if fail_at == "none" else "CALLBACK_FAILED") and result.next_stage is None
    assert owner._policy.capital["total"] == D(123)
    assert [e["kind"] for e in result.observation.prefix.events] == (["request_market"] if fail_at == "application" else ["request_market", "request_earn", "check_websocket"])
    assert calls == ([] if fail_at == "application" else ["leverage", "risk"])
    duplicate = owner._start_poll(p, lambda *_: pytest.fail("retry"))
    assert str(duplicate.observation.prefix.exception) == str(result.observation.prefix.exception)
    assert duplicate.observation.prefix.events == result.observation.prefix.events and duplicate.next_stage is None
    rejected(owner, lambda: owner._resume_poll(delivery))
    assert owner._codec.inspect_state() == native
    assert calls == ([] if fail_at == "application" else ["leverage", "risk"])


def test_representation_translation_does_not_hide_programming_errors(monkeypatch):
    owner = replay()
    def bug(_):
        raise RuntimeError("bug")
    monkeypatch.setattr(protocol, "_poll_occurrence_plan_digest", bug)
    before = state(owner)
    with pytest.raises(RuntimeError, match="^bug$"):
        owner._start_poll(plan(), preflight)
    assert state(owner) == before


@pytest.mark.parametrize("stage", ["earn", "trading", "positions", "open_orders"])
@pytest.mark.parametrize("field,value", [("account_key", dict(ACCOUNT, account="B")),
    ("continuation_id", "other"), ("snapshot_kind", "MARKET"), ("captured_at", 160000)])
def test_each_stage_request_is_checked_before_preflight(stage, field, value):
    owner, p = replay(), plan(issued=160001, earn=True)
    p[stage]["snapshot_request"][field] = value
    original = deepcopy(p)
    rejected(owner, lambda: owner._start_poll(p, lambda *_: pytest.fail("invalid preflight")))
    assert p == original


def test_forward_visible_boundary_pause_schema_and_normalized_representation():
    owner, p = replay(), plan()
    p["trading"]["delivery_projection"]["visible_at"] += 1
    result = owner._start_poll(p, preflight)
    assert result.next_stage == p["trading"]
    owner._policy.paused = True
    rejected(owner, lambda: owner._start_poll(dict(plan("Q2"), poll_id="\ud800"), preflight))
    rejected(owner, lambda: owner._start_poll(dict(plan("Q2"), issued_at=2**63), preflight))
    rejected(owner, lambda: owner._start_poll(dict(plan("Q2"), account_key=dict(ACCOUNT, account="B")), preflight))
    before = state(owner)
    duplicate = owner._start_poll(p, lambda *_: pytest.fail("duplicate preflight"))
    assert duplicate.next_stage is duplicate.clock_advance is None and state(owner) == before


@pytest.mark.parametrize("stage,previous", [("trading", "earn"), ("positions", "trading"), ("open_orders", "positions")])
def test_each_later_capture_must_exceed_previous_visible(stage, previous):
    owner, p = replay(), plan(issued=160001, earn=True)
    p[stage]["snapshot_request"]["captured_at"] = p[previous]["delivery_projection"]["visible_at"]
    rejected(owner, lambda: owner._start_poll(p, preflight))


def test_account_optional_null_is_same_canonical_identity():
    account: wire.Account = {**ACCOUNT, "subaccount": None}
    owner = _ReplayComposition("SYNTHETIC_BTC_ETH_V1", account)
    result = owner._start_poll(plan(), preflight)
    assert owner._resume_poll(response(owner, result.next_stage)).observation.status == "IN_PROGRESS"
