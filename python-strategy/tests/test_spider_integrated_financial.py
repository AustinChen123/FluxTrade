from copy import deepcopy
from decimal import Decimal as D, localcontext
from typing import Any, cast
from unittest.mock import patch

import pytest

from src.core.backtest import spider_policy_protocol as protocol
from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition
from spider_acceptance_fixtures import (
    ACCOUNT, canonical, checkpoint, delivery_id, empty_plan, group, projection, snapshot, source, twice,
)


def intent(client="C", identity="I", at=500, price="10", quantity="10", product="P_A", side="LONG", strategy="S"):
    return dict(intent_id=identity, client_order_id=client, config_id="scenario-v1", product_id=product,
                strategy_id=strategy, side=side, order_type="LIMIT", quantity_contracts=D(quantity),
                limit_price=D(price), reduce_only=False, requested_at=at)


def execution(order, name, at, quantity, account_version, order_version, golden=True):
    return dict(namespace="synthetic-v1", product_id="P_A" if golden else "BTC-USDT-SWAP",
                external_execution_id=name, order_id=order, side="LONG" if golden else "SHORT",
                price=D(10 if golden else 50000), quantity_contracts=D(quantity), liquidity="SYNTHETIC_TAKER",
                matching_effective_at=at, candidate_id=f"candidate-{name}", source_id=f"source-{name}", visible_at=at,
                expected_account_version=account_version, expected_order_version=order_version,
                spec_version="gt03-spec-v1" if golden else "spec-v1", rule_data_version="gt03-rule-v1" if golden else "tier-v1")


def replacement_plan():
    # Literal source-policy output fixed before either subject run.
    orders: list[dict[str, Any]] = [dict(instId="P_A", instIdCode=1, tdMode="cross", clOrdId=client,
                   tag="cce88b5c6506BCDE", side=side, ordType="limit", px=price, sz="1")
              for client, side, price in [("0000014998", "buy", "9"), ("0000025001", "sell", "11")]]
    event = dict(at_ms=100000, kind="send", operation="send", route="REST", orders=orders)
    digest = protocol._policy_event_digest(event)
    plan = empty_plan("X2", "EXECUTION_FACT")
    plan["expected_policy_events"] = [dict(kind="send", event_digest=digest)]
    plan["financial_items"] = [dict(event_digest=digest, action_kind="ORDER_INTENT", schedule_sequence=i,
        expected_group=group(f"replacement-{i}", 507+i, "INTENT",
            intent(client=order["clOrdId"], identity=f"replacement-I{i}", at=507+i,
                   price=order["px"], quantity="1", side="LONG" if i == 0 else "SHORT"), 60))
        for i, order in enumerate(orders)]
    return event, plan


def deliver(replay, request, plan):
    before = replay._codec.inspect_state()
    delivery = replay._codec.build_delivery(cast(wire.Projection, request))
    assert delivery["delivery_id"] == delivery_id(request["reference"]["fact_id"], request["payload_kind"])
    original = deepcopy(delivery)
    assert replay._enqueue(dict(kind="DELIVERY", delivery=delivery), plan)["classification"] == "PENDING"
    assert replay._dispatch_due(request["visible_at"])["classification"] == "SUCCESS"
    assert replay._codec.inspect_state() == before and delivery == original
    assert replay._codec.build_delivery(cast(wire.Projection, request)) == original
    assert replay._codec.inspect_state() == before
    return delivery


def golden_run(trace, full):
    expected_event, refill = replacement_plan()
    plans = [empty_plan("X1", "EXECUTION_FACT"), refill if full else empty_plan("X2", "EXECUTION_FACT"),
             empty_plan("effect", "TRANSPORT_ACK")]
    replay = _ReplayComposition("SYNTHETIC_GOLDEN_CANCEL_V1", ACCOUNT, {p["delivery_id"]: p for p in plans})
    assert source(replay, group("admit", 500, "INTENT", intent(), 60), 500)[1]["committed_references"] == [dict(namespace="SOURCE", fact_id="admit")]
    opened = replay._codec.capture_snapshot(cast(wire.SnapshotRequest, snapshot("initial-orders", "OPEN_ORDERS", 500)))
    row = cast(wire.OpenOrders, opened["immutable_payload"])["rows"][0]
    order = row["order_id"]
    assert row["client_order_id"] == "C" and row["created_at"] == 500
    checkpoint(replay, trace, "admitted")
    operations = [
        ("X1", 501, "EXECUTION", execution(order, "X1", 501, "4", 1, 1), 30, "999.6", "0.4", "939.6", 2),
        ("request", 502, "CANCEL_REQUEST", dict(targets=[dict(target_order_id=order, reason="EXPLICIT_SCENARIO")]), 40, "999.6", "0.4", "939.6", 3),
        ("X2", 503, "EXECUTION", execution(order, "X2", 503, "6" if full else "3", 3, 3), 30,
         "999" if full else "999.3", "1" if full else "0.7", "999" if full else "969.3", 4),
        ("effect", 504, "CANCEL_EFFECT", dict(effects=[dict(detecting_event_id="request", target_order_id=order,
             reason="EXPLICIT_SCENARIO")]), 50, "999" if full else "999.3", "1" if full else "0.7", "999" if full else "999.3", 5),
    ]
    for name, at, kind, payload, ordinal, cash, fee, available, version in operations:
        result, committed = source(replay, group(name, at, kind, payload, ordinal), at)
        assert result["classification"] == "SUCCESS" and committed["classification"] == "COMMITTED"
        assert committed["committed_references"] == [dict(namespace="SOURCE", fact_id=name)]
        owner = checkpoint(replay, trace, name)
        assert (owner["cash"], owner["total_fees"], owner["gross_realized"], owner["account_version"]) == (D(cash), D(fee), D(0), version)
        trading = replay._codec.capture_snapshot(cast(wire.SnapshotRequest, snapshot(f"trading-{name}", "TRADING", at)))
        assert trading["immutable_payload"] == dict(outcome="SUCCESS", equity=D(cash), available_equity=D(available))
        assert replay._codec.inspect_state() == owner
    closed_orders = replay._codec.capture_snapshot(cast(wire.SnapshotRequest, snapshot("closed-orders", "OPEN_ORDERS", 504)))
    assert closed_orders["immutable_payload"] == dict(outcome="SUCCESS", rows=[])
    first = deliver(replay, projection("X1", "EXECUTION_FACT", 505, 0), plans[0])
    first_fact = cast(wire.ExecutionFact, first["immutable_payload"])
    assert first_fact["commit_account_version"] == 2
    assert first_fact["state"] == "partially_filled" and replay._policy.events == []
    assert replay._policy.replies[order]["accFillSz"] == "4"
    second = deliver(replay, projection("X2", "EXECUTION_FACT", 505, 1), plans[1])
    fact = cast(wire.ExecutionFact, second["immutable_payload"])
    assert (fact["owner_client_order_id"], fact["policy_client_order_id"], fact["original_size_contracts"],
            fact["cumulative_filled_size_contracts"], fact["execution_effective_at"], fact["commit_account_version"]) == (
                "C", "0000015000", D(10), D(10 if full else 7), 503, 4)
    assert fact["state"] == ("filled" if full else "partially_filled")
    assert (fact["limit_price"], fact["fill_price"], fact["contract_value"], fact["spec_version"],
            fact["rule_data_version"], fact["side"]) == (D(10), D(10), D(1), "gt03-spec-v1", "gt03-rule-v1", "buy")
    assert replay._policy.events == ([expected_event] if full else [])
    policy_before, owner_before = deepcopy(vars(replay._policy)), replay._codec.inspect_state()
    assert replay._enqueue(dict(kind="DELIVERY", delivery=second), plans[1])["classification"] == "SUCCESS"
    assert vars(replay._policy) == policy_before and replay._codec.inspect_state() == owner_before
    checkpoint(replay, trace, "executions-delivered")
    ack = projection("effect", "TRANSPORT_ACK", 506, 0, dict(route="REST", operation="CANCEL",
                     client_order_id="C", order_id=order, code="0"))
    delivered_ack = deliver(replay, ack, plans[2])
    assert delivered_ack["source_fact_id"] == "effect" and "snapshot_version" not in delivered_ack
    assert replay._codec.inspect_state()["account_version"] == 5
    checkpoint(replay, trace, "phase-two-ack")
    if full:
        failure = replay._dispatch_due(508)
        assert failure["reason"] == "INVALID_GOLDEN_CANCEL_INTENT"
        observation = replay._scheduler_observation()
        actions = observation["callbacks"][refill["delivery_id"]][0]["actions"]
        assert [a["status"] for a in actions] == ["SUBMITTED", "UNSUBMITTED"]
        assert actions[0]["group_result"]["failure"] == "INVALID_GOLDEN_CANCEL_INTENT"
        assert observation["records"][(0, "replacement-1")]["result"]["classification"] == "PENDING"
        assert replay._codec.inspect_state()["account_version"] == 5
        assert replay._codec.inspect_state()["cash"] == D(999)
    return replay


@pytest.mark.parametrize("full", [False, True])
def test_gt03_real_financial_policy_trace_twice(monkeypatch, full):
    twice(monkeypatch, lambda trace: golden_run(trace, full))


def minimum_run(trace):
    frozen_payload = dict(order_id="MIN-O1", owner_client_order_id="MIN-C1", policy_client_order_id="MIN-C1",
        product_id="BTC-USDT-SWAP", state="filled", side="sell", limit_price=D(50000), fill_price=D(50000),
        original_size_contracts=D(20), cumulative_filled_size_contracts=D(20), contract_value=D("0.01"),
        execution_effective_at=501, commit_account_version=1, spec_version="spec-v1", rule_data_version="tier-v1")
    plan = empty_plan("MIN-X1", "EXECUTION_FACT")
    replay = _ReplayComposition("SYNTHETIC_MIN_CASH_V1", ACCOUNT, {plan["delivery_id"]: plan})
    initial = checkpoint(replay, trace, "seed")
    assert initial["valuation_context_id"] == "a130fcbba43b12aad016ffdd1e09f250658721a313fceb909c6a40750938c357"
    result, committed = source(replay, group("MIN-X1", 501, "EXECUTION",
        execution("MIN-O1", "MIN-X1", 501, "20", 0, 0, False), 30), 501)
    assert result["classification"] == "SUCCESS" and committed["committed_references"] == [dict(namespace="SOURCE", fact_id="MIN-X1")]
    closed = checkpoint(replay, trace, "close501")
    assert (closed["cash"], closed["total_fees"], closed["gross_realized"], closed["account_version"]) == (D(990), D(10), D(0), 1)
    request = projection("MIN-X1", "EXECUTION_FACT", 504, 0)
    original = replay._codec.build_delivery(cast(wire.Projection, request))
    negative = intent("MIN-NEGATIVE-C", "MIN-NEGATIVE-I", 502, "50000", "1", "BTC-USDT-SWAP", "LONG", "min-cash-policy")
    result, rejected = source(replay, group("reject", 502, "INTENT", negative, 60), 502)
    assert result["classification"] == "SUCCESS" and rejected["classification"] == "REJECTED"
    assert rejected["rejections"] == [dict(event_id="reject", reason="MIN_CASH")]
    unchanged = checkpoint(replay, trace, "reject502")
    for field in ("cash", "total_fees", "gross_realized", "account_version", "orders_digest", "positions_digest", "reservations_digest"):
        assert unchanged[field] == closed[field]
    # Independent literal hash_fields reconstruction: BTC/ETH spec-v1/tier-v1,
    # complete frozen tables, marks 50000@[0,3000) and 101@[503,3000), leverage10.
    context = dict(expected_before=initial["valuation_context_id"],
        expected_after="1ea48d053febc00bb7b39f380dbde9dc5e486607bb92286e345df3408a222195",
        rows=[dict(product_id="BTC-USDT-SWAP", mark=D(50000), valid_from=0, valid_to=3000),
              dict(product_id="ETH-USDT-SWAP", mark=D(101), valid_from=503, valid_to=3000)])
    result, activated = source(replay, group("context", 503, "CONTEXT_MARKS", context, 20), 503)
    assert result["classification"] == "SUCCESS" and activated["classification"] == "COMMITTED"
    current = checkpoint(replay, trace, "context503")
    assert current["account_version"] == 2 and current["cash"] == D(990)
    callback_inputs = []
    apply_payload = replay._apply_payload
    def observe_callback(actual):
        callback_inputs.append(deepcopy(actual))
        return apply_payload(actual)
    with patch.object(replay, "_apply_payload", observe_callback):
        delivered = deliver(replay, request, plan)
    trace.append(dict(checkpoint="callback504-entry", callback_inputs=deepcopy(callback_inputs)))
    assert len(callback_inputs) == 1 and callback_inputs[0]["immutable_payload"] == frozen_payload
    assert delivered == original and delivered["delivery_id"] == plan["delivery_id"]
    assert delivered["immutable_payload"] == frozen_payload
    assert replay._policy.events == [] and "MIN-O1" not in replay._policy.replies
    before = deepcopy(vars(replay._policy))
    assert replay._enqueue(dict(kind="DELIVERY", delivery=delivered), plan)["classification"] == "SUCCESS"
    assert vars(replay._policy) == before and replay._codec.inspect_state() == current
    return replay


def test_a07a_real_delayed_immutable_fill_twice(monkeypatch):
    twice(monkeypatch, minimum_run)


def test_trace_normalization_is_exact_and_closed():
    with localcontext() as context:
        context.prec = 2
        assert canonical((D("123456789.1234500"), D("-0.00"), b"\x00\xff", "雪")) == (
            '[{"decimal":"123456789.12345"},{"decimal":"0"},{"bytes":"00ff"},"雪"]'.encode())
    assert canonical({(2, "z"): 1, (0, "a"): 2}) == b'[[[0,"a"],2],[[2,"z"],1]]'
    for invalid in (1.0, ValueError("closed"), object(), {1: "bad"}, D("NaN"), D("Infinity")):
        with pytest.raises((TypeError, ValueError)):
            canonical(invalid)
