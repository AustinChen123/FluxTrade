from copy import deepcopy
from decimal import Decimal as D
from typing import Any, cast

import pytest

from src.core.backtest import spider_policy_protocol as protocol
from src.core.backtest.synthetic_scenario_codec import Account
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition, _validate_emission_plan

ACCOUNT = dict(venue="okx-scenario", environment="test", account="A")
DELIVERY = dict(delivery_id="D", visible_at=500)


def fixture() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    orders = [dict(instId="BTC-USDT-SWAP", instIdCode=1, tdMode="cross", clOrdId="C", tag="",
                   side=side, ordType=kind, px=px, sz="2") for side, kind, px in
              [("buy", "limit", "10"), ("sell", "market", "")]]
    events = [dict(at_ms=500, kind="send", operation="send", route="REST", orders=orders),
              dict(at_ms=500, kind="request_market"),
              dict(at_ms=500, kind="cancel", operation="cancel", route="WS",
                   orders=[dict(ordId="O", instId="BTC-USDT-SWAP", instIdCode=1)])]
    expected = [dict(kind=e["kind"], event_digest=protocol._policy_event_digest(e)) for e in events]
    financial = []
    for i in range(3):
        stamp = dict(event_id=f"e{i}", effective_at=501+i, causal_parent_ids=[],
                     ordering_contract_id="S_order_v1", scenario_ordinal=60 if i < 2 else 40)
        payload = (dict(intent_id=f"I{i}", client_order_id="C", config_id="cfg", product_id="BTC-USDT-SWAP",
                        strategy_id="s", side="LONG" if i == 0 else "SHORT", order_type="LIMIT" if i == 0 else "MARKET",
                        quantity_contracts=D(2), reduce_only=False, requested_at=501+i,
                        **({"limit_price": D(10)} if i == 0 else {})) if i < 2 else
                   dict(targets=[dict(target_order_id="O", reason="EXPLICIT_SCENARIO")]))
        group = dict(schema_version="scenario_group_v1", group_id=f"G{i}", account_key=deepcopy(ACCOUNT),
                     ordering_contract_id="S_order_v1", group_effective_at=501+i, declared_member_count=1,
                     members=[dict(stamp=stamp, kind="INTENT" if i < 2 else "CANCEL_REQUEST", payload=payload)])
        financial.append(dict(event_digest=expected[0 if i < 2 else 2]["event_digest"],
                              action_kind="ORDER_INTENT" if i < 2 else "CANCEL_REQUEST",
                              schedule_sequence=i, expected_group=group))
    market = dict(event_digest=expected[1]["event_digest"], capture_sequence=0,
                  snapshot_request=dict(schema_version="snapshot_request_v1", account_key=deepcopy(ACCOUNT),
                      snapshot_id="S", snapshot_kind="MARKET", capture_mode="FROZEN_POLL_FIXTURE",
                      fixture_key="MARKET_GOLDEN_V1", captured_at=501),
                  delivery_projection=dict(schema_version="delivery_projection_v1", reference=dict(namespace="SNAPSHOT", fact_id="S"),
                      payload_kind="MARKET_SNAPSHOT", occurrence_index=0, schedule_sequence=0, visible_at=501))
    return events, dict(delivery_id="D", expected_policy_events=expected, financial_items=financial, market_requests=[market])


def check(events, plan, reason=None):
    before = deepcopy((ACCOUNT, DELIVERY, events, plan))
    if reason:
        with pytest.raises(RuntimeError if reason == "injected" else ValueError, match=f"^{reason}$"):
            _validate_emission_plan(ACCOUNT, DELIVERY, events, plan)
        result = None
    else:
        result = _validate_emission_plan(ACCOUNT, DELIVERY, events, plan)
    assert (ACCOUNT, DELIVERY, events, plan) == before
    return result


@pytest.mark.parametrize("projection_time", [501, 502])
def test_mixed_batch_order_detachment_and_no_composition_mutation(projection_time):
    replay = _ReplayComposition("SYNTHETIC_BTC_ETH_V1", cast(Account, ACCOUNT))
    owner, policy = replay._codec.inspect_state(), deepcopy(vars(replay._policy))
    events, plan = fixture()
    plan["market_requests"][0]["delivery_projection"]["visible_at"] = projection_time
    result = check(events, plan)
    assert result is not None
    assert result == tuple([*plan["financial_items"][:2], plan["market_requests"][0], plan["financial_items"][2]])
    result[0]["expected_group"]["members"][0]["payload"]["client_order_id"] = "detached"
    assert plan["financial_items"][0]["expected_group"]["members"][0]["payload"]["client_order_id"] == "C"
    plan["market_requests"][0]["snapshot_request"]["snapshot_id"] = "changed"
    assert result[2]["snapshot_request"]["snapshot_id"] == "S"
    assert replay._codec.inspect_state() == owner and vars(replay._policy) == policy


@pytest.mark.parametrize("path,value", [
    (("delivery_id",), "wrong"), (("financial_items", 0, "event_digest"), "00"*32),
    (("financial_items", 0, "action_kind"), "CANCEL_REQUEST"), (("financial_items", 0, "schedule_sequence"), -1),
    (("financial_items", 0, "schedule_sequence"), True),
    *[(("financial_items", 0, "expected_group", key), value) for key, value in
      [("account_key", dict(ACCOUNT, account="B")), ("declared_member_count", 2), ("members", []),
       ("schema_version", "wrong"), ("ordering_contract_id", "S_order_v1_reverse_execution_cancel_effective"),
       ("group_effective_at", 500)]],
    *[(("financial_items", 0, "expected_group", "members", 0, "stamp", key), value) for key, value in
      [("scenario_ordinal", 40), ("effective_at", 500), ("effective_at", 499), ("effective_at", 502),
       ("source_sequence", True), ("event_id", "\ud800")]],
    *[(("financial_items", 0, "expected_group", "members", 0, "payload", key), value) for key, value in
      [("client_order_id", "X"), ("product_id", "ETH-USDT-SWAP"), ("side", "SHORT"),
       ("quantity_contracts", D(3)), ("order_type", "MARKET"), ("limit_price", D(11)), ("extra", 1)]],
    (("financial_items", 2, "expected_group", "members", 0, "payload", "targets"), []),
    *[(("financial_items", 2, "expected_group", "members", 0, "payload", "targets", 0, key), value)
      for key, value in [("target_order_id", "X"), ("reason", "MMR_BREACH")]],
    *[(("market_requests", 0, "snapshot_request", key), value) for key, value in
      [("account_key", dict(ACCOUNT, account="B")), ("snapshot_kind", "TRADING"), ("capture_mode", "OWNER_CURRENT"),
       ("fixture_key", "EARN_GOLDEN_V1"), ("continuation_id", "Q"), ("captured_at", 500), ("captured_at", 499)]],
    *[(("market_requests", 0, "delivery_projection", key), value) for key, value in
      [("reference", dict(namespace="SOURCE", fact_id="S")), ("reference", dict(namespace="SNAPSHOT", fact_id="X")),
       ("payload_kind", "TRADING_SNAPSHOT"), ("continuation_id", "Q"), ("visible_at", 500), ("visible_at", 2**63),
       ("transport", dict(route="REST", operation="ORDER", client_order_id="C", code="0"))]],
    (("market_requests", 0, "capture_sequence"), -1), (("market_requests", 0, "extra"), 1),
    (("market_requests", 0, "event_digest"), "00"*32),
])
def test_mapping_boundaries_preserve_inputs(path, value):
    events, plan = fixture()
    target = plan
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value
    check(events, plan, "POLICY_EMISSION_MISMATCH")


@pytest.mark.parametrize("case", ["count", "kind", "digest", "financial_missing", "market_missing", "financial_wrong", "market_wrong", "equal", "backward", "overflow", "runtime"])
def test_precedence_and_strict_financial_time(case, monkeypatch):
    events, plan = fixture()
    if case in ("count", "kind", "digest"):
        plan["financial_items"] = []
        if case == "count":
            plan["expected_policy_events"].pop()
        else:
            plan["expected_policy_events"][2]["kind" if case == "kind" else "event_digest"] = "wrong"
    elif case.startswith("financial"):
        plan["market_requests"] = [None]
        plan["financial_items"] = [] if case.endswith("missing") else [None]
    elif case.startswith("market"):
        events = events[1:2] + events[:1] + events[2:]
        plan["expected_policy_events"] = plan["expected_policy_events"][1:2] + plan["expected_policy_events"][:1] + plan["expected_policy_events"][2:]
        plan["market_requests"] = [] if case.endswith("missing") else [None]
        plan["financial_items"] = [None]
    elif case in ("overflow", "runtime"):
        def fail(_):
            raise (OverflowError if case == "overflow" else RuntimeError)("injected")
        monkeypatch.setattr(protocol, "_plan_snapshot", fail)
    else:
        group = plan["financial_items"][1]["expected_group"]
        group["group_effective_at"] = group["members"][0]["stamp"]["effective_at"] = 501 if case == "equal" else 500
    check(events, plan, "injected" if case == "runtime" else "MISSING_NEXT_EVENT_STAMP" if case.endswith("missing") else "POLICY_EMISSION_MISMATCH")


def test_ignored_events_and_unused_tail_precedence():
    events = [dict(at_ms=500, kind=k) for k in ("request_earn", "check_websocket")]
    events += [dict(at_ms=500, kind="alert", reason="daily_stop"),
               dict(at_ms=500, kind="console_only", operation="send", route="REST", orders=fixture()[0][0]["orders"]),
               dict(at_ms=500, kind="unsupported_source_path", reason="non_grid_fill_enters_orderFilled",
                    order=dict(instId="BTC-USDT-SWAP", side="buy", px="", clOrdId="C", state="filled", sz="2", accFillSz="2"))]
    plan: dict[str, Any] = dict(delivery_id="D", expected_policy_events=[dict(kind=e["kind"], event_digest=protocol._policy_event_digest(e)) for e in events],
                financial_items=[], market_requests=[])
    assert check(events, plan) == ()
    class Unvisited(list):
        def __len__(self):
            raise AssertionError("market tail inspected before unused financial")
    plan.update(financial_items=[None], market_requests=Unvisited())
    check(events, plan, "POLICY_EMISSION_MISMATCH")
    plan.update(financial_items=[], market_requests=[None])
    check(events, plan, "POLICY_EMISSION_MISMATCH")


@pytest.mark.parametrize("case", ["members", "role", "cancel_targets", "stamp_contract", "missing", "market_price"])
def test_complete_consumed_shapes_and_roles(case):
    events, plan = fixture()
    group = plan["financial_items"][0]["expected_group"]
    if case == "members":
        group["members"] *= 2
        group["declared_member_count"] = 2
    elif case == "role":
        group["members"][0].update(kind="CANCEL_REQUEST", payload=dict(targets=[]))
    elif case == "cancel_targets":
        plan["financial_items"][2]["expected_group"]["members"][0]["payload"]["targets"] *= 2
    elif case == "stamp_contract":
        group["members"][0]["stamp"]["ordering_contract_id"] = "S_order_v1_reverse_execution_cancel_effective"
    elif case == "missing":
        del group["members"][0]["payload"]["config_id"]
    else:
        plan["financial_items"][1]["expected_group"]["members"][0]["payload"]["limit_price"] = D(10)
    check(events, plan, "POLICY_EMISSION_MISMATCH")
