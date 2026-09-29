from copy import deepcopy
from decimal import Decimal as D

import pytest

from src.core.backtest import spider_policy_protocol as protocol
from src.core.backtest.synthetic_scenario_replay import (
    _ReplayComposition,
)
from test_spider_callback_plan_digest import fixture as emission_fixture
from test_spider_configured_composition import ACCOUNT, configured
from test_spider_emission_pairing import fixture as _paired_fixture


PRODUCTS = ("PRODUCT-01", "PRODUCT-02", "PRODUCT-03")


def group(product_ids=PRODUCTS):
    plan = emission_fixture()
    members = plan["financial_items"][0]["expected_group"]["members"]
    members[0]["payload"]["rows"][0]["product_id"] = product_ids[0]
    members[0]["payload"]["rows"][1]["product_id"] = product_ids[-1]
    members[1]["payload"]["product_id"] = product_ids[1]
    members[2]["payload"]["product_id"] = product_ids[1]
    return plan["financial_items"][0]["expected_group"]


def test_configured_group_and_nested_emission_accept_membership_without_encoding_context():
    plan = emission_fixture()
    members = plan["financial_items"][0]["expected_group"]["members"]
    members[0]["payload"]["rows"][0]["product_id"] = PRODUCTS[0]
    members[0]["payload"]["rows"][1]["product_id"] = PRODUCTS[2]
    members[1]["payload"]["product_id"] = PRODUCTS[1]
    members[2]["payload"]["product_id"] = PRODUCTS[1]
    raw = protocol._emission_plan_bytes(plan, PRODUCTS)
    assert protocol._plan_group(plan["financial_items"][0]["expected_group"], PRODUCTS)
    assert raw == protocol._emission_plan_bytes(plan, ("OTHER", *PRODUCTS))
    assert protocol._emission_plan_digest(
        plan, PRODUCTS
    ) == protocol._emission_plan_digest(plan, ("OTHER", *PRODUCTS))


@pytest.mark.parametrize("product", PRODUCTS)
@pytest.mark.parametrize("path", ["mark", "execution", "intent"])
def test_configured_membership_matrix_accepts_first_middle_last_for_each_product_path(
    product, path
):
    value = group()
    members = value["members"]
    if path == "mark":
        members[0]["payload"]["rows"][0]["product_id"] = product
    elif path == "execution":
        members[1]["payload"]["product_id"] = product
    else:
        members[2]["payload"]["product_id"] = product
    assert protocol._plan_group(value, PRODUCTS)


@pytest.mark.parametrize("product", ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "P_A"])
@pytest.mark.parametrize("path", ["mark", "execution", "intent"])
def test_explicit_configured_membership_rejects_p1_products(product, path):
    value = group()
    members = value["members"]
    if path == "mark":
        members[0]["payload"]["rows"][0]["product_id"] = product
    elif path == "execution":
        members[1]["payload"]["product_id"] = product
    else:
        members[2]["payload"]["product_id"] = product
    with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
        protocol._plan_group(value, PRODUCTS)


def test_empty_context_rejects_while_none_retains_exact_p1_domains():
    _, plan = _paired_fixture()
    value = plan["financial_items"][0]["expected_group"]
    assert protocol._plan_group(value)
    with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
        protocol._plan_group(value, ())
    p1 = emission_fixture()
    assert protocol._emission_plan_bytes(p1) == protocol._emission_plan_bytes(p1, None)
    p1_group = p1["financial_items"][0]["expected_group"]
    mark = deepcopy(p1_group["members"][0]["payload"]["rows"][0])
    mark["product_id"] = "P_A"
    with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
        protocol._plan_mark(mark)
    for index in (1, 2):
        member = deepcopy(p1_group["members"][index])
        member["payload"]["product_id"] = "P_A"
        assert protocol._plan_member(member)


def source_group(product, identity):
    member = dict(kind="INTENT", stamp=dict(event_id=identity, effective_at=500, causal_parent_ids=[],
        ordering_contract_id="S_order_v1", scenario_ordinal=60), payload=dict(
            intent_id=identity, client_order_id=identity, config_id="cfg", product_id=product,
            strategy_id="S", side="LONG", order_type="LIMIT", quantity_contracts=D(1),
            limit_price=D(10), reduce_only=False, requested_at=500))
    group = dict(schema_version="scenario_group_v1", group_id=identity, account_key=ACCOUNT,
        ordering_contract_id="S_order_v1", group_effective_at=500, declared_member_count=1, members=[member])
    return dict(kind="SOURCE_GROUP", schedule_sequence=0, group=group)


@pytest.mark.parametrize("product", PRODUCTS)
def test_configured_scheduler_enqueues_products_in_configured_order(product):
    composition = _ReplayComposition("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT, configuration=configured(3))
    assert composition._enqueue(source_group(product, product))["classification"] == "PENDING"


@pytest.mark.parametrize("product", ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "P_A"])
def test_unconfigured_source_product_is_terminal_without_native_or_queue_mutation(
    product,
):
    composition = _ReplayComposition("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT, configuration=configured(3))
    before = composition._codec.inspect_state()
    item = source_group(product, "unknown")
    result = composition._enqueue(item)
    assert result["classification"] == "TERMINAL" and result["reason"] == "INVALID_SCHEMA"
    assert composition._codec.inspect_state() == before
    assert composition._queue == [] and composition._records == {}


def _seed_policy_for_product_fill(composition):
    policy = composition._policy
    product = PRODUCTS[1]
    policy.rows = [dict(name=product, active="true", leverage="4", 歩差="0.02", 單數="2",
                        hold上限="0.8", hold下限="-0.8", hold="0")]
    policy.running = True
    policy.capital = dict(earn=D(0), usdt=D(1000), avail=D(1000), total=D(1000), position=D(0))
    policy.markets[product].update(price=D(10), ctVal=D(1), lotSz=D("0.01"), minSz=D("0.01"))
    policy.replies["old"] = dict(instId=product, side="sell", px="11", sz="1", clOrdId="client-0002", state="live")


def _product_fill_delivery():
    payload = dict(order_id="fill-order", policy_client_order_id="client-0001", product_id=PRODUCTS[1],
        state="filled", side="buy", limit_price=D(10), fill_price=D(10), original_size_contracts=D(1),
        cumulative_filled_size_contracts=D(1), contract_value=D(1), execution_effective_at=500,
        commit_account_version=1, spec_version="spec-v1", rule_data_version="tier-v1")
    return dict(account_key=ACCOUNT, delivery_id="C3-DELIVERY", source_fact_id="fill-order",
        source_namespace="SOURCE", payload_kind="EXECUTION_FACT", occurrence_index=0, schedule_sequence=0,
        immutable_payload=payload, payload_digest="00" * 32, visible_at=500)


def _emission_plan_for_policy_events(delivery, events):
    expected = [dict(kind=e["kind"], event_digest=protocol._policy_event_digest(e)) for e in events]
    financial = []
    previous = delivery["visible_at"]
    for event in events:
        if event["kind"] not in ("send", "cancel"):
            continue
        digest = protocol._policy_event_digest(event)
        for order in event["orders"]:
            ordinal = len(financial)
            effective_at = previous + 1
            previous = effective_at
            is_send = event["kind"] == "send"
            payload = (dict(intent_id=f"C3-I{ordinal}", client_order_id=order["clOrdId"], config_id="cfg",
                product_id=order["instId"], strategy_id="strategy", side="LONG" if order["side"] == "buy" else "SHORT",
                order_type="LIMIT" if order["ordType"] == "limit" else "MARKET", quantity_contracts=D(order["sz"]),
                reduce_only=False, requested_at=effective_at,
                **({"limit_price": D(order["px"])} if order["ordType"] == "limit" else {})) if is_send else
                dict(targets=[dict(target_order_id=order["ordId"], reason="EXPLICIT_SCENARIO")]))
            member = dict(stamp=dict(event_id=f"C3-E{ordinal}", effective_at=effective_at, causal_parent_ids=[],
                ordering_contract_id="S_order_v1", scenario_ordinal=60 if is_send else 40),
                kind="INTENT" if is_send else "CANCEL_REQUEST", payload=payload)
            group = dict(schema_version="scenario_group_v1", group_id=f"C3-G{ordinal}", account_key=ACCOUNT,
                ordering_contract_id="S_order_v1", group_effective_at=effective_at,
                declared_member_count=1, members=[member])
            financial.append(dict(event_digest=digest, action_kind="ORDER_INTENT" if is_send else "CANCEL_REQUEST",
                                  schedule_sequence=ordinal, expected_group=group))
    return dict(delivery_id=delivery["delivery_id"], expected_policy_events=expected,
                financial_items=financial, market_requests=[])


def test_configured_delivery_dispatch_validates_and_queues_policy_groups_with_context():
    preview = _ReplayComposition("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT, configuration=configured(3))
    _seed_policy_for_product_fill(preview)
    delivery = _product_fill_delivery()
    events = list(preview._capture_callback(delivery).events)
    assert {event["kind"] for event in events} >= {"send", "cancel"}
    plan = _emission_plan_for_policy_events(delivery, events)

    evidence = []
    composition = _ReplayComposition("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT,
        callback_plans={delivery["delivery_id"]: plan},
        evidence_callback=lambda kind, key, value: evidence.append((kind, key, value)), configuration=configured(3))
    _seed_policy_for_product_fill(composition)
    assert composition._enqueue(dict(kind="DELIVERY", delivery=delivery), plan)["classification"] == "PENDING"
    result = composition._dispatch_due(500)
    assert result["classification"] == "SUCCESS"
    callback = next(value for kind, _, value in evidence if kind == "CALLBACK_RESULT")
    assert callback["outcome"] == "SUCCESS"
    events = callback["policy_events"]
    attempt = next(value for kind, _, value in evidence if kind == "DELIVERY_ATTEMPT")
    assert attempt["emission_plan_digest"] == protocol._emission_plan_digest(plan, PRODUCTS)
    assert {event["kind"] for event in events} >= {"send", "cancel"}
    assert {key[3] for key in composition._queue} == {item["expected_group"]["group_id"] for item in plan["financial_items"]}
