from copy import deepcopy
from decimal import Decimal as D, localcontext
from hashlib import sha256
from struct import pack
from typing import Any

import pytest

from src.core.backtest.spider_policy_protocol import _emission_plan_bytes, _emission_plan_digest, _plan_decimal, _plan_sequence


def text(value):
    raw = value.encode("utf-8")
    return pack(">q", len(raw)) + raw


def seq(*values):
    return b"".join(text(v) if isinstance(v, str) else pack(">q", v) for v in values)


def fixture(present=True) -> dict[str, Any]:
    account = dict(venue="V", environment="E", account="雪", **({"subaccount": "S"} if present else {}))
    stamp = dict(event_id="e", effective_at=501, causal_parent_ids=["a", "b"], ordering_contract_id="S_order_v1", scenario_ordinal=60)
    if present:
        stamp["source_sequence"] = 9
    payloads = [
        ("CONTEXT_MARKS", dict(expected_before="11" * 32, expected_after="22" * 32, rows=[
            dict(product_id="BTC-USDT-SWAP", valid_from=0, valid_to=1000, mark=D(10)),
            dict(product_id="ETH-USDT-SWAP", valid_from=1, valid_to=1001, mark=D(20))])),
        ("EXECUTION", dict(namespace="N", product_id="P_A", external_execution_id="X", order_id="O", side="LONG",
            price=D(10), quantity_contracts=D(2), liquidity="SYNTHETIC_TAKER", matching_effective_at=501,
            candidate_id="K", source_id="R", visible_at=502, expected_account_version=3, expected_order_version=4,
            spec_version="v", rule_data_version="r", **(dict(fee_asset="USDT", reported_fee=D("0.1")) if present else {}))),
        ("INTENT", dict(intent_id="I", client_order_id="C", config_id="cfg", product_id="P_A", strategy_id="s",
            side="SHORT", order_type="LIMIT", quantity_contracts=D(3), reduce_only=True, requested_at=503,
            **({"limit_price": D(11)} if present else {}))),
        ("CANCEL_REQUEST", dict(targets=[dict(target_order_id="O", reason="EXPLICIT_SCENARIO"), dict(target_order_id="P", reason="MMR_BREACH")])),
        ("CANCEL_EFFECT", dict(effects=[dict(detecting_event_id="d", target_order_id="O", reason="SPEC_MIGRATION")])),
    ]
    group = dict(schema_version="scenario_group_v1", group_id="G", account_key=account, ordering_contract_id="S_order_v1",
        group_effective_at=501, declared_member_count=5, members=[dict(stamp=deepcopy(stamp), kind=k, payload=p) for k, p in payloads])
    snapshot = dict(schema_version="snapshot_request_v1", account_key=deepcopy(account), snapshot_id="snap",
        snapshot_kind="MARKET", capture_mode="FROZEN_POLL_FIXTURE", captured_at=504,
        **(dict(fixture_key="MARKET_GOLDEN_V1", continuation_id="q") if present else {}))
    transport = dict(route="REST", operation="ORDER", client_order_id="C", code="0",
        **(dict(order_id="O", message="雪", product_id="P_A", side="buy", limit_price=D(12), size_contracts=D(4)) if present else {}))
    projection = dict(schema_version="delivery_projection_v1", reference=dict(namespace="SOURCE", fact_id="e"),
        payload_kind="TRANSPORT_ACK", occurrence_index=1, schedule_sequence=2, visible_at=505, transport=transport,
        **({"continuation_id": "q"} if present else {}))
    return dict(delivery_id="D", expected_policy_events=[dict(event_digest="33" * 32, kind="send"), dict(event_digest="44" * 32, kind="cancel")],
        financial_items=[dict(event_digest="33" * 32, action_kind="ORDER_INTENT", schedule_sequence=5, expected_group=group)],
        market_requests=[dict(event_digest="55" * 32, capture_sequence=6, snapshot_request=snapshot, delivery_projection=projection)])


def oracle(present):
    # Literal protocol reconstruction: no production encoder or fixture traversal.
    def optional(raw):
        return b"\x01" + raw if present else b"\x00"
    account = seq("V", "E", "雪") + optional(text("S"))
    stamp = seq("e", 501) + optional(pack(">q", 9)) + seq(2, "a", "b", "S_order_v1", 60)
    payloads = [
        text("CONTEXT_MARKS") + bytes.fromhex("11" * 32 + "22" * 32) + seq(2, "BTC-USDT-SWAP", 0, 1000, "10", "ETH-USDT-SWAP", 1, 1001, "20"),
        seq("EXECUTION", "N", "P_A", "X", "O", "LONG", "10", "2", "SYNTHETIC_TAKER")
        + optional(text("USDT")) + optional(text("0.1")) + seq(501, "K", "R", 502, 3, 4, "v", "r"),
        seq("INTENT", "I", "C", "cfg", "P_A", "s", "SHORT", "LIMIT", "3") + optional(text("11")) + b"\x01" + pack(">q", 503),
        seq("CANCEL_REQUEST", 2, "O", "EXPLICIT_SCENARIO", "P", "MMR_BREACH"),
        seq("CANCEL_EFFECT", 1, "d", "O", "SPEC_MIGRATION"),
    ]
    group = seq("scenario_group_v1", "G") + account + seq("S_order_v1", 501, 5, 5) + b"".join(stamp + p for p in payloads)
    snapshot = text("snapshot_request_v1") + account + seq("snap", "MARKET", "FROZEN_POLL_FIXTURE")
    snapshot += optional(text("MARKET_GOLDEN_V1")) + pack(">q", 504) + optional(text("q"))
    transport = seq("REST", "ORDER", "C") + optional(text("O")) + text("0")
    transport += b"".join(optional(text(v)) for v in ("雪", "P_A", "buy", "12", "4"))
    projection = seq("delivery_projection_v1", "SOURCE", "e", "TRANSPORT_ACK", 1, 2, 505) + optional(text("q")) + b"\x01" + transport
    return (seq("SPIDER_CALLBACK_EMISSION_PLAN_V1", "D", 2) + bytes.fromhex("33" * 32) + text("send")
        + bytes.fromhex("44" * 32) + seq("cancel", 1) + bytes.fromhex("33" * 32) + seq("ORDER_INTENT", 5) + group
        + pack(">q", 1) + bytes.fromhex("55" * 32) + pack(">q", 6) + snapshot + projection)


@pytest.mark.parametrize("present", [False, True])
def test_complete_nonempty_plan_independent_vector(present):
    plan = fixture(present)
    before = deepcopy(plan)
    expected = oracle(present)
    assert _emission_plan_bytes(plan) == expected
    assert _emission_plan_digest(plan) == sha256(expected).hexdigest()
    assert plan == before
    assert _emission_plan_digest(plan) != sha256(expected[len(text("SPIDER_CALLBACK_EMISSION_PLAN_V1")):]).hexdigest()


def leaves(value, path=()):
    if isinstance(value, dict):
        for key, child in value.items():
            yield from leaves(child, (*path, key))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            yield from leaves(child, (*path, index))
    else:
        yield path, value


def at(value, path) -> Any:
    for key in path:
        value = value[key]
    return value


def test_every_leaf_sensitivity_and_schema_validation():
    plan = fixture()
    baseline = _emission_plan_digest(plan)
    alternatives = {"P_A": "BTC-USDT-SWAP", "BTC-USDT-SWAP": "ETH-USDT-SWAP", "ETH-USDT-SWAP": "BTC-USDT-SWAP",
        "LONG": "SHORT", "SHORT": "LONG", "LIMIT": "MARKET", "REST": "WS", "ORDER": "CANCEL", "buy": "sell",
        "MARKET": "EARN", "SOURCE": "SNAPSHOT", "TRANSPORT_ACK": "EXECUTION_FACT", "ORDER_INTENT": "CANCEL_REQUEST",
        "FROZEN_POLL_FIXTURE": "OWNER_CURRENT", "S_order_v1": "S_order_v1_reverse_execution_cancel_effective",
        "send": "cancel", "cancel": "send", "EXPLICIT_SCENARIO": "MMR_BREACH", "MMR_BREACH": "RISK_SHORTFALL", "SPEC_MIGRATION": "UNSUPPORTED"}
    for path, value in leaves(plan):
        changed = deepcopy(plan)
        replacement = not value if type(value) is bool else value + 1 if isinstance(value, (int, D)) else alternatives.get(value, "changed")
        if isinstance(value, str) and len(value) == 64:
            replacement = "66" * 32
        at(changed, path[:-1])[path[-1]] = replacement
        if not (isinstance(value, str) and value not in alternatives and path[-1] in ("schema_version", "kind", "liquidity")):
            assert _emission_plan_digest(changed) != baseline, path
        if path[-1] in {"schema_version", "kind", "side", "order_type", "liquidity", "reason", "action_kind",
                        "snapshot_kind", "capture_mode", "route", "operation", "payload_kind"}:
            at(changed, path[:-1])[path[-1]] = "UNKNOWN"
            with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
                _emission_plan_bytes(changed)
        for invalid in [[], {}, None]:
            if path[-1] in OPTIONAL and invalid is None:
                continue
            at(changed, path[:-1])[path[-1]] = invalid
            with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
                _emission_plan_bytes(changed)


OPTIONAL = {"subaccount", "source_sequence", "fee_asset", "reported_fee", "limit_price", "fixture_key", "continuation_id",
            "order_id", "message", "product_id", "side", "size_contracts", "transport"}


def test_optional_absent_null_and_list_order():
    plan = fixture()
    baseline = _emission_plan_digest(plan)
    group, market = ("financial_items", 0, "expected_group"), ("market_requests", 0)
    paths = [(*group, "account_key", "subaccount"), (*market, "snapshot_request", "account_key", "subaccount")]
    paths += [(*group, "members", i, "stamp", "source_sequence") for i in range(5)]
    paths += [(*group, "members", 1, "payload", k) for k in ("fee_asset", "reported_fee")]
    paths += [(*group, "members", 2, "payload", "limit_price")]
    paths += [(*market, "snapshot_request", k) for k in ("fixture_key", "continuation_id")]
    paths += [(*market, "delivery_projection", k) for k in ("continuation_id", "transport")]
    paths += [(*market, "delivery_projection", "transport", k) for k in ("order_id", "message", "product_id", "side", "limit_price", "size_contracts")]
    for path in paths:
        changed = deepcopy(plan)
        target = at(changed, path[:-1])
        target[path[-1]] = None
        null_digest = _emission_plan_digest(changed)
        del target[path[-1]]
        assert _emission_plan_digest(changed) == null_digest != baseline
    paths = [("expected_policy_events",), ("financial_items",), ("market_requests",),
        ("financial_items", 0, "expected_group", "members"),
        ("financial_items", 0, "expected_group", "members", 0, "stamp", "causal_parent_ids"),
        (*group, "members", 0, "payload", "rows"),
        (*group, "members", 4, "payload", "effects"),
        ("financial_items", 0, "expected_group", "members", 3, "payload", "targets")]
    for path in paths:
        changed = deepcopy(plan)
        items = at(changed, path)
        if len(items) == 1:
            items.append(deepcopy(items[0]))
            items[-1]["detecting_event_id" if path[-1] == "effects" else "event_digest"] = "77" * 32
        original = _emission_plan_digest(changed)
        items.reverse()
        assert _emission_plan_digest(changed) != original


def test_numeric_normalization_bounds_and_raw_hash_errors():
    with localcontext() as context:
        context.prec = 2
        for value, expected in [("-0.000", "0"), ("1E+3", "1000"), ("123456789.1234500", "123456789.12345")]:
            assert _plan_decimal(D(value)) == text(expected)
    assert _plan_sequence((1 << 63) - 1) == pack(">q", (1 << 63) - 1)
    plan = fixture()
    path = ("financial_items", 0, "expected_group", "members", 2, "payload")
    row = at(plan, path)
    row["quantity_contracts"] = D("3.000")
    with localcontext() as context:
        context.prec = 2
        assert _emission_plan_bytes(plan) == oracle(True)
    for value in [True, "3", 3.0, D("NaN"), D("Infinity")]:
        row["quantity_contracts"] = value
        with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
            _emission_plan_bytes(plan)
    for value in [-1, True, "5", 5.0, 1 << 63, -(1 << 63) - 1]:
        changed = fixture()
        changed["financial_items"][0]["schedule_sequence"] = value
        with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
            _emission_plan_bytes(changed)


def test_nested_object_schema_and_boolean_representation():
    plan = fixture()
    paths = {path[:index] for path, _ in leaves(plan) for index in range(len(path))}
    for path in paths:
        original = at(plan, path)
        if not isinstance(original, dict):
            continue
        for field in [*original, "extra"]:
            changed = deepcopy(plan)
            row = at(changed, path)
            if field == "extra":
                row[field] = "bad"
            else:
                del row[field]
            try:
                _emission_plan_bytes(changed)
            except ValueError as exc:
                assert str(exc) == "POLICY_EMISSION_MISMATCH"
            else:
                assert field in OPTIONAL
    row = plan["financial_items"][0]["expected_group"]["members"][2]["payload"]
    for value in [0, 1, "true"]:
        row["reduce_only"] = value
        with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
            _emission_plan_bytes(plan)
    for value in ["aa" * 31, "AA" * 32, "gg" * 32]:
        changed = fixture()
        changed["expected_policy_events"][0]["event_digest"] = value
        with pytest.raises(ValueError, match="^POLICY_EMISSION_MISMATCH$"):
            _emission_plan_bytes(changed)
