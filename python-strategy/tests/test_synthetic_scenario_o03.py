from decimal import Decimal as D
from typing import cast

import pytest

from src.core.backtest import synthetic_scenario_codec as c

KEY: c.Account = {"venue": "okx-scenario", "environment": "test", "account": "A"}


def group(name: str, at: int, members: list[tuple[str, str, int, dict]]) -> c.Group:
    return cast(c.Group, {"schema_version": "scenario_group_v1", "group_id": name,
        "account_key": KEY, "ordering_contract_id": "S_order_v1", "group_effective_at": at,
        "declared_member_count": len(members), "members": [
            {"kind": kind, "stamp": {"event_id": event, "effective_at": at,
                "causal_parent_ids": [], "ordering_contract_id": "S_order_v1",
                "scenario_ordinal": ordinal}, "payload": payload}
            for kind, event, ordinal, payload in members]})


def plan() -> list[c.Group]:
    return [
        group("O03-X1", 601, [("EXECUTION", "O03-X1", 30, {
            "namespace": "synthetic-v1", "product_id": "P_A", "external_execution_id": "O03-X1",
            "order_id": "O_OLD1", "side": "LONG", "price": D("10"), "quantity_contracts": D("1"),
            "liquidity": "SYNTHETIC_TAKER", "matching_effective_at": 601,
            "candidate_id": "candidate-O03-X1", "source_id": "source-O03-X1", "visible_at": 601,
            "expected_account_version": 0, "expected_order_version": 0,
            "spec_version": "gt03-spec-v1", "rule_data_version": "gt03-rule-v1"})]),
        group("O03-C1", 602, [
            ("CANCEL_EFFECT", "O03-C1-EFFECT", 50, {"effects": [
                {"detecting_event_id": "O03-C1-EFFECT", "target_order_id": "O_OLD1", "reason": "SPEC_MIGRATION"}]})]),
        group("O03-C2-REJECT", 603, [("CANCEL_REQUEST", "O03-C2-REJECT", 40, {"targets": [
            {"target_order_id": order, "reason": "EXPLICIT_SCENARIO"} for order in ("O_OLD1", "O_OLD2")]})]),
        group("O03-N1", 604, [("INTENT", "O03-N1", 60, {
            "intent_id": "O03-N1-I", "client_order_id": "O03-N1-C", "config_id": "scenario-v1",
            "product_id": "P_A", "strategy_id": "o03-grid", "side": "LONG", "order_type": "LIMIT",
            "quantity_contracts": D("100"), "limit_price": D("10"), "reduce_only": False, "requested_at": 604})]),
    ]


def snapshot(codec: c.ScenarioCodec, kind: c.Kind, at: int) -> dict:
    fact = codec.capture_snapshot({"schema_version": "snapshot_request_v1", "account_key": KEY,
        "snapshot_id": f"O03-{at}-{kind}", "snapshot_kind": kind, "capture_mode": "OWNER_CURRENT", "captured_at": at})
    assert fact.get("captured_account_version") == codec.inspect_state()["account_version"]
    return cast(dict, fact["immutable_payload"])


def test_o03_retains_non_atomic_prefix_and_duplicate_results():
    codec = c.ScenarioCodec("SYNTHETIC_P1_O03_V1", KEY)
    initial = codec.inspect_state()
    assert initial["profile_id"] == "SYNTHETIC_P1_O03_V1"
    assert snapshot(codec, "TRADING", 600)["available_equity"] == D("950")
    assert snapshot(codec, "POSITIONS", 600)["rows"][0]["position_contracts"] == D("5")
    previous = initial
    for request, version, reason, available in zip(plan(), [1, 2, 2, 2],
            [None, None, "CANCEL_REQUEST_TOO_LATE", "CAPACITY_EXCEEDED"], ["960", "970", "970", "970"], strict=True):
        result = codec.apply_group(request)
        current = codec.inspect_state()
        assert result["classification"] == ("COMMITTED" if reason is None else "REJECTED")
        assert (result["account_version_before"], result["account_version_after"]) == (previous["account_version"], version)
        assert current["account_version"] == version
        assert (current["cash"], current["gross_realized"], current["total_fees"]) == (D("1000"), D("0"), D("0"))
        assert current["gate"] == "RUNNING" and current["lifecycle"] == "RISK_STABLE"
        if reason:
            assert result["rejections"][0]["reason"] == reason
            for field in ("positions_digest", "orders_digest", "reservations_digest"):
                assert current[field] == previous[field]
        assert codec.apply_group(request) == result
        assert codec.inspect_state() == current
        at = request["group_effective_at"]
        assert snapshot(codec, "TRADING", at)["available_equity"] == D(available)
        position = snapshot(codec, "POSITIONS", at)["rows"][0]
        assert (position["position_contracts"], position["last_price"], position["notional_usd"]) == (D("6"), D("10"), D("60"))
        orders = snapshot(codec, "OPEN_ORDERS", at)["rows"]
        assert [row["order_id"] for row in orders] == (["O_OLD1", "O_OLD2"] if version == 1 else ["O_OLD2"])
        if version == 1:
            assert orders[0]["state"] == "partially_filled"
            assert orders[0]["cumulative_filled_size_contracts"] == D("1")
        previous = current
    assert snapshot(codec, "TRADING", 604)["equity"] == D("1000")


@pytest.mark.parametrize("field,value", [
    ("product_id", "BTC-USDT-SWAP"), ("reduce_only", True), ("quantity_contracts", D("0")),
    ("quantity_contracts", D("1.5")), ("limit_price", D("0")), ("limit_price", D("10.5")), ("order_type", "MARKET"),
])
def test_o03_invalid_intents_never_change_financial_components(field, value):
    codec = c.ScenarioCodec("SYNTHETIC_P1_O03_V1", KEY)
    request = plan()[-1]
    cast(dict, request["members"][0]["payload"])[field] = value
    before = codec.inspect_state()
    if field == "order_type":
        with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
            codec.apply_group(request)
    else:
        result = codec.apply_group(request)
        assert result["classification"] != "COMMITTED"
    after = codec.inspect_state()
    for key in ("account_version", "cash", "gross_realized", "total_fees", "positions_digest", "orders_digest", "reservations_digest"):
        assert after[key] == before[key]


@pytest.mark.parametrize("case,reason", [
    ("non-o03", "CANCEL_EFFECT_BEFORE_REQUEST"), ("request", "INVALID_SCENARIO_GROUP"),
    ("multi", "CANCEL_EFFECT_BEFORE_REQUEST"), ("detecting", "CANCEL_EFFECT_BEFORE_REQUEST"),
    ("pending", "CANCEL_ACTION_CONFLICT"), ("terminal", "CANCEL_EFFECT_BEFORE_REQUEST"),
])
def test_direct_migration_route_is_closed(case, reason):
    profile: c.Profile = "SYNTHETIC_BTC_ETH_V1" if case == "non-o03" else "SYNTHETIC_P1_O03_V1"
    codec = c.ScenarioCodec(profile, KEY)
    request = group("direct", 604, [("CANCEL_EFFECT", "direct", 50, {"effects": [
        {"detecting_event_id": "direct", "target_order_id": "O1" if case == "non-o03" else "O_OLD1", "reason": "SPEC_MIGRATION"}]})])
    payload = cast(dict, request["members"][0]["payload"])
    if case == "request":
        request = group("direct", 604, [("CANCEL_REQUEST", "direct", 40, {"targets": [
            {"target_order_id": "O_OLD1", "reason": "SPEC_MIGRATION"}]})])
    elif case == "multi":
        payload["effects"].append({"detecting_event_id": "direct", "target_order_id": "O_OLD2", "reason": "SPEC_MIGRATION"})
    elif case == "detecting":
        payload["effects"][0]["detecting_event_id"] = "other"
    elif case == "pending":
        codec.apply_group(group("pending", 601, [("CANCEL_REQUEST", "pending", 40, {"targets": [
            {"target_order_id": "O_OLD1", "reason": "EXPLICIT_SCENARIO"}]})]))
    elif case == "terminal":
        for prefix in plan()[:2]:
            assert codec.apply_group(prefix)["classification"] == "COMMITTED"
    before = codec.inspect_state()
    result = codec.apply_group(request)
    assert result["classification"] == "FAULT" and result.get("failure") == reason
    after = codec.inspect_state()
    for key in ("account_version", "cash", "total_fees", "positions_digest", "orders_digest", "reservations_digest"):
        assert after[key] == before[key]
