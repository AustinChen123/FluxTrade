import json
from copy import deepcopy
from decimal import Decimal, localcontext
from typing import cast

import pytest

from src.core.backtest import synthetic_scenario_codec as c

KEY: c.Account = {"venue": "okx-scenario", "environment": "test", "account": "A"}
# Independent protocol field inventory: never derive coverage from codec dispatch.
MONEY_FIELDS = (
    "mark price quantity_contracts reported_fee limit_price size_contracts fill_price "
    "original_size_contracts cumulative_filled_size_contracts contract_value lot_size "
    "minimum_size price_increment high_low_ratio earn equity available_equity "
    "position_contracts last_price notional_usd cash gross_realized total_fees"
).split()
INTEGER_FIELDS = (
    "source_sequence effective_at scenario_ordinal valid_from valid_to matching_effective_at "
    "visible_at expected_account_version expected_order_version requested_at group_effective_at "
    "declared_member_count occurrence_index schedule_sequence captured_at captured_account_version "
    "snapshot_as_of snapshot_version execution_effective_at commit_account_version instrument_code "
    "created_at account_version account_version_before account_version_after"
).split()
OPTIONAL_FIELDS = (
    "subaccount source_sequence fee_asset reported_fee limit_price fixture_key continuation_id "
    "transport order_id message product_id side size_contracts"
).split()


def test_every_financial_field_is_exact_and_not_context_rounded():
    with localcontext() as context:
        context.prec = 2
        for original, expected in [("123456789.1234500", "123456789.12345"), ("-0.000", "0"), ("1E+4", "10000"), ("-1E-8", "-0.00000001")]:
            for field in MONEY_FIELDS:
                raw = c._encode({field: Decimal(original)})
                assert json.loads(raw) == {field: expected}
                assert c._decode(raw) == {field: Decimal(original)}
    for invalid in [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity"), 1.0, "1", 1, True]:
        with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
            c._encode({"price": invalid})
    for field in INTEGER_FIELDS:
        assert json.loads(c._encode({field: 12}))[field] == 12
        for invalid in [True, "12", Decimal("12"), 12.0]:
            with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
                c._encode({field: invalid})


def test_all_request_variants_options_and_outputs_are_detached():
    payloads = [
        ("CONTEXT_MARKS", {"rows": [{"mark": Decimal("5")}]}),
        ("EXECUTION", {"price": Decimal("5"), "quantity_contracts": Decimal("2"), "reported_fee": None}),
        ("INTENT", {"quantity_contracts": Decimal("2"), "limit_price": Decimal("5"), "reduce_only": True}),
        ("CANCEL_REQUEST", {"targets": [{"target_order_id": "O", "reason": "MMR_BREACH"}]}),
        ("CANCEL_EFFECT", {"effects": [{"detecting_event_id": "E", "target_order_id": "O", "reason": "SPEC_MIGRATION"}]}),
    ]
    for kind, payload in payloads:
        original = {"kind": kind, "payload": payload}
        encoded = json.loads(c._encode(original))
        assert encoded["kind"] == kind
        if kind == "EXECUTION":
            assert "reported_fee" not in encoded["payload"]
        assert original["payload"] is payload
    for field in OPTIONAL_FIELDS:
        assert json.loads(c._encode({field: None})) == {}
    variants = [
        {"markets": [{"price": "10", "contract_value": "1", "instrument_code": 7}]},
        {"outcome": "SUCCESS", "earn": "0"},
        {"outcome": "SUCCESS", "equity": "10", "available_equity": "8"},
        {"outcome": "SUCCESS", "rows": [{"position_contracts": "-2", "notional_usd": "20", "last_price": "10"}]},
        {"outcome": "SUCCESS", "rows": [{"limit_price": "10", "original_size_contracts": "2", "cumulative_filled_size_contracts": "1", "created_at": 500}]},
        {"outcome": "FAILURE", "reason": "SYNTHETIC_FAILURE"},
        {"limit_price": "10", "fill_price": "9", "original_size_contracts": "2", "commit_account_version": 1},
        {"route": "REST", "operation": "ORDER", "side": "buy", "size_contracts": "2", "message": "雪"},
    ]
    for payload in variants:
        raw = json.dumps({"immutable_payload": payload})
        first = c._decode(raw)
        assert first == c._decode(raw)
        assert json.loads(c._encode(first)) == json.loads(raw)


def test_real_native_routes_preserve_receipts_and_isolate_accounts():
    codec = c.ScenarioCodec("SYNTHETIC_GOLDEN_CANCEL_V1", KEY)
    group = cast(c.Group, {"schema_version": "scenario_group_v1", "group_id": "G", "account_key": KEY,
        "ordering_contract_id": "S_order_v1", "group_effective_at": 500, "declared_member_count": 1,
        "members": [{"kind": "INTENT", "stamp": {"event_id": "A", "effective_at": 500, "causal_parent_ids": [],
            "ordering_contract_id": "S_order_v1", "scenario_ordinal": 60}, "payload": {"intent_id": "I", "client_order_id": "C",
            "config_id": "scenario-v1", "product_id": "P_A", "strategy_id": "S", "side": "LONG", "order_type": "LIMIT",
            "quantity_contracts": Decimal("10"), "limit_price": Decimal("10"), "reduce_only": False, "requested_at": 500}}]})
    original = codec.apply_group(group)
    assert original["classification"] == "COMMITTED"
    assert (original["account_version_before"], original["account_version_after"]) == (0, 1)
    assert codec.apply_group(group) == original
    request: c.SnapshotRequest = {"schema_version": "snapshot_request_v1", "account_key": KEY, "snapshot_id": "S",
        "snapshot_kind": "TRADING", "capture_mode": "OWNER_CURRENT", "captured_at": 500, "continuation_id": None}
    fact = codec.capture_snapshot(request)
    assert fact["immutable_payload"] == {"outcome": "SUCCESS", "equity": Decimal("1000"), "available_equity": Decimal("900")}
    projection: c.Projection = {"schema_version": "delivery_projection_v1", "reference": {"namespace": "SNAPSHOT", "fact_id": "S"},
        "payload_kind": "TRADING_SNAPSHOT", "occurrence_index": 0, "schedule_sequence": 0, "visible_at": 500}
    before = codec.inspect_state()
    delivery = codec.build_delivery(projection)
    assert delivery["immutable_payload"] == fact["immutable_payload"]
    delivery["immutable_payload"] = {"outcome": "SUCCESS", "earn": Decimal("999")}
    assert codec.build_delivery(projection)["immutable_payload"] == fact["immutable_payload"]
    assert codec.inspect_state() == before
    assert codec.__slots__ == ("_session",)
    other = c.ScenarioCodec("SYNTHETIC_GOLDEN_CANCEL_V1", {**KEY, "account": "B"})
    with pytest.raises(ValueError, match="^ACCOUNT_KEY_MISMATCH$"):
        other.capture_snapshot(request)
    with pytest.raises(LookupError, match="^UNKNOWN_RECEIPT_REFERENCE$"):
        other.build_delivery(projection)


def test_source_sequence_optional_integer_boundary():
    stamp: c.Stamp = {"event_id": "E", "effective_at": 500, "causal_parent_ids": [],
        "ordering_contract_id": "S_order_v1", "scenario_ordinal": 60}
    assert json.loads(c._encode(stamp)) == stamp
    assert json.loads(c._encode({**stamp, "source_sequence": None})) == stamp
    assert json.loads(c._encode({**stamp, "source_sequence": 7})) == {**stamp, "source_sequence": 7}
    with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
        c._encode({**stamp, "source_sequence": True})


def _configured_input(count: int = 1) -> dict[str, object]:
    names = ["WIRE-Z", "WIRE-A", "WIRE-M"][:count]
    products = []
    for index, name in enumerate(names, start=1):
        products.append({
            "product_id": name,
            "instrument_code": index,
            "taker_fee_rate": Decimal("0.0010"),
            "liquidation_fee_rate": Decimal("0.0020"),
            "specs": [{"version": "s1", "valid_from": 0, "valid_to": None,
                "contract_value": Decimal("1"), "multiplier": Decimal("1"),
                "price_tick": Decimal("1"), "quantity_step": Decimal("0.5"),
                "minimum_quantity": Decimal("0.5")}],
            "tiers": [{"version": "t1", "valid_from": 0, "valid_to": None,
                "rows": [{"minimum_contracts": Decimal("0"), "maximum_contracts": Decimal("10"),
                    "mmr": Decimal("0.005"), "imr": Decimal("0.1"), "max_leverage": Decimal("10")}]}],
            "marks": [{"valid_from": 0, "valid_to": 100, "mark": Decimal("1")}],
        })
    return {"schema_version": "synthetic_multi_product_config_v1", "config_id": "codec-config",
        "seed_effective_at": 50, "cash": Decimal("10"), "leverage": Decimal("1"),
        "products": products, "positions": [], "orders": []}


@pytest.mark.parametrize("count", [1, 3])
def test_configured_constructor_transports_native_owned_config_and_snapshots_input(count):
    configuration = _configured_input(count)
    before_encoding = deepcopy(configuration)
    original = json.loads(c._encode_configuration(configuration))
    assert configuration == before_encoding
    codec = c.ScenarioCodec("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", KEY, configuration)
    configuration["config_id"] = "mutated-after-construction"
    cast(list[dict[str, object]], configuration["products"])[0]["product_id"] = "CHANGED"
    state = codec.inspect_state()
    assert state["profile_id"] == "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1"
    assert state["config_id"] == "codec-config"
    assert [row["product_id"] for row in original["products"]] == [
        "WIRE-Z", "WIRE-A", "WIRE-M"
    ][:count]
    assert codec.__slots__ == ("_session",)
    if count == 3:
        reversed_configuration = _configured_input(count)
        cast(list[dict[str, object]], reversed_configuration["products"]).reverse()
        reversed_state = c.ScenarioCodec(
            "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", KEY, reversed_configuration
        ).inspect_state()
        assert reversed_state["valuation_context_id"] != state["valuation_context_id"]


def test_configured_constructor_preserves_p1_two_argument_call_and_native_validation(monkeypatch):
    calls = []

    class Session:
        def __init__(self, *args):
            calls.append(args)

    monkeypatch.setattr(c._native, "_SyntheticScenarioReplaySession", Session)
    c.ScenarioCodec("SYNTHETIC_BTC_ETH_V1", KEY)
    assert len(calls[0]) == 2
    assert calls[0] == ("SYNTHETIC_BTC_ETH_V1", c._encode(KEY))
    c.ScenarioCodec("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", KEY, _configured_input())
    assert len(calls[1]) == 3
    assert calls[1][2] == c._encode_configuration(_configured_input())
    # Restore the real binding for native-owned construction/validation checks.
    monkeypatch.undo()
    with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
        c.ScenarioCodec("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", KEY)
    for profile, config in [
        ("SYNTHETIC_BTC_ETH_V1", _configured_input()),
        ("unknown", _configured_input()),
        ("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", {}),
    ]:
        with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
            c.ScenarioCodec(profile, KEY, config)


@pytest.mark.parametrize("invalid", [
    {"x": 1.25}, {"x": Decimal("NaN")}, {1: "non-string key"},
    {"x": (1, 2)}, {"x": object()}, ["not a mapping"],
])
def test_configured_transport_rejects_noncanonical_python_values(invalid):
    with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
        c._encode_configuration(cast(dict[str, object], invalid))


def test_configured_transport_preserves_mapping_and_list_order_and_nested_nulls():
    value = {"second": [Decimal("1.2500"), None], "first": {"enabled": True}}
    encoded = c._encode_configuration(value)
    assert encoded == '{"second":["1.25",null],"first":{"enabled":true}}'
    assert value == {"second": [Decimal("1.2500"), None], "first": {"enabled": True}}


@pytest.mark.parametrize("fixture,kind", [
    ("MARKET_GOLDEN_V1", "MARKET"), ("POLL_EARN_ZERO", "EARN"),
    ("POLL_Q1_S1", "TRADING"), ("POLL_Q2_S2", "TRADING"),
    ("POLL_POSITIONS_EMPTY", "POSITIONS"), ("POLL_OPEN_ORDERS_EMPTY", "OPEN_ORDERS"),
    ("POLL_EARN_FAILURE", "EARN"), ("POLL_TRADING_FAILURE", "TRADING"),
    ("POLL_POSITIONS_FAILURE", "POSITIONS"), ("POLL_OPEN_ORDERS_FAILURE", "OPEN_ORDERS"),
])
def test_every_frozen_output_variant_uses_native_payload(fixture, kind):
    codec = c.ScenarioCodec("SYNTHETIC_GOLDEN_CANCEL_V1", KEY)
    before = codec.inspect_state()
    fact = codec.capture_snapshot(cast(c.SnapshotRequest, {"schema_version":"snapshot_request_v1", "account_key":KEY,
        "snapshot_id":"F", "snapshot_kind":kind, "capture_mode":"FROZEN_POLL_FIXTURE", "fixture_key":fixture, "captured_at":500}))
    assert "captured_account_version" not in fact
    payload = fact["immutable_payload"]
    if fixture.endswith("FAILURE"):
        assert payload == {"outcome":"FAILURE", "reason":"SYNTHETIC_FAILURE"}
    elif kind == "MARKET":
        row = cast(c.MarketSnapshot, payload)["markets"][0]
        assert row == {"product_id": "P_A", "price": Decimal("10"),
            "contract_value": Decimal("1"), "lot_size": Decimal("1"), "minimum_size": Decimal("1"),
            "price_increment": Decimal("1"), "high_low_ratio": Decimal("0.1"),
            "state": "live", "instrument_code": 1}
        assert type(row["instrument_code"]) is int
    assert codec.inspect_state() == before
