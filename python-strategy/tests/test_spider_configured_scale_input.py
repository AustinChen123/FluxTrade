"""Causal evidence for the frozen section-6 input through the real codec."""

import hashlib
import json
from decimal import Decimal
from typing import Any, cast

from src.core.backtest import spider_configured_scale_input as scale
from src.core.backtest import spider_scenario_plans
from src.core.backtest import synthetic_scenario_codec as codec
from src.core.backtest.spider_run_artifacts import canonical_bytes

_PRODUCTS = [
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "XRP-USDT-SWAP",
    "DOGE-USDT-SWAP",
    "ARB-USDT-SWAP",
    "OP-USDT-SWAP",
    "NEAR-USDT-SWAP",
    "APT-USDT-SWAP",
    "SUI-USDT-SWAP",
    "ADA-USDT-SWAP",
]
_ORDER_ID = "scenario-order-v1:[17, 9a, 5a, ba, 18, 13, 08, a2, cd, 67, 71, 3c, ff, 56, 81, a4, 88, 71, 5d, 81, ea, 11, 59, 12, 5e, 82, 2e, 43, 5f, 4d, a2, 26]"
_CONFIGURATION_SHA256 = (
    "807054044bdd182274509535ecf8bbc4598f00b6a92b598c6228129a6ff11b70"
)
_PLAN_INPUT_SHA256 = "a0be02b617817716be635b60c0d6332e759341a75040fe8ed83e9d8249f93c6f"


def _input() -> dict[str, Any]:
    return scale._configured_scale_plan_input()


def _wire_group(value: object) -> codec.Group:
    return cast(codec.Group, codec._decode(json.dumps(value, separators=(",", ":"))))


def _snapshot(
    owner: codec.ScenarioCodec,
    account: codec.Account,
    kind: codec.Kind,
    at: int,
    identity: str,
) -> codec.SnapshotFact:
    return owner.capture_snapshot(
        cast(
            codec.SnapshotRequest,
            {
                "schema_version": "snapshot_request_v1",
                "account_key": account,
                "snapshot_id": identity,
                "snapshot_kind": kind,
                "capture_mode": "OWNER_CURRENT",
                "captured_at": at,
            },
        )
    )


def _account_values(
    owner: codec.ScenarioCodec, account: codec.Account, at: int
) -> tuple[Decimal, Decimal]:
    fact = _snapshot(owner, account, "TRADING", at, f"TRADING-{at}")
    payload = cast(codec.Trading, fact["immutable_payload"])
    return payload["equity"], payload["available_equity"]


def _expected_decimal_checkpoints() -> dict[int, tuple[Decimal, Decimal, Decimal]]:
    # Independent section-6 arithmetic: held exposure + fee, then paid fee and
    # effect-time release. No production calculation or wire encoder is used.
    cash = Decimal("121.2")
    per_order_margin = Decimal("100") / Decimal("10")
    per_order_fee = Decimal("100") * Decimal("0.001")
    used_512 = Decimal(12) * (per_order_margin + per_order_fee)
    equity_514 = cash - Decimal("0.5") * Decimal("100") * Decimal("0.001")
    used_514 = (
        Decimal(11) * (per_order_margin + per_order_fee)
        + per_order_margin
        + Decimal("0.05")
    )
    used_516 = Decimal(11) * (per_order_margin + per_order_fee) + Decimal("5")
    return {
        512: (cash, used_512, cash - used_512),
        513: (cash, used_512, cash - used_512),
        514: (equity_514, used_514, equity_514 - used_514),
        515: (equity_514, used_514, equity_514 - used_514),
        516: (equity_514, used_516, equity_514 - used_516),
        517: (equity_514, used_516, equity_514 - used_516),
    }


def _assert_checkpoint(
    owner: codec.ScenarioCodec, account: codec.Account, at: int
) -> None:
    equity, available = _account_values(owner, account, at)
    expected_equity, expected_used, expected_available = (
        _expected_decimal_checkpoints()[at]
    )
    assert equity == expected_equity
    assert equity - available == expected_used
    assert available == expected_available


def test_plan_input_schema_hash_detachment_and_p1_vectors_are_frozen():
    first = _input()
    expected_keys = {
        "schema_version",
        "scenario_plan_id",
        "native_profile",
        "account_key",
        "terminal_policy",
        "configuration",
        "configuration_sha256",
        "products",
        "recipe",
    }
    assert set(first) == expected_keys
    assert first["schema_version"] == "spider_configured_plan_input_v1"
    assert first["scenario_plan_id"] == "SPIDER_P2_CONFIGURED_SCALE_V1"
    assert first["native_profile"] == "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1"
    assert first["terminal_policy"] == "SCHEDULED_MTM"
    assert first["products"] == _PRODUCTS
    configuration = first["configuration"]
    assert (
        first["configuration_sha256"]
        == hashlib.sha256(canonical_bytes(configuration)).hexdigest()
    )
    assert first["configuration_sha256"] == _CONFIGURATION_SHA256
    assert (
        first["configuration_sha256"]
        == scale._configured_scale_plan_input()["configuration_sha256"]
    )
    assert (
        scale._PLAN_INPUT_SHA256 == hashlib.sha256(canonical_bytes(first)).hexdigest()
    )
    assert scale._PLAN_INPUT_SHA256 == _PLAN_INPUT_SHA256
    assert len(first["recipe"]) == 17
    assert [step["at"] for step in first["recipe"]] == list(range(501, 518))
    assert [step["group"]["group_id"] for step in first["recipe"][:16]] == [
        f"G-{at}" for at in range(501, 517)
    ]
    assert first["recipe"][16]["projection"]["reference"] == {
        "namespace": "SOURCE",
        "fact_id": "event-516",
    }

    def no_float(value: object) -> None:
        assert type(value) is not float
        if isinstance(value, dict):
            for nested in value.values():
                no_float(nested)
        elif isinstance(value, list):
            for nested in value:
                no_float(nested)

    no_float(first)
    before = canonical_bytes(first)
    first["configuration"]["products"][0]["specs"][0]["contract_value"] = "9"
    first["recipe"][0]["group"]["members"][0]["payload"]["product_id"] = "changed"
    assert canonical_bytes(_input()) == before

    assert spider_scenario_plans.PLAN_IDS == (
        "SPIDER_P1_SCHEDULED_MTM_V1",
        "SPIDER_P1_LEGAL_LIQUIDATION_V1",
        "SPIDER_P1_O03_DURABLE_V1",
    )
    expected_vectors = (
        (1451, "8fb335d6a98afb6bb08fa837386347c4db90f0b00e8661e305599618f05f1520"),
        (598, "4e149110d228fc7422b6149efe31de1dc036eabf648340049ca95ce12dda2512"),
        (1200, "439906a52cba32b5d1d1bcc1cbd4439270aad175f58a118568fcb754dd42eef8"),
    )
    for index, (length, digest) in enumerate(expected_vectors):
        bundle = spider_scenario_plans.plan_bundle(
            spider_scenario_plans.PLAN_IDS[index]
        )
        raw = canonical_bytes(bundle["plan"])
        assert (len(raw), hashlib.sha256(raw).hexdigest()) == (length, digest)


def test_literal_section6_configuration_and_recipe_drive_real_native_codec():
    plan_input = _input()
    account = cast(codec.Account, plan_input["account_key"])
    config = cast(dict[str, object], plan_input["configuration"])
    assert config == {
        "schema_version": "synthetic_multi_product_config_v1",
        "config_id": "SYNTHETIC_P2_SCALE_12_V1",
        "seed_effective_at": 500,
        "cash": "121.2",
        "leverage": "10",
        "products": config["products"],
        "positions": [],
        "orders": [],
    }
    rows = cast(list[dict[str, Any]], config["products"])
    assert [row["product_id"] for row in rows] == _PRODUCTS
    assert [row["instrument_code"] for row in rows] == list(range(1, 13))
    assert all(
        row["taker_fee_rate"] == row["liquidation_fee_rate"] == "0.001" for row in rows
    )
    assert all(
        row["specs"]
        == [
            {
                "version": "scale-spec-v1",
                "valid_from": 0,
                "valid_to": None,
                "contract_value": "1",
                "multiplier": "1",
                "price_tick": "1",
                "quantity_step": "0.5",
                "minimum_quantity": "0.5",
            }
        ]
        for row in rows
    )
    assert all(
        row["tiers"]
        == [
            {
                "version": "scale-tier-v1",
                "valid_from": 0,
                "valid_to": None,
                "rows": [
                    {
                        "minimum_contracts": "0",
                        "maximum_contracts": "100000",
                        "mmr": "0.005",
                        "imr": "0.1",
                        "max_leverage": "10",
                    }
                ],
            }
        ]
        for row in rows
    )
    assert all(
        row["marks"] == [{"valid_from": 0, "valid_to": 3000, "mark": "100"}]
        for row in rows
    )

    owner = codec.ScenarioCodec(
        cast(codec.Profile, plan_input["native_profile"]), account, config
    )
    initial = owner.inspect_state()
    assert initial["profile_id"] == plan_input["native_profile"]
    assert initial["config_id"] == config["config_id"]
    assert initial["account_version"] == 0

    recipe = cast(list[dict[str, Any]], plan_input["recipe"])
    for index, step in enumerate(recipe[:12]):
        group = _wire_group(step["group"])
        result = owner.apply_group(group)
        assert result["classification"] == "COMMITTED"
        assert result["rejections"] == []
        assert result["account_version_after"] == index + 1
        assert result["committed_references"] == [
            {"namespace": "SOURCE", "fact_id": f"event-{501 + index}"}
        ]
    _assert_checkpoint(owner, account, 512)
    open_512 = cast(
        codec.OpenOrders,
        _snapshot(owner, account, "OPEN_ORDERS", 512, "ORDERS-512")[
            "immutable_payload"
        ],
    )
    assert len(open_512["rows"]) == 12
    assert {row["product_id"] for row in open_512["rows"]} == set(_PRODUCTS)
    assert all(
        row["original_size_contracts"] == Decimal("1")
        and row["cumulative_filled_size_contracts"] == 0
        for row in open_512["rows"]
    )

    rejected_step = recipe[12]
    rejected = owner.apply_group(_wire_group(rejected_step["group"]))
    assert rejected_step["expected_rejection_label"] == "INSUFFICIENT_SHARED_EQUITY"
    assert rejected["classification"] == "REJECTED"
    assert rejected["rejections"] == [
        {"event_id": "event-513", "reason": "INSUFFICIENT_SHARED_EQUITY"}
    ]
    assert rejected["account_version_before"] == rejected["account_version_after"] == 12
    _assert_checkpoint(owner, account, 513)

    filled = owner.apply_group(_wire_group(recipe[13]["group"]))
    assert filled["classification"] == "COMMITTED"
    assert filled["account_version_after"] == 13
    _assert_checkpoint(owner, account, 514)
    positions = cast(
        codec.Positions,
        _snapshot(owner, account, "POSITIONS", 514, "POSITIONS-514")[
            "immutable_payload"
        ],
    )
    assert positions["rows"] == [
        {
            "product_id": _PRODUCTS[0],
            "margin_mode": "cross",
            "position_contracts": Decimal("0.5"),
            "last_price": Decimal("100"),
            "notional_usd": Decimal("50"),
        }
    ]
    orders_514 = cast(
        codec.OpenOrders,
        _snapshot(owner, account, "OPEN_ORDERS", 514, "ORDERS-514")[
            "immutable_payload"
        ],
    )
    first_order = next(
        row for row in orders_514["rows"] if row["product_id"] == _PRODUCTS[0]
    )
    assert first_order["order_id"] == _ORDER_ID
    assert (
        first_order["state"],
        first_order["original_size_contracts"],
        first_order["cumulative_filled_size_contracts"],
    ) == ("partially_filled", Decimal("1"), Decimal("0.5"))

    requested = owner.apply_group(_wire_group(recipe[14]["group"]))
    assert requested["classification"] == "COMMITTED"
    assert requested["account_version_after"] == 14
    _assert_checkpoint(owner, account, 515)
    orders_515 = cast(
        codec.OpenOrders,
        _snapshot(owner, account, "OPEN_ORDERS", 515, "ORDERS-515")[
            "immutable_payload"
        ],
    )
    first_order_515 = next(
        row for row in orders_515["rows"] if row["product_id"] == _PRODUCTS[0]
    )
    assert first_order_515["state"] == "partially_filled"
    assert first_order_515["cumulative_filled_size_contracts"] == Decimal("0.5")

    effected = owner.apply_group(_wire_group(recipe[15]["group"]))
    assert effected["classification"] == "COMMITTED"
    assert effected["account_version_after"] == 15
    _assert_checkpoint(owner, account, 516)
    orders_516 = cast(
        codec.OpenOrders,
        _snapshot(owner, account, "OPEN_ORDERS", 516, "ORDERS-516")[
            "immutable_payload"
        ],
    )
    assert len(orders_516["rows"]) == 11
    assert all(row["product_id"] != _PRODUCTS[0] for row in orders_516["rows"])
    before_ack = owner.inspect_state()
    ack = owner.build_delivery(cast(codec.Projection, recipe[16]["projection"]))
    assert ack["source_fact_id"] == "event-516"
    assert ack["payload_kind"] == "TRANSPORT_ACK"
    assert ack["visible_at"] == 517
    assert ack["immutable_payload"] == {
        "route": "WS",
        "operation": "CANCEL",
        "client_order_id": "C-501",
        "code": "0",
    }
    assert owner.inspect_state() == before_ack
    _assert_checkpoint(owner, account, 517)
