from copy import deepcopy
from decimal import Decimal as D
from collections.abc import Mapping
from types import MappingProxyType
from typing import cast

import pytest

from src.core.backtest import synthetic_scenario_codec as wire
from src.core.backtest.synthetic_scenario_replay import _ReplayComposition, _closed_policy


ACCOUNT: wire.Account = {"venue": "okx-scenario", "environment": "test", "account": "A"}


def configured(count: int = 1, *, decimals: bool = True) -> dict[str, object]:
    value = D if decimals else str
    products = []
    for index in range(1, count + 1):
        name = f"PRODUCT-{index:02d}"
        products.append({
            "product_id": name,
            "instrument_code": 100 + index,
            "taker_fee_rate": value("0.001"),
            "liquidation_fee_rate": value("0.002"),
            "specs": [
                {"version": "old", "valid_from": 0, "valid_to": 50,
                 "contract_value": value("7"), "multiplier": value("1"),
                 "price_tick": value("0.25"), "quantity_step": value("0.01"),
                 "minimum_quantity": value("0.02")},
                {"version": "active", "valid_from": 50, "valid_to": None,
                 "contract_value": value(str(index + 1)), "multiplier": value("1"),
                 "price_tick": value(str(index + 2)), "quantity_step": value(str(index + 3)),
                 "minimum_quantity": value(str(index + 4))},
            ],
            "tiers": [{"version": "tier", "valid_from": 0, "valid_to": None,
                       "rows": [{"minimum_contracts": value("0"), "maximum_contracts": value("100"),
                                 "mmr": value("0.005"), "imr": value("0.1"),
                                 "max_leverage": value("10")}]}],
            "marks": [
                {"valid_from": 0, "valid_to": 50, "mark": value("9")},
                {"valid_from": 50, "valid_to": 100, "mark": value(str(index + 10))},
            ],
        })
    return {"schema_version": "synthetic_multi_product_config_v1", "config_id": "composition-test",
            "seed_effective_at": 50, "cash": value("100"), "leverage": value("1"),
            "products": products, "positions": [], "orders": []}


def read_only(value: object) -> object:
    if isinstance(value, dict):
        return MappingProxyType({key: read_only(item) for key, item in value.items()})
    if isinstance(value, list):
        return [read_only(item) for item in value]
    return value


@pytest.mark.parametrize("count", [1, 3, 12])
def test_configured_markets_follow_product_order_and_active_seed_rows(count):
    config = configured(count)
    composition = _ReplayComposition(
        "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT, configuration=config
    )
    markets = composition._policy.markets
    products = cast(list[dict[str, object]], config["products"])
    assert list(markets) == [product["product_id"] for product in products]
    for index, product in enumerate(products, start=1):
        market = markets[cast(str, product["product_id"])]
        assert market == {
            "price": D(index + 10), "ctVal": D(index + 1), "lotSz": D(index + 3),
            "minSz": D(index + 4), "increment": D(index + 2), "ratioHL": "0.1",
            "state": "live", "instIdCode": 100 + index,
        }
    assert composition._policy.rows == [] and composition._policy.running is False


def test_configuration_snapshot_is_detached_and_not_retained():
    config = configured(3)
    composition = _ReplayComposition(
        "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT, configuration=config
    )
    original_markets = deepcopy(composition._policy.markets)
    config["config_id"] = "mutated"
    products = cast(list[dict[str, object]], config["products"])
    specs = cast(list[dict[str, object]], products[0]["specs"])
    specs[1]["contract_value"] = D("999")
    assert composition._policy.markets == original_markets
    assert not any(value is config for value in vars(composition).values())
    assert not any(value == config for value in vars(composition).values())


def test_top_level_and_nested_read_only_mappings_are_supported():
    config = cast(Mapping[str, object], read_only(configured(3)))
    composition = _ReplayComposition(
        "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1",
        ACCOUNT,
        configuration=config,
    )
    assert list(composition._policy.markets) == [
        "PRODUCT-01", "PRODUCT-02", "PRODUCT-03"
    ]


def test_decimal_and_canonical_string_configuration_project_equivalently():
    decimal_owner = _ReplayComposition(
        "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT, configuration=configured(3)
    )
    string_owner = _ReplayComposition(
        "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT,
        configuration=configured(3, decimals=False),
    )
    assert decimal_owner._policy.markets == string_owner._policy.markets


def test_invalid_or_missing_config_keeps_native_invalid_schema_precedence(monkeypatch):
    def policy_must_not_be_built(_profile):
        pytest.fail("Policy was constructed before native configuration validation")

    monkeypatch.setattr("src.core.backtest.synthetic_scenario_replay._closed_policy", policy_must_not_be_built)
    with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
        _ReplayComposition("SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT)
    invalid = configured()
    products = cast(list[dict[str, object]], invalid["products"])
    specs = cast(list[dict[str, object]], products[0]["specs"])
    del specs[1]["multiplier"]
    with pytest.raises(ValueError, match="^INVALID_SCHEMA$"):
        _ReplayComposition(
            "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT,
            configuration=cast(Mapping[str, object], read_only(invalid)),
        )


def test_native_valid_non_unit_multiplier_rejects_before_policy_or_callbacks(monkeypatch):
    config = configured()
    products = cast(list[dict[str, object]], config["products"])
    specs = cast(list[dict[str, object]], products[0]["specs"])
    specs[1]["multiplier"] = D("2")
    calls = []

    def policy_must_not_be_built(_profile):
        calls.append("policy")
        pytest.fail("unsupported multiplier reached Policy construction")

    monkeypatch.setattr("src.core.backtest.synthetic_scenario_replay._closed_policy", policy_must_not_be_built)
    with pytest.raises(ValueError, match="^UNSUPPORTED_CONFIGURATION$"):
        _ReplayComposition(
            "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1", ACCOUNT,
            evidence_callback=lambda *args: calls.append("callback"),
            configuration=config,
        )
    assert calls == []


def test_p1_composition_and_closed_policy_defaults_remain_unchanged():
    callback_plans, evidence_callback = {}, lambda *_: None
    composition = _ReplayComposition(
        "SYNTHETIC_BTC_ETH_V1", ACCOUNT, callback_plans, evidence_callback
    )
    expected = _closed_policy("SYNTHETIC_BTC_ETH_V1")
    assert composition._policy.markets == expected.markets
    assert vars(composition._policy) == vars(expected)
    assert composition._callback_plans == {}
    assert composition._evidence_callback is evidence_callback
    assert composition._current_time == 500 and composition._last_popped is None
    assert composition._queue == [] and composition._records == {}
