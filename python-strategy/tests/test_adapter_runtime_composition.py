from __future__ import annotations

import json
import os
import subprocess
import sys
from unittest.mock import MagicMock

import pytest

from src.core import adapter_runtime_composition


def test_main_import_does_not_eager_load_provider_owners() -> None:
    provider_modules = (
        "src.core.adapters.backpack_live_config",
        "src.core.adapters.binance_live_config",
        "src.core.adapters.bybit_live_config",
        "src.core.adapters.okx_live_config",
        "src.core.adapters.rithmic_live_config",
        "src.core.adapters.rithmic_runtime_composition",
    )
    script = "\n".join(
        (
            "import json, sys",
            "import src.main",
            f"names = {provider_modules!r}",
            "print(json.dumps([name for name in names if name in sys.modules]))",
        )
    )

    result = subprocess.run(
        [sys.executable, "-B", "-c", script],
        check=True,
        capture_output=True,
        env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)},
        text=True,
    )

    assert json.loads(result.stdout) == []


@pytest.mark.parametrize("exchange", ["okx", "kraken"])
def test_incomplete_live_venues_fail_before_any_configuration_dependency(
    monkeypatch,
    exchange,
) -> None:
    balance_parser = MagicMock(side_effect=AssertionError("balance parsed"))
    websocket_reader = MagicMock(side_effect=AssertionError("websocket read"))
    monkeypatch.setattr(adapter_runtime_composition, "to_base_quote", balance_parser)

    with pytest.raises(ValueError) as raised:
        adapter_runtime_composition.build_live_adapter_config(
            exchange=exchange,
            product_ids=[f"{exchange.upper()}:BTCUSDT-PERP"],
            environ={"EXCHANGE_API_KEY": "credential-sentinel"},
            read_enable_ws=websocket_reader,
        )

    assert raised.value.args == (
        f"unsupported_or_unavailable_live_execution_venue: exchange={exchange}",
    )
    balance_parser.assert_not_called()
    websocket_reader.assert_not_called()


@pytest.mark.parametrize(
    "config",
    [
        {"mode": "simulated"},
        {"mode": "live", "exchange": "binance"},
        {"mode": "live", "exchange": "backpack"},
        {"mode": "live", "exchange": "bybit"},
        {"mode": "live", "exchange": "okx"},
    ],
)
def test_non_rithmic_config_returns_no_runtime_factories(config: dict) -> None:
    assert adapter_runtime_composition.runtime_factories_for_config(config) == (
        None,
        None,
    )


def test_rithmic_config_returns_exact_runtime_factories(monkeypatch) -> None:
    bootstrap = MagicMock()
    capabilities = MagicMock()
    monkeypatch.setattr(
        "src.core.adapters.rithmic_runtime_composition.prepare_rithmic_runtime_bootstrap",
        bootstrap,
    )
    monkeypatch.setattr(
        "src.core.adapters.rithmic_runtime_composition.build_rithmic_runtime_owners",
        capabilities,
    )

    assert adapter_runtime_composition.runtime_factories_for_config(
        {"mode": "live", "exchange": "rithmic"}
    ) == (bootstrap, capabilities)


def test_dedicated_venue_owner_precedes_shared_websocket_reader(monkeypatch) -> None:
    owner_result = {"mode": "live", "exchange": "binance"}
    owner = MagicMock(return_value=owner_result)
    websocket_reader = MagicMock(
        side_effect=AssertionError("shared websocket reader called")
    )
    monkeypatch.setattr(
        "src.core.adapters.binance_live_config.build_binance_live_adapter_config",
        owner,
    )
    products = ["BINANCE:BTCUSDT-PERP"]
    environ = {"EXCHANGE_ENABLE_WS": "provider-owned"}

    result = adapter_runtime_composition.build_live_adapter_config(
        exchange="binance",
        product_ids=products,
        environ=environ,
        read_enable_ws=websocket_reader,
    )

    assert result is owner_result
    assert result["balance_asset"] == "USDT"
    owner.assert_called_once_with(product_ids=products, environ=environ)
    websocket_reader.assert_not_called()


@pytest.mark.parametrize(
    ("exchange", "product_id", "owner_path", "expected_asset"),
    [
        (
            "binance",
            "BINANCE:BTCUSDT-PERP",
            "src.core.adapters.binance_live_config.build_binance_live_adapter_config",
            "USDT",
        ),
        (
            "backpack",
            "BACKPACK:SOLUSDC-PERP",
            "src.core.adapters.backpack_live_config.build_backpack_live_adapter_config",
            "USDC",
        ),
        (
            "bybit",
            "BYBIT:BTCUSDT-PERP",
            "src.core.adapters.bybit_live_config.build_bybit_live_adapter_config",
            "USDT",
        ),
    ],
)
def test_live_ccxt_config_derives_exact_balance_asset(
    monkeypatch,
    exchange,
    product_id,
    owner_path,
    expected_asset,
) -> None:
    owner = MagicMock(return_value={"mode": "live", "exchange": exchange})
    monkeypatch.setattr(owner_path, owner)

    result = adapter_runtime_composition.build_live_adapter_config(
        exchange=exchange,
        product_ids=[product_id],
        environ={},
        read_enable_ws=MagicMock(),
    )

    assert result["balance_asset"] == expected_asset
    owner.assert_called_once_with(product_ids=[product_id], environ={})


@pytest.mark.parametrize(
    "product_ids",
    [
        ["BINANCE:BTCUSDT-PERP", "BINANCE:BTCUSDC-PERP"],
        ["BINANCE:BTCUSDT-PERP", "BINANCE:ETHUSDT-PERP"],
    ],
    ids=["mixed", "missing"],
)
def test_invalid_balance_asset_fails_before_venue_owner(
    monkeypatch,
    product_ids,
) -> None:
    owner = MagicMock(side_effect=AssertionError("venue owner called"))
    monkeypatch.setattr(
        "src.core.adapters.binance_live_config.build_binance_live_adapter_config",
        owner,
    )
    if product_ids[1].startswith("BINANCE:ETH"):
        original = adapter_runtime_composition.to_base_quote
        monkeypatch.setattr(
            adapter_runtime_composition,
            "to_base_quote",
            lambda product_id: (
                ("ETH", "")
                if product_id.startswith("BINANCE:ETH")
                else original(product_id)
            ),
        )

    with pytest.raises(ValueError, match="common balance asset"):
        adapter_runtime_composition.build_live_adapter_config(
            exchange="binance",
            product_ids=product_ids,
            environ={"EXCHANGE_API_KEY": "credential-sentinel"},
            read_enable_ws=MagicMock(),
        )

    owner.assert_not_called()


def test_rithmic_config_has_no_generic_balance_asset(monkeypatch) -> None:
    owner = MagicMock(return_value={"account_id": "ACCOUNT"})
    monkeypatch.setattr(
        "src.core.adapters.rithmic_live_config.build_rithmic_live_adapter_config",
        owner,
    )

    result = adapter_runtime_composition.build_live_adapter_config(
        exchange="rithmic",
        product_ids=["RITHMIC:MNQ-202609"],
        environ={},
        read_enable_ws=lambda: False,
    )

    assert "balance_asset" not in result


def test_live_ccxt_runtime_config_requires_composed_balance_asset() -> None:
    with pytest.raises(ValueError, match="common balance asset"):
        adapter_runtime_composition.validate_runtime_config(
            {"mode": "live", "exchange": "binance"},
            audit_external_orders=True,
        )
