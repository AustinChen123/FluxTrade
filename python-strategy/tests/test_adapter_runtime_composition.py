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
    owner.assert_called_once_with(product_ids=products, environ=environ)
    websocket_reader.assert_not_called()
