"""Backpack-owned live configuration policy and generic-main routing tests."""

import importlib
from unittest.mock import MagicMock

import pytest

import main as strategy_main


_PROVIDER_OWNERS = {
    "build_backpack_live_adapter_config": (
        "src.core.adapters.backpack_live_config.build_backpack_live_adapter_config"
    ),
    "build_ccxt_live_credentials": (
        "src.core.adapters.ccxt_live_credentials.build_ccxt_live_credentials"
    ),
    "build_rithmic_live_adapter_config": (
        "src.core.adapters.rithmic_live_config.build_rithmic_live_adapter_config"
    ),
}


def _patch_provider_owner(monkeypatch, name: str, value: object) -> None:
    monkeypatch.setattr(_PROVIDER_OWNERS[name], value)


def _owner():
    return importlib.import_module("src.core.adapters.backpack_live_config")


def _valid_environ() -> dict[str, str]:
    return {
        "EXCHANGE_API_KEY": " key ",
        "EXCHANGE_SECRET": " secret ",
        "EXCHANGE_TESTNET": " false ",
        "EXCHANGE_ENABLE_WS": " false ",
    }


def _set_live_backpack_env(monkeypatch) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("ADAPTER_MODE", "live")
    monkeypatch.setenv("EXCHANGE_ID", "backpack")
    monkeypatch.setenv(
        "INSTRUMENT_PRODUCT_IDS",
        "BACKPACK:BTC_USDC-PERP,BACKPACK:SOL_USDC-PERP",
    )
    for name, value in _valid_environ().items():
        monkeypatch.setenv(name, value)
    for name in (
        "ACCOUNT_POSITION_MODE",
        "ACCOUNT_LEVERAGE",
        "ACCOUNT_MARGIN_MODE",
    ):
        monkeypatch.delenv(name, raising=False)


def _assert_owner_error(environ: dict[str, str], expected: str) -> None:
    with pytest.raises(ValueError) as raised:
        _owner().build_backpack_live_adapter_config(
            product_ids=["BACKPACK:BTC_USDC-PERP"],
            environ=environ,
        )

    assert raised.value.args == (expected,)


def test_backpack_owner_returns_exact_supported_live_config() -> None:
    product_ids = ["BACKPACK:BTC_USDC-PERP", "BACKPACK:SOL_USDC-PERP"]

    config = _owner().build_backpack_live_adapter_config(
        product_ids=product_ids,
        environ=_valid_environ(),
    )

    assert config == {
        "mode": "live",
        "exchange": "backpack",
        "api_key": "key",
        "secret": "secret",
        "testnet": False,
        "instrument_product_ids": product_ids,
    }
    assert config["instrument_product_ids"] is product_ids
    assert "enable_ws" not in config
    assert "account_initialization" not in config


def test_backpack_owner_delegates_to_shared_credentials_exactly_once(
    monkeypatch,
) -> None:
    owner = _owner()
    environ = _valid_environ()
    product_ids = ["BACKPACK:BTC_USDC-PERP"]
    credentials = {"api_key": object(), "secret": object(), "testnet": False}
    shared_owner = MagicMock(return_value=credentials)
    monkeypatch.setattr(owner, "build_ccxt_live_credentials", shared_owner)

    config = owner.build_backpack_live_adapter_config(
        product_ids=product_ids,
        environ=environ,
    )

    shared_owner.assert_called_once_with(environ)
    assert config["api_key"] is credentials["api_key"]
    assert config["secret"] is credentials["secret"]
    assert config["instrument_product_ids"] is product_ids


def test_backpack_owner_progressively_reveals_compound_invalid_config() -> None:
    environ = {
        "EXCHANGE_API_KEY": "",
        "EXCHANGE_SECRET": "",
        "EXCHANGE_ENABLE_WS": "enabled",
        "ACCOUNT_POSITION_MODE": "one_way",
        "ACCOUNT_LEVERAGE": "3",
        "ACCOUNT_MARGIN_MODE": "cross",
    }

    _assert_owner_error(environ, "EXCHANGE_API_KEY must be set explicitly")
    environ["EXCHANGE_API_KEY"] = "key"
    _assert_owner_error(environ, "EXCHANGE_SECRET must be set explicitly")
    environ["EXCHANGE_SECRET"] = "secret"
    _assert_owner_error(environ, "EXCHANGE_TESTNET must be set explicitly")
    environ["EXCHANGE_TESTNET"] = "enabled"
    _assert_owner_error(environ, "EXCHANGE_TESTNET must be a boolean")
    environ["EXCHANGE_TESTNET"] = "true"
    _assert_owner_error(environ, "backpack_testnet_unsupported")
    environ["EXCHANGE_TESTNET"] = "false"
    _assert_owner_error(environ, "EXCHANGE_ENABLE_WS must be a boolean")
    environ["EXCHANGE_ENABLE_WS"] = "true"
    _assert_owner_error(environ, "backpack_websocket_order_entry_unsupported")
    environ["EXCHANGE_ENABLE_WS"] = "false"
    _assert_owner_error(
        environ,
        "backpack_account_initialization_unsupported: ACCOUNT_POSITION_MODE",
    )
    environ["ACCOUNT_POSITION_MODE"] = ""
    _assert_owner_error(
        environ,
        "backpack_account_initialization_unsupported: ACCOUNT_LEVERAGE",
    )
    environ["ACCOUNT_LEVERAGE"] = "   "
    _assert_owner_error(
        environ,
        "backpack_account_initialization_unsupported: ACCOUNT_MARGIN_MODE",
    )
    environ["ACCOUNT_MARGIN_MODE"] = ""

    assert (
        _owner().build_backpack_live_adapter_config(
            product_ids=["BACKPACK:BTC_USDC-PERP"],
            environ=environ,
        )["testnet"]
        is False
    )


def test_backpack_main_delegates_to_venue_owner_once(
    monkeypatch,
) -> None:
    _set_live_backpack_env(monkeypatch)
    products = ["BACKPACK:BTC_USDC-PERP", "BACKPACK:SOL_USDC-PERP"]
    monkeypatch.setattr(strategy_main, "_env_csv", lambda _name: products)
    owner_result = {
        "mode": "live",
        "exchange": "backpack",
        "instrument_product_ids": products,
        "api_key": object(),
        "secret": object(),
        "testnet": False,
    }
    backpack_owner = MagicMock(return_value=owner_result)
    _patch_provider_owner(
        monkeypatch, "build_backpack_live_adapter_config", backpack_owner
    )

    config = strategy_main._adapter_config_from_env()

    assert config is owner_result
    backpack_owner.assert_called_once_with(
        product_ids=products,
        environ=strategy_main.os.environ,
    )


def test_backpack_config_failure_stops_main_before_audit_and_runtime(
    monkeypatch,
) -> None:
    _set_live_backpack_env(monkeypatch)
    sentinel = RuntimeError("backpack-config-owner-sentinel")
    backpack_owner = MagicMock(side_effect=sentinel)
    forbidden = {
        name: MagicMock(side_effect=AssertionError(name))
        for name in (
            "_required_env_flag",
            "_validate_runtime_config",
            "SessionLocal",
            "DataConsumer",
            "StrategyEngine",
            "configure_metrics",
        )
    }
    _patch_provider_owner(
        monkeypatch, "build_backpack_live_adapter_config", backpack_owner
    )
    for name, value in forbidden.items():
        monkeypatch.setattr(strategy_main, name, value)

    with pytest.raises(RuntimeError) as raised:
        strategy_main.main()

    assert raised.value is sentinel
    backpack_owner.assert_called_once()
    for value in forbidden.values():
        value.assert_not_called()


@pytest.mark.parametrize("exchange", ["binance", "bybit", "rithmic"])
def test_other_live_venues_never_call_backpack_owner(monkeypatch, exchange) -> None:
    backpack_owner = MagicMock(side_effect=AssertionError("Backpack owner called"))
    _patch_provider_owner(
        monkeypatch, "build_backpack_live_adapter_config", backpack_owner
    )
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("ADAPTER_MODE", "live")
    monkeypatch.setenv("EXCHANGE_ID", exchange)
    monkeypatch.setenv("EXCHANGE_ENABLE_WS", "false")
    if exchange == "rithmic":
        product_id = "RITHMIC:NQ-202609"
        result = {
            "rithmic_profile": "orders",
            "account_id": "ACCOUNT",
            "rithmic_instruments": {product_id: {"exchange": "CME"}},
            "rithmic_recovery_profile": "orders",
            "rithmic_recovery_account_id": "ACCOUNT",
        }
        _patch_provider_owner(
            monkeypatch,
            "build_rithmic_live_adapter_config",
            MagicMock(return_value=result),
        )
    else:
        product_id = f"{exchange.upper()}:BTCUSDT-PERP"
        for name, value in _valid_environ().items():
            monkeypatch.setenv(name, value)
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", product_id)

    assert strategy_main._adapter_config_from_env()["exchange"] == exchange
    backpack_owner.assert_not_called()


def test_simulated_config_never_calls_backpack_owner(monkeypatch) -> None:
    backpack_owner = MagicMock(side_effect=AssertionError("Backpack owner called"))
    _patch_provider_owner(
        monkeypatch, "build_backpack_live_adapter_config", backpack_owner
    )
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
    monkeypatch.setenv("ADAPTER_MODE", "simulated")

    assert strategy_main._adapter_config_from_env() == {"mode": "simulated"}
    backpack_owner.assert_not_called()
