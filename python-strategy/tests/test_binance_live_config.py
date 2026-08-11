"""Binance-owned live configuration policy tests."""

import importlib
import inspect
from unittest.mock import MagicMock

import pytest

import main as strategy_main


def _owner():
    return importlib.import_module("src.core.adapters.binance_live_config")


def _valid_environ() -> dict[str, str]:
    return {
        "EXCHANGE_API_KEY": " key ",
        "EXCHANGE_SECRET": " secret ",
        "EXCHANGE_TESTNET": " true ",
        "EXCHANGE_ENABLE_WS": " false ",
        "ACCOUNT_POSITION_MODE": "one_way",
        "ACCOUNT_LEVERAGE": "3",
        "ACCOUNT_MARGIN_MODE": "isolated",
    }


def _set_live_binance_env(monkeypatch) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("ADAPTER_MODE", "live")
    monkeypatch.setenv("EXCHANGE_ID", "binance")
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", "BINANCE:BTCUSDT-PERP")
    for name, value in _valid_environ().items():
        monkeypatch.setenv(name, value)


def test_binance_owner_returns_exact_current_live_config() -> None:
    product_ids = ["BINANCE:BTCUSDT-PERP", "BINANCE:ETHUSDT-PERP"]

    config = _owner().build_binance_live_adapter_config(
        product_ids=product_ids,
        environ=_valid_environ(),
    )

    assert tuple(config) == (
        "mode",
        "exchange",
        "enable_ws",
        "instrument_product_ids",
        "account_initialization",
        "api_key",
        "secret",
        "testnet",
    )
    assert config == {
        "mode": "live",
        "exchange": "binance",
        "enable_ws": False,
        "instrument_product_ids": product_ids,
        "account_initialization": {
            "product_ids": product_ids,
            "position_mode": "one_way",
            "leverage": "3",
            "margin_mode": "isolated",
        },
        "api_key": "key",
        "secret": "secret",
        "testnet": True,
    }
    assert config["instrument_product_ids"] is product_ids
    assert config["account_initialization"]["product_ids"] is product_ids


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        (None, False),
        ("1", True),
        ("TRUE", True),
        (" yes ", True),
        ("On", True),
        ("0", False),
        ("FALSE", False),
        (" no ", False),
        ("Off", False),
    ],
)
def test_binance_owner_preserves_websocket_boolean_aliases(raw, expected) -> None:
    environ = _valid_environ()
    if raw is None:
        environ.pop("EXCHANGE_ENABLE_WS")
    else:
        environ["EXCHANGE_ENABLE_WS"] = raw

    config = _owner().build_binance_live_adapter_config(
        product_ids=["BINANCE:BTCUSDT-PERP"],
        environ=environ,
    )

    assert config["enable_ws"] is expected


def test_binance_owner_progressively_reveals_current_error_precedence() -> None:
    environ = {
        "EXCHANGE_ENABLE_WS": "enabled",
        "EXCHANGE_API_KEY": "",
        "EXCHANGE_SECRET": "",
    }

    with pytest.raises(ValueError) as raised:
        _owner().build_binance_live_adapter_config(
            product_ids=["BINANCE:BTCUSDT-PERP"],
            environ=environ,
        )
    assert raised.value.args == ("EXCHANGE_ENABLE_WS must be a boolean",)

    environ["EXCHANGE_ENABLE_WS"] = "false"
    expected = (
        ("EXCHANGE_API_KEY", "key"),
        ("EXCHANGE_SECRET", "secret"),
        ("EXCHANGE_TESTNET", "true"),
    )
    for name, repaired in expected:
        with pytest.raises(ValueError) as raised:
            _owner().build_binance_live_adapter_config(
                product_ids=["BINANCE:BTCUSDT-PERP"],
                environ=environ,
            )
        assert raised.value.args == (f"{name} must be set explicitly",)
        environ[name] = repaired

    environ["EXCHANGE_TESTNET"] = "enabled"
    with pytest.raises(ValueError) as raised:
        _owner().build_binance_live_adapter_config(
            product_ids=["BINANCE:BTCUSDT-PERP"],
            environ=environ,
        )
    assert raised.value.args == ("EXCHANGE_TESTNET must be a boolean",)


def test_binance_owner_calls_shared_credentials_once_after_websocket(
    monkeypatch,
) -> None:
    owner = _owner()
    environ = _valid_environ()
    product_ids = ["BINANCE:BTCUSDT-PERP"]
    credentials = {
        "api_key": object(),
        "secret": object(),
        "testnet": object(),
    }
    shared_owner = MagicMock(return_value=credentials)
    monkeypatch.setattr(owner, "build_ccxt_live_credentials", shared_owner)

    config = owner.build_binance_live_adapter_config(
        product_ids=product_ids,
        environ=environ,
    )

    shared_owner.assert_called_once_with(environ)
    for name, value in credentials.items():
        assert config[name] is value

    shared_owner.reset_mock()
    environ["EXCHANGE_ENABLE_WS"] = "enabled"
    with pytest.raises(ValueError, match="EXCHANGE_ENABLE_WS must be a boolean"):
        owner.build_binance_live_adapter_config(
            product_ids=product_ids,
            environ=environ,
        )
    shared_owner.assert_not_called()


def test_binance_owner_preserves_shared_credential_exception_identity(
    monkeypatch,
) -> None:
    owner = _owner()
    sentinel = RuntimeError("credential-owner-sentinel")
    monkeypatch.setattr(
        owner,
        "build_ccxt_live_credentials",
        MagicMock(side_effect=sentinel),
    )

    with pytest.raises(RuntimeError) as raised:
        owner.build_binance_live_adapter_config(
            product_ids=["BINANCE:BTCUSDT-PERP"],
            environ=_valid_environ(),
        )

    assert raised.value is sentinel


@pytest.mark.parametrize("raw", [None, "", "   ", "hedge"])
def test_binance_owner_preserves_position_mode_truthiness(raw) -> None:
    environ = _valid_environ()
    if raw is None:
        environ.pop("ACCOUNT_POSITION_MODE")
        expected = "one_way"
    else:
        environ["ACCOUNT_POSITION_MODE"] = raw
        expected = raw

    config = _owner().build_binance_live_adapter_config(
        product_ids=["BINANCE:BTCUSDT-PERP"],
        environ=environ,
    )

    assert config["account_initialization"]["position_mode"] == expected


@pytest.mark.parametrize("name", ["ACCOUNT_LEVERAGE", "ACCOUNT_MARGIN_MODE"])
@pytest.mark.parametrize("raw", [None, "", "   ", "normal"])
def test_binance_owner_preserves_optional_account_field_truthiness(
    name,
    raw,
) -> None:
    environ = _valid_environ()
    if raw is None:
        environ.pop(name)
    else:
        environ[name] = raw

    account = _owner().build_binance_live_adapter_config(
        product_ids=["BINANCE:BTCUSDT-PERP"],
        environ=environ,
    )["account_initialization"]
    field = "leverage" if name == "ACCOUNT_LEVERAGE" else "margin_mode"
    if raw in {None, ""}:
        assert field not in account
    else:
        assert account[field] == raw


def test_binance_main_delegates_once_without_other_provider_owners(
    monkeypatch,
) -> None:
    _set_live_binance_env(monkeypatch)
    products = ["BINANCE:BTCUSDT-PERP"]
    result = {"owner": object()}
    owner = MagicMock(return_value=result)
    forbidden = [
        MagicMock(side_effect=AssertionError(name))
        for name in ("shared", "Rithmic", "Backpack")
    ]
    monkeypatch.setattr(strategy_main, "_env_csv", lambda _name: products)
    monkeypatch.setattr(
        strategy_main,
        "build_binance_live_adapter_config",
        owner,
        raising=False,
    )
    monkeypatch.setattr(strategy_main, "build_ccxt_live_credentials", forbidden[0])
    monkeypatch.setattr(
        strategy_main,
        "build_rithmic_live_adapter_config",
        forbidden[1],
    )
    monkeypatch.setattr(
        strategy_main,
        "build_backpack_live_adapter_config",
        forbidden[2],
    )

    config = strategy_main._adapter_config_from_env()

    assert config is result
    owner.assert_called_once_with(
        product_ids=products,
        environ=strategy_main.os.environ,
    )
    for other_owner in forbidden:
        other_owner.assert_not_called()


def test_binance_config_failure_stops_main_before_audit_and_runtime(
    monkeypatch,
) -> None:
    _set_live_binance_env(monkeypatch)
    sentinel = RuntimeError("binance-config-owner-sentinel")
    owner = MagicMock(side_effect=sentinel)
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
    monkeypatch.setattr(
        strategy_main,
        "build_binance_live_adapter_config",
        owner,
        raising=False,
    )
    for name, value in forbidden.items():
        monkeypatch.setattr(strategy_main, name, value)

    with pytest.raises(RuntimeError) as raised:
        strategy_main.main()

    assert raised.value is sentinel
    owner.assert_called_once()
    for value in forbidden.values():
        value.assert_not_called()


@pytest.mark.parametrize(
    ("exchange", "product_id"),
    [
        ("bybit", "BYBIT:BTCUSDT-PERP"),
        ("okx", "OKX:BTCUSDT-PERP"),
        ("rithmic", "RITHMIC:NQ-202609"),
        ("backpack", "BACKPACK:BTC_USDC-PERP"),
    ],
)
def test_other_live_venues_never_call_binance_owner(
    monkeypatch,
    exchange,
    product_id,
) -> None:
    owner = MagicMock(side_effect=AssertionError("Binance owner called"))
    monkeypatch.setattr(
        strategy_main,
        "build_binance_live_adapter_config",
        owner,
        raising=False,
    )
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("ADAPTER_MODE", "live")
    monkeypatch.setenv("EXCHANGE_ID", exchange)
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", product_id)
    monkeypatch.setenv("EXCHANGE_ENABLE_WS", "false")
    if exchange == "rithmic":
        result = {
            "rithmic_profile": "orders",
            "account_id": "ACCOUNT",
            "rithmic_instruments": {product_id: {"exchange": "CME"}},
            "rithmic_recovery_profile": "orders",
            "rithmic_recovery_account_id": "ACCOUNT",
        }
        monkeypatch.setattr(
            strategy_main,
            "build_rithmic_live_adapter_config",
            MagicMock(return_value=result),
        )
    elif exchange == "backpack":
        result = {"mode": "live", "exchange": "backpack"}
        monkeypatch.setattr(
            strategy_main,
            "build_backpack_live_adapter_config",
            MagicMock(return_value=result),
        )
    else:
        for name, value in {
            "EXCHANGE_API_KEY": "key",
            "EXCHANGE_SECRET": "secret",
            "EXCHANGE_TESTNET": "false",
        }.items():
            monkeypatch.setenv(name, value)
        monkeypatch.setattr(
            strategy_main,
            "build_ccxt_live_credentials",
            MagicMock(
                return_value={"api_key": "key", "secret": "secret", "testnet": False}
            ),
        )

    config = strategy_main._adapter_config_from_env()

    assert config["exchange"] == exchange
    owner.assert_not_called()


def test_simulated_config_never_calls_binance_owner(monkeypatch) -> None:
    owner = MagicMock(side_effect=AssertionError("Binance owner called"))
    monkeypatch.setattr(
        strategy_main,
        "build_binance_live_adapter_config",
        owner,
        raising=False,
    )
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
    monkeypatch.setenv("ADAPTER_MODE", "simulated")

    assert strategy_main._adapter_config_from_env() == {"mode": "simulated"}
    owner.assert_not_called()


def test_generic_main_has_one_binance_config_entrypoint() -> None:
    source = inspect.getsource(strategy_main._adapter_config_from_env)

    assert source.count("build_binance_live_adapter_config(") == 1


def test_binance_owner_has_no_variable_provider_boundary() -> None:
    owner = _owner()

    assert tuple(
        inspect.signature(owner.build_binance_live_adapter_config).parameters
    ) == ("product_ids", "environ")
    assert '"exchange": "binance"' in inspect.getsource(
        owner.build_binance_live_adapter_config
    )
