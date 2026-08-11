"""Bybit-owned live environment configuration policy tests."""

import ast
import importlib
import inspect
from unittest.mock import MagicMock

import pytest

import main as strategy_main


def _owner():
    return importlib.import_module("src.core.adapters.bybit_live_config")


def _valid_environ() -> dict[str, str]:
    return {
        "EXCHANGE_API_KEY": " key ",
        "EXCHANGE_SECRET": " secret ",
        "EXCHANGE_TESTNET": " false ",
        "EXCHANGE_ENABLE_WS": " false ",
    }


def _set_live_bybit_env(monkeypatch) -> None:
    values = {
        "FLUXTRADE_ENVIRONMENT": "live",
        "ADAPTER_MODE": "live",
        "EXCHANGE_ID": "bybit",
        "INSTRUMENT_PRODUCT_IDS": "BYBIT:BTCUSDT-PERP",
        **_valid_environ(),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    for name in (
        "ACCOUNT_POSITION_MODE",
        "ACCOUNT_LEVERAGE",
        "ACCOUNT_MARGIN_MODE",
    ):
        monkeypatch.delenv(name, raising=False)


def _forbid_runtime(monkeypatch) -> dict[str, MagicMock]:
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
    for name, value in forbidden.items():
        monkeypatch.setattr(strategy_main, name, value)
    return forbidden


def test_bybit_owner_returns_exact_current_config_and_key_order() -> None:
    products = ["BYBIT:BTCUSDT-PERP", "BYBIT:ETHUSDT-PERP"]
    environ = {
        **_valid_environ(),
        "EXCHANGE_ENABLE_WS": "true",
        "ACCOUNT_POSITION_MODE": "hedge",
        "ACCOUNT_LEVERAGE": "3",
        "ACCOUNT_MARGIN_MODE": "isolated",
    }

    config = _owner().build_bybit_live_adapter_config(
        product_ids=products,
        environ=environ,
    )

    assert config == {
        "mode": "live",
        "exchange": "bybit",
        "enable_ws": True,
        "instrument_product_ids": products,
        "account_initialization": {
            "product_ids": products,
            "position_mode": "hedge",
            "leverage": "3",
            "margin_mode": "isolated",
        },
        "api_key": "key",
        "secret": "secret",
        "testnet": False,
    }
    assert list(config) == [
        "mode",
        "exchange",
        "enable_ws",
        "instrument_product_ids",
        "account_initialization",
        "api_key",
        "secret",
        "testnet",
    ]
    assert config["instrument_product_ids"] is products
    assert config["account_initialization"]["product_ids"] is products


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
def test_bybit_owner_preserves_websocket_aliases(raw, expected) -> None:
    environ = _valid_environ()
    if raw is None:
        environ.pop("EXCHANGE_ENABLE_WS")
    else:
        environ["EXCHANGE_ENABLE_WS"] = raw

    config = _owner().build_bybit_live_adapter_config(
        product_ids=["BYBIT:BTCUSDT-PERP"],
        environ=environ,
    )

    assert config["enable_ws"] is expected


def test_bybit_owner_parses_websocket_before_credentials(monkeypatch) -> None:
    owner = _owner()
    credentials = MagicMock(side_effect=AssertionError("credentials called"))
    monkeypatch.setattr(owner, "build_ccxt_live_credentials", credentials)
    environ = _valid_environ()
    environ["EXCHANGE_ENABLE_WS"] = "enabled"

    with pytest.raises(ValueError) as raised:
        owner.build_bybit_live_adapter_config(
            product_ids=["BYBIT:BTCUSDT-PERP"],
            environ=environ,
        )

    assert raised.value.args == ("EXCHANGE_ENABLE_WS must be a boolean",)
    credentials.assert_not_called()


@pytest.mark.parametrize(
    ("updates", "expected"),
    [
        (
            {
                "EXCHANGE_ENABLE_WS": "enabled",
                "EXCHANGE_API_KEY": "",
                "EXCHANGE_SECRET": "",
                "EXCHANGE_TESTNET": "enabled",
            },
            "EXCHANGE_ENABLE_WS must be a boolean",
        ),
        (
            {
                "EXCHANGE_API_KEY": "",
                "EXCHANGE_SECRET": "",
                "EXCHANGE_TESTNET": "enabled",
            },
            "EXCHANGE_API_KEY must be set explicitly",
        ),
        (
            {"EXCHANGE_SECRET": "", "EXCHANGE_TESTNET": "enabled"},
            "EXCHANGE_SECRET must be set explicitly",
        ),
        (
            {"EXCHANGE_TESTNET": ""},
            "EXCHANGE_TESTNET must be set explicitly",
        ),
        (
            {"EXCHANGE_TESTNET": "enabled"},
            "EXCHANGE_TESTNET must be a boolean",
        ),
    ],
)
def test_bybit_owner_has_deterministic_compound_error_precedence(
    updates: dict[str, str], expected: str
) -> None:
    environ = _valid_environ()
    environ.update(updates)

    with pytest.raises(ValueError) as raised:
        _owner().build_bybit_live_adapter_config(
            product_ids=["BYBIT:BTCUSDT-PERP"],
            environ=environ,
        )

    assert raised.value.args == (expected,)


@pytest.mark.parametrize("raw", [None, "", "   ", "hedge"])
def test_bybit_owner_preserves_position_mode_truthiness(raw) -> None:
    environ = _valid_environ()
    if raw is None:
        expected = "one_way"
    else:
        environ["ACCOUNT_POSITION_MODE"] = raw
        expected = raw

    config = _owner().build_bybit_live_adapter_config(
        product_ids=["BYBIT:BTCUSDT-PERP"],
        environ=environ,
    )

    assert config["account_initialization"]["position_mode"] == expected


@pytest.mark.parametrize("name", ["ACCOUNT_LEVERAGE", "ACCOUNT_MARGIN_MODE"])
@pytest.mark.parametrize("raw", [None, "", "   ", "normal"])
def test_bybit_owner_preserves_optional_account_truthiness(name, raw) -> None:
    environ = _valid_environ()
    if raw is not None:
        environ[name] = raw
    field = "leverage" if name == "ACCOUNT_LEVERAGE" else "margin_mode"

    account = _owner().build_bybit_live_adapter_config(
        product_ids=["BYBIT:BTCUSDT-PERP"],
        environ=environ,
    )["account_initialization"]

    if raw in {None, ""}:
        assert field not in account
    else:
        assert account[field] == raw


def test_bybit_owner_calls_shared_credentials_once_and_preserves_failure(
    monkeypatch,
) -> None:
    owner = _owner()
    sentinel = RuntimeError("bybit-credentials-sentinel")
    credentials = MagicMock(side_effect=sentinel)
    environ = _valid_environ()
    monkeypatch.setattr(owner, "build_ccxt_live_credentials", credentials)

    with pytest.raises(RuntimeError) as raised:
        owner.build_bybit_live_adapter_config(
            product_ids=["BYBIT:BTCUSDT-PERP"],
            environ=environ,
        )

    assert raised.value is sentinel
    credentials.assert_called_once_with(environ)


def test_bybit_owner_uses_one_successful_shared_credential_projection(
    monkeypatch,
) -> None:
    owner = _owner()
    api_key = object()
    secret = object()
    testnet = object()
    credentials = MagicMock(
        return_value={"api_key": api_key, "secret": secret, "testnet": testnet}
    )
    environ = _valid_environ()
    monkeypatch.setattr(owner, "build_ccxt_live_credentials", credentials)

    config = owner.build_bybit_live_adapter_config(
        product_ids=["BYBIT:BTCUSDT-PERP"],
        environ=environ,
    )

    credentials.assert_called_once_with(environ)
    assert config["api_key"] is api_key
    assert config["secret"] is secret
    assert config["testnet"] is testnet


def test_bybit_main_delegates_once_without_other_provider_owners(monkeypatch) -> None:
    _set_live_bybit_env(monkeypatch)
    products = ["BYBIT:BTCUSDT-PERP"]
    monkeypatch.setattr(strategy_main, "_env_csv", lambda _name: products)
    result = {"mode": "live", "exchange": "bybit"}
    bybit = MagicMock(return_value=result)
    forbidden = {
        name: MagicMock(side_effect=AssertionError(name))
        for name in (
            "build_ccxt_live_credentials",
            "build_binance_live_adapter_config",
            "build_backpack_live_adapter_config",
            "build_rithmic_live_adapter_config",
        )
    }
    monkeypatch.setattr(
        strategy_main,
        "build_bybit_live_adapter_config",
        bybit,
        raising=False,
    )
    for name, value in forbidden.items():
        monkeypatch.setattr(strategy_main, name, value)

    assert strategy_main._adapter_config_from_env() is result
    bybit.assert_called_once_with(
        product_ids=products,
        environ=strategy_main.os.environ,
    )
    for value in forbidden.values():
        value.assert_not_called()


def test_bybit_owner_failure_precedes_audit_and_runtime(monkeypatch) -> None:
    _set_live_bybit_env(monkeypatch)
    sentinel = RuntimeError("bybit-config-owner-sentinel")
    owner = MagicMock(side_effect=sentinel)
    monkeypatch.setattr(
        strategy_main,
        "build_bybit_live_adapter_config",
        owner,
        raising=False,
    )
    forbidden = _forbid_runtime(monkeypatch)

    with pytest.raises(RuntimeError) as raised:
        strategy_main.main()

    assert raised.value is sentinel
    owner.assert_called_once()
    for value in forbidden.values():
        value.assert_not_called()


def test_bybit_product_mismatch_precedes_all_owners_and_runtime(monkeypatch) -> None:
    _set_live_bybit_env(monkeypatch)
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", "BINANCE:BTCUSDT-PERP")
    owners = {
        name: MagicMock(side_effect=AssertionError(name))
        for name in (
            "build_bybit_live_adapter_config",
            "build_binance_live_adapter_config",
            "build_backpack_live_adapter_config",
            "build_rithmic_live_adapter_config",
            "build_ccxt_live_credentials",
        )
    }
    for name, value in owners.items():
        monkeypatch.setattr(strategy_main, name, value, raising=False)
    forbidden = _forbid_runtime(monkeypatch)

    with pytest.raises(ValueError) as raised:
        strategy_main.main()

    assert raised.value.args == (
        "INSTRUMENT_PRODUCT_IDS must use BYBIT venue: BINANCE:BTCUSDT-PERP",
    )
    for value in (*owners.values(), *forbidden.values()):
        value.assert_not_called()


def test_bybit_owner_has_fixed_signature_dependencies_and_provider() -> None:
    owner = _owner()
    source = inspect.getsource(owner)
    tree = ast.parse(source)
    imports = [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
    ]

    assert tuple(
        inspect.signature(owner.build_bybit_live_adapter_config).parameters
    ) == ("product_ids", "environ")
    assert imports == [
        "from collections.abc import Mapping",
        "from src.core.adapters.ccxt_live_credentials import build_ccxt_live_credentials",
    ]
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "build_bybit_live_adapter_config"
    )
    assert not any(
        isinstance(node, ast.Import | ast.ImportFrom) for node in ast.walk(function)
    )
    exchange_values = [
        value
        for node in ast.walk(function)
        if isinstance(node, ast.Dict)
        for key, value in zip(node.keys, node.values, strict=True)
        if isinstance(key, ast.Constant) and key.value == "exchange"
    ]
    assert len(exchange_values) == 1
    assert isinstance(exchange_values[0], ast.Constant)
    assert exchange_values[0].value == "bybit"
    module_source = ast.unparse(tree).lower()
    assert "exchange_id" not in module_source
    for forbidden in (
        "account_id",
        "subaccount",
        "account_list",
        "discovery",
        "random",
        "round_robin",
        "binance",
        "backpack",
        "rithmic",
    ):
        assert forbidden not in module_source
