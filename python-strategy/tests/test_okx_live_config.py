"""OKX-owned live environment configuration policy tests."""

import ast
import importlib
import inspect
from unittest.mock import MagicMock

import pytest

import main as strategy_main


_PROVIDER_OWNERS = {
    name: f"src.core.adapters.{module}.{name}"
    for name, module in {
        "build_backpack_live_adapter_config": "backpack_live_config",
        "build_binance_live_adapter_config": "binance_live_config",
        "build_bybit_live_adapter_config": "bybit_live_config",
        "build_ccxt_live_credentials": "ccxt_live_credentials",
        "build_okx_live_adapter_config": "okx_live_config",
        "build_rithmic_live_adapter_config": "rithmic_live_config",
    }.items()
}


def _patch_provider_owner(monkeypatch, name: str, value: object) -> None:
    monkeypatch.setattr(_PROVIDER_OWNERS[name], value)


def _owner():
    return importlib.import_module("src.core.adapters.okx_live_config")


def _valid_environ() -> dict[str, str]:
    return {
        "EXCHANGE_API_KEY": " key ",
        "EXCHANGE_SECRET": " secret ",
        "EXCHANGE_TESTNET": " false ",
        "EXCHANGE_ENABLE_WS": " false ",
    }


def _set_live_okx_env(monkeypatch) -> None:
    values = {
        "FLUXTRADE_ENVIRONMENT": "live",
        "ADAPTER_MODE": "live",
        "EXCHANGE_ID": "okx",
        "INSTRUMENT_PRODUCT_IDS": "OKX:BTCUSDT-PERP",
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


def test_okx_owner_returns_exact_current_config_and_key_order() -> None:
    products = ["OKX:BTCUSDT-PERP", "OKX:ETHUSDT-PERP"]
    environ = {
        **_valid_environ(),
        "EXCHANGE_ENABLE_WS": "true",
        "ACCOUNT_POSITION_MODE": "hedge",
        "ACCOUNT_LEVERAGE": "3",
        "ACCOUNT_MARGIN_MODE": "isolated",
    }

    config = _owner().build_okx_live_adapter_config(
        product_ids=products,
        environ=environ,
    )

    assert config == {
        "mode": "live",
        "exchange": "okx",
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
def test_okx_owner_preserves_websocket_aliases(raw, expected) -> None:
    environ = _valid_environ()
    if raw is None:
        environ.pop("EXCHANGE_ENABLE_WS")
    else:
        environ["EXCHANGE_ENABLE_WS"] = raw

    config = _owner().build_okx_live_adapter_config(
        product_ids=["OKX:BTCUSDT-PERP"],
        environ=environ,
    )

    assert config["enable_ws"] is expected


def test_okx_owner_parses_websocket_before_credentials(monkeypatch) -> None:
    owner = _owner()
    credentials = MagicMock(side_effect=AssertionError("credentials called"))
    monkeypatch.setattr(owner, "build_ccxt_live_credentials", credentials)
    environ = _valid_environ()
    environ["EXCHANGE_ENABLE_WS"] = "enabled"

    with pytest.raises(ValueError) as raised:
        owner.build_okx_live_adapter_config(
            product_ids=["OKX:BTCUSDT-PERP"],
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
def test_okx_owner_has_deterministic_compound_error_precedence(
    updates: dict[str, str], expected: str
) -> None:
    environ = _valid_environ()
    environ.update(updates)

    with pytest.raises(ValueError) as raised:
        _owner().build_okx_live_adapter_config(
            product_ids=["OKX:BTCUSDT-PERP"],
            environ=environ,
        )

    assert raised.value.args == (expected,)


@pytest.mark.parametrize("raw", [None, "", "   ", "hedge"])
def test_okx_owner_preserves_position_mode_truthiness(raw) -> None:
    environ = _valid_environ()
    if raw is None:
        expected = "one_way"
    else:
        environ["ACCOUNT_POSITION_MODE"] = raw
        expected = raw

    config = _owner().build_okx_live_adapter_config(
        product_ids=["OKX:BTCUSDT-PERP"],
        environ=environ,
    )

    assert config["account_initialization"]["position_mode"] == expected


@pytest.mark.parametrize("name", ["ACCOUNT_LEVERAGE", "ACCOUNT_MARGIN_MODE"])
@pytest.mark.parametrize("raw", [None, "", "   ", "normal"])
def test_okx_owner_preserves_optional_account_truthiness(name, raw) -> None:
    environ = _valid_environ()
    if raw is not None:
        environ[name] = raw
    field = "leverage" if name == "ACCOUNT_LEVERAGE" else "margin_mode"

    account = _owner().build_okx_live_adapter_config(
        product_ids=["OKX:BTCUSDT-PERP"],
        environ=environ,
    )["account_initialization"]

    if raw in {None, ""}:
        assert field not in account
    else:
        assert account[field] == raw


def test_okx_owner_calls_shared_credentials_once_and_preserves_failure(
    monkeypatch,
) -> None:
    owner = _owner()
    sentinel = RuntimeError("okx-credentials-sentinel")
    credentials = MagicMock(side_effect=sentinel)
    environ = _valid_environ()
    monkeypatch.setattr(owner, "build_ccxt_live_credentials", credentials)

    with pytest.raises(RuntimeError) as raised:
        owner.build_okx_live_adapter_config(
            product_ids=["OKX:BTCUSDT-PERP"],
            environ=environ,
        )

    assert raised.value is sentinel
    credentials.assert_called_once_with(environ)


def test_okx_owner_uses_one_successful_shared_credential_projection(
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

    config = owner.build_okx_live_adapter_config(
        product_ids=["OKX:BTCUSDT-PERP"],
        environ=environ,
    )

    credentials.assert_called_once_with(environ)
    assert config["api_key"] is api_key
    assert config["secret"] is secret
    assert config["testnet"] is testnet


def test_okx_main_delegates_to_venue_owner_once(monkeypatch) -> None:
    _set_live_okx_env(monkeypatch)
    products = ["OKX:BTCUSDT-PERP"]
    monkeypatch.setattr(strategy_main, "_env_csv", lambda _name: products)
    result = {"mode": "live", "exchange": "okx"}
    okx = MagicMock(return_value=result)
    _patch_provider_owner(monkeypatch, "build_okx_live_adapter_config", okx)
    assert strategy_main._adapter_config_from_env() is result
    okx.assert_called_once_with(
        product_ids=products,
        environ=strategy_main.os.environ,
    )


def test_okx_owner_failure_precedes_audit_and_runtime(monkeypatch) -> None:
    _set_live_okx_env(monkeypatch)
    sentinel = RuntimeError("okx-config-owner-sentinel")
    owner = MagicMock(side_effect=sentinel)
    _patch_provider_owner(monkeypatch, "build_okx_live_adapter_config", owner)
    forbidden = _forbid_runtime(monkeypatch)

    with pytest.raises(RuntimeError) as raised:
        strategy_main.main()

    assert raised.value is sentinel
    owner.assert_called_once()
    for value in forbidden.values():
        value.assert_not_called()


def test_okx_product_mismatch_precedes_all_owners_and_runtime(monkeypatch) -> None:
    _set_live_okx_env(monkeypatch)
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", "BINANCE:BTCUSDT-PERP")
    owner = MagicMock(side_effect=AssertionError("composition owner called"))
    monkeypatch.setattr(strategy_main, "build_live_adapter_config", owner)
    forbidden = _forbid_runtime(monkeypatch)

    with pytest.raises(ValueError) as raised:
        strategy_main.main()

    assert raised.value.args == (
        "INSTRUMENT_PRODUCT_IDS must use OKX venue: BINANCE:BTCUSDT-PERP",
    )
    owner.assert_not_called()
    for value in forbidden.values():
        value.assert_not_called()


@pytest.mark.parametrize(
    ("exchange", "product_id", "selected_owner"),
    [
        ("binance", "BINANCE:BTCUSDT-PERP", "build_binance_live_adapter_config"),
        ("bybit", "BYBIT:BTCUSDT-PERP", "build_bybit_live_adapter_config"),
        (
            "backpack",
            "BACKPACK:BTC_USDC-PERP",
            "build_backpack_live_adapter_config",
        ),
        ("rithmic", "RITHMIC:NQ-202609", "build_rithmic_live_adapter_config"),
        ("kraken", "KRAKEN:BTCUSDT-PERP", "build_ccxt_live_credentials"),
    ],
)
def test_non_okx_live_routes_never_call_okx_owner(
    monkeypatch, exchange: str, product_id: str, selected_owner: str
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("ADAPTER_MODE", "live")
    monkeypatch.setenv("EXCHANGE_ID", exchange)
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", product_id)
    monkeypatch.setenv("EXCHANGE_ENABLE_WS", "false")
    for name, value in _valid_environ().items():
        monkeypatch.setenv(name, value)
    products = [product_id]
    monkeypatch.setattr(strategy_main, "_env_csv", lambda _name: products)
    okx = MagicMock(side_effect=AssertionError("OKX owner called"))
    selected_result = (
        {"api_key": "key", "secret": "secret", "testnet": False}
        if selected_owner == "build_ccxt_live_credentials"
        else {"mode": "live", "exchange": exchange}
    )
    selected = MagicMock(return_value=selected_result)
    _patch_provider_owner(monkeypatch, "build_okx_live_adapter_config", okx)
    _patch_provider_owner(monkeypatch, selected_owner, selected)

    config = strategy_main._adapter_config_from_env()

    okx.assert_not_called()
    if selected_owner == "build_ccxt_live_credentials":
        selected.assert_called_once_with(strategy_main.os.environ)
    else:
        selected.assert_called_once_with(
            product_ids=products,
            environ=strategy_main.os.environ,
        )
    assert config["exchange"] == exchange


def test_simulated_route_never_calls_okx_owner(monkeypatch) -> None:
    owner = MagicMock(side_effect=AssertionError("OKX owner called"))
    _patch_provider_owner(monkeypatch, "build_okx_live_adapter_config", owner)
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
    monkeypatch.setenv("ADAPTER_MODE", "simulated")

    assert strategy_main._adapter_config_from_env() == {"mode": "simulated"}
    owner.assert_not_called()


def test_okx_owner_has_fixed_signature_dependencies_and_provider() -> None:
    owner = _owner()
    source = inspect.getsource(owner)
    tree = ast.parse(source)
    imports = [
        ast.unparse(node)
        for node in ast.walk(tree)
        if isinstance(node, ast.Import | ast.ImportFrom)
    ]

    assert tuple(inspect.signature(owner.build_okx_live_adapter_config).parameters) == (
        "product_ids",
        "environ",
    )
    assert imports == [
        "from collections.abc import Mapping",
        "from src.core.adapters.ccxt_live_credentials import build_ccxt_live_credentials",
    ]
    function = next(
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name == "build_okx_live_adapter_config"
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
    assert exchange_values[0].value == "okx"
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
        "bybit",
        "backpack",
        "rithmic",
    ):
        assert forbidden not in module_source
