import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src import main as strategy_main
from src.core import adapter_runtime_composition


def _forbid_runtime_initialization(monkeypatch) -> dict[str, MagicMock]:
    owners = {
        name: MagicMock(side_effect=AssertionError(f"{name} initialized"))
        for name in (
            "DataConsumer",
            "SessionLocal",
            "StrategyEngine",
            "configure_metrics",
        )
    }
    for name, owner in owners.items():
        monkeypatch.setattr(strategy_main, name, owner)
    return owners


def _assert_initialization_not_called(owners: dict[str, MagicMock]) -> None:
    for owner in owners.values():
        owner.assert_not_called()


def _set_live_ccxt_env(monkeypatch) -> None:
    values = {
        "FLUXTRADE_ENVIRONMENT": "live",
        "ADAPTER_MODE": "live",
        "EXCHANGE_ID": "binance",
        "INSTRUMENT_PRODUCT_IDS": "BINANCE:BTCUSDT-PERP",
        "EXCHANGE_API_KEY": "key",
        "EXCHANGE_SECRET": "secret",
        "EXCHANGE_TESTNET": "true",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def _set_live_rithmic_env(monkeypatch) -> None:
    values = {
        "FLUXTRADE_ENVIRONMENT": "live",
        "ADAPTER_MODE": "live",
        "EXCHANGE_ID": "rithmic",
        "INSTRUMENT_PRODUCT_IDS": "RITHMIC:NQ-202609",
        "RITHMIC_PROFILE": "orders",
        "RITHMIC_ACCOUNT_ID": "ACCOUNT",
        "RITHMIC_INSTRUMENTS_JSON": (
            '{"RITHMIC:NQ-202609":{"exchange":"CME","quantity_step":"1",'
            '"price_tick":"0.25"}}'
        ),
        "FLUXTRADE_CREDENTIALS_PATH": str(Path(__file__)),
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


@pytest.mark.parametrize(
    ("environment", "adapter_mode", "production_policy"),
    [
        ("live", "live", True),
        ("live", "simulated", True),
        ("test", "live", False),
        ("test", "simulated", False),
    ],
)
def test_strategy_artifact_policy_is_selected_only_by_environment(
    monkeypatch,
    environment,
    adapter_mode,
    production_policy,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", environment)
    monkeypatch.setenv("ADAPTER_MODE", adapter_mode)
    production_scan = MagicMock(return_value={})
    local_scan = MagicMock(return_value={})
    monkeypatch.setattr(
        strategy_main.StrategyLoader,
        "scan_production_sources",
        production_scan,
        raising=False,
    )
    monkeypatch.setattr(strategy_main.StrategyLoader, "scan_directory", local_scan)

    loader = strategy_main._strategy_artifact_loader_from_env()
    assert loader() == {}

    if production_policy:
        production_scan.assert_called_once_with(
            "/app/strategy_artifacts",
            break_glass_path=None,
        )
        local_scan.assert_not_called()
    else:
        local_scan.assert_called_once_with("/app/strategies_hot")
        production_scan.assert_not_called()


def test_live_strategy_artifact_primary_path_cannot_bypass_break_glass(
    monkeypatch,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("STRATEGY_ARTIFACTS_PATH", "/private/tmp/operator-source")

    with pytest.raises(
        ValueError,
        match="STRATEGY_ARTIFACTS_PATH must be /app/strategy_artifacts in live",
    ):
        strategy_main._strategy_artifact_loader_from_env()


@pytest.mark.parametrize("raw_flag", ["enabled", "sometimes"])
def test_invalid_break_glass_flag_preserves_env_flag_error(
    monkeypatch,
    raw_flag,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("STRATEGY_BREAK_GLASS_ENABLED", raw_flag)

    with pytest.raises(
        ValueError,
        match="STRATEGY_BREAK_GLASS_ENABLED must be a boolean",
    ):
        strategy_main._strategy_artifact_loader_from_env()


@pytest.mark.parametrize("raw_path", [None, "", "   "])
def test_enabled_break_glass_requires_nonblank_path(monkeypatch, raw_path) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("STRATEGY_BREAK_GLASS_ENABLED", "true")
    if raw_path is None:
        monkeypatch.delenv("STRATEGY_BREAK_GLASS_PATH", raising=False)
    else:
        monkeypatch.setenv("STRATEGY_BREAK_GLASS_PATH", raw_path)

    with pytest.raises(
        ValueError,
        match=("STRATEGY_BREAK_GLASS_PATH must be set when break-glass is enabled"),
    ):
        strategy_main._strategy_artifact_loader_from_env()


def test_disabled_break_glass_does_not_read_path_or_warn(monkeypatch) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("STRATEGY_BREAK_GLASS_ENABLED", "false")
    monkeypatch.setenv("STRATEGY_BREAK_GLASS_PATH", "/must-not-be-read")
    real_getenv = strategy_main.os.getenv
    reads: list[str] = []

    def recording_getenv(name, default=None):
        reads.append(name)
        return real_getenv(name, default)

    production_scan = MagicMock(return_value={})
    monkeypatch.setattr(strategy_main.os, "getenv", recording_getenv)
    monkeypatch.setattr(
        strategy_main.StrategyLoader,
        "scan_production_sources",
        production_scan,
        raising=False,
    )
    warning = MagicMock()
    monkeypatch.setattr(strategy_main.logger, "warning", warning)

    loader = strategy_main._strategy_artifact_loader_from_env()
    loader()

    assert "STRATEGY_BREAK_GLASS_PATH" not in reads
    warning.assert_not_called()
    production_scan.assert_called_once_with(
        "/app/strategy_artifacts",
        break_glass_path=None,
    )


def test_enabled_break_glass_warns_before_source_validation(monkeypatch) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.setenv("STRATEGY_BREAK_GLASS_ENABLED", "true")
    monkeypatch.setenv("STRATEGY_BREAK_GLASS_PATH", "/read-only/operator-pack")
    events: list[str] = []
    warning = MagicMock(side_effect=lambda *_args: events.append("warning"))
    production_scan = MagicMock(
        side_effect=lambda *_args, **_kwargs: events.append("scan") or {}
    )
    monkeypatch.setattr(strategy_main.logger, "warning", warning)
    monkeypatch.setattr(
        strategy_main.StrategyLoader,
        "scan_production_sources",
        production_scan,
        raising=False,
    )

    loader = strategy_main._strategy_artifact_loader_from_env()
    loader()

    assert events == ["warning", "scan"]
    warning.assert_called_once_with("Strategy break-glass artifact source enabled")
    production_scan.assert_called_once_with(
        "/app/strategy_artifacts",
        break_glass_path="/read-only/operator-pack",
    )


@pytest.mark.parametrize(
    ("environment_update", "expected_message"),
    [
        (
            {"STRATEGY_ARTIFACTS_PATH": "/alternate"},
            "STRATEGY_ARTIFACTS_PATH must be /app/strategy_artifacts in live",
        ),
        (
            {"STRATEGY_BREAK_GLASS_ENABLED": "ambiguous"},
            "STRATEGY_BREAK_GLASS_ENABLED must be a boolean",
        ),
        (
            {"STRATEGY_BREAK_GLASS_ENABLED": "true"},
            "STRATEGY_BREAK_GLASS_PATH must be set when break-glass is enabled",
        ),
    ],
)
def test_main_strategy_source_config_failure_precedes_runtime_initialization(
    monkeypatch,
    environment_update,
    expected_message,
) -> None:
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")
    monkeypatch.delenv("STRATEGY_ARTIFACTS_PATH", raising=False)
    monkeypatch.delenv("STRATEGY_BREAK_GLASS_ENABLED", raising=False)
    monkeypatch.delenv("STRATEGY_BREAK_GLASS_PATH", raising=False)
    for name, value in environment_update.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setattr(
        strategy_main,
        "_adapter_config_from_env",
        lambda: {"mode": "simulated"},
    )
    monkeypatch.setattr(strategy_main, "_required_env_flag", lambda _name: True)
    initialization = _forbid_runtime_initialization(monkeypatch)

    with pytest.raises(ValueError, match=expected_message):
        strategy_main.main()

    _assert_initialization_not_called(initialization)


def test_env_flag_parses_truthy_values(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
    assert strategy_main._env_flag("AUDIT_EXTERNAL_ORDERS") is True

    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "0")
    assert strategy_main._env_flag("AUDIT_EXTERNAL_ORDERS") is False


def test_env_flag_rejects_ambiguous_value(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "enabled")

    with pytest.raises(ValueError, match="AUDIT_EXTERNAL_ORDERS must be a boolean"):
        strategy_main._env_flag("AUDIT_EXTERNAL_ORDERS")


def test_env_nonnegative_int_uses_default_and_override(monkeypatch) -> None:
    monkeypatch.delenv("MARKET_PENDING_CLAIM_IDLE_MS", raising=False)
    assert (
        strategy_main._env_nonnegative_int(
            "MARKET_PENDING_CLAIM_IDLE_MS",
            60_000,
        )
        == 60_000
    )

    monkeypatch.setenv("MARKET_PENDING_CLAIM_IDLE_MS", "2500")
    assert (
        strategy_main._env_nonnegative_int(
            "MARKET_PENDING_CLAIM_IDLE_MS",
            60_000,
        )
        == 2500
    )


def test_env_nonnegative_int_rejects_negative_value(monkeypatch) -> None:
    monkeypatch.setenv("MARKET_PENDING_CLAIM_IDLE_MS", "-1")

    with pytest.raises(
        ValueError,
        match="MARKET_PENDING_CLAIM_IDLE_MS must be non-negative",
    ):
        strategy_main._env_nonnegative_int(
            "MARKET_PENDING_CLAIM_IDLE_MS",
            60_000,
        )


def test_env_positive_int_uses_default_and_rejects_zero(monkeypatch) -> None:
    monkeypatch.delenv("MARKET_CONSUMER_LEASE_MS", raising=False)
    assert (
        strategy_main._env_positive_int(
            "MARKET_CONSUMER_LEASE_MS",
            10_000,
        )
        == 10_000
    )

    monkeypatch.setenv("MARKET_CONSUMER_LEASE_MS", "0")
    with pytest.raises(
        ValueError,
        match="MARKET_CONSUMER_LEASE_MS must be positive",
    ):
        strategy_main._env_positive_int(
            "MARKET_CONSUMER_LEASE_MS",
            10_000,
        )


def test_adapter_config_from_env_defaults_to_simulated(monkeypatch) -> None:
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.delenv("EXCHANGE_MODE", raising=False)
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)

    assert strategy_main._adapter_config_from_env() == {"mode": "simulated"}


def test_live_environment_requires_explicit_adapter_mode(monkeypatch) -> None:
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.delenv("EXCHANGE_MODE", raising=False)
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")

    with pytest.raises(
        ValueError,
        match="ADAPTER_MODE must be set explicitly",
    ):
        strategy_main._adapter_config_from_env()


def test_live_environment_does_not_accept_legacy_exchange_mode(monkeypatch) -> None:
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.setenv("EXCHANGE_MODE", "live")
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "live")

    with pytest.raises(ValueError, match="ADAPTER_MODE must be set explicitly"):
        strategy_main._adapter_config_from_env()


def test_adapter_config_from_env_wires_live_account_initialization(monkeypatch) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.setenv("EXCHANGE_ENABLE_WS", "false")
    monkeypatch.setenv("ACCOUNT_LEVERAGE", "3")
    monkeypatch.setenv("ACCOUNT_MARGIN_MODE", "isolated")
    monkeypatch.setenv("ACCOUNT_POSITION_MODE", "one_way")

    config = strategy_main._adapter_config_from_env()

    assert config == {
        "mode": "live",
        "exchange": "binance",
        "api_key": "key",
        "secret": "secret",
        "testnet": True,
        "enable_ws": False,
        "instrument_product_ids": ["BINANCE:BTCUSDT-PERP"],
        "balance_asset": "USDT",
        "account_initialization": {
            "product_ids": ["BINANCE:BTCUSDT-PERP"],
            "position_mode": "one_way",
            "leverage": "3",
            "margin_mode": "isolated",
        },
    }


@pytest.mark.parametrize(
    "env_name",
    [
        "EXCHANGE_ID",
        "INSTRUMENT_PRODUCT_IDS",
        "EXCHANGE_API_KEY",
        "EXCHANGE_SECRET",
        "EXCHANGE_TESTNET",
    ],
)
def test_live_ccxt_config_requires_explicit_runtime_values(
    monkeypatch,
    env_name,
) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.delenv(env_name)

    with pytest.raises(ValueError, match=env_name):
        strategy_main._adapter_config_from_env()


@pytest.mark.parametrize(
    "product_ids",
    [
        "BYBIT:BTCUSDT-PERP",
        "BINANCE:BTCUSDT-PERP,BYBIT:ETHUSDT-PERP",
    ],
)
def test_live_ccxt_products_must_match_selected_venue(
    monkeypatch,
    product_ids,
) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", product_ids)

    with pytest.raises(ValueError, match="must use BINANCE venue"):
        strategy_main._adapter_config_from_env()


def test_adapter_config_from_env_rejects_unknown_mode(monkeypatch) -> None:
    monkeypatch.setenv("ADAPTER_MODE", "simulation")

    with pytest.raises(ValueError, match="unsupported_adapter_mode"):
        strategy_main._adapter_config_from_env()


def test_adapter_config_from_env_wires_rithmic_live_identity(monkeypatch) -> None:
    _set_live_rithmic_env(monkeypatch)
    monkeypatch.setenv("RITHMIC_PROFILE", " orders ")
    monkeypatch.setenv("RITHMIC_RECOVERY_PROFILE", " test ")
    monkeypatch.setenv("RITHMIC_ACCOUNT_ID", " ACCOUNT ")

    config = strategy_main._adapter_config_from_env()

    assert config["rithmic_profile"] == "orders"
    assert config["account_id"] == "ACCOUNT"
    assert config["rithmic_recovery_profile"] == "test"
    assert config["rithmic_recovery_account_id"] == "ACCOUNT"


def test_rithmic_live_config_delegates_to_venue_owner_exactly_once(
    monkeypatch,
) -> None:
    _set_live_rithmic_env(monkeypatch)
    owner_result = {
        "rithmic_profile": "OWNER-PROFILE",
        "account_id": "OWNER-ACCOUNT",
        "rithmic_instruments": {"owner": "result"},
        "rithmic_recovery_profile": "OWNER-RECOVERY",
        "rithmic_recovery_account_id": "OWNER-ACCOUNT",
    }
    owner = MagicMock(return_value=owner_result)
    monkeypatch.setattr(
        "src.core.adapters.rithmic_live_config.build_rithmic_live_adapter_config",
        owner,
    )

    config = strategy_main._adapter_config_from_env()

    owner.assert_called_once()
    assert owner.call_args.kwargs["product_ids"] == ["RITHMIC:NQ-202609"]
    assert owner.call_args.kwargs["environ"] is strategy_main.os.environ
    assert {key: config[key] for key in owner_result} == owner_result


def test_generic_main_has_one_rithmic_policy_entrypoint() -> None:
    source = inspect.getsource(adapter_runtime_composition.build_live_adapter_config)

    assert source.count("build_rithmic_live_adapter_config(") == 1
    assert "RITHMIC_" not in source
    assert "json." not in source
    assert "Path(" not in source
    assert ".is_file(" not in source


@pytest.mark.parametrize("mode", ["simulated", "binance"])
def test_non_rithmic_config_never_calls_rithmic_owner(monkeypatch, mode) -> None:
    owner = MagicMock(side_effect=AssertionError("Rithmic owner called"))
    monkeypatch.setattr(
        "src.core.adapters.rithmic_live_config.build_rithmic_live_adapter_config",
        owner,
    )
    monkeypatch.setenv("RITHMIC_ACCOUNT_ID", "MISLEADING")
    if mode == "simulated":
        monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
        monkeypatch.setenv("ADAPTER_MODE", "simulated")
    else:
        _set_live_ccxt_env(monkeypatch)

    strategy_main._adapter_config_from_env()

    owner.assert_not_called()


def test_simulated_config_never_calls_ccxt_credential_owner(monkeypatch) -> None:
    owner = MagicMock(side_effect=AssertionError("CCXT credential owner called"))
    rithmic_owner = MagicMock(side_effect=AssertionError("Rithmic owner called"))
    monkeypatch.setattr(
        "src.core.adapters.ccxt_live_credentials.build_ccxt_live_credentials",
        owner,
    )
    monkeypatch.setattr(
        "src.core.adapters.rithmic_live_config.build_rithmic_live_adapter_config",
        rithmic_owner,
    )
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
    monkeypatch.setenv("ADAPTER_MODE", "simulated")

    assert strategy_main._adapter_config_from_env() == {"mode": "simulated"}
    owner.assert_not_called()
    rithmic_owner.assert_not_called()


@pytest.mark.parametrize("enable_ws", ["true", "enabled"])
def test_rithmic_config_never_calls_ccxt_credential_owner(
    monkeypatch,
    enable_ws,
) -> None:
    _set_live_rithmic_env(monkeypatch)
    monkeypatch.setenv("EXCHANGE_ENABLE_WS", enable_ws)
    owner = MagicMock(side_effect=AssertionError("CCXT credential owner called"))
    rithmic_result = {
        "rithmic_profile": "orders",
        "account_id": "ACCOUNT",
        "rithmic_instruments": {
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        "rithmic_recovery_profile": "orders",
        "rithmic_recovery_account_id": "ACCOUNT",
    }
    rithmic_owner = MagicMock(return_value=rithmic_result)
    monkeypatch.setattr(
        "src.core.adapters.ccxt_live_credentials.build_ccxt_live_credentials",
        owner,
    )
    monkeypatch.setattr(
        "src.core.adapters.rithmic_live_config.build_rithmic_live_adapter_config",
        rithmic_owner,
    )

    if enable_ws == "enabled":
        with pytest.raises(
            ValueError,
            match="EXCHANGE_ENABLE_WS must be a boolean",
        ):
            strategy_main._adapter_config_from_env()
    else:
        config = strategy_main._adapter_config_from_env()
        assert config == {
            "mode": "live",
            "exchange": "rithmic",
            "enable_ws": True,
            "instrument_product_ids": ["RITHMIC:NQ-202609"],
            "account_initialization": {
                "product_ids": ["RITHMIC:NQ-202609"],
                "position_mode": "one_way",
            },
            "rithmic_profile": "orders",
            "account_id": "ACCOUNT",
            "rithmic_instruments": {
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            "rithmic_recovery_profile": "orders",
            "rithmic_recovery_account_id": "ACCOUNT",
        }
        rithmic_owner.assert_called_once()
        assert (
            rithmic_owner.call_args.kwargs["product_ids"]
            is config["instrument_product_ids"]
        )
        assert (
            config["instrument_product_ids"]
            is config["account_initialization"]["product_ids"]
        )

    owner.assert_not_called()
    if enable_ws == "enabled":
        rithmic_owner.assert_not_called()


def test_generic_live_config_fails_closed_before_ccxt_owner(
    monkeypatch,
) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.setenv("EXCHANGE_ID", "kraken")
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", "KRAKEN:BTCUSDT-PERP")
    product_id = "KRAKEN:BTCUSDT-PERP"
    products = [product_id]
    monkeypatch.setattr(strategy_main, "_env_csv", lambda _name: products)
    owner = MagicMock(side_effect=AssertionError("CCXT owner called"))
    rithmic_owner = MagicMock(side_effect=AssertionError("Rithmic owner called"))
    monkeypatch.setattr(
        "src.core.adapters.ccxt_live_credentials.build_ccxt_live_credentials",
        owner,
    )
    monkeypatch.setattr(
        "src.core.adapters.rithmic_live_config.build_rithmic_live_adapter_config",
        rithmic_owner,
    )

    with pytest.raises(ValueError) as raised:
        strategy_main._adapter_config_from_env()

    assert raised.value.args == (
        "unsupported_or_unavailable_live_execution_venue: exchange=kraken",
    )
    owner.assert_not_called()
    rithmic_owner.assert_not_called()


def test_incomplete_live_venue_precedes_audit_and_initialization(
    monkeypatch,
) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.setenv("EXCHANGE_ID", "kraken")
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", "KRAKEN:BTCUSDT-PERP")
    owner = MagicMock(side_effect=AssertionError("CCXT owner called"))
    audit_reader = MagicMock(side_effect=AssertionError("audit read"))
    initialization = _forbid_runtime_initialization(monkeypatch)
    adapter_factory = MagicMock(side_effect=AssertionError("adapter created"))
    monkeypatch.setattr(
        "src.core.adapters.ccxt_live_credentials.build_ccxt_live_credentials",
        owner,
    )
    monkeypatch.setattr(strategy_main, "_required_env_flag", audit_reader)
    monkeypatch.setattr(strategy_main, "create_adapter", adapter_factory)

    with pytest.raises(ValueError) as raised:
        strategy_main.main()

    assert raised.value.args == (
        "unsupported_or_unavailable_live_execution_venue: exchange=kraken",
    )
    owner.assert_not_called()
    audit_reader.assert_not_called()
    adapter_factory.assert_not_called()
    _assert_initialization_not_called(initialization)


def test_ambiguous_websocket_precedes_ccxt_credential_owner(monkeypatch) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.setenv("EXCHANGE_ENABLE_WS", "enabled")
    owner = MagicMock(side_effect=AssertionError("CCXT credential owner called"))
    monkeypatch.setattr(
        "src.core.adapters.ccxt_live_credentials.build_ccxt_live_credentials",
        owner,
    )

    with pytest.raises(ValueError) as raised:
        strategy_main._adapter_config_from_env()

    assert raised.value.args == ("EXCHANGE_ENABLE_WS must be a boolean",)
    owner.assert_not_called()


@pytest.mark.parametrize("raw", [None, "", "   ", "hedge"])
def test_account_position_mode_preserves_existing_truthiness(
    monkeypatch,
    raw,
) -> None:
    _set_live_ccxt_env(monkeypatch)
    if raw is None:
        monkeypatch.delenv("ACCOUNT_POSITION_MODE", raising=False)
        expected = "one_way"
    else:
        monkeypatch.setenv("ACCOUNT_POSITION_MODE", raw)
        expected = raw

    config = strategy_main._adapter_config_from_env()

    assert config["account_initialization"]["position_mode"] == expected


@pytest.mark.parametrize("name", ["ACCOUNT_LEVERAGE", "ACCOUNT_MARGIN_MODE"])
@pytest.mark.parametrize("raw", [None, "", "   ", "normal"])
def test_optional_account_fields_preserve_raw_truthiness(
    monkeypatch,
    name,
    raw,
) -> None:
    _set_live_ccxt_env(monkeypatch)
    if raw is None:
        monkeypatch.delenv(name, raising=False)
    else:
        monkeypatch.setenv(name, raw)

    account = strategy_main._adapter_config_from_env()["account_initialization"]
    field = "leverage" if name == "ACCOUNT_LEVERAGE" else "margin_mode"
    if raw in {None, ""}:
        assert field not in account
    else:
        assert account[field] == raw


def test_entrypoint_composition_has_no_generic_credential_fallback() -> None:
    source = inspect.getsource(adapter_runtime_composition.build_live_adapter_config)

    assert "build_ccxt_live_credentials(" not in source
    assert "build_okx_live_adapter_config(" not in source
    assert "EXCHANGE_API_KEY" not in source
    assert "EXCHANGE_SECRET" not in source
    assert "EXCHANGE_TESTNET" not in source


def test_common_product_failure_precedes_rithmic_owner_and_audit_reader(
    monkeypatch,
) -> None:
    _set_live_rithmic_env(monkeypatch)
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", "BINANCE:BTCUSDT-PERP")
    owner = MagicMock(side_effect=AssertionError("Rithmic owner called"))
    audit_reader = MagicMock(side_effect=AssertionError("audit read"))
    initialization = _forbid_runtime_initialization(monkeypatch)
    monkeypatch.setattr(
        strategy_main,
        "build_live_adapter_config",
        owner,
    )
    monkeypatch.setattr(strategy_main, "_required_env_flag", audit_reader)

    with pytest.raises(ValueError, match="must use RITHMIC venue"):
        strategy_main.main()

    owner.assert_not_called()
    audit_reader.assert_not_called()
    _assert_initialization_not_called(initialization)


def test_rithmic_owner_failure_precedes_audit_and_initialization(monkeypatch) -> None:
    _set_live_rithmic_env(monkeypatch)
    sentinel = RuntimeError("rithmic-config-owner-sentinel")
    owner = MagicMock(side_effect=sentinel)
    audit_reader = MagicMock(side_effect=AssertionError("audit read"))
    initialization = _forbid_runtime_initialization(monkeypatch)
    monkeypatch.setattr(
        "src.core.adapters.rithmic_live_config.build_rithmic_live_adapter_config",
        owner,
    )
    monkeypatch.setattr(strategy_main, "_required_env_flag", audit_reader)

    with pytest.raises(RuntimeError) as raised:
        strategy_main.main()

    assert raised.value is sentinel
    owner.assert_called_once()
    audit_reader.assert_not_called()
    _assert_initialization_not_called(initialization)


def test_live_audit_failure_precedes_recovery_identity_validation(
    monkeypatch,
) -> None:
    adapter_config = {
        "mode": "live",
        "exchange": "rithmic",
        "rithmic_recovery_profile": "orders",
    }
    validator = MagicMock(side_effect=AssertionError("identity validation reached"))
    initialization = _forbid_runtime_initialization(monkeypatch)
    monkeypatch.setattr(
        strategy_main, "_adapter_config_from_env", lambda: adapter_config
    )
    monkeypatch.setattr(strategy_main, "_required_env_flag", lambda _name: False)
    monkeypatch.setattr(
        "src.core.adapters.rithmic_live_config.validate_rithmic_recovery_identity",
        validator,
    )

    with pytest.raises(ValueError, match="live_adapter_requires_audit_external_orders"):
        strategy_main.main()

    validator.assert_not_called()
    _assert_initialization_not_called(initialization)


def test_runtime_validation_delegates_recovery_identity_after_audit(
    monkeypatch,
) -> None:
    adapter_config = {
        "mode": "live",
        "exchange": "rithmic",
        "rithmic_recovery_profile": "orders",
        "rithmic_recovery_account_id": "ACCOUNT",
    }
    sentinel = RuntimeError("rithmic-identity-validator-sentinel")
    validator = MagicMock(side_effect=sentinel)
    monkeypatch.setattr(
        "src.core.adapters.rithmic_live_config.validate_rithmic_recovery_identity",
        validator,
    )

    with pytest.raises(RuntimeError) as raised:
        strategy_main._validate_runtime_config(
            adapter_config,
            audit_external_orders=True,
        )

    assert raised.value is sentinel
    validator.assert_called_once_with(adapter_config)


def test_rithmic_recovery_profile_defaults_to_order_profile(monkeypatch) -> None:
    _set_live_rithmic_env(monkeypatch)
    monkeypatch.delenv("RITHMIC_RECOVERY_PROFILE", raising=False)

    config = strategy_main._adapter_config_from_env()

    assert config["rithmic_recovery_profile"] == "orders"


@pytest.mark.parametrize(
    "env_name",
    [
        "RITHMIC_PROFILE",
        "RITHMIC_ACCOUNT_ID",
        "RITHMIC_INSTRUMENTS_JSON",
        "FLUXTRADE_CREDENTIALS_PATH",
    ],
)
def test_rithmic_config_requires_explicit_runtime_values(
    monkeypatch,
    env_name,
) -> None:
    _set_live_rithmic_env(monkeypatch)
    monkeypatch.delenv(env_name)

    with pytest.raises(ValueError, match=env_name):
        strategy_main._adapter_config_from_env()


@pytest.mark.parametrize(
    "raw_value",
    ["not-json", "[]", "{}", "null"],
)
def test_rithmic_instrument_map_rejects_invalid_json_object(
    monkeypatch,
    raw_value,
) -> None:
    _set_live_rithmic_env(monkeypatch)
    monkeypatch.setenv("RITHMIC_INSTRUMENTS_JSON", raw_value)

    with pytest.raises(ValueError, match="RITHMIC_INSTRUMENTS_JSON"):
        strategy_main._adapter_config_from_env()


def test_rithmic_credentials_path_must_exist(monkeypatch, tmp_path) -> None:
    _set_live_rithmic_env(monkeypatch)
    monkeypatch.setenv(
        "FLUXTRADE_CREDENTIALS_PATH",
        str(tmp_path / "missing.toml"),
    )

    with pytest.raises(
        ValueError,
        match="FLUXTRADE_CREDENTIALS_PATH must reference a file",
    ):
        strategy_main._adapter_config_from_env()


def test_rithmic_instrument_map_must_match_subscribed_products(
    monkeypatch,
) -> None:
    _set_live_rithmic_env(monkeypatch)
    monkeypatch.setenv(
        "RITHMIC_INSTRUMENTS_JSON",
        '{"RITHMIC:MNQ-202609":{"exchange":"CME"}}',
    )

    with pytest.raises(
        ValueError,
        match="keys must match INSTRUMENT_PRODUCT_IDS",
    ):
        strategy_main._adapter_config_from_env()


def test_validate_runtime_config_rejects_live_without_audit() -> None:
    with pytest.raises(ValueError, match="live_adapter_requires_audit_external_orders"):
        strategy_main._validate_runtime_config(
            {"mode": "live"},
            audit_external_orders=False,
        )


def test_validate_runtime_config_allows_simulated_without_audit() -> None:
    strategy_main._validate_runtime_config(
        {"mode": "simulated"},
        audit_external_orders=False,
    )


def test_validate_runtime_config_allows_live_with_audit() -> None:
    strategy_main._validate_runtime_config(
        {"mode": "live", "exchange": "binance", "balance_asset": "USDT"},
        audit_external_orders=True,
    )


def test_validate_runtime_config_requires_rithmic_account_id() -> None:
    with pytest.raises(ValueError, match="rithmic_recovery_requires_account_id"):
        strategy_main._validate_runtime_config(
            {
                "mode": "live",
                "rithmic_recovery_profile": "test",
            },
            audit_external_orders=True,
        )


def test_adapter_config_preserves_incomplete_identity_for_validation(
    monkeypatch,
) -> None:
    with pytest.raises(
        ValueError, match="rithmic_account_id_requires_recovery_profile"
    ):
        strategy_main._validate_runtime_config(
            {
                "mode": "live",
                "rithmic_recovery_account_id": "ACCOUNT",
            },
            audit_external_orders=True,
        )


def test_main_rejects_live_without_audit_before_initialization(monkeypatch) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "false")

    with (
        patch("src.main.configure_metrics") as configure_metrics,
        patch("src.main.SessionLocal") as session_local,
        patch("src.main.StrategyEngine") as engine_cls,
    ):
        with pytest.raises(
            ValueError, match="live_adapter_requires_audit_external_orders"
        ):
            strategy_main.main()

    configure_metrics.assert_not_called()
    session_local.assert_not_called()
    engine_cls.assert_not_called()


def test_main_requires_explicit_live_audit_flag(monkeypatch) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.delenv("AUDIT_EXTERNAL_ORDERS", raising=False)

    with (
        patch("src.main.configure_metrics") as configure_metrics,
        patch("src.main.SessionLocal") as session_local,
    ):
        with pytest.raises(
            ValueError,
            match="AUDIT_EXTERNAL_ORDERS must be set explicitly",
        ):
            strategy_main.main()

    configure_metrics.assert_not_called()
    session_local.assert_not_called()


def test_main_wires_session_factory_and_audit_flag(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.delenv("EXCHANGE_MODE", raising=False)
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
    db_session = MagicMock()
    engine = MagicMock()
    engine.build_stream_channels.return_value = []
    consumer = MagicMock(spec=strategy_main.DataConsumer)
    events = []
    consumer.acquire_service_ownership.side_effect = lambda: events.append("ownership")
    engine.startup.side_effect = lambda **_kwargs: events.append("startup")
    engine.shutdown.side_effect = lambda **_kwargs: events.append("engine_shutdown")
    consumer.stop.side_effect = lambda: events.append("consumer_stop")
    artifact_loader = MagicMock(return_value={})
    adapter = MagicMock()
    adapter_factory = MagicMock(
        side_effect=lambda *_args, **_kwargs: events.append("adapter_construct")
        or adapter
    )
    monkeypatch.setattr(
        strategy_main,
        "create_adapter",
        adapter_factory,
        raising=False,
    )

    def build_engine(**_kwargs):
        events.append("engine_construct")
        return engine

    with (
        patch("src.main.configure_metrics"),
        patch("src.main.SessionLocal", return_value=db_session),
        patch("src.main.StrategyEngine", side_effect=build_engine) as engine_cls,
        patch("src.main.DataConsumer", return_value=consumer) as consumer_cls,
        patch(
            "src.main._strategy_artifact_loader_from_env",
            return_value=artifact_loader,
        ) as artifact_loader_builder,
    ):
        strategy_main.main()

    kwargs = engine_cls.call_args.kwargs
    assert kwargs["db_session"] is db_session
    assert kwargs["adapter_config"] == {"mode": "simulated"}
    assert kwargs["adapter"] is adapter
    assert callable(kwargs["db_session_factory"])
    assert kwargs["audit_external_orders"] is True
    assert kwargs["leadership_guard"] is consumer.assert_service_ownership
    assert kwargs["runtime_bootstrap_factory"] is None
    assert kwargs["runtime_capabilities_factory"] is None
    assert kwargs["strategy_artifact_loader"] is artifact_loader
    artifact_loader_builder.assert_called_once_with()
    engine.add_strategy.assert_not_called()
    assert events == [
        "ownership",
        "adapter_construct",
        "engine_construct",
        "startup",
        "engine_shutdown",
        "consumer_stop",
    ]
    consumer.acquire_service_ownership.assert_called_once()
    assert adapter_factory.call_args.args == ({"mode": "simulated"},)
    assert adapter_factory.call_args.kwargs == {
        "operation_guard": consumer.assert_service_ownership
    }
    engine.startup.assert_called_once_with()
    assert (
        consumer.configure_callbacks.call_args.kwargs["channel_provider"]
        is engine.build_stream_channels
    )
    assert (
        consumer.configure_callbacks.call_args.kwargs["pending_replay_callback"]
        is engine.replay_pending_market_data
    )
    assert consumer_cls.call_args.kwargs["pending_claim_idle_ms"] == 60_000
    assert consumer_cls.call_args.kwargs["ownership_lease_ms"] == 10_000
    consumer.start.assert_called_once()
    consumer.stop.assert_called_once()
    engine.shutdown.assert_called_once_with(clean_exit=True)
    db_session.close.assert_called_once()


def test_main_closes_unowned_adapter_when_engine_construction_fails(
    monkeypatch,
    caplog,
) -> None:
    error = RuntimeError("engine construction failed")
    adapter = MagicMock()
    adapter.close.side_effect = OSError("cleanup details")
    adapter_factory = MagicMock(return_value=adapter)
    consumer = MagicMock(spec=strategy_main.DataConsumer)
    db_session = MagicMock()
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.delenv("EXCHANGE_MODE", raising=False)
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
    monkeypatch.setattr(
        strategy_main,
        "create_adapter",
        adapter_factory,
        raising=False,
    )

    with (
        patch("src.main.configure_metrics"),
        patch("src.main.SessionLocal", return_value=db_session),
        patch("src.main.StrategyEngine", side_effect=error),
        patch("src.main.DataConsumer", return_value=consumer),
        pytest.raises(RuntimeError) as caught,
    ):
        strategy_main.main()

    assert caught.value is error
    adapter_factory.assert_called_once_with(
        {"mode": "simulated"},
        operation_guard=consumer.assert_service_ownership,
    )
    adapter.close.assert_called_once_with()
    assert "Failed to close unowned adapter" in caplog.text
    assert "OSError" in caplog.text
    assert "cleanup details" not in caplog.text
    consumer.stop.assert_called_once_with()
    db_session.close.assert_called_once_with()


def test_main_preserves_adapter_construction_failure(monkeypatch) -> None:
    error = RuntimeError("adapter construction failed")
    adapter_factory = MagicMock(side_effect=error)
    consumer = MagicMock(spec=strategy_main.DataConsumer)
    db_session = MagicMock()
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.delenv("EXCHANGE_MODE", raising=False)
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
    monkeypatch.setattr(strategy_main, "create_adapter", adapter_factory)

    with (
        patch("src.main.configure_metrics"),
        patch("src.main.SessionLocal", return_value=db_session),
        patch("src.main.StrategyEngine") as engine_cls,
        patch("src.main.DataConsumer", return_value=consumer),
        pytest.raises(RuntimeError) as caught,
    ):
        strategy_main.main()

    assert caught.value is error
    adapter_factory.assert_called_once_with(
        {"mode": "simulated"},
        operation_guard=consumer.assert_service_ownership,
    )
    engine_cls.assert_not_called()
    consumer.stop.assert_called_once_with()
    db_session.close.assert_called_once_with()


def test_main_injects_rithmic_runtime_composition_factories(monkeypatch) -> None:
    adapter_config = {
        "mode": "live",
        "exchange": "rithmic",
        "instrument_product_ids": ["RITHMIC:NQ-202609"],
        "rithmic_recovery_profile": "orders",
        "rithmic_recovery_account_id": "ACCOUNT",
    }
    bootstrap_factory = MagicMock()
    capabilities_factory = MagicMock()
    factory_selector = MagicMock(return_value=(bootstrap_factory, capabilities_factory))
    db_session = MagicMock()
    engine = MagicMock()
    engine.build_stream_channels.return_value = []
    consumer = MagicMock(spec=strategy_main.DataConsumer)
    adapter = MagicMock()
    monkeypatch.setattr(
        strategy_main, "_adapter_config_from_env", lambda: adapter_config
    )
    monkeypatch.setattr(strategy_main, "_validate_runtime_config", MagicMock())
    monkeypatch.setattr(strategy_main, "_required_env_flag", lambda _name: True)
    monkeypatch.setattr(
        strategy_main,
        "runtime_factories_for_config",
        factory_selector,
    )
    monkeypatch.setattr(
        strategy_main,
        "create_adapter",
        MagicMock(return_value=adapter),
    )

    with (
        patch("src.main.configure_metrics"),
        patch("src.main.SessionLocal", return_value=db_session),
        patch("src.main.StrategyEngine", return_value=engine) as engine_cls,
        patch("src.main.DataConsumer", return_value=consumer),
    ):
        strategy_main.main()

    kwargs = engine_cls.call_args.kwargs
    factory_selector.assert_called_once_with(adapter_config)
    assert kwargs["adapter"] is adapter
    assert kwargs["runtime_bootstrap_factory"] is bootstrap_factory
    assert kwargs["runtime_capabilities_factory"] is capabilities_factory


@pytest.mark.parametrize(
    ("exchange", "product_id"),
    [
        ("binance", "BINANCE:BTCUSDT-PERP"),
        ("backpack", "BACKPACK:BTC_USDC-PERP"),
        ("bybit", "BYBIT:BTCUSDT-PERP"),
    ],
)
def test_main_does_not_inject_rithmic_factories_for_ccxt_live(
    monkeypatch,
    exchange,
    product_id,
) -> None:
    adapter_config = {
        "mode": "live",
        "exchange": exchange,
        "instrument_product_ids": [product_id],
    }
    db_session = MagicMock()
    engine = MagicMock()
    engine.build_stream_channels.return_value = []
    consumer = MagicMock(spec=strategy_main.DataConsumer)
    adapter = MagicMock()
    monkeypatch.setattr(
        strategy_main, "_adapter_config_from_env", lambda: adapter_config
    )
    monkeypatch.setattr(strategy_main, "_validate_runtime_config", MagicMock())
    monkeypatch.setattr(strategy_main, "_required_env_flag", lambda _name: True)
    monkeypatch.setattr(
        strategy_main,
        "create_adapter",
        MagicMock(return_value=adapter),
    )

    with (
        patch("src.main.configure_metrics"),
        patch("src.main.SessionLocal", return_value=db_session),
        patch("src.main.StrategyEngine", return_value=engine) as engine_cls,
        patch("src.main.DataConsumer", return_value=consumer),
    ):
        strategy_main.main()

    kwargs = engine_cls.call_args.kwargs
    assert kwargs["adapter"] is adapter
    assert kwargs["runtime_bootstrap_factory"] is None
    assert kwargs["runtime_capabilities_factory"] is None


@pytest.mark.parametrize("failure_stage", ["startup", "consumer"])
def test_main_never_marks_abnormal_service_exit_clean(
    monkeypatch,
    failure_stage,
) -> None:
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.delenv("EXCHANGE_MODE", raising=False)
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
    db_session = MagicMock()
    engine = MagicMock()
    consumer = MagicMock(spec=strategy_main.DataConsumer)
    if failure_stage == "startup":
        engine.startup.side_effect = RuntimeError("startup phase failed")
    else:
        consumer.start.side_effect = RuntimeError("ownership lost")

    with (
        patch("src.main.configure_metrics"),
        patch("src.main.SessionLocal", return_value=db_session),
        patch("src.main.StrategyEngine", return_value=engine),
        patch("src.main.DataConsumer", return_value=consumer),
    ):
        with pytest.raises(RuntimeError):
            strategy_main.main()

    engine.shutdown.assert_called_once_with(clean_exit=False)
    consumer.stop.assert_called_once()
    db_session.close.assert_called_once()


def test_main_releases_ownership_when_engine_shutdown_fails(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.delenv("EXCHANGE_MODE", raising=False)
    monkeypatch.delenv("FLUXTRADE_ENVIRONMENT", raising=False)
    db_session = MagicMock()
    engine = MagicMock()
    engine.shutdown.side_effect = RuntimeError("engine shutdown failed")
    consumer = MagicMock(spec=strategy_main.DataConsumer)

    with (
        patch("src.main.configure_metrics"),
        patch("src.main.SessionLocal", return_value=db_session),
        patch("src.main.StrategyEngine", return_value=engine),
        patch("src.main.DataConsumer", return_value=consumer),
    ):
        with pytest.raises(RuntimeError, match="engine shutdown failed"):
            strategy_main.main()

    db_session.close.assert_called_once()
    consumer.stop.assert_called_once()
