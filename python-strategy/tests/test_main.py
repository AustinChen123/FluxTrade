import inspect
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src import main as strategy_main


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
        strategy_main,
        "build_rithmic_live_adapter_config",
        owner,
        raising=False,
    )

    config = strategy_main._adapter_config_from_env()

    owner.assert_called_once()
    assert owner.call_args.kwargs["product_ids"] == ["RITHMIC:NQ-202609"]
    assert owner.call_args.kwargs["environ"] is strategy_main.os.environ
    assert {key: config[key] for key in owner_result} == owner_result


def test_generic_main_has_one_rithmic_policy_entrypoint() -> None:
    source = inspect.getsource(strategy_main._adapter_config_from_env)

    assert source.count("build_rithmic_live_adapter_config(") == 1
    assert "RITHMIC_" not in source
    assert "json." not in source
    assert "Path(" not in source
    assert ".is_file(" not in source


@pytest.mark.parametrize("mode", ["simulated", "binance"])
def test_non_rithmic_config_never_calls_rithmic_owner(monkeypatch, mode) -> None:
    owner = MagicMock(side_effect=AssertionError("Rithmic owner called"))
    monkeypatch.setattr(
        strategy_main,
        "build_rithmic_live_adapter_config",
        owner,
        raising=False,
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
        strategy_main,
        "build_ccxt_live_credentials",
        owner,
        raising=False,
    )
    monkeypatch.setattr(
        strategy_main,
        "build_rithmic_live_adapter_config",
        rithmic_owner,
        raising=False,
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
        strategy_main,
        "build_ccxt_live_credentials",
        owner,
        raising=False,
    )
    monkeypatch.setattr(
        strategy_main,
        "build_rithmic_live_adapter_config",
        rithmic_owner,
        raising=False,
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


@pytest.mark.parametrize(
    ("exchange", "product_id"),
    [
        ("bybit", "BYBIT:BTCUSDT-PERP"),
        ("okx", "OKX:BTCUSDT-PERP"),
    ],
)
def test_non_rithmic_live_config_delegates_to_ccxt_owner_once(
    monkeypatch,
    exchange,
    product_id,
) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.setenv("EXCHANGE_ID", exchange)
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", product_id)
    products = [product_id]
    monkeypatch.setattr(strategy_main, "_env_csv", lambda _name: products)
    owner_result = {"api_key": object(), "secret": object(), "testnet": object()}
    owner = MagicMock(return_value=owner_result)
    rithmic_owner = MagicMock(side_effect=AssertionError("Rithmic owner called"))
    monkeypatch.setattr(
        strategy_main,
        "build_ccxt_live_credentials",
        owner,
        raising=False,
    )
    monkeypatch.setattr(
        strategy_main,
        "build_rithmic_live_adapter_config",
        rithmic_owner,
        raising=False,
    )

    config = strategy_main._adapter_config_from_env()

    owner.assert_called_once_with(strategy_main.os.environ)
    assert {name: config[name] for name in owner_result} == owner_result
    assert config["instrument_product_ids"] is products
    assert config["account_initialization"]["product_ids"] is products
    rithmic_owner.assert_not_called()


def test_ccxt_credential_owner_failure_precedes_audit_and_initialization(
    monkeypatch,
) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.setenv("EXCHANGE_ID", "bybit")
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", "BYBIT:BTCUSDT-PERP")
    sentinel = RuntimeError("ccxt-credential-owner-sentinel")
    owner = MagicMock(side_effect=sentinel)
    audit_reader = MagicMock(side_effect=AssertionError("audit read"))
    initialization = _forbid_runtime_initialization(monkeypatch)
    monkeypatch.setattr(
        strategy_main,
        "build_ccxt_live_credentials",
        owner,
        raising=False,
    )
    monkeypatch.setattr(strategy_main, "_required_env_flag", audit_reader)

    with pytest.raises(RuntimeError) as raised:
        strategy_main.main()

    assert raised.value is sentinel
    owner.assert_called_once_with(strategy_main.os.environ)
    audit_reader.assert_not_called()
    _assert_initialization_not_called(initialization)


def test_ambiguous_websocket_precedes_ccxt_credential_owner(monkeypatch) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.setenv("EXCHANGE_ENABLE_WS", "enabled")
    owner = MagicMock(side_effect=AssertionError("CCXT credential owner called"))
    monkeypatch.setattr(
        strategy_main,
        "build_ccxt_live_credentials",
        owner,
        raising=False,
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


def test_generic_main_has_one_ccxt_credential_policy_entrypoint() -> None:
    source = inspect.getsource(strategy_main._adapter_config_from_env)

    assert source.count("build_ccxt_live_credentials(") == 1
    assert "EXCHANGE_API_KEY" not in source
    assert "EXCHANGE_SECRET" not in source
    assert "EXCHANGE_TESTNET" not in source


def test_common_product_failure_precedes_rithmic_owner_and_audit_reader(
    monkeypatch,
) -> None:
    _set_live_rithmic_env(monkeypatch)
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", "BINANCE:BTCUSDT-PERP")
    owner = MagicMock(side_effect=AssertionError("Rithmic owner called"))
    ccxt_owner = MagicMock(side_effect=AssertionError("CCXT owner called"))
    audit_reader = MagicMock(side_effect=AssertionError("audit read"))
    initialization = _forbid_runtime_initialization(monkeypatch)
    monkeypatch.setattr(
        strategy_main,
        "build_rithmic_live_adapter_config",
        owner,
        raising=False,
    )
    monkeypatch.setattr(
        strategy_main,
        "build_ccxt_live_credentials",
        ccxt_owner,
        raising=False,
    )
    monkeypatch.setattr(strategy_main, "_required_env_flag", audit_reader)

    with pytest.raises(ValueError, match="must use RITHMIC venue"):
        strategy_main.main()

    owner.assert_not_called()
    ccxt_owner.assert_not_called()
    audit_reader.assert_not_called()
    _assert_initialization_not_called(initialization)


def test_rithmic_owner_failure_precedes_audit_and_initialization(monkeypatch) -> None:
    _set_live_rithmic_env(monkeypatch)
    sentinel = RuntimeError("rithmic-config-owner-sentinel")
    owner = MagicMock(side_effect=sentinel)
    audit_reader = MagicMock(side_effect=AssertionError("audit read"))
    initialization = _forbid_runtime_initialization(monkeypatch)
    monkeypatch.setattr(
        strategy_main,
        "build_rithmic_live_adapter_config",
        owner,
        raising=False,
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
        strategy_main,
        "validate_rithmic_recovery_identity",
        validator,
        raising=False,
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
        strategy_main,
        "validate_rithmic_recovery_identity",
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
        {"mode": "live"},
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

    def build_engine(**_kwargs):
        events.append("engine_construct")
        return engine

    with (
        patch("src.main.configure_metrics"),
        patch("src.main.SessionLocal", return_value=db_session),
        patch("src.main.StrategyEngine", side_effect=build_engine) as engine_cls,
        patch("src.main.DataConsumer", return_value=consumer) as consumer_cls,
    ):
        strategy_main.main()

    kwargs = engine_cls.call_args.kwargs
    assert kwargs["db_session"] is db_session
    assert kwargs["adapter_config"] == {"mode": "simulated"}
    assert callable(kwargs["db_session_factory"])
    assert kwargs["audit_external_orders"] is True
    assert kwargs["leadership_guard"] is consumer.assert_service_ownership
    engine.add_strategy.assert_not_called()
    assert events == [
        "ownership",
        "engine_construct",
        "startup",
        "engine_shutdown",
        "consumer_stop",
    ]
    consumer.acquire_service_ownership.assert_called_once()
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
