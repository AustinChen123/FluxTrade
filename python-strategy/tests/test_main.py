from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src import main as strategy_main


def _set_live_ccxt_env(monkeypatch) -> None:
    values = {
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


def test_adapter_config_preserves_incomplete_identity_for_validation(monkeypatch) -> None:
    with pytest.raises(ValueError, match="rithmic_account_id_requires_recovery_profile"):
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

    with patch("src.main.configure_metrics") as configure_metrics, \
         patch("src.main.SessionLocal") as session_local, \
         patch("src.main.StrategyEngine") as engine_cls:
        with pytest.raises(ValueError, match="live_adapter_requires_audit_external_orders"):
            strategy_main.main()

    configure_metrics.assert_not_called()
    session_local.assert_not_called()
    engine_cls.assert_not_called()


def test_main_requires_explicit_live_audit_flag(monkeypatch) -> None:
    _set_live_ccxt_env(monkeypatch)
    monkeypatch.delenv("AUDIT_EXTERNAL_ORDERS", raising=False)

    with patch("src.main.configure_metrics") as configure_metrics, \
         patch("src.main.SessionLocal") as session_local:
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
    consumer = MagicMock()

    with patch("src.main.configure_metrics"), \
         patch("src.main.SessionLocal", return_value=db_session), \
         patch("src.main.StrategyEngine", return_value=engine) as engine_cls, \
         patch("src.main.DataConsumer", return_value=consumer) as consumer_cls:
        strategy_main.main()

    kwargs = engine_cls.call_args.kwargs
    assert kwargs["db_session"] is db_session
    assert kwargs["adapter_config"] == {"mode": "simulated"}
    assert callable(kwargs["db_session_factory"])
    assert kwargs["audit_external_orders"] is True
    engine.add_strategy.assert_not_called()
    assert (
        consumer_cls.call_args.kwargs["channel_provider"]
        is engine.build_stream_channels
    )
    consumer.start.assert_called_once()
    engine.shutdown.assert_called_once()
    db_session.close.assert_called_once()
