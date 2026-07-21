from unittest.mock import MagicMock, patch

import pytest

from src import main as strategy_main


def test_env_flag_parses_truthy_values(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
    assert strategy_main._env_flag("AUDIT_EXTERNAL_ORDERS") is True

    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "0")
    assert strategy_main._env_flag("AUDIT_EXTERNAL_ORDERS") is False


def test_adapter_config_from_env_defaults_to_simulated(monkeypatch) -> None:
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.delenv("EXCHANGE_MODE", raising=False)

    assert strategy_main._adapter_config_from_env() == {"mode": "simulated"}


def test_adapter_config_from_env_wires_live_account_initialization(monkeypatch) -> None:
    monkeypatch.setenv("ADAPTER_MODE", "live")
    monkeypatch.setenv("EXCHANGE_ID", "binance")
    monkeypatch.setenv("EXCHANGE_API_KEY", "key")
    monkeypatch.setenv("EXCHANGE_SECRET", "secret")
    monkeypatch.setenv("EXCHANGE_TESTNET", "true")
    monkeypatch.setenv("EXCHANGE_ENABLE_WS", "false")
    monkeypatch.setenv("INSTRUMENT_PRODUCT_IDS", "BINANCE:BTCUSDT-PERP")
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


def test_adapter_config_from_env_rejects_unknown_mode(monkeypatch) -> None:
    monkeypatch.setenv("ADAPTER_MODE", "simulation")

    with pytest.raises(ValueError, match="unsupported_adapter_mode"):
        strategy_main._adapter_config_from_env()


def test_adapter_config_from_env_wires_optional_rithmic_recovery(monkeypatch) -> None:
    monkeypatch.setenv("ADAPTER_MODE", "live")
    monkeypatch.setenv("RITHMIC_RECOVERY_PROFILE", " test ")
    monkeypatch.setenv("RITHMIC_ACCOUNT_ID", " ACCOUNT ")

    config = strategy_main._adapter_config_from_env()

    assert config["rithmic_recovery_profile"] == "test"
    assert config["rithmic_recovery_account_id"] == "ACCOUNT"


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
    monkeypatch.setenv("ADAPTER_MODE", "live")
    monkeypatch.delenv("RITHMIC_RECOVERY_PROFILE", raising=False)
    monkeypatch.setenv("RITHMIC_ACCOUNT_ID", "ACCOUNT")

    config = strategy_main._adapter_config_from_env()

    assert config["rithmic_recovery_account_id"] == "ACCOUNT"
    with pytest.raises(ValueError, match="rithmic_account_id_requires_recovery_profile"):
        strategy_main._validate_runtime_config(
            config,
            audit_external_orders=True,
        )

    with pytest.raises(ValueError, match="rithmic_account_id_requires_recovery_profile"):
        strategy_main._validate_runtime_config(
            {
                "mode": "live",
                "rithmic_recovery_account_id": "ACCOUNT",
            },
            audit_external_orders=True,
        )


def test_main_rejects_live_without_audit_before_initialization(monkeypatch) -> None:
    monkeypatch.setenv("ADAPTER_MODE", "live")
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "false")

    with patch("src.main.configure_metrics") as configure_metrics, \
         patch("src.main.SessionLocal") as session_local, \
         patch("src.main.StrategyEngine") as engine_cls:
        with pytest.raises(ValueError, match="live_adapter_requires_audit_external_orders"):
            strategy_main.main()

    configure_metrics.assert_not_called()
    session_local.assert_not_called()
    engine_cls.assert_not_called()


def test_main_wires_session_factory_and_audit_flag(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
    monkeypatch.delenv("ADAPTER_MODE", raising=False)
    monkeypatch.delenv("EXCHANGE_MODE", raising=False)
    db_session = MagicMock()
    engine = MagicMock()
    engine.build_stream_channels.return_value = []
    consumer = MagicMock()

    with patch("src.main.configure_metrics"), \
         patch("src.main.SessionLocal", return_value=db_session), \
         patch("src.main.StrategyEngine", return_value=engine) as engine_cls, \
         patch("src.main.RandomStrategy"), \
         patch("src.main.DataConsumer", return_value=consumer):
        strategy_main.main()

    kwargs = engine_cls.call_args.kwargs
    assert kwargs["db_session"] is db_session
    assert kwargs["adapter_config"] == {"mode": "simulated"}
    assert callable(kwargs["db_session_factory"])
    assert kwargs["audit_external_orders"] is True
    consumer.start.assert_called_once()
    engine.shutdown.assert_called_once()
    db_session.close.assert_called_once()
