from unittest.mock import MagicMock, patch

from src import main as strategy_main


def test_env_flag_parses_truthy_values(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
    assert strategy_main._env_flag("AUDIT_EXTERNAL_ORDERS") is True

    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "0")
    assert strategy_main._env_flag("AUDIT_EXTERNAL_ORDERS") is False


def test_main_wires_session_factory_and_audit_flag(monkeypatch) -> None:
    monkeypatch.setenv("AUDIT_EXTERNAL_ORDERS", "true")
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
    assert callable(kwargs["db_session_factory"])
    assert kwargs["audit_external_orders"] is True
    consumer.start.assert_called_once()
    engine.shutdown.assert_called_once()
    db_session.close.assert_called_once()
