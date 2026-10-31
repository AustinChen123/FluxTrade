import time
from unittest.mock import patch

from src.core.notifications import NotificationDispatcher


def test_strategy_engine_starts_dispatcher_before_phases_and_stops_normally(
    engine_factory, tmp_path
):
    dispatcher = NotificationDispatcher(path=tmp_path / "engine-alerts.jsonl")
    with patch("src.core.engine.NotificationDispatcher", return_value=dispatcher):
        engine = engine_factory()

    def first_startup_phase():
        assert dispatcher._state == "running"

    engine._halt_for_kill_switch = first_startup_phase
    engine._start_command_listener = lambda: None
    engine._check_system_state = lambda: False
    engine._reconcile_startup_balance = lambda: None
    engine._initialize_strategy_state_cache_on_startup = lambda: None
    engine._start_strategy_state_subscriber_on_startup = lambda: None
    engine._reconcile_recoverable_orders_on_startup = lambda: None
    engine._start_exchange_order_event_stream = lambda: None
    engine._start_heartbeat = lambda: None
    engine.scan_strategies = lambda: None
    engine._restore_active_strategies_on_startup = lambda: None

    engine.startup()
    assert dispatcher._state == "running"
    engine.shutdown(timeout=0.1, clean_exit=False)

    deadline = time.monotonic() + 1
    while dispatcher._state != "stopped":
        assert time.monotonic() < deadline
        time.sleep(0.005)
    assert dispatcher._worker is not None
    assert not dispatcher._worker.is_alive()
