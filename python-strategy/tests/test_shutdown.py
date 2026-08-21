"""Tests for StrategyEngine.shutdown(): graceful teardown of threads, executor, and Redis."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

from src.core.clock import Clock


class _MockClock(Clock):
    def now(self) -> float:
        return 1704067200.0


def _make_engine():
    """Create a StrategyEngine with mocked dependencies (no real Redis/DB)."""
    with (
        patch("src.core.engine.create_redis_client") as mock_factory,
        patch("src.core.engine.create_simulated_adapter") as mock_create_adapter,
    ):
        mock_factory.return_value = MagicMock()
        mock_create_adapter.return_value = MagicMock()

        from src.core.engine import StrategyEngine

        engine = StrategyEngine(
            db_session=MagicMock(),
            clock=_MockClock(),
            adapter_config={"mode": "simulated"},
            account_service=MagicMock(),
        )
    return engine


class TestEngineShutdown:
    """Tests for StrategyEngine.shutdown() method."""

    def test_shutdown_sets_running_false(self):
        engine = _make_engine()
        engine.running = True
        engine.shutdown()
        assert engine.running is False

    def test_shutdown_closes_redis(self):
        engine = _make_engine()
        redis_client = MagicMock()
        engine.redis_client = redis_client
        engine.shutdown()
        redis_client.close.assert_called_once()

    def test_shutdown_calls_executor_shutdown(self):
        engine = _make_engine()
        engine.executor = MagicMock(spec=ThreadPoolExecutor)
        engine.shutdown()
        engine.executor.shutdown.assert_called_once_with(
            wait=True, cancel_futures=False
        )

    def test_shutdown_joins_heartbeat_thread(self):
        engine = _make_engine()
        mock_thread = MagicMock(spec=threading.Thread)
        mock_thread.is_alive.return_value = True
        engine.heartbeat_thread = mock_thread
        engine.shutdown(timeout=5.0)
        mock_thread.join.assert_called_once_with(timeout=5.0)

    def test_shutdown_joins_command_thread(self):
        engine = _make_engine()
        mock_thread = MagicMock(spec=threading.Thread)
        mock_thread.is_alive.return_value = True
        engine.command_thread = mock_thread
        engine.shutdown(timeout=5.0)
        mock_thread.join.assert_called_once_with(timeout=5.0)

    def test_live_command_listener_exits_promptly_without_a_message(self):
        engine = _make_engine()
        redis_client = MagicMock()
        engine.redis_client = redis_client
        subscribed = threading.Event()

        class IdlePubSub:
            def subscribe(self, _channel):
                subscribed.set()

            def get_message(self, timeout):
                time.sleep(min(timeout, 0.01))
                return None

            def close(self):
                return None

        redis_client.pubsub.return_value = IdlePubSub()
        engine._start_command_listener()
        assert subscribed.wait(timeout=1.0)
        command_thread = engine.command_thread
        assert command_thread is not None

        started = time.monotonic()
        engine.shutdown(timeout=1.0)

        assert time.monotonic() - started < 0.5
        assert not command_thread.is_alive()

    def test_shutdown_skips_dead_threads(self):
        engine = _make_engine()
        mock_thread = MagicMock(spec=threading.Thread)
        mock_thread.is_alive.return_value = False
        engine.heartbeat_thread = mock_thread
        engine.shutdown()
        mock_thread.join.assert_not_called()

    def test_shutdown_handles_redis_close_error(self):
        engine = _make_engine()
        redis_client = MagicMock()
        redis_client.close.side_effect = Exception("already closed")
        engine.redis_client = redis_client
        # Should not raise
        engine.shutdown()
        assert engine.running is False

    def test_safe_started_boot_is_marked_clean_before_redis_close(self):
        engine = _make_engine()
        engine._boot_started = True
        engine._kill_switch_halted = False
        engine.ops_safety._recovery_pending = False
        engine.ops_safety.persist_engine_boot_state = MagicMock()

        engine.shutdown(clean_exit=True)

        engine.ops_safety.persist_engine_boot_state.assert_called_once_with(
            "CLEAN",
            boot_id=engine._boot_id,
        )

    def test_abnormal_started_boot_is_not_marked_clean(self):
        engine = _make_engine()
        engine._boot_started = True
        engine._kill_switch_halted = False
        engine.ops_safety._recovery_pending = False
        engine.ops_safety.persist_engine_boot_state = MagicMock()

        engine.shutdown(clean_exit=False)

        engine.ops_safety.persist_engine_boot_state.assert_not_called()

    def test_unsafe_boot_is_never_marked_clean(self):
        for boot_started, halted, recovery_pending in (
            (False, False, False),
            (True, True, False),
            (True, False, True),
        ):
            engine = _make_engine()
            engine._boot_started = boot_started
            engine._kill_switch_halted = halted
            engine.ops_safety._recovery_pending = recovery_pending
            engine.ops_safety.persist_engine_boot_state = MagicMock()

            engine.shutdown(clean_exit=True)

            engine.ops_safety.persist_engine_boot_state.assert_not_called()
