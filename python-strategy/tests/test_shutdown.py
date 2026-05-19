"""Tests for StrategyEngine.shutdown(): graceful teardown of threads, executor, and Redis."""

import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

from src.core.clock import Clock


class _MockClock(Clock):
    def now(self) -> float:
        return 1704067200.0


def _make_engine():
    """Create a StrategyEngine with mocked dependencies (no real Redis/DB)."""
    with patch("src.core.engine.create_redis_client") as mock_factory, \
         patch("src.core.engine.create_adapter") as mock_create_adapter:
        mock_factory.return_value = MagicMock()
        mock_create_adapter.return_value = MagicMock()

        from src.core.engine import StrategyEngine

        engine = StrategyEngine(
            db_session=MagicMock(),
            clock=_MockClock(),
            adapter_config={"mode": "simulated"},
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
        engine.shutdown()
        engine.redis_client.close.assert_called_once()

    def test_shutdown_calls_executor_shutdown(self):
        engine = _make_engine()
        engine.executor = MagicMock(spec=ThreadPoolExecutor)
        engine.shutdown()
        engine.executor.shutdown.assert_called_once_with(wait=True, cancel_futures=False)

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

    def test_shutdown_skips_dead_threads(self):
        engine = _make_engine()
        mock_thread = MagicMock(spec=threading.Thread)
        mock_thread.is_alive.return_value = False
        engine.heartbeat_thread = mock_thread
        engine.shutdown()
        mock_thread.join.assert_not_called()

    def test_shutdown_handles_redis_close_error(self):
        engine = _make_engine()
        engine.redis_client.close.side_effect = Exception("already closed")
        # Should not raise
        engine.shutdown()
        assert engine.running is False
