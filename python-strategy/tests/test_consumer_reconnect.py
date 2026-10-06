"""Tests for DataConsumer: stop() fix regression and reconnection with exponential backoff."""

from unittest.mock import MagicMock, patch

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from src.core.consumer import (
    DataConsumer,
    INITIAL_BACKOFF,
    MAX_BACKOFF,
    MarketStreamOwnershipError,
    MarketStreamPendingError,
)


@pytest.fixture
def mock_callback():
    return MagicMock()


@pytest.fixture
def consumer(mock_callback):
    with patch("src.core.consumer.create_redis_client") as mock_factory:
        mock_client = MagicMock()
        mock_factory.return_value = mock_client
        c = DataConsumer(channels=["stream:test"], on_message_callback=mock_callback)
        mock_client.set.return_value = True
        mock_client.get.side_effect = lambda _key: c._ownership_token
        mock_client.eval.return_value = 1
        yield c
        c.stop()


class TestStopMethod:
    """Regression tests: stop() must be a real class method, not nested."""

    def test_stop_is_class_method(self):
        """stop() must exist directly on DataConsumer, not nested inside _parse_message."""
        assert hasattr(DataConsumer, "stop")
        assert callable(getattr(DataConsumer, "stop"))

    def test_stop_sets_running_false(self, consumer):
        consumer.running = True
        consumer.stop()
        assert consumer.running is False

    def test_stop_closes_redis(self, consumer):
        consumer.running = True
        consumer.stop()
        consumer.redis_client.close.assert_called_once()

    def test_stop_callable_on_instance(self, consumer):
        """self.stop() must not raise AttributeError (the original bug)."""
        consumer.running = True
        try:
            consumer.stop()
        except AttributeError:
            pytest.fail(
                "stop() raised AttributeError — still nested inside _parse_message"
            )


class TestReconnectionBackoff:
    """Tests for exponential backoff reconnection logic in start()."""

    def test_backoff_doubles_on_connection_error(self, consumer):
        """Backoff should double after each failed attempt."""
        call_count = 0
        backoffs = []

        def track_sleep(seconds):
            backoffs.append(seconds)

        def fail_then_stop(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise RedisConnectionError("connection refused")
            # Stop after 3 failures
            consumer.running = False

        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=fail_then_stop)

        with patch.object(
            consumer._stop_requested,
            "wait",
            side_effect=track_sleep,
        ):
            consumer.start()

        assert len(backoffs) == 3
        assert backoffs[0] == INITIAL_BACKOFF
        assert backoffs[1] == INITIAL_BACKOFF * 2
        assert backoffs[2] == INITIAL_BACKOFF * 4

    def test_backoff_caps_at_max(self, consumer):
        """Backoff must not exceed MAX_BACKOFF."""
        backoffs = []

        def track_sleep(seconds):
            backoffs.append(seconds)

        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=RedisConnectionError("refused"))

        with patch.object(
            consumer._stop_requested,
            "wait",
            side_effect=track_sleep,
        ):
            with pytest.raises(RedisConnectionError):
                consumer.start()

        # All backoffs should be <= MAX_BACKOFF
        for b in backoffs:
            assert b <= MAX_BACKOFF

    def test_max_attempts_raises(self, consumer):
        """After MAX_RETRIES, start() should re-raise the exception."""
        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=RedisConnectionError("refused"))

        with patch.object(
            consumer._stop_requested,
            "wait",
            return_value=False,
        ):
            with pytest.raises(RedisConnectionError):
                consumer.start()

    def test_successful_connection_resets_backoff(self, consumer):
        """After a successful consume loop iteration, backoff should reset."""
        call_count = 0
        backoffs = []

        def track_sleep(seconds):
            backoffs.append(seconds)

        def fail_once_then_succeed(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RedisConnectionError("refused")
            # Second call: succeed then stop
            consumer.running = False

        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=fail_once_then_succeed)

        with patch.object(
            consumer._stop_requested,
            "wait",
            side_effect=track_sleep,
        ):
            consumer.start()

        # Only one backoff sleep (from the first failure)
        assert len(backoffs) == 1
        assert backoffs[0] == INITIAL_BACKOFF

    def test_keyboard_interrupt_calls_stop(self, consumer):
        """KeyboardInterrupt should call stop() and break."""
        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=KeyboardInterrupt)

        consumer.start()

        assert consumer.running is False
        consumer.redis_client.close.assert_not_called()

    def test_consume_loop_clean_exit(self, consumer):
        """When _consume_loop returns normally (running=False), start() should exit."""

        def stop_loop():
            consumer.running = False

        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=stop_loop)

        consumer.start()
        # Should exit without error
        assert consumer.running is False

    def test_startup_shutdown_request_is_sticky(self, consumer):
        consumer.acquire_service_ownership()
        consumer.request_stop()
        consumer._consume_loop = MagicMock()

        consumer.start()

        consumer._consume_loop.assert_not_called()
        assert consumer.running is False
        assert consumer._stop_requested.is_set()

    def test_os_error_also_retries(self, consumer):
        """OSError (network-level) should also trigger backoff retry."""
        call_count = 0

        def fail_then_stop(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise OSError("network unreachable")
            consumer.running = False

        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=fail_then_stop)

        with patch.object(
            consumer._stop_requested,
            "wait",
            return_value=False,
        ):
            consumer.start()

        assert call_count == 2

    def test_unexpected_exception_propagates(self, consumer):
        """Uncaught exception types (e.g. RuntimeError) should propagate immediately."""
        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=RuntimeError("unexpected"))

        with pytest.raises(RuntimeError, match="unexpected"):
            consumer.start()

    def test_ambiguous_delivery_backpressures_without_process_exit(self, consumer):
        calls = 0

        def block_then_stop():
            nonlocal calls
            calls += 1
            if calls == 1:
                raise MarketStreamPendingError("callback outcome unknown")
            consumer.running = False

        consumer._consume_loop = MagicMock(side_effect=block_then_stop)

        with patch.object(
            consumer._stop_requested,
            "wait",
            return_value=False,
        ) as wait:
            consumer.start()

        wait.assert_called_once_with(INITIAL_BACKOFF)
        assert consumer._consume_loop.call_count == 2

    @pytest.mark.parametrize(
        ("stage", "cause_type", "cause_status"),
        [
            ("market_delivery", RuntimeError, "known"),
            ("pending_replay", ValueError, "known"),
            ("pending_delivery", None, "unknown"),
        ],
    )
    def test_start_suppresses_pending_retry_logs_and_preserves_safe_cause(
        self, consumer, caplog, monkeypatch, stage, cause_type, cause_status
    ):
        import json

        now = [5.0]
        monkeypatch.setattr("src.core.consumer.time.monotonic", lambda: now[0])
        caplog.set_level("ERROR", logger="src.core.consumer")
        calls = 0

        def fail_pending():
            nonlocal calls
            calls += 1
            if calls == 4:
                now[0] = 65.0
            error = MarketStreamPendingError(
                "private stream/account identifiers must not be logged"
            )
            if stage != "pending_delivery":
                setattr(error, "_diagnostic_stage", stage)
            if cause_type is not None:
                try:
                    raise cause_type("secret provider text must not be logged")
                except cause_type as cause:
                    raise error from cause
            raise error

        consumer._consume_loop = MagicMock(side_effect=fail_pending)
        waits = 0

        def stop_after_four_waits(_seconds):
            nonlocal waits
            waits += 1
            if waits == 4:
                consumer.running = False
            return False

        with patch.object(
            consumer._stop_requested, "wait", side_effect=stop_after_four_waits
        ):
            consumer.start()

        events = [
            record.getMessage()
            for record in caplog.records
            if "consumer_callback_failure " in record.getMessage()
        ]
        assert len(events) == 2
        first = json.loads(events[0].split("consumer_callback_failure ", 1)[1])
        assert first["repeat_count"] == 0
        diagnostic = json.loads(events[1].split("consumer_callback_failure ", 1)[1])
        assert diagnostic["stage"] == stage
        assert diagnostic["cause_status"] == cause_status
        assert diagnostic["repeat_count"] == 3
        if cause_type is None:
            assert diagnostic["exception_types"] == []
        else:
            assert diagnostic["exception_types"] == [cause_type.__name__]
        assert "secret" not in caplog.text
        assert "private stream" not in caplog.text
        assert consumer._consume_loop.call_count == 4

    @pytest.mark.parametrize(
        ("failure_target", "failure"),
        [
            ("src.core.consumer.time.monotonic", OSError("clock unavailable")),
            ("src.core.consumer.json.dumps", ValueError("format unavailable")),
            ("src.core.consumer.logger.error", RuntimeError("handler unavailable")),
        ],
    )
    def test_diagnostic_failures_do_not_change_pending_retry_or_stop(
        self, consumer, monkeypatch, failure_target, failure
    ):
        monkeypatch.setattr(failure_target, MagicMock(side_effect=failure))
        calls = 0

        def pending_failure():
            nonlocal calls
            calls += 1
            error = MarketStreamPendingError("not emitted")
            setattr(error, "_diagnostic_stage", "market_delivery")
            try:
                raise ValueError("private cause text")
            except ValueError as cause:
                raise error from cause

        consumer._consume_loop = MagicMock(side_effect=pending_failure)
        waits = 0

        def stop_after_two_waits(_seconds):
            nonlocal waits
            waits += 1
            if waits == 2:
                consumer.running = False
            return False

        with patch.object(
            consumer._stop_requested, "wait", side_effect=stop_after_two_waits
        ):
            consumer.start()

        assert calls == 2
        assert waits == 2
        assert consumer._completed_pending == set()

    def test_ownership_loss_exits_for_fresh_service_restart(self, consumer):
        consumer._consume_loop = MagicMock(
            side_effect=MarketStreamOwnershipError("successor took ownership")
        )

        with pytest.raises(
            MarketStreamOwnershipError,
            match="successor took ownership",
        ):
            consumer.start()

        assert consumer._ownership_active is True
