"""Tests for DataConsumer: stop() fix regression and reconnection with exponential backoff."""

from unittest.mock import MagicMock, patch

import pytest
import redis

from src.core.consumer import DataConsumer, INITIAL_BACKOFF, MAX_BACKOFF


@pytest.fixture
def mock_callback():
    return MagicMock()


@pytest.fixture
def consumer(mock_callback):
    with patch("src.core.consumer.create_redis_client") as mock_factory:
        mock_client = MagicMock()
        mock_factory.return_value = mock_client
        c = DataConsumer(channels=["stream:test"], on_message_callback=mock_callback)
        yield c


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
            pytest.fail("stop() raised AttributeError — still nested inside _parse_message")


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
                raise redis.exceptions.ConnectionError("connection refused")
            # Stop after 3 failures
            consumer.running = False

        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=fail_then_stop)

        with patch("src.core.consumer.time.sleep", side_effect=track_sleep):
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
        consumer._consume_loop = MagicMock(
            side_effect=redis.exceptions.ConnectionError("refused")
        )

        with patch("src.core.consumer.time.sleep", side_effect=track_sleep):
            with pytest.raises(redis.exceptions.ConnectionError):
                consumer.start()

        # All backoffs should be <= MAX_BACKOFF
        for b in backoffs:
            assert b <= MAX_BACKOFF

    def test_max_attempts_raises(self, consumer):
        """After MAX_RETRIES, start() should re-raise the exception."""
        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(
            side_effect=redis.exceptions.ConnectionError("refused")
        )

        with patch("src.core.consumer.time.sleep"):
            with pytest.raises(redis.exceptions.ConnectionError):
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
                raise redis.exceptions.ConnectionError("refused")
            # Second call: succeed then stop
            consumer.running = False

        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=fail_once_then_succeed)

        with patch("src.core.consumer.time.sleep", side_effect=track_sleep):
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
        consumer.redis_client.close.assert_called_once()

    def test_consume_loop_clean_exit(self, consumer):
        """When _consume_loop returns normally (running=False), start() should exit."""
        def stop_loop():
            consumer.running = False

        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=stop_loop)

        consumer.start()
        # Should exit without error
        assert consumer.running is False

    def test_general_exception_also_retries(self, consumer):
        """Non-ConnectionError exceptions should also trigger backoff retry."""
        call_count = 0

        def fail_then_stop(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise RuntimeError("unexpected")
            consumer.running = False

        consumer._ensure_consumer_groups = MagicMock()
        consumer._consume_loop = MagicMock(side_effect=fail_then_stop)

        with patch("src.core.consumer.time.sleep"):
            consumer.start()

        assert call_count == 2
