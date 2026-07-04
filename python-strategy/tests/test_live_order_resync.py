"""Tests for live order stream keepalive helpers."""

from unittest.mock import MagicMock

import pytest

from src.core.interfaces.exchange import ExchangeError, ExchangeUserStreamUnsupported
from src.core.live_order_resync import UserStreamKeepalive


def test_user_stream_keepalive_creates_and_refreshes_key() -> None:
    adapter = MagicMock()
    adapter.create_user_stream_listen_key.return_value = "listen-key-1"

    result = UserStreamKeepalive(adapter).keepalive_once()

    assert result.action == "refreshed"
    assert result.attempts == 1
    assert result.listen_key == "listen-key-1"
    adapter.create_user_stream_listen_key.assert_called_once_with()
    adapter.keepalive_user_stream.assert_called_once_with("listen-key-1")


def test_user_stream_keepalive_returns_unsupported_without_retry() -> None:
    adapter = MagicMock()
    adapter.create_user_stream_listen_key.side_effect = ExchangeUserStreamUnsupported(
        "unsupported"
    )

    result = UserStreamKeepalive(adapter).keepalive_once()

    assert result.action == "unsupported"
    assert result.attempts == 1
    assert result.error == "unsupported"
    adapter.keepalive_user_stream.assert_not_called()


def test_user_stream_keepalive_recreates_key_after_refresh_failure() -> None:
    adapter = MagicMock()
    adapter.create_user_stream_listen_key.side_effect = [
        "expired-listen-key",
        "fresh-listen-key",
    ]
    adapter.keepalive_user_stream.side_effect = [ExchangeError("expired"), None]

    result = UserStreamKeepalive(adapter, max_attempts=2).keepalive_once()

    assert result.action == "refreshed"
    assert result.attempts == 2
    assert result.listen_key == "fresh-listen-key"
    assert adapter.create_user_stream_listen_key.call_count == 2
    adapter.keepalive_user_stream.assert_any_call("expired-listen-key")
    adapter.keepalive_user_stream.assert_any_call("fresh-listen-key")


def test_user_stream_keepalive_reports_failed_after_retry_budget() -> None:
    adapter = MagicMock()
    adapter.create_user_stream_listen_key.side_effect = ["key-1", "key-2"]
    adapter.keepalive_user_stream.side_effect = ExchangeError("network")

    result = UserStreamKeepalive(adapter, max_attempts=2).keepalive_once()

    assert result.action == "failed"
    assert result.attempts == 2
    assert result.error == "network"
    assert adapter.create_user_stream_listen_key.call_count == 2


def test_user_stream_keepalive_rejects_invalid_retry_budget() -> None:
    adapter = MagicMock()

    with pytest.raises(ValueError, match="max_attempts"):
        UserStreamKeepalive(adapter, max_attempts=0)
