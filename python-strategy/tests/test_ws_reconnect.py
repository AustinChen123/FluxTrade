"""Tests for WebSocketOrderConnector exponential backoff reconnection."""

import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from src.core.ws_connector import (
    WebSocketOrderConnector,
    INITIAL_BACKOFF,
    MAX_BACKOFF,
    MAX_RETRIES,
)


@pytest.fixture
def connector():
    with patch.dict("os.environ", {}, clear=False):
        c = WebSocketOrderConnector(
            api_key="test_key",
            secret="test_secret",
            exchange_id="binance",
            testnet=True,
        )
        return c


class TestWSReconnectBackoff:
    """Tests for exponential backoff in _connect_and_listen."""

    def test_max_retries_stops_connector(self, connector):
        """After MAX_RETRIES failures, running should be set to False."""
        connector.running = True

        async def run():
            with patch("src.core.ws_connector.websockets.connect",
                        side_effect=ConnectionError("refused")):
                with patch("src.core.ws_connector.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
                    await connector._connect_and_listen()
                    return mock_sleep

        mock_sleep = asyncio.run(run())
        assert connector.running is False
        assert mock_sleep.call_count == MAX_RETRIES

    def test_backoff_increases_exponentially(self, connector):
        """Backoff values should double each attempt."""
        connector.running = True
        sleep_values = []

        async def track_sleep(seconds):
            sleep_values.append(seconds)

        async def run():
            with patch("src.core.ws_connector.websockets.connect",
                        side_effect=ConnectionError("refused")):
                with patch("src.core.ws_connector.asyncio.sleep", side_effect=track_sleep):
                    await connector._connect_and_listen()

        asyncio.run(run())

        for i in range(min(len(sleep_values), 5)):
            expected = min(INITIAL_BACKOFF * (2 ** i), MAX_BACKOFF)
            assert sleep_values[i] == expected

    def test_backoff_caps_at_max(self, connector):
        """No backoff value should exceed MAX_BACKOFF."""
        connector.running = True
        sleep_values = []

        async def track_sleep(seconds):
            sleep_values.append(seconds)

        async def run():
            with patch("src.core.ws_connector.websockets.connect",
                        side_effect=ConnectionError("refused")):
                with patch("src.core.ws_connector.asyncio.sleep", side_effect=track_sleep):
                    await connector._connect_and_listen()

        asyncio.run(run())

        for val in sleep_values:
            assert val <= MAX_BACKOFF
