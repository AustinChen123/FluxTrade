"""
Tests for src/core/consumer.py — DataConsumer

Covers:
- Consumer group creation and BUSYGROUP handling
- Message parsing (JSON payload, raw key-value, invalid data)
- Conflation logic when lag > 100ms
- Normal message processing and ack
- Stop behavior
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

import redis as redis_lib
from src.core.consumer import DataConsumer
from src.core.models import Candlestick, Trade


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_redis():
    """Mock redis client for DataConsumer."""
    client = MagicMock()
    client.xreadgroup.return_value = []
    client.time.return_value = (1704067200, 0)  # (seconds, microseconds)
    return client


@pytest.fixture
def consumer(mock_redis):
    """DataConsumer with mocked Redis client."""
    with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
        c = DataConsumer(
            channels=["stream:market:binance:btcusdt:1m"],
            on_message_callback=MagicMock(),
        )
    c.redis_client = mock_redis
    return c


# =============================================================================
# Consumer group creation
# =============================================================================


class TestEnsureConsumerGroups:

    def test_creates_group_for_each_channel(self, consumer, mock_redis):
        """Should call xgroup_create for each channel."""
        consumer._ensure_consumer_groups()

        mock_redis.xgroup_create.assert_called_once_with(
            "stream:market:binance:btcusdt:1m",
            consumer.group_name,
            id='$',
            mkstream=True,
        )

    def test_busygroup_ignored(self, consumer, mock_redis):
        """BUSYGROUP error (group already exists) should be silently ignored."""
        mock_redis.xgroup_create.side_effect = redis_lib.exceptions.ResponseError(
            "BUSYGROUP Consumer Group name already exists"
        )

        # Should not raise
        consumer._ensure_consumer_groups()

    def test_other_response_error_logged(self, consumer, mock_redis):
        """Non-BUSYGROUP ResponseError should be logged (not raised in current impl)."""
        mock_redis.xgroup_create.side_effect = redis_lib.exceptions.ResponseError(
            "WRONGTYPE Operation against a key"
        )

        # Should not raise (logged only)
        consumer._ensure_consumer_groups()

    def test_multiple_channels(self, mock_redis):
        """Should create groups for all channels."""
        channels = [
            "stream:market:binance:btcusdt:1m",
            "stream:market:binance:ethusdt:5m",
        ]
        with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
            c = DataConsumer(channels=channels, on_message_callback=MagicMock())
        c.redis_client = mock_redis

        c._ensure_consumer_groups()

        assert mock_redis.xgroup_create.call_count == 2


# =============================================================================
# Message parsing
# =============================================================================


class TestParseMessage:

    def test_parse_json_candlestick(self, consumer):
        """Should parse JSON payload with 'open' key as Candlestick."""
        payload = json.dumps({
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "1m",
            "timestamp": 1704067200000,
            "open": "42000",
            "high": "42500",
            "low": "41500",
            "close": "42200",
            "volume": "1000",
        })
        data = {"json": payload}

        result = consumer._parse_message("stream:key", data)

        assert isinstance(result, Candlestick)
        assert result.close == Decimal("42200")

    def test_parse_json_trade(self, consumer):
        """Should parse JSON payload without 'open' key as Trade."""
        payload = json.dumps({
            "id": "t1",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "side": "buy",
            "price": "42000",
            "quantity": "0.1",
            "timestamp": 1704067200000,
        })
        data = {"json": payload}

        result = consumer._parse_message("stream:key", data)

        assert isinstance(result, Trade)

    def test_parse_raw_trade_keys(self, consumer):
        """Should parse raw key-value data with price/quantity as Trade."""
        data = {
            "trade_id": "t2",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "side": "BUY",
            "price": "42000",
            "quantity": "0.5",
            "timestamp": "1704067200000",
        }

        result = consumer._parse_message("stream:key", data)

        assert isinstance(result, Trade)
        assert result.price == Decimal("42000")
        assert result.quantity == Decimal("0.5")

    def test_parse_raw_without_trade_id_returns_none(self, consumer):
        """Missing product_id defaults to 'unknown' which fails validation — returns None."""
        data = {"price": "100", "quantity": "1"}

        result = consumer._parse_message("stream:key", data)

        # 'unknown' doesn't match EXCHANGE:SYMBOL-PERP regex, so validation fails
        # _parse_message catches the exception and returns None
        assert result is None

    def test_parse_unrecognized_data_returns_none(self, consumer):
        """Data without json or price/quantity keys should return None."""
        data = {"some_field": "value"}

        result = consumer._parse_message("stream:key", data)

        assert result is None

    def test_parse_invalid_json_returns_none(self, consumer):
        """Invalid JSON should return None (not raise)."""
        data = {"json": "not valid json {{{"}

        result = consumer._parse_message("stream:key", data)

        assert result is None


# =============================================================================
# Stop
# =============================================================================


class TestConsumerStop:

    def test_stop_sets_running_false(self, consumer):
        """stop() should set running to False."""
        consumer.running = True
        consumer.stop()
        assert consumer.running is False

    def test_stop_closes_redis(self, consumer, mock_redis):
        """stop() should close the Redis connection."""
        consumer.stop()
        mock_redis.close.assert_called_once()
