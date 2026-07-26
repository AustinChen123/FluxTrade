"""
Tests for src/core/consumer.py — DataConsumer

Covers:
- Consumer group creation and BUSYGROUP handling
- Message parsing (JSON payload, raw key-value, invalid data)
- Ordered processing regardless of lag
- Fail-closed processing and pending-delivery detection
- Stop behavior
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

import redis as redis_lib
from src.core.consumer import DataConsumer, MarketStreamPendingError
from src.core.models import Candlestick, Trade


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
def mock_redis():
    """Mock redis client for DataConsumer."""
    client = MagicMock()
    client.xreadgroup.return_value = []
    client.xpending.return_value = {"pending": 0}
    client.scan_iter.return_value = []
    client.xinfo_groups.return_value = []
    client.smembers.return_value = set()
    client.sadd.return_value = 1
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

    def test_busygroup_ignored(self, consumer, mock_redis, caplog):
        """BUSYGROUP error (group already exists) should be silently ignored."""
        mock_redis.xgroup_create.side_effect = redis_lib.exceptions.ResponseError(
            "BUSYGROUP Consumer Group name already exists"
        )

        caplog.set_level("ERROR", logger="src.core.consumer")
        consumer._ensure_consumer_groups()

        mock_redis.xgroup_create.assert_called_once_with(
            "stream:market:binance:btcusdt:1m",
            consumer.group_name,
            id='$',
            mkstream=True,
        )
        assert "Error creating group" not in caplog.text

    def test_other_response_error_fails_closed(self, consumer, mock_redis):
        """A missing or invalid group must stop consumption."""
        mock_redis.xgroup_create.side_effect = redis_lib.exceptions.ResponseError(
            "WRONGTYPE Operation against a key"
        )

        with pytest.raises(
            redis_lib.exceptions.ResponseError,
            match="WRONGTYPE",
        ):
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

    def test_multiple_channels_are_polled_one_at_a_time(self, mock_redis):
        channels = [
            "stream:market:rithmic:nq-202609:1m",
            "stream:market:rithmic:es-202609:1m",
        ]
        with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
            consumer = DataConsumer(
                channels=channels,
                on_message_callback=MagicMock(),
            )
        calls = 0

        def stop_after_two_reads(**_kwargs):
            nonlocal calls
            calls += 1
            if calls == 2:
                consumer.running = False
            return []

        mock_redis.xreadgroup.side_effect = stop_after_two_reads
        consumer.running = True
        consumer._consume_loop()

        assert [
            call.kwargs["streams"] for call in mock_redis.xreadgroup.call_args_list
        ] == [
            {channels[0]: ">"},
            {channels[1]: ">"},
        ]
        assert all(
            call.kwargs["count"] == 1
            for call in mock_redis.xreadgroup.call_args_list
        )
        assert all(
            call.kwargs["block"] == 50
            for call in mock_redis.xreadgroup.call_args_list
        )


class TestDynamicChannels:

    def test_refreshes_channels_from_provider(self, mock_redis):
        provider = MagicMock(return_value=["stream:market:rithmic:nq-202609:1m"])
        with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
            consumer = DataConsumer(
                channels=[],
                on_message_callback=MagicMock(),
                channel_provider=provider,
            )

        assert consumer._current_channels() == [
            "stream:market:rithmic:nq-202609:1m"
        ]

    def test_empty_channels_idle_until_strategy_becomes_active(self, mock_redis):
        stream = "stream:market:rithmic:nq-202609:1m"
        provider = MagicMock(side_effect=[[], [stream]])
        with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
            consumer = DataConsumer(
                channels=[],
                on_message_callback=MagicMock(),
                channel_provider=provider,
            )

        def stop_after_read(**kwargs):
            consumer.running = False
            return []

        mock_redis.xreadgroup.side_effect = stop_after_read
        consumer.running = True
        with patch("src.core.consumer.time.sleep") as sleep:
            consumer._consume_loop()

        sleep.assert_called_once_with(0.1)
        mock_redis.xreadgroup.assert_called_once()
        assert mock_redis.xreadgroup.call_args.kwargs["streams"] == {stream: ">"}
        mock_redis.xgroup_create.assert_called_once()


class _BehaviorRedis:
    """Small stateful Redis Stream fake for delivery/ACK behavior."""

    def __init__(
        self,
        messages,
        *,
        pending=0,
        server_time=(1704067210, 0),
        existing_group_streams=(),
    ):
        self.messages = list(messages)
        self.pending = pending
        self.server_time = server_time
        self.acked = []
        self.reads = 0
        self.requested_counts = []
        self.registered_streams = set()
        self.existing_group_streams = set(existing_group_streams)

    def xgroup_create(self, *_args, **_kwargs):
        return True

    def xpending(self, *_args, **_kwargs):
        return {"pending": self.pending}

    def scan_iter(self, *, match, _type):
        assert match == "stream:market:*"
        assert _type == "stream"
        return iter(self.existing_group_streams)

    def xinfo_groups(self, stream_key):
        if stream_key in self.existing_group_streams:
            return [{"name": "strategy_group", "pending": self.pending}]
        return []

    def smembers(self, _key):
        return set(self.registered_streams)

    def sadd(self, _key, *stream_keys):
        self.registered_streams.update(stream_keys)
        return len(stream_keys)

    def xreadgroup(self, **kwargs):
        self.reads += 1
        count = kwargs["count"]
        self.requested_counts.append(count)
        batch = self.messages[:count]
        del self.messages[:count]
        self.pending += len(batch)
        return (
            [("stream:market:rithmic:mnq-202609:1m", batch)]
            if batch
            else []
        )

    def time(self):
        return self.server_time

    def xack(self, stream, group, message_id):
        self.acked.append((stream, group, message_id))
        self.pending -= 1
        return 1

    def close(self):
        return None


def _candle_payload(timestamp: int, close: str) -> dict[str, str]:
    return {
        "json": json.dumps(
            {
                "product_id": "RITHMIC:MNQ-202609",
                "timeframe": "1m",
                "timestamp": timestamp,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": "1",
            }
        )
    }


class TestDeliverySemantics:

    def test_lagged_candles_are_processed_individually_in_order(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _BehaviorRedis(
            [
                ("1704067200000-0", _candle_payload(1704067200000, "20000")),
                ("1704067200001-0", _candle_payload(1704067260000, "20001")),
            ]
        )
        closes = []
        consumer = None

        def record_candle(candle):
            closes.append(candle.close)
            if len(closes) == 2:
                assert consumer is not None
                consumer.running = False

        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=record_candle,
        )
        consumer.redis_client = redis
        consumer.running = True

        consumer._consume_loop()

        assert closes == [Decimal("20000"), Decimal("20001")]
        assert redis.requested_counts == [1, 1]
        assert [ack[2] for ack in redis.acked] == [
            "1704067200000-0",
            "1704067200001-0",
        ]

    def test_callback_failure_claims_only_the_ambiguous_delivery(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _BehaviorRedis(
            [
                ("1704067200000-0", _candle_payload(1704067200000, "20000")),
                ("1704067200001-0", _candle_payload(1704067260000, "20001")),
            ]
        )
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=lambda _candle: (_ for _ in ()).throw(
                RuntimeError("strategy failed")
            ),
        )
        consumer.redis_client = redis
        consumer.running = True

        with pytest.raises(MarketStreamPendingError, match="market callback failed"):
            consumer._consume_loop()

        assert redis.requested_counts == [1]
        assert redis.pending == 1
        assert len(redis.messages) == 1

    def test_removed_channel_cannot_bypass_ambiguous_delivery_gate(self):
        stream_a = "stream:market:rithmic:mnq-202609:1m"
        stream_b = "stream:market:rithmic:es-202609:1m"
        active_channels = [stream_a]
        redis = _BehaviorRedis(
            [("1704067200000-0", _candle_payload(1704067200000, "20000"))]
        )
        consumer = DataConsumer(
            channels=[],
            channel_provider=lambda: active_channels,
            on_message_callback=lambda _candle: (_ for _ in ()).throw(
                RuntimeError("strategy failed")
            ),
        )
        consumer.redis_client = redis
        consumer.running = True

        with pytest.raises(MarketStreamPendingError, match="market callback failed"):
            consumer._consume_loop()

        assert consumer._blocked_streams == {stream_a}
        assert redis.reads == 1

        active_channels[:] = [stream_b]
        with pytest.raises(MarketStreamPendingError, match="unresolved deliveries"):
            consumer._consume_loop()

        assert redis.reads == 1
        assert consumer._blocked_streams == {stream_a}

    def test_restart_restores_removed_stream_pending_gate(self):
        stream_a = "stream:market:rithmic:mnq-202609:1m"
        stream_b = "stream:market:rithmic:es-202609:1m"
        redis = _BehaviorRedis([], pending=1)
        redis.registered_streams.add(stream_a)
        consumer = DataConsumer(
            channels=[],
            channel_provider=lambda: [stream_b],
            on_message_callback=lambda _: None,
        )
        consumer.redis_client = redis
        consumer.running = True

        with pytest.raises(MarketStreamPendingError, match="unresolved deliveries"):
            consumer._consume_loop()

        assert redis.reads == 0
        assert stream_b not in redis.registered_streams

    def test_upgrade_bootstrap_finds_unregistered_removed_stream_pending(self):
        stream_a = "stream:market:rithmic:mnq-202609:1m"
        stream_b = "stream:market:rithmic:es-202609:1m"
        redis = _BehaviorRedis(
            [],
            pending=1,
            existing_group_streams=[stream_a],
        )
        consumer = DataConsumer(
            channels=[],
            channel_provider=lambda: [stream_b],
            on_message_callback=lambda _: None,
        )
        consumer.redis_client = redis
        consumer.running = True

        with pytest.raises(MarketStreamPendingError, match="unresolved deliveries"):
            consumer._consume_loop()

        assert redis.reads == 0
        assert redis.registered_streams == {stream_a}

    def test_stream_is_durably_registered_before_read(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _BehaviorRedis([])
        consumer = DataConsumer(channels=[stream], on_message_callback=lambda _: None)
        consumer.redis_client = redis
        consumer.running = True

        def stop_after_observing_registry(**_kwargs):
            assert stream in redis.registered_streams
            consumer.running = False
            return []

        redis.xreadgroup = stop_after_observing_registry
        consumer._consume_loop()

        assert stream in redis.registered_streams

    def test_registry_failure_blocks_reading_market_data(self, mock_redis):
        stream = "stream:market:rithmic:mnq-202609:1m"
        with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
            consumer = DataConsumer(
                channels=[stream],
                on_message_callback=MagicMock(),
            )
        mock_redis.smembers.side_effect = redis_lib.exceptions.ConnectionError(
            "registry unavailable"
        )
        consumer.running = True

        with pytest.raises(
            redis_lib.exceptions.ConnectionError,
            match="registry unavailable",
        ):
            consumer._consume_loop()

        mock_redis.xreadgroup.assert_not_called()

    def test_read_response_failure_blocks_polled_stream_until_pel_check(
        self,
        mock_redis,
    ):
        stream = "stream:market:rithmic:mnq-202609:1m"
        with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
            consumer = DataConsumer(
                channels=[stream],
                on_message_callback=MagicMock(),
            )
        mock_redis.xreadgroup.side_effect = redis_lib.exceptions.ConnectionError(
            "response lost"
        )
        consumer.running = True

        with pytest.raises(redis_lib.exceptions.ConnectionError, match="response lost"):
            consumer._consume_loop()

        assert consumer._blocked_streams == {stream}

    def test_callback_failure_leaves_message_pending_and_stops(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _BehaviorRedis(
            [("1704067200000-0", _candle_payload(1704067200000, "20000"))]
        )
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=lambda _candle: (_ for _ in ()).throw(
                RuntimeError("strategy failed")
            ),
        )
        consumer.redis_client = redis
        consumer.running = True

        with pytest.raises(MarketStreamPendingError, match="market callback failed"):
            consumer._consume_loop()

        assert redis.acked == []

    def test_unparseable_message_leaves_message_pending_and_stops(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _BehaviorRedis([("1704067200000-0", {"invalid": "payload"})])
        consumer = DataConsumer(channels=[stream], on_message_callback=lambda _: None)
        consumer.redis_client = redis
        consumer.running = True

        with pytest.raises(MarketStreamPendingError, match="unparseable"):
            consumer._consume_loop()

        assert redis.acked == []

    def test_ack_failure_retries_ack_without_replaying_callback(self, mock_redis):
        stream = "stream:market:rithmic:mnq-202609:1m"
        callback = MagicMock()
        with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
            consumer = DataConsumer(channels=[stream], on_message_callback=callback)
        message_id = "1704067200000-0"
        reads = iter(
            [[(stream, [(message_id, _candle_payload(1704067200000, "20000"))])]]
        )

        def read_then_stop(**_kwargs):
            try:
                return next(reads)
            except StopIteration:
                consumer.running = False
                return []

        mock_redis.xreadgroup.side_effect = read_then_stop
        mock_redis.xack.side_effect = [
            redis_lib.exceptions.ConnectionError("redis disconnected"),
            1,
        ]
        mock_redis.xpending.side_effect = [
            {"pending": 0},
            {"pending": 0},
            {"pending": 0},
        ]

        with patch("src.core.consumer.time.sleep"):
            consumer.start()

        callback.assert_called_once()
        assert mock_redis.xack.call_count == 2
        assert consumer._completed_pending == set()
        assert consumer._blocked_streams == set()

    def test_abandoned_pending_delivery_blocks_new_market_data(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _BehaviorRedis([], pending=1)
        consumer = DataConsumer(channels=[stream], on_message_callback=lambda _: None)
        consumer.redis_client = redis
        consumer.running = True

        with pytest.raises(MarketStreamPendingError, match="unresolved deliveries"):
            consumer._consume_loop()

        assert redis.reads == 0

    def test_redis_error_invalidates_consumer_group_cache(self, mock_redis):
        with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
            consumer = DataConsumer(
                channels=["stream:market:binance:btcusdt:1m"],
                on_message_callback=MagicMock(),
            )
        consumer._initialized_channels.add(consumer.channels[0])
        consumer._consume_loop = MagicMock(
            side_effect=[
                redis_lib.exceptions.RedisError(
                    "NOGROUP No such key or consumer group"
                ),
                None,
            ]
        )

        with patch("src.core.consumer.time.sleep"):
            consumer.start()

        assert consumer._initialized_channels == set()
        assert consumer._registered_streams == set()
        assert consumer._existing_group_streams_scanned is False
        assert consumer._durable_registry_loaded is False
        assert consumer._consume_loop.call_count == 2


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

    def test_parse_rithmic_dated_future_without_rewriting_product_id(self, consumer):
        payload = json.dumps({
            "product_id": "RITHMIC:MNQ-202509",
            "timeframe": "1m",
            "timestamp": 1704067200000,
            "open": "20000.00",
            "high": "20000.25",
            "low": "19999.75",
            "close": "20000.00",
            "volume": "10",
        })

        result = consumer._parse_message(
            "stream:market:rithmic:mnq-202509:1m",
            {"json": payload},
        )

        assert isinstance(result, Candlestick)
        assert result.product_id == "RITHMIC:MNQ-202509"

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

        # 'unknown' is not a canonical product ID, so validation fails.
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
