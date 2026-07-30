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
from unittest.mock import MagicMock, call, patch

import pytest

import redis as redis_lib
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError
from src.core.consumer import (
    DataConsumer,
    FENCED_EPHEMERAL_GROUP_CLEANUP,
    FENCED_XACK,
    FENCED_XREADGROUP,
    INITIAL_BACKOFF,
    MarketStreamOwnershipError,
    MarketStreamPendingError,
    RELEASE_OWNERSHIP_LEASE,
    RENEW_OWNERSHIP_LEASE,
    XAUTOCLAIM_WITH_QUARANTINE,
)
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
    client.xgroup_destroy.return_value = 1
    client.time.return_value = (1704067200, 0)  # (seconds, microseconds)
    client.xack.return_value = 1
    def eval_script(script, *args):
        if script in {RENEW_OWNERSHIP_LEASE, RELEASE_OWNERSHIP_LEASE}:
            return 1
        if script == FENCED_EPHEMERAL_GROUP_CLEANUP:
            stream_keys = args[4:-2]
            pending = []
            existing_streams = []
            for index, stream_key in enumerate(stream_keys, start=4):
                try:
                    summary = client.xpending(stream_key, args[-1])
                except redis_lib.exceptions.ResponseError as error:
                    if "NOGROUP" in str(error):
                        continue
                    raise
                pending.append((index, summary))
                existing_streams.append(stream_key)
            for index, summary in pending:
                count = (
                    int(summary.get("pending", 0))
                    if isinstance(summary, dict)
                    else int(summary[0])
                )
                if count:
                    return [1, count, index]
            for stream_key in existing_streams:
                client.xgroup_destroy(stream_key, args[-1])
            client.delete(args[2], args[3])
            return [1, 0, len(existing_streams)]
        if script == FENCED_XACK:
            return [1, client.xack(args[3], args[4], args[5])]
        if script == FENCED_XREADGROUP:
            response = client.xreadgroup(count=int(args[6]))
            if not response:
                return [1]
            return [
                1,
                [
                    [
                        stream,
                        [
                            [
                                message_id,
                                [
                                    item
                                    for key, value in fields.items()
                                    for item in (key, value)
                                ],
                            ]
                            for message_id, fields in messages
                        ],
                    ]
                    for stream, messages in response
                ],
            ]
        raise AssertionError("unexpected Lua script")

    client.eval.side_effect = eval_script
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


def test_custom_group_name_isolated_from_strategy_group(mock_redis):
    with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
        consumer = DataConsumer(
            channels=[],
            on_message_callback=MagicMock(),
            group_name="paper_forward:run-1",
        )

    assert consumer.group_name == "paper_forward:run-1"
    assert "paper_forward:run-1" in consumer._stream_registry_key
    assert "paper_forward:run-1" in consumer._ownership_key


@pytest.mark.parametrize("group_name", ["", "paper forward", " paper"])
def test_invalid_group_name_rejected(group_name):
    with pytest.raises(
        ValueError,
        match="group_name must be non-empty without whitespace",
    ):
        DataConsumer(
            channels=[],
            on_message_callback=MagicMock(),
            group_name=group_name,
        )


def test_unresolved_delivery_assertion_reports_blocked_stream(consumer):
    consumer._blocked_streams.add("stream:market:rithmic:mnq-202609:5m")

    with pytest.raises(
        MarketStreamPendingError,
        match="unresolved local deliveries",
    ):
        consumer.assert_no_unresolved_deliveries()


def test_cleanup_consumer_group_removes_only_registered_ephemeral_state(
    mock_redis,
):
    stream = "stream:market:binance:btcusdt:1m"
    with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=MagicMock(),
            group_name="paper_forward:test",
            ephemeral_group=True,
        )
    consumer._registered_streams.add(stream)
    consumer._initialized_channels.add(stream)
    consumer._ownership_active = True
    mock_redis.get.return_value = consumer._ownership_token

    consumer.cleanup_consumer_group()

    mock_redis.xgroup_destroy.assert_called_once_with(
        stream,
        consumer.group_name,
    )
    mock_redis.delete.assert_called_once_with(
        consumer._stream_registry_key,
        consumer._quarantine_key,
    )
    assert consumer._registered_streams == set()
    assert consumer._initialized_channels == set()


def test_durable_consumer_rejects_group_cleanup(consumer):
    with pytest.raises(
        RuntimeError,
        match="requires ephemeral_group=True",
    ):
        consumer.cleanup_consumer_group()


def test_ephemeral_group_cleanup_is_retry_safe_after_partial_destroy(
    mock_redis,
):
    streams = [
        "stream:market:rithmic:mnq-202609:1m",
        "stream:market:rithmic:mnq-202609:5m",
    ]
    with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
        consumer = DataConsumer(
            channels=streams,
            on_message_callback=MagicMock(),
            group_name="paper_forward:retry",
            ephemeral_group=True,
        )
    consumer._registered_streams.update(streams)
    consumer._ownership_active = True
    mock_redis.get.return_value = consumer._ownership_token
    original_eval = mock_redis.eval.side_effect
    cleanup_attempts = 0

    def fail_once(script, *args):
        nonlocal cleanup_attempts
        if script == FENCED_EPHEMERAL_GROUP_CLEANUP:
            cleanup_attempts += 1
            if cleanup_attempts == 1:
                raise redis_lib.exceptions.ConnectionError("lost during cleanup")
        return original_eval(script, *args)

    mock_redis.eval.side_effect = fail_once
    with pytest.raises(redis_lib.exceptions.ConnectionError):
        consumer.cleanup_consumer_group()
    mock_redis.delete.assert_not_called()

    def pending(stream_key, _group_name):
        if stream_key == streams[0]:
            raise redis_lib.exceptions.ResponseError("NOGROUP missing")
        return {"pending": 0}

    mock_redis.xpending.side_effect = pending
    consumer.cleanup_consumer_group()

    mock_redis.xgroup_destroy.assert_called_once_with(
        streams[1],
        consumer.group_name,
    )
    mock_redis.delete.assert_called_once_with(
        consumer._stream_registry_key,
        consumer._quarantine_key,
    )
    assert consumer._registered_streams == set()


def test_ephemeral_cleanup_rejects_ownership_loss_before_destructive_work(
    mock_redis,
):
    stream = "stream:market:rithmic:mnq-202609:5m"
    with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=MagicMock(),
            group_name="paper_forward:ownership-loss",
            ephemeral_group=True,
        )
    consumer._registered_streams.add(stream)
    consumer._ownership_active = True
    mock_redis.get.return_value = consumer._ownership_token
    original_eval = mock_redis.eval.side_effect

    def lose_ownership_at_cleanup(script, *args):
        if script == FENCED_EPHEMERAL_GROUP_CLEANUP:
            return [0]
        return original_eval(script, *args)

    mock_redis.eval.side_effect = lose_ownership_at_cleanup

    with pytest.raises(
        MarketStreamOwnershipError,
        match="ownership was lost before cleanup",
    ):
        consumer.cleanup_consumer_group()

    mock_redis.xgroup_destroy.assert_not_called()
    mock_redis.delete.assert_not_called()


def test_ephemeral_cleanup_preserves_group_with_pending_deliveries(mock_redis):
    stream = "stream:market:rithmic:mnq-202609:5m"
    with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=MagicMock(),
            group_name="paper_forward:pending",
            ephemeral_group=True,
        )
    consumer._registered_streams.add(stream)
    consumer._ownership_active = True
    mock_redis.get.return_value = consumer._ownership_token
    mock_redis.xpending.return_value = {"pending": 1}

    with pytest.raises(
        MarketStreamPendingError,
        match="has 1 pending deliveries",
    ):
        consumer.cleanup_consumer_group()

    mock_redis.xgroup_destroy.assert_not_called()
    mock_redis.delete.assert_not_called()
    assert consumer._registered_streams == {stream}


def test_ephemeral_cleanup_includes_initialized_unregistered_streams(mock_redis):
    registered = "stream:market:rithmic:mnq-202609:1m"
    initialized = "stream:market:rithmic:mnq-202609:5m"
    with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
        consumer = DataConsumer(
            channels=[registered, initialized],
            on_message_callback=MagicMock(),
            group_name="paper_forward:multi-stream",
            ephemeral_group=True,
        )
    consumer._registered_streams.add(registered)
    consumer._initialized_channels.update({registered, initialized})
    consumer._ownership_active = True
    mock_redis.get.return_value = consumer._ownership_token

    consumer.cleanup_consumer_group()

    assert mock_redis.xgroup_destroy.call_args_list == [
        call(registered, consumer.group_name),
        call(initialized, consumer.group_name),
    ]
    assert consumer._registered_streams == set()
    assert consumer._initialized_channels == set()


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

        assert [call.args[3] for call in mock_redis.eval.call_args_list] == channels
        assert all(call.args[-1] == 1 for call in mock_redis.eval.call_args_list)


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

        assert sleep.call_args_list == [call(0.1), call(0.1)]
        mock_redis.xreadgroup.assert_called_once()
        assert mock_redis.eval.call_args.args[3] == stream
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
        self.quarantined_deliveries = set()
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

    def smembers(self, key):
        if str(key).endswith(":quarantine"):
            return set(self.quarantined_deliveries)
        return set(self.registered_streams)

    def sadd(self, key, *stream_keys):
        if str(key).endswith(":quarantine"):
            self.quarantined_deliveries.update(stream_keys)
            return len(stream_keys)
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

    def eval(self, script, *args):
        if script == FENCED_XACK:
            return [1, self.xack(args[3], args[4], args[5])]
        if script != FENCED_XREADGROUP:
            raise AssertionError("unexpected Lua script")
        response = self.xreadgroup(count=int(args[6]))
        if not response:
            return [1]
        return [
            1,
            [
                [
                    stream,
                    [
                        [
                            message_id,
                            [
                                item
                                for key, value in fields.items()
                                for item in (key, value)
                            ],
                        ]
                        for message_id, fields in messages
                    ],
                ]
                for stream, messages in response
            ],
        ]

    def time(self):
        return self.server_time

    def xack(self, stream, group, message_id):
        self.acked.append((stream, group, message_id))
        self.pending -= 1
        return 1

    def close(self):
        return None


class _PendingRedis(_BehaviorRedis):
    def __init__(
        self,
        stream: str,
        pending_messages,
        *,
        claimable: bool = True,
        deleted_ids=(),
    ):
        super().__init__([], pending=len(pending_messages))
        self.stream = stream
        self.pending_messages = list(pending_messages)
        self.claimable = claimable
        self.deleted_ids = list(deleted_ids)
        self.claim_calls = []
        self.registered_streams.add(stream)

    def xautoclaim(
        self,
        stream,
        group,
        consumer,
        min_idle_time,
        start_id,
        *,
        count,
    ):
        self.claim_calls.append(
            (stream, group, consumer, min_idle_time, start_id, count)
        )
        claimed = self.pending_messages if self.claimable else []
        if self.deleted_ids:
            self.pending = 0
        return ("0-0", claimed, self.deleted_ids)

    def eval(self, script, *args):
        if script in {FENCED_XREADGROUP, FENCED_XACK}:
            return super().eval(script, *args)
        assert script == XAUTOCLAIM_WITH_QUARANTINE
        (
            _key_count,
            stream,
            quarantine_key,
            _ownership_key,
            _ownership_token,
            group,
            consumer,
            min_idle_time,
            start_id,
            count,
        ) = args
        response = self.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time,
            start_id,
            count=count,
        )
        for message_id in response[2]:
            assert str(quarantine_key).endswith(":quarantine")
            self.quarantined_deliveries.add(
                json.dumps(
                    {
                        "stream": stream,
                        "message_id": message_id,
                        "reason": "payload_deleted_while_pending",
                    },
                    sort_keys=True,
                )
            )
        return [
            1,
            (
                response[0],
                [
                    (
                        message_id,
                        [
                            item
                            for key, value in fields.items()
                            for item in (key, value)
                        ],
                    )
                    for message_id, fields in response[1]
                ],
                response[2],
            ),
        ]


class _LeasePendingRedis(_PendingRedis):
    def __init__(self, stream):
        super().__init__(stream, [])
        self.owner = None
        self.expire_before_fenced_read = False
        self.expire_before_fenced_ack = False

    def set(self, _key, value, *, nx, px):
        assert nx is True
        assert px > 0
        if self.owner is not None:
            return False
        self.owner = value
        return True

    def get(self, _key):
        return self.owner

    def eval(self, script, *args):
        if script == FENCED_XACK:
            token = args[2]
            if self.expire_before_fenced_ack:
                self.owner = "successor"
                self.expire_before_fenced_ack = False
            if self.owner != token:
                return [0]
            return _BehaviorRedis.eval(self, script, *args)
        if script == FENCED_XREADGROUP:
            token = args[3]
            if self.expire_before_fenced_read:
                self.owner = "successor"
                self.expire_before_fenced_read = False
            if self.owner != token:
                return [0]
            return _BehaviorRedis.eval(self, script, *args)
        if script == XAUTOCLAIM_WITH_QUARANTINE:
            token = args[4]
            if self.owner != token:
                return [0]
            return super().eval(script, *args)
        if script == RENEW_OWNERSHIP_LEASE:
            token = args[2]
            return 1 if self.owner == token else 0
        if script == RELEASE_OWNERSHIP_LEASE:
            token = args[2]
            if self.owner == token:
                self.owner = None
                return 1
            return 0
        raise AssertionError("unexpected Lua script")


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

    def test_fenced_read_rejects_lease_expiry_before_claim(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _LeasePendingRedis(stream)
        redis.messages = [
            ("1704067200000-0", _candle_payload(1704067200000, "20000"))
        ]
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=MagicMock(),
        )
        consumer.redis_client = redis
        consumer._acquire_ownership()
        redis.expire_before_fenced_read = True
        consumer.running = True

        with pytest.raises(
            MarketStreamOwnershipError,
            match="ownership was lost before claim",
        ):
            consumer._consume_loop()

        assert redis.reads == 0
        assert len(redis.messages) == 1

    def test_pending_replay_losing_ownership_does_not_ack(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        first = ("1704067200000-0", _candle_payload(1704067200000, "20000"))
        redis = _LeasePendingRedis(stream)
        redis.pending_messages = [first]
        redis.pending = 1
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=MagicMock(),
            pending_replay_callback=lambda _model: setattr(
                redis,
                "owner",
                "successor",
            ),
            pending_claim_idle_ms=0,
        )
        consumer.redis_client = redis
        consumer._acquire_ownership()

        with pytest.raises(
            MarketStreamOwnershipError,
            match="consumer ownership was lost",
        ):
            consumer._ensure_no_abandoned_pending([stream])

        assert redis.acked == []
        assert redis.pending == 1

    def test_fenced_ack_rejects_takeover_before_pel_removal(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _LeasePendingRedis(stream)
        redis.pending = 1
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=MagicMock(),
        )
        consumer.redis_client = redis
        consumer._acquire_ownership()
        redis.expire_before_fenced_ack = True

        with pytest.raises(
            MarketStreamOwnershipError,
            match="ownership was lost before ACK",
        ):
            consumer._ack_message(stream, "1704067200000-0")

        assert redis.acked == []
        assert redis.pending == 1

    def test_pending_claim_is_atomically_rejected_after_ownership_loss(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        first = ("1704067200000-0", _candle_payload(1704067200000, "20000"))
        redis = _LeasePendingRedis(stream)
        redis.pending_messages = [first]
        redis.pending = 1
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=MagicMock(),
            pending_replay_callback=MagicMock(),
            pending_claim_idle_ms=0,
        )
        consumer.redis_client = redis
        consumer._acquire_ownership()
        redis.owner = "successor"

        with pytest.raises(
            MarketStreamOwnershipError,
            match="ownership was lost before pending claim",
        ):
            consumer._ensure_no_abandoned_pending([stream])

        assert redis.claim_calls == []
        assert redis.pending == 1

    def test_takeover_fences_old_consumer_before_next_delivery(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        first = ("1704067200000-0", _candle_payload(1704067200000, "20000"))
        second = ("1704067260000-0", _candle_payload(1704067260000, "20001"))
        redis = _LeasePendingRedis(stream)
        redis.messages = [first, second]
        old_callbacks = []
        replayed = []
        new_callbacks = []

        old = DataConsumer(
            channels=[stream],
            on_message_callback=lambda model: old_callbacks.append(model.timestamp),
            pending_claim_idle_ms=0,
        )
        new = DataConsumer(
            channels=[stream],
            on_message_callback=lambda model: (
                new_callbacks.append(model.timestamp),
                setattr(new, "running", False),
            ),
            pending_replay_callback=lambda model: replayed.append(model.timestamp),
            pending_claim_idle_ms=0,
        )
        old.redis_client = redis
        new.redis_client = redis
        old._acquire_ownership()

        def lose_ownership_after_callback(model):
            old_callbacks.append(model.timestamp)
            redis.pending_messages = [first]
            redis.owner = None
            new._acquire_ownership()

        old.callback = lose_ownership_after_callback
        old.running = True
        with pytest.raises(
            MarketStreamPendingError,
            match="consumer ownership was lost",
        ):
            old._consume_loop()

        assert old_callbacks == [1704067200000]
        assert redis.acked == []
        assert redis.reads == 1

        new._ensure_no_abandoned_pending([stream])
        new.running = True
        new._consume_loop()

        assert replayed == [1704067200000]
        assert new_callbacks == [1704067260000]
        assert redis.reads == 2

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
        mock_redis.set.return_value = True
        mock_redis.get.side_effect = lambda _key: consumer._ownership_token
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

        with patch.object(
            consumer._stop_requested,
            "wait",
            return_value=False,
        ):
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

    def test_idle_pending_delivery_rebuilds_then_replays_and_acks(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        message_id = "1704067200000-0"
        redis = _PendingRedis(
            stream,
            [(message_id, _candle_payload(1704067200000, "20000"))],
        )
        actions = []
        normal_callback = MagicMock()
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=normal_callback,
            pending_replay_callback=lambda model: actions.append(
                ("replay", model.timestamp)
            ),
            pending_claim_idle_ms=0,
        )
        consumer.redis_client = redis

        consumer._ensure_no_abandoned_pending([stream])

        assert actions == [("replay", 1704067200000)]
        normal_callback.assert_not_called()
        assert redis.acked == [(stream, consumer.group_name, message_id)]
        assert redis.claim_calls[0][3] == 0

    def test_non_idle_pending_delivery_remains_backpressured(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _PendingRedis(
            stream,
            [
                (
                    "1704067200000-0",
                    _candle_payload(1704067200000, "20000"),
                )
            ],
            claimable=False,
        )
        callback = MagicMock()
        replay = MagicMock()
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=callback,
            pending_replay_callback=replay,
        )
        consumer.redis_client = redis

        with pytest.raises(
            MarketStreamPendingError,
            match="not idle enough to reclaim",
        ):
            consumer._ensure_no_abandoned_pending([stream])

        replay.assert_not_called()
        callback.assert_not_called()
        assert redis.acked == []

    def test_deleted_pending_payload_fails_closed(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _PendingRedis(
            stream,
            [],
            deleted_ids=["1704067200000-0"],
        )
        redis.pending = 1
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=MagicMock(),
            pending_replay_callback=MagicMock(),
            pending_claim_idle_ms=0,
        )
        consumer.redis_client = redis

        with pytest.raises(
            MarketStreamPendingError,
            match="pending market payload was deleted",
        ):
            consumer._ensure_no_abandoned_pending([stream])

        assert redis.acked == []
        assert redis.quarantined_deliveries
        consumer.running = True

        with pytest.raises(
            MarketStreamPendingError,
            match="quarantine requires operator recovery",
        ):
            consumer._consume_loop()
        assert redis.reads == 0

    def test_zero_ack_result_remains_ambiguous(self, consumer, mock_redis):
        mock_redis.xack.return_value = 0

        with pytest.raises(
            MarketStreamPendingError,
            match="ACK removed 0 deliveries",
        ):
            consumer._ack_message(
                "stream:market:rithmic:mnq-202609:1m",
                "1704067200000-0",
            )

    def test_completed_callback_accepts_ack_response_lost_when_pel_is_absent(
        self,
        consumer,
        mock_redis,
    ):
        stream = "stream:market:rithmic:mnq-202609:1m"
        message_id = "1704067200000-0"
        consumer._completed_pending.add((stream, message_id))
        mock_redis.xack.return_value = 0
        mock_redis.xpending_range.return_value = []
        mock_redis.xpending.return_value = {"pending": 0}

        consumer._ensure_no_abandoned_pending([stream])

        assert consumer._completed_pending == set()
        mock_redis.xpending_range.assert_called_once_with(
            stream,
            consumer.group_name,
            min=message_id,
            max=message_id,
            count=1,
        )

    def test_pending_replay_failure_keeps_delivery_unacked(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _PendingRedis(
            stream,
            [
                (
                    "1704067200000-0",
                    _candle_payload(1704067200000, "20000"),
                )
            ],
        )
        callback = MagicMock()
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=callback,
            pending_replay_callback=MagicMock(
                side_effect=RuntimeError("database unavailable")
            ),
            pending_claim_idle_ms=0,
        )
        consumer.redis_client = redis

        with pytest.raises(
            MarketStreamPendingError,
            match="pending market callback failed",
        ):
            consumer._ensure_no_abandoned_pending([stream])

        callback.assert_not_called()
        assert redis.pending == 1
        assert redis.acked == []

    def test_pending_callback_failure_keeps_delivery_unacked(self):
        stream = "stream:market:rithmic:mnq-202609:1m"
        redis = _PendingRedis(
            stream,
            [
                (
                    "1704067200000-0",
                    _candle_payload(1704067200000, "20000"),
                )
            ],
        )
        consumer = DataConsumer(
            channels=[stream],
            on_message_callback=MagicMock(
                side_effect=RuntimeError("strategy failed")
            ),
            pending_replay_callback=MagicMock(
                side_effect=RuntimeError("strategy failed")
            ),
            pending_claim_idle_ms=0,
        )
        consumer.redis_client = redis

        with pytest.raises(
            MarketStreamPendingError,
            match="pending market callback failed",
        ):
            consumer._ensure_no_abandoned_pending([stream])

        assert redis.pending == 1
        assert redis.acked == []

    def test_redis_error_invalidates_consumer_group_cache(self, mock_redis):
        with patch("src.core.consumer.create_redis_client", return_value=mock_redis):
            consumer = DataConsumer(
                channels=["stream:market:binance:btcusdt:1m"],
                on_message_callback=MagicMock(),
            )
        mock_redis.set.return_value = True
        mock_redis.get.side_effect = lambda _key: consumer._ownership_token
        consumer._initialized_channels.add(consumer.channels[0])
        consumer._consume_loop = MagicMock(
            side_effect=[
                redis_lib.exceptions.RedisError(
                    "NOGROUP No such key or consumer group"
                ),
                None,
            ]
        )

        with patch.object(
            consumer._stop_requested,
            "wait",
            return_value=False,
        ):
            consumer.start()

        assert consumer._initialized_channels == set()
        assert consumer._registered_streams == set()
        assert consumer._existing_group_streams_scanned is False
        assert consumer._durable_registry_loaded is False
        assert consumer._consume_loop.call_count == 2

    @pytest.mark.parametrize(
        "error",
        [
            MarketStreamPendingError("pending"),
            RedisConnectionError("disconnected"),
            RedisError("redis failed"),
        ],
    )
    def test_request_stop_interrupts_retry_backoff(self, consumer, error):
        consumer._ownership_active = True
        consumer.redis_client.get.return_value = consumer._ownership_token
        consumer._consume_loop = MagicMock(side_effect=error)

        def request_stop(_delay):
            consumer.request_stop()
            return True

        with patch.object(
            consumer._stop_requested,
            "wait",
            side_effect=request_stop,
        ) as wait:
            consumer.start()

        wait.assert_called_once_with(INITIAL_BACKOFF)
        assert consumer._consume_loop.call_count == 1


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

    def test_service_ownership_guard_requires_active_lease(self, consumer):
        with pytest.raises(
            MarketStreamOwnershipError,
            match="service ownership is not active",
        ):
            consumer.assert_service_ownership()

    def test_service_ownership_guard_rejects_requested_stop(self, consumer):
        consumer.request_stop()

        with pytest.raises(
            MarketStreamOwnershipError,
            match="service stop was requested",
        ):
            consumer.assert_service_ownership()

    def test_stop_sets_running_false(self, consumer):
        """stop() should set running to False."""
        consumer.running = True
        consumer.stop()
        assert consumer.running is False

    def test_stop_closes_redis(self, consumer, mock_redis):
        """stop() should close the Redis connection."""
        consumer.stop()
        mock_redis.close.assert_called_once()
