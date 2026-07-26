import json
import logging
import os
import time

from decimal import Decimal
from typing import Any, Callable, List, Union, cast

from dotenv import load_dotenv
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import RedisError, ResponseError

from src.core.models import Candlestick, Trade
from src.core.redis_factory import create_redis_client
from src.core.metrics import CONSUMER_LAG_MS
from src.core.runtime_environment import RuntimeEnvironment

# Load env
load_dotenv(os.path.join(os.path.dirname(__file__), '../../../.env'))

logger = logging.getLogger(__name__)

INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 300.0
MAX_RETRIES = 10


class MarketStreamPendingError(RuntimeError):
    """A previous delivery has an ambiguous processing outcome."""


class DataConsumer:
    def __init__(
        self,
        channels: List[str],
        on_message_callback: Callable[[Union[Candlestick, Trade]], None],
        channel_provider: Callable[[], List[str]] | None = None,
        runtime_environment: RuntimeEnvironment | None = None,
    ):
        """
        :param channels: List of Redis Stream Keys to consume (e.g., ['stream:market:binance:btcusdt'])
        :param on_message_callback: Function to call when a valid data item is received
        """
        self.redis_client = create_redis_client()
        self.channels = channels
        self.channel_provider = channel_provider
        self.runtime_environment = runtime_environment or RuntimeEnvironment.from_env()
        self.group_name = "strategy_group"
        self._stream_registry_key = self.runtime_environment.key(
            f"consumer:{self.group_name}:streams"
        )
        self._initialized_channels: set[str] = set()
        self._completed_pending: set[tuple[str, str]] = set()
        self._blocked_streams: set[str] = set()
        self._registered_streams: set[str] = set()
        self._existing_group_streams_scanned = False
        self._durable_registry_loaded = False
        self._next_channel_index = 0
        self.callback = on_message_callback
        self.running = False
        self.consumer_name = f"consumer_{os.getpid()}"

    def start(self):
        """Outer reconnection loop with exponential backoff."""
        self.running = True
        logger.info("DataConsumer started. Stream Group: %s | Consumer: %s",
                     self.group_name, self.consumer_name)

        backoff = INITIAL_BACKOFF
        attempts = 0

        while self.running:
            try:
                self._consume_loop()
                # _consume_loop exits cleanly when self.running is False
                break
            except KeyboardInterrupt:
                self.stop()
                break
            except MarketStreamPendingError as e:
                # Keep the process alive but stop reading newer entries. An
                # operator/recovery workflow may resolve the pending delivery;
                # the next loop rechecks the group before consumption resumes.
                self._initialized_channels.clear()
                logger.critical(
                    "Market stream blocked by ambiguous delivery: %s",
                    e,
                )
                time.sleep(INITIAL_BACKOFF)
            except RedisConnectionError as e:
                self._initialized_channels.clear()
                self._registered_streams.clear()
                self._existing_group_streams_scanned = False
                self._durable_registry_loaded = False
                attempts += 1
                if attempts > MAX_RETRIES:
                    logger.error("Max reconnection attempts (%d) exceeded. Giving up.", MAX_RETRIES)
                    raise
                logger.warning("Redis connection lost: %s. Reconnecting in %.1fs (attempt %d/%d)",
                               e, backoff, attempts, MAX_RETRIES)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
            except (ConnectionError, OSError, RedisError) as e:
                self._initialized_channels.clear()
                self._registered_streams.clear()
                self._existing_group_streams_scanned = False
                self._durable_registry_loaded = False
                attempts += 1
                if attempts > MAX_RETRIES:
                    logger.error("Max reconnection attempts (%d) exceeded. Giving up.", MAX_RETRIES)
                    raise
                logger.error("Stream Consumer Error: %s. Reconnecting in %.1fs (attempt %d/%d)",
                             e, backoff, attempts, MAX_RETRIES)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)

    def _current_channels(self) -> list[str]:
        if self.channel_provider is not None:
            self.channels = sorted(set(self.channel_provider()))
        return self.channels

    def _ensure_consumer_groups(self, channels: list[str] | None = None):
        """Create consumer groups for all channels if they don't exist."""
        channels = self.channels if channels is None else channels
        for stream_key in channels:
            if stream_key in self._initialized_channels:
                continue
            try:
                self.redis_client.xgroup_create(stream_key, self.group_name, id='$', mkstream=True)
                self._initialized_channels.add(stream_key)
            except ResponseError as e:
                if "BUSYGROUP" in str(e):
                    self._initialized_channels.add(stream_key)
                else:
                    raise
            self._ensure_no_abandoned_pending([stream_key])

    def _ensure_no_abandoned_pending(self, channels: list[str]) -> None:
        """Fail closed rather than replay a candle with unknown order side effects."""
        for stream_key in channels:
            completed_ids = [
                message_id
                for completed_stream, message_id in self._completed_pending
                if completed_stream == stream_key
            ]
            for message_id in completed_ids:
                self.redis_client.xack(
                    stream_key,
                    self.group_name,
                    message_id,
                )
                self._completed_pending.discard((stream_key, message_id))
            summary = self.redis_client.xpending(stream_key, self.group_name)
            if isinstance(summary, dict):
                pending = int(summary.get("pending", 0))
            elif isinstance(summary, (list, tuple)) and summary:
                pending = int(summary[0])
            else:
                raise MarketStreamPendingError(
                    f"invalid pending summary for {stream_key}"
                )
            if pending:
                raise MarketStreamPendingError(
                    f"market stream has {pending} unresolved deliveries: {stream_key}"
                )
            self._blocked_streams.discard(stream_key)

    def _load_durable_stream_gate(self) -> None:
        """Restore every stream this consumer group may have claimed."""
        if self._durable_registry_loaded:
            return
        self._bootstrap_existing_group_streams()
        registered = {
            value.decode() if isinstance(value, bytes) else str(value)
            for value in self.redis_client.smembers(self._stream_registry_key)
        }
        self._registered_streams = registered
        if registered:
            self._ensure_no_abandoned_pending(sorted(registered))
        self._durable_registry_loaded = True

    def _bootstrap_existing_group_streams(self) -> None:
        """Discover streams claimed before the durable registry was introduced."""
        if self._existing_group_streams_scanned:
            return
        existing_group_streams = []
        for value in self.redis_client.scan_iter(
            match="stream:market:*",
            _type="stream",
        ):
            stream_key = value.decode() if isinstance(value, bytes) else str(value)
            groups = self.redis_client.xinfo_groups(stream_key)
            if any(
                (
                    group.get("name")
                    if "name" in group
                    else group.get(b"name")
                )
                in {self.group_name, self.group_name.encode()}
                for group in groups
            ):
                existing_group_streams.append(stream_key)
        if existing_group_streams:
            self.redis_client.sadd(
                self._stream_registry_key,
                *existing_group_streams,
            )
        self._existing_group_streams_scanned = True

    def _register_stream_before_read(self, stream_key: str) -> None:
        """Durably record the stream before Redis can assign a delivery."""
        if stream_key in self._registered_streams:
            return
        self.redis_client.sadd(self._stream_registry_key, stream_key)
        self._registered_streams.add(stream_key)

    def _consume_loop(self):
        """Inner xreadgroup loop. Exits when self.running is False or on error."""
        while self.running:
            self._load_durable_stream_gate()
            # A stream remains a global submission gate even if the dynamic
            # strategy set no longer subscribes to it. Resolve every delivery
            # with an uncertain outcome before reading any newer market data.
            if self._blocked_streams:
                self._ensure_no_abandoned_pending(sorted(self._blocked_streams))
            channels = self._current_channels()
            if not channels:
                time.sleep(0.1)
                continue
            self._ensure_consumer_groups(channels)
            # Redis applies COUNT per stream. Poll exactly one stream so this
            # process can never claim a second delivery before the first one
            # reaches an ACK-safe outcome.
            stream_key = channels[self._next_channel_index % len(channels)]
            self._next_channel_index = (self._next_channel_index + 1) % len(channels)
            self._register_stream_before_read(stream_key)
            streams = {stream_key: '>'}
            try:
                response = cast(
                    list[tuple[str, list[tuple[str, dict[str, str]]]]],
                    self.redis_client.xreadgroup(
                        groupname=self.group_name,
                        consumername=self.consumer_name,
                        streams=cast(Any, streams),
                        # Without a durable per-message callback phase, claiming
                        # more than one delivery can strand later, untouched
                        # messages behind an ambiguous callback outcome.
                        count=1,
                        block=max(1, 100 // len(channels)),
                    ),
                )
            except (ConnectionError, OSError, RedisError):
                # The server may have claimed a delivery even if its response
                # never reached this process. The next loop must inspect this
                # stream's PEL before any other stream can advance.
                self._blocked_streams.add(stream_key)
                raise

            if not response:
                continue

            for stream_key, messages in response:
                if not messages:
                    continue
                self._blocked_streams.add(stream_key)

                # Record delivery lag without changing ordered delivery.
                last_msg_id, _ = messages[-1]
                last_msg_ts = int(last_msg_id.split('-')[0])
                t = cast(tuple[int, int], self.redis_client.time())
                server_time_ms = int(t[0] * 1000) + int(t[1] / 1000)

                lag = server_time_ms - last_msg_ts
                CONSUMER_LAG_MS.labels(stream_key=stream_key).set(lag)
                for message_id, data in messages:
                    model = self._parse_message(stream_key, data)
                    if model is None:
                        raise MarketStreamPendingError(
                            f"unparseable market message {stream_key}:{message_id}"
                        )
                    try:
                        self.callback(model)
                    except Exception as exc:
                        raise MarketStreamPendingError(
                            f"market callback failed {stream_key}:{message_id}"
                        ) from exc
                    # Remember callback completion across a Redis reconnect.
                    # This process may retry only the ACK, never the callback.
                    self._completed_pending.add((stream_key, message_id))
                    self.redis_client.xack(
                        stream_key,
                        self.group_name,
                        message_id,
                    )
                    self._completed_pending.discard((stream_key, message_id))
                    self._blocked_streams.discard(stream_key)

    def _parse_message(self, stream_key: str, data: dict) -> Union[Candlestick, Trade, None]:
        """Helper to parse raw stream data into models."""
        try:
            if 'json' in data:
                payload = data['json']
                parsed = json.loads(payload)
                if 'open' in parsed:
                    return Candlestick.model_validate_json(payload)
                else:
                    return Trade.model_validate_json(payload)

            if 'price' in data and 'quantity' in data:
                return Trade(
                    id=data.get('trade_id', 'unknown'),
                    product_id=data.get('product_id', 'unknown'),
                    side=data.get('side', 'buy').lower(),
                    price=Decimal(data['price']),
                    quantity=Decimal(data['quantity']),
                    timestamp=int(data.get('timestamp', 0))
                )
            return None
        except Exception as e:
            logger.error("Parse error: %s", e)
            return None

    def stop(self):
        """Stop consuming and close Redis connection."""
        self.running = False
        self.redis_client.close()
        logger.info("DataConsumer stopped.")
