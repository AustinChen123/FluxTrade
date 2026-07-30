import json
import logging
import os
import socket
import threading
import time
import uuid

from decimal import Decimal
from typing import Any, Callable, Iterable, List, Union, cast

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
DEFAULT_PENDING_CLAIM_IDLE_MS = 60_000
DEFAULT_OWNERSHIP_LEASE_MS = 10_000
XAUTOCLAIM_WITH_QUARANTINE = """
if redis.call('GET', KEYS[3]) ~= ARGV[1] then
    return {0}
end
local result = redis.call(
    'XAUTOCLAIM',
    KEYS[1],
    ARGV[2],
    ARGV[3],
    ARGV[4],
    ARGV[5],
    'COUNT',
    ARGV[6]
)
if result[3] then
    for _, message_id in ipairs(result[3]) do
        redis.call(
            'SADD',
            KEYS[2],
            cjson.encode({
                stream = KEYS[1],
                message_id = message_id,
                reason = 'payload_deleted_while_pending'
            })
        )
    end
end
return {1, result}
"""
RENEW_OWNERSHIP_LEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""
RELEASE_OWNERSHIP_LEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
"""
FENCED_XREADGROUP = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return {0}
end
local response = redis.call(
    'XREADGROUP',
    'GROUP',
    ARGV[2],
    ARGV[3],
    'COUNT',
    ARGV[4],
    'STREAMS',
    KEYS[2],
    '>'
)
if not response then
    return {1}
end
return {1, response}
"""
FENCED_XACK = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return {0}
end
return {
    1,
    redis.call('XACK', ARGV[2], ARGV[3], ARGV[4])
}
"""
FENCED_EPHEMERAL_GROUP_CLEANUP = """
if redis.call('GET', KEYS[1]) ~= ARGV[1] then
    return {0}
end
local existing_groups = {}
for index = 4, #KEYS do
    local pending_result = redis.pcall('XPENDING', KEYS[index], ARGV[2])
    if type(pending_result) == 'table' and pending_result.err then
        if not string.find(pending_result.err, 'NOGROUP') then
            return redis.error_reply(pending_result.err)
        end
        existing_groups[index] = false
    else
        existing_groups[index] = true
        if tonumber(pending_result[1]) > 0 then
            return {1, tonumber(pending_result[1]), index}
        end
    end
end
local destroyed = 0
for index = 4, #KEYS do
    if existing_groups[index] then
        destroyed = destroyed + redis.call(
            'XGROUP',
            'DESTROY',
            KEYS[index],
            ARGV[2]
        )
    end
end
redis.call('DEL', KEYS[2], KEYS[3])
return {1, 0, destroyed}
"""


class MarketStreamPendingError(RuntimeError):
    """A previous delivery has an ambiguous processing outcome."""


class MarketStreamOwnershipError(MarketStreamPendingError):
    """Another process currently owns market stream consumption."""


class DataConsumer:
    def __init__(
        self,
        channels: List[str],
        on_message_callback: Callable[[Union[Candlestick, Trade]], None],
        channel_provider: Callable[[], List[str]] | None = None,
        runtime_environment: RuntimeEnvironment | None = None,
        pending_replay_callback: (
            Callable[[Union[Candlestick, Trade]], None] | None
        ) = None,
        pending_claim_idle_ms: int = DEFAULT_PENDING_CLAIM_IDLE_MS,
        ownership_lease_ms: int = DEFAULT_OWNERSHIP_LEASE_MS,
        group_name: str = "strategy_group",
        ephemeral_group: bool = False,
    ):
        """
        :param channels: List of Redis Stream Keys to consume (e.g., ['stream:market:binance:btcusdt'])
        :param on_message_callback: Function to call when a valid data item is received
        """
        self.redis_client = create_redis_client()
        self.channels = channels
        self.channel_provider = channel_provider
        self.runtime_environment = runtime_environment or RuntimeEnvironment.from_env()
        if not group_name or any(char.isspace() for char in group_name):
            raise ValueError("consumer group_name must be non-empty without whitespace")
        self.group_name = group_name
        self._ephemeral_group = ephemeral_group
        self._stream_registry_key = self.runtime_environment.key(
            f"consumer:{self.group_name}:streams"
        )
        self._quarantine_key = self.runtime_environment.key(
            f"consumer:{self.group_name}:quarantine"
        )
        self._ownership_key = self.runtime_environment.key(
            f"consumer:{self.group_name}:owner"
        )
        self._initialized_channels: set[str] = set()
        self._completed_pending: set[tuple[str, str]] = set()
        self._blocked_streams: set[str] = set()
        self._registered_streams: set[str] = set()
        self._existing_group_streams_scanned = False
        self._durable_registry_loaded = False
        self._next_channel_index = 0
        self.callback = on_message_callback
        self.pending_replay_callback = pending_replay_callback
        if pending_claim_idle_ms < 0:
            raise ValueError("pending_claim_idle_ms must be non-negative")
        if ownership_lease_ms <= 0:
            raise ValueError("ownership_lease_ms must be positive")
        self.pending_claim_idle_ms = pending_claim_idle_ms
        self._ownership_lease_ms = ownership_lease_ms
        self._ownership_token = uuid.uuid4().hex
        self._ownership_lost = threading.Event()
        self._ownership_heartbeat_stop = threading.Event()
        self._ownership_heartbeat: threading.Thread | None = None
        self._ownership_active = False
        self._stop_requested = threading.Event()
        self.running = False
        self.consumer_name = (
            f"consumer_{socket.gethostname()}_{os.getpid()}_"
            f"{self._ownership_token}"
        )

    def _acquire_ownership(self) -> None:
        acquired = self.redis_client.set(
            self._ownership_key,
            self._ownership_token,
            nx=True,
            px=self._ownership_lease_ms,
        )
        if not acquired:
            raise MarketStreamOwnershipError(
                "market stream consumer ownership is held by another process"
            )
        self._ownership_lost.clear()
        self._ownership_active = True

    def _renew_ownership(self) -> None:
        renewed = self.redis_client.eval(
            RENEW_OWNERSHIP_LEASE,
            1,
            self._ownership_key,
            self._ownership_token,
            self._ownership_lease_ms,
        )
        if renewed != 1:
            self._ownership_lost.set()

    def _assert_ownership(self) -> None:
        if not self._ownership_active:
            return
        owner = self.redis_client.get(self._ownership_key)
        if isinstance(owner, bytes):
            owner = owner.decode()
        if self._ownership_lost.is_set() or owner != self._ownership_token:
            self._ownership_lost.set()
            raise MarketStreamOwnershipError(
                "market stream consumer ownership was lost"
            )

    def _ownership_heartbeat_loop(self) -> None:
        interval = max(self._ownership_lease_ms / 3 / 1000, 0.05)
        while not self._ownership_heartbeat_stop.wait(interval):
            try:
                self._renew_ownership()
            except (ConnectionError, OSError, RedisError):
                self._ownership_lost.set()
            if self._ownership_lost.is_set():
                return

    def _start_ownership_heartbeat(self) -> None:
        self._ownership_heartbeat_stop.clear()
        self._ownership_heartbeat = threading.Thread(
            target=self._ownership_heartbeat_loop,
            name="market-stream-ownership-heartbeat",
            daemon=True,
        )
        self._ownership_heartbeat.start()

    def _release_ownership(self) -> None:
        if not self._ownership_active:
            return
        self._ownership_heartbeat_stop.set()
        heartbeat = self._ownership_heartbeat
        if heartbeat is not None and heartbeat is not threading.current_thread():
            heartbeat.join(timeout=1)
        try:
            self.redis_client.eval(
                RELEASE_OWNERSHIP_LEASE,
                1,
                self._ownership_key,
                self._ownership_token,
            )
        except (ConnectionError, OSError, RedisError):
            pass
        self._ownership_active = False

    def _establish_ownership(self) -> None:
        while self.running and not self._stop_requested.is_set():
            try:
                self._acquire_ownership()
            except MarketStreamOwnershipError:
                time.sleep(INITIAL_BACKOFF)
                continue
            self._start_ownership_heartbeat()
            return

    def acquire_service_ownership(self) -> None:
        """Become the sole live service before engine startup."""
        self.running = True
        self._establish_ownership()
        if not self._ownership_active:
            raise RuntimeError("market stream ownership acquisition cancelled")

    def assert_service_ownership(self) -> None:
        """Reject startup work after shutdown or leadership loss."""
        if self._stop_requested.is_set():
            raise MarketStreamOwnershipError(
                "market stream service stop was requested"
            )
        if not self._ownership_active:
            raise MarketStreamOwnershipError(
                "market stream service ownership is not active"
            )
        self._assert_ownership()

    def configure_callbacks(
        self,
        *,
        on_message_callback: Callable[[Union[Candlestick, Trade]], None],
        channel_provider: Callable[[], List[str]],
        pending_replay_callback: Callable[[Union[Candlestick, Trade]], None],
    ) -> None:
        self.callback = on_message_callback
        self.channel_provider = channel_provider
        self.pending_replay_callback = pending_replay_callback
        self.channels = channel_provider()

    def start(self):
        """Outer reconnection loop with exponential backoff."""
        if self._stop_requested.is_set():
            return
        self.running = True
        if not self._ownership_active:
            self._establish_ownership()
        logger.info("DataConsumer started. Stream Group: %s | Consumer: %s",
                     self.group_name, self.consumer_name)

        backoff = INITIAL_BACKOFF
        attempts = 0

        while self.running and not self._stop_requested.is_set():
            try:
                self._assert_ownership()
                self._consume_loop()
                # _consume_loop exits cleanly when self.running is False
                break
            except KeyboardInterrupt:
                self.request_stop()
                break
            except MarketStreamOwnershipError as e:
                logger.critical("Market stream ownership lost: %s", e)
                raise
            except MarketStreamPendingError as e:
                # Keep the process alive but stop reading newer entries. An
                # operator/recovery workflow may resolve the pending delivery;
                # the next loop rechecks the group before consumption resumes.
                self._initialized_channels.clear()
                logger.critical(
                    "Market stream blocked by ambiguous delivery: %s",
                    e,
                )
                if self._stop_requested.wait(INITIAL_BACKOFF):
                    break
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
                if self._stop_requested.wait(backoff):
                    break
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
                if self._stop_requested.wait(backoff):
                    break
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

    @staticmethod
    def _pending_count(summary: object, stream_key: str) -> int:
        if isinstance(summary, dict):
            return int(summary.get("pending", 0))
        if isinstance(summary, (list, tuple)) and summary:
            return int(summary[0])
        raise MarketStreamPendingError(
            f"invalid pending summary for {stream_key}"
        )

    @staticmethod
    def _stream_id_key(message_id: str) -> tuple[int, int]:
        milliseconds, sequence = message_id.split("-", 1)
        return int(milliseconds), int(sequence)

    @staticmethod
    def _decode_claimed_fields(raw_fields: object) -> dict:
        if isinstance(raw_fields, dict):
            return raw_fields
        if isinstance(raw_fields, (list, tuple)) and len(raw_fields) % 2 == 0:
            return dict(zip(raw_fields[::2], raw_fields[1::2]))
        raise MarketStreamPendingError(
            "invalid claimed market message fields"
        )

    def _claim_pending(
        self,
        stream_key: str,
        pending: int,
    ) -> list[tuple[str, dict]]:
        cursor = "0-0"
        claimed: list[tuple[str, dict]] = []
        deleted_ids: list[str] = []
        while True:
            response = cast(
                Any,
                self.redis_client.eval(
                    XAUTOCLAIM_WITH_QUARANTINE,
                    3,
                    stream_key,
                    self._quarantine_key,
                    self._ownership_key,
                    self._ownership_token,
                    self.group_name,
                    self.consumer_name,
                    self.pending_claim_idle_ms,
                    cursor,
                    1,
                ),
            )
            if not isinstance(response, (list, tuple)) or not response:
                raise MarketStreamPendingError(
                    f"invalid XAUTOCLAIM response for {stream_key}"
                )
            if int(response[0]) != 1:
                self._ownership_lost.set()
                raise MarketStreamOwnershipError(
                    "market stream consumer ownership was lost before pending claim"
                )
            response = response[1]
            if not isinstance(response, (list, tuple)) or len(response) not in {2, 3}:
                raise MarketStreamPendingError(
                    f"invalid XAUTOCLAIM response for {stream_key}"
                )
            cursor = (
                response[0].decode()
                if isinstance(response[0], bytes)
                else str(response[0])
            )
            claimed.extend(
                (
                    raw_message_id,
                    self._decode_claimed_fields(raw_fields),
                )
                for raw_message_id, raw_fields in response[1]
            )
            if len(response) == 3:
                deleted_ids.extend(
                    value.decode() if isinstance(value, bytes) else str(value)
                    for value in response[2]
                )
            if cursor == "0-0":
                break

        if deleted_ids:
            raise MarketStreamPendingError(
                f"pending market payload was deleted for {stream_key}: {deleted_ids}"
            )
        if len(claimed) != pending:
            raise MarketStreamPendingError(
                f"market stream has {pending - len(claimed)} pending deliveries "
                f"that are not idle enough to reclaim: {stream_key}"
            )
        return claimed

    def _ack_message(
        self,
        stream_key: str,
        message_id: str,
        *,
        allow_already_absent: bool = False,
    ) -> None:
        response = self.redis_client.eval(
            FENCED_XACK,
            1,
            self._ownership_key,
            self._ownership_token,
            stream_key,
            self.group_name,
            message_id,
        )
        if not isinstance(response, (list, tuple)) or not response:
            raise MarketStreamPendingError(
                f"invalid fenced ACK response for {stream_key}:{message_id}"
            )
        if int(response[0]) != 1:
            self._ownership_lost.set()
            raise MarketStreamOwnershipError(
                "market stream consumer ownership was lost before ACK"
            )
        if len(response) != 2:
            raise MarketStreamPendingError(
                f"invalid fenced ACK response for {stream_key}:{message_id}"
            )
        acknowledged = int(response[1])
        if acknowledged == 0 and allow_already_absent:
            pending = self.redis_client.xpending_range(
                stream_key,
                self.group_name,
                min=message_id,
                max=message_id,
                count=1,
            )
            if not pending:
                return
        if acknowledged != 1:
            raise MarketStreamPendingError(
                f"market message ACK removed {acknowledged} deliveries: "
                f"{stream_key}:{message_id}"
            )

    def _recover_abandoned_pending(
        self,
        pending_by_stream: list[tuple[str, int]],
    ) -> None:
        if self.pending_replay_callback is None:
            streams = ", ".join(stream for stream, _ in pending_by_stream)
            raise MarketStreamPendingError(
                "market stream has unresolved deliveries but pending replay "
                f"callback is not configured: {streams}"
            )

        recovered: list[tuple[str, str, Union[Candlestick, Trade]]] = []
        for stream_key, pending in pending_by_stream:
            for raw_message_id, data in self._claim_pending(stream_key, pending):
                message_id = (
                    raw_message_id.decode()
                    if isinstance(raw_message_id, bytes)
                    else str(raw_message_id)
                )
                model = self._parse_message(stream_key, data)
                if model is None:
                    raise MarketStreamPendingError(
                        f"unparseable pending market message "
                        f"{stream_key}:{message_id}"
                    )
                recovered.append((stream_key, message_id, model))

        recovered.sort(
            key=lambda item: (*self._stream_id_key(item[1]), item[0])
        )
        for stream_key, message_id, model in recovered:
            self._assert_ownership()
            try:
                self.pending_replay_callback(model)
            except Exception as exc:
                raise MarketStreamPendingError(
                    f"pending market callback failed {stream_key}:{message_id}"
                ) from exc
            self._assert_ownership()
            self._completed_pending.add((stream_key, message_id))
            self._ack_message(stream_key, message_id)
            self._completed_pending.discard((stream_key, message_id))
            self._blocked_streams.discard(stream_key)
    def _ensure_no_abandoned_pending(self, channels: list[str]) -> None:
        """Recover idle deliveries before allowing any newer market data."""
        pending_by_stream: list[tuple[str, int]] = []
        for stream_key in channels:
            completed_ids = [
                message_id
                for completed_stream, message_id in self._completed_pending
                if completed_stream == stream_key
            ]
            for message_id in completed_ids:
                self._ack_message(
                    stream_key,
                    message_id,
                    allow_already_absent=True,
                )
                self._completed_pending.discard((stream_key, message_id))
            summary = self.redis_client.xpending(stream_key, self.group_name)
            pending = self._pending_count(summary, stream_key)
            if pending:
                pending_by_stream.append((stream_key, pending))
            else:
                self._blocked_streams.discard(stream_key)
        if pending_by_stream:
            self._recover_abandoned_pending(pending_by_stream)

    def _ensure_no_quarantined_delivery(self) -> None:
        values = cast(
            set[Any],
            self.redis_client.smembers(self._quarantine_key),
        )
        if values:
            raise MarketStreamPendingError(
                "market stream quarantine requires operator recovery: "
                + ", ".join(sorted(str(value) for value in values))
            )

    def _load_durable_stream_gate(self) -> None:
        """Restore every stream this consumer group may have claimed."""
        if self._durable_registry_loaded:
            return
        self._bootstrap_existing_group_streams()
        registered_values = cast(
            set[Any],
            self.redis_client.smembers(self._stream_registry_key),
        )
        registered = {
            value.decode() if isinstance(value, bytes) else str(value)
            for value in registered_values
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
        stream_values = cast(
            Iterable[Any],
            self.redis_client.scan_iter(
                match="stream:market:*",
                _type="stream",
            ),
        )
        for value in stream_values:
            stream_key = value.decode() if isinstance(value, bytes) else str(value)
            groups = cast(
                Iterable[dict[Any, Any]],
                self.redis_client.xinfo_groups(stream_key),
            )
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

    def _read_next_owned_message(
        self,
        stream_key: str,
    ) -> list[tuple[str, list[tuple[str, dict]]]]:
        raw = cast(
            Any,
            self.redis_client.eval(
                FENCED_XREADGROUP,
                2,
                self._ownership_key,
                stream_key,
                self._ownership_token,
                self.group_name,
                self.consumer_name,
                1,
            ),
        )
        if not isinstance(raw, (list, tuple)) or not raw:
            raise MarketStreamPendingError(
                "invalid fenced XREADGROUP response"
            )
        if int(raw[0]) != 1:
            self._ownership_lost.set()
            raise MarketStreamOwnershipError(
                "market stream consumer ownership was lost before claim"
            )
        if len(raw) == 1:
            return []
        streams = []
        for raw_stream, raw_messages in raw[1]:
            stream = (
                raw_stream.decode()
                if isinstance(raw_stream, bytes)
                else str(raw_stream)
            )
            messages = [
                (
                    message_id.decode()
                    if isinstance(message_id, bytes)
                    else str(message_id),
                    self._decode_claimed_fields(fields),
                )
                for message_id, fields in raw_messages
            ]
            streams.append((stream, messages))
        return streams

    def _consume_loop(self):
        """Inner xreadgroup loop. Exits when self.running is False or on error."""
        while self.running and not self._stop_requested.is_set():
            self._assert_ownership()
            self._ensure_no_quarantined_delivery()
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
            try:
                response = self._read_next_owned_message(stream_key)
            except (ConnectionError, OSError, RedisError):
                # The server may have claimed a delivery even if its response
                # never reached this process. The next loop must inspect this
                # stream's PEL before any other stream can advance.
                self._blocked_streams.add(stream_key)
                raise

            if not response:
                time.sleep(max(0.001, 0.1 / len(channels)))
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
                    self._assert_ownership()
                    # Remember callback completion across a Redis reconnect.
                    # This process may retry only the ACK, never the callback.
                    self._completed_pending.add((stream_key, message_id))
                    self._ack_message(stream_key, message_id)
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

    def request_stop(self) -> None:
        """Stop reading while retaining leadership during engine shutdown."""
        self._stop_requested.set()
        self.running = False

    def assert_no_unresolved_deliveries(self) -> None:
        """Fail when this process stopped with callback or ACK state unresolved."""
        self._assert_ownership()
        unresolved = sorted(
            {
                *self._blocked_streams,
                *(stream for stream, _message_id in self._completed_pending),
            }
        )
        if unresolved:
            raise MarketStreamPendingError(
                "market stream has unresolved local deliveries: "
                + ", ".join(unresolved)
            )

    def cleanup_consumer_group(self) -> None:
        """Delete one clean, ephemeral consumer group's durable Redis state."""
        if not self._ephemeral_group:
            raise RuntimeError(
                "consumer group cleanup requires ephemeral_group=True"
            )
        self.assert_no_unresolved_deliveries()
        self._ensure_no_quarantined_delivery()
        streams = sorted(self._registered_streams | self._initialized_channels)
        result = cast(
            Any,
            self.redis_client.eval(
                FENCED_EPHEMERAL_GROUP_CLEANUP,
                3 + len(streams),
                self._ownership_key,
                self._stream_registry_key,
                self._quarantine_key,
                *streams,
                self._ownership_token,
                self.group_name,
            ),
        )
        if not isinstance(result, (list, tuple)) or not result:
            raise MarketStreamPendingError(
                "invalid ephemeral consumer group cleanup response"
            )
        if int(result[0]) != 1:
            self._ownership_lost.set()
            raise MarketStreamOwnershipError(
                "market stream consumer ownership was lost before cleanup"
            )
        if len(result) != 3:
            raise MarketStreamPendingError(
                "invalid ephemeral consumer group cleanup response"
            )
        pending = int(result[1])
        if pending:
            stream_index = int(result[2]) - 4
            stream = (
                streams[stream_index]
                if 0 <= stream_index < len(streams)
                else "unknown"
            )
            raise MarketStreamPendingError(
                f"market stream has {pending} pending deliveries: {stream}"
            )
        self._registered_streams.difference_update(streams)
        self._initialized_channels.difference_update(streams)

    def stop(self):
        """Release leadership and close Redis after engine shutdown."""
        self.request_stop()
        self._release_ownership()
        self.redis_client.close()
        logger.info("DataConsumer stopped.")
