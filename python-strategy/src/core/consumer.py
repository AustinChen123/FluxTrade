import copy
import json
import logging
import os
import time

import redis
from decimal import Decimal
from typing import Callable, List, Union
from dotenv import load_dotenv
from src.core.models import Candlestick, Trade
from src.core.redis_factory import create_redis_client
from src.core.metrics import CONSUMER_LAG_MS

# Load env
load_dotenv(os.path.join(os.path.dirname(__file__), '../../../.env'))

logger = logging.getLogger(__name__)

INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 300.0
MAX_RETRIES = 10


class DataConsumer:
    def __init__(self, channels: List[str], on_message_callback: Callable[[Union[Candlestick, Trade]], None]):
        """
        :param channels: List of Redis Stream Keys to consume (e.g., ['stream:market:binance:btcusdt'])
        :param on_message_callback: Function to call when a valid data item is received
        """
        self.redis_client = create_redis_client()
        self.channels = channels
        self.callback = on_message_callback
        self.running = False
        self.group_name = "strategy_group"
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
                self._ensure_consumer_groups()
                self._consume_loop()
                # _consume_loop exits cleanly when self.running is False
                break
            except KeyboardInterrupt:
                self.stop()
                break
            except redis.exceptions.ConnectionError as e:
                attempts += 1
                if attempts > MAX_RETRIES:
                    logger.error("Max reconnection attempts (%d) exceeded. Giving up.", MAX_RETRIES)
                    raise
                logger.warning("Redis connection lost: %s. Reconnecting in %.1fs (attempt %d/%d)",
                               e, backoff, attempts, MAX_RETRIES)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
            except Exception as e:
                attempts += 1
                if attempts > MAX_RETRIES:
                    logger.error("Max reconnection attempts (%d) exceeded. Giving up.", MAX_RETRIES)
                    raise
                logger.error("Stream Consumer Error: %s. Reconnecting in %.1fs (attempt %d/%d)",
                             e, backoff, attempts, MAX_RETRIES)
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)

    def _ensure_consumer_groups(self):
        """Create consumer groups for all channels if they don't exist."""
        for stream_key in self.channels:
            try:
                self.redis_client.xgroup_create(stream_key, self.group_name, id='$', mkstream=True)
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    logger.error("Error creating group for %s: %s", stream_key, e)

    def _consume_loop(self):
        """Inner xreadgroup loop. Exits when self.running is False or on error."""
        while self.running:
            # 1. Read from Streams
            streams = {key: '>' for key in self.channels}
            response = self.redis_client.xreadgroup(
                groupname=self.group_name,
                consumername=self.consumer_name,
                streams=streams,
                count=10,
                block=100
            )

            if not response:
                continue

            for stream_key, messages in response:
                if not messages:
                    continue

                # 2. Conflation Logic
                # Check the timestamp of the *latest* message in this batch
                # Note: ID format is "timestamp-sequence"
                last_msg_id, _ = messages[-1]
                last_msg_ts = int(last_msg_id.split('-')[0])
                t = self.redis_client.time()
                server_time_ms = int(t[0] * 1000) + int(t[1] / 1000)

                lag = server_time_ms - last_msg_ts
                CONSUMER_LAG_MS.labels(stream_key=stream_key).set(lag)

                if lag > 100:
                    # Lag > 100ms: CONFLATE backlog
                    logger.warning("LAG DETECTED (%dms > 100ms) on %s. Conflating %d msgs.",
                                   lag, stream_key, len(messages))

                    # 1. Ack all
                    msg_ids = [mid for mid, _ in messages]
                    self.redis_client.xack(stream_key, self.group_name, *msg_ids)

                    # 2. Synthesize Batch
                    synthesized_model = None
                    total_qty = Decimal("0")
                    max_price = Decimal("-Infinity")
                    min_price = Decimal("Infinity")

                    for _, m_data in messages:
                        m_model = self._parse_message(stream_key, m_data)
                        if not m_model:
                            continue

                        if isinstance(m_model, Trade):
                            total_qty += m_model.quantity
                            max_price = max(max_price, m_model.price)
                            min_price = min(min_price, m_model.price)
                        elif isinstance(m_model, Candlestick):
                            total_qty += m_model.volume
                            max_price = max(max_price, m_model.high)
                            min_price = min(min_price, m_model.low)

                        synthesized_model = m_model  # Keep latest as base

                    if synthesized_model:
                        # Copy to avoid mutating the original parsed object
                        synthesized_model = copy.copy(synthesized_model)
                        if isinstance(synthesized_model, Trade):
                            synthesized_model.quantity = total_qty
                        elif isinstance(synthesized_model, Candlestick):
                            synthesized_model.volume = total_qty
                            synthesized_model.high = max_price
                            synthesized_model.low = min_price
                            # OHLC invariant check
                            if synthesized_model.high < max(synthesized_model.open, synthesized_model.close):
                                logger.warning("OHLC invariant violated: high < max(open, close)")
                                synthesized_model.high = max(synthesized_model.open, synthesized_model.close, synthesized_model.high)
                            if synthesized_model.low > min(synthesized_model.open, synthesized_model.close):
                                logger.warning("OHLC invariant violated: low > min(open, close)")
                                synthesized_model.low = min(synthesized_model.open, synthesized_model.close, synthesized_model.low)

                        self.callback(synthesized_model)

                    # Jump to latest
                    self.redis_client.xgroup_setid(stream_key, self.group_name, '$')
                    continue

                # 3. Process Messages Normally
                for message_id, data in messages:
                    try:
                        model = self._parse_message(stream_key, data)
                        if model:
                            self.callback(model)
                            self.redis_client.xack(stream_key, self.group_name, message_id)
                    except Exception as e:
                        logger.error("Error processing %s: %s", message_id, e)

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
                    side=data.get('side', 'BUY'),
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
