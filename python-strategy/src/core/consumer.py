import os
import redis
from decimal import Decimal
from typing import Callable, List, Union
from dotenv import load_dotenv
from src.core.models import Candlestick, Trade

# Load env
load_dotenv(os.path.join(os.path.dirname(__file__), '../../../.env'))

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

class DataConsumer:
    def __init__(self, channels: List[str], on_message_callback: Callable[[Union[Candlestick, Trade]], None]):
        """
        :param channels: List of Redis Stream Keys to consume (e.g., ['stream:market:binance:btcusdt'])
        :param on_message_callback: Function to call when a valid data item is received
        """
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.channels = channels
        self.callback = on_message_callback
        self.running = False
        self.group_name = "strategy_group"
        self.consumer_name = f"consumer_{os.getpid()}"

    def start(self):
        self.running = True
        print(f"DataConsumer started. Stream Group: {self.group_name} | Consumer: {self.consumer_name}")

        # Initialize Consumer Groups
        for stream_key in self.channels:
            try:
                self.redis_client.xgroup_create(stream_key, self.group_name, id='$', mkstream=True)
            except redis.exceptions.ResponseError as e:
                if "BUSYGROUP" not in str(e):
                    print(f"Error creating group for {stream_key}: {e}")

        try:
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
                    server_time_ms = int(self.redis_client.time()[0] * 1000) + int(self.redis_client.time()[1] / 1000)
                    
                    lag = server_time_ms - last_msg_ts
                    
                    if lag > 100:
                        # Lag > 100ms: CONFLATE backlog
                        print(f"⚠️  LAG DETECTED ({lag}ms > 100ms) on {stream_key}. Conflating {len(messages)} msgs.")
                        
                        # 1. Ack all
                        msg_ids = [mid for mid, _ in messages]
                        self.redis_client.xack(stream_key, self.group_name, *msg_ids)
                        
                        # 2. Synthesize Batch
                        # We take the latest message as the template, but accumulate volume and track H/L
                        synthesized_model = None
                        total_qty = Decimal("0")
                        max_price = Decimal("-Infinity")
                        min_price = Decimal("Infinity")

                        for _, m_data in messages:
                            m_model = self._parse_message(stream_key, m_data)
                            if not m_model: continue
                            
                            if isinstance(m_model, Trade):
                                total_qty += m_model.quantity
                                max_price = max(max_price, m_model.price)
                                min_price = min(min_price, m_model.price)
                            elif isinstance(m_model, Candlestick):
                                total_qty += m_model.volume
                                max_price = max(max_price, m_model.high)
                                min_price = min(min_price, m_model.low)
                            
                            synthesized_model = m_model # Keep latest as base

                        if synthesized_model:
                            # Update with accumulated values
                            if isinstance(synthesized_model, Trade):
                                synthesized_model.quantity = total_qty
                            elif isinstance(synthesized_model, Candlestick):
                                synthesized_model.volume = total_qty
                                synthesized_model.high = max_price
                                synthesized_model.low = min_price
                            
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
                            print(f"Error processing {message_id}: {e}")

        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            print(f"Stream Consumer Error: {e}")
            raise e

    def _parse_message(self, stream_key: str, data: dict) -> Union[Candlestick, Trade, None]:
        """Helper to parse raw stream data into models."""
        try:
            # Check if JSON payload exists (from Rust XADD)
            if 'json' in data:
                payload = data['json']
                # Determine type by keys in JSON
                import json
                parsed = json.loads(payload)
                if 'open' in parsed:
                    return Candlestick.model_validate_json(payload)
                else:
                    return Trade.model_validate_json(payload)
            
            # Handle raw fields (Manual XADD)
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
            print(f"Parse error: {e}")
            return None

        def stop(self):

            self.running = False

            self.redis_client.close()

            print("DataConsumer stopped.")

    