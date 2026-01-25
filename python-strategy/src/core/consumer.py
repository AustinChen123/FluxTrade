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

                    # 2. Conflation Logic (PY-601)
                    # Check the timestamp of the *latest* message in this batch
                    # Note: ID format is "timestamp-sequence"
                    last_msg_id, _ = messages[-1]
                    last_msg_ts = int(last_msg_id.split('-')[0])
                    server_time_ms = int(self.redis_client.time()[0] * 1000) + int(self.redis_client.time()[1] / 1000)
                    
                    lag = server_time_ms - last_msg_ts
                    
                    if lag > 100:
                        # Lag > 100ms: DROP backlog
                        print(f"⚠️  LAG DETECTED ({lag}ms > 100ms) on {stream_key}. Dropping backlog & Jumping to $. {len(messages)} msgs skipped.")
                        
                        # ACK all messages in this batch (so they are not pending)
                        msg_ids = [mid for mid, _ in messages]
                        if msg_ids:
                            self.redis_client.xack(stream_key, self.group_name, *msg_ids)
                            
                        # Reset ID to '$' (latest) to skip any other pending backlog on server
                        # Note: XREADGROUP with '>' already gives new, but if we are behind, 
                        # we might want to skip everything currently in the stream.
                        # However, xgroup_setid affects *future* reads. 
                        # We just ACK'd the current batch. The 'backlog' might be larger.
                        # To truly 'jump to $', we set the group id.
                        self.redis_client.xgroup_setid(stream_key, self.group_name, '$')
                        continue

                    # 3. Process Messages
                    for message_id, data in messages:
                        try:
                            # Map Stream Data to Model
                            # Expected keys: 'type' (trade/candle), 'data' (json) or fields
                            # Assuming the producer sends JSON in 'data' field or fields map
                            # For PY-602 compatibility, we might receive raw fields. 
                            # But legacy adapter might send 'data'. 
                            # Let's handle both or assume standard format.
                            # Instructions say: "Stream Key: stream:market:{exchange}:{symbol}"
                            
                            # Auto-detect content
                            payload = None
                            if 'data' in data:
                                payload = data['data']
                                if stream_key.endswith('.trades') or 'trade' in stream_key:
                                    model = Trade.model_validate_json(payload)
                                else:
                                    model = Candlestick.model_validate_json(payload)
                            else:
                                # Handle raw fields (PY-602 XADD format)
                                # "strategy_id", "product_id", "side", "price", "quantity", "timestamp"
                                if 'price' in data and 'quantity' in data:
                                     # This looks like a Trade
                                     model = Trade(
                                         id=data.get('trade_id', message_id),
                                         order_id=data.get('order_id', ''),
                                         exchange_trade_id=message_id,
                                         product_id=data.get('product_id', 'unknown'),
                                         side=data.get('side', 'BUY'),
                                         price=Decimal(data['price']),
                                         quantity=Decimal(data['quantity']),
                                         fee=Decimal("0"),
                                         fee_asset="USDT",
                                         timestamp=int(data.get('timestamp', 0))
                                     )
                                else:
                                    # Skip unknown format
                                    continue

                            self.callback(model)
                            
                            # ACK processed message
                            self.redis_client.xack(stream_key, self.group_name, message_id)
                            
                        except Exception as e:
                            print(f"Error processing {message_id} on {stream_key}: {e}")

        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            print(f"Stream Consumer Error: {e}")
            # Crash on critical error (Watchdog will restart)
            raise e