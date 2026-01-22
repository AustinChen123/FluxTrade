import os
import redis
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
        :param channels: List of Redis channels to subscribe to (e.g., ['market_data.*'])
        :param on_message_callback: Function to call when a valid data item is received
        """
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.pubsub = self.redis_client.pubsub()
        self.channels = channels
        self.callback = on_message_callback
        self.running = False

    def start(self):
        self.running = True
        # Subscribe to patterns
        for channel in self.channels:
            self.pubsub.psubscribe(channel)
        
        print(f"DataConsumer started. Listening on: {self.channels}")

        try:
            for message in self.pubsub.listen():
                if not self.running:
                    break
                
                if message['type'] == 'pmessage':
                    channel = message['channel']
                    data_str = message['data']
                    
                    try:
                        # Determine model based on channel suffix
                        # Format assumption: market_data.<exchange>.<symbol>.<type>
                        # e.g. market_data.BINANCE.BTCUSDT-PERP.trades
                        # e.g. market_data.BINANCE.BTCUSDT-PERP.1m
                        
                        if channel.endswith('.trades'):
                            data = Trade.model_validate_json(data_str)
                        else:
                            # Assume it's a timeframe (candlestick)
                            data = Candlestick.model_validate_json(data_str)
                            
                        self.callback(data)
                    except Exception as e:
                        # Log error but don't crash
                        print(f"Error processing message from {channel}: {e}. Data payload: {data_str[:50]}...")
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            print(f"Connection error: {e}")

    def stop(self):
        self.running = False
        self.pubsub.close()
        self.redis_client.close()
        print("DataConsumer stopped.")