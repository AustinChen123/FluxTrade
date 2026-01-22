import os
import json
import redis
from typing import Callable, List
from dotenv import load_dotenv
from src.core.models import Candlestick

# Load env
load_dotenv(os.path.join(os.path.dirname(__file__), '../../../.env'))

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

class DataConsumer:
    def __init__(self, channels: List[str], on_message_callback: Callable[[Candlestick], None]):
        """
        :param channels: List of Redis channels to subscribe to (e.g., ['market_data.*'])
        :param on_message_callback: Function to call when a valid Candlestick is received
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
                    try:
                        data_str = message['data']
                        # Deserialize and validate
                        candle = Candlestick.model_validate_json(data_str)
                        self.callback(candle)
                    except Exception as e:
                        # Log error but don't crash
                        print(f"Error processing message: {e}. Data: {message}")
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            print(f"Connection error: {e}")

    def stop(self):
        self.running = False
        self.pubsub.close()
        self.redis_client.close()
        print("DataConsumer stopped.")
