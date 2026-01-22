import os
import time
import json
import random
import redis
from decimal import Decimal
from dotenv import load_dotenv
from src.core.models import Candlestick

# Load env from root
load_dotenv(os.path.join(os.path.dirname(__file__), '../../../.env'))

REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

class MockRedisPublisher:
    def __init__(self):
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.running = True
        print(f"Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")

    def generate_fake_candle(self, current_price: Decimal, product_id: str, timestamp: int) -> Candlestick:
        # Random walk
        change = Decimal(str(random.uniform(-0.005, 0.005)))  # +/- 0.5%
        open_price = current_price
        close_price = open_price * (1 + change)
        high_price = max(open_price, close_price) * Decimal("1.001")
        low_price = min(open_price, close_price) * Decimal("0.999")
        volume = Decimal(str(random.uniform(1.0, 10.0)))

        return Candlestick(
            product_id=product_id,
            timeframe="1m",
            timestamp=timestamp,
            open=round(open_price, 2),
            high=round(high_price, 2),
            low=round(low_price, 2),
            close=round(close_price, 2),
            volume=round(volume, 4)
        )

    def start(self):
        product_id = "BINANCE:BTCUSDT-PERP"
        current_price = Decimal("50000.00")
        
        print(f"Starting mock data stream for {product_id}...")
        try:
            while self.running:
                timestamp = int(time.time() * 1000)
                candle = self.generate_fake_candle(current_price, product_id, timestamp)
                current_price = candle.close

                # Channel format: market_data.<exchange>.<symbol>.<timeframe>
                # Extract exchange and symbol from product_id (e.g. BINANCE:BTCUSDT-PERP)
                exchange, symbol = product_id.split(':')
                channel = f"market_data.{exchange}.{symbol}.1m"
                
                # Publish using Pydantic's model_dump_json which handles Decimal
                payload = candle.model_dump_json()
                self.redis_client.publish(channel, payload)
                
                # Also SET a key for the latest status
                self.redis_client.set(f"latest_candle:{product_id}", payload)
                
                print(f"Published to {channel}: Close={candle.close}")
                time.sleep(1) # Publish every second
        except KeyboardInterrupt:
            print("Stopping mock publisher...")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    publisher = MockRedisPublisher()
    publisher.start()
