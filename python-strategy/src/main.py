import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.consumer import DataConsumer
from src.core.engine import StrategyEngine
from src.strategies.example import RandomStrategy

def main():
    print("Starting FluxTrade Strategy Service...")
    
    # 1. Initialize Engine
    engine = StrategyEngine()
    
    # 2. Register Strategies
    # Note: In a real app, this might load from DB or Config
    strategy_1 = RandomStrategy(strategy_id="strat_demo_01", product_id="BINANCE:BTCUSDT-PERP")
    engine.add_strategy(strategy_1)
    
    # 3. Initialize Data Consumer
    # Subscribe to all market data
    channels = ["market_data.*"]
    consumer = DataConsumer(channels=channels, on_message_callback=engine.on_market_data)
    
    # 4. Start
    try:
        consumer.start()
    except KeyboardInterrupt:
        print("Service stopping...")

if __name__ == "__main__":
    main()
