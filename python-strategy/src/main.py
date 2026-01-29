import os
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.consumer import DataConsumer
from src.core.engine import StrategyEngine
from src.strategies.example import RandomStrategy
from src.core.db import SessionLocal
from src.core.clock import RealtimeClock

def main():
    print("Starting FluxTrade Strategy Service...")
    
    # 0. Init DB Session
    db_session = SessionLocal()

    # 1. Initialize Engine
    clock = RealtimeClock()
    engine = StrategyEngine(db_session=db_session, clock=clock)
    
    # Run Startup Checks (System State & Heartbeat)
    engine.startup()
    
    # 2. Register Strategies
    # Use 'strategy_1' which exists in seed data
    strategy_1 = RandomStrategy(strategy_id="strategy_1", product_id="BINANCE:BTCUSDT-PERP")
    engine.add_strategy(strategy_1)
    
    # 3. Initialize Data Consumer (Redis Streams)
    # Subscribe to per-timeframe streams derived from strategy requirements
    channels = engine.build_stream_channels()
    consumer = DataConsumer(channels=channels, on_message_callback=engine.on_market_data)
    
    # 4. Start
    try:
        consumer.start()
    except KeyboardInterrupt:
        print("Service stopping...")
    finally:
        db_session.close()

if __name__ == "__main__":
    main()