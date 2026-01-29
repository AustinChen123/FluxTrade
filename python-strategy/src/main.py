import logging
import os
import signal
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.consumer import DataConsumer
from src.core.engine import StrategyEngine
from src.strategies.example import RandomStrategy
from src.core.db import SessionLocal
from src.core.clock import RealtimeClock

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting FluxTrade Strategy Service...")

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
    channels = engine.build_stream_channels()
    consumer = DataConsumer(channels=channels, on_message_callback=engine.on_market_data)

    # 4. Signal handlers for graceful shutdown
    def handle_shutdown(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info("Received %s, initiating shutdown...", sig_name)
        consumer.stop()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    # 5. Start
    try:
        consumer.start()
    finally:
        engine.shutdown()
        db_session.close()
        logger.info("FluxTrade Strategy Service stopped.")


if __name__ == "__main__":
    main()
