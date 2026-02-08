import logging
import os
import signal
import sys

import structlog

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.consumer import DataConsumer
from src.core.engine import StrategyEngine
from src.strategies.example import RandomStrategy
from src.core.db import SessionLocal
from src.core.clock import RealtimeClock
from src.core.metrics import configure_metrics


def _setup_logging() -> None:
    """Configure structlog to wrap stdlib logging.

    - ``LOG_FORMAT=json`` (default in Docker): machine-readable JSON lines.
    - ``LOG_FORMAT=console``: colored human-friendly output for local dev.

    Existing ``logger.info("msg %s", arg)`` calls keep working because
    ``PositionalArgumentsFormatter`` is in the processor chain.
    ``merge_contextvars`` automatically attaches ``trace_id`` when bound
    via ``structlog.contextvars.bind_contextvars()``.
    """
    log_format = os.getenv("LOG_FORMAT", "console").lower()

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    if log_format == "json":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer()

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            renderer,
        ],
        foreign_pre_chain=shared_processors,
    )

    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root.addHandler(handler)
    root.setLevel(logging.INFO)


_setup_logging()
logger = logging.getLogger(__name__)


def main():
    logger.info("Starting FluxTrade Strategy Service...")

    # 0. Metrics
    metrics_enabled = os.getenv("METRICS_ENABLED", "true").lower() == "true"
    metrics_port = int(os.getenv("METRICS_PORT", "9090"))
    configure_metrics(enabled=metrics_enabled, port=metrics_port)

    # 1. Init DB Session
    db_session = SessionLocal()

    # 2. Initialize Engine
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
