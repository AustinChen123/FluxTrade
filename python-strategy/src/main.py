import logging
import os
import signal
import sys
from contextlib import contextmanager

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


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _env_csv(name: str, default: list[str] | None = None) -> list[str]:
    value = os.getenv(name)
    if value is None:
        return list(default or [])
    return [item.strip() for item in value.split(",") if item.strip()]


def _adapter_config_from_env() -> dict:
    mode = os.getenv("ADAPTER_MODE") or os.getenv("EXCHANGE_MODE") or "simulated"
    mode = mode.lower()
    if mode == "simulated":
        return {"mode": "simulated"}
    if mode != "live":
        raise ValueError(f"unsupported_adapter_mode: mode={mode}")

    product_ids = _env_csv("INSTRUMENT_PRODUCT_IDS", ["BINANCE:BTCUSDT-PERP"])
    account_initialization = {
        "product_ids": product_ids,
        "position_mode": os.getenv("ACCOUNT_POSITION_MODE", "one_way"),
    }
    leverage = os.getenv("ACCOUNT_LEVERAGE")
    if leverage:
        account_initialization["leverage"] = leverage
    margin_mode = os.getenv("ACCOUNT_MARGIN_MODE")
    if margin_mode:
        account_initialization["margin_mode"] = margin_mode

    config = {
        "mode": "live",
        "exchange": os.getenv("EXCHANGE_ID", "binance"),
        "api_key": os.getenv("EXCHANGE_API_KEY"),
        "secret": os.getenv("EXCHANGE_SECRET"),
        "testnet": _env_flag("EXCHANGE_TESTNET", True),
        "enable_ws": _env_flag("EXCHANGE_ENABLE_WS", False),
        "instrument_product_ids": product_ids,
        "account_initialization": account_initialization,
    }
    rithmic_profile = os.getenv("RITHMIC_RECOVERY_PROFILE")
    if rithmic_profile:
        config["rithmic_recovery_profile"] = rithmic_profile
        account_id = os.getenv("RITHMIC_ACCOUNT_ID")
        if account_id:
            config["rithmic_recovery_account_id"] = account_id
    return config


def _validate_runtime_config(adapter_config: dict, *, audit_external_orders: bool) -> None:
    if adapter_config.get("mode") == "live" and not audit_external_orders:
        raise ValueError(
            "live_adapter_requires_audit_external_orders: "
            "set AUDIT_EXTERNAL_ORDERS=true for live trading"
        )


@contextmanager
def _session_scope():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def main():
    logger.info("Starting FluxTrade Strategy Service...")
    adapter_config = _adapter_config_from_env()
    audit_external_orders = _env_flag("AUDIT_EXTERNAL_ORDERS")
    _validate_runtime_config(
        adapter_config,
        audit_external_orders=audit_external_orders,
    )

    # 0. Metrics
    metrics_enabled = os.getenv("METRICS_ENABLED", "true").lower() == "true"
    metrics_port = int(os.getenv("METRICS_PORT", "9090"))
    configure_metrics(enabled=metrics_enabled, port=metrics_port)

    # 1. Init DB Session
    db_session = SessionLocal()

    # 2. Initialize Engine
    clock = RealtimeClock()
    engine = StrategyEngine(
        db_session=db_session,
        clock=clock,
        adapter_config=adapter_config,
        db_session_factory=_session_scope,
        audit_external_orders=audit_external_orders,
    )

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
