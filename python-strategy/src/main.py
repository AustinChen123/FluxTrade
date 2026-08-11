import logging
import os
import signal
import sys
from contextlib import contextmanager

import structlog

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.core.consumer import (
    DEFAULT_OWNERSHIP_LEASE_MS,
    DEFAULT_PENDING_CLAIM_IDLE_MS,
    DataConsumer,
)
from src.core.engine import StrategyEngine
from src.core.db import SessionLocal
from src.core.clock import RealtimeClock
from src.core.metrics import configure_metrics
from src.core.product_registry import to_exchange_name
from src.core.adapters.backpack_live_config import (
    build_backpack_live_adapter_config,
)
from src.core.adapters.binance_live_config import build_binance_live_adapter_config
from src.core.adapters.bybit_live_config import build_bybit_live_adapter_config
from src.core.adapters.ccxt_live_credentials import build_ccxt_live_credentials
from src.core.adapters.okx_live_config import build_okx_live_adapter_config
from src.core.adapters.rithmic_live_config import (
    build_rithmic_live_adapter_config,
    validate_rithmic_recovery_identity,
)
from src.core.adapters.rithmic_runtime_composition import (
    build_rithmic_runtime_owners,
    prepare_rithmic_runtime_bootstrap,
)


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
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"{name} must be a boolean")


def _env_nonnegative_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value < 0:
        raise ValueError(f"{name} must be non-negative")
    return value


def _env_positive_int(name: str, default: int) -> int:
    value = int(os.getenv(name, str(default)))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


def _env_csv(name: str, default: list[str] | None = None) -> list[str]:
    value = os.getenv(name)
    if value is None:
        return list(default or [])
    return [item.strip() for item in value.split(",") if item.strip()]


def _required_env(name: str) -> str:
    value = (os.getenv(name) or "").strip()
    if not value:
        raise ValueError(f"{name} must be set explicitly")
    return value


def _required_env_flag(name: str) -> bool:
    _required_env(name)
    return _env_flag(name)


def _adapter_config_from_env() -> dict:
    if os.getenv("FLUXTRADE_ENVIRONMENT") == "live":
        mode = _required_env("ADAPTER_MODE")
    else:
        mode = os.getenv("ADAPTER_MODE") or os.getenv("EXCHANGE_MODE") or "simulated"
    mode = mode.strip().lower()
    if mode == "simulated":
        return {"mode": "simulated"}
    if mode != "live":
        raise ValueError(f"unsupported_adapter_mode: mode={mode}")

    exchange = _required_env("EXCHANGE_ID").lower()
    product_ids = _env_csv("INSTRUMENT_PRODUCT_IDS")
    if not product_ids:
        raise ValueError("INSTRUMENT_PRODUCT_IDS must be set explicitly")
    mismatched_products = [
        product_id
        for product_id in product_ids
        if to_exchange_name(product_id) != exchange
    ]
    if mismatched_products:
        raise ValueError(
            f"INSTRUMENT_PRODUCT_IDS must use {exchange.upper()} venue: "
            f"{', '.join(mismatched_products)}"
        )
    if exchange == "backpack":
        return build_backpack_live_adapter_config(
            product_ids=product_ids,
            environ=os.environ,
        )
    if exchange == "binance":
        return build_binance_live_adapter_config(
            product_ids=product_ids,
            environ=os.environ,
        )
    if exchange == "bybit":
        return build_bybit_live_adapter_config(
            product_ids=product_ids,
            environ=os.environ,
        )
    if exchange == "okx":
        return build_okx_live_adapter_config(
            product_ids=product_ids,
            environ=os.environ,
        )
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
        "exchange": exchange,
        "enable_ws": _env_flag("EXCHANGE_ENABLE_WS", False),
        "instrument_product_ids": product_ids,
        "account_initialization": account_initialization,
    }
    if exchange != "rithmic":
        config.update(build_ccxt_live_credentials(os.environ))
        return config

    config.update(
        build_rithmic_live_adapter_config(
            product_ids=product_ids,
            environ=os.environ,
        )
    )
    return config


def _validate_runtime_config(
    adapter_config: dict, *, audit_external_orders: bool
) -> None:
    if adapter_config.get("mode") == "live" and not audit_external_orders:
        raise ValueError(
            "live_adapter_requires_audit_external_orders: "
            "set AUDIT_EXTERNAL_ORDERS=true for live trading"
        )
    validate_rithmic_recovery_identity(adapter_config)


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
    audit_external_orders = (
        _required_env_flag("AUDIT_EXTERNAL_ORDERS")
        if os.getenv("FLUXTRADE_ENVIRONMENT") == "live"
        else _env_flag("AUDIT_EXTERNAL_ORDERS")
    )
    _validate_runtime_config(
        adapter_config,
        audit_external_orders=audit_external_orders,
    )
    runtime_bootstrap_factory = None
    runtime_capabilities_factory = None
    if adapter_config.get("exchange") == "rithmic":
        runtime_bootstrap_factory = prepare_rithmic_runtime_bootstrap
        runtime_capabilities_factory = build_rithmic_runtime_owners

    consumer = DataConsumer(
        channels=[],
        on_message_callback=lambda _data: None,
        pending_claim_idle_ms=_env_nonnegative_int(
            "MARKET_PENDING_CLAIM_IDLE_MS",
            DEFAULT_PENDING_CLAIM_IDLE_MS,
        ),
        ownership_lease_ms=_env_positive_int(
            "MARKET_CONSUMER_LEASE_MS",
            DEFAULT_OWNERSHIP_LEASE_MS,
        ),
    )

    def handle_shutdown(signum, frame):
        sig_name = signal.Signals(signum).name
        logger.info("Received %s, initiating shutdown...", sig_name)
        consumer.request_stop()

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    db_session = None
    engine = None
    clean_exit = False
    try:
        consumer.acquire_service_ownership()
        metrics_enabled = os.getenv("METRICS_ENABLED", "true").lower() == "true"
        metrics_port = int(os.getenv("METRICS_PORT", "9090"))
        configure_metrics(enabled=metrics_enabled, port=metrics_port)

        db_session = SessionLocal()
        clock = RealtimeClock()
        engine = StrategyEngine(
            db_session=db_session,
            clock=clock,
            adapter_config=adapter_config,
            db_session_factory=_session_scope,
            audit_external_orders=audit_external_orders,
            leadership_guard=consumer.assert_service_ownership,
            runtime_bootstrap_factory=runtime_bootstrap_factory,
            runtime_capabilities_factory=runtime_capabilities_factory,
        )
        engine.startup()
        consumer.configure_callbacks(
            on_message_callback=engine.on_market_data,
            channel_provider=engine.build_stream_channels,
            pending_replay_callback=engine.replay_pending_market_data,
        )
        consumer.start()
        clean_exit = True
    finally:
        try:
            if engine is not None:
                engine.shutdown(clean_exit=clean_exit)
        finally:
            try:
                if db_session is not None:
                    db_session.close()
            finally:
                consumer.stop()
                logger.info("FluxTrade Strategy Service stopped.")


if __name__ == "__main__":
    main()
