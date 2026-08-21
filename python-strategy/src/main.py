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
from src.core.strategy_loader import StrategyLoader
from src.core.db import SessionLocal
from src.core.clock import RealtimeClock
from src.core.metrics import configure_metrics
from src.core.product_registry import to_exchange_name
from src.core.adapters import create_adapter
from src.core.adapter_runtime_composition import (
    build_live_adapter_config,
    runtime_factories_for_config,
    validate_runtime_config as _validate_runtime_config,
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

_PRODUCTION_STRATEGY_ARTIFACTS_PATH = "/app/strategy_artifacts"
_LOCAL_STRATEGIES_PATH = "/app/strategies_hot"


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


def _strategy_artifact_loader_from_env():
    if os.getenv("FLUXTRADE_ENVIRONMENT") != "live":
        path = os.getenv("HOT_STRATEGIES_PATH", _LOCAL_STRATEGIES_PATH)
        return lambda: StrategyLoader.scan_directory(path)

    configured_path = os.getenv("STRATEGY_ARTIFACTS_PATH")
    if configured_path not in {None, _PRODUCTION_STRATEGY_ARTIFACTS_PATH}:
        raise ValueError(
            "STRATEGY_ARTIFACTS_PATH must be /app/strategy_artifacts in live"
        )
    break_glass_path = None
    if _env_flag("STRATEGY_BREAK_GLASS_ENABLED", False):
        break_glass_path = (os.getenv("STRATEGY_BREAK_GLASS_PATH") or "").strip()
        if not break_glass_path:
            raise ValueError(
                "STRATEGY_BREAK_GLASS_PATH must be set when break-glass is enabled"
            )
        logger.warning("Strategy break-glass artifact source enabled")
    return lambda: StrategyLoader.scan_production_sources(
        _PRODUCTION_STRATEGY_ARTIFACTS_PATH,
        break_glass_path=break_glass_path,
    )


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
    return build_live_adapter_config(
        exchange=exchange,
        product_ids=product_ids,
        environ=os.environ,
        read_enable_ws=lambda: _env_flag("EXCHANGE_ENABLE_WS", False),
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
    audit_external_orders = (
        _required_env_flag("AUDIT_EXTERNAL_ORDERS")
        if os.getenv("FLUXTRADE_ENVIRONMENT") == "live"
        else _env_flag("AUDIT_EXTERNAL_ORDERS")
    )
    _validate_runtime_config(
        adapter_config,
        audit_external_orders=audit_external_orders,
    )
    strategy_artifact_loader = _strategy_artifact_loader_from_env()
    runtime_bootstrap_factory, runtime_capabilities_factory = (
        runtime_factories_for_config(adapter_config)
    )

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
    adapter = None
    clean_exit = False
    try:
        consumer.acquire_service_ownership()
        metrics_enabled = os.getenv("METRICS_ENABLED", "true").lower() == "true"
        metrics_port = int(os.getenv("METRICS_PORT", "9090"))
        configure_metrics(enabled=metrics_enabled, port=metrics_port)

        db_session = SessionLocal()
        clock = RealtimeClock()
        try:
            adapter = create_adapter(
                adapter_config,
                operation_guard=consumer.assert_service_ownership,
            )
        except Exception as exc:
            logger.critical(
                "Failed to init adapter: %s. NOT falling back silently.",
                exc,
            )
            raise
        try:
            engine = StrategyEngine(
                db_session=db_session,
                clock=clock,
                adapter_config=adapter_config,
                adapter=adapter,
                db_session_factory=_session_scope,
                audit_external_orders=audit_external_orders,
                leadership_guard=consumer.assert_service_ownership,
                runtime_bootstrap_factory=runtime_bootstrap_factory,
                runtime_capabilities_factory=runtime_capabilities_factory,
                strategy_artifact_loader=strategy_artifact_loader,
            )
        except Exception:
            close_adapter = getattr(adapter, "close", None)
            if callable(close_adapter):
                try:
                    close_adapter()
                except Exception as close_error:
                    logger.warning(
                        "Failed to close unowned adapter after Engine construction failure: %s",
                        type(close_error).__name__,
                    )
            adapter = None
            raise
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
            elif adapter is not None:
                close_adapter = getattr(adapter, "close", None)
                if callable(close_adapter):
                    close_adapter()
        finally:
            try:
                if db_session is not None:
                    db_session.close()
            finally:
                consumer.stop()
                logger.info("FluxTrade Strategy Service stopped.")


if __name__ == "__main__":
    main()
