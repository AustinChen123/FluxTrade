"""Prometheus metrics for FluxTrade Strategy Service.

Module-level metric definitions with a toggle to avoid exposing an HTTP
server during backtests.  Call ``configure_metrics(enabled, port)`` once
at startup; all instrument call-sites use the module-level objects
directly and are safe to call even when metrics are disabled (Prometheus
client simply discards them).
"""

import logging
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = logging.getLogger(__name__)

# ── Metric definitions ────────────────────────────────────────────

SIGNALS_TOTAL = Counter(
    "fluxtrade_signals_total",
    "Total signals emitted by strategies",
    ["strategy_id", "signal_type", "risk_status"],
)

ORDERS_TOTAL = Counter(
    "fluxtrade_orders_total",
    "Total orders submitted to exchange adapter",
    ["order_type", "status"],
)

EXECUTION_LATENCY = Histogram(
    "fluxtrade_execution_latency_seconds",
    "Latency of adapter.place_order() calls",
)

BALANCE_USDT = Gauge(
    "fluxtrade_balance_usdt",
    "Current USDT balance reported by account service",
)

CONSUMER_LAG_MS = Gauge(
    "fluxtrade_consumer_lag_ms",
    "Redis stream consumer lag in milliseconds",
    ["stream_key"],
)

ACTIVE_STRATEGIES = Gauge(
    "fluxtrade_active_strategies",
    "Number of currently active strategies",
)

# ── Configuration ─────────────────────────────────────────────────

_enabled = False


def configure_metrics(enabled: bool = True, port: int = 9090) -> None:
    """Start the Prometheus HTTP exporter.

    Safe to call multiple times — only the first ``enabled=True`` call
    actually starts the server.
    """
    global _enabled
    if not enabled:
        logger.info("Metrics disabled — Prometheus HTTP server not started")
        return
    if _enabled:
        return
    start_http_server(port)
    _enabled = True
    logger.info("Prometheus metrics server started on :%d", port)


def is_enabled() -> bool:
    """Return whether the metrics HTTP server is running."""
    return _enabled
