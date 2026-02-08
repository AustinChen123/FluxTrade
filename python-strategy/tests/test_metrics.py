"""Tests for the Prometheus metrics module."""

import pytest
from prometheus_client import Counter, Gauge, Histogram

from src.core.metrics import (
    SIGNALS_TOTAL,
    ORDERS_TOTAL,
    EXECUTION_LATENCY,
    BALANCE_USDT,
    CONSUMER_LAG_MS,
    ACTIVE_STRATEGIES,
    configure_metrics,
    is_enabled,
)


class TestMetricObjects:
    """Verify all 6 metric definitions exist with correct types."""

    def test_signals_total_is_counter(self):
        assert isinstance(SIGNALS_TOTAL, Counter)

    def test_orders_total_is_counter(self):
        assert isinstance(ORDERS_TOTAL, Counter)

    def test_execution_latency_is_histogram(self):
        assert isinstance(EXECUTION_LATENCY, Histogram)

    def test_balance_usdt_is_gauge(self):
        assert isinstance(BALANCE_USDT, Gauge)

    def test_consumer_lag_ms_is_gauge(self):
        assert isinstance(CONSUMER_LAG_MS, Gauge)

    def test_active_strategies_is_gauge(self):
        assert isinstance(ACTIVE_STRATEGIES, Gauge)


class TestConfigureMetrics:
    """Test configure_metrics() enabled/disabled paths."""

    def test_disabled_does_not_start_server(self):
        configure_metrics(enabled=False)
        # No exception — HTTP server NOT started
        assert not is_enabled()

    def test_is_enabled_returns_bool(self):
        result = is_enabled()
        assert isinstance(result, bool)


class TestMetricsIncrement:
    """Verify metric operations don't raise exceptions."""

    def test_signals_total_inc(self):
        SIGNALS_TOTAL.labels(
            strategy_id="test_strat",
            signal_type="LONG",
            risk_status="PASS",
        ).inc()

    def test_orders_total_inc(self):
        ORDERS_TOTAL.labels(order_type="market", status="placed").inc()

    def test_execution_latency_observe(self):
        EXECUTION_LATENCY.observe(0.042)

    def test_balance_usdt_set(self):
        BALANCE_USDT.set(10000.50)

    def test_consumer_lag_ms_set(self):
        CONSUMER_LAG_MS.labels(stream_key="stream:market:binance:btcusdt:1m").set(15)

    def test_active_strategies_set(self):
        ACTIVE_STRATEGIES.set(3)
