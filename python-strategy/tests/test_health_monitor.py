"""Tests for src/core/health_monitor.py."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from src.core.health_monitor import HealthMonitor
from src.core.models import Candlestick, Signal, SignalType
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy, StrategyRequirements


class DummyStrategy(BaseStrategy):
    def __init__(self, strategy_id: str):
        super().__init__(strategy_id, "BINANCE:BTCUSDT-PERP")

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 10)

    def on_candle(self, candle: Candlestick) -> Signal:
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="1m",
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )


@pytest.fixture()
def registry() -> StrategyRegistry:
    return StrategyRegistry()


def test_update_heartbeat_records_timestamp(
    monkeypatch: pytest.MonkeyPatch,
    registry: StrategyRegistry,
) -> None:
    monkeypatch.setattr("src.core.health_monitor.time.time", lambda: 100.0)
    monitor = HealthMonitor(registry)

    monitor.update_heartbeat("s1")

    assert monitor.is_healthy("s1") is True


def test_is_healthy_returns_false_after_timeout(
    monkeypatch: pytest.MonkeyPatch,
    registry: StrategyRegistry,
) -> None:
    now = 100.0
    monkeypatch.setattr("src.core.health_monitor.time.time", lambda: now)
    monitor = HealthMonitor(registry, timeout_threshold=5.0)
    monitor.update_heartbeat("s1")

    now = 106.0

    assert monitor.is_healthy("s1") is False


def test_is_healthy_returns_false_for_unknown_strategy(
    registry: StrategyRegistry,
) -> None:
    monitor = HealthMonitor(registry)

    assert monitor.is_healthy("missing") is False


def test_get_uptime_uses_first_heartbeat(
    monkeypatch: pytest.MonkeyPatch,
    registry: StrategyRegistry,
) -> None:
    now = 100.0
    monkeypatch.setattr("src.core.health_monitor.time.time", lambda: now)
    monitor = HealthMonitor(registry)
    monitor.update_heartbeat("s1")

    now = 103.5
    monitor.update_heartbeat("s1")

    assert monitor.get_uptime("s1") == 3.5


def test_get_uptime_returns_zero_for_unknown_strategy(
    registry: StrategyRegistry,
) -> None:
    monitor = HealthMonitor(registry)

    assert monitor.get_uptime("missing") == 0.0


def test_expose_prometheus_metrics_lists_registered_strategies(
    monkeypatch: pytest.MonkeyPatch,
    registry: StrategyRegistry,
) -> None:
    now = 100.0
    monkeypatch.setattr("src.core.health_monitor.time.time", lambda: now)
    registry.register(DummyStrategy("s1"))
    registry.register(DummyStrategy("s2"))
    monitor = HealthMonitor(registry)
    monitor.update_heartbeat("s1")

    output = monitor.expose_prometheus_metrics()

    assert 'fluxtrade_strategy_uptime_seconds{strategy_id="s1"} 0.000000' in output
    assert 'fluxtrade_strategy_healthy{strategy_id="s1"} 1' in output
    assert 'fluxtrade_strategy_uptime_seconds{strategy_id="s2"} 0.000000' in output
    assert 'fluxtrade_strategy_healthy{strategy_id="s2"} 0' in output


def test_multiple_heartbeat_updates_keep_strategy_healthy(
    monkeypatch: pytest.MonkeyPatch,
    registry: StrategyRegistry,
) -> None:
    now = 100.0
    monkeypatch.setattr("src.core.health_monitor.time.time", lambda: now)
    monitor = HealthMonitor(registry, timeout_threshold=5.0)
    monitor.update_heartbeat("s1")

    now = 104.0
    monitor.update_heartbeat("s1")
    now = 108.0

    assert monitor.is_healthy("s1") is True


def test_update_heartbeat_writes_to_redis_when_provided(
    registry: StrategyRegistry,
) -> None:
    redis_client = MagicMock()
    monitor = HealthMonitor(
        registry,
        timeout_threshold=5.0,
        redis_client=redis_client,
    )

    monitor.update_heartbeat("s1")

    redis_client.setex.assert_called_once_with("heartbeat:strategy:s1", 5, "1")
