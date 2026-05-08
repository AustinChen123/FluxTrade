"""Tests for src/core/command_router.py."""

from __future__ import annotations

from unittest.mock import MagicMock

from src.core.command_router import CommandResult, CommandRouter
from src.core.health_monitor import HealthMonitor
from src.core.models import Candlestick, Signal, SignalType
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy, StrategyRequirements


class DummyStrategy(BaseStrategy):
    def __init__(
        self,
        strategy_id: str,
        product_id: str = "BINANCE:BTCUSDT-PERP",
        timeframe: str = "1m",
    ):
        super().__init__(strategy_id, product_id)
        self._timeframe = timeframe

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, self._timeframe, 10)

    def on_candle(self, candle: Candlestick) -> Signal:
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=self._timeframe,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )


def test_command_result_dataclass() -> None:
    result = CommandResult(True, "ok", {"value": 1})

    assert result.success is True
    assert result.message == "ok"
    assert result.data == {"value": 1}


def test_start_delegates_to_state_manager() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())

    result = router.handle({"command": "START", "params": {"id": "s1"}})

    assert result.success is True
    router.state_manager.transition_to_running.assert_called_once_with("s1")


def test_stop_delegates_to_state_manager() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())

    result = router.handle({"command": "STOP", "strategy_id": "s1"})

    assert result.success is True
    router.state_manager.transition_to_stopped.assert_called_once_with("s1")


def test_reload_returns_placeholder_without_registry_reload() -> None:
    registry = StrategyRegistry()
    router = CommandRouter(registry, MagicMock())

    result = router.handle({"command": "RELOAD", "params": {"strategy_id": "s1"}})

    assert result.success is True
    assert "later implementation" in result.message


def test_list_returns_active_strategy_metadata() -> None:
    registry = StrategyRegistry()
    registry.register(DummyStrategy("s1", timeframe="1m"))
    registry.register(DummyStrategy("s2", timeframe="5m"))
    router = CommandRouter(registry, MagicMock())

    result = router.handle({"command": "LIST"})

    assert result == CommandResult(
        True,
        "Listed active strategies",
        {
            "strategies": [
                {
                    "strategy_id": "s1",
                    "product_id": "BINANCE:BTCUSDT-PERP",
                    "timeframe": "1m",
                },
                {
                    "strategy_id": "s2",
                    "product_id": "BINANCE:BTCUSDT-PERP",
                    "timeframe": "5m",
                },
            ]
        },
    )


def test_health_check_returns_per_strategy_status(
    monkeypatch,
) -> None:
    now = 100.0
    monkeypatch.setattr("src.core.health_monitor.time.time", lambda: now)
    registry = StrategyRegistry()
    registry.register(DummyStrategy("s1"))
    registry.register(DummyStrategy("s2"))
    monitor = HealthMonitor(registry)
    monitor.update_heartbeat("s1")
    router = CommandRouter(registry, MagicMock(), health_monitor=monitor)

    result = router.handle({"command": "HEALTH_CHECK"})

    assert result == CommandResult(
        True,
        "Health check complete",
        {"healthy": {"s1": True, "s2": False}},
    )


def test_unknown_command_returns_failure() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())

    result = router.handle({"command": "NOPE"})

    assert result.success is False
    assert result.message == "Unknown command: NOPE"


def test_malformed_command_returns_failure() -> None:
    router = CommandRouter(StrategyRegistry(), MagicMock())

    assert router.handle({}).success is False
    assert router.handle({"command": "START"}).success is False
    assert router.handle({"command": "LIST", "params": "bad"}).success is False
