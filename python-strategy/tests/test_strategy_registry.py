"""Tests for src/core/strategy_registry.py."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

import pytest

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


def test_register_and_get_strategy() -> None:
    registry = StrategyRegistry()
    strategy = DummyStrategy("s1")

    registry.register(strategy)

    assert registry.get("s1") is strategy


def test_unregister_removes_strategy() -> None:
    registry = StrategyRegistry()
    registry.register(DummyStrategy("s1"))

    registry.unregister("s1")

    assert registry.get("s1") is None


def test_unregister_missing_strategy_is_noop() -> None:
    registry = StrategyRegistry()

    registry.unregister("missing")

    assert registry.list_active() == []


def test_list_active_returns_snapshot() -> None:
    registry = StrategyRegistry()
    first = DummyStrategy("s1")
    second = DummyStrategy("s2")
    registry.register(first)
    registry.register(second)

    active = registry.list_active()
    active.clear()

    assert registry.list_active() == [first, second]


def test_list_by_timeframe_filters_registered_strategies() -> None:
    registry = StrategyRegistry()
    one_min = DummyStrategy("s1", timeframe="1m")
    five_min = DummyStrategy("s2", timeframe="5m")
    registry.register(one_min)
    registry.register(five_min)

    assert registry.list_by_timeframe("1m") == [one_min]
    assert registry.list_by_timeframe("5m") == [five_min]
    assert registry.list_by_timeframe("15m") == []


def test_register_overwrites_existing_strategy_id() -> None:
    registry = StrategyRegistry()
    old = DummyStrategy("s1", timeframe="1m")
    new = DummyStrategy("s1", timeframe="5m")

    registry.register(old)
    registry.register(new)

    assert registry.get("s1") is new
    assert registry.list_by_timeframe("1m") == []
    assert registry.list_by_timeframe("5m") == [new]


def test_thread_safe_register_and_unregister() -> None:
    registry = StrategyRegistry()

    def register_one(index: int) -> None:
        registry.register(DummyStrategy(f"s{index}"))

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(register_one, range(100)))

    assert len(registry.list_active()) == 100

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(lambda index: registry.unregister(f"s{index}"), range(100)))

    assert registry.list_active() == []


def test_reload_is_phase_placeholder() -> None:
    registry = StrategyRegistry()

    with pytest.raises(NotImplementedError, match="not implemented yet"):
        registry.reload("s1")
