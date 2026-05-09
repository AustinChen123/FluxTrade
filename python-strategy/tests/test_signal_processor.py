"""Tests for src/core/signal_processor.py."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock

from src.core.models import Candlestick, Signal, SignalType
from src.core.signal_processor import SignalProcessor
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy, StrategyRequirements


class DummyStrategy(BaseStrategy):
    def __init__(
        self,
        strategy_id: str,
        product_id: str = "BINANCE:BTCUSDT-PERP",
        timeframe: str = "1m",
        result=None,
        should_raise: bool = False,
    ):
        super().__init__(strategy_id, product_id)
        self._timeframe = timeframe
        self.result = result
        self.should_raise = should_raise
        self.candles_received: list[Candlestick] = []

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, self._timeframe, 10)

    def on_candle(self, candle: Candlestick):
        self.candles_received.append(candle)
        if self.should_raise:
            raise RuntimeError("strategy failed")
        return self.result


class DummyStateManager:
    def __init__(self, running: set[str]):
        self.running = running

    def is_running(self, strategy_id: str) -> bool:
        return strategy_id in self.running


def make_candle(
    product_id: str = "BINANCE:BTCUSDT-PERP",
    timeframe: str = "1m",
) -> Candlestick:
    return Candlestick(
        product_id=product_id,
        timeframe=timeframe,
        timestamp=1704067200000,
        open=Decimal("42000"),
        high=Decimal("42500"),
        low=Decimal("41500"),
        close=Decimal("42200"),
        volume=Decimal("100"),
    )


def make_signal(
    strategy_id: str = "s1",
    signal_type: SignalType = SignalType.LONG,
) -> Signal:
    return Signal(
        strategy_id=strategy_id,
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=1704067200000,
        type=signal_type,
        value=Decimal("42200"),
    )


def test_on_candle_routes_matching_strategy() -> None:
    signal = make_signal()
    strategy = DummyStrategy("s1", result=signal)
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    processor = SignalProcessor(registry, execution)
    candle = make_candle()

    processor.on_candle(candle)

    assert strategy.candles_received == [candle]
    execution.execute_signal.assert_called_once_with(signal, candle)


def test_on_candle_skips_timeframe_mismatch() -> None:
    strategy = DummyStrategy("s1", timeframe="5m", result=make_signal())
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()

    SignalProcessor(registry, execution).on_candle(make_candle(timeframe="1m"))

    assert strategy.candles_received == []
    execution.execute_signal.assert_not_called()


def test_on_candle_skips_product_mismatch() -> None:
    strategy = DummyStrategy(
        "s1",
        product_id="BINANCE:ETHUSDT-PERP",
        result=make_signal(),
    )
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()

    SignalProcessor(registry, execution).on_candle(make_candle())

    assert strategy.candles_received == []
    execution.execute_signal.assert_not_called()


def test_on_candle_skips_stopped_strategy() -> None:
    strategy = DummyStrategy("s1", result=make_signal())
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    state_manager = DummyStateManager(running=set())

    SignalProcessor(registry, execution, state_manager).on_candle(make_candle())

    assert strategy.candles_received == []
    execution.execute_signal.assert_not_called()


def test_dispatch_normalizes_none_signal_and_list() -> None:
    processor = SignalProcessor(StrategyRegistry(), MagicMock())
    candle = make_candle()
    single = make_signal()
    multiple = [make_signal("s1"), make_signal("s1", SignalType.SHORT)]

    assert processor._dispatch_to_strategy(DummyStrategy("s1", result=None), candle) == []
    assert processor._dispatch_to_strategy(DummyStrategy("s1", result=single), candle) == [single]
    assert processor._dispatch_to_strategy(DummyStrategy("s1", result=multiple), candle) == multiple


def test_process_signals_skips_no_signal() -> None:
    execution = MagicMock()
    processor = SignalProcessor(StrategyRegistry(), execution)

    processor._process_signals("s1", [make_signal(signal_type=SignalType.NO_SIGNAL)])

    execution.execute_signal.assert_not_called()


def test_process_signals_executes_multiple_actionable_signals() -> None:
    execution = MagicMock()
    processor = SignalProcessor(StrategyRegistry(), execution)
    candle = make_candle()
    signals = [make_signal("s1", SignalType.LONG), make_signal("s1", SignalType.SHORT)]

    processor._process_signals("s1", signals, candle)

    assert execution.execute_signal.call_count == 2
    execution.execute_signal.assert_any_call(signals[0], candle)
    execution.execute_signal.assert_any_call(signals[1], candle)


def test_process_signals_uses_signal_handler_when_provided() -> None:
    execution = MagicMock()
    signal_handler = MagicMock()
    processor = SignalProcessor(StrategyRegistry(), execution, signal_handler=signal_handler)
    candle = make_candle()
    signal = make_signal("s1", SignalType.LONG)

    processor._process_signals("s1", [signal], candle)

    signal_handler.assert_called_once_with(signal, candle)
    execution.execute_signal.assert_not_called()


def test_strategy_exception_does_not_stop_other_strategies() -> None:
    good_signal = make_signal("good")
    failing = DummyStrategy("bad", should_raise=True)
    good = DummyStrategy("good", result=good_signal)
    registry = StrategyRegistry()
    registry.register(failing)
    registry.register(good)
    execution = MagicMock()

    SignalProcessor(registry, execution).on_candle(make_candle())

    execution.execute_signal.assert_called_once_with(good_signal, make_candle())
