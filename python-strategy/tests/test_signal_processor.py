"""Tests for src/core/signal_processor.py."""

from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, sentinel

import pytest

from src.core.models import Candlestick, Signal, SignalType, Trade
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
        trade_result=None,
        should_raise: bool = False,
    ):
        super().__init__(strategy_id, product_id)
        self._timeframe = timeframe
        self.result = result
        self.trade_result = trade_result
        self.should_raise = should_raise
        self.candles_received: list[Candlestick] = []
        self.trades_received: list[Trade] = []

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, self._timeframe, 10)

    def on_candle(self, candle: Candlestick):
        self.candles_received.append(candle)
        if self.should_raise:
            raise RuntimeError("strategy failed")
        return self.result

    def on_trade(self, trade: Trade):
        self.trades_received.append(trade)
        if self.should_raise:
            raise RuntimeError("strategy failed")
        return self.trade_result


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


def make_trade(
    product_id: str = "BINANCE:BTCUSDT-PERP",
) -> Trade:
    return Trade(
        id="t1",
        product_id=product_id,
        price=Decimal("42200"),
        quantity=Decimal("0.1"),
        side="buy",
        timestamp=1704067200000,
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


def test_warm_up_routes_target_strategy_without_emitting_signals() -> None:
    signal = make_signal()
    strategy = DummyStrategy("s1", result=signal)
    other = DummyStrategy("s2", result=make_signal("s2"))
    registry = StrategyRegistry()
    registry.register(strategy)
    registry.register(other)
    execution = MagicMock()
    state_manager = DummyStateManager(running=set())
    processor = SignalProcessor(registry, execution, state_manager)
    candle = make_candle()

    processor.warm_up(strategy, [candle])

    assert strategy.candles_received == [candle]
    assert other.candles_received == []
    execution.execute_signal.assert_not_called()


def test_warm_up_restores_trade_state_after_dropped_signals() -> None:
    class StatefulStrategy(DummyStrategy):
        def __init__(self):
            super().__init__("s1")
            self.position = 0
            self._in_position = False

        def on_candle(self, candle: Candlestick):
            self.candles_received.append(candle)
            self.position = 1
            self._in_position = True
            return make_signal("s1", SignalType.LONG)

    strategy = StatefulStrategy()
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    processor = SignalProcessor(registry, execution)

    processor.warm_up(strategy, [make_candle()])

    assert strategy.candles_received == [make_candle()]
    assert strategy.position == 0
    assert strategy._in_position is False
    execution.execute_signal.assert_not_called()


def test_warm_up_failure_restores_trade_state_and_propagates() -> None:
    class FailingWarmupStrategy(DummyStrategy):
        def __init__(self):
            super().__init__("s1")
            self.position = 0
            self._in_position = False

        def on_candle(self, candle: Candlestick):
            self.candles_received.append(candle)
            self.position = 1
            self._in_position = True
            raise RuntimeError("warm-up replay failed")

    strategy = FailingWarmupStrategy()
    processor = SignalProcessor(StrategyRegistry(), MagicMock())

    try:
        processor.warm_up(strategy, [make_candle()])
    except RuntimeError as exc:
        assert str(exc) == "warm-up replay failed"
    else:
        raise AssertionError("warm-up failure should propagate")

    assert strategy.position == 0
    assert strategy._in_position is False


def test_set_position_state_maps_common_strategy_flags() -> None:
    class StatefulStrategy(DummyStrategy):
        def __init__(self):
            super().__init__("s1")
            self.position = 99
            self._in_position = True

    processor = SignalProcessor(StrategyRegistry(), MagicMock())

    cases = [
        (None, 0, False),
        ("LONG", 1, True),
        ("SHORT", -1, True),
    ]
    for position_side, expected_position, expected_in_position in cases:
        strategy = StatefulStrategy()

        processor.set_position_state(strategy, position_side)

        assert strategy.position == expected_position
        assert strategy._in_position is expected_in_position


def test_set_position_state_prefers_explicit_strategy_hook() -> None:
    class HookStrategy(DummyStrategy):
        def __init__(self):
            super().__init__("s1")
            self.position = 99
            self.synced_side = None

        def sync_position_state(self, position_side: str | None) -> bool:
            self.synced_side = position_side
            return True

    strategy = HookStrategy()
    processor = SignalProcessor(StrategyRegistry(), MagicMock())

    applied = processor.set_position_state(strategy, "LONG")

    assert applied is True
    assert strategy.synced_side == "LONG"
    assert strategy.position == 99


# ---------------------------------------------------------------------------
# Parametrized decision-table for set_position_state
# ---------------------------------------------------------------------------

_SENTINEL = sentinel.untouched


class _HookTrueStrategy(BaseStrategy):
    """Hook always returns True; exposes attrs that would be mutated by fallback."""

    def __init__(self):
        super().__init__("hook_true", "BINANCE:BTCUSDT-PERP")
        self._in_position = _SENTINEL

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(self, candle):
        return None

    def sync_position_state(self, position_side: str | None) -> bool:
        return True


class _HookFalseStrategy(BaseStrategy):
    """Hook always returns False; _in_position sentinel must be untouched."""

    def __init__(self):
        super().__init__("hook_false", "BINANCE:BTCUSDT-PERP")
        self._in_position = _SENTINEL

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(self, candle):
        return None

    def sync_position_state(self, position_side: str | None) -> bool:
        return False


class _HookNoneFullStrategy(BaseStrategy):
    """Hook returns None (inherits default) + has both position and _in_position."""

    def __init__(self):
        super().__init__("hook_none_full", "BINANCE:BTCUSDT-PERP")
        self.position = 99
        self._in_position = _SENTINEL
    # sync_position_state NOT overridden → BaseStrategy.sync_position_state → None

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(self, candle):
        return None


class _InPositionOnlyStrategy(BaseStrategy):
    """No ``position`` attr; only ``_in_position``. Cannot represent direction."""

    def __init__(self):
        super().__init__("in_pos_only", "BINANCE:BTCUSDT-PERP")
        self._in_position = _SENTINEL
    # sync_position_state NOT overridden → None

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(self, candle):
        return None


class _NoAttrsStrategy(BaseStrategy):
    """No state attrs at all; hook returns None."""

    def __init__(self):
        super().__init__("no_attrs", "BINANCE:BTCUSDT-PERP")

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "1m", 1)

    def on_candle(self, candle):
        return None


@pytest.mark.parametrize(
    "strategy_factory, side, expected_result, checks",
    [
        # hook returns True → result True, fallback attrs untouched
        pytest.param(
            _HookTrueStrategy,
            "LONG",
            True,
            {"_in_position": _SENTINEL},  # fallback must NOT have run
            id="hook_returns_true",
        ),
        # hook returns False → result False, fallback attrs untouched (P1 regression)
        pytest.param(
            _HookFalseStrategy,
            "LONG",
            False,
            {"_in_position": _SENTINEL},  # fallback must NOT have run
            id="hook_returns_false_attrs_untouched",
        ),
        # hook returns None (base default) + both attrs + LONG → True, position=1
        pytest.param(
            _HookNoneFullStrategy,
            "LONG",
            True,
            {"position": 1, "_in_position": True},
            id="hook_none_full_attrs_LONG",
        ),
        # hook returns None + both attrs + SHORT → True, position=-1
        pytest.param(
            _HookNoneFullStrategy,
            "SHORT",
            True,
            {"position": -1, "_in_position": True},
            id="hook_none_full_attrs_SHORT",
        ),
        # hook returns None + both attrs + None → True, position=0
        pytest.param(
            _HookNoneFullStrategy,
            None,
            True,
            {"position": 0, "_in_position": False},
            id="hook_none_full_attrs_flat",
        ),
        # hook returns None + _in_position only + LONG → False, attr untouched (direction-safety)
        pytest.param(
            _InPositionOnlyStrategy,
            "LONG",
            False,
            {"_in_position": _SENTINEL},
            id="in_position_only_LONG_direction_safety",
        ),
        # hook returns None + _in_position only + None → True, _in_position=False
        pytest.param(
            _InPositionOnlyStrategy,
            None,
            True,
            {"_in_position": False},
            id="in_position_only_flat",
        ),
        # hook returns None + no attrs at all + LONG → False
        pytest.param(
            _NoAttrsStrategy,
            "LONG",
            False,
            {},
            id="no_attrs_LONG",
        ),
    ],
)
def test_set_position_state_decision_matrix(
    strategy_factory, side, expected_result, checks
) -> None:
    """Full decision table for set_position_state hook contract + fallback."""
    strategy = strategy_factory()
    processor = SignalProcessor(StrategyRegistry(), MagicMock())

    result = processor.set_position_state(strategy, side)

    assert result is expected_result
    for attr, expected_val in checks.items():
        assert getattr(strategy, attr) is expected_val or getattr(strategy, attr) == expected_val, (
            f"{attr}: expected {expected_val!r}, got {getattr(strategy, attr)!r}"
        )


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


def test_on_trade_routes_matching_strategy_without_timeframe_filter() -> None:
    signal = make_signal()
    strategy = DummyStrategy("s1", timeframe="15m", trade_result=signal)
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    signal_handler = MagicMock()

    SignalProcessor(
        registry,
        execution,
        signal_handler=signal_handler,
    ).on_trade(make_trade())

    assert strategy.trades_received == [make_trade()]
    signal_handler.assert_called_once_with(signal, None)
    execution.execute_signal.assert_not_called()


def test_on_trade_skips_product_mismatch() -> None:
    strategy = DummyStrategy(
        "s1",
        product_id="BINANCE:ETHUSDT-PERP",
        trade_result=make_signal(),
    )
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()

    SignalProcessor(registry, execution).on_trade(make_trade())

    assert strategy.trades_received == []
    execution.execute_signal.assert_not_called()


def test_on_trade_skips_stopped_strategy() -> None:
    strategy = DummyStrategy("s1", trade_result=make_signal())
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    state_manager = DummyStateManager(running=set())

    SignalProcessor(registry, execution, state_manager).on_trade(make_trade())

    assert strategy.trades_received == []
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
