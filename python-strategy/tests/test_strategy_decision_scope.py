from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.core.signal_processor import SignalProcessor, StrategyDecisionSkipped
from src.core.strategy_context import StrategyContext
from src.core.strategy_registry import StrategyRegistry
from test_signal_processor import DummyStrategy, make_candle, make_signal


@pytest.mark.parametrize("mode", ["positional", "keyword", "none"])
@pytest.mark.parametrize("scoped", [False, True])
def test_scope_context_and_signal_order(mode, scoped):
    events = []
    base = StrategyContext(
        "s1",
        "BINANCE:BTCUSDT-PERP",
        make_candle().timestamp,
        Decimal(1),
        Decimal(1),
        Decimal(0),
        Decimal(0),
        Decimal(0),
        Decimal(0),
    )
    enriched = replace(base, available_cash=Decimal(2))
    expected = enriched if scoped else base

    class Positional(DummyStrategy):
        def on_candle(self, candle, context=None, /):  # pyright: ignore[reportIncompatibleMethodOverride]
            assert context is expected
            events.append("callback")
            return make_signal()

    class Keyword(DummyStrategy):
        def on_candle(self, candle, *, context=None):  # pyright: ignore[reportIncompatibleMethodOverride]
            assert context is expected
            events.append("callback")
            return make_signal()

    class NoContext(DummyStrategy):
        def on_candle(self, candle):  # pyright: ignore[reportIncompatibleMethodOverride]
            events.append("callback")
            return make_signal()

    strategy = {"positional": Positional, "keyword": Keyword, "none": NoContext}[mode](
        "s1"
    )
    registry = StrategyRegistry()
    registry.register(strategy)
    execution = MagicMock()
    execution.execute_signal.side_effect = lambda *_: events.append("signal")
    processor = SignalProcessor(
        registry, execution, strategy_context_loader=lambda *_: base
    )

    @contextmanager
    def scope(received, candle, context):
        assert received is strategy and context is (None if mode == "none" else base)
        events.append("enter")
        yield enriched
        events.append("exit")

    processor.on_candle(make_candle(), decision_scope=scope if scoped else None)
    assert events == (
        ["enter", "callback", "exit", "signal"] if scoped else ["callback", "signal"]
    )


def test_enter_skip_continues_next_strategy_and_empty_decision():
    first, second = DummyStrategy("s1", result=make_signal()), DummyStrategy("s2")
    registry = StrategyRegistry()
    registry.register(first)
    registry.register(second)
    execution = MagicMock()
    processor = SignalProcessor(registry, execution)
    processor._process_signals = MagicMock(wraps=processor._process_signals)
    events = []

    @contextmanager
    def scope(strategy, candle, context):
        events.append(strategy.strategy_id)
        if strategy is first:
            raise StrategyDecisionSkipped()
        yield context

    processor.on_candle(make_candle(), decision_scope=scope)
    assert events == ["s1", "s2"]
    assert first.candles_received == [] and second.candles_received == [make_candle()]
    execution.execute_signal.assert_not_called()
    assert [call.args[:2] for call in processor._process_signals.call_args_list] == [
        ("s1", []),
        ("s2", []),
    ]


@pytest.mark.parametrize("phase", ["enter", "callback", "exit"])
def test_base_exception_scope_boundaries(phase):
    class Abort(BaseException):
        pass

    failure = Abort()
    events = []

    class Strategy(DummyStrategy):
        def on_candle(self, candle):  # pyright: ignore[reportIncompatibleMethodOverride]
            events.append("callback")
            if phase == "callback":
                raise failure

    class Scope:
        def __enter__(self):
            events.append("enter")
            if phase == "enter":
                raise failure
            return None

        def __exit__(self, cls, error, traceback):
            events.append("exit")
            if phase == "callback":
                assert cls is Abort and error is failure and traceback is not None
            else:
                assert cls is None and error is None and traceback is None
            if phase == "exit":
                raise failure
            return True

    registry = StrategyRegistry()
    registry.register(Strategy("s1"))
    processor = SignalProcessor(registry, MagicMock())
    with pytest.raises(Abort) as caught:
        processor.on_candle(make_candle(), decision_scope=lambda *_: Scope())
    assert caught.value is failure
    assert events == (["enter"] if phase == "enter" else ["enter", "callback", "exit"])


@pytest.mark.parametrize("phase", ["enter", "callback", "exit"])
@pytest.mark.parametrize("skip_error", [False, True])
def test_failure_identity_and_abnormal_exit(phase, skip_error):
    failure = StrategyDecisionSkipped() if skip_error else RuntimeError("failure")
    events = []

    class Strategy(DummyStrategy):
        def on_candle(self, candle):  # pyright: ignore[reportIncompatibleMethodOverride]
            events.append("callback")
            if phase == "callback":
                raise failure

    registry = StrategyRegistry()
    registry.register(Strategy("s1"))
    processor = SignalProcessor(registry, MagicMock())

    class Scope:
        def __enter__(self):
            events.append("enter")
            if phase == "enter":
                raise failure
            return None

        def __exit__(self, cls, error, traceback):
            events.append(("exit", error))
            if phase == "exit":
                raise failure
            return True  # Cannot suppress a callback failure into APPLIED.

    if phase == "enter" and skip_error:
        processor.on_candle(make_candle(), decision_scope=lambda *_: Scope())
        assert events == ["enter"]
    else:
        with pytest.raises(type(failure)) as caught:
            processor.on_candle(make_candle(), decision_scope=lambda *_: Scope())
        assert caught.value is failure
        if phase == "callback":
            assert events == ["enter", "callback", ("exit", failure)]
