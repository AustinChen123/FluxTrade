from contextlib import contextmanager
from dataclasses import replace
from decimal import Decimal
from unittest.mock import MagicMock

import pytest

from src.core.models import Candlestick, Signal
from src.core.signal_processor import SignalProcessor, StrategyDecisionSkipped
from src.core.strategy_context import RiskSnapshot, StrategyContext
from test_signal_processor import DummyStrategy, make_candle, make_signal


def _context(strategy: DummyStrategy, candle: Candlestick) -> StrategyContext:
    return StrategyContext(
        strategy_id=strategy.strategy_id,
        product_id=candle.product_id,
        timestamp=candle.timestamp,
        available_cash=Decimal("100"),
        total_equity=Decimal("100"),
        realized_pnl=Decimal("0"),
        unrealized_pnl=Decimal("0"),
        current_drawdown=Decimal("0"),
        max_drawdown=Decimal("0"),
    )


class _RecordingStrategy(DummyStrategy):
    def __init__(self, *, failure: BaseException | None = None) -> None:
        super().__init__("s1")
        self.contexts: list[StrategyContext | None] = []
        self.failure = failure
        self.position = 0
        self._in_position = False

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        self.candles_received.append(candle)
        self.contexts.append(context)
        self.position = 1
        self._in_position = True
        if self.failure is not None:
            raise self.failure
        return make_signal("s1")


def test_opt_in_warmup_scope_loads_and_enriches_context_without_emitting():
    strategy = _RecordingStrategy()
    candle = make_candle()
    base = _context(strategy, candle)
    enriched = replace(base, risk=RiskSnapshot(False, "recorded"))
    context_loader = MagicMock(return_value=base)
    scope_loader = MagicMock()
    manager = MagicMock()
    manager.__enter__.return_value = enriched
    manager.__exit__.return_value = False
    scope = MagicMock(return_value=manager)
    scope_loader.return_value = scope
    execution = MagicMock()
    processor = SignalProcessor(
        MagicMock(), execution, strategy_context_loader=context_loader
    )

    processor.warm_up(strategy, [candle], decision_scope_loader=scope_loader)

    context_loader.assert_called_once_with(strategy, candle, ())
    scope_loader.assert_called_once_with(candle)
    scope.assert_called_once_with(strategy, candle, base)
    manager.__exit__.assert_called_once_with(None, None, None)
    assert strategy.contexts == [enriched]
    assert strategy.position == 0
    assert strategy._in_position is False
    execution.execute_signal.assert_not_called()


def test_recorded_skip_does_not_start_warmup_callback():
    strategy = _RecordingStrategy()
    candle = make_candle()
    context_loader = MagicMock(return_value=_context(strategy, candle))

    @contextmanager
    def skipped(_strategy, _candle, _context):
        raise StrategyDecisionSkipped()
        yield

    processor = SignalProcessor(
        MagicMock(), MagicMock(), strategy_context_loader=context_loader
    )
    processor.warm_up(
        strategy,
        [candle],
        decision_scope_loader=lambda _candle: skipped,
    )

    assert strategy.candles_received == []
    assert strategy.contexts == []


class _CallbackAbort(BaseException):
    pass


def test_started_base_exception_cannot_be_suppressed_and_restores_trade_state():
    failure = _CallbackAbort("callback failed")
    strategy = _RecordingStrategy(failure=failure)
    candle = make_candle()
    manager = MagicMock()
    manager.__enter__.return_value = _context(strategy, candle)
    manager.__exit__.return_value = True
    scope = MagicMock(return_value=manager)
    processor = SignalProcessor(
        MagicMock(),
        MagicMock(),
        strategy_context_loader=MagicMock(return_value=_context(strategy, candle)),
    )

    with pytest.raises(_CallbackAbort, match="callback failed") as caught:
        processor.warm_up(
            strategy,
            [candle],
            decision_scope_loader=lambda _candle: scope,
        )

    exit_args = manager.__exit__.call_args.args
    assert caught.value is failure
    assert exit_args[0] is _CallbackAbort and exit_args[1] is failure
    assert exit_args[2] is not None
    traceback = caught.value.__traceback__
    while traceback is not None and traceback is not exit_args[2]:
        traceback = traceback.tb_next
    assert traceback is exit_args[2]
    assert strategy.position == 0
    assert strategy._in_position is False


def test_callback_strategy_decision_skipped_is_not_treated_as_pre_callback_skip():
    failure = StrategyDecisionSkipped()
    strategy = _RecordingStrategy(failure=failure)
    candle = make_candle()
    manager = MagicMock()
    manager.__enter__.return_value = _context(strategy, candle)
    manager.__exit__.return_value = True
    processor = SignalProcessor(
        MagicMock(),
        MagicMock(),
        strategy_context_loader=MagicMock(return_value=_context(strategy, candle)),
    )

    with pytest.raises(StrategyDecisionSkipped) as caught:
        processor.warm_up(
            strategy,
            [candle],
            decision_scope_loader=lambda _candle: MagicMock(return_value=manager),
        )

    assert caught.value is failure
    manager.__exit__.assert_called_once()
    assert strategy.candles_received == [candle]


@pytest.mark.parametrize("stage", ["enter", "exit"])
def test_scope_base_exception_propagates_from_exact_lifecycle_stage(stage):
    failure = _CallbackAbort(f"{stage} failed")
    strategy = _RecordingStrategy()
    candle = make_candle()
    manager = MagicMock()
    manager.__enter__.return_value = _context(strategy, candle)
    manager.__exit__.return_value = False
    if stage == "enter":
        manager.__enter__.side_effect = failure
    else:
        manager.__exit__.side_effect = failure
    processor = SignalProcessor(
        MagicMock(),
        MagicMock(),
        strategy_context_loader=MagicMock(return_value=_context(strategy, candle)),
    )

    with pytest.raises(_CallbackAbort) as caught:
        processor.warm_up(
            strategy,
            [candle],
            decision_scope_loader=lambda _candle: MagicMock(return_value=manager),
        )

    assert caught.value is failure
    if stage == "enter":
        manager.__exit__.assert_not_called()
        assert strategy.candles_received == []
    else:
        manager.__exit__.assert_called_once_with(None, None, None)
        assert strategy.candles_received == [candle]
    assert strategy.position == 0
    assert strategy._in_position is False
