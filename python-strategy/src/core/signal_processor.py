"""Route candles to strategies and execute resulting signals."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Optional

from src.core.models import Candlestick, Signal, SignalType, Trade
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


class SignalProcessor:
    """Dispatch market candles to registered strategies."""

    def __init__(
        self,
        registry: StrategyRegistry,
        execution_engine: Any,
        state_manager: Any | None = None,
        signal_handler: Callable[[Signal, Optional[Candlestick]], None] | None = None,
    ) -> None:
        self.registry = registry
        self.execution_engine = execution_engine
        self.state_manager = state_manager
        self.signal_handler = signal_handler

    def on_candle(
        self,
        candle: Candlestick,
        *,
        emit_signals: bool = True,
        respect_state: bool = True,
    ) -> None:
        """Route a candle to matching, running strategies.

        ``emit_signals=False`` is used for startup warm-up replay: strategy
        memory is rebuilt from candles, but any generated signals are ignored.
        """
        for strategy in self.registry.list_active():
            if strategy.product_id != candle.product_id:
                continue
            if strategy.requirements.timeframe != candle.timeframe:
                continue
            if (
                respect_state
                and self.state_manager is not None
                and not self.state_manager.is_running(strategy.strategy_id)
            ):
                logger.debug(
                    "Skipping strategy %s because it is not running",
                    strategy.strategy_id,
                )
                continue

            try:
                signals = self._dispatch_to_strategy(strategy, candle)
                if emit_signals:
                    self._process_signals(strategy.strategy_id, signals, candle)
            except Exception:
                logger.exception("Error processing strategy %s", strategy.strategy_id)

    def warm_up(self, strategy: BaseStrategy, candles: list[Candlestick]) -> None:
        """Replay candles through one strategy without emitting orders."""
        trade_state = self._snapshot_trade_state(strategy)
        try:
            for candle in candles:
                if strategy.product_id != candle.product_id:
                    continue
                if strategy.requirements.timeframe != candle.timeframe:
                    continue
                self._dispatch_to_strategy(strategy, candle)
        finally:
            self._restore_trade_state(strategy, trade_state)

    def set_position_state(self, strategy: BaseStrategy, position_side: str | None) -> bool:
        """Align common strategy trade-state flags with actual account position."""
        sync_hook = getattr(strategy, "sync_position_state", None)
        if callable(sync_hook) and sync_hook(position_side):
            return True

        applied = False
        normalized_side = position_side.upper() if position_side else None
        if hasattr(strategy, "_in_position"):
            setattr(strategy, "_in_position", normalized_side is not None)
            applied = True
        if hasattr(strategy, "position"):
            if normalized_side == "LONG":
                setattr(strategy, "position", 1)
            elif normalized_side == "SHORT":
                setattr(strategy, "position", -1)
            else:
                setattr(strategy, "position", 0)
            applied = True
        return applied

    @staticmethod
    def _snapshot_trade_state(strategy: BaseStrategy) -> dict[str, Any]:
        return {
            attr: getattr(strategy, attr)
            for attr in ("_in_position", "position")
            if hasattr(strategy, attr)
        }

    @staticmethod
    def _restore_trade_state(strategy: BaseStrategy, state: dict[str, Any]) -> None:
        for attr, value in state.items():
            setattr(strategy, attr, value)

    def on_trade(self, trade: Trade) -> None:
        """Route a trade to matching, running strategies."""
        for strategy in self.registry.list_active():
            if strategy.product_id != trade.product_id:
                continue
            if self.state_manager is not None and not self.state_manager.is_running(
                strategy.strategy_id
            ):
                logger.debug(
                    "Skipping strategy %s because it is not running",
                    strategy.strategy_id,
                )
                continue

            try:
                signal = strategy.on_trade(trade)
                if signal is not None:
                    self._process_signals(strategy.strategy_id, [signal], None)
            except Exception:
                logger.exception("Error processing strategy %s", strategy.strategy_id)

    def _dispatch_to_strategy(
        self,
        strategy: BaseStrategy,
        candle: Candlestick,
    ) -> list[Signal]:
        """Call strategy.on_candle() and normalize the result."""
        result = strategy.on_candle(candle)
        if result is None:
            return []
        if isinstance(result, Signal):
            return [result]
        if isinstance(result, list):
            return result
        raise TypeError(
            "strategy.on_candle() must return None, Signal, or list[Signal]"
        )

    def _process_signals(
        self,
        strategy_id: str,
        signals: list[Signal],
        candle: Optional[Candlestick] = None,
    ) -> None:
        """Execute actionable signals."""
        for signal in signals:
            if signal.type == SignalType.NO_SIGNAL:
                continue
            if signal.strategy_id != strategy_id:
                logger.warning(
                    "Signal strategy_id mismatch: expected %s, got %s",
                    strategy_id,
                    signal.strategy_id,
                )
            if self.signal_handler is not None:
                self.signal_handler(signal, candle)
            else:
                self.execution_engine.execute_signal(signal, candle)
