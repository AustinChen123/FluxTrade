"""Route candles to strategies and execute resulting signals."""

from __future__ import annotations

import logging
from typing import Any, Optional

from src.core.models import Candlestick, Signal, SignalType
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
    ) -> None:
        self.registry = registry
        self.execution_engine = execution_engine
        self.state_manager = state_manager

    def on_candle(self, candle: Candlestick) -> None:
        """Route a candle to matching, running strategies."""
        for strategy in self.registry.list_active():
            if strategy.product_id != candle.product_id:
                continue
            if strategy.requirements.timeframe != candle.timeframe:
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
                signals = self._dispatch_to_strategy(strategy, candle)
                self._process_signals(strategy.strategy_id, signals, candle)
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
            self.execution_engine.execute_signal(signal, candle)
