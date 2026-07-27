"""Route candles to strategies and execute resulting signals."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any, Optional

from src.core.client_order_id import market_signal_client_order_id
from src.core.models import Candlestick, Signal, SignalType, Trade
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


def apply_strategy_position_state(
    strategy: BaseStrategy,
    position_side: str | None,
) -> bool:
    """Align one strategy's internal trade state with authoritative position side."""
    sync_hook = getattr(strategy, "sync_position_state", None)
    if callable(sync_hook):
        hook_result = sync_hook(position_side)
        if hook_result is not None:
            return bool(hook_result)

    normalized_side = position_side.upper() if position_side else None
    has_direction_attr = hasattr(strategy, "position")
    if normalized_side is not None and not has_direction_attr:
        return False
    applied = False
    if hasattr(strategy, "_in_position"):
        setattr(strategy, "_in_position", normalized_side is not None)
        applied = True
    if has_direction_attr:
        if normalized_side == "LONG":
            setattr(strategy, "position", 1)
        elif normalized_side == "SHORT":
            setattr(strategy, "position", -1)
        else:
            setattr(strategy, "position", 0)
        applied = True
    return applied


class SignalProcessor:
    """Dispatch market candles to registered strategies."""

    def __init__(
        self,
        registry: StrategyRegistry,
        execution_engine: Any,
        state_manager: Any | None = None,
        signal_handler: Callable[[Signal, Optional[Candlestick]], None] | None = None,
        position_loader: Callable[[str, str], Any | None] | None = None,
    ) -> None:
        self.registry = registry
        self.execution_engine = execution_engine
        self.state_manager = state_manager
        self.signal_handler = signal_handler
        self.position_loader = position_loader
        self._observed_position_sides: dict[tuple[str, str], str | None] = {}

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
        decisions: list[tuple[str, list[Signal]]] = []
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

            if not self._sync_position_if_changed(strategy):
                continue
            signals = self._dispatch_to_strategy(strategy, candle)
            decisions.append((strategy.strategy_id, signals))

        if emit_signals:
            for strategy_id, signals in decisions:
                replay_stable_signals = (
                    [
                        self._with_market_idempotency(
                            signal,
                            product_id=candle.product_id,
                            event_scope=candle.timeframe,
                            event_timestamp=candle.timestamp,
                            ordinal=ordinal,
                        )
                        for ordinal, signal in enumerate(signals)
                    ]
                    if self.signal_handler is not None
                    else signals
                )
                self._process_signals(
                    strategy_id,
                    replay_stable_signals,
                    candle,
                )

    def warm_up(
        self,
        strategy: BaseStrategy,
        candles: list[Candlestick],
        *,
        require_complete_trade_state: bool = False,
    ) -> None:
        """Replay candles through one strategy without emitting orders.

        Startup restore retains the legacy generic position snapshot. Research
        folds require the strategy's explicit complete trade-state contract so
        warm-up signals cannot leak synthetic trades into scoring.
        """
        complete_trade_state = None
        generic_trade_state = None
        if require_complete_trade_state:
            complete_trade_state = strategy.snapshot_walk_forward_trade_state()
        else:
            generic_trade_state = self._snapshot_trade_state(strategy)
        try:
            for candle in candles:
                if strategy.product_id != candle.product_id:
                    continue
                if strategy.requirements.timeframe != candle.timeframe:
                    continue
                self._dispatch_to_strategy(strategy, candle)
        finally:
            if require_complete_trade_state:
                strategy.restore_walk_forward_trade_state(complete_trade_state)
            else:
                assert generic_trade_state is not None
                self._restore_trade_state(strategy, generic_trade_state)

    def set_position_state(self, strategy: BaseStrategy, position_side: str | None) -> bool:
        """Align strategy trade-state with the actual account position.

        Hook contract: True = synced, False = side unsupported (caller fails
        closed), None = not handled -> generic attribute fallback. The
        fallback only accepts a non-flat side for strategies exposing a
        direction-aware ``position`` attribute; ``_in_position``-only
        strategies cannot represent direction and may only sync flat state.
        """
        return apply_strategy_position_state(strategy, position_side)

    def _sync_position_if_changed(self, strategy: BaseStrategy) -> bool:
        if self.position_loader is None:
            return True
        key = (strategy.strategy_id, strategy.product_id)
        position = self.position_loader(strategy.strategy_id, strategy.product_id)
        position_side = (
            None
            if position is None
            else str(getattr(position.side, "value", position.side)).upper()
        )
        if (
            key in self._observed_position_sides
            and self._observed_position_sides[key] == position_side
        ):
            return True
        if not apply_strategy_position_state(strategy, position_side):
            logger.debug(
                "Strategy does not consume authoritative position sync: "
                "strategy=%s product=%s side=%s",
                strategy.strategy_id,
                strategy.product_id,
                position_side,
            )
        self._observed_position_sides[key] = position_side
        return True

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
        if (
            not trade.id.strip()
            or trade.id.strip().lower() == "unknown"
            or trade.timestamp <= 0
        ):
            raise ValueError(
                "trade requires a stable id and positive timestamp for replay safety"
            )
        decisions: list[tuple[str, Signal]] = []
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

            signal = strategy.on_trade(trade)
            if signal is not None:
                decisions.append((strategy.strategy_id, signal))

        for strategy_id, signal in decisions:
            replay_stable_signal = (
                self._with_market_idempotency(
                    signal,
                    product_id=trade.product_id,
                    event_scope=f"trade:{trade.id}:{trade.side}",
                    event_timestamp=trade.timestamp,
                    # on_trade() has a singular Signal contract, so the
                    # per-strategy ordinal is always zero and remains stable
                    # if unrelated strategies are added or removed.
                    ordinal=0,
                )
                if self.signal_handler is not None
                else signal
            )
            self._process_signals(strategy_id, [replay_stable_signal], None)

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

    @staticmethod
    def _with_market_idempotency(
        signal: Signal,
        *,
        product_id: str,
        event_scope: str,
        event_timestamp: int,
        ordinal: int,
    ) -> Signal:
        metadata = dict(signal.metadata or {})
        derived_client_order_id = market_signal_client_order_id(
            signal.strategy_id,
            product_id,
            event_scope,
            event_timestamp,
            signal.type.value.lower(),
            ordinal,
        )
        requested_client_order_id = metadata.get("client_order_id")
        if (
            requested_client_order_id is not None
            and requested_client_order_id != derived_client_order_id
        ):
            metadata["requested_client_order_id"] = str(
                requested_client_order_id
            )
        metadata["client_order_id"] = derived_client_order_id
        return signal.model_copy(update={"metadata": metadata})
