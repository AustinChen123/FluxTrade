"""Route candles to strategies and execute resulting signals."""

from __future__ import annotations

import logging
from contextlib import nullcontext
from collections.abc import Callable
from decimal import Decimal
from typing import Any, Mapping, Optional

from src.core.client_order_id import market_signal_client_order_id
from src.core.models import Candlestick, Signal, SignalType, Trade
from src.core.portfolio_runtime import (
    PortfolioCoordinator,
    PortfolioDecisionRejected,
    PortfolioExposureSnapshot,
)
from src.core.strategy_registry import StrategyRegistry
from src.strategies.base import BaseStrategy

logger = logging.getLogger(__name__)


class SignalObserverError(RuntimeError):
    """Report a finalized signal observer failure before execution."""

    stage = "post_coordination_pre_execution"


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
        signal_handler: (
            Callable[[Signal, Optional[Candlestick]], bool | None] | None
        ) = None,
        position_loader: Callable[[str, str], Any | None] | None = None,
        exposure_loader: (
            Callable[
                [tuple[str, ...], str, Mapping[str, str]],
                PortfolioExposureSnapshot,
            ]
            | None
        ) = None,
        portfolio_coordinator: PortfolioCoordinator | None = None,
        signal_batch_observer: Callable[[tuple[Signal, ...]], None] | None = None,
        entry_admission_handler: Callable[[Signal], bool] | None = None,
    ) -> None:
        self.registry = registry
        self.execution_engine = execution_engine
        self.state_manager = state_manager
        self.signal_handler = signal_handler
        self.position_loader = position_loader
        self.exposure_loader = exposure_loader
        self.portfolio_coordinator = portfolio_coordinator
        self.signal_batch_observer = signal_batch_observer
        self.entry_admission_handler = entry_admission_handler
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
        admission_states: dict[str, tuple[BaseStrategy, bool, object]] = {}
        transaction_context = (
            self.portfolio_coordinator.decision_state_transaction()
            if emit_signals and self.portfolio_coordinator is not None
            else nullcontext()
        )
        with transaction_context as decision_state_transaction:
            for strategy in self.registry.list_active():
                if strategy.product_id != candle.product_id:
                    continue
                if strategy.requirements.timeframe != candle.timeframe:
                    continue
                if (
                    respect_state
                    and self.state_manager is not None
                    and not self.state_manager.is_running(
                        self._lifecycle_id(strategy.strategy_id)
                    )
                ):
                    logger.debug(
                        "Skipping strategy %s because it is not running",
                        strategy.strategy_id,
                    )
                    continue

                if not self._sync_position_if_changed(strategy):
                    continue
                if self.entry_admission_handler is not None:
                    admission_states[strategy.strategy_id] = (
                        strategy,
                        *self._snapshot_entry_admission_state(strategy),
                    )
                if (
                    decision_state_transaction is not None
                    and self.portfolio_coordinator is not None
                ):
                    decision_state_transaction.capture(strategy)
                signals = self._dispatch_to_strategy(strategy, candle)
                decisions.append((strategy.strategy_id, signals))

            if emit_signals:
                if (
                    self.signal_handler is not None
                    or self.portfolio_coordinator is not None
                ):
                    decisions = [
                        (
                            strategy_id,
                            [
                                self._with_market_idempotency(
                                    signal,
                                    product_id=candle.product_id,
                                    event_scope=candle.timeframe,
                                    event_timestamp=candle.timestamp,
                                    ordinal=ordinal,
                                )
                                for ordinal, signal in enumerate(signals)
                            ],
                        )
                        for strategy_id, signals in decisions
                    ]
                if self.portfolio_coordinator is not None:
                    decisions = self.portfolio_coordinator.coordinate_candle_decisions(
                        candle,
                        decisions,
                        exposure_loader=self.exposure_loader,
                        default_quantity=Decimal(
                            str(self.execution_engine.default_quantity)
                        ),
                        decision_state_transaction=(decision_state_transaction),
                    )
                if self.entry_admission_handler is not None:
                    entry_strategy_ids = {
                        strategy_id
                        for strategy_id, signals in decisions
                        if any(
                            signal.type in (SignalType.LONG, SignalType.SHORT)
                            for signal in signals
                        )
                    }
                    locally_restored_ids = self._locally_restored_entry_ids(
                        entry_strategy_ids
                    )
                    try:
                        entry_admissions = [
                            self._entry_is_admitted(signal)
                            for _strategy_id, signals in decisions
                            for signal in signals
                            if signal.type in (SignalType.LONG, SignalType.SHORT)
                        ]
                    except Exception:
                        self._restore_entry_admission_states(
                            locally_restored_ids,
                            admission_states,
                        )
                        raise
                    admitted_decisions = (
                        decisions
                        if all(entry_admissions)
                        else [
                            (
                                strategy_id,
                                [
                                    signal
                                    for signal in signals
                                    if signal.type
                                    not in (SignalType.LONG, SignalType.SHORT)
                                ],
                            )
                            for strategy_id, signals in decisions
                        ]
                    )
                    if not all(entry_admissions):
                        self._restore_entry_admission_states(
                            locally_restored_ids,
                            admission_states,
                        )
                    if decision_state_transaction is not None:
                        decision_state_transaction.restore_suppressed(
                            decisions,
                            admitted_decisions,
                        )
                    decisions = admitted_decisions
                if self.signal_batch_observer is not None:
                    finalized_batch = tuple(
                        signal
                        for _strategy_id, signals in decisions
                        for signal in signals
                    )
                    if finalized_batch:
                        try:
                            self.signal_batch_observer(finalized_batch)
                        except Exception as exc:
                            raise SignalObserverError("signal observer failed") from exc

        if emit_signals:
            for strategy_id, signals in decisions:
                self._process_signals(
                    strategy_id,
                    signals,
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
    def _snapshot_entry_admission_state(
        strategy: BaseStrategy,
    ) -> tuple[bool, object]:
        uses_explicit_contract = (
            type(strategy).snapshot_walk_forward_trade_state
            is not BaseStrategy.snapshot_walk_forward_trade_state
            and type(strategy).restore_walk_forward_trade_state
            is not BaseStrategy.restore_walk_forward_trade_state
        )
        if uses_explicit_contract:
            return True, strategy.snapshot_walk_forward_trade_state()
        return False, SignalProcessor._snapshot_trade_state(strategy)

    @staticmethod
    def _restore_entry_admission_states(
        strategy_ids: set[str],
        states: Mapping[str, tuple[BaseStrategy, bool, object]],
    ) -> None:
        for strategy_id in sorted(strategy_ids):
            strategy, uses_explicit_contract, state = states[strategy_id]
            if uses_explicit_contract:
                strategy.restore_walk_forward_trade_state(state)
            else:
                assert isinstance(state, dict)
                SignalProcessor._restore_trade_state(strategy, state)

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
        eligible_strategies: list[BaseStrategy] = []
        for strategy in self.registry.list_active():
            if strategy.product_id != trade.product_id:
                continue
            if self.state_manager is not None and not self.state_manager.is_running(
                self._lifecycle_id(strategy.strategy_id)
            ):
                logger.debug(
                    "Skipping strategy %s because it is not running",
                    strategy.strategy_id,
                )
                continue
            eligible_strategies.append(strategy)

        unsupported_portfolio_strategies = [
            strategy.strategy_id
            for strategy in eligible_strategies
            if (
                self.portfolio_coordinator is not None
                and self.portfolio_coordinator.portfolio_id_for_sleeve(
                    strategy.strategy_id
                )
                is not None
                and type(strategy).on_trade is not BaseStrategy.on_trade
            )
        ]
        if unsupported_portfolio_strategies:
            raise PortfolioDecisionRejected(
                "portfolio_trade_signals_unsupported:"
                f"strategy_ids={','.join(unsupported_portfolio_strategies)}"
            )

        decisions: list[tuple[str, Signal]] = []
        admission_states: dict[str, tuple[BaseStrategy, bool, object]] = {}
        for strategy in eligible_strategies:
            if type(strategy).on_trade is BaseStrategy.on_trade:
                continue
            if self.entry_admission_handler is not None:
                admission_states[strategy.strategy_id] = (
                    strategy,
                    *self._snapshot_entry_admission_state(strategy),
                )
            signal = strategy.on_trade(trade)
            if signal is not None:
                decisions.append((strategy.strategy_id, signal))

        portfolio_signal_ids = [
            strategy_id
            for strategy_id, _signal in decisions
            if (
                self.portfolio_coordinator is not None
                and self.portfolio_coordinator.portfolio_id_for_sleeve(
                    strategy_id
                )
                is not None
            )
        ]
        if portfolio_signal_ids:
            raise PortfolioDecisionRejected(
                "portfolio_trade_signals_unsupported:"
                f"strategy_ids={','.join(portfolio_signal_ids)}"
            )

        replay_decisions = [
            (
                strategy_id,
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
                else signal,
            )
            for strategy_id, signal in decisions
        ]
        if self.entry_admission_handler is not None:
            entry_strategy_ids = {
                strategy_id
                for strategy_id, signal in replay_decisions
                if signal.type in (SignalType.LONG, SignalType.SHORT)
            }
            try:
                entry_admissions = [
                    self._entry_is_admitted(signal)
                    for _strategy_id, signal in replay_decisions
                    if signal.type in (SignalType.LONG, SignalType.SHORT)
                ]
            except Exception:
                self._restore_entry_admission_states(
                    entry_strategy_ids,
                    admission_states,
                )
                raise
            if not all(entry_admissions):
                self._restore_entry_admission_states(
                    entry_strategy_ids,
                    admission_states,
                )
                replay_decisions = [
                    (strategy_id, signal)
                    for strategy_id, signal in replay_decisions
                    if signal.type not in (SignalType.LONG, SignalType.SHORT)
                ]

        for strategy_id, replay_stable_signal in replay_decisions:
            self._process_signals(strategy_id, [replay_stable_signal], None)

    def _entry_is_admitted(self, signal: Signal) -> bool:
        return (
            signal.type not in (SignalType.LONG, SignalType.SHORT)
            or self.entry_admission_handler is None
            or self.entry_admission_handler(signal)
        )

    def _locally_restored_entry_ids(
        self,
        strategy_ids: set[str],
    ) -> set[str]:
        return {
            strategy_id
            for strategy_id in strategy_ids
            if self.portfolio_coordinator is None
            or not self.portfolio_coordinator.requires_decision_state_rollback(
                strategy_id
            )
        }

    def _lifecycle_id(self, strategy_id: str) -> str:
        if self.portfolio_coordinator is None:
            return strategy_id
        return self.portfolio_coordinator.lifecycle_id_for_strategy(strategy_id)

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
                submitted = self.signal_handler(signal, candle)
                if (
                    submitted is False
                    and self.portfolio_coordinator is not None
                    and self.portfolio_coordinator.portfolio_id_for_sleeve(
                        strategy_id
                    )
                    is not None
                ):
                    raise PortfolioDecisionRejected(
                        "portfolio_submission_rejected:"
                        f"strategy_id={strategy_id}"
                    )
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
