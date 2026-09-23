"""Collect one candle's terminal decision outcomes before receipt persistence."""

from __future__ import annotations

from contextlib import AbstractContextManager
from types import TracebackType
from typing import TypeVar

from src.core.models import Candlestick, Signal

from .decision_application import (
    MAX_DECISION_BATCH_PARTICIPANTS,
    MarketDataDecisionBatch,
    MarketDataDecisionKey,
    MarketDataDecisionOutcome,
)


_Context = TypeVar("_Context")


class DecisionBatchBuildError(ValueError):
    def __init__(self) -> None:
        super().__init__("MARKET_DATA_DECISION_BATCH_BUILD_INVALID")


class DecisionBatchBuilder:
    """Own participant/outcome completeness for one uncommitted candle."""

    def __init__(
        self,
        *,
        environment: str,
        execution_scope_id: str,
        candle: Candlestick,
    ) -> None:
        try:
            if type(candle) is not Candlestick:
                raise ValueError
            self._prototype = MarketDataDecisionBatch(
                environment=environment,
                execution_scope_id=execution_scope_id,
                product_id=candle.product_id,
                timeframe=candle.timeframe,
                bar_start_ms=candle.timestamp,
                participants=(),
                outcomes=(),
            )
        except (TypeError, ValueError):
            raise DecisionBatchBuildError() from None
        self._participants: dict[str, MarketDataDecisionKey] = {}
        self._outcomes: dict[str, MarketDataDecisionOutcome] = {}
        self._sealed = False

    def _admit(self, key: MarketDataDecisionKey) -> None:
        try:
            if self._sealed or type(key) is not MarketDataDecisionKey:
                raise ValueError
            trigger = f"{self._prototype.timeframe}:{self._prototype.bar_start_ms}"
            if (
                key.environment != self._prototype.environment
                or key.execution_scope_id != self._prototype.execution_scope_id
                or key.product_id != self._prototype.product_id
                or key.trigger_kind != "CANDLE"
                or key.trigger_id != trigger
                or key.strategy_id in self._participants
                or len(self._participants) >= MAX_DECISION_BATCH_PARTICIPANTS
            ):
                raise ValueError
            self._participants[key.strategy_id] = key
        except ValueError:
            raise DecisionBatchBuildError() from None

    def applied_scope(
        self,
        *,
        key: MarketDataDecisionKey,
        input_id: str,
        input_digest: str,
        context: _Context,
        signal_suppressed: bool = False,
        suppression_reason: str | None = None,
    ) -> AbstractContextManager[_Context]:
        """Record APPLIED only after the wrapped callback returns normally."""
        try:
            outcome = MarketDataDecisionOutcome(
                key=key,
                disposition="APPLIED",
                input_id=input_id,
                input_digest=input_digest,
                signal_suppressed=signal_suppressed,
                suppression_reason=suppression_reason,
            )
        except ValueError:
            raise DecisionBatchBuildError() from None
        self._admit(key)

        outcomes = self._outcomes

        class AppliedScope(AbstractContextManager):
            _state = "NEW"

            def __enter__(self) -> _Context:
                if self._state != "NEW":
                    raise DecisionBatchBuildError()
                self._state = "ENTERED"
                return context

            def __exit__(
                self,
                exc_type: type[BaseException] | None,
                exc: BaseException | None,
                traceback: TracebackType | None,
            ) -> bool:
                if self._state != "ENTERED":
                    raise DecisionBatchBuildError()
                self._state = "CLOSED"
                if exc_type is None:
                    outcomes[key.strategy_id] = outcome
                return False

            def filter_callback_signals(self, signals: list[Signal]) -> list[Signal]:
                """Suppression affects signals only, never completed callback state."""
                return [] if signal_suppressed else signals

        return AppliedScope()

    def record_skipped(
        self,
        *,
        key: MarketDataDecisionKey,
        reason: str,
        input_id: str | None = None,
        input_digest: str | None = None,
    ) -> None:
        """Record a callback that the caller guarantees never started."""
        try:
            outcome = MarketDataDecisionOutcome(
                key=key,
                disposition="SKIPPED",
                input_id=input_id,
                input_digest=input_digest,
                reason=reason,
            )
        except ValueError:
            raise DecisionBatchBuildError() from None
        self._admit(key)
        self._outcomes[key.strategy_id] = outcome

    def build(self) -> MarketDataDecisionBatch | None:
        """Seal the exact terminal set; empty means no strategy opted in."""
        if self._sealed:
            raise DecisionBatchBuildError()
        if not self._participants:
            self._sealed = True
            return None
        if set(self._participants) != set(self._outcomes):
            raise DecisionBatchBuildError()
        try:
            batch = MarketDataDecisionBatch(
                environment=self._prototype.environment,
                execution_scope_id=self._prototype.execution_scope_id,
                product_id=self._prototype.product_id,
                timeframe=self._prototype.timeframe,
                bar_start_ms=self._prototype.bar_start_ms,
                participants=tuple(self._participants.values()),
                outcomes=tuple(self._outcomes.values()),
            )
        except ValueError:
            raise DecisionBatchBuildError() from None
        self._sealed = True
        return batch
