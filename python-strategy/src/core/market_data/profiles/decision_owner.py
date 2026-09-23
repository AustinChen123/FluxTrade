from __future__ import annotations

import logging
from dataclasses import InitVar, dataclass, field
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext

from src.core.models import Candlestick
from src.core.signal_processor import StrategyDecisionSkipped
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy

from .context_enrichment import enrich_profile_context
from .decision_application import (
    MarketDataDecisionBatch,
    MarketDataDecisionKey,
    MarketDataDecisionOutcome,
)
from .decision_batch_builder import DecisionBatchBuilder
from .decision_identity import (
    MarketDataDecisionCompositionIdentity,
    decision_config_hash,
)
from .decision_input import MarketDataDecisionInput
from .decision_input_store import (
    DecisionInputRecord,
    DecisionInputPinResult,
    DecisionInputPinStatus,
    DecisionInputStore,
)
from .snapshot_cache import ProfileSnapshotCache

logger = logging.getLogger(__name__)

IdentityResolver = Callable[[BaseStrategy], MarketDataDecisionCompositionIdentity]
_PREPARED_TOKEN = object()


class MarketDataDecisionOwnerError(ValueError):
    def __init__(self) -> None:
        super().__init__("MARKET_DATA_DECISION_OWNER_INVALID")


@contextmanager
def _skipped_scope() -> Iterator[StrategyContext | None]:
    raise StrategyDecisionSkipped()
    yield None


class MarketDataDecisionOwner:
    """Own request identity and create one terminal-evidence scope per candle."""

    def __init__(
        self,
        *,
        environment: str,
        execution_scope_id: str,
        identity_resolver: IdentityResolver,
        cache: ProfileSnapshotCache,
        input_store: DecisionInputStore,
        utc_ms: Callable[[], int],
        monotonic_ms: Callable[[], int],
        revoked_checker: Callable[[tuple[str, ...]], bool] | None = None,
    ) -> None:
        if (
            type(environment) is not str
            or not environment
            or type(execution_scope_id) is not str
            or not execution_scope_id
            or not callable(identity_resolver)
            or not callable(utc_ms)
            or not callable(monotonic_ms)
            or (revoked_checker is not None and not callable(revoked_checker))
            or not callable(getattr(cache, "decision_many", None))
            or not callable(getattr(cache, "live_requests", None))
            or not callable(getattr(input_store, "get", None))
            or not callable(getattr(input_store, "pin_confirmed", None))
        ):
            raise MarketDataDecisionOwnerError()
        self._environment = environment
        self._execution_scope_id = execution_scope_id
        self._identity_resolver = identity_resolver
        self._cache = cache
        self._input_store = input_store
        self._utc_ms = utc_ms
        self._monotonic_ms = monotonic_ms
        self._revoked_checker = revoked_checker

    def begin_candle(self, candle: Candlestick) -> "MarketDataCandleDecision":
        try:
            decision_time_ms = self._utc_ms()
            current_monotonic_ms = self._monotonic_ms()
            if (
                type(candle) is not Candlestick
                or type(decision_time_ms) is not int
                or not 0 <= decision_time_ms <= 2**63 - 1
                or type(current_monotonic_ms) is not int
                or not 0 <= current_monotonic_ms <= 2**63 - 1
            ):
                raise ValueError
        except (TypeError, ValueError, OverflowError):
            raise MarketDataDecisionOwnerError() from None
        return MarketDataCandleDecision(
            self, candle, decision_time_ms, current_monotonic_ms
        )

    def replay_candle(
        self,
        candle: Candlestick,
        batch: MarketDataDecisionBatch,
    ) -> "RecordedMarketDataCandleDecision":
        self._validate_replay_candle(candle, batch)
        return RecordedMarketDataCandleDecision(self, candle, batch)

    def _validate_replay_candle(
        self, candle: Candlestick, batch: MarketDataDecisionBatch
    ) -> None:
        if (
            type(candle) is not Candlestick
            or type(batch) is not MarketDataDecisionBatch
            or batch.environment != self._environment
            or batch.execution_scope_id != self._execution_scope_id
            or batch.product_id != candle.product_id
            or batch.timeframe != candle.timeframe
            or batch.bar_start_ms != candle.timestamp
        ):
            raise MarketDataDecisionOwnerError()

    def prepare_replay_candle(
        self,
        strategy: BaseStrategy,
        candle: Candlestick,
        batch: MarketDataDecisionBatch,
    ) -> "PreparedRecordedDecision":
        self._validate_replay_candle(candle, batch)
        if not isinstance(strategy, BaseStrategy):
            raise MarketDataDecisionOwnerError()
        requirements = strategy.requirements.profile_requirements
        outcome = next(
            (
                item
                for item in batch.outcomes
                if item.key.strategy_id == strategy.strategy_id
            ),
            None,
        )
        if not requirements:
            if outcome is not None:
                raise MarketDataDecisionOwnerError()
            return PreparedRecordedDecision(
                strategy, candle, None, None, _PREPARED_TOKEN
            )
        identity = self._identity_resolver(strategy)
        if (
            type(identity) is not MarketDataDecisionCompositionIdentity
            or identity.execution_scope_id != self._execution_scope_id
            or strategy.product_id != candle.product_id
            or strategy.requirements.timeframe != candle.timeframe
        ):
            raise MarketDataDecisionOwnerError()
        key = MarketDataDecisionKey.for_candle(
            environment=self._environment,
            execution_scope_id=identity.execution_scope_id,
            strategy_id=strategy.strategy_id,
            strategy_version=identity.strategy_version,
            config_hash=identity.config_hash,
            product_id=candle.product_id,
            timeframe=candle.timeframe,
            bar_start_ms=candle.timestamp,
        )
        if outcome is None or outcome.key != key:
            raise MarketDataDecisionOwnerError()
        pinned = None
        if outcome.disposition == "APPLIED":
            record = self._input_store.get(key)
            if type(record) is not DecisionInputRecord:
                raise MarketDataDecisionOwnerError()
            pinned = record.value
        return PreparedRecordedDecision(
            strategy, candle, outcome, pinned, _PREPARED_TOKEN, key
        )


@dataclass(frozen=True, slots=True)
class PreparedRecordedDecision:
    """Validated fixed replay evidence; no owner, store, session, cache, or clock."""

    strategy: BaseStrategy
    candle: Candlestick
    outcome: MarketDataDecisionOutcome | None
    pinned_input: MarketDataDecisionInput | None
    _token: InitVar[object] = None
    _expected_key: InitVar[MarketDataDecisionKey | None] = None
    _strategy_state: str = field(init=False, repr=False)
    _candle_state: tuple[object, ...] = field(init=False, repr=False)

    def _snapshot(self) -> tuple[str, tuple[object, ...]]:
        strategy = self.strategy
        state = decision_config_hash(
            {
                "strategy_id": strategy.strategy_id,
                "product_id": strategy.product_id,
                "requirements": strategy.requirements,
                "version": getattr(
                    type(strategy), "__fluxtrade_artifact_version__", None
                ),
                "configuration": strategy.replay_configuration()
                if self.outcome is not None
                else None,
            }
        )
        values = tuple(
            getattr(self.candle, name)
            for name in (
                "product_id",
                "timeframe",
                "timestamp",
                "open",
                "high",
                "low",
                "close",
                "volume",
            )
        )
        return state, tuple((type(value), value) for value in values)

    def __post_init__(
        self, _token: object, _expected_key: MarketDataDecisionKey | None
    ) -> None:
        if (
            _token is not _PREPARED_TOKEN
            or not isinstance(self.strategy, BaseStrategy)
            or type(self.candle) is not Candlestick
        ):
            raise MarketDataDecisionOwnerError()
        outcome, pinned = self.outcome, self.pinned_input
        requirements = self.strategy.requirements.profile_requirements
        if outcome is None:
            if requirements or pinned is not None or _expected_key is not None:
                raise MarketDataDecisionOwnerError()
        else:
            if (
                type(outcome) is not MarketDataDecisionOutcome
                or type(_expected_key) is not MarketDataDecisionKey
                or outcome.key != _expected_key
                or outcome.key.strategy_id != self.strategy.strategy_id
                or outcome.key.product_id != self.strategy.product_id
                or outcome.key.product_id != self.candle.product_id
                or outcome.key.trigger_id
                != f"{self.candle.timeframe}:{self.candle.timestamp}"
            ):
                raise MarketDataDecisionOwnerError()
            if outcome.disposition == "SKIPPED":
                if pinned is not None:
                    raise MarketDataDecisionOwnerError()
            elif (
                type(pinned) is not MarketDataDecisionInput
                or pinned.key != outcome.key
                or pinned.input_id != outcome.input_id
                or pinned.input_digest != outcome.input_digest
                or pinned.requirements != requirements
            ):
                raise MarketDataDecisionOwnerError()
        state, candle_state = self._snapshot()
        object.__setattr__(self, "_strategy_state", state)
        object.__setattr__(self, "_candle_state", candle_state)

    def __call__(
        self,
        strategy: BaseStrategy,
        candle: Candlestick,
        context: StrategyContext | None,
    ) -> AbstractContextManager[StrategyContext | None]:
        if strategy is not self.strategy or candle is not self.candle:
            raise MarketDataDecisionOwnerError()
        if self._snapshot() != (self._strategy_state, self._candle_state):
            raise MarketDataDecisionOwnerError()
        if self.outcome is None:
            if strategy.requirements.profile_requirements:
                raise MarketDataDecisionOwnerError()
            return nullcontext(context)
        if self.outcome.disposition == "SKIPPED" and context is None:
            return _skipped_scope()
        if (
            type(context) is not StrategyContext
            or context.strategy_id != strategy.strategy_id
            or context.product_id != candle.product_id
            or context.timestamp != candle.timestamp
        ):
            raise MarketDataDecisionOwnerError()
        if self.outcome.disposition == "SKIPPED":
            return _skipped_scope()
        pinned = self.pinned_input
        if (
            pinned is None
            or pinned.requirements != strategy.requirements.profile_requirements
        ):
            raise MarketDataDecisionOwnerError()
        return nullcontext(
            enrich_profile_context(
                context,
                pinned.requirements,
                pinned.context,
                decision_time_ms=pinned.decision_time_ms,
            )
        )


class RecordedMarketDataCandleDecision:
    """Compatibility callable delegates all evidence validation to prepare."""

    def __init__(
        self,
        owner: MarketDataDecisionOwner,
        candle: Candlestick,
        batch: MarketDataDecisionBatch,
    ) -> None:
        self._owner, self._candle, self._batch = owner, candle, batch

    def __call__(
        self,
        strategy: BaseStrategy,
        candle: Candlestick,
        context: StrategyContext | None,
    ) -> AbstractContextManager[StrategyContext | None]:
        if candle is not self._candle:
            raise MarketDataDecisionOwnerError()
        return self._owner.prepare_replay_candle(strategy, candle, self._batch)(
            strategy, candle, context
        )


class MarketDataCandleDecision:
    def __init__(
        self,
        owner: MarketDataDecisionOwner,
        candle: Candlestick,
        decision_time_ms: int,
        current_monotonic_ms: int,
    ) -> None:
        self._owner = owner
        self._candle = candle
        self._decision_time_ms = decision_time_ms
        self._current_monotonic_ms = current_monotonic_ms
        self._batch = DecisionBatchBuilder(
            environment=owner._environment,
            execution_scope_id=owner._execution_scope_id,
            candle=candle,
        )

    def __call__(
        self,
        strategy: BaseStrategy,
        candle: Candlestick,
        context: StrategyContext | None,
    ) -> AbstractContextManager[StrategyContext | None]:
        if not isinstance(strategy, BaseStrategy) or candle is not self._candle:
            raise MarketDataDecisionOwnerError()
        requirements = strategy.requirements.profile_requirements
        if not requirements:
            return nullcontext(context)
        if (
            type(context) is not StrategyContext
            or context.strategy_id != strategy.strategy_id
            or context.product_id != candle.product_id
            or context.timestamp != candle.timestamp
        ):
            raise MarketDataDecisionOwnerError()
        identity = self._owner._identity_resolver(strategy)
        if (
            type(identity) is not MarketDataDecisionCompositionIdentity
            or identity.execution_scope_id != self._owner._execution_scope_id
        ):
            raise MarketDataDecisionOwnerError()
        key = MarketDataDecisionKey.for_candle(
            environment=self._owner._environment,
            execution_scope_id=identity.execution_scope_id,
            strategy_id=strategy.strategy_id,
            strategy_version=identity.strategy_version,
            config_hash=identity.config_hash,
            product_id=candle.product_id,
            timeframe=candle.timeframe,
            bar_start_ms=candle.timestamp,
        )
        try:
            existing = self._owner._input_store.get(key)
        except Exception:
            return self._skip(strategy, key, "INPUT_STORE_FAILED")
        if existing is not None:
            if (
                type(existing) is not DecisionInputRecord
                or existing.value.key != key
                or existing.value.requirements != requirements
            ):
                raise MarketDataDecisionOwnerError()
            identifiers = tuple(
                sorted(
                    {
                        day.snapshot_id
                        for item in existing.value.context.profiles
                        if item.profile is not None
                        for day in item.profile.manifest.days
                    }
                )
            )
            suppressed = False
            if identifiers and self._owner._revoked_checker is not None:
                suppressed = self._owner._revoked_checker(identifiers)
                if type(suppressed) is not bool:
                    raise MarketDataDecisionOwnerError()
            enriched = enrich_profile_context(
                context,
                requirements,
                existing.value.context,
                decision_time_ms=existing.value.decision_time_ms,
            )
            return self._batch.applied_scope(
                key=key,
                input_id=existing.value.input_id,
                input_digest=existing.value.input_digest,
                context=enriched,
                signal_suppressed=suppressed,
                suppression_reason="SNAPSHOT_REVOKED" if suppressed else None,
            )
        requests = self._owner._cache.live_requests(
            requirements, selection_time_ms=self._decision_time_ms
        )
        market_data = self._owner._cache.decision_many(
            requests,
            decision_time_ms=self._decision_time_ms,
            current_monotonic_ms=self._current_monotonic_ms,
        )
        enriched = enrich_profile_context(
            context,
            requirements,
            market_data,
            decision_time_ms=self._decision_time_ms,
        )
        value = MarketDataDecisionInput(
            key, requirements, self._decision_time_ms, market_data
        )
        pin = self._owner._input_store.pin_confirmed(value)
        if type(pin) is not DecisionInputPinResult:
            raise MarketDataDecisionOwnerError()
        if pin.status is not DecisionInputPinStatus.CONFIRMED:
            reason = (
                "INPUT_STORE_FAILED"
                if pin.status is DecisionInputPinStatus.FAILED
                else "INPUT_COMMIT_UNCONFIRMED"
            )
            return self._skip(strategy, key, reason)
        record = pin.record
        if (
            type(record) is not DecisionInputRecord
            or record.value.canonical_bytes != value.canonical_bytes
        ):
            raise MarketDataDecisionOwnerError()
        return self._batch.applied_scope(
            key=key,
            input_id=record.value.input_id,
            input_digest=record.value.input_digest,
            context=enriched,
        )

    def _skip(
        self,
        strategy: BaseStrategy,
        key: MarketDataDecisionKey,
        reason: str,
    ) -> AbstractContextManager[StrategyContext | None]:
        self._batch.record_skipped(key=key, reason=reason)
        logger.warning(
            "market_data_decision_input_skipped strategy_id=%s reason=%s",
            strategy.strategy_id,
            reason,
        )
        return _skipped_scope()

    def build(self) -> MarketDataDecisionBatch | None:
        return self._batch.build()
