from __future__ import annotations

import logging
from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager, nullcontext

from src.core.models import Candlestick
from src.core.signal_processor import StrategyDecisionSkipped
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy

from .context_enrichment import enrich_profile_context
from .decision_application import MarketDataDecisionBatch, MarketDataDecisionKey
from .decision_batch_builder import DecisionBatchBuilder
from .decision_identity import MarketDataDecisionCompositionIdentity
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
    ) -> None:
        if (
            type(environment) is not str
            or not environment
            or type(execution_scope_id) is not str
            or not execution_scope_id
            or not callable(identity_resolver)
            or not callable(utc_ms)
            or not callable(monotonic_ms)
            or not callable(getattr(cache, "decision_many", None))
            or not callable(getattr(cache, "live_requests", None))
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
            self._batch.record_skipped(key=key, reason=reason)
            logger.warning(
                "market_data_decision_input_skipped strategy_id=%s reason=%s",
                strategy.strategy_id,
                reason,
            )
            return _skipped_scope()
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

    def build(self) -> MarketDataDecisionBatch | None:
        return self._batch.build()
