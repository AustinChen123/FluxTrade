"""Pure ordered evidence plans and executable bindings, not database authority.

Plans retain immutable seed and terminal descriptors. Bindings validate strategy
and candle identities before exposing scopes without querying current modeled data.
SKIPPED never consumes a pin or starts a callback; the plan validates references. Mutable
runtime candles belong only to the drift-checked binding, not the evidence plan.
Caller limits bound construction, not semantic evidence identity.
"""

from dataclasses import InitVar, dataclass, field
from contextlib import nullcontext

from src.core.models import Candlestick
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy

from .bootstrap_seed import (
    BootstrapCandle,
    BootstrapKey,
    BootstrapDisposition,
    BootstrapSeed,
    BootstrapSeedError,
    classify_bootstrap,
)
from .decision_application import MarketDataDecisionOutcome
from .decision_input import MarketDataDecisionInput
from .requirements import ProfileRequirement
from .context_enrichment import enrich_profile_context
from .decision_identity import (
    MarketDataDecisionCompositionIdentity,
    decision_config_hash,
)
from .decision_owner import PreparedRecordedDecision


@dataclass(frozen=True, slots=True)
class BootstrapHydrationPlan:
    seed: BootstrapSeed
    recorded: tuple[
        tuple[MarketDataDecisionOutcome, MarketDataDecisionInput | None], ...
    ]
    completed_recorded_through_ms: int | None
    max_seed_candles: InitVar[int]
    max_recorded_candles: InitVar[int]
    expected_key: InitVar[BootstrapKey]
    requirements: InitVar[tuple[ProfileRequirement, ...]]
    lookback: InitVar[int]

    def __post_init__(
        self,
        max_seed_candles: int,
        max_recorded_candles: int,
        expected_key: BootstrapKey,
        requirements: tuple[ProfileRequirement, ...],
        lookback: int,
    ) -> None:
        if (
            type(self.seed) is not BootstrapSeed
            or type(expected_key) is not BootstrapKey
            or type(requirements) is not tuple
            or any(type(item) is not ProfileRequirement for item in requirements)
            or type(lookback) is not int
            or self.seed.key != expected_key
            or self.seed.requirements != requirements
            or self.seed.lookback != lookback
        ):
            raise BootstrapSeedError()
        disposition = classify_bootstrap(
            requirements,
            proposed=self.seed,
            stored=self.seed,
            history_known_absent=False,
            completed_recorded_through_ms=self.completed_recorded_through_ms,
            recorded=self.recorded,
            max_seed_candles=max_seed_candles,
            max_recorded_candles=max_recorded_candles,
        )
        if disposition is not BootstrapDisposition.REPLAY:
            raise BootstrapSeedError()

    @property
    def seed_candles(self) -> tuple[BootstrapCandle, ...]:
        """Completed seed in callback order; suffix descriptors follow this tuple."""
        return self.seed.candles


@dataclass(frozen=True, slots=True)
class BoundBootstrapHydration:
    """Frozen binding; mutable runtime candles/strategy are checked before every scope.

    Caller supplies authoritative suffix candles. This pure binding does not prove
    their database provenance and never constructs missing recorded evidence.
    """

    plan: BootstrapHydrationPlan
    strategy: BaseStrategy
    identity: MarketDataDecisionCompositionIdentity
    suffix: tuple[tuple[Candlestick, PreparedRecordedDecision], ...]
    candles: tuple[Candlestick, ...] = field(init=False)
    _candle_state: tuple[tuple[object, ...], ...] = field(init=False, repr=False)
    _configuration_state: str = field(init=False, repr=False)

    def _configuration_fingerprint(self) -> str:
        return decision_config_hash(
            {
                "replay_configuration": self.strategy.replay_configuration(),
                "artifact_version": getattr(
                    type(self.strategy), "__fluxtrade_artifact_version__", None
                ),
            }
        )

    def _check_strategy(self) -> None:
        key = self.plan.seed.key
        identity = self.identity
        requirements = self.strategy.requirements
        if (
            self.strategy.strategy_id != key.strategy_id
            or self.strategy.product_id != key.product_id
            or requirements.product_id != key.product_id
            or requirements.timeframe != key.timeframe
            or requirements.profile_requirements != self.plan.seed.requirements
            or requirements.lookback_window != self.plan.seed.lookback
            or identity.execution_scope_id != key.execution_scope_id
            or identity.strategy_version != key.strategy_version
            or identity.config_hash != key.config_hash
        ):
            raise BootstrapSeedError()

    @staticmethod
    def _snapshot(candle: Candlestick) -> tuple[object, ...]:
        return tuple(
            (type(value), value)
            for value in (
                candle.product_id,
                candle.timeframe,
                candle.timestamp,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
            )
        )

    def __post_init__(self) -> None:
        if (
            type(self.plan) is not BootstrapHydrationPlan
            or not isinstance(self.strategy, BaseStrategy)
            or type(self.identity) is not MarketDataDecisionCompositionIdentity
            or type(self.suffix) is not tuple
            or len(self.suffix) != len(self.plan.recorded)
        ):
            raise BootstrapSeedError()
        self._check_strategy()
        # Local mutation fingerprint only, never a durable composition identity.
        object.__setattr__(
            self,
            "_configuration_state",
            self._configuration_fingerprint(),
        )
        key = self.plan.seed.key
        candles = [
            Candlestick(
                product_id=key.product_id,
                timeframe=key.timeframe,
                timestamp=item.bar_start_ms,
                open=item.open,
                high=item.high,
                low=item.low,
                close=item.close,
                volume=item.volume,
            )
            for item in self.plan.seed_candles
        ]
        for pair, (outcome, pinned) in zip(
            self.suffix, self.plan.recorded, strict=True
        ):
            if type(pair) is not tuple or len(pair) != 2:
                raise BootstrapSeedError()
            candle, prepared = pair
            if (
                type(candle) is not Candlestick
                or type(prepared) is not PreparedRecordedDecision
                or prepared.strategy is not self.strategy
                or prepared.candle is not candle
                or prepared.outcome != outcome
                or prepared.pinned_input
                != (None if outcome.disposition == "SKIPPED" else pinned)
                or candle.product_id != key.product_id
                or candle.timeframe != key.timeframe
                or outcome.key != key.decision_key(candle.timestamp)
                or prepared._snapshot()
                != (prepared._strategy_state, prepared._candle_state)
            ):
                raise BootstrapSeedError()
            candles.append(candle)
        object.__setattr__(self, "candles", tuple(candles))
        object.__setattr__(
            self, "_candle_state", tuple(self._snapshot(c) for c in candles)
        )

    def _check(self) -> None:
        self._check_strategy()
        if self._configuration_fingerprint() != self._configuration_state:
            raise BootstrapSeedError()
        if tuple(self._snapshot(c) for c in self.candles) != self._candle_state:
            raise BootstrapSeedError()

    def decision_scope_loader(self, candle: Candlestick):
        self._check()
        index = next((i for i, item in enumerate(self.candles) if item is candle), None)
        if index is None:
            raise BootstrapSeedError()
        if index >= len(self.plan.seed_candles):
            prepared = self.suffix[index - len(self.plan.seed_candles)][1]

            def recorded_scope(strategy, supplied_candle, context):
                self._check()
                return prepared(strategy, supplied_candle, context)

            return recorded_scope
        evidence = self.plan.seed_candles[index].context

        def seed_scope(strategy, supplied_candle, context):
            self._check()
            if (
                strategy is not self.strategy
                or supplied_candle is not candle
                or type(context) is not StrategyContext
                or context.strategy_id != strategy.strategy_id
                or context.product_id != candle.product_id
                or context.timestamp != candle.timestamp
            ):
                raise BootstrapSeedError()
            return nullcontext(
                enrich_profile_context(
                    context,
                    self.plan.seed.requirements,
                    evidence,
                    decision_time_ms=evidence.decision_time_ms,
                )
            )

        return seed_scope
