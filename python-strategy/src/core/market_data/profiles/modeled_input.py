"""Run-scoped modeled profile input boundary; providers must already be preloaded."""

from collections.abc import Callable
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, field
from typing import Protocol

from src.core.data_provider import timeframe_to_ms
from src.core.models import Candlestick
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy

from .context_enrichment import (
    ProfileContextEnrichmentError,
    enrich_profile_context,
    validate_profile_context_coverage,
)
from .decision_context import ProfileDecisionBasis, StrategyMarketDataContext
from .read_types import _hex, _integer, _safe
from .requirements import ProfileRequirement


class ModeledProfileContextProvider(Protocol):
    """Deterministic in-memory lookup; HTTP, DB and latest reads are forbidden."""

    @property
    def availability_policy_id(self) -> str: ...

    @property
    def availability_policy_digest(self) -> str: ...

    @property
    def dataset_digest(self) -> str: ...

    def context_for(
        self,
        requirements: tuple[ProfileRequirement, ...],
        *,
        decision_time_ms: int,
        availability_policy_id: str,
    ) -> StrategyMarketDataContext: ...


class ModeledProfileInputError(ValueError):
    def __init__(self) -> None:
        super().__init__("MODELED_PROFILE_INPUT_INVALID")


ModeledWarmupScope = Callable[
    [BaseStrategy, Candlestick, StrategyContext | None],
    AbstractContextManager[StrategyContext | None],
]
ModeledWarmupScopeLoader = Callable[[Candlestick], ModeledWarmupScope]


def _provider_identity(
    provider: ModeledProfileContextProvider, expected_policy_id: str
) -> tuple[str, str]:
    try:
        provider_policy_id = getattr(provider, "availability_policy_id")
        policy_digest = getattr(provider, "availability_policy_digest")
        dataset_digest = getattr(provider, "dataset_digest")
        if (
            type(provider_policy_id) is not str
            or provider_policy_id != expected_policy_id
            or type(policy_digest) is not str
            or type(dataset_digest) is not str
        ):
            raise ValueError
        _hex(policy_digest)
        _hex(dataset_digest)
        return policy_digest, dataset_digest
    except (AttributeError, ValueError):
        raise ModeledProfileInputError() from None


def completed_candle_decision_time_ms(candle_timestamp_ms: int, timeframe: str) -> int:
    """Return the exclusive end of one completed decision candle."""
    try:
        _integer(candle_timestamp_ms)
        if type(timeframe) is not str:
            raise ValueError
        duration_ms = timeframe_to_ms(timeframe)
        if type(duration_ms) is not int or duration_ms <= 0:
            raise ValueError
        result = candle_timestamp_ms + duration_ms
        _integer(result)
        return result
    except (IndexError, TypeError, ValueError):
        raise ModeledProfileInputError() from None


@dataclass(frozen=True, slots=True)
class ModeledProfileInput:
    provider: ModeledProfileContextProvider
    availability_policy_id: str
    availability_policy_digest: str = field(init=False)
    dataset_digest: str = field(init=False)

    def __post_init__(self) -> None:
        try:
            if not callable(getattr(self.provider, "context_for", None)):
                raise ValueError
            _safe(self.availability_policy_id)
            policy_digest, dataset_digest = _provider_identity(
                self.provider, self.availability_policy_id
            )
        except ValueError:
            raise ModeledProfileInputError() from None
        object.__setattr__(self, "availability_policy_digest", policy_digest)
        object.__setattr__(self, "dataset_digest", dataset_digest)

    def resolve(
        self,
        requirements: tuple[ProfileRequirement, ...],
        *,
        decision_time_ms: int,
    ) -> StrategyMarketDataContext:
        try:
            _integer(decision_time_ms)
            if type(requirements) is not tuple or any(
                type(requirement) is not ProfileRequirement
                for requirement in requirements
            ):
                raise ValueError
        except ValueError:
            raise ModeledProfileInputError() from None
        expected_identity = (
            self.availability_policy_digest,
            self.dataset_digest,
        )
        if (
            _provider_identity(self.provider, self.availability_policy_id)
            != expected_identity
        ):
            raise ModeledProfileInputError() from None
        result = self.provider.context_for(
            requirements,
            decision_time_ms=decision_time_ms,
            availability_policy_id=self.availability_policy_id,
        )
        if (
            _provider_identity(self.provider, self.availability_policy_id)
            != expected_identity
        ):
            raise ModeledProfileInputError() from None
        try:
            if type(result) is not StrategyMarketDataContext:
                raise ValueError
            validate_profile_context_coverage(requirements, result, decision_time_ms)
            for item in result.profiles:
                request = item.request
                if (
                    item.basis is not ProfileDecisionBasis.MODELED
                    or request.purpose != "MODELED_RESEARCH"
                    or request.availability_policy_id != self.availability_policy_id
                    or request.as_of_ms != decision_time_ms
                ):
                    raise ValueError
        except (ValueError, ProfileContextEnrichmentError):
            raise ModeledProfileInputError() from None
        return result


def modeled_profile_warmup_scope_loader(
    modeled_input: ModeledProfileInput,
) -> ModeledWarmupScopeLoader:
    """Create a pure completed-candle warm-up scope with no I/O or persistence."""
    if type(modeled_input) is not ModeledProfileInput:
        raise ModeledProfileInputError()

    def load(bound_candle: Candlestick) -> ModeledWarmupScope:
        if type(bound_candle) is not Candlestick:
            raise ModeledProfileInputError()
        decision_time_ms = completed_candle_decision_time_ms(
            bound_candle.timestamp,
            bound_candle.timeframe,
        )

        def scope(
            strategy: BaseStrategy,
            candle: Candlestick,
            context: StrategyContext | None,
        ) -> AbstractContextManager[StrategyContext | None]:
            if not isinstance(strategy, BaseStrategy) or candle is not bound_candle:
                raise ModeledProfileInputError()
            requirements = strategy.requirements.profile_requirements
            if not requirements:
                return nullcontext(context)
            if (
                strategy.product_id != candle.product_id
                or strategy.requirements.timeframe != candle.timeframe
                or type(context) is not StrategyContext
                or context.strategy_id != strategy.strategy_id
                or context.product_id != candle.product_id
                or context.timestamp != candle.timestamp
                or context.market_data is not None
            ):
                raise ModeledProfileInputError()
            market_data = modeled_input.resolve(
                requirements,
                decision_time_ms=decision_time_ms,
            )
            try:
                enriched = enrich_profile_context(
                    context,
                    requirements,
                    market_data,
                    decision_time_ms=decision_time_ms,
                )
            except ProfileContextEnrichmentError:
                raise ModeledProfileInputError() from None
            return nullcontext(enriched)

        return scope

    return load
