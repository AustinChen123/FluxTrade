"""Run-scoped modeled profile input boundary; providers must already be preloaded."""

from dataclasses import dataclass, field
from typing import Protocol

from src.core.data_provider import timeframe_to_ms

from .context_enrichment import (
    ProfileContextEnrichmentError,
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
        if _provider_identity(self.provider, self.availability_policy_id) != expected_identity:
            raise ModeledProfileInputError() from None
        result = self.provider.context_for(
            requirements,
            decision_time_ms=decision_time_ms,
            availability_policy_id=self.availability_policy_id,
        )
        if _provider_identity(self.provider, self.availability_policy_id) != expected_identity:
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
