"""Run-scoped modeled profile input boundary; providers must already be preloaded."""

from dataclasses import dataclass
from typing import Protocol

from .context_enrichment import (
    ProfileContextEnrichmentError,
    validate_profile_context_coverage,
)
from .decision_context import ProfileDecisionBasis, StrategyMarketDataContext
from .read_types import _integer, _safe
from .requirements import ProfileRequirement


class ModeledProfileContextProvider(Protocol):
    """Deterministic in-memory lookup; HTTP, DB and latest reads are forbidden."""

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


@dataclass(frozen=True, slots=True)
class ModeledProfileInput:
    provider: ModeledProfileContextProvider
    availability_policy_id: str

    def __post_init__(self) -> None:
        try:
            if not callable(getattr(self.provider, "context_for", None)):
                raise ValueError
            _safe(self.availability_policy_id)
        except ValueError:
            raise ModeledProfileInputError() from None

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
        result = self.provider.context_for(
            requirements,
            decision_time_ms=decision_time_ms,
            availability_policy_id=self.availability_policy_id,
        )
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
