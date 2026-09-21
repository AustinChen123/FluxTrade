"""Pure consumer acceptance evidence; no clocks, cache, or refresh policy."""

from dataclasses import dataclass

from .decision_context import ProfileDecisionContext
from .live_query import ProfileQueryError
from .live_validation import (
    LiveProfileValidationUnavailable,
    finalize_live_profile_decision,
)
from .wire import ProfileWireEvidence


def _clock(value: int) -> None:
    if type(value) is not int or not 0 <= value <= 2**63 - 1:
        raise ProfileQueryError("INTEGRITY") from None


@dataclass(frozen=True, slots=True)
class ObservedProfileSnapshot:
    """Evidence accepted after full decode and atomic local acceptance.

    Observation is consumer-owned, not server served_at. Monotonic values must
    share one process/clock domain; this object is not a persistence contract.
    Clock contradictions retain evidence but finalize as CLOCK_UNCERTAIN.
    """

    evidence: ProfileWireEvidence
    observed_at_ms: int
    observed_monotonic_ms: int

    def __post_init__(self) -> None:
        if type(self.evidence) is not ProfileWireEvidence:
            raise ProfileQueryError("INTEGRITY") from None
        _clock(self.observed_at_ms)
        _clock(self.observed_monotonic_ms)

    def finalize(
        self, *, decision_time_ms: int, current_monotonic_ms: int
    ) -> ProfileDecisionContext:
        _clock(decision_time_ms)
        _clock(current_monotonic_ms)
        validated = self.evidence.validated
        request = validated.query.request
        reason = None
        if (
            current_monotonic_ms < self.observed_monotonic_ms
            or decision_time_ms < self.observed_at_ms
            or self.evidence.served_at_ms > self.observed_at_ms
        ):
            reason = "CLOCK_UNCERTAIN"
        baseline = finalize_live_profile_decision(
            request,
            validated,
            observed_at_ms=self.observed_at_ms,
            decision_time_ms=decision_time_ms,
        )
        if reason is None and baseline.reason == "CLOCK_UNCERTAIN":
            return baseline
        if reason is None and (
            current_monotonic_ms - self.observed_monotonic_ms
            >= validated.validation_expires_at_ms - self.observed_at_ms
        ):
            reason = "VALIDATION_EXPIRED"
        if reason is None:
            return baseline
        return finalize_live_profile_decision(
            request,
            LiveProfileValidationUnavailable(request, reason),
            observed_at_ms=None,
            decision_time_ms=decision_time_ms,
        )
