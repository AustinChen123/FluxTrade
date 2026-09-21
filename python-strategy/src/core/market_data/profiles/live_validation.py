"""Conservative server pinned-read evidence, not consumer observed/FRESH/deadline
or zero-delay revocation evidence.
"""

from dataclasses import dataclass
from typing import Callable

from .live_query import (
    LiveProfileQueryResult,
    LiveProfileQueryUnavailable,
    ProfileLiveProvider,
    ProfileQueryError,
    query_live_profile,
)
from .live_selection import LiveSelectionContext
from .read_types import ProfileQueryRequest
from .decision_context import (
    ProfileDecisionContext,
    ProfileDecisionBasis,
    ProfileDecisionStatus,
)

_MAX = (1 << 63) - 1
_AGE = 300000


def _integer(value: int) -> int:
    if type(value) is not int or not 0 <= value <= _MAX:
        raise ValueError("invalid validation clock")
    return value


@dataclass(frozen=True, slots=True)
class LiveProfileValidationUnavailable:
    request: ProfileQueryRequest
    reason: str

    def __post_init__(self) -> None:
        if (
            type(self.request) is not ProfileQueryRequest
            or self.request.purpose != "LIVE_QUERY"
            or self.request.freshness_policy_id != "utc_complete_strict_v1"
        ):
            raise ProfileQueryError("INTEGRITY") from None
        if type(self.reason) is not str or self.reason not in (
            "CLOCK_UNCERTAIN",
            "VALIDATION_EXPIRED",
        ):
            raise ValueError("invalid validation reason") from None


@dataclass(frozen=True, slots=True)
class ValidatedLiveProfileQuery:
    """Server pinned-read evidence only; expiry is anchored to validation start."""

    query: LiveProfileQueryResult
    validation_started_at_ms: int
    validation_completed_at_ms: int
    validation_elapsed_ms: int

    def __post_init__(self) -> None:
        try:
            start = _integer(self.validation_started_at_ms)
            end = _integer(self.validation_completed_at_ms)
            elapsed = _integer(self.validation_elapsed_ms)
            if (
                type(self.query) is not LiveProfileQueryResult
                or self.query.selection.decision_time_ms != start
                or start > _MAX - _AGE
                or not 0 <= end - start <= elapsed < _AGE
            ):
                raise ValueError
        except ValueError:
            raise ProfileQueryError("INTEGRITY") from None

    @property
    def validation_max_age_ms(self) -> int:
        return _AGE

    @property
    def validation_expires_at_ms(self) -> int:
        return self.validation_completed_at_ms + _AGE - self.validation_elapsed_ms


def validate_live_profile(
    provider: ProfileLiveProvider,
    request: ProfileQueryRequest,
    *,
    utc_ms: Callable[[], int],
    monotonic_ms: Callable[[], int],
) -> (
    ValidatedLiveProfileQuery
    | LiveProfileQueryUnavailable
    | LiveProfileValidationUnavailable
):
    try:
        start = _integer(utc_ms())
        mono_start = _integer(monotonic_ms())
        if start > _MAX - _AGE:
            raise ValueError
    except Exception:
        return LiveProfileValidationUnavailable(request, "CLOCK_UNCERTAIN")
    result = query_live_profile(provider, request, LiveSelectionContext(start))
    if type(result) is LiveProfileQueryUnavailable:
        return result
    if (
        type(result) is not LiveProfileQueryResult
        or result.selection.decision_time_ms != start
    ):
        raise ProfileQueryError("INTEGRITY") from None
    try:
        mono_end = _integer(monotonic_ms())
        end = _integer(utc_ms())
    except Exception:
        return LiveProfileValidationUnavailable(request, "CLOCK_UNCERTAIN")
    if end < start or mono_end < mono_start:
        return LiveProfileValidationUnavailable(request, "CLOCK_UNCERTAIN")
    elapsed = max(end - start, mono_end - mono_start)
    if elapsed >= _AGE:
        return LiveProfileValidationUnavailable(request, "VALIDATION_EXPIRED")
    return ValidatedLiveProfileQuery(result, start, end, elapsed)


def finalize_live_profile_decision(
    request: ProfileQueryRequest,
    result: ValidatedLiveProfileQuery
    | LiveProfileQueryUnavailable
    | LiveProfileValidationUnavailable,
    *,
    observed_at_ms: int | None,
    decision_time_ms: int,
) -> ProfileDecisionContext:
    """Finalize supplied evidence only; never refresh, reselect or read a clock."""
    try:
        _integer(decision_time_ms)
        if (
            type(request) is not ProfileQueryRequest
            or request.purpose != "LIVE_QUERY"
            or request.freshness_policy_id != "utc_complete_strict_v1"
            or type(result)
            not in (
                ValidatedLiveProfileQuery,
                LiveProfileQueryUnavailable,
                LiveProfileValidationUnavailable,
            )
        ):
            raise ValueError
        if type(result) is ValidatedLiveProfileQuery:
            if result.query.request is not request or observed_at_ms is None:
                raise ValueError
            _integer(observed_at_ms)
        elif (
            observed_at_ms is not None
            or not isinstance(
                result, (LiveProfileQueryUnavailable, LiveProfileValidationUnavailable)
            )
            or result.request is not request
        ):
            raise ValueError
    except ValueError:
        raise ProfileQueryError("INTEGRITY") from None
    reason = None
    if isinstance(result, ValidatedLiveProfileQuery):
        assert observed_at_ms is not None
        available = result.query.selection.available_at_ms
        completed = result.validation_completed_at_ms
        if not available <= completed <= observed_at_ms <= decision_time_ms:
            reason = "CLOCK_UNCERTAIN"
        elif decision_time_ms >= result.validation_expires_at_ms:
            reason = "VALIDATION_EXPIRED"
        else:
            expected_end = decision_time_ms // 86400000 * 86400000
            if request.end_ms < expected_end:
                reason = "PROFILE_EXPIRED"
            elif (
                request.end_ms > expected_end
            ):  # Defensive; valid ordered clocks preclude this.
                reason = "PROFILE_NOT_READY"
        if reason is None:
            return ProfileDecisionContext(
                request,
                decision_time_ms,
                ProfileDecisionBasis.LIVE_OBSERVED,
                ProfileDecisionStatus.FRESH,
                profile=result.query.profile,
                available_at_ms=available,
                validation_checked_at_ms=completed,
                observed_at_ms=observed_at_ms,
            )
    else:
        reason = "PROFILE_NOT_READY" if result.reason == "NOT_READY" else result.reason
    status = (
        ProfileDecisionStatus.INVALID
        if reason in ("SNAPSHOT_REVOKED", "INVALID_PROFILE", "QUERY_TOO_LARGE")
        else ProfileDecisionStatus.MISSING
    )
    return ProfileDecisionContext(
        request, decision_time_ms, ProfileDecisionBasis.LIVE_OBSERVED, status, reason
    )
