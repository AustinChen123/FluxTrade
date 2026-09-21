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

_MAX = (1 << 63) - 1
_AGE = 300000


def _integer(value: int) -> int:
    if type(value) is not int or not 0 <= value <= _MAX:
        raise ValueError("invalid validation clock")
    return value


@dataclass(frozen=True, slots=True)
class LiveProfileValidationUnavailable:
    reason: str

    def __post_init__(self) -> None:
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
        return LiveProfileValidationUnavailable("CLOCK_UNCERTAIN")
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
        return LiveProfileValidationUnavailable("CLOCK_UNCERTAIN")
    if end < start or mono_end < mono_start:
        return LiveProfileValidationUnavailable("CLOCK_UNCERTAIN")
    elapsed = max(end - start, mono_end - mono_start)
    if elapsed >= _AGE:
        return LiveProfileValidationUnavailable("VALIDATION_EXPIRED")
    return ValidatedLiveProfileQuery(result, start, end, elapsed)
