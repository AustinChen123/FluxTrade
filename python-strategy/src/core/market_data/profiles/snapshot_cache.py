"""In-process last-good snapshots; callers own scheduling and request lifetime."""

from dataclasses import dataclass
from threading import Lock
from typing import Callable, Protocol

from .decision_context import ProfileDecisionContext, StrategyMarketDataContext
from .live_selection import plan_live_profile_requests
from .live_query import LiveProfileQueryUnavailable, ProfileQueryError
from .live_validation import (
    LiveProfileValidationUnavailable,
    ValidatedLiveProfileQuery,
    finalize_live_profile_decision,
    assemble_live_profile_decisions,
)
from .observed_snapshot import ObservedProfileSnapshot
from .read_types import ProfileQueryRequest
from .requirements import ProfileRequirement
from .wire import ProfileWireEvidence

_State = (
    ObservedProfileSnapshot
    | LiveProfileQueryUnavailable
    | LiveProfileValidationUnavailable
)


class ProfileSnapshotFetcher(Protocol):
    def fetch(
        self, request: ProfileQueryRequest
    ) -> ProfileWireEvidence | LiveProfileQueryUnavailable: ...


@dataclass(frozen=True, slots=True)
class ProfileRefreshResult:
    applied: bool
    outcome: str

    def __post_init__(self) -> None:
        if (
            type(self.applied) is not bool
            or type(self.outcome) is not str
            or self.outcome
            not in (
                "SUCCESS",
                "NOT_READY",
                "PROFILE_EXPIRED",
                "SNAPSHOT_REVOKED",
                "QUERY_TOO_LARGE",
                "INVALID_PROFILE",
                "BACKEND_UNAVAILABLE",
                "CLOCK_UNCERTAIN",
            )
        ):
            raise ProfileQueryError("INTEGRITY") from None


@dataclass(slots=True)
class _Entry:
    request: ProfileQueryRequest
    generation: int = 0
    success_generation: int = 0
    revoked_validation: ValidatedLiveProfileQuery | None = None
    validation_high_water: ValidatedLiveProfileQuery | None = None
    state: _State | None = None


class ProfileSnapshotCache:
    """No threads or retries. Retained success never receives a new lease.

    Request objects are owner-scoped and retained for this cache's lifetime.
    Equal copies are programming errors, not interchangeable cache keys.
    """

    def __init__(
        self,
        client: ProfileSnapshotFetcher,
        *,
        utc_ms: Callable[[], int],
        monotonic_ms: Callable[[], int],
    ) -> None:
        if (
            not callable(getattr(client, "fetch", None))
            or not callable(utc_ms)
            or not callable(monotonic_ms)
        ):
            raise ProfileQueryError("INTEGRITY") from None
        self._client, self._utc, self._mono = client, utc_ms, monotonic_ms
        self._lock = Lock()
        self._entries: dict[ProfileQueryRequest, _Entry] = {}

    def live_requests(
        self,
        requirements: tuple[ProfileRequirement, ...],
        *,
        selection_time_ms: int,
    ) -> tuple[ProfileQueryRequest, ...]:
        """Retain exact request objects shared by refresh and decision paths."""
        planned = plan_live_profile_requests(
            requirements, selection_time_ms=selection_time_ms
        )
        with self._lock:
            retained = []
            for request in planned:
                entry = self._entries.get(request)
                if entry is None:
                    entry = _Entry(request)
                    self._entries[request] = entry
                retained.append(entry.request)
            return tuple(retained)

    def _entry(self, request: ProfileQueryRequest) -> _Entry:
        # Only called under the lock. Validation precedes hashing caller input.
        if (
            type(request) is not ProfileQueryRequest
            or request.purpose != "LIVE_QUERY"
            or request.freshness_policy_id != "utc_complete_strict_v1"
        ):
            raise ProfileQueryError("INTEGRITY") from None
        entry = self._entries.get(request)
        if entry is not None and entry.request is not request:
            raise ProfileQueryError("INTEGRITY") from None
        return entry if entry is not None else _Entry(request)

    def refresh(self, request: ProfileQueryRequest) -> ProfileRefreshResult:
        with self._lock:
            entry = self._entry(request)
            self._entries[request] = entry
            entry.generation += 1
            generation = entry.generation
        result = self._client.fetch(request)
        state: _State
        if type(result) is ProfileWireEvidence:
            if result.validated.query.request is not request:
                raise ProfileQueryError("INTEGRITY") from None
            try:
                state = ObservedProfileSnapshot(result, self._utc(), self._mono())
            except Exception:
                state = LiveProfileValidationUnavailable(request, "CLOCK_UNCERTAIN")
            outcome = (
                "SUCCESS"
                if type(state) is ObservedProfileSnapshot
                else "CLOCK_UNCERTAIN"
            )
        elif type(result) is LiveProfileQueryUnavailable and result.request is request:
            state, outcome = result, result.reason
        else:
            raise ProfileQueryError("INTEGRITY") from None
        with self._lock:
            if entry.generation != generation:
                if (
                    outcome == "SNAPSHOT_REVOKED"
                    and isinstance(entry.state, ObservedProfileSnapshot)
                    and entry.success_generation <= generation
                ):
                    entry.revoked_validation = entry.state.evidence.validated
                    entry.state = state
                    return ProfileRefreshResult(True, outcome)
                return ProfileRefreshResult(False, outcome)
            if isinstance(state, ObservedProfileSnapshot):
                incoming = state.evidence.validated
                high_water = entry.validation_high_water
                if high_water is not None and (
                    incoming.validation_started_at_ms
                    < high_water.validation_started_at_ms
                    or (
                        incoming.validation_started_at_ms
                        == high_water.validation_started_at_ms
                        and incoming != high_water
                    )
                ):
                    return ProfileRefreshResult(False, outcome)
                if entry.revoked_validation == state.evidence.validated:
                    return ProfileRefreshResult(False, outcome)
                if (
                    isinstance(entry.state, ObservedProfileSnapshot)
                    and entry.state.evidence.validated == state.evidence.validated
                ):
                    return ProfileRefreshResult(True, outcome)
                entry.success_generation = generation
                entry.validation_high_water = incoming
                entry.revoked_validation = None
            elif outcome == "SNAPSHOT_REVOKED" and isinstance(
                entry.state, ObservedProfileSnapshot
            ):
                entry.revoked_validation = entry.state.evidence.validated
            elif (
                isinstance(entry.state, LiveProfileQueryUnavailable)
                and entry.state.reason == "SNAPSHOT_REVOKED"
            ):
                return ProfileRefreshResult(True, outcome)
            if not isinstance(entry.state, ObservedProfileSnapshot) or outcome in (
                "SUCCESS",
                "SNAPSHOT_REVOKED",
            ):
                entry.state = state
            return ProfileRefreshResult(True, outcome)

    def decision(
        self,
        request: ProfileQueryRequest,
        *,
        decision_time_ms: int,
        current_monotonic_ms: int,
    ) -> ProfileDecisionContext:
        if (
            type(current_monotonic_ms) is not int
            or not 0 <= current_monotonic_ms <= 2**63 - 1
            or type(decision_time_ms) is not int
            or not 0 <= decision_time_ms <= 2**63 - 1
        ):
            raise ProfileQueryError("INTEGRITY") from None
        with self._lock:
            entry = self._entry(request)
            self._entries[request] = entry
            state = entry.state
        return self._finalize(request, state, decision_time_ms, current_monotonic_ms)

    def _plan(self, requests: tuple[ProfileQueryRequest, ...]) -> tuple[_Entry, ...]:
        # Under the lock: validate the entire plan before any fetch or replacement.
        if type(requests) is not tuple:
            raise ProfileQueryError("INTEGRITY") from None
        entries = tuple(self._entry(request) for request in requests)
        if len(set(requests)) != len(requests) or len({r.end_ms for r in requests}) > 1:
            raise ProfileQueryError("INTEGRITY") from None
        return entries

    def refresh_many(
        self, requests: tuple[ProfileQueryRequest, ...]
    ) -> tuple[ProfileRefreshResult, ...]:
        """Sequential fetches outside callbacks; errors propagate, no batch retry.

        Each completed refresh is committed independently; no partial result tuple
        is returned if a programming error interrupts the batch.
        """
        with self._lock:
            entries = self._plan(requests)
            for entry in entries:
                self._entries[entry.request] = entry
        return tuple(self.refresh(request) for request in requests)

    def decision_many(
        self,
        requests: tuple[ProfileQueryRequest, ...],
        *,
        decision_time_ms: int,
        current_monotonic_ms: int,
    ) -> StrategyMarketDataContext:
        """Copy one atomic cache view, then assemble exact all-or-error coverage."""
        for value in (decision_time_ms, current_monotonic_ms):
            if type(value) is not int or not 0 <= value <= 2**63 - 1:
                raise ProfileQueryError("INTEGRITY") from None
        with self._lock:
            entries = self._plan(requests)
            for entry in entries:
                self._entries[entry.request] = entry
            states = tuple(entry.state for entry in entries)
        decisions = tuple(
            self._finalize(request, state, decision_time_ms, current_monotonic_ms)
            for request, state in zip(requests, states, strict=True)
        )
        return assemble_live_profile_decisions(
            requests, decisions, decision_time_ms=decision_time_ms
        )

    @staticmethod
    def _finalize(
        request: ProfileQueryRequest,
        state: _State | None,
        decision_time_ms: int,
        current_monotonic_ms: int,
    ) -> ProfileDecisionContext:
        if isinstance(state, ObservedProfileSnapshot):
            return state.finalize(
                decision_time_ms=decision_time_ms,
                current_monotonic_ms=current_monotonic_ms,
            )
        return finalize_live_profile_decision(
            request,
            state
            if state is not None
            else LiveProfileQueryUnavailable(request, "NOT_READY"),
            observed_at_ms=None,
            decision_time_ms=decision_time_ms,
        )
