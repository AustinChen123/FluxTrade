"""Strict LIVE header window policy only, not content verification or strategy readiness."""

from dataclasses import dataclass
from datetime import datetime, timezone

from .read_types import OrderedProfileManifest, ProfileQueryRequest, _integer
from .selection import ProfileCandidateBatch, ProfileSelectionError

_DAY = 86400000
_POLICY = "utc_complete_strict_v1"


@dataclass(frozen=True, slots=True)
class LiveSelectionContext:
    decision_time_ms: int

    def __post_init__(self) -> None:
        try:
            _integer(self.decision_time_ms)
        except ValueError:
            raise ProfileSelectionError() from None


@dataclass(frozen=True, slots=True)
class LiveProfileSelection:
    """Selected versions' source availability upper bound, not observation or FRESH."""

    manifest: OrderedProfileManifest
    decision_time_ms: int
    available_at_ms: int
    policy: str = _POLICY

    def __post_init__(self) -> None:
        LiveSelectionContext(self.decision_time_ms)
        try:
            _integer(self.available_at_ms)
        except ValueError:
            raise ProfileSelectionError() from None
        if (
            type(self.manifest) is not OrderedProfileManifest
            or type(self.policy) is not str
            or self.policy != _POLICY
            or self.available_at_ms < self.manifest.days[-1].window_end_ms
            or self.manifest.days[-1].window_end_ms
            != self.decision_time_ms // _DAY * _DAY
        ):
            raise ProfileSelectionError() from None


@dataclass(frozen=True, slots=True)
class LiveProfileSelectionUnavailable:
    reason: str

    def __post_init__(self) -> None:
        if type(self.reason) is not str or self.reason not in (
            "NOT_READY",
            "PROFILE_EXPIRED",
            "SNAPSHOT_REVOKED",
        ):
            raise ProfileSelectionError() from None


def select_live_profile(
    request: ProfileQueryRequest,
    batch: ProfileCandidateBatch,
    context: LiveSelectionContext,
) -> LiveProfileSelection | LiveProfileSelectionUnavailable:
    if (
        type(request) is not ProfileQueryRequest
        or type(batch) is not ProfileCandidateBatch
        or type(context) is not LiveSelectionContext
    ):
        raise ProfileSelectionError() from None
    if (
        request.purpose != "LIVE_QUERY"
        or type(request.freshness_policy_id) is not str
        or request.freshness_policy_id != _POLICY
        or (request.product_id, request.base_grid_id, request.algorithm_version)
        != (batch.product_id, batch.base_grid_id, batch.algorithm_version)
    ):
        raise ProfileSelectionError() from None
    expected_end = context.decision_time_ms // _DAY * _DAY
    if request.end_ms != expected_end:
        return LiveProfileSelectionUnavailable(
            "NOT_READY" if request.end_ms > expected_end else "PROFILE_EXPIRED"
        )
    selected = []
    missing = revoked = False
    for start in range(request.start_ms, request.end_ms, _DAY):
        eligible = []
        observed = False
        for candidate in batch.candidates:
            ref = candidate.ref
            if (ref.window_start_ms, ref.window_end_ms) != (start, start + _DAY) or (
                request.revision is not None and ref.revision != request.revision
            ):
                continue
            if candidate.availability_basis != "OBSERVED":
                continue
            if candidate.source_available_at is None:
                raise ProfileSelectionError() from None
            observed = True
            if not candidate.revoked:
                eligible.append(candidate)
        if not observed:
            missing = True
        elif not eligible:
            revoked = True
        else:
            selected.append(max(eligible, key=lambda candidate: candidate.ref.revision))
    if missing or revoked:
        return LiveProfileSelectionUnavailable(
            "NOT_READY" if missing else "SNAPSHOT_REVOKED"
        )
    manifest = OrderedProfileManifest(
        batch.product_id,
        batch.base_grid_id,
        batch.algorithm_version,
        tuple(candidate.ref for candidate in selected),
    )
    available = []
    for candidate in selected:
        assert candidate.source_available_at is not None
        delta = candidate.source_available_at - datetime(
            1970, 1, 1, tzinfo=timezone.utc
        )
        micros = (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds
        stamp = -(-micros // 1000)
        try:
            _integer(stamp)
        except ValueError:
            raise ProfileSelectionError() from None
        available.append(stamp)
    return LiveProfileSelection(manifest, context.decision_time_ms, max(available))
