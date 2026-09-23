"""Immutable precomposed profile windows for modeled runner decisions."""

from dataclasses import dataclass
import hashlib
import json
from types import MappingProxyType
from typing import Mapping

from .composite import compose_profile
from .composite_types import CompositeProfile
from .decision_context import (
    ProfileDecisionBasis,
    ProfileDecisionContext,
    ProfileDecisionStatus,
    StrategyMarketDataContext,
)
from .modeled_selection import (
    MODELED_AVAILABILITY_POLICY,
    ModeledProfileSelection,
    ModeledProfileSelectionUnavailable,
    select_modeled_profile,
)
from .read_results import VerifiedManifestRead
from .read_types import ProfileQueryRequest, _integer
from .requirements import ProfileRequirement

_DAY = 86_400_000
_MAX_WINDOWS = 1000
_WindowKey = tuple[str, str, str, int, int]


class PreloadedModeledProfileError(ValueError):
    def __init__(self) -> None:
        super().__init__("PRELOADED_MODELED_PROFILE_INVALID")


@dataclass(frozen=True, slots=True)
class _Window:
    read: VerifiedManifestRead
    profile: CompositeProfile | None


def _key(read: VerifiedManifestRead) -> _WindowKey:
    manifest = read.manifest
    return (
        manifest.product_id,
        manifest.base_grid_id,
        manifest.algorithm_version,
        manifest.days[0].window_start_ms,
        manifest.days[-1].window_end_ms,
    )


def _invalidations(read: VerifiedManifestRead) -> list[dict[str, object]]:
    rows = []
    for day in read.days:
        for event in day.invalidations:
            rows.append(
                {
                    "event_id": event.event_id,
                    "snapshot_id": event.snapshot_id,
                    "reason_code": event.reason_code,
                    "recorded_at": event.recorded_at.isoformat(),
                    "replacement_snapshot_id": event.replacement_snapshot_id,
                    "source": event.source,
                }
            )
    return sorted(rows, key=lambda row: json.dumps(row, sort_keys=True))


def _identity(window: _Window) -> dict[str, object]:
    read, profile = window.read, window.profile
    manifest = read.manifest
    return {
        "scope": list(_key(read)[:3]),
        "window": list(_key(read)[3:]),
        "manifest": json.loads(manifest.canonical_bytes),
        "manifest_digest": manifest.manifest_digest,
        "invalidations": _invalidations(read),
        "merge_algorithm_version": (
            "aligned-sum-v1" if profile is None else profile.merge_algorithm_version
        ),
        "composite_id": None if profile is None else profile.composite_id,
    }


class PreloadedModeledProfileProvider:
    """Exact-window provider; construction performs all composition work."""

    __slots__ = ("_windows", "_canonical_bytes", "_dataset_digest")

    def __init__(self, reads: tuple[VerifiedManifestRead, ...]) -> None:
        if (
            type(reads) is not tuple
            or len(reads) > _MAX_WINDOWS
            or any(type(read) is not VerifiedManifestRead for read in reads)
        ):
            raise PreloadedModeledProfileError() from None
        staged: list[tuple[_WindowKey, VerifiedManifestRead]] = []
        keys: set[_WindowKey] = set()
        for read in reads:
            key = _key(read)
            if key in keys:
                raise PreloadedModeledProfileError() from None
            keys.add(key)
            staged.append((key, read))
        windows = []
        for key, read in staged:
            revoked = any(day.invalidations for day in read.days)
            profile = None if revoked else compose_profile(read)
            if not revoked and (
                type(profile) is not CompositeProfile or profile.manifest != read.manifest
            ):
                raise PreloadedModeledProfileError() from None
            windows.append((key, _Window(read, profile)))
        windows.sort(key=lambda item: item[0])
        projection = {
            "schema_version": 1,
            "windows": [_identity(window) for _, window in windows],
        }
        canonical = json.dumps(
            projection, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        self._windows: Mapping[_WindowKey, _Window] = MappingProxyType(dict(windows))
        self._canonical_bytes = canonical
        self._dataset_digest = hashlib.sha256(canonical).hexdigest()

    @property
    def canonical_bytes(self) -> bytes:
        return self._canonical_bytes

    @property
    def dataset_digest(self) -> str:
        return self._dataset_digest

    def context_for(
        self,
        requirements: tuple[ProfileRequirement, ...],
        *,
        decision_time_ms: int,
        availability_policy_id: str,
    ) -> StrategyMarketDataContext:
        try:
            _integer(decision_time_ms)
            if (
                type(availability_policy_id) is not str
                or availability_policy_id != MODELED_AVAILABILITY_POLICY.policy_id
                or type(requirements) is not tuple
                or any(type(value) is not ProfileRequirement for value in requirements)
                or any(
                    value.freshness_policy_id != "utc_complete_strict_v1"
                    for value in requirements
                )
                or len({value.canonical_bytes for value in requirements}) != len(requirements)
            ):
                raise ValueError
            end = decision_time_ms // _DAY * _DAY
            if any(value.window_days * _DAY > end for value in requirements):
                raise ValueError
        except ValueError:
            raise PreloadedModeledProfileError() from None
        decisions = []
        for requirement in requirements:
            start = end - requirement.window_days * _DAY
            request = ProfileQueryRequest(
                requirement.product_id,
                requirement.base_grid_id,
                requirement.output_grid_id,
                requirement.algorithm_version,
                start,
                end,
                "MODELED_RESEARCH",
                availability_policy_id=availability_policy_id,
                as_of_ms=decision_time_ms,
            )
            window = self._windows.get(
                (request.product_id, request.base_grid_id, request.algorithm_version, start, end)
            )
            decisions.append(self._decision(request, window))
        return StrategyMarketDataContext(decision_time_ms, tuple(decisions))

    @staticmethod
    def _decision(
        request: ProfileQueryRequest, window: _Window | None
    ) -> ProfileDecisionContext:
        decision_time_ms = request.as_of_ms
        if type(decision_time_ms) is not int:
            raise PreloadedModeledProfileError() from None
        if window is None:
            return ProfileDecisionContext(
                request, decision_time_ms, ProfileDecisionBasis.MODELED,
                ProfileDecisionStatus.MISSING, "PROFILE_NOT_READY")
        selection = select_modeled_profile(request, window.read)
        if isinstance(selection, ModeledProfileSelectionUnavailable):
            if selection.reason == "SNAPSHOT_REVOKED":
                return ProfileDecisionContext(
                    request, decision_time_ms, ProfileDecisionBasis.MODELED,
                    ProfileDecisionStatus.INVALID, selection.reason)
        profile = window.profile
        if profile is None or request.output_grid_id != profile.output_grid.grid_id:
            return ProfileDecisionContext(
                request, decision_time_ms, ProfileDecisionBasis.MODELED,
                ProfileDecisionStatus.INVALID, "INVALID_PROFILE")
        if isinstance(selection, ModeledProfileSelectionUnavailable):
            return ProfileDecisionContext(
                request, decision_time_ms, ProfileDecisionBasis.MODELED,
                ProfileDecisionStatus.MISSING, selection.reason)
        assert isinstance(selection, ModeledProfileSelection)
        return ProfileDecisionContext(
            request, decision_time_ms, ProfileDecisionBasis.MODELED,
            ProfileDecisionStatus.FRESH, profile=profile,
            available_at_ms=selection.available_at_ms)
