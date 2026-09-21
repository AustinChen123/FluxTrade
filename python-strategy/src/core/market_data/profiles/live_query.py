"""Pinned LIVE read orchestration, not freshness/lease or strategy-ready evidence."""

from dataclasses import dataclass
from typing import Protocol

from .composite import ProfileCompositionError, compose_profile
from .composite_types import CompositeProfile
from .grid import InvalidProfileGrid, UnsupportedProfileGrid, resolve_profile_grid
from .live_selection import (
    LiveProfileSelection,
    LiveProfileSelectionUnavailable,
    LiveSelectionContext,
    select_live_profile,
)
from .read_results import ProfileCandidate, VerifiedManifestRead
from .read_types import OrderedProfileManifest, ProfileQueryRequest
from .selection import ProfileCandidateBatch, ProfileSelectionError


class ProfileLiveProvider(Protocol):
    def list_candidates(
        self,
        *,
        product_id: str,
        base_grid_id: str,
        algorithm_version: str,
        start_ms: int,
        end_ms: int,
        revision: int | None = None,
    ) -> tuple[ProfileCandidate, ...]: ...
    def get_manifest(
        self, manifest: OrderedProfileManifest
    ) -> VerifiedManifestRead | None: ...


class ProfileQueryError(ValueError):
    def __init__(self, reason: str) -> None:
        if type(reason) is not str or reason not in ("INVALID", "INTEGRITY"):
            raise ValueError("PROFILE_QUERY_INVALID") from None
        self.reason = reason
        super().__init__("PROFILE_QUERY_" + reason)


@dataclass(frozen=True, slots=True)
class LiveProfileQueryResult:
    selection: LiveProfileSelection
    profile: CompositeProfile

    def __post_init__(self) -> None:
        if (
            type(self.selection) is not LiveProfileSelection
            or type(self.profile) is not CompositeProfile
            or self.selection.manifest != self.profile.manifest
        ):
            raise ProfileQueryError("INTEGRITY") from None


@dataclass(frozen=True, slots=True)
class LiveProfileQueryUnavailable:
    reason: str

    def __post_init__(self) -> None:
        if type(self.reason) is not str or self.reason not in (
            "NOT_READY",
            "PROFILE_EXPIRED",
            "SNAPSHOT_REVOKED",
            "QUERY_TOO_LARGE",
            "INVALID_PROFILE",
            "BACKEND_UNAVAILABLE",
        ):
            raise ProfileQueryError("INTEGRITY") from None


_COMPOSITION = dict(
    REVOKED="SNAPSHOT_REVOKED",
    TOO_LARGE="QUERY_TOO_LARGE",
    ARITHMETIC="INVALID_PROFILE",
    INTEGRITY="INVALID_PROFILE",
    NATIVE_UNAVAILABLE="BACKEND_UNAVAILABLE",
    NATIVE_FAILURE="BACKEND_UNAVAILABLE",
)


def query_live_profile(
    provider: ProfileLiveProvider,
    request: ProfileQueryRequest,
    context: LiveSelectionContext,
) -> LiveProfileQueryResult | LiveProfileQueryUnavailable:
    if (
        type(request) is not ProfileQueryRequest
        or type(context) is not LiveSelectionContext
        or request.purpose != "LIVE_QUERY"
        or request.freshness_policy_id != "utc_complete_strict_v1"
    ):
        raise ProfileQueryError("INVALID") from None
    try:
        base = resolve_profile_grid(
            request.product_id, request.base_grid_id, request.algorithm_version
        )
        output = resolve_profile_grid(
            request.product_id, request.output_grid_id, request.algorithm_version
        )
        if base != output:
            raise ProfileQueryError("INVALID") from None
    except (InvalidProfileGrid, UnsupportedProfileGrid):
        raise ProfileQueryError("INVALID") from None
    rows = provider.list_candidates(
        product_id=request.product_id,
        base_grid_id=request.base_grid_id,
        algorithm_version=request.algorithm_version,
        start_ms=request.start_ms,
        end_ms=request.end_ms,
        revision=request.revision,
    )
    try:
        batch = ProfileCandidateBatch(
            request.product_id, request.base_grid_id, request.algorithm_version, rows
        )
        selection = select_live_profile(request, batch, context)
    except ProfileSelectionError:
        raise ProfileQueryError("INTEGRITY") from None
    if isinstance(selection, LiveProfileSelectionUnavailable):
        return LiveProfileQueryUnavailable(selection.reason)
    read = provider.get_manifest(selection.manifest)
    if read is None:
        return LiveProfileQueryUnavailable("NOT_READY")
    if type(read) is not VerifiedManifestRead or read.manifest != selection.manifest:
        raise ProfileQueryError("INTEGRITY") from None
    try:
        profile = compose_profile(read)
    except ProfileCompositionError as error:
        reason = _COMPOSITION.get(error.reason)
        if reason is None:
            raise ProfileQueryError("INTEGRITY") from None
        return LiveProfileQueryUnavailable(reason)
    return LiveProfileQueryResult(selection, profile)
