"""Recorded header selection only; no content verification or freshness policy."""

from dataclasses import dataclass

from .read_results import MAX_CANDIDATES, ProfileCandidate
from .read_types import OrderedProfileManifest, ProfileQueryRequest, _scope


class ProfileSelectionError(ValueError):
    def __init__(self, reason: str = "INVALID") -> None:
        if type(reason) is not str or reason != "INVALID":
            raise ValueError("PROFILE_SELECTION_INVALID") from None
        self.reason = reason
        super().__init__("PROFILE_SELECTION_INVALID")


@dataclass(frozen=True, slots=True)
class ProfileCandidateBatch:
    """Header-only: producer proves scope; this is not content verification."""

    product_id: str
    base_grid_id: str
    algorithm_version: str
    candidates: tuple[ProfileCandidate, ...]

    def __post_init__(self) -> None:
        try:
            _scope(self.product_id, self.base_grid_id, self.algorithm_version)
            if (
                type(self.candidates) is not tuple
                or len(self.candidates) > MAX_CANDIDATES
            ):
                raise ValueError
            snapshots, revisions = set(), set()
            for candidate in self.candidates:
                if type(candidate) is not ProfileCandidate:
                    raise ValueError
                ref = candidate.ref
                key = (ref.window_start_ms, ref.window_end_ms, ref.revision)
                if ref.snapshot_id in snapshots or key in revisions:
                    raise ValueError
                snapshots.add(ref.snapshot_id)
                revisions.add(key)
        except ValueError:
            raise ProfileSelectionError() from None


@dataclass(frozen=True, slots=True)
class RecordedProfileSelection:
    manifest: OrderedProfileManifest
    revoked_snapshot_ids: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            type(self.manifest) is not OrderedProfileManifest
            or type(self.revoked_snapshot_ids) is not tuple
        ):
            raise ProfileSelectionError() from None
        ids = self.revoked_snapshot_ids
        if any(type(value) is not str for value in ids) or len(set(ids)) != len(ids):
            raise ProfileSelectionError() from None
        if (
            tuple(
                ref.snapshot_id for ref in self.manifest.days if ref.snapshot_id in ids
            )
            != ids
        ):
            raise ProfileSelectionError() from None


@dataclass(frozen=True, slots=True)
class ProfileSelectionUnavailable:
    reason: str = "MISSING"

    def __post_init__(self) -> None:
        if type(self.reason) is not str or self.reason != "MISSING":
            raise ProfileSelectionError() from None


def select_recorded_profile(
    request: ProfileQueryRequest, batch: ProfileCandidateBatch
) -> RecordedProfileSelection | ProfileSelectionUnavailable:
    if (
        type(request) is not ProfileQueryRequest
        or type(batch) is not ProfileCandidateBatch
    ):
        raise ProfileSelectionError() from None
    if request.purpose != "RECORDED_REPLAY" or (
        request.product_id,
        request.base_grid_id,
        request.algorithm_version,
    ) != (batch.product_id, batch.base_grid_id, batch.algorithm_version):
        raise ProfileSelectionError() from None
    manifest = request.pinned_manifest
    if type(manifest) is not OrderedProfileManifest:
        raise ProfileSelectionError() from None
    candidates = {
        candidate.ref.snapshot_id: candidate for candidate in batch.candidates
    }
    missing = False
    revoked = []
    for ref in manifest.days:
        candidate = candidates.get(ref.snapshot_id)
        if candidate is None:
            missing = True
        elif candidate.ref != ref:
            raise ProfileSelectionError() from None
        elif candidate.revoked:
            revoked.append(ref.snapshot_id)
    return (
        ProfileSelectionUnavailable()
        if missing
        else RecordedProfileSelection(manifest, tuple(revoked))
    )
