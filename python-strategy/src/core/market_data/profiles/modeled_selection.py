"""Pure fixed-dataset modeled selection; never infers historical observation."""

from dataclasses import dataclass
import hashlib
import json

from .read_results import VerifiedManifestRead
from .read_types import OrderedProfileManifest, ProfileQueryRequest, _integer
from .selection import ProfileSelectionError

MODELED_DAILY_DELAY_20M_V1 = "utc_daily_delay_20m_v1"
_DAILY_DELAY_MS = 20 * 60 * 1000


@dataclass(frozen=True, slots=True)
class ModeledAvailabilityPolicy:
    """Closed policy identity for fixed-dataset replay, not observed availability."""

    policy_id: str
    daily_delay_ms: int
    semantics: str

    def __post_init__(self) -> None:
        if (
            type(self.policy_id) is not str
            or self.policy_id != MODELED_DAILY_DELAY_20M_V1
            or type(self.daily_delay_ms) is not int
            or self.daily_delay_ms != _DAILY_DELAY_MS
            or type(self.semantics) is not str
            or self.semantics != "FIXED_DATASET_REPLAY"
        ):
            raise ProfileSelectionError() from None

    @property
    def canonical_bytes(self) -> bytes:
        return json.dumps(
            {
                "schema_version": 1,
                "policy_id": self.policy_id,
                "daily_delay_ms": self.daily_delay_ms,
                "semantics": self.semantics,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


MODELED_AVAILABILITY_POLICY = ModeledAvailabilityPolicy(
    MODELED_DAILY_DELAY_20M_V1,
    _DAILY_DELAY_MS,
    "FIXED_DATASET_REPLAY",
)


@dataclass(frozen=True, slots=True)
class ModeledProfileSelection:
    manifest: OrderedProfileManifest
    decision_time_ms: int
    available_at_ms: int
    policy_id: str
    policy_digest: str

    def __post_init__(self) -> None:
        try:
            _integer(self.decision_time_ms)
            _integer(self.available_at_ms)
            if (
                type(self.manifest) is not OrderedProfileManifest
                or type(self.policy_id) is not str
                or self.policy_id != MODELED_AVAILABILITY_POLICY.policy_id
                or type(self.policy_digest) is not str
                or self.policy_digest != MODELED_AVAILABILITY_POLICY.digest
                or self.available_at_ms
                != self.manifest.days[-1].window_end_ms
                + MODELED_AVAILABILITY_POLICY.daily_delay_ms
                or self.available_at_ms > self.decision_time_ms
            ):
                raise ValueError
        except ValueError:
            raise ProfileSelectionError() from None


@dataclass(frozen=True, slots=True)
class ModeledProfileSelectionUnavailable:
    reason: str

    def __post_init__(self) -> None:
        if type(self.reason) is not str or self.reason not in (
            "PROFILE_NOT_READY",
            "SNAPSHOT_REVOKED",
        ):
            raise ProfileSelectionError() from None


def select_modeled_profile(
    request: ProfileQueryRequest,
    read: VerifiedManifestRead,
) -> ModeledProfileSelection | ModeledProfileSelectionUnavailable:
    """Select an exact pinned dataset under a named availability assumption."""
    if type(request) is not ProfileQueryRequest or type(read) is not VerifiedManifestRead:
        raise ProfileSelectionError() from None
    manifest = read.manifest
    if (
        request.purpose != "MODELED_RESEARCH"
        or request.availability_policy_id != MODELED_AVAILABILITY_POLICY.policy_id
        or request.as_of_ms is None
        or request.as_of_ms < request.end_ms
        or (
            request.revision is not None
            and (
                len(manifest.days) != 1
                or request.revision != manifest.days[0].revision
            )
        )
        or (
            request.product_id,
            request.base_grid_id,
            request.algorithm_version,
            request.start_ms,
            request.end_ms,
        )
        != (
            manifest.product_id,
            manifest.base_grid_id,
            manifest.algorithm_version,
            manifest.days[0].window_start_ms,
            manifest.days[-1].window_end_ms,
        )
    ):
        raise ProfileSelectionError() from None
    if any(day.invalidations for day in read.days):
        return ModeledProfileSelectionUnavailable("SNAPSHOT_REVOKED")
    available_at_ms = request.end_ms + MODELED_AVAILABILITY_POLICY.daily_delay_ms
    try:
        _integer(available_at_ms)
    except ValueError:
        raise ProfileSelectionError() from None
    if request.as_of_ms < available_at_ms:
        return ModeledProfileSelectionUnavailable("PROFILE_NOT_READY")
    return ModeledProfileSelection(
        manifest,
        request.as_of_ms,
        available_at_ms,
        MODELED_AVAILABILITY_POLICY.policy_id,
        MODELED_AVAILABILITY_POLICY.digest,
    )
