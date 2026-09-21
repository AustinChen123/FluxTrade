"""Immutable reader results and fixed reader budgets; no I/O or selection policy."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

from .publication import VerifiedProfilePublication
from .read_types import DailyProfileRef, OrderedProfileManifest, ProfileInvalidation

MAX_CANDIDATES = 1000
MAX_MANIFEST_DAYS = 90
MAX_BINS = 100000
MAX_INVALIDATIONS_PER_SNAPSHOT = 1000
MAX_INVALIDATIONS_PER_OPERATION = 10000
MAX_METADATA_TEXT_BYTES = 131072
MAX_NUMERIC_TEXT_BYTES = 64
MAX_OPERATION_BYTES = 32 * 1024 * 1024
CANDIDATE_ACCOUNTING_BYTES = 1024
HEADER_ACCOUNTING_BYTES = 1024
BIN_ACCOUNTING_BYTES = 256
EVENT_ACCOUNTING_BYTES = 512


def _utc(value: datetime) -> datetime:
    try:
        if type(value) is not datetime or value.utcoffset() != timedelta(0):
            raise ValueError
        return value.replace(tzinfo=timezone.utc)
    except Exception:
        raise ValueError("invalid profile read timestamp") from None


@dataclass(frozen=True, slots=True)
class ProfileCandidate:
    """Header-only candidate; not proof of content integrity or source completeness."""
    ref: DailyProfileRef
    computed_at: datetime
    published_at: datetime
    source_available_at: datetime | None
    availability_basis: Literal["OBSERVED", "MODELED"]
    revoked: bool

    def __post_init__(self) -> None:
        if (type(self.ref) is not DailyProfileRef or type(self.revoked) is not bool
                or type(self.availability_basis) is not str or self.availability_basis not in ("OBSERVED", "MODELED")):
            raise ValueError("invalid profile candidate")
        object.__setattr__(self, "computed_at", _utc(self.computed_at))
        object.__setattr__(self, "published_at", _utc(self.published_at))
        if self.source_available_at is not None:
            object.__setattr__(self, "source_available_at", _utc(self.source_available_at))


@dataclass(frozen=True, slots=True)
class VerifiedDailyRead:
    ref: DailyProfileRef
    computed_at: datetime
    published_at: datetime
    publication: VerifiedProfilePublication
    invalidations: tuple[ProfileInvalidation, ...]

    def __post_init__(self) -> None:
        if type(self.ref) is not DailyProfileRef or type(self.publication) is not VerifiedProfilePublication:
            raise ValueError("invalid verified daily read")
        content = self.publication.content
        if (self.ref.snapshot_id != content.content_sha256 or self.ref.content_sha256 != content.content_sha256
                or (self.ref.window_start_ms, self.ref.window_end_ms) != (content.window_start_ms, content.window_end_ms)):
            raise ValueError("inconsistent verified daily read")
        if type(self.invalidations) is not tuple or any(type(event) is not ProfileInvalidation
                or event.snapshot_id != self.ref.snapshot_id for event in self.invalidations):
            raise ValueError("invalid daily read invalidations")
        object.__setattr__(self, "computed_at", _utc(self.computed_at))
        object.__setattr__(self, "published_at", _utc(self.published_at))


@dataclass(frozen=True, slots=True)
class VerifiedManifestRead:
    manifest: OrderedProfileManifest
    days: tuple[VerifiedDailyRead, ...]

    def __post_init__(self) -> None:
        if (type(self.manifest) is not OrderedProfileManifest or type(self.days) is not tuple
                or len(self.days) != len(self.manifest.days) or any(type(day) is not VerifiedDailyRead for day in self.days)):
            raise ValueError("invalid verified manifest read")
        for ref, day in zip(self.manifest.days, self.days):
            content = day.publication.content
            if (day.ref != ref or (content.product_id, content.grid_id, content.algorithm_version)
                    != (self.manifest.product_id, self.manifest.base_grid_id, self.manifest.algorithm_version)):
                raise ValueError("inconsistent verified manifest read")
