"""Immutable profile read contracts; no selection, freshness or grid-alignment proof."""
import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

from src.core.product_registry import validate_product_id

_MAX = (1 << 63) - 1
_DAY = 86_400_000


def _integer(value: int, minimum: int = 0) -> None:
    if type(value) is not int or not minimum <= value <= _MAX:
        raise ValueError("invalid profile read integer")


def _safe(value: str, maximum: int = 64) -> None:
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9_-]{1," + str(maximum) + "}", value) is None:
        raise ValueError("invalid profile read identifier")


def _hex(value: str) -> None:
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ValueError("invalid profile read digest")


def _scope(product: str, grid: str, algorithm: str) -> None:
    if type(product) is not str or len(product) > 64 or not product.isascii():
        raise ValueError("invalid profile read product")
    validate_product_id(product)
    _safe(grid)
    _safe(algorithm, 32)


def _window(start: int, end: int, maximum_days: int) -> None:
    _integer(start)
    _integer(end)
    if start % _DAY or end % _DAY or not _DAY <= end - start <= maximum_days * _DAY:
        raise ValueError("invalid profile read window")


@dataclass(frozen=True, slots=True)
class DailyProfileRef:
    snapshot_id: str
    revision: int
    content_sha256: str
    window_start_ms: int
    window_end_ms: int

    def __post_init__(self) -> None:
        _hex(self.snapshot_id)
        _hex(self.content_sha256)
        _integer(self.revision, 1)
        _window(self.window_start_ms, self.window_end_ms, 1)


@dataclass(frozen=True, slots=True)
class OrderedProfileManifest:
    product_id: str
    base_grid_id: str
    algorithm_version: str
    days: tuple[DailyProfileRef, ...]

    def __post_init__(self) -> None:
        _scope(self.product_id, self.base_grid_id, self.algorithm_version)
        if type(self.days) is not tuple or not 1 <= len(self.days) <= 90 or any(type(day) is not DailyProfileRef for day in self.days):
            raise ValueError("invalid profile manifest days")
        if (len({day.snapshot_id for day in self.days}) != len(self.days)
                or any(a.window_end_ms != b.window_start_ms for a, b in zip(self.days, self.days[1:]))):
            raise ValueError("invalid profile manifest order")

    @property
    def canonical_bytes(self) -> bytes:
        return json.dumps([dict(snapshot_id=d.snapshot_id, revision=d.revision, content_sha256=d.content_sha256,
                                start_ms=d.window_start_ms, end_ms=d.window_end_ms) for d in self.days],
                          sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

    @property
    def manifest_digest(self) -> str:
        return hashlib.sha256(self.canonical_bytes).hexdigest()


@dataclass(frozen=True, slots=True)
class ProfileQueryRequest:
    product_id: str
    base_grid_id: str
    output_grid_id: str
    algorithm_version: str
    start_ms: int
    end_ms: int
    purpose: Literal["LIVE_QUERY", "MODELED_RESEARCH", "RECORDED_REPLAY"]
    freshness_policy_id: str | None = None
    availability_policy_id: str | None = None
    as_of_ms: int | None = None
    revision: int | None = None
    pinned_manifest: OrderedProfileManifest | None = None

    def __post_init__(self) -> None:
        _scope(self.product_id, self.base_grid_id, self.algorithm_version)
        _safe(self.output_grid_id)
        _window(self.start_ms, self.end_ms, 90)
        for policy in (self.freshness_policy_id, self.availability_policy_id):
            if policy is not None:
                _safe(policy)
        if self.as_of_ms is not None:
            _integer(self.as_of_ms)
        if self.revision is not None:
            _integer(self.revision, 1)
            if self.end_ms - self.start_ms != _DAY or self.pinned_manifest is not None:
                raise ValueError("invalid profile revision selection")
        if type(self.purpose) is not str:
            raise ValueError("invalid profile query purpose")
        if self.purpose == "LIVE_QUERY":
            valid = (self.freshness_policy_id is not None and self.availability_policy_id is None
                     and self.as_of_ms is None and self.pinned_manifest is None)
        elif self.purpose == "MODELED_RESEARCH":
            valid = (self.freshness_policy_id is None and self.availability_policy_id is not None
                     and self.as_of_ms is not None and self.pinned_manifest is None)
        elif self.purpose == "RECORDED_REPLAY":
            manifest = self.pinned_manifest
            valid = (type(manifest) is OrderedProfileManifest and self.freshness_policy_id is None
                     and self.availability_policy_id is None and self.as_of_ms is None and self.revision is None)
            if valid and manifest is not None:
                valid = ((manifest.product_id, manifest.base_grid_id, manifest.algorithm_version,
                          manifest.days[0].window_start_ms, manifest.days[-1].window_end_ms)
                         == (self.product_id, self.base_grid_id, self.algorithm_version, self.start_ms, self.end_ms))
        else:
            valid = False
        if not valid:
            raise ValueError("invalid profile query selection")


@dataclass(frozen=True, slots=True)
class ProfileInvalidation:
    event_id: str
    snapshot_id: str
    reason_code: str
    recorded_at: datetime
    replacement_snapshot_id: str | None
    source: str

    def __post_init__(self) -> None:
        _safe(self.event_id, 128)
        _hex(self.snapshot_id)
        _safe(self.reason_code)
        _safe(self.source)
        if self.replacement_snapshot_id is not None:
            _hex(self.replacement_snapshot_id)
            if self.replacement_snapshot_id == self.snapshot_id:
                raise ValueError("invalid profile replacement")
        stamp = self.recorded_at
        try:
            if type(stamp) is not datetime or stamp.utcoffset() != timedelta(0) or stamp.microsecond % 1000:
                raise ValueError
            object.__setattr__(self, "recorded_at", stamp.replace(tzinfo=timezone.utc))
        except Exception:
            raise ValueError("invalid profile invalidation timestamp") from None
