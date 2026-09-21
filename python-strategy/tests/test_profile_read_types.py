"""Pure read contracts, not data availability or PostgreSQL acceptance."""
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone, tzinfo
from itertools import product
from typing import Any

import pytest

from src.core.market_data.profiles.read_types import (
    DailyProfileRef, OrderedProfileManifest, ProfileInvalidation, ProfileQueryRequest,
)

DAY = 86400000
MAX = (1 << 63) - 1


def ref(index: int = 0) -> DailyProfileRef:
    return DailyProfileRef(f"{index + 1:064x}", 1, "b" * 64, index * DAY, (index + 1) * DAY)


def manifest(count: int = 1) -> OrderedProfileManifest:
    return OrderedProfileManifest("BINANCE:BTCUSDT-SPOT", "base", "vp-v1", tuple(ref(i) for i in range(count)))


def query(count: int = 1, **kwargs: Any) -> ProfileQueryRequest:
    return ProfileQueryRequest("BINANCE:BTCUSDT-SPOT", "base", "coarse", "vp-v1", 0, count * DAY, **kwargs)


def test_manifest_golden_excludes_scope_and_keeps_snapshot_content_distinct() -> None:
    value = OrderedProfileManifest("BINANCE:BTCUSDT-SPOT", "g1", "vp-v1",
                                   (DailyProfileRef("a" * 64, 1, "b" * 64, 0, DAY),))
    expected = ('[{"content_sha256":"' + "b" * 64 + '","end_ms":86400000,"revision":1,"snapshot_id":"'
                + "a" * 64 + '","start_ms":0}]').encode()
    assert value.canonical_bytes == expected
    assert value.manifest_digest == "40064980a0bb153e3c77421ed2a71fbaffd348401389b8ba3e232c439ecb280d"
    assert replace(value, base_grid_id="other").manifest_digest == value.manifest_digest
    assert replace(value, days=(replace(value.days[0], revision=2),)).manifest_digest != value.manifest_digest
    for obj, field in ((value, "days"), (value.days[0], "revision"),
                       (query(purpose="LIVE_QUERY", freshness_policy_id="fresh"), "purpose")):
        with pytest.raises(FrozenInstanceError):
            setattr(obj, field, None)
        assert not hasattr(obj, "__dict__")


@pytest.mark.parametrize("count", [1, 7, 30, 90])
def test_complete_days_are_representable_without_wall_clock(count: int) -> None:
    value = manifest(count)
    assert query(count, purpose="RECORDED_REPLAY", pinned_manifest=value).pinned_manifest == value
    assert query(count, purpose="LIVE_QUERY", freshness_policy_id="fresh").end_ms == count * DAY
    assert query(count, purpose="MODELED_RESEARCH", availability_policy_id="model", as_of_ms=0).as_of_ms == 0
    last = MAX // DAY * DAY
    future = replace(query(purpose="LIVE_QUERY", freshness_policy_id="fresh"), start_ms=last - DAY, end_ms=last)
    assert future.end_ms == last


@pytest.mark.parametrize("field,value", [
    ("snapshot_id", "A" * 64), ("content_sha256", "b" * 63), ("revision", True),
    ("revision", 0), ("revision", MAX + 1), ("window_start_ms", -DAY),
    ("window_start_ms", False), ("window_end_ms", MAX + 1), ("window_end_ms", DAY + 1),
    ("window_end_ms", 2 * DAY), ("window_end_ms", 0),
])
def test_daily_rejects_invalid_fields_on_replace(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        replace(ref(), **{field: value})


@pytest.mark.parametrize("days", [(), [ref()], (ref(), ref()), (ref(1), ref()),
                                  (ref(), ref(2)), (ref(), replace(ref(1), snapshot_id=ref().snapshot_id)),
                                  tuple(ref(i) for i in range(91))])
def test_manifest_rejects_mutability_gaps_overlaps_order_and_duplicates(days: Any) -> None:
    with pytest.raises(ValueError):
        replace(manifest(), days=days)


def test_full_purpose_optional_field_matrix_and_revision_limits() -> None:
    for purpose in ("LIVE_QUERY", "MODELED_RESEARCH", "RECORDED_REPLAY", "unknown"):
        for fresh, available, asof, revision, pinned in product((False, True), repeat=5):
            valid = ((purpose == "LIVE_QUERY" and fresh and not (available or asof or pinned))
                     or (purpose == "MODELED_RESEARCH" and available and asof and not (fresh or pinned))
                     or (purpose == "RECORDED_REPLAY" and pinned and not (fresh or available or asof or revision)))
            values: dict[str, Any] = dict(purpose=purpose, freshness_policy_id="f" if fresh else None,
                          availability_policy_id="a" if available else None, as_of_ms=0 if asof else None,
                          revision=1 if revision else None, pinned_manifest=manifest() if pinned else None)
            if valid:
                request = query(**values)
                assert request.purpose == purpose
                if revision:
                    with pytest.raises(ValueError):
                        replace(request, end_ms=2 * DAY)
            else:
                with pytest.raises(ValueError):
                    query(**values)


@pytest.mark.parametrize("changes", [dict(product_id="BINANCE:ETHUSDT-SPOT"), dict(base_grid_id="other"),
                                     dict(algorithm_version="v2"), dict(days=(ref(1),))])
def test_replay_manifest_must_match_every_scope_dimension(changes: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        query(purpose="RECORDED_REPLAY", pinned_manifest=replace(manifest(), **changes))


@pytest.mark.parametrize("field,value", [("product_id", "bad"), ("base_grid_id", "é"),
    ("output_grid_id", "nul\x00"), ("algorithm_version", "a" * 33), ("freshness_policy_id", ""),
    ("start_ms", True), ("end_ms", 91 * DAY), ("end_ms", MAX + 1), ("revision", True),
    ("purpose", True)])
def test_query_field_domains_are_exact(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        replace(query(purpose="LIVE_QUERY", freshness_policy_id="fresh"), **{field: value})


def test_modeled_research_as_of_exact_integer_domain() -> None:
    class SubInt(int):
        pass

    baseline = query(purpose="MODELED_RESEARCH", availability_policy_id="model", as_of_ms=0)
    for value in (True, -1, MAX + 1, 1.0, SubInt(1)):
        with pytest.raises(ValueError, match="^invalid profile read integer$"):
            replace(baseline, as_of_ms=value)
    for value in (0, MAX):
        assert replace(baseline, as_of_ms=value).as_of_ms == value


def invalidation() -> ProfileInvalidation:
    return ProfileInvalidation("event", "a" * 64, "REVOKED", datetime(2026, 1, 1, tzinfo=timezone.utc), "b" * 64, "source")


@pytest.mark.parametrize("field,value", [("event_id", "e" * 129), ("reason_code", "r" * 65),
    ("source", "☃"), ("snapshot_id", "a" * 63), ("replacement_snapshot_id", "a" * 64),
    ("recorded_at", datetime(2026, 1, 1)), ("recorded_at", datetime(2026, 1, 1, microsecond=1, tzinfo=timezone.utc)),
    ("recorded_at", datetime(2026, 1, 1, tzinfo=timezone(timedelta(hours=1))))])
def test_invalidation_rejects_invalid_fields(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        replace(invalidation(), **{field: value})


def test_invalidation_detaches_mutable_utc_timezone_and_rejects_subclasses() -> None:
    class MutableUTC(tzinfo):
        offset = timedelta(0)

        def utcoffset(self, dt: datetime | None) -> timedelta:
            return self.offset

    zone = MutableUTC()
    stamp = datetime(2026, 1, 1, microsecond=1000, tzinfo=zone)
    value = replace(invalidation(), recorded_at=stamp, replacement_snapshot_id=None)
    zone.offset = timedelta(hours=1)
    assert value.recorded_at.tzinfo is timezone.utc and value.recorded_at.utcoffset() == timedelta(0)
    with pytest.raises(FrozenInstanceError):
        setattr(value, "source", "other")
    class SubInt(int):
        pass
    class SubStr(str):
        pass
    class SubDate(datetime):
        pass
    class SubRef(DailyProfileRef):
        pass
    for action in (lambda: replace(ref(), revision=SubInt(1)),
                   lambda: replace(ref(), snapshot_id=SubStr("a" * 64)),
                   lambda: replace(manifest(), days=(SubRef("a" * 64, 1, "b" * 64, 0, DAY),)),
                   lambda: replace(invalidation(), recorded_at=SubDate(2026, 1, 1, tzinfo=timezone.utc))):
        with pytest.raises(ValueError):
            action()
