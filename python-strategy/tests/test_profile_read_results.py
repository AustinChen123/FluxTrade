"""Pure result/budget contracts, without I/O or content verification claims for candidates."""
import subprocess
import sys
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timedelta, timezone, tzinfo
from typing import Any

import pytest

from src.core.market_data.profiles import read_results as results
from src.core.market_data.profiles.publication import VerifiedProfilePublication
from src.core.market_data.profiles.read_types import DailyProfileRef, OrderedProfileManifest, ProfileInvalidation
from test_profile_publication import publication

NOW = datetime(2026, 1, 1, microsecond=123456, tzinfo=timezone.utc)
DAY = 86400000


def daily(index: int = 0) -> results.VerifiedDailyRead:
    value = publication()
    value = replace(value, content=replace(value.content, window_start_ms=index * DAY, window_end_ms=(index + 1) * DAY))
    digest = value.content_sha256
    ref = DailyProfileRef(digest, 1, digest, index * DAY, (index + 1) * DAY)
    return results.VerifiedDailyRead(ref, NOW, NOW, value, ())


def manifest(count: int = 1) -> results.VerifiedManifestRead:
    days = tuple(daily(i) for i in range(count))
    content = days[0].publication.content
    refs = OrderedProfileManifest(content.product_id, content.grid_id, content.algorithm_version, tuple(day.ref for day in days))
    return results.VerifiedManifestRead(refs, days)


def candidate() -> results.ProfileCandidate:
    return results.ProfileCandidate(daily().ref, NOW, NOW, None, "OBSERVED", False)


def event() -> ProfileInvalidation:
    return ProfileInvalidation("event", daily().ref.snapshot_id, "BAD", NOW.replace(microsecond=123000), None, "source")


@pytest.mark.parametrize("count", [1, 7, 30, 90])
def test_valid_manifest_days_are_exact_and_deeply_immutable(count: int) -> None:
    value = manifest(count)
    assert len(value.days) == count and tuple(day.ref for day in value.days) == value.manifest.days
    assert value.days[0].computed_at.microsecond == 123456
    for obj, field in ((value, "days"), (value.days[0], "publication"), (candidate(), "revoked")):
        with pytest.raises(FrozenInstanceError):
            setattr(obj, field, None)
        assert not hasattr(obj, "__dict__")


def test_header_only_candidate_does_not_claim_digest_identity_or_completeness() -> None:
    value = replace(candidate(), ref=replace(daily().ref, snapshot_id="a" * 64), revoked=True, availability_basis="MODELED")
    assert value.ref.snapshot_id != value.ref.content_sha256 and value.source_available_at is None
    assert replace(daily(), invalidations=(event(),)).invalidations == (event(),)


@pytest.mark.parametrize("field,value", [("revoked", 1), ("revoked", None), ("availability_basis", True),
    ("availability_basis", "unknown"), ("ref", None)])
def test_candidate_exact_fields(field: str, value: Any) -> None:
    with pytest.raises(ValueError):
        replace(candidate(), **{field: value})


@pytest.mark.parametrize("field", ["computed_at", "published_at", "source_available_at"])
def test_timestamp_exactness_and_detached_mutable_utc(field: str) -> None:
    class MutableUTC(tzinfo):
        offset = timedelta(0)

        def utcoffset(self, dt: datetime | None) -> timedelta:
            return self.offset

    class SubDate(datetime):
        pass

    for stamp in (NOW.replace(tzinfo=None), NOW.replace(tzinfo=timezone(timedelta(hours=1))),
                  SubDate(2026, 1, 1, tzinfo=timezone.utc)):
        with pytest.raises(ValueError):
            replace(candidate(), **{field: stamp})
        if field != "source_available_at":
            with pytest.raises(ValueError):
                replace(daily(), **{field: stamp})
    zone = MutableUTC()
    stamp = NOW.replace(tzinfo=zone)
    value = replace(candidate(), **{field: stamp})
    verified = replace(daily(), **{field: stamp}) if field != "source_available_at" else None
    zone.offset = timedelta(hours=1)
    assert getattr(value, field).tzinfo is timezone.utc and getattr(value, field).microsecond == stamp.microsecond
    if verified is not None:
        assert getattr(verified, field).tzinfo is timezone.utc


@pytest.mark.parametrize("damage", ["snapshot", "digest", "window", "event", "events_list", "publication"])
def test_verified_daily_rejects_identity_or_nested_mismatch(damage: str) -> None:
    value = daily()
    changes: dict[str, Any] = {
        "snapshot": dict(ref=replace(value.ref, snapshot_id="a" * 64)),
        "digest": dict(ref=replace(value.ref, content_sha256="a" * 64)),
        "window": dict(ref=replace(value.ref, window_start_ms=DAY, window_end_ms=2 * DAY)),
        "event": dict(invalidations=(replace(event(), snapshot_id="a" * 64),)),
        "events_list": dict(invalidations=[event()]), "publication": dict(publication=None),
    }[damage]
    with pytest.raises(ValueError):
        replace(value, **changes)


@pytest.mark.parametrize("damage", ["product", "grid", "algorithm", "ref", "window", "order", "length", "list"])
def test_verified_manifest_rejects_scope_and_order_mismatch(damage: str) -> None:
    value = manifest(2)
    changed = value.manifest
    if damage in ("product", "grid", "algorithm"):
        field, replacement = {"product": ("product_id", "BINANCE:ETHUSDT-SPOT"), "grid": ("base_grid_id", "other"),
                              "algorithm": ("algorithm_version", "other")}[damage]
        changed = replace(changed, **{field: replacement})
    elif damage == "ref":
        changed = replace(changed, days=(replace(changed.days[0], revision=2), changed.days[1]))
    elif damage == "window":
        changed = manifest(2).manifest
        changed = replace(changed, days=(daily(1).ref, daily(2).ref))
    days = value.days[::-1] if damage == "order" else value.days[:-1] if damage == "length" else list(value.days) if damage == "list" else value.days
    with pytest.raises(ValueError):
        results.VerifiedManifestRead(changed, days)  # type: ignore[arg-type]


def test_nested_exact_subclasses_are_rejected() -> None:
    class SubTuple(tuple):
        pass
    class SubStr(str):
        pass
    class SubRef(DailyProfileRef):
        pass
    class SubEvent(ProfileInvalidation):
        pass
    class SubPublication(VerifiedProfilePublication):
        pass
    class SubDaily(results.VerifiedDailyRead):
        pass
    class SubManifest(OrderedProfileManifest):
        pass
    from dataclasses import fields
    def subclass(kind: Any, original: Any) -> Any:
        return kind(**{field.name: getattr(original, field.name) for field in fields(original)})
    for action in (lambda: replace(candidate(), availability_basis=SubStr("OBSERVED")),
                   lambda: replace(candidate(), ref=subclass(SubRef, daily().ref)),
                   lambda: replace(daily(), publication=subclass(SubPublication, publication())),
                   lambda: replace(daily(), invalidations=SubTuple()),
                   lambda: replace(daily(), invalidations=(subclass(SubEvent, event()),)),
                   lambda: replace(manifest(), days=(subclass(SubDaily, daily()),)),
                   lambda: replace(manifest(), days=SubTuple((daily(),))),
                   lambda: replace(manifest(), manifest=subclass(SubManifest, manifest().manifest))):
        with pytest.raises(ValueError):
            action()


def test_fixed_budget_constants_and_isolated_import() -> None:
    expected = {"MAX_CANDIDATES": 1000, "MAX_MANIFEST_DAYS": 90, "MAX_BINS": 100000,
                "MAX_INVALIDATIONS_PER_SNAPSHOT": 1000, "MAX_INVALIDATIONS_PER_OPERATION": 10000,
                "MAX_METADATA_TEXT_BYTES": 131072, "MAX_NUMERIC_TEXT_BYTES": 64, "MAX_OPERATION_BYTES": 33554432,
                "CANDIDATE_ACCOUNTING_BYTES": 1024, "HEADER_ACCOUNTING_BYTES": 1024,
                "BIN_ACCOUNTING_BYTES": 256, "EVENT_ACCOUNTING_BYTES": 512}
    for name, value in expected.items():
        assert type(getattr(results, name)) is int and getattr(results, name) == value
    with pytest.raises(ValueError):
        manifest(91)
    with pytest.raises(ValueError):
        replace(manifest(), days=())
    code = "import sys; import src.core.market_data.profiles.read_results; assert not any(n.startswith('sqlalchemy') or n.endswith('.orm') or n.endswith('.orm_models') for n in sys.modules)"
    subprocess.run([sys.executable, "-c", code], check=True, timeout=10)
