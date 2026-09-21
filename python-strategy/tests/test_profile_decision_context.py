from dataclasses import FrozenInstanceError, fields, replace
from decimal import Decimal
import hashlib
import json

import pytest

from src.core.market_data.profiles.decision_context import (
    ProfileDecisionBasis as B,
    ProfileDecisionContext as C,
    ProfileDecisionStatus as S,
)
from src.core.market_data.profiles.read_types import ProfileQueryRequest
from test_profile_composite_types import FULL, POC

DAY = 86400000
SCOPE = (
    FULL.product_id,
    FULL.base_grid_id,
    FULL.output_grid.grid_id,
    FULL.algorithm_version,
)
REQUEST = ProfileQueryRequest(*SCOPE, 0, DAY, "LIVE_QUERY", "strict")


def item(basis=B.LIVE_OBSERVED):
    request = REQUEST
    if basis == B.MODELED:
        request = replace(
            REQUEST,
            purpose="MODELED_RESEARCH",
            freshness_policy_id=None,
            availability_policy_id="modeled",
            as_of_ms=DAY + 3,
        )
    checked, observed = (DAY + 1, DAY + 2) if basis == B.LIVE_OBSERVED else (None, None)
    return C(request, DAY + 3, basis, S.FRESH, None, FULL, DAY, checked, observed)


@pytest.mark.parametrize("basis", list(B))
def test_status_basis_matrix(basis):
    value = item(basis)
    for invalid in ("SECRET", "FRESH", "PROFILE_NOT_READY", True):
        with pytest.raises(ValueError, match="^PROFILE_DECISION_INVALID$"):
            replace(value, reason=invalid)
    with pytest.raises(ValueError):
        replace(value, basis=B.MODELED if basis == B.LIVE_OBSERVED else B.LIVE_OBSERVED)
    if basis == B.MODELED:
        with pytest.raises(ValueError):
            replace(value, request=replace(value.request, as_of_ms=DAY + 4))


@pytest.mark.parametrize(
    "field",
    "decision_time_ms available_at_ms validation_checked_at_ms observed_at_ms".split(),
)
@pytest.mark.parametrize("bad", [True, -1, 1 << 63, 1.0, type("Int", (int,), {})(DAY)])
def test_exact_timestamp_domain(field, bad):
    with pytest.raises(ValueError):
        replace(item(), **{field: bad})


@pytest.mark.parametrize(
    "change",
    [
        {"available_at_ms": DAY - 1},
        {"available_at_ms": DAY + 2},
        {"validation_checked_at_ms": DAY - 1},
        {"validation_checked_at_ms": DAY + 3},
        {"observed_at_ms": DAY},
        {"observed_at_ms": DAY + 4},
        {"available_at_ms": None},
        {"validation_checked_at_ms": None},
        {"observed_at_ms": None},
        {"profile": None},
        {"request": None},
        {"status": "STALE"},
        {"basis": "MODELED"},
    ],
)
def test_invalid_live_shape_and_time(change):
    with pytest.raises(ValueError):
        replace(item(), **change)


def test_modeled_boundaries_and_recorded_rejection():
    value = item(B.MODELED)
    for stamp in (DAY, DAY + 3):
        assert replace(value, available_at_ms=stamp)
    for change in (
        {"available_at_ms": DAY - 1},
        {"available_at_ms": DAY + 4},
        {"observed_at_ms": DAY},
        {"validation_checked_at_ms": DAY},
    ):
        with pytest.raises(ValueError):
            replace(value, **change)
    assert replace(
        item(),
        available_at_ms=DAY + 3,
        validation_checked_at_ms=DAY + 3,
        observed_at_ms=DAY + 3,
    )
    replay = replace(
        REQUEST,
        purpose="RECORDED_REPLAY",
        freshness_policy_id=None,
        pinned_manifest=FULL.manifest,
    )
    with pytest.raises(ValueError):
        replace(value, request=replay)


@pytest.mark.parametrize(
    "change",
    [
        {"product_id": "BINANCE:ETHUSDT-SPOT"},
        {"base_grid_id": "other"},
        {"output_grid_id": "other"},
        {"algorithm_version": "other"},
        {"start_ms": DAY, "end_ms": 2 * DAY},
        {"revision": 2},
    ],
)
def test_profile_request_identity(change):
    value = replace(
        item(),
        decision_time_ms=3 * DAY,
        available_at_ms=2 * DAY,
        validation_checked_at_ms=2 * DAY,
        observed_at_ms=2 * DAY,
    )
    with pytest.raises(ValueError):
        replace(value, request=replace(REQUEST, **change))
    assert replace(item(), request=replace(REQUEST, revision=1))


def test_canonical_full_content_and_immutability():
    value = item()
    payload = json.loads(value.canonical_bytes)
    assert payload["schema_version"] == 1
    assert set(payload["request"]) == {f.name for f in fields(REQUEST)}
    assert payload["request"]["pinned_manifest"] is None
    assert payload["profile"]["bins"][0]["base_volume"] == "1"
    assert value.digest == hashlib.sha256(value.canonical_bytes).hexdigest()
    assert (
        replace(value, profile=replace(FULL, base_volume=Decimal("1.00"))).digest
        == value.digest
    )
    for field, replacement in (
        ("base_volume", Decimal(2)),
        ("quote_volume", Decimal(2)),
        ("aggregate_count", 2),
        ("bins", (replace(FULL.bins[0], aggregate_count=2),)),
        ("poc", replace(POC, high_exclusive=Decimal(11))),
    ):
        assert (
            replace(value, profile=replace(FULL, **{field: replacement})).digest
            != value.digest
        )
    assert replace(value, observed_at_ms=DAY + 3).digest != value.digest
    with pytest.raises(FrozenInstanceError):
        setattr(value, "reason", "SECRET")
    assert not hasattr(value, "__dict__")


@pytest.mark.parametrize("field,original", [("request", REQUEST), ("profile", FULL)])
def test_exact_nested_types(field, original):
    subclass = type("Subclass", (type(original),), {})
    inherited = subclass(
        **{f.name: getattr(original, f.name) for f in fields(original)}
    )
    for bad in (None, inherited, {}):
        with pytest.raises(ValueError, match="^PROFILE_DECISION_INVALID$") as error:
            replace(item(), **{field: bad})
        assert error.value.__cause__ is None


def test_request_and_manifest_content_are_digest_inputs():
    value = item()
    assert (
        replace(value, request=replace(REQUEST, freshness_policy_id="other")).digest
        != value.digest
    )
    for field, new in (
        ("snapshot_id", "c" * 64),
        ("content_sha256", "d" * 64),
        ("revision", 2),
    ):
        ref = replace(FULL.manifest.days[0], **{field: new})
        profile = replace(FULL, manifest=replace(FULL.manifest, days=(ref,)))
        assert replace(value, profile=profile).digest != value.digest
