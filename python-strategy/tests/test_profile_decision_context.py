from dataclasses import FrozenInstanceError, asdict, fields, replace
from decimal import Decimal
import hashlib
import json
from itertools import permutations
from typing import cast

import pytest

from src.core.market_data.profiles.decision_context import (
    ProfileDecisionBasis as B,
    ProfileDecisionContext as C,
    ProfileDecisionStatus as S,
    StrategyMarketDataContext as Collection,
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
    assert payload["schema_version"] == 2
    assert payload["profile"]["merge_algorithm_version"] == FULL.merge_algorithm_version
    assert payload["profile"]["composite_id"] == FULL.composite_id
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


MISSING = (
    "PROFILE_NOT_READY",
    "PROFILE_EXPIRED",
    "BACKEND_UNAVAILABLE",
    "CLOCK_UNCERTAIN",
    "VALIDATION_EXPIRED",
)
INVALID = ("SNAPSHOT_REVOKED", "INVALID_PROFILE", "QUERY_TOO_LARGE")


def unavailable(basis=B.LIVE_OBSERVED, status=S.MISSING, reason="PROFILE_NOT_READY"):
    return C(item(basis).request, DAY + 3, basis, status, reason)


@pytest.mark.parametrize("basis", list(B))
@pytest.mark.parametrize("status", list(S))
@pytest.mark.parametrize("reason", (None, "", "SECRET", True) + MISSING + INVALID)
def test_complete_reason_matrix(basis, status, reason):
    valid = (
        reason is None
        if status == S.FRESH
        else reason in (MISSING if status == S.MISSING else INVALID)
    )

    def construct():
        return (
            replace(item(basis), reason=reason)
            if status == S.FRESH
            else unavailable(basis, status, reason)
        )

    if valid:
        assert construct().reason == reason
    else:
        with pytest.raises(ValueError, match="^PROFILE_DECISION_INVALID$") as error:
            construct()
        assert error.value.__cause__ is None


@pytest.mark.parametrize("basis", list(B))
@pytest.mark.parametrize(
    "status,reason", [(S.MISSING, MISSING[0]), (S.INVALID, INVALID[0])]
)
@pytest.mark.parametrize(
    "field,value",
    [
        ("profile", FULL),
        ("available_at_ms", DAY),
        ("validation_checked_at_ms", DAY),
        ("observed_at_ms", DAY),
    ],
)
def test_unavailable_forbids_each_evidence(basis, status, reason, field, value):
    with pytest.raises(ValueError):
        replace(unavailable(basis, status, reason), **{field: value})


@pytest.mark.parametrize(
    "status,reason", [(S.MISSING, MISSING[0]), (S.INVALID, INVALID[0])]
)
def test_unavailable_request_pairing_and_as_of(status, reason):
    for basis in B:
        value = unavailable(basis, status, reason)
        other = B.MODELED if basis == B.LIVE_OBSERVED else B.LIVE_OBSERVED
        with pytest.raises(ValueError):
            replace(value, request=item(other).request)
        replay = replace(
            REQUEST,
            purpose="RECORDED_REPLAY",
            freshness_policy_id=None,
            pinned_manifest=FULL.manifest,
        )
        with pytest.raises(ValueError):
            replace(value, request=replay)
    modeled = unavailable(B.MODELED, status, reason)
    for stamp in (DAY + 2, DAY + 4):
        with pytest.raises(ValueError):
            replace(modeled, request=replace(modeled.request, as_of_ms=stamp))


@pytest.mark.parametrize(
    "field,bad",
    [
        ("status", "MISSING"),
        ("status", "STALE"),
        ("status", True),
        ("reason", type("String", (str,), {})(MISSING[0])),
        ("basis", "LIVE_OBSERVED"),
        ("decision_time_ms", True),
        ("decision_time_ms", -1),
        ("decision_time_ms", 1 << 63),
    ],
)
def test_unavailable_exact_types(field, bad):
    with pytest.raises(ValueError):
        replace(unavailable(), **{field: bad})


def test_unavailable_canonical_nulls_and_distinct_digests():
    values = [
        unavailable(basis, status, reason)
        for basis in B
        for status, reasons in ((S.MISSING, MISSING), (S.INVALID, INVALID))
        for reason in reasons
    ]
    assert len({value.digest for value in values}) == len(values)
    for value in values:
        payload = json.loads(value.canonical_bytes)
        assert payload["schema_version"] == 2
        assert payload["status"] == value.status.value
        assert payload["reason"] == value.reason
        assert payload["basis"] == value.basis.value
        for field in (
            "profile",
            "available_at_ms",
            "validation_checked_at_ms",
            "observed_at_ms",
        ):
            assert field in payload and payload[field] is None
        assert value.digest == hashlib.sha256(value.canonical_bytes).hexdigest()
        assert value.digest != item(value.basis).digest


def test_collection_empty_golden_and_frozen():
    value = Collection(0, ())
    assert (
        value.canonical_bytes
        == b'{"decision_time_ms":0,"profiles":[],"schema_version":1}'
    )
    assert value.digest == hashlib.sha256(value.canonical_bytes).hexdigest()
    assert Collection((1 << 63) - 1, ())
    assert replace(value, decision_time_ms=1).digest != value.digest
    with pytest.raises(FrozenInstanceError):
        setattr(value, "profiles", ())
    assert not hasattr(value, "__dict__")


def test_collection_permutations_use_full_request_bytes():
    first = item()
    second = replace(first, request=replace(REQUEST, revision=1))
    third = item(B.MODELED)
    entries = (first, second, third)

    def key(value):
        return json.dumps(
            asdict(value.request),
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()

    expected = tuple(sorted(entries, key=key))
    reference = Collection(DAY + 3, entries)
    for ordering in permutations(entries):
        value = Collection(DAY + 3, ordering)
        assert value.profiles == expected
        assert value.canonical_bytes == reference.canonical_bytes
        assert value.digest == reference.digest
        payload = json.loads(value.canonical_bytes)
        assert payload == {
            "schema_version": 1,
            "decision_time_ms": DAY + 3,
            "profiles": [json.loads(entry.canonical_bytes) for entry in expected],
        }
    assert json.loads(key(first))["revision"] is None


@pytest.mark.parametrize(
    "other",
    [
        item(),
        unavailable(),
        unavailable(status=S.INVALID, reason=INVALID[0]),
        replace(item(), profile=replace(FULL, base_volume=Decimal(2))),
    ],
)
def test_collection_duplicate_request_never_selects_winner(other):
    for entries in ((item(), other), (other, item())):
        with pytest.raises(ValueError, match="^PROFILE_DECISION_INVALID$"):
            Collection(DAY + 3, cast(tuple[C, ...], entries))


@pytest.mark.parametrize("stamp", [True, -1, 1 << 63, 1.0, type("Int", (int,), {})(0)])
def test_collection_exact_clock(stamp):
    with pytest.raises(ValueError):
        Collection(stamp, ())


def test_collection_exact_items_container_and_time():
    subclass = type("Child", (C,), {})
    child = subclass(**{f.name: getattr(item(), f.name) for f in fields(C)})
    for entries in (
        [item()],
        type("Tuple", (tuple,), {})((item(),)),
        (None,),
        (child,),
    ):
        with pytest.raises(ValueError):
            Collection(DAY + 3, cast(tuple[C, ...], entries))
    with pytest.raises(ValueError):
        Collection(DAY + 4, (item(),))
    with pytest.raises(ValueError):
        Collection(
            DAY + 3,
            (
                item(),
                replace(
                    item(B.MODELED),
                    decision_time_ms=DAY + 4,
                    request=replace(item(B.MODELED).request, as_of_ms=DAY + 4),
                ),
            ),
        )


def test_collection_digest_tracks_valid_nested_changes():
    first = item()
    changed = [
        replace(first, request=replace(REQUEST, freshness_policy_id="other")),
        replace(first, profile=replace(FULL, quote_volume=Decimal(2))),
        replace(first, observed_at_ms=DAY + 3),
        unavailable(),
        unavailable(reason=MISSING[1]),
        unavailable(status=S.INVALID, reason=INVALID[0]),
        item(B.MODELED),
    ]
    values = [Collection(DAY + 3, (entry,)) for entry in [first, *changed]]
    assert len({value.digest for value in values}) == len(values)
    later = Collection(DAY + 4, (replace(first, decision_time_ms=DAY + 4),))
    assert later.digest != values[0].digest
