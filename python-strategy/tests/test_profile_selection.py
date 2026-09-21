from dataclasses import FrozenInstanceError, replace
from typing import Callable, cast

import pytest

from src.core.market_data.profiles import selection as owner
from src.core.market_data.profiles.read_results import ProfileCandidate
from src.core.market_data.profiles.read_types import ProfileQueryRequest
from test_profile_read_results import manifest


def inputs(count: int = 1):
    read = manifest(count)
    pinned = read.manifest
    request = ProfileQueryRequest(
        pinned.product_id,
        pinned.base_grid_id,
        pinned.base_grid_id,
        pinned.algorithm_version,
        0,
        count * 86400000,
        "RECORDED_REPLAY",
        pinned_manifest=pinned,
    )
    candidates = tuple(
        ProfileCandidate(
            day.ref, day.computed_at, day.published_at, None, "OBSERVED", False
        )
        for day in read.days
    )
    return request, owner.ProfileCandidateBatch(
        pinned.product_id, pinned.base_grid_id, pinned.algorithm_version, candidates
    )


@pytest.mark.parametrize("count", [1, 7, 30, 90])
def test_order_and_revoked_forensic_selection(count: int) -> None:
    request, batch = inputs(count)
    batch = replace(
        batch,
        candidates=tuple(replace(c, revoked=True) for c in reversed(batch.candidates)),
    )
    result = owner.select_recorded_profile(request, batch)
    assert isinstance(result, owner.RecordedProfileSelection)
    assert result.manifest is request.pinned_manifest
    assert result.revoked_snapshot_ids == tuple(
        ref.snapshot_id for ref in result.manifest.days
    )
    assert not hasattr(result, "__dict__")
    with pytest.raises(FrozenInstanceError):
        setattr(result, "manifest", None)


@pytest.mark.parametrize("revision", [None, 1, 3])
def test_missing_never_falls_back(revision: int | None) -> None:
    request, batch = inputs(7)
    rows = batch.candidates[1:]
    if revision is not None:
        candidate = batch.candidates[0]
        rows += (
            replace(
                candidate,
                ref=replace(candidate.ref, snapshot_id="f" * 64, revision=revision),
            ),
        )
    assert (
        owner.select_recorded_profile(request, replace(batch, candidates=rows))
        == owner.ProfileSelectionUnavailable()
    )


@pytest.mark.parametrize(
    "field,value",
    [("revision", 2), ("content_sha256", "f" * 64), ("window_start_ms", 86400000)],
)
def test_same_id_mismatch(field: str, value: object) -> None:
    request, batch = inputs()
    ref = batch.candidates[0].ref
    ref = (
        replace(ref, window_start_ms=86400000, window_end_ms=172800000)
        if field == "window_start_ms"
        else replace(ref, **{field: value})
    )
    with pytest.raises(
        owner.ProfileSelectionError, match="^PROFILE_SELECTION_INVALID$"
    ):
        owner.select_recorded_profile(
            request, replace(batch, candidates=(replace(batch.candidates[0], ref=ref),))
        )


def test_timestamp_access_is_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    request, batch = inputs()

    def forbidden(self):
        raise AssertionError("timestamp read")

    for name in ("computed_at", "published_at", "source_available_at"):
        monkeypatch.setattr(ProfileCandidate, name, property(forbidden))
    assert isinstance(
        owner.select_recorded_profile(request, batch), owner.RecordedProfileSelection
    )


def test_batch_conflicts_and_bounds() -> None:
    _, batch = inputs()
    candidate = batch.candidates[0]
    for rows in (
        [candidate],
        (True,),
        (candidate, candidate),
        (candidate,) * 1001,
        (
            candidate,
            replace(candidate, ref=replace(candidate.ref, snapshot_id="f" * 64)),
        ),
    ):
        with pytest.raises(owner.ProfileSelectionError) as caught:
            replace(batch, candidates=rows)
        assert (
            str(caught.value) == "PROFILE_SELECTION_INVALID"
            and caught.value.__cause__ is None
        )


def test_scope_and_purpose_reject() -> None:
    request, batch = inputs()
    with pytest.raises(owner.ProfileSelectionError):
        owner.select_recorded_profile(request, replace(batch, base_grid_id="other"))
    live = replace(
        request, purpose="LIVE_QUERY", pinned_manifest=None, freshness_policy_id="fresh"
    )
    with pytest.raises(owner.ProfileSelectionError):
        owner.select_recorded_profile(live, batch)


def test_exact_types_and_unavailable_contract() -> None:
    request, batch = inputs()
    subclass = type("BatchSubclass", (owner.ProfileCandidateBatch,), {})
    derived = subclass(
        batch.product_id, batch.base_grid_id, batch.algorithm_version, batch.candidates
    )
    for invalid_request, invalid_batch in (
        (True, batch),
        (request, True),
        (request, derived),
    ):
        with pytest.raises(owner.ProfileSelectionError) as caught:
            cast(Callable[..., object], owner.select_recorded_profile)(
                invalid_request, invalid_batch
            )
        assert caught.value.__cause__ is None and caught.value.__suppress_context__
    for reason in (True, "SECRET", type("Text", (str,), {})("MISSING")):
        with pytest.raises(owner.ProfileSelectionError):
            cast(Callable[..., object], owner.ProfileSelectionUnavailable)(reason)
    with pytest.raises(owner.ProfileSelectionError):
        replace(batch, product_id="SECRET")


def test_distinct_candidate_capacity() -> None:
    _, batch = inputs()
    seed = batch.candidates[0]
    rows = tuple(
        replace(
            seed,
            ref=replace(
                seed.ref,
                snapshot_id=f"{i:064x}",
                window_start_ms=i * 86400000,
                window_end_ms=(i + 1) * 86400000,
            ),
        )
        for i in range(1001)
    )
    assert len(replace(batch, candidates=rows[:1000]).candidates) == 1000
    with pytest.raises(owner.ProfileSelectionError):
        replace(batch, candidates=rows)


@pytest.mark.parametrize("missing_index", [0, 1])
def test_conflict_wins_over_missing_in_either_order(missing_index: int) -> None:
    request, batch = inputs(2)
    conflict = batch.candidates[1 - missing_index]
    conflict = replace(conflict, ref=replace(conflict.ref, content_sha256="f" * 64))
    with pytest.raises(
        owner.ProfileSelectionError, match="^PROFILE_SELECTION_INVALID$"
    ):
        owner.select_recorded_profile(request, replace(batch, candidates=(conflict,)))


def test_selection_constructor_hostile_matrix() -> None:
    request, _ = inputs(3)
    manifest = request.pinned_manifest
    assert manifest is not None
    ids = tuple(ref.snapshot_id for ref in manifest.days)
    derived = type("ManifestSubclass", (type(manifest),), {})(
        manifest.product_id,
        manifest.base_grid_id,
        manifest.algorithm_version,
        manifest.days,
    )
    constructor = cast(Callable[..., object], owner.RecordedProfileSelection)
    for bad_manifest in (True, derived):
        with pytest.raises(owner.ProfileSelectionError):
            constructor(bad_manifest, ())
    for bad_ids in (
        list(ids),
        type("TupleSubclass", (tuple,), {})(ids),
        (True,),
        (type("Text", (str,), {})(ids[0]),),
        (ids[0], ids[0]),
        ("f" * 64,),
        tuple(reversed(ids)),
    ):
        with pytest.raises(owner.ProfileSelectionError):
            constructor(manifest, bad_ids)
    for valid in ((), (ids[1],), (ids[0], ids[2]), ids):
        assert (
            owner.RecordedProfileSelection(manifest, valid).revoked_snapshot_ids
            == valid
        )
