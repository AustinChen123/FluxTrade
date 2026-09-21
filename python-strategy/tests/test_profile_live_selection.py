from dataclasses import FrozenInstanceError, replace
from typing import Callable, cast
import subprocess
import sys

import pytest

from src.core.market_data.profiles import live_selection as live
from src.core.market_data.profiles.read_results import ProfileCandidate
from src.core.market_data.profiles.selection import ProfileSelectionError
from test_profile_selection import inputs
from test_profile_read_results import NOW

DAY = 86400000


def setup(count: int = 1):
    request, batch = inputs(count)
    request = replace(
        request,
        purpose="LIVE_QUERY",
        pinned_manifest=None,
        freshness_policy_id="utc_complete_strict_v1",
    )
    batch = replace(
        batch,
        candidates=tuple(replace(c, source_available_at=NOW) for c in batch.candidates),
    )
    return request, batch, live.LiveSelectionContext(count * DAY)


@pytest.mark.parametrize("count", [1, 7, 30, 90])
def test_exact_window_and_reverse_order(count: int) -> None:
    request, batch, context = setup(count)
    result = live.select_live_profile(
        request, replace(batch, candidates=tuple(reversed(batch.candidates))), context
    )
    assert isinstance(result, live.LiveProfileSelection)
    assert result.manifest.days == tuple(c.ref for c in batch.candidates)
    assert (
        result.decision_time_ms == context.decision_time_ms
        and result.policy == request.freshness_policy_id
    )
    assert not hasattr(result, "__dict__")
    with pytest.raises(FrozenInstanceError):
        setattr(result, "policy", "other")


@pytest.mark.parametrize(
    "now,reason", [(DAY - 1, "NOT_READY"), (2 * DAY, "PROFILE_EXPIRED")]
)
def test_midnight_boundary(now: int, reason: str) -> None:
    request, batch, _ = setup()
    assert live.select_live_profile(
        request, batch, live.LiveSelectionContext(now)
    ) == live.LiveProfileSelectionUnavailable(reason)


@pytest.mark.parametrize(
    "explicit,latest_revoked,reason",
    [
        (None, False, None),
        (None, True, None),
        (2, True, "SNAPSHOT_REVOKED"),
        (3, False, "NOT_READY"),
    ],
)
@pytest.mark.parametrize("reverse", [False, True])
def test_revisions_and_no_explicit_fallback(
    explicit: int | None, latest_revoked: bool, reason: str | None, reverse: bool
) -> None:
    request, batch, context = setup()
    old = batch.candidates[0]
    newer = replace(
        old,
        ref=replace(old.ref, snapshot_id="f" * 64, revision=2),
        revoked=latest_revoked,
    )
    result = live.select_live_profile(
        replace(request, revision=explicit),
        replace(batch, candidates=(old, newer) if reverse else (newer, old)),
        context,
    )
    if reason:
        assert result == live.LiveProfileSelectionUnavailable(reason)
    else:
        assert isinstance(result, live.LiveProfileSelection)
        assert result.manifest.days == ((old if latest_revoked else newer).ref,)


@pytest.mark.parametrize("mode", ["missing", "modeled", "revoked", "null"])
def test_candidate_modes(mode: str) -> None:
    request, batch, context = setup()
    candidate = batch.candidates[0]
    candidate = replace(
        candidate,
        availability_basis="MODELED" if mode == "modeled" else "OBSERVED",
        revoked=mode == "revoked",
        source_available_at=None if mode == "null" else NOW,
    )
    batch = replace(batch, candidates=() if mode == "missing" else (candidate,))
    if mode == "null":
        with pytest.raises(ProfileSelectionError):
            live.select_live_profile(request, batch, context)
    else:
        assert live.select_live_profile(
            request, batch, context
        ) == live.LiveProfileSelectionUnavailable(
            "SNAPSHOT_REVOKED" if mode == "revoked" else "NOT_READY"
        )


@pytest.mark.parametrize("missing", [0, 1])
def test_missing_precedes_revoked(missing: int) -> None:
    request, batch, context = setup(2)
    batch = replace(
        batch, candidates=(replace(batch.candidates[1 - missing], revoked=True),)
    )
    assert live.select_live_profile(
        request, batch, context
    ) == live.LiveProfileSelectionUnavailable("NOT_READY")


def test_computed_and_published_not_read(monkeypatch: pytest.MonkeyPatch) -> None:
    request, batch, context = setup()

    def forbidden(self):
        raise AssertionError("visibility timestamp read")

    for name in ("computed_at", "published_at"):
        monkeypatch.setattr(ProfileCandidate, name, property(forbidden))
    assert isinstance(
        live.select_live_profile(request, batch, context), live.LiveProfileSelection
    )


def test_malformed_and_output_contracts() -> None:
    request, batch, context = setup()
    selector = cast(Callable[..., object], live.select_live_profile)
    for args in (
        (True, batch, context),
        (request, True, context),
        (request, batch, True),
        (replace(request, freshness_policy_id="SECRET"), batch, context),
        (request, replace(batch, base_grid_id="other"), context),
        (*inputs(), context),
    ):
        with pytest.raises(ProfileSelectionError) as caught:
            selector(*args)
        assert (
            str(caught.value) == "PROFILE_SELECTION_INVALID"
            and caught.value.__cause__ is None
        )
    for value in (True, -1, 1 << 63, type("Integer", (int,), {})(1)):
        with pytest.raises(ProfileSelectionError):
            cast(Callable[..., object], live.LiveSelectionContext)(value)
    assert live.LiveSelectionContext(0).decision_time_ms == 0
    assert live.LiveSelectionContext((1 << 63) - 1).decision_time_ms == (1 << 63) - 1
    with pytest.raises(ProfileSelectionError):
        live.LiveProfileSelectionUnavailable("SECRET")
    result = live.select_live_profile(request, batch, context)
    assert isinstance(result, live.LiveProfileSelection)
    with pytest.raises(ProfileSelectionError):
        replace(result, policy="SECRET")


def test_import_has_no_external_owners() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import src.core.market_data.profiles.live_selection; "
            "assert not any(n.startswith(('sqlalchemy','fluxtrade_core')) or n=='src.core.db' for n in sys.modules)",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize("now", [DAY + 1, 2 * DAY - 1])
def test_intra_day_decision_floors_to_completed_day(now: int) -> None:
    request, batch, _ = setup()
    selected = live.select_live_profile(request, batch, live.LiveSelectionContext(now))
    assert isinstance(selected, live.LiveProfileSelection)
    assert selected.manifest.days[-1].window_end_ms == DAY
    assert selected.decision_time_ms == now


@pytest.mark.parametrize("null_index", [0, 1])
@pytest.mark.parametrize("other", ["missing", "revoked"])
def test_null_integrity_precedes_unavailable(null_index: int, other: str) -> None:
    request, batch, context = setup(2)
    rows = [replace(batch.candidates[null_index], source_available_at=None)]
    if other == "revoked":
        rows.append(replace(batch.candidates[1 - null_index], revoked=True))
    batch = replace(
        batch, candidates=tuple(sorted(rows, key=lambda c: c.ref.window_start_ms))
    )
    with pytest.raises(ProfileSelectionError, match="^PROFILE_SELECTION_INVALID$"):
        live.select_live_profile(request, batch, context)


def test_output_hostile_constructor_matrix() -> None:
    request, batch, context = setup()
    selected = live.select_live_profile(request, batch, context)
    assert isinstance(selected, live.LiveProfileSelection)
    manifest = selected.manifest
    derived = type("ManifestSubclass", (type(manifest),), {})(
        manifest.product_id,
        manifest.base_grid_id,
        manifest.algorithm_version,
        manifest.days,
    )
    construct = cast(Callable[..., object], live.LiveProfileSelection)
    for bad in (True, derived):
        with pytest.raises(ProfileSelectionError):
            construct(bad, DAY)
    for now in (DAY - 1, 2 * DAY, True):
        with pytest.raises(ProfileSelectionError):
            construct(manifest, now)
    for policy in (True, "SECRET", type("Text", (str,), {})("utc_complete_strict_v1")):
        with pytest.raises(ProfileSelectionError):
            construct(manifest, DAY, policy)
    assert live.LiveProfileSelection(manifest, DAY) == selected
    unavailable = cast(Callable[..., object], live.LiveProfileSelectionUnavailable)
    for bad in (True, "SECRET", type("Text", (str,), {})("NOT_READY")):
        with pytest.raises(ProfileSelectionError):
            unavailable(bad)
    for reason in ("NOT_READY", "PROFILE_EXPIRED", "SNAPSHOT_REVOKED"):
        assert live.LiveProfileSelectionUnavailable(reason).reason == reason
