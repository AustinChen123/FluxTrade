from dataclasses import asdict, replace
from itertools import permutations
from typing import Any, cast

import pytest

from src.core.market_data.profiles.live_selection import plan_live_profile_requests
from src.core.market_data.profiles.requirements import ProfileRequirement
from src.core.market_data.profiles.selection import ProfileSelectionError

DAY = 86400000
MAX = (1 << 63) - 1
REQUIREMENT = ProfileRequirement(
    "BINANCE:BTCUSDT-SPOT",
    "unregistered_base",
    "different_output",
    "future_algorithm",
    1,
    "utc_complete_strict_v1",
)


@pytest.mark.parametrize("days", [1, 7, 30, 90])
def test_minimum_history_and_each_field(days):
    requirement = replace(REQUIREMENT, window_days=days)
    (request,) = plan_live_profile_requests(
        (requirement,), selection_time_ms=days * DAY
    )
    assert asdict(request) == {
        "product_id": requirement.product_id,
        "base_grid_id": requirement.base_grid_id,
        "output_grid_id": requirement.output_grid_id,
        "algorithm_version": requirement.algorithm_version,
        "start_ms": 0,
        "end_ms": days * DAY,
        "purpose": "LIVE_QUERY",
        "freshness_policy_id": requirement.freshness_policy_id,
        "availability_policy_id": None,
        "as_of_ms": None,
        "revision": None,
        "pinned_manifest": None,
    }
    with pytest.raises(ProfileSelectionError):
        plan_live_profile_requests((requirement,), selection_time_ms=days * DAY - 1)


@pytest.mark.parametrize(
    "stamp,end", [(2 * DAY - 1, DAY), (2 * DAY, 2 * DAY), (MAX, MAX // DAY * DAY)]
)
def test_floor_window_boundaries(stamp, end):
    (request,) = plan_live_profile_requests((REQUIREMENT,), selection_time_ms=stamp)
    assert (request.start_ms, request.end_ms) == (end - DAY, end)


def test_auxiliary_product_is_not_bound_to_strategy_or_registry():
    requirement = replace(REQUIREMENT, product_id="BINANCE:ETHUSDT-SPOT")
    (request,) = plan_live_profile_requests((requirement,), selection_time_ms=DAY)
    assert request.product_id == requirement.product_id
    assert request.output_grid_id != request.base_grid_id


def test_permutations_and_duplicate_rejection():
    entries = (
        REQUIREMENT,
        replace(REQUIREMENT, window_days=7),
        replace(REQUIREMENT, base_grid_id="other"),
    )
    expected = tuple(sorted(entries, key=lambda entry: entry.canonical_bytes))
    outputs = []
    for ordering in permutations(entries):
        planned = plan_live_profile_requests(ordering, selection_time_ms=90 * DAY)
        assert tuple(
            (r.base_grid_id, (r.end_ms - r.start_ms) // DAY) for r in planned
        ) == tuple((r.base_grid_id, r.window_days) for r in expected)
        outputs.append(planned)
    assert all(output == outputs[0] for output in outputs)
    for duplicate in (REQUIREMENT, replace(REQUIREMENT)):
        with pytest.raises(ProfileSelectionError):
            plan_live_profile_requests((REQUIREMENT, duplicate), selection_time_ms=DAY)


@pytest.mark.parametrize("stamp", [0, DAY - 1, DAY, MAX])
def test_empty_legal_clock(stamp):
    assert plan_live_profile_requests((), selection_time_ms=stamp) == ()


@pytest.mark.parametrize(
    "bad", [True, -1, MAX + 1, 0.0, "0", type("Int", (int,), {})(0)]
)
def test_empty_still_requires_exact_clock(bad):
    with pytest.raises(ProfileSelectionError):
        plan_live_profile_requests((), selection_time_ms=bad)


def test_exact_container_and_item_types():
    child = type("Child", (ProfileRequirement,), {})(**asdict(REQUIREMENT))
    for bad in (
        [REQUIREMENT],
        type("Tuple", (tuple,), {})((REQUIREMENT,)),
        (child,),
        (None,),
        None,
    ):
        with pytest.raises(ProfileSelectionError):
            plan_live_profile_requests(cast(Any, bad), selection_time_ms=DAY)


@pytest.mark.parametrize(
    "invalid",
    [
        replace(REQUIREMENT, freshness_policy_id="unknown"),
        replace(REQUIREMENT, window_days=90),
    ],
)
def test_mixed_input_is_atomic_error_and_unchanged(invalid):
    for entries in ((REQUIREMENT, invalid), (invalid, REQUIREMENT)):
        before = tuple(entry.canonical_bytes for entry in entries)
        with pytest.raises(
            ProfileSelectionError, match="^PROFILE_SELECTION_INVALID$"
        ) as error:
            plan_live_profile_requests(entries, selection_time_ms=DAY)
        assert error.value.__cause__ is None
        assert tuple(entry.canonical_bytes for entry in entries) == before
