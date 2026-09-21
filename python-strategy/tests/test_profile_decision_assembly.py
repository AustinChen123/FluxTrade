from dataclasses import fields, replace
from itertools import permutations
from typing import Any

import pytest

from src.core.market_data.profiles.live_validation import (
    assemble_live_profile_decisions,
    ProfileQueryError,
)
from src.core.market_data.profiles.decision_context import StrategyMarketDataContext
from test_profile_decision_context import (
    item as base_item,
    unavailable as base_unavailable,
    B,
    S,
    DAY,
)


def item(basis=B.LIVE_OBSERVED):
    value = base_item(basis)
    return (
        replace(
            value,
            request=replace(
                value.request, freshness_policy_id="utc_complete_strict_v1"
            ),
        )
        if basis == B.LIVE_OBSERVED
        else value
    )


def unavailable(**kwargs):
    value = base_unavailable(**kwargs)
    return replace(
        value,
        request=replace(value.request, freshness_policy_id="utc_complete_strict_v1"),
    )


def assembly(plans: Any, decisions: Any, time: Any = DAY + 3):
    return assemble_live_profile_decisions(plans, decisions, decision_time_ms=time)


def entries():
    fresh = item()
    missing = unavailable()
    missing = replace(missing, request=replace(missing.request, revision=1))
    invalid = unavailable(status=S.INVALID, reason="INVALID_PROFILE")
    invalid = replace(invalid, request=replace(invalid.request, output_grid_id="other"))
    return fresh, missing, invalid


def test_permutations_exact_coverage_all_statuses_and_unchanged():
    decisions = entries()
    plans = tuple(decision.request for decision in decisions)
    before = tuple(decision.canonical_bytes for decision in decisions)
    expected = StrategyMarketDataContext(DAY + 3, decisions)
    for plan_order in permutations(plans):
        for decision_order in permutations(decisions):
            result = assembly(plan_order, decision_order)
            assert result == expected
            assert result.canonical_bytes == expected.canonical_bytes
            assert result.digest == expected.digest
            assert all(
                any(value is original for original in decisions)
                for value in result.profiles
            )
    assert tuple(decision.canonical_bytes for decision in decisions) == before


@pytest.mark.parametrize("time", [0, DAY, (1 << 63) - 1])
def test_empty(time):
    assert assembly((), (), time) == StrategyMarketDataContext(time, ())


@pytest.mark.parametrize("time", [True, -1, 1 << 63, 0.0, type("Int", (int,), {})(0)])
def test_exact_clock_even_empty(time):
    with pytest.raises(ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
        assembly((), (), time)


def test_missing_extra_duplicate_and_equal_copy():
    first, second, _ = entries()
    alternate = replace(unavailable(), request=first.request)
    assert alternate.request is first.request
    plans = (first.request, second.request)
    cases = [
        (plans, (first,)),
        ((first.request,), (first, second)),
        (plans, (first, first)),
        ((first.request, first.request), (first,)),
        ((first.request, replace(first.request)), (first,)),
        ((replace(first.request),), (first,)),
        ((first.request,), (replace(first, request=replace(first.request)),)),
        ((first.request,), (first, alternate)),
        ((first.request,), (alternate, first)),
        ((), (first,)),
        ((first.request,), ()),
    ]
    for requests, decisions in cases:
        with pytest.raises(
            ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"
        ) as error:
            assembly(requests, decisions)
        assert error.value.__cause__ is None


def test_exact_containers_and_nested_types():
    first = item()
    child = type("Child", (type(first),), {})(
        **{f.name: getattr(first, f.name) for f in fields(first)}
    )
    request_child = type("RequestChild", (type(first.request),), {})(
        **{f.name: getattr(first.request, f.name) for f in fields(first.request)}
    )
    for plans, decisions in (
        ([first.request], (first,)),
        ((first.request,), [first]),
        (type("Tuple", (tuple,), {})((first.request,)), (first,)),
        ((first.request,), type("Tuple", (tuple,), {})((first,))),
        ((request_child,), (first,)),
        ((first.request,), (child,)),
        ((None,), (first,)),
        ((first.request,), (None,)),
    ):
        with pytest.raises(ProfileQueryError):
            assembly(plans, decisions)


def test_basis_policy_and_time_mismatch():
    first = item()
    modeled = item(B.MODELED)
    wrong_policy = replace(
        first, request=replace(first.request, freshness_policy_id="other")
    )
    for decision in (modeled, wrong_policy):
        with pytest.raises(ProfileQueryError):
            assembly((decision.request,), (decision,))
    with pytest.raises(ProfileQueryError):
        assembly((first.request,), (replace(first, decision_time_ms=DAY + 4),))
