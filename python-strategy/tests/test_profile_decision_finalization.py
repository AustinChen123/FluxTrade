from dataclasses import fields, replace
from typing import Any, cast

import pytest

from src.core.market_data.profiles import live_validation as owner
from test_profile_live_validation import fixture


def evidence(monkeypatch, *, start_offset=0):
    request, _, query, _, start = fixture(monkeypatch)
    start += start_offset
    selection = replace(query.selection, available_at_ms=start, decision_time_ms=start)
    result = owner.ValidatedLiveProfileQuery(
        replace(query, selection=selection), start, start, 0
    )
    return request, result, start


@pytest.mark.parametrize(
    "cls,reason,status,expected",
    [
        (
            owner.LiveProfileQueryUnavailable,
            "NOT_READY",
            "MISSING",
            "PROFILE_NOT_READY",
        ),
        *[
            (owner.LiveProfileQueryUnavailable, r, "MISSING", r)
            for r in ("PROFILE_EXPIRED", "BACKEND_UNAVAILABLE")
        ],
        *[
            (owner.LiveProfileQueryUnavailable, r, "INVALID", r)
            for r in ("SNAPSHOT_REVOKED", "INVALID_PROFILE", "QUERY_TOO_LARGE")
        ],
        *[
            (owner.LiveProfileValidationUnavailable, r, "MISSING", r)
            for r in ("CLOCK_UNCERTAIN", "VALIDATION_EXPIRED")
        ],
    ],
)
def test_unavailable_complete_mapping(monkeypatch, cls, reason, status, expected):
    request, _, start = evidence(monkeypatch)
    result = cls(reason)
    decision = owner.finalize_live_profile_decision(
        request, result, observed_at_ms=None, decision_time_ms=start
    )
    assert decision.status == status and decision.reason == expected
    assert (
        decision.profile
        is decision.available_at_ms
        is decision.validation_checked_at_ms
        is decision.observed_at_ms
        is None
    )
    with pytest.raises(owner.ProfileQueryError):
        owner.finalize_live_profile_decision(
            request, result, observed_at_ms=start, decision_time_ms=start
        )


@pytest.mark.parametrize(
    "offset,reason",
    [
        (0, None),
        (299999, None),
        (300000, "VALIDATION_EXPIRED"),
        (300001, "VALIDATION_EXPIRED"),
    ],
)
def test_expiry_equality_and_exact_evidence(monkeypatch, offset, reason):
    request, result, start = evidence(monkeypatch)
    decision = owner.finalize_live_profile_decision(
        request, result, observed_at_ms=start, decision_time_ms=start + offset
    )
    assert decision.reason == reason
    if reason is None:
        assert decision.profile is result.query.profile
        assert decision.request is request
        assert (
            decision.available_at_ms
            == decision.validation_checked_at_ms
            == decision.observed_at_ms
            == start
        )
        assert decision.decision_time_ms == start + offset


@pytest.mark.parametrize("kind", ["available", "completed", "observed"])
def test_clock_contradiction_precedes_expiry(monkeypatch, kind):
    request, result, start = evidence(monkeypatch)
    observed, decision = start, start + 300000
    if kind == "available":
        result = replace(
            result,
            query=replace(
                result.query,
                selection=replace(result.query.selection, available_at_ms=start + 1),
            ),
        )
    elif kind == "completed":
        result = replace(
            result, validation_completed_at_ms=start + 1, validation_elapsed_ms=1
        )
    else:
        observed = decision + 1
    finalized = owner.finalize_live_profile_decision(
        request, result, observed_at_ms=observed, decision_time_ms=decision
    )
    assert finalized.reason == "CLOCK_UNCERTAIN"


def test_midnight_crossing_with_valid_lease(monkeypatch):
    request, result, start = evidence(monkeypatch, start_offset=86400000 - 1)
    assert start + 1 < result.validation_expires_at_ms
    decision = owner.finalize_live_profile_decision(
        request, result, observed_at_ms=start, decision_time_ms=start + 1
    )
    assert decision.reason == "PROFILE_EXPIRED"


@pytest.mark.parametrize("bad", [True, -1, 1 << 63, 1.0, type("Int", (int,), {})(0)])
@pytest.mark.parametrize("field", ["observed_at_ms", "decision_time_ms"])
def test_exact_clock_domain(monkeypatch, bad, field):
    request, result, start = evidence(monkeypatch)
    kwargs: dict[str, Any] = dict(observed_at_ms=start, decision_time_ms=start)
    kwargs[field] = bad
    with pytest.raises(owner.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"):
        owner.finalize_live_profile_decision(request, result, **kwargs)


def test_exact_request_result_and_unavailable_validation(monkeypatch):
    request, result, start = evidence(monkeypatch)
    child = type("Child", (type(result),), {})(
        **{f.name: getattr(result, f.name) for f in fields(result)}
    )
    for bad in (None, True, child):
        with pytest.raises(owner.ProfileQueryError):
            owner.finalize_live_profile_decision(
                request, cast(Any, bad), observed_at_ms=start, decision_time_ms=start
            )
    with pytest.raises(owner.ProfileQueryError):
        owner.finalize_live_profile_decision(
            replace(request), result, observed_at_ms=start, decision_time_ms=start
        )
    with pytest.raises(owner.ProfileQueryError):
        owner.finalize_live_profile_decision(
            request, result, observed_at_ms=None, decision_time_ms=start
        )
    unavailable = owner.LiveProfileQueryUnavailable("NOT_READY")
    for bad_request in (
        None,
        replace(request, freshness_policy_id="other"),
        replace(
            request,
            purpose="MODELED_RESEARCH",
            freshness_policy_id=None,
            availability_policy_id="modeled",
            as_of_ms=start,
        ),
    ):
        with pytest.raises(owner.ProfileQueryError):
            owner.finalize_live_profile_decision(
                cast(Any, bad_request),
                unavailable,
                observed_at_ms=None,
                decision_time_ms=start,
            )


def test_unavailable_subclasses_and_clock_validation(monkeypatch):
    request, _, start = evidence(monkeypatch)
    child_request = type("RequestChild", (type(request),), {})(
        **{f.name: getattr(request, f.name) for f in fields(request)}
    )
    for cls, reason in (
        (owner.LiveProfileQueryUnavailable, "NOT_READY"),
        (owner.LiveProfileValidationUnavailable, "CLOCK_UNCERTAIN"),
    ):
        exact = cls(reason)
        child = type("UnavailableChild", (cls,), {})(reason)
        for req, value, stamp in (
            (child_request, exact, start),
            (request, child, start),
            (request, exact, True),
        ):
            with pytest.raises(
                owner.ProfileQueryError, match="^PROFILE_QUERY_INTEGRITY$"
            ):
                owner.finalize_live_profile_decision(
                    req, value, observed_at_ms=None, decision_time_ms=stamp
                )
