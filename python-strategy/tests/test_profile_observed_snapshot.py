from dataclasses import FrozenInstanceError, fields, replace
from typing import Any, cast
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles.live_query import ProfileQueryError
from src.core.market_data.profiles.observed_snapshot import ObservedProfileSnapshot
from src.core.market_data.profiles.wire import ProfileWireEvidence
from test_profile_wire import DAY, sample


def observed():
    request, validated, _ = sample()
    selection = replace(validated.query.selection, available_at_ms=DAY)
    validated = replace(validated, query=replace(validated.query, selection=selection))
    return request, ObservedProfileSnapshot(
        ProfileWireEvidence(validated, DAY + 11), DAY + 12, 100
    )


def test_fresh_identity_determinism_and_immutability():
    request, snapshot = observed()
    before = repr(snapshot)
    result = snapshot.finalize(decision_time_ms=DAY + 13, current_monotonic_ms=101)
    assert result.status == "FRESH"
    assert result.request is request
    assert snapshot.evidence.validated.query.request is request
    assert result.observed_at_ms == DAY + 12
    assert result.validation_checked_at_ms == DAY + 10
    assert result == snapshot.finalize(
        decision_time_ms=DAY + 13, current_monotonic_ms=101
    )
    assert repr(snapshot) == before
    with pytest.raises(FrozenInstanceError):
        cast(Any, snapshot).observed_at_ms = 0
    assert not hasattr(snapshot, "__dict__")


@pytest.mark.parametrize("case", ["served", "utc", "mono", "source"])
def test_clock_contradictions(case):
    _, snapshot = observed()
    decision, mono = DAY + 13, 101
    if case == "served":
        snapshot = replace(snapshot, observed_at_ms=DAY + 10)
    elif case == "utc":
        decision = DAY + 11
    elif case == "mono":
        mono = 99
    else:
        _, validated, _ = sample()
        snapshot = replace(snapshot, evidence=ProfileWireEvidence(validated, DAY + 11))
    result = snapshot.finalize(decision_time_ms=decision, current_monotonic_ms=mono)
    assert result.reason == "CLOCK_UNCERTAIN"
    assert result.profile is None


@pytest.mark.parametrize("clock", ["utc", "mono"])
@pytest.mark.parametrize(
    "offset,reason", [(-1, None), (0, "VALIDATION_EXPIRED"), (1, "VALIDATION_EXPIRED")]
)
def test_expiry_boundaries_including_frozen_utc(clock, offset, reason):
    _, snapshot = observed()
    expiry = snapshot.evidence.validated.validation_expires_at_ms
    decision = expiry + offset if clock == "utc" else snapshot.observed_at_ms
    mono = 100 if clock == "utc" else 100 + expiry - snapshot.observed_at_ms + offset
    result = snapshot.finalize(decision_time_ms=decision, current_monotonic_ms=mono)
    assert result.reason == reason


def test_monotonic_max_no_addition_overflow():
    _, snapshot = observed()
    snapshot = replace(snapshot, observed_monotonic_ms=2**63 - 2)
    assert (
        snapshot.finalize(
            decision_time_ms=DAY + 13, current_monotonic_ms=2**63 - 1
        ).status
        == "FRESH"
    )


class Integer(int):
    pass


@pytest.mark.parametrize("bad", [True, Integer(1), -1, 2**63, 1.0, None])
@pytest.mark.parametrize(
    "field",
    [
        "observed_at_ms",
        "observed_monotonic_ms",
        "decision_time_ms",
        "current_monotonic_ms",
    ],
)
def test_exact_clock_domain(bad, field):
    _, snapshot = observed()
    with pytest.raises(ProfileQueryError, match="PROFILE_QUERY_INTEGRITY"):
        if field.startswith("observed"):
            replace(snapshot, **{field: bad})
        else:
            args = dict(decision_time_ms=DAY + 13, current_monotonic_ms=101)
            args[field] = bad
            snapshot.finalize(**args)


def test_exact_evidence():
    _, snapshot = observed()
    child = type("Child", (ProfileWireEvidence,), {})(
        **{
            field.name: getattr(snapshot.evidence, field.name)
            for field in fields(snapshot.evidence)
        }
    )
    for bad in (None, child):
        with pytest.raises(ProfileQueryError):
            replace(snapshot, evidence=cast(Any, bad))


def test_finalization_no_clock_or_io(monkeypatch):
    _, snapshot = observed()
    forbidden = Mock(side_effect=AssertionError("I/O forbidden"))
    with monkeypatch.context() as patch:
        for name in (
            "time.time_ns",
            "time.monotonic_ns",
            "socket.socket",
            "builtins.open",
        ):
            patch.setattr(name, forbidden)
        result = snapshot.finalize(decision_time_ms=DAY + 13, current_monotonic_ms=101)
    assert result.status == "FRESH"
    forbidden.assert_not_called()


@pytest.mark.parametrize("past_expiry", [0, 1])
def test_source_clock_contradiction_precedes_monotonic_expiry(past_expiry):
    _, validated, _ = sample()
    assert (
        validated.query.selection.available_at_ms > validated.validation_completed_at_ms
    )
    snapshot = ObservedProfileSnapshot(
        ProfileWireEvidence(validated, DAY + 11), DAY + 12, 100
    )
    elapsed = validated.validation_expires_at_ms - snapshot.observed_at_ms + past_expiry
    result = snapshot.finalize(
        decision_time_ms=DAY + 13, current_monotonic_ms=100 + elapsed
    )
    assert result.reason == "CLOCK_UNCERTAIN"
    assert result.profile is None
