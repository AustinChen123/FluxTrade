from dataclasses import replace
from itertools import permutations
from concurrent.futures import ThreadPoolExecutor
from threading import Event
from typing import Any, cast

import pytest

from src.core.market_data.profiles.live_query import (
    LiveProfileQueryUnavailable,
    ProfileQueryError,
)
from src.core.market_data.profiles import snapshot_cache as owner
from test_profile_snapshot_cache import setup, DAY


def plans(request):
    return (
        request,
        replace(request, revision=1),
        replace(request, output_grid_id="other"),
    )


def decide(cache, requests, time=DAY + 13):
    return cache.decision_many(
        requests, decision_time_ms=time, current_monotonic_ms=101
    )


def test_mixed_batch_permutation_and_no_callback_io():
    request, snapshot, client, utc, mono, cache = setup()
    requests = plans(request)
    client.fetch.side_effect = [
        snapshot.evidence,
        LiveProfileQueryUnavailable(requests[1], "NOT_READY"),
        LiveProfileQueryUnavailable(requests[2], "SNAPSHOT_REVOKED"),
    ]
    assert [r.outcome for r in cache.refresh_many(requests)] == [
        "SUCCESS",
        "NOT_READY",
        "SNAPSHOT_REVOKED",
    ]
    assert [call.args[0] for call in client.fetch.call_args_list] == list(requests)
    for callback in (client.fetch, utc, mono):
        callback.side_effect = AssertionError("forbidden callback")
    expected = decide(cache, requests)
    assert {item.status.value for item in expected.profiles} == {
        "FRESH",
        "MISSING",
        "INVALID",
    }
    for order in permutations(requests):
        assert decide(cache, order) == expected
    assert client.fetch.call_count == 3 and utc.call_count == mono.call_count == 1


def test_empty_and_invalid_plan_preflight():
    request, _, client, _, _, cache = setup()
    assert cache.refresh_many(()) == ()
    assert decide(cache, ()).profiles == ()
    for bad in (
        [request],
        (request, request),
        (request, replace(request)),
        (None,),
        (request, replace(request, start_ms=DAY, end_ms=2 * DAY)),
    ):
        with pytest.raises(ProfileQueryError):
            cache.refresh_many(cast(Any, bad))
        with pytest.raises(ProfileQueryError):
            decide(cache, bad)
    client.fetch.assert_not_called()
    cache.refresh_many((request,))
    for operation in (cache.refresh_many, lambda value: decide(cache, value)):
        with pytest.raises(ProfileQueryError):
            operation((replace(request),))


def test_error_propagates_without_fetching_remaining():
    request, snapshot, client, _, _, cache = setup()
    requests = plans(request)
    error = RuntimeError("SECRET")
    client.fetch.side_effect = [snapshot.evidence, error]
    with pytest.raises(RuntimeError) as caught:
        cache.refresh_many(requests)
    assert caught.value is error and client.fetch.call_count == 2
    outcomes = {id(item.request): item for item in decide(cache, requests).profiles}
    assert outcomes[id(requests[0])].status == "FRESH"
    assert outcomes[id(requests[0])].observed_at_ms == DAY + 12
    assert outcomes[id(requests[1])].reason == "PROFILE_NOT_READY"
    assert outcomes[id(requests[2])].reason == "PROFILE_NOT_READY"
    assert all(
        call.args[0] is req
        for call, req in zip(client.fetch.call_args_list, requests[:2], strict=True)
    )


def test_atomic_state_copy_before_finalize(monkeypatch):
    request, snapshot, client, _, _, cache = setup()
    requests = plans(request)[:2]
    cache.refresh_many((request,))
    original = owner.ProfileSnapshotCache._finalize
    calls = []

    def finalize(req, state, utc, mono):
        if not calls:
            client.fetch.return_value = LiveProfileQueryUnavailable(
                requests[1], "SNAPSHOT_REVOKED"
            )
            cache.refresh(requests[1])
        calls.append(req)
        return original(req, state, utc, mono)

    monkeypatch.setattr(owner.ProfileSnapshotCache, "_finalize", staticmethod(finalize))
    first = decide(cache, requests)
    assert {item.reason for item in first.profiles} == {None, "PROFILE_NOT_READY"}
    assert {item.reason for item in decide(cache, requests).profiles} == {
        None,
        "SNAPSHOT_REVOKED",
    }
    assert decide(cache, (request,), 2 * DAY).profiles[0].reason == "VALIDATION_EXPIRED"


@pytest.mark.parametrize("bad", [True, -1, 2**63, 1.0])
def test_empty_still_validates_clocks(bad):
    *_, cache = setup()
    with pytest.raises(ProfileQueryError):
        cache.decision_many((), decision_time_ms=bad, current_monotonic_ms=0)
    with pytest.raises(ProfileQueryError):
        cache.decision_many((), decision_time_ms=0, current_monotonic_ms=bad)


def test_batch_refresh_preserves_generation_fence():
    request, snapshot, client, _, _, cache = setup()
    entered, release = Event(), Event()

    def slow(_):
        entered.set()
        assert release.wait(3)
        return snapshot.evidence

    client.fetch.side_effect = slow
    with ThreadPoolExecutor(max_workers=2) as pool:
        old = pool.submit(cache.refresh_many, (request,))
        try:
            assert entered.wait(3)
            client.fetch.side_effect = None
            client.fetch.return_value = LiveProfileQueryUnavailable(
                request, "SNAPSHOT_REVOKED"
            )
            assert (
                pool.submit(cache.refresh_many, (request,)).result(timeout=3)[0].applied
            )
        finally:
            release.set()
        assert not old.result(timeout=3)[0].applied
    assert decide(cache, (request,)).profiles[0].reason == "SNAPSHOT_REVOKED"


@pytest.mark.parametrize(
    "first_batch,second_batch", [(True, True), (False, True), (True, False)]
)
def test_cold_reads_pin_identity(first_batch, second_batch):
    request, _, _, _, _, cache = setup()

    def read(req, batch):
        return (
            decide(cache, (req,))
            if batch
            else cache.decision(
                req, decision_time_ms=DAY + 13, current_monotonic_ms=101
            )
        )

    read(request, first_batch)
    with pytest.raises(ProfileQueryError):
        read(replace(request), second_batch)


@pytest.mark.parametrize("failure", ["invalid", "copy", "window"])
def test_invalid_batch_does_not_partially_register(failure):
    request, _, _, _, _, cache = setup()
    if failure == "copy":
        existing = replace(request, revision=1)
        decide(cache, (existing,))
        bad = replace(existing)
    elif failure == "window":
        bad = replace(request, start_ms=DAY, end_ms=2 * DAY)
    else:
        bad = None
    with pytest.raises(ProfileQueryError):
        decide(cache, (request, bad))
    copy = replace(request)
    assert decide(cache, (copy,)).profiles[0].reason == "PROFILE_NOT_READY"
    with pytest.raises(ProfileQueryError):
        decide(cache, (request,))
