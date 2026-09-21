from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from threading import Event
from typing import Any, cast
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles.live_query import (
    LiveProfileQueryUnavailable,
    ProfileQueryError,
)
from src.core.market_data.profiles.snapshot_cache import (
    ProfileSnapshotCache,
    ProfileRefreshResult,
)
from test_profile_observed_snapshot import observed, DAY


def setup():
    request, snapshot = observed()
    client = Mock()
    client.fetch.return_value = snapshot.evidence
    utc, mono = Mock(return_value=DAY + 12), Mock(return_value=100)
    return (
        request,
        snapshot,
        client,
        utc,
        mono,
        ProfileSnapshotCache(client, utc_ms=utc, monotonic_ms=mono),
    )


def read(cache, request, **changes):
    args = dict(decision_time_ms=DAY + 13, current_monotonic_ms=101)
    args.update(changes)
    return cache.decision(request, **args)


def test_cold_success_and_read_without_callbacks():
    request, _, client, utc, mono, cache = setup()
    assert read(cache, request).reason == "PROFILE_NOT_READY"
    client.fetch.assert_not_called()
    assert cache.refresh(request) == ProfileRefreshResult(True, "SUCCESS")
    for callback in (client.fetch, utc, mono):
        callback.side_effect = AssertionError("callback forbidden")
    first = read(cache, request)
    assert first.status == "FRESH" and first.request is request
    assert read(cache, request) == first
    for callback in (client.fetch, utc, mono):
        assert callback.call_count == 1


@pytest.mark.parametrize(
    "reason",
    [
        "NOT_READY",
        "PROFILE_EXPIRED",
        "INVALID_PROFILE",
        "QUERY_TOO_LARGE",
        "BACKEND_UNAVAILABLE",
        "SNAPSHOT_REVOKED",
    ],
)
@pytest.mark.parametrize("warm", [False, True])
def test_outcome_retention(reason, warm):
    request, snapshot, client, _, _, cache = setup()
    if warm:
        cache.refresh(request)
    client.fetch.return_value = LiveProfileQueryUnavailable(request, reason)
    assert cache.refresh(request) == ProfileRefreshResult(True, reason)
    result = read(cache, request)
    if warm and reason != "SNAPSHOT_REVOKED":
        assert result.status == "FRESH"
        assert result.observed_at_ms == DAY + 12
        assert (
            read(
                cache,
                request,
                decision_time_ms=snapshot.evidence.validated.validation_expires_at_ms,
            ).reason
            == "VALIDATION_EXPIRED"
        )
    else:
        assert result.reason == (
            "PROFILE_NOT_READY" if reason == "NOT_READY" else reason
        )


@pytest.mark.parametrize("warm", [False, True])
@pytest.mark.parametrize("failure", [RuntimeError("SECRET"), True, -1, 2**63])
def test_clock_failure_retains_last_good(warm, failure):
    request, _, _, utc, _, cache = setup()
    if warm:
        cache.refresh(request)
    if isinstance(failure, Exception):
        utc.side_effect = failure
    else:
        utc.return_value = failure
    assert cache.refresh(request).outcome == "CLOCK_UNCERTAIN"
    result = read(cache, request)
    assert result.status == "FRESH" if warm else result.reason == "CLOCK_UNCERTAIN"
    assert "SECRET" not in repr(result)


@pytest.mark.parametrize("warm", [False, True])
def test_invalid_result_and_request_identity(warm):
    request, snapshot, client, _, _, cache = setup()
    if warm:
        cache.refresh(request)
    client.fetch.return_value = None
    with pytest.raises(ProfileQueryError):
        cache.refresh(request)
    assert (
        read(cache, request).status == "FRESH"
        if warm
        else read(cache, request).reason == "PROFILE_NOT_READY"
    )
    for action in (cache.refresh, lambda req: read(cache, req)):
        with pytest.raises(ProfileQueryError):
            action(replace(request))
    client.fetch.return_value = replace(
        snapshot.evidence,
        validated=replace(
            snapshot.evidence.validated,
            query=replace(snapshot.evidence.validated.query, request=replace(request)),
        ),
    )
    with pytest.raises(ProfileQueryError):
        cache.refresh(request)


@pytest.mark.parametrize(
    "older,newer",
    [
        ("SUCCESS", "SUCCESS"),
        ("SNAPSHOT_REVOKED", "SUCCESS"),
        ("SUCCESS", "SNAPSHOT_REVOKED"),
    ],
)
def test_generation_race_without_lock_during_fetch(older, newer):
    request, snapshot, client, utc, _, cache = setup()
    utc.side_effect = [DAY + 12, DAY + 13]
    entered, release = Event(), Event()

    def result(kind):
        return (
            snapshot.evidence
            if kind == "SUCCESS"
            else LiveProfileQueryUnavailable(request, kind)
        )

    def slow(_):
        entered.set()
        assert release.wait(3)
        return result(older)

    client.fetch.side_effect = slow
    with ThreadPoolExecutor(max_workers=2) as pool:
        old = pool.submit(cache.refresh, request)
        try:
            assert entered.wait(3)
            client.fetch.side_effect = lambda _: result(newer)
            fresh = pool.submit(cache.refresh, request).result(timeout=3)
            assert fresh == ProfileRefreshResult(True, newer)
        finally:
            release.set()
        assert old.result(timeout=3) == ProfileRefreshResult(False, older)
    decision = read(cache, request)
    assert (
        decision.status == "FRESH"
        if newer == "SUCCESS"
        else decision.reason == "SNAPSHOT_REVOKED"
    )
    if newer == "SUCCESS":
        assert decision.observed_at_ms == DAY + 12


@pytest.mark.parametrize("bad", [True, -1, 2**63, 1.0])
def test_invalid_decision_clocks(bad):
    request, _, _, _, _, cache = setup()
    for field in ("decision_time_ms", "current_monotonic_ms"):
        with pytest.raises(ProfileQueryError):
            read(cache, request, **{field: bad})
    with pytest.raises(ProfileQueryError):
        cache.refresh(cast(Any, bad))


def test_base_exception_not_caught():
    request, _, client, _, _, cache = setup()
    client.fetch.side_effect = KeyboardInterrupt()
    with pytest.raises(KeyboardInterrupt):
        cache.refresh(request)


@pytest.mark.parametrize("copy", [False, True])
@pytest.mark.parametrize("utc_delta", [0, -1])
def test_equal_evidence_never_renews_expired_anchor(copy, utc_delta):
    request, snapshot, client, utc, mono, cache = setup()
    cache.refresh(request)
    expiry_mono = (
        100 + snapshot.evidence.validated.validation_expires_at_ms - (DAY + 12)
    )
    assert (
        read(cache, request, current_monotonic_ms=expiry_mono).reason
        == "VALIDATION_EXPIRED"
    )
    client.fetch.return_value = (
        replace(snapshot.evidence) if copy else snapshot.evidence
    )
    utc.return_value = DAY + 12 + utc_delta
    mono.return_value = expiry_mono
    assert cache.refresh(request) == ProfileRefreshResult(True, "SUCCESS")
    result = read(cache, request, current_monotonic_ms=expiry_mono)
    assert result.reason == "VALIDATION_EXPIRED"
    assert result.profile is None


@pytest.mark.parametrize("newer", ["SUCCESS", "DUPLICATE", "BACKEND_UNAVAILABLE"])
def test_pending_revoke_follows_success_generation_not_latest_attempt(newer):
    request, snapshot, client, _, _, cache = setup()
    cache.refresh(request)
    entered, release = Event(), Event()

    def revoke(_):
        entered.set()
        assert release.wait(3)
        return LiveProfileQueryUnavailable(request, "SNAPSHOT_REVOKED")

    client.fetch.side_effect = revoke
    with ThreadPoolExecutor(max_workers=2) as pool:
        pending = pool.submit(cache.refresh, request)
        try:
            assert entered.wait(3)
            client.fetch.side_effect = None
            client.fetch.return_value = (
                newer_evidence(snapshot.evidence)
                if newer == "SUCCESS"
                else replace(snapshot.evidence, served_at_ms=DAY + 12)
                if newer == "DUPLICATE"
                else LiveProfileQueryUnavailable(request, newer)
            )
            assert pool.submit(cache.refresh, request).result(timeout=3).applied
        finally:
            release.set()
        assert pending.result(timeout=3) == ProfileRefreshResult(
            newer != "SUCCESS", "SNAPSHOT_REVOKED"
        )
    result = read(cache, request)
    assert (
        result.status == "FRESH"
        if newer == "SUCCESS"
        else result.reason == "SNAPSHOT_REVOKED"
    )


@pytest.mark.parametrize("warm", [False, True])
def test_client_programming_exception_propagates_without_replacement(warm):
    from src.core.market_data.profiles.snapshot_client import ProfileSnapshotClientError

    request, _, client, utc, mono, cache = setup()
    if warm:
        cache.refresh(request)
    before = read(cache, request)
    error = ProfileSnapshotClientError()
    client.fetch.side_effect = error
    with pytest.raises(ProfileSnapshotClientError) as caught:
        cache.refresh(request)
    assert caught.value is error
    assert read(cache, request) == before
    assert utc.call_count == mono.call_count == int(warm)


def newer_evidence(evidence):
    validated = evidence.validated
    selection = replace(validated.query.selection, decision_time_ms=DAY + 1)
    return replace(
        evidence,
        validated=replace(
            validated,
            query=replace(validated.query, selection=selection),
            validation_started_at_ms=DAY + 1,
        ),
    )


@pytest.mark.parametrize("variant", ["same", "copy", "served"])
def test_revoked_validation_cannot_replay(variant):
    request, snapshot, client, _, _, cache = setup()
    cache.refresh(request)
    client.fetch.return_value = LiveProfileQueryUnavailable(request, "SNAPSHOT_REVOKED")
    cache.refresh(request)
    evidence = snapshot.evidence
    if variant == "copy":
        evidence = replace(evidence, validated=replace(evidence.validated))
    elif variant == "served":
        evidence = replace(evidence, served_at_ms=DAY + 12)
    client.fetch.return_value = evidence
    assert cache.refresh(request) == ProfileRefreshResult(False, "SUCCESS")
    assert read(cache, request).reason == "SNAPSHOT_REVOKED"
    for reason in (
        "NOT_READY",
        "BACKEND_UNAVAILABLE",
        "PROFILE_EXPIRED",
        "INVALID_PROFILE",
        "QUERY_TOO_LARGE",
    ):
        client.fetch.return_value = LiveProfileQueryUnavailable(request, reason)
        cache.refresh(request)
        assert read(cache, request).reason == "SNAPSHOT_REVOKED"
    client.fetch.return_value = newer_evidence(evidence)
    assert cache.refresh(request).applied
    assert read(cache, request).status == "FRESH"


def test_served_only_change_cannot_renew_expiry():
    request, snapshot, client, _, mono, cache = setup()
    cache.refresh(request)
    mono.return_value = (
        100 + snapshot.evidence.validated.validation_expires_at_ms - (DAY + 12)
    )
    client.fetch.return_value = replace(snapshot.evidence, served_at_ms=DAY + 12)
    cache.refresh(request)
    assert (
        read(cache, request, current_monotonic_ms=mono.return_value).reason
        == "VALIDATION_EXPIRED"
    )


@pytest.mark.parametrize("revoked", [False, True])
def test_validation_high_water_rejects_server_clock_rollback(revoked):
    request, snapshot, client, _, mono, cache = setup()
    cache.refresh(request)  # A
    newer = newer_evidence(snapshot.evidence)
    client.fetch.return_value = newer
    assert cache.refresh(request).applied  # B
    if revoked:
        client.fetch.return_value = LiveProfileQueryUnavailable(
            request, "SNAPSHOT_REVOKED"
        )
        cache.refresh(request)
    mono.return_value = 100 + newer.validated.validation_expires_at_ms - (DAY + 12)
    before = read(cache, request, current_monotonic_ms=mono.return_value)
    assert before.reason == ("SNAPSHOT_REVOKED" if revoked else "VALIDATION_EXPIRED")
    client.fetch.return_value = (
        snapshot.evidence
    )  # Server validation clock rolled back.
    assert cache.refresh(request) == ProfileRefreshResult(False, "SUCCESS")
    assert read(cache, request, current_monotonic_ms=mono.return_value) == before
    client.fetch.return_value = replace(newer, served_at_ms=DAY + 12)
    assert cache.refresh(request).applied is (not revoked)
    assert read(cache, request, current_monotonic_ms=mono.return_value) == before


def test_same_start_different_validation_rejected_without_anchor_change():
    request, snapshot, client, _, mono, cache = setup()
    cache.refresh(request)
    before = read(cache, request)
    client.fetch.return_value = replace(
        snapshot.evidence,
        validated=replace(
            snapshot.evidence.validated,
            validation_elapsed_ms=21,
        ),
    )
    mono.return_value = 1000
    assert cache.refresh(request) == ProfileRefreshResult(False, "SUCCESS")
    assert read(cache, request) == before
