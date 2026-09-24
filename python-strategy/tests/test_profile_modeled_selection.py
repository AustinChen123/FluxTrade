from dataclasses import replace
from datetime import timedelta

import pytest

from src.core.market_data.profiles import modeled_selection as modeled
from src.core.market_data.profiles.read_results import VerifiedManifestRead
from src.core.market_data.profiles.read_types import ProfileQueryRequest
from src.core.market_data.profiles.selection import ProfileSelectionError
from test_profile_read_results import DAY, NOW, event, manifest

DELAY = 20 * 60 * 1000
MAX = (1 << 63) - 1


def request(read: VerifiedManifestRead, as_of_ms: int, *, policy: str = modeled.MODELED_DAILY_DELAY_20M_V1,
            product_id: str | None = None) -> ProfileQueryRequest:
    value = read.manifest
    return ProfileQueryRequest(
        product_id or value.product_id, value.base_grid_id, value.base_grid_id,
        value.algorithm_version, value.days[0].window_start_ms, value.days[-1].window_end_ms,
        "MODELED_RESEARCH", availability_policy_id=policy, as_of_ms=as_of_ms,
    )


def test_policy_contract_is_closed_and_golden() -> None:
    policy = modeled.MODELED_AVAILABILITY_POLICY
    assert policy.canonical_bytes == (
        b'{"daily_delay_ms":1200000,"policy_id":"utc_daily_delay_20m_v1",'
        b'"schema_version":1,"semantics":"FIXED_DATASET_REPLAY"}'
    )
    assert policy.digest == "81194996768585c332c0873d1e92f1855cb928a9607b165a640fe3e9fb092600"
    for change in ({"daily_delay_ms": DELAY + 1}, {"policy_id": "other"}, {"semantics": "OBSERVED"}):
        with pytest.raises(ProfileSelectionError):
            replace(policy, **change)


@pytest.mark.parametrize("count", [1, 7, 30, 90])
@pytest.mark.parametrize("offset", [-1, 0, 1])
def test_last_day_controls_exact_availability_boundary(count: int, offset: int) -> None:
    read = manifest(count)
    result = modeled.select_modeled_profile(request(read, count * DAY + DELAY + offset), read)
    if offset < 0:
        assert result == modeled.ModeledProfileSelectionUnavailable("PROFILE_NOT_READY")
    else:
        assert isinstance(result, modeled.ModeledProfileSelection)
        assert result.manifest is read.manifest
        assert result.available_at_ms == count * DAY + DELAY
        assert result.policy_digest == modeled.MODELED_AVAILABILITY_POLICY.digest


def test_publication_clocks_do_not_change_modeled_eligibility() -> None:
    read = manifest(2)
    shifted = replace(read, days=tuple(replace(day, computed_at=NOW + timedelta(days=100),
                                              published_at=NOW + timedelta(days=200)) for day in read.days))
    original = modeled.select_modeled_profile(request(read, 2 * DAY + DELAY), read)
    changed = modeled.select_modeled_profile(request(shifted, 2 * DAY + DELAY), shifted)
    assert original == changed


def test_pinned_revision_is_returned_without_replacement() -> None:
    read = manifest(2)
    days = tuple(replace(day, ref=replace(day.ref, revision=7)) for day in read.days)
    pinned = replace(read.manifest, days=tuple(day.ref for day in days))
    revised = VerifiedManifestRead(pinned, days)
    result = modeled.select_modeled_profile(request(revised, 2 * DAY + DELAY), revised)
    assert isinstance(result, modeled.ModeledProfileSelection)
    assert tuple(ref.revision for ref in result.manifest.days) == (7, 7)


def test_explicit_single_day_revision_must_match_pin() -> None:
    read = manifest()
    value = replace(request(read, DAY + DELAY), revision=1)
    result = modeled.select_modeled_profile(value, read)
    assert isinstance(result, modeled.ModeledProfileSelection)
    with pytest.raises(ProfileSelectionError):
        modeled.select_modeled_profile(replace(value, revision=99), read)


def test_revoked_input_is_invalid_without_fallback() -> None:
    read = manifest(2)
    revoked = replace(read, days=(replace(read.days[0], invalidations=(event(),)), read.days[1]))
    assert modeled.select_modeled_profile(request(revoked, 2 * DAY + DELAY), revoked) == (
        modeled.ModeledProfileSelectionUnavailable("SNAPSHOT_REVOKED")
    )


@pytest.mark.parametrize("mode", ["policy", "scope", "window", "future", "type"])
def test_invalid_requests_fail_closed(mode: str) -> None:
    read = manifest(2)
    value = request(read, 2 * DAY + DELAY)
    if mode == "policy":
        value = request(read, 2 * DAY + DELAY, policy="unknown")
    elif mode == "scope":
        value = request(read, 2 * DAY + DELAY, product_id="BINANCE:ETHUSDT-SPOT")
    elif mode == "window":
        value = replace(value, start_ms=DAY)
    elif mode == "future":
        value = replace(value, as_of_ms=2 * DAY - 1)
    with pytest.raises(ProfileSelectionError):
        modeled.select_modeled_profile(value if mode != "type" else object(), read)  # type: ignore[arg-type]


def test_selection_timestamp_overflow_fails_closed() -> None:
    read = manifest()
    with pytest.raises(ProfileSelectionError):
        modeled.ModeledProfileSelection(
            read.manifest,
            MAX,
            MAX + 1,
            modeled.MODELED_AVAILABILITY_POLICY.policy_id,
            modeled.MODELED_AVAILABILITY_POLICY.digest,
        )


def test_public_dtos_reject_string_subclasses() -> None:
    class SubStr(str):
        pass

    policy = modeled.MODELED_AVAILABILITY_POLICY
    read = manifest()
    with pytest.raises(ProfileSelectionError):
        replace(policy, policy_id=SubStr(policy.policy_id))
    with pytest.raises(ProfileSelectionError):
        replace(policy, semantics=SubStr(policy.semantics))
    with pytest.raises(ProfileSelectionError):
        replace(
            modeled.select_modeled_profile(request(read, DAY + DELAY), read),
            policy_digest=SubStr(policy.digest),
        )
    with pytest.raises(ProfileSelectionError):
        modeled.ModeledProfileSelectionUnavailable(SubStr("PROFILE_NOT_READY"))
