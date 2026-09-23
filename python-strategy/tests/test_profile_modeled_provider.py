from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
import json
from typing import Any, cast

import pytest

from src.core.market_data.profiles import modeled_provider as owner
from src.core.market_data.profiles.composite_types import CompositeProfile
from src.core.market_data.profiles.decision_context import ProfileDecisionStatus
from src.core.market_data.profiles.grid import resolve_profile_grid
from src.core.market_data.profiles.modeled_selection import MODELED_DAILY_DELAY_20M_V1
from src.core.market_data.profiles.read_results import VerifiedManifestRead
from src.core.market_data.profiles.read_types import DailyProfileRef, OrderedProfileManifest
from src.core.market_data.profiles.requirements import ProfileRequirement
from test_profile_read_results import DAY, event, manifest

DELAY = 20 * 60 * 1000


class Reader:
    def __init__(self, outputs) -> None:
        self.outputs = iter(outputs)
        self.calls = []

    def get_manifest(self, pinned):
        self.calls.append(pinned)
        value = next(self.outputs)
        if isinstance(value, BaseException):
            raise value
        return value


def requirement(days: int = 1, *, output: str = "btc_spot_usdt_10_v1") -> ProfileRequirement:
    return ProfileRequirement("BINANCE:BTCUSDT-SPOT", "btc_spot_usdt_10_v1", output,
                              "vp-v1", days, "utc_complete_strict_v1")


def fake(read) -> CompositeProfile:
    value = read.manifest
    grid = resolve_profile_grid(value.product_id, value.base_grid_id, value.algorithm_version)
    return CompositeProfile(value, grid, (), Decimal(0), Decimal(0), 0, None)


def reading(count: int = 1) -> VerifiedManifestRead:
    days = []
    for day in manifest(count).days:
        content = replace(day.publication.content, grid_id="btc_spot_usdt_10_v1",
                          bin_origin=Decimal(0), bin_step=Decimal(10))
        digest = content.content_sha256
        ref = DailyProfileRef(digest, day.ref.revision, digest,
                              day.ref.window_start_ms, day.ref.window_end_ms)
        days.append(replace(day, ref=ref, publication=replace(day.publication, content=content)))
    first = days[0].publication.content
    pinned = OrderedProfileManifest(first.product_id, first.grid_id, first.algorithm_version,
                                    tuple(day.ref for day in days))
    return VerifiedManifestRead(pinned, tuple(days))


@pytest.mark.parametrize("count", [1, 7, 30, 90])
@pytest.mark.parametrize("offset,status", [(-1, ProfileDecisionStatus.MISSING),
                                             (0, ProfileDecisionStatus.FRESH),
                                             (1, ProfileDecisionStatus.FRESH)])
def test_exact_windows_and_availability_are_precomposed_once(monkeypatch, count, offset, status) -> None:
    calls = []
    monkeypatch.setattr(owner, "compose_profile", lambda read: calls.append(read) or fake(read))
    provider = owner.PreloadedModeledProfileProvider((reading(count),))
    decision = count * DAY + DELAY + offset
    first = provider.context_for((requirement(count),), decision_time_ms=decision,
                                 availability_policy_id=MODELED_DAILY_DELAY_20M_V1)
    second = provider.context_for((requirement(count),), decision_time_ms=decision,
                                  availability_policy_id=MODELED_DAILY_DELAY_20M_V1)
    assert first == second and first.profiles[0].status is status and len(calls) == 1
    if status is ProfileDecisionStatus.FRESH:
        assert first.profiles[0].profile is second.profiles[0].profile
def test_missing_revoked_and_wrong_output_are_local_results(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(owner, "compose_profile", lambda read: calls.append(read) or fake(read))
    empty = owner.PreloadedModeledProfileProvider(())
    assert empty.context_for((requirement(),), decision_time_ms=DAY + DELAY,
                             availability_policy_id=MODELED_DAILY_DELAY_20M_V1).profiles[0].status is ProfileDecisionStatus.MISSING
    read = reading()
    revoked_event = replace(event(), snapshot_id=read.days[0].ref.snapshot_id)
    revoked = replace(read, days=(replace(read.days[0], invalidations=(revoked_event,)),))
    provider = owner.PreloadedModeledProfileProvider((revoked,))
    invalid = provider.context_for((requirement(),), decision_time_ms=DAY,
                                   availability_policy_id=MODELED_DAILY_DELAY_20M_V1).profiles[0]
    assert invalid.status is ProfileDecisionStatus.INVALID and invalid.reason == "SNAPSHOT_REVOKED"
    wrong = owner.PreloadedModeledProfileProvider((read,)).context_for(
        (requirement(output="other"),), decision_time_ms=DAY + DELAY,
        availability_policy_id=MODELED_DAILY_DELAY_20M_V1).profiles[0]
    assert wrong.status is ProfileDecisionStatus.INVALID and wrong.reason == "INVALID_PROFILE"
    assert calls == [read]
def test_all_admission_precedes_composition_and_native_failure_escapes(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(owner, "compose_profile", lambda read: calls.append(read) or fake(read))
    read = reading()
    with pytest.raises(owner.PreloadedModeledProfileError):
        owner.PreloadedModeledProfileProvider((read, read))
    assert calls == []
    marker = RuntimeError("native failed")
    monkeypatch.setattr(owner, "compose_profile", lambda _read: (_ for _ in ()).throw(marker))
    with pytest.raises(RuntimeError) as caught:
        owner.PreloadedModeledProfileProvider((read,))
    assert caught.value is marker
@pytest.mark.parametrize("requirements,time,policy", [([], DAY, MODELED_DAILY_DELAY_20M_V1),
    ((requirement(), requirement()), DAY, MODELED_DAILY_DELAY_20M_V1),
    ((requirement(),), True, MODELED_DAILY_DELAY_20M_V1), ((requirement(),), DAY, "unknown")])
def test_runtime_contract_rejects_malformed_inputs(monkeypatch, requirements, time, policy) -> None:
    monkeypatch.setattr(owner, "compose_profile", fake)
    provider = owner.PreloadedModeledProfileProvider((reading(),))
    with pytest.raises(owner.PreloadedModeledProfileError):
        provider.context_for(cast(Any, requirements), decision_time_ms=cast(Any, time),
                             availability_policy_id=policy)


def test_empty_dataset_identity_is_golden() -> None:
    provider = owner.PreloadedModeledProfileProvider(())
    assert provider.canonical_bytes == b'{"schema_version":1,"windows":[]}'
    assert provider.dataset_digest == "ddfe3e93aab39125ea9bcb5d3ad7b5cbebe2292eb92f1a7fda4f7ef0c69b263a"


def test_dataset_identity_is_order_and_publication_clock_independent(monkeypatch) -> None:
    monkeypatch.setattr(owner, "compose_profile", fake)
    one, two = reading(), reading(2)
    forward = owner.PreloadedModeledProfileProvider((one, two))
    reverse = owner.PreloadedModeledProfileProvider((two, one))
    shifted = replace(one, days=tuple(replace(day, computed_at=day.computed_at + timedelta(days=50),
                                              published_at=day.published_at + timedelta(days=100))
                                      for day in one.days))
    changed_clocks = owner.PreloadedModeledProfileProvider((shifted, two))
    assert forward.canonical_bytes == reverse.canonical_bytes == changed_clocks.canonical_bytes
    assert forward.dataset_digest == reverse.dataset_digest == changed_clocks.dataset_digest
    projection = json.loads(forward.canonical_bytes)
    assert all(row["merge_algorithm_version"] == "aligned-sum-v1"
               and type(row["composite_id"]) is str for row in projection["windows"])


def test_revision_and_invalidation_change_dataset_identity(monkeypatch) -> None:
    calls = []
    monkeypatch.setattr(owner, "compose_profile", lambda read: calls.append(read) or fake(read))
    read = reading()
    original = owner.PreloadedModeledProfileProvider((read,))
    days = tuple(replace(day, ref=replace(day.ref, revision=2)) for day in read.days)
    revised = VerifiedManifestRead(replace(read.manifest, days=tuple(day.ref for day in days)), days)
    changed_revision = owner.PreloadedModeledProfileProvider((revised,))
    revoked_event = replace(event(), snapshot_id=read.days[0].ref.snapshot_id)
    revoked = replace(read, days=(replace(read.days[0], invalidations=(revoked_event,)),))
    changed_invalidation = owner.PreloadedModeledProfileProvider((revoked,))
    assert len({original.dataset_digest, changed_revision.dataset_digest,
                changed_invalidation.dataset_digest}) == 3
    assert len(calls) == 2 and json.loads(changed_invalidation.canonical_bytes)["windows"][0]["invalidations"]
    with pytest.raises(owner.PreloadedModeledProfileError):
        owner.PreloadedModeledProfileProvider((read, revised))


def test_permanent_output_error_precedes_temporary_unavailability(monkeypatch) -> None:
    monkeypatch.setattr(owner, "compose_profile", fake)
    provider = owner.PreloadedModeledProfileProvider((reading(),))
    item = provider.context_for(
        (requirement(output="other"),), decision_time_ms=DAY + DELAY - 1,
        availability_policy_id=MODELED_DAILY_DELAY_20M_V1).profiles[0]
    assert item.status is ProfileDecisionStatus.INVALID and item.reason == "INVALID_PROFILE"
    unsupported = replace(requirement(), freshness_policy_id="other")
    with pytest.raises(owner.PreloadedModeledProfileError):
        provider.context_for((unsupported,), decision_time_ms=DAY + DELAY,
                             availability_policy_id=MODELED_DAILY_DELAY_20M_V1)


@pytest.mark.parametrize("result", [None, object(), fake(reading(2))])
def test_wrong_composite_identity_never_publishes_provider(monkeypatch, result) -> None:
    monkeypatch.setattr(owner, "compose_profile", lambda _read: cast(Any, result))
    with pytest.raises(owner.PreloadedModeledProfileError):
        owner.PreloadedModeledProfileProvider((reading(),))


def test_preload_reads_exact_manifests_once_before_runtime(monkeypatch) -> None:
    composed = []
    monkeypatch.setattr(owner, "compose_profile", lambda read: composed.append(read) or fake(read))
    one, two = reading(), reading(2)
    reader = Reader((one, two))
    provider = owner.preload_modeled_profile_provider(reader, (one.manifest, two.manifest))
    assert reader.calls == [one.manifest, two.manifest] and composed == [one, two]
    before = provider.dataset_digest
    context = provider.context_for((requirement(2),), decision_time_ms=2 * DAY + DELAY,
                                   availability_policy_id=MODELED_DAILY_DELAY_20M_V1)
    assert context.profiles[0].status is ProfileDecisionStatus.FRESH
    assert reader.calls == [one.manifest, two.manifest] and composed == [one, two]
    assert provider.dataset_digest == before


def test_preload_admission_rejects_without_io(monkeypatch) -> None:
    monkeypatch.setattr(owner, "compose_profile", fake)
    read = reading()
    for reader, manifests in ((Reader(()), [read.manifest]),
                              (Reader(()), (read.manifest, read.manifest)),
                              (object(), (read.manifest,))):
        with pytest.raises(owner.PreloadedModeledProfileError):
            owner.preload_modeled_profile_provider(cast(Any, reader), cast(Any, manifests))
        if isinstance(reader, Reader):
            assert reader.calls == []


@pytest.mark.parametrize("last", [None, reading(30)])
def test_preload_missing_or_mismatched_read_never_composes(monkeypatch, last) -> None:
    calls = []
    monkeypatch.setattr(owner, "compose_profile", lambda read: calls.append(read) or fake(read))
    one, two = reading(), reading(2)
    reader = Reader((one, last))
    with pytest.raises(owner.PreloadedModeledProfileError):
        owner.preload_modeled_profile_provider(reader, (one.manifest, two.manifest))
    assert reader.calls == [one.manifest, two.manifest] and calls == []


def test_preload_reader_failure_preserves_identity(monkeypatch) -> None:
    monkeypatch.setattr(owner, "compose_profile", fake)
    marker = RuntimeError("reader unavailable")
    reader = Reader((marker,))
    with pytest.raises(RuntimeError) as caught:
        owner.preload_modeled_profile_provider(reader, (reading().manifest,))
    assert caught.value is marker and len(reader.calls) == 1
