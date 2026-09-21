"""Real native acceptance, enabled only by the offline provenance-checking harness."""

import os
from dataclasses import replace
from decimal import Decimal, Inexact, Rounded, localcontext
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles import composite as owner
from src.core.market_data.profiles.read_results import (
    VerifiedDailyRead,
    VerifiedManifestRead,
)
from src.core.market_data.profiles.read_types import (
    DailyProfileRef,
    OrderedProfileManifest,
)
from src.core.market_data.profiles.types import ProfileBin
from test_profile_composite import reading

pytestmark = pytest.mark.skipif(
    os.environ.get("FLUXTRADE_COMPOSITE_NATIVE_GATE") != "1",
    reason="run scripts/verify_profile_composite_native.py with an offline wheel",
)

DAY = 86400000


def read_bins(days: tuple[tuple[ProfileBin, ...], ...]) -> VerifiedManifestRead:
    seed = reading().days[0]
    results = []
    for i, bins in enumerate(days):
        content = replace(
            seed.publication.content,
            window_start_ms=i * DAY,
            window_end_ms=(i + 1) * DAY,
            bins=bins,
        )
        digest = content.content_sha256
        ref = DailyProfileRef(digest, 1, digest, i * DAY, (i + 1) * DAY)
        results.append(
            VerifiedDailyRead(
                ref,
                seed.computed_at,
                seed.published_at,
                replace(seed.publication, content=content),
                (),
            )
        )
    manifest = OrderedProfileManifest(
        "BINANCE:BTCUSDT-SPOT",
        "btc_spot_usdt_10_v1",
        "vp-v1",
        tuple(day.ref for day in results),
    )
    return VerifiedManifestRead(manifest, tuple(results))


def bin_at(index: int, quantity: str = "1") -> ProfileBin:
    return ProfileBin(index, Decimal(quantity), Decimal(quantity), 1)


@pytest.mark.parametrize("count", [1, 7, 30, 90])
def test_contiguous_days_exact_result(count: int) -> None:
    read = read_bins(((bin_at(0),),) * count)
    result = owner.compose_profile(read)
    assert (result.window_start_ms, result.window_end_ms) == (0, count * DAY)
    assert result.bins == (ProfileBin(0, Decimal(count), Decimal(count), count),)
    assert (result.base_volume, result.quote_volume, result.aggregate_count) == (
        Decimal(count),
        Decimal(count),
        count,
    )
    assert (
        result.manifest is read.manifest
        and result.manifest_digest == read.manifest.manifest_digest
    )
    assert result.composite_id == owner.compose_profile(read).composite_id
    assert result.poc is not None and (
        result.poc.index,
        result.poc.low,
        result.poc.high_exclusive,
    ) == (0, Decimal(0), Decimal(10))


@pytest.mark.parametrize("days", [((bin_at(0),), (), (bin_at(0),)), ((), (), ())])
def test_empty_days_preserve_window(days: tuple[tuple[ProfileBin, ...], ...]) -> None:
    result = owner.compose_profile(read_bins(days))
    assert (result.window_start_ms, result.window_end_ms) == (0, 3 * DAY)
    if not days[0]:
        assert result.bins == () and result.poc is None
        assert (result.base_volume, result.quote_volume, result.aggregate_count) == (
            Decimal(0),
            Decimal(0),
            0,
        )
    else:
        assert result.bins == (ProfileBin(0, Decimal(2), Decimal(2), 2),)


def test_poc_and_decimal_context() -> None:
    read = read_bins(
        ((bin_at(0, "5"), bin_at(2, "4")), (bin_at(1, "5"), bin_at(2, "4")))
    )
    expected = owner.compose_profile(read)
    assert expected.poc is not None and expected.poc.index == 2
    with localcontext() as context:
        context.prec = 1
        context.traps[Inexact] = context.traps[Rounded] = True
        assert owner.compose_profile(read) == expected
    tie = owner.compose_profile(read_bins(((bin_at(-1),), (bin_at(2),))))
    assert tie.poc is not None and tie.poc.index == -1


def test_rust_arithmetic_overflow() -> None:
    read = read_bins(((bin_at(0, "79228162514264337593543950335"),), (bin_at(0),)))
    with pytest.raises(owner.ProfileCompositionError) as caught:
        owner.compose_profile(read)
    assert caught.value.reason == "ARITHMETIC"
    assert str(caught.value) == "PROFILE_COMPOSITION_ARITHMETIC"


def test_exact_compute_ceiling_and_pre_native_one_over(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bins = tuple(bin_at(index) for index in range(65536))
    result = owner.compose_profile(read_bins((bins, bins)))
    assert len(result.bins) == 65536
    assert all(
        row == ProfileBin(index, Decimal(2), Decimal(2), 2)
        for index, row in enumerate(result.bins)
    )
    assert (result.base_volume, result.quote_volume, result.aggregate_count) == (
        Decimal(131072),
        Decimal(131072),
        131072,
    )
    assert result.poc is not None and result.poc.index == 0
    loader = Mock(side_effect=AssertionError("native must not load"))
    monkeypatch.setattr(owner, "_load_native", loader)
    with pytest.raises(owner.ProfileCompositionError) as caught:
        owner.compose_profile(read_bins((bins, bins + (bin_at(65536),))))
    assert caught.value.reason == "TOO_LARGE"
    loader.assert_not_called()
