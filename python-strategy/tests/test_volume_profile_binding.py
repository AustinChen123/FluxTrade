"""Smoke the installed extension; these tests never build or install it."""

import fluxtrade_core
import pytest


def test_coarse_profile_and_detached_readonly_result() -> None:
    bins = [(1, "1.00", "10", 1), (2, "2", "30.0", 1)]
    result = fluxtrade_core.merge_volume_profiles(
        "BINANCE:BTCUSDT-SPOT",
        "0",
        "10",
        "USDT",
        [(0, 86400000, bins), (86400000, 172800000, ())],
        "50",
    )
    assert type(result) is fluxtrade_core.VolumeProfileMergeResult
    assert (result.product_id, result.window_start_ms, result.window_end_ms) == (
        "BINANCE:BTCUSDT-SPOT",
        0,
        172800000,
    )
    assert (result.bin_origin, result.bin_step, result.unit) == ("0", "50", "USDT")
    assert result.bins == [(0, "3", "40", 2)]
    assert (result.base_volume, result.quote_volume, result.aggregate_count) == (
        "3",
        "40",
        2,
    )
    assert (result.poc_index, result.poc_low, result.poc_high_exclusive) == (
        0,
        "0",
        "50",
    )
    detached = result.bins
    detached.clear()
    assert result.bins == [(0, "3", "40", 2)]
    assert bins == [(1, "1.00", "10", 1), (2, "2", "30.0", 1)]
    with pytest.raises(AttributeError):
        setattr(result, "base_volume", "9")
    with pytest.raises(TypeError):
        fluxtrade_core.VolumeProfileMergeResult()
    with pytest.raises(TypeError):
        type("Derived", (fluxtrade_core.VolumeProfileMergeResult,), {})


def test_empty_profile() -> None:
    result = fluxtrade_core.merge_volume_profiles(
        "BINANCE:BTCUSDT-SPOT", "0", "10", "USDT", ((0, 86400000, []),), "10"
    )
    assert result.bins == []
    assert (result.base_volume, result.quote_volume, result.aggregate_count) == (
        "0",
        "0",
        0,
    )
    assert (result.poc_index, result.poc_low, result.poc_high_exclusive) == (
        None,
        None,
        None,
    )
