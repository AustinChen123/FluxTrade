"""Smoke the installed extension; these tests never build or install it."""

import fluxtrade_core
import pytest

from copy import deepcopy
from typing import Callable, Never, cast


DAY = 86400000
CALLBACK_LOG: list[str] = []
# Deliberately ill-typed boundary inputs must reach the actual extension decoder.
merge = cast(
    Callable[..., fluxtrade_core.VolumeProfileMergeResult],
    fluxtrade_core.merge_volume_profiles,
)


def _merge(
    days: object,
    *,
    product: object = "BINANCE:BTCUSDT-SPOT",
    step: object = "10",
    output: object = "10",
) -> fluxtrade_core.VolumeProfileMergeResult:
    return merge(product, "0", step, "USDT", days, output)


def _error(code: str, days: object, **kwargs: object) -> None:
    CALLBACK_LOG.clear()
    with pytest.raises(ValueError) as caught:
        _merge(days, **kwargs)
    assert str(caught.value) == "PROFILE_MERGE_" + code
    assert CALLBACK_LOG == []


def test_day_and_bin_compute_ceiling() -> None:
    days = [(i * DAY, (i + 1) * DAY, []) for i in range(90)]
    assert _merge(days).window_end_ms == 90 * DAY
    _error("RESOURCE_LIMIT", days + [(90 * DAY, 91 * DAY, [])])
    bins = [(i, "1", "1", 1) for i in range(65536)]
    large = [(0, DAY, bins), (DAY, 2 * DAY, bins)]
    result = _merge(large, step="1", output="65536")
    assert result.bins == [(0, "131072", "131072", 131072)]
    assert (result.base_volume, result.quote_volume, result.aggregate_count) == (
        "131072",
        "131072",
        131072,
    )
    assert (result.poc_index, result.poc_low, result.poc_high_exclusive) == (
        0,
        "0",
        "65536",
    )
    _error(
        "RESOURCE_LIMIT",
        [large[0], (DAY, 2 * DAY, bins + [(65536, "1", "1", 1)])],
        step="1",
        output="65536",
    )
    assert len(bins) == 65536 and bins[0] == (0, "1", "1", 1)
    assert bins[-1] == (65535, "1", "1", 1)


@pytest.mark.parametrize(
    "product,step,output,code",
    [
        ("SECRET invalid product", "10", "10", "INVALID_PRODUCT"),
        ("BINANCE:BTCUSDT-SPOT", "0", "10", "INVALID_GRID"),
        ("BINANCE:BTCUSDT-SPOT", "-1", "10", "INVALID_GRID"),
        ("BINANCE:BTCUSDT-SPOT", "10", "15", "INVALID_GRID"),
    ],
)
def test_grid_product_errors(product: str, step: str, output: str, code: str) -> None:
    days = [(0, DAY, [(0, "1", "1", 1)])]
    before = deepcopy(days)
    _error(code, days, product=product, step=step, output=output)
    assert days == before


@pytest.mark.parametrize(
    "windows",
    [
        [(-1, DAY)],
        [(0, 0)],
        [(DAY, 0)],
        [(1, DAY + 1)],
        [(0, DAY - 1)],
        [(0, DAY), (2 * DAY, 3 * DAY)],
        [(0, DAY), (0, DAY)],
        [(DAY, 2 * DAY), (0, DAY)],
    ],
)
def test_window_errors(windows: list[tuple[int, int]]) -> None:
    _error("INVALID_WINDOW", [(start, end, []) for start, end in windows])


@pytest.mark.parametrize(
    "bins",
    [
        [(0, "1", "1", 1), (0, "1", "1", 1)],
        [(0, "0", "1", 1)],
        [(0, "-1", "1", 1)],
        [(0, "1", "0", 1)],
        [(0, "1", "-1", 1)],
    ],
)
def test_invalid_bins(bins: list[tuple[int, str, str, int]]) -> None:
    before = deepcopy(bins)
    _error("INVALID_BIN", [(0, DAY, bins)])
    assert bins == before


@pytest.mark.parametrize("outer", [list, tuple])
@pytest.mark.parametrize("inner", [list, tuple])
def test_exact_sequence_combinations(outer: type, inner: type) -> None:
    result = _merge(outer([(0, DAY, inner([(0, "1", "1", 1)]))]))
    assert result.bins == [(0, "1", "1", 1)]


def test_merged_poc_recomputed_and_tie_uses_lowest_index() -> None:
    days = [
        (0, DAY, [(0, "5", "5", 1), (2, "4", "4", 1)]),
        (DAY, 2 * DAY, [(1, "5", "5", 1), (2, "4", "4", 1)]),
    ]
    before = deepcopy(days)
    assert _merge(days).poc_index == 2
    assert days == before
    assert (
        _merge(
            [(0, DAY, [(2, "1", "1", 1)]), (DAY, 2 * DAY, [(-1, "1", "1", 1)])]
        ).poc_index
        == -1
    )


class TupleSubclass(tuple):
    def __getitem__(self, key: object) -> Never:
        CALLBACK_LOG.append("tuple.__getitem__")
        raise AssertionError("custom tuple method called")


class ListSubclass(list):
    def __len__(self) -> int:
        CALLBACK_LOG.append("list.__len__")
        raise AssertionError("custom list method called")


class IntSubclass(int):
    def __index__(self) -> int:
        CALLBACK_LOG.append("int.__index__")
        raise AssertionError("custom int method called")


class IndexObject:
    def __index__(self) -> int:
        CALLBACK_LOG.append("index.__index__")
        raise AssertionError("custom index method called")


class StringSubclass(str):
    def __str__(self) -> str:
        CALLBACK_LOG.append("str.__str__")
        raise AssertionError("custom string method called")


@pytest.mark.parametrize(
    "days",
    [
        [TupleSubclass((0, DAY, []))],
        [(0, DAY, [TupleSubclass((0, "1", "1", 1))])],
        [(0, DAY, ListSubclass())],
        [(IntSubclass(0), DAY, [])],
        [(IndexObject(), DAY, [])],
        [(0, DAY, [(IntSubclass(0), "1", "1", 1)])],
        [(0, DAY, [(0, "1", "1", IndexObject())])],
        [(0, DAY, [(0, StringSubclass("SECRET"), "1", 1)])],
    ],
)
def test_rejects_custom_nested_types_without_callbacks(days: object) -> None:
    _error("INVALID_INPUT", days)


def test_string_subclass_and_malicious_decimal_are_sanitized() -> None:
    _error("INVALID_INPUT", [(0, DAY, [])], product=StringSubclass("SECRET"))
    _error("INVALID_DECIMAL", [(0, DAY, [(0, "SECRET", "1", 1)])])


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
        cast(Callable[[], object], fluxtrade_core.VolumeProfileMergeResult)()
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
