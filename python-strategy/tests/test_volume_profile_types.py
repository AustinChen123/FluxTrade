import subprocess
import sys
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, Inexact, Rounded, localcontext
from pathlib import Path
from typing import Any

import pytest

from src.core.market_data.profiles.types import (
    BIGINT_MAX,
    ProfileBin,
    VolumeProfileContent,
)


def profile() -> VolumeProfileContent:
    return VolumeProfileContent(
        "BINANCE:BTCUSDT-SPOT",
        0,
        86_400_000,
        "g1",
        Decimal("-0.00"),
        Decimal("10.0"),
        "vp-v1",
        (
            ProfileBin(-1, Decimal("1.20"), Decimal("12.00"), 2),
            ProfileBin(2, Decimal("0.3"), Decimal("3.0"), 1),
        ),
    )


def test_canonical_bytes_digest_scale_and_context_golden() -> None:
    value = profile()
    golden = (
        b'{"algorithm_version":"vp-v1","bin_origin":"0","bin_step":"10","bins":['
        b'{"aggregate_count":2,"base_volume":"1.2","bin_index":-1,"quote_volume":"12"},'
        b'{"aggregate_count":1,"base_volume":"0.3","bin_index":2,"quote_volume":"3"}],'
        b'"grid_id":"g1","period":"1d","product_id":"BINANCE:BTCUSDT-SPOT",'
        b'"schema_version":1,"timezone":"UTC","window_end_ms":86400000,"window_start_ms":0}'
    )
    digest = "6c8fc0387739cddaa7f34f4098f7cd1f7b225a4fc67b75738fd3807327a8180d"
    bins = (ProfileBin(-1, Decimal("1.2000"), Decimal("12"), 2),
            ProfileBin(2, Decimal("0.3000"), Decimal("3"), 1))
    scaled = replace(value, bin_origin=Decimal("0"), bin_step=Decimal("1E1"), bins=bins)
    for precision in (1, 6, 50):
        with localcontext() as context:
            context.prec, context.Emax, context.Emin = precision, 1, -1
            context.traps[Inexact] = context.traps[Rounded] = True
            for item in (profile(), scaled):
                assert item.content_bytes == golden
                assert item.content_sha256 == digest
                assert (item.base_volume, item.quote_volume) == (
                    Decimal("1.5"),
                    Decimal("15"),
                )
                assert (item.aggregate_count, item.occupied_bins) == (3, 2)
            first = ProfileBin(0, Decimal("12345678901234567890.1"), Decimal("0.00001"), 1)
            second = ProfileBin(1, Decimal("0.02"), Decimal("0.000002"), 1)
            large = replace(value, bins=(first, second))
            assert large.base_volume == Decimal("12345678901234567890.12")
            assert large.quote_volume == Decimal("0.000012")


def test_empty_content_and_deep_immutability() -> None:
    value = replace(profile(), bins=())
    assert (value.base_volume, value.quote_volume) == (Decimal(0), Decimal(0))
    assert (value.aggregate_count, value.occupied_bins) == (0, 0)
    assert b'"bins":[]' in value.content_bytes
    assert not hasattr(value, "quality") and not hasattr(value, "published_at")
    for obj, field in [(value, "grid_id"), (profile().bins[0], "base_volume")]:
        with pytest.raises(FrozenInstanceError):
            setattr(obj, field, "changed")
        assert not hasattr(obj, "__dict__")


class IntSubclass(int):
    pass


class DecimalSubclass(Decimal):
    pass


class TupleSubclass(tuple):
    pass


def test_invalid_bins_matrix() -> None:
    original = profile().bins[0]
    cases: dict[str, list[Any]] = {
        "bin_index": [True, IntSubclass(1), -(1 << 63) - 1, BIGINT_MAX + 1],
        "aggregate_count": [True, IntSubclass(1), 0, -1, BIGINT_MAX + 1],
        "base_volume": [
            1,
            "1",
            DecimalSubclass("1"),
            Decimal(0),
            Decimal("-1"),
            Decimal("NaN"),
            Decimal("sNaN"),
            Decimal("Infinity"),
        ],
        "quote_volume": [1, Decimal(0), Decimal("-Infinity"), Decimal("NaN")],
    }
    for field, invalids in cases.items():
        for invalid in invalids:
            with pytest.raises(ValueError):
                replace(original, **{field: invalid})


def test_invalid_content_matrix_and_ascending_tuple_contract() -> None:
    value = profile()
    cases: dict[str, list[Any]] = {
        "product_id": [None, "BTCUSDT", "binance:BTCUSDT-SPOT"],
        "grid_id": ["", "g/1", "é", "a" * 65],
        "algorithm_version": ["", "../v1", "a" * 33],
        "window_start_ms": [True, IntSubclass(0), -86_400_000, 1, BIGINT_MAX + 1],
        "window_end_ms": [True, 0, 86_400_001, 172_800_000, BIGINT_MAX + 1],
        "bin_origin": [0, Decimal("NaN"), Decimal("-Infinity")],
        "bin_step": [10, Decimal(0), Decimal("-1"), Decimal("Infinity")],
        "bins": [
            TupleSubclass(value.bins),
            list(value.bins),
            (object(),),
            value.bins[::-1],
            (value.bins[0],) * 2,
        ],
    }
    for field, invalids in cases.items():
        for invalid in invalids:
            with pytest.raises(ValueError):
                replace(value, **{field: invalid})
    with pytest.raises(ValueError):
        replace(
            value,
            bins=(replace(value.bins[0], aggregate_count=BIGINT_MAX), value.bins[1]),
        )


def test_isolated_import_has_no_persistence_dependency() -> None:
    script = "import sys; import src.core.market_data.profiles.types; assert 'src.core.orm_models' not in sys.modules; assert not any(n == 'sqlalchemy' or n.startswith('sqlalchemy.') for n in sys.modules)"
    subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        timeout=10,
        check=True,
        capture_output=True,
    )


def test_rust_decimal_domain_and_product_storage_boundaries() -> None:
    value = profile()
    product = "BINANCE:" + "X" * 47 + "USDT-SPOT"
    assert len(replace(value, product_id=product).product_id) == 64
    with pytest.raises(ValueError):
        replace(value, product_id=product.replace("X", "XX", 1))
    for raw in ["79228162514264337593543950335", "1E-28", "1" + "0" * 5000 + "E-5000"]:
        number = Decimal(raw)
        item = ProfileBin(0, number, number, 1)
        assert replace(value, bins=(item,)).base_volume == number
    for raw in ["79228162514264337593543950336", "1E-29", "1E+4300", "1E-4300"]:
        for field in ("bin_origin", "bin_step"):
            with pytest.raises(ValueError, match="Rust coefficient/scale domain"):
                replace(value, **{field: Decimal(raw)})
        for field in ("base_volume", "quote_volume"):
            with pytest.raises(ValueError, match="Rust coefficient/scale domain"):
                replace(value.bins[0], **{field: Decimal(raw)})


@pytest.mark.parametrize("field", ["base_volume", "quote_volume"])
@pytest.mark.parametrize("precision,traps", [(1, False), (2, True), (28, True), (80, False)])
@pytest.mark.parametrize("parts,valid", [
    (("79228162514264337593543950335",), True),
    (("79228162514264337593543950334", "1"), True),
    (("79228162514264337593543950335", "1"), False),
    (("79228162514264337593543950335", "1E-28"), False),
])
def test_exact_totals_must_fit_rust_domain_at_construction(
    field: str, precision: int, traps: bool, parts: tuple[str, ...], valid: bool,
) -> None:
    with localcontext() as context:
        context.prec = precision
        for signal in context.traps:
            context.traps[signal] = traps
        # Every individual bin is valid; only the chosen aggregate can overflow.
        bins = tuple(replace(ProfileBin(index, Decimal(1), Decimal(1), 1),
                             **{field: Decimal(raw)}) for index, raw in enumerate(parts))
        if valid:
            content = replace(profile(), bins=bins)
            assert getattr(content, field) == Decimal("79228162514264337593543950335")
            assert getattr(content, "quote_volume" if field == "base_volume" else "base_volume") == Decimal(len(parts))
        else:
            # No content escapes construction for publication or any Session call.
            with pytest.raises(ValueError, match="Rust coefficient/scale domain"):
                replace(profile(), bins=bins)
