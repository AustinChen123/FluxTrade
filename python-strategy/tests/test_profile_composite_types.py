from dataclasses import FrozenInstanceError, fields, is_dataclass, replace
from decimal import Decimal, Inexact, Rounded, localcontext
import subprocess
import sys

import pytest

from src.core.market_data.profiles.composite_types import (
    CompositeProfile,
    CompositeProfilePoc,
    InvalidCompositeProfile,
)
from src.core.market_data.profiles.grid import resolve_profile_grid
from src.core.market_data.profiles.read_types import (
    DailyProfileRef,
    OrderedProfileManifest,
)
from src.core.market_data.profiles.types import ProfileBin

GRID = resolve_profile_grid("BINANCE:BTCUSDT-SPOT", "btc_spot_usdt_10_v1", "vp-v1")
REF = DailyProfileRef("a" * 64, 1, "b" * 64, 0, 86400000)
MANIFEST = OrderedProfileManifest(
    GRID.product_id, GRID.grid_id, GRID.algorithm_version, (REF,)
)
EMPTY = CompositeProfile(MANIFEST, GRID, (), Decimal(0), Decimal(0), 0, None)
BIN = ProfileBin(0, Decimal(1), Decimal(1), 1)
POC = CompositeProfilePoc(0, Decimal(0), Decimal(10))
FULL = replace(
    EMPTY,
    bins=(BIN,),
    base_volume=Decimal(1),
    quote_volume=Decimal(1),
    aggregate_count=1,
    poc=POC,
)


def test_identity_golden_and_immutability() -> None:
    expected = (
        b'{"algorithm_version":"vp-v1","base_grid_id":"btc_spot_usdt_10_v1",'
        b'"manifest_digest":"40064980a0bb153e3c77421ed2a71fbaffd348401389b8ba3e232c439ecb280d",'
        b'"merge_algorithm_version":"aligned-sum-v1","output_grid_id":"btc_spot_usdt_10_v1",'
        b'"product_id":"BINANCE:BTCUSDT-SPOT"}'
    )
    assert EMPTY.identity_bytes == expected
    assert (
        EMPTY.composite_id
        == "5766b5d445bededce8aae025da6629f16784fd3c8463672452c7e0fb70dfd3a4"
    )
    assert (EMPTY.window_start_ms, EMPTY.window_end_ms) == (0, 86400000)
    assert (
        FULL.composite_id == EMPTY.composite_id
    )  # Identity is not aggregate validity.
    assert replace(FULL, base_volume=Decimal(99)).identity_bytes == expected
    for instance in (EMPTY, POC, GRID, MANIFEST, REF, BIN):
        assert not hasattr(instance, "__dict__")
    with pytest.raises(FrozenInstanceError):
        setattr(EMPTY, "bins", ())
    with pytest.raises((FrozenInstanceError, TypeError)):
        setattr(EMPTY, "merge_algorithm_version", "other")


@pytest.mark.parametrize(
    "ref",
    [
        replace(REF, revision=2),
        replace(REF, content_sha256="c" * 64),
        replace(REF, snapshot_id="d" * 64),
        replace(REF, window_start_ms=86400000, window_end_ms=172800000),
    ],
)
def test_pinned_identity_changes(ref: DailyProfileRef) -> None:
    assert (
        replace(EMPTY, manifest=replace(MANIFEST, days=(ref,))).composite_id
        != EMPTY.composite_id
    )


@pytest.mark.parametrize(
    "field,value",
    [
        ("bins", []),
        ("bins", (BIN, BIN)),
        ("bins", (replace(BIN, bin_index=1), BIN)),
        ("poc", replace(POC, index=1)),
        ("poc", None),
        ("output_grid", replace(GRID, step=Decimal(20))),
        ("output_grid", replace(GRID, grid_id="unknown")),
        ("manifest", replace(MANIFEST, product_id="BINANCE:ETHUSDT-SPOT")),
        ("manifest", replace(MANIFEST, algorithm_version="other")),
        ("manifest", replace(MANIFEST, base_grid_id="unknown")),
    ],
)
def test_invalid_structure(field: str, value: object) -> None:
    with pytest.raises(InvalidCompositeProfile) as caught:
        replace(FULL, **{field: value})
    assert str(caught.value) == "PROFILE_COMPOSITE_INVALID"
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


@pytest.mark.parametrize(
    "field,value",
    [
        ("base_volume", Decimal(1)),
        ("quote_volume", Decimal(1)),
        ("aggregate_count", 1),
        ("poc", POC),
    ],
)
def test_empty_matrix(field: str, value: object) -> None:
    with pytest.raises(InvalidCompositeProfile):
        replace(EMPTY, **{field: value})


@pytest.mark.parametrize(
    "field,value",
    [("base_volume", Decimal(0)), ("quote_volume", Decimal(0)), ("aggregate_count", 0)],
)
def test_nonempty_requires_positive_summary(field: str, value: object) -> None:
    with pytest.raises(InvalidCompositeProfile):
        replace(FULL, **{field: value})


def test_isolated_import() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import src.core.market_data.profiles.composite_types; "
            "assert not any(n.startswith(('sqlalchemy','fluxtrade_core')) or n.endswith('.orm') for n in sys.modules)",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def _subclass(value: object) -> object:
    # Exercise exact-type rejection using valid inherited constructors, not forged state.
    assert is_dataclass(value)
    derived = type("Derived", (type(value),), {})
    return derived(
        **{field.name: getattr(value, field.name) for field in fields(value)}
    )


def _fixed_error(caught: pytest.ExceptionInfo[InvalidCompositeProfile]) -> None:
    assert type(caught.value) is InvalidCompositeProfile
    assert str(caught.value) == "PROFILE_COMPOSITE_INVALID"
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


@pytest.mark.parametrize(
    "field,value",
    [
        ("manifest", True),
        ("manifest", _subclass(MANIFEST)),
        ("output_grid", True),
        ("output_grid", _subclass(GRID)),
        ("bins", type("TupleSubclass", (tuple,), {})((BIN,))),
        ("bins", (True,)),
        ("bins", (_subclass(BIN),)),
        ("poc", True),
        ("poc", _subclass(POC)),
        ("base_volume", True),
        ("quote_volume", "SECRET"),
        ("base_volume", type("DecimalSubclass", (Decimal,), {})("1")),
        ("aggregate_count", True),
        ("aggregate_count", type("IntSubclass", (int,), {})(1)),
        ("aggregate_count", -1),
        ("aggregate_count", 1 << 63),
    ],
)
def test_exact_nested_and_integer_domains(field: str, value: object) -> None:
    with pytest.raises(InvalidCompositeProfile) as caught:
        replace(FULL, **{field: value})
    _fixed_error(caught)


@pytest.mark.parametrize("field", ["base_volume", "quote_volume"])
@pytest.mark.parametrize(
    "value",
    [
        "-1",
        "NaN",
        "sNaN",
        "Infinity",
        "-Infinity",
        "79228162514264337593543950336",
        "1e-29",
    ],
)
def test_summary_decimal_rejections(field: str, value: str) -> None:
    with pytest.raises(InvalidCompositeProfile) as caught:
        replace(FULL, **{field: Decimal(value)})
    _fixed_error(caught)


def test_summary_decimal_boundaries_and_context() -> None:
    for precision in (1, 28):
        with localcontext() as context:
            context.prec = precision
            context.traps[Inexact] = context.traps[Rounded] = True
            for field in ("base_volume", "quote_volume"):
                for text in ("79228162514264337593543950335", "1e-28", "1.2300"):
                    result = replace(FULL, **{field: Decimal(text)})
                    assert getattr(result, field) == Decimal(text)
                zero = replace(EMPTY, **{field: Decimal("-0.00")})
                assert getattr(zero, field).as_tuple() == Decimal(0).as_tuple()
            assert (
                replace(FULL, aggregate_count=(1 << 63) - 1).aggregate_count
                == (1 << 63) - 1
            )
            assert EMPTY.aggregate_count == 0
            with pytest.raises(InvalidCompositeProfile) as caught:
                replace(FULL, base_volume=Decimal("1e-29"))
            _fixed_error(caught)


@pytest.mark.parametrize(
    "field,value",
    [
        ("index", True),
        ("index", type("IntSubclass", (int,), {})(0)),
        ("index", -(1 << 63) - 1),
        ("index", 1 << 63),
        ("low", True),
        ("high_exclusive", "SECRET"),
        ("low", type("DecimalSubclass", (Decimal,), {})(0)),
        ("low", Decimal(10)),
        ("low", Decimal(11)),
    ],
)
def test_poc_exact_types_and_order(field: str, value: object) -> None:
    with pytest.raises(InvalidCompositeProfile) as caught:
        replace(POC, **{field: value})
    _fixed_error(caught)


@pytest.mark.parametrize("field", ["low", "high_exclusive"])
@pytest.mark.parametrize(
    "text",
    ["NaN", "sNaN", "Infinity", "-Infinity", "1e-29", "79228162514264337593543950336"],
)
def test_poc_decimal_domain(field: str, text: str) -> None:
    with pytest.raises(InvalidCompositeProfile) as caught:
        replace(POC, **{field: Decimal(text)})
    _fixed_error(caught)


def test_poc_exact_boundaries_under_traps() -> None:
    maximum = Decimal("79228162514264337593543950335")
    for precision in (1, 28):
        with localcontext() as context:
            context.prec = precision
            context.traps[Inexact] = context.traps[Rounded] = True
            for index in (-(1 << 63), (1 << 63) - 1):
                result = CompositeProfilePoc(index, maximum.copy_negate(), maximum)
                assert result.index == index and result.high_exclusive == maximum
                scaled = CompositeProfilePoc(index, Decimal("-0.00"), Decimal("1e-28"))
                assert scaled.low.as_tuple() == Decimal(0).as_tuple()
                assert scaled.high_exclusive == Decimal("1e-28")
