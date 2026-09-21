"""Pure grid identity and Decimal boundary contracts."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, Inexact, Rounded, localcontext
import subprocess
import sys
from typing import Callable, cast

import pytest

from src.core.market_data.profiles.grid import (
    InvalidProfileGrid,
    ProfileGridDefinition,
    UnsupportedProfileGrid,
    resolve_profile_grid,
)


KEY = ("BINANCE:BTCUSDT-SPOT", "btc_spot_usdt_10_v1", "vp-v1")
MAX = "79228162514264337593543950335"


class Text(str):
    pass


class Number(Decimal):
    pass


def test_registered_identity_is_fresh_and_immutable() -> None:
    first, second = resolve_profile_grid(*KEY), resolve_profile_grid(*KEY)
    assert first == second and first is not second
    assert first == ProfileGridDefinition(*KEY, Decimal(0), Decimal(10), "USDT")
    assert (first.product_id, first.grid_id, first.algorithm_version) == KEY
    assert not hasattr(first, "__dict__")
    with pytest.raises(FrozenInstanceError):
        setattr(first, "step", Decimal(20))
    arbitrary = replace(first, step=Decimal(20))
    assert arbitrary.step == 20 and resolve_profile_grid(*KEY).step == 10


@pytest.mark.parametrize(
    "key",
    [
        ("BINANCE:ETHUSDT-SPOT", KEY[1], KEY[2]),
        (KEY[0], "btc_spot_usdt_50_v1", KEY[2]),
        (KEY[0], KEY[1], "vp-v2"),
    ],
)
def test_valid_unknown_scope_is_unsupported(key: tuple[str, str, str]) -> None:
    with pytest.raises(UnsupportedProfileGrid) as caught:
        resolve_profile_grid(*key)
    assert str(caught.value) == "PROFILE_GRID_UNSUPPORTED"
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


@pytest.mark.parametrize(
    "index,value",
    [
        (0, True),
        (0, Text(KEY[0])),
        (0, "SECRET"),
        (0, "B" * 65),
        (1, False),
        (1, Text("g")),
        (1, "g" * 65),
        (1, ""),
        (1, "é"),
        (1, "a\x00"),
        (2, True),
        (2, Text("v")),
        (2, "a" * 33),
        (2, "a/b"),
    ],
)
def test_invalid_lookup_precedes_unsupported(index: int, value: object) -> None:
    key: list[object] = list(KEY)
    key[index] = value
    resolver = cast(Callable[..., ProfileGridDefinition], resolve_profile_grid)
    with pytest.raises(InvalidProfileGrid) as caught:
        resolver(*key)
    assert str(caught.value) == "PROFILE_GRID_INVALID"
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


@pytest.mark.parametrize(
    "field,value",
    [
        ("product_id", True),
        ("grid_id", Text("g")),
        ("algorithm_version", ""),
        ("unit", Text("USDT")),
        ("unit", True),
        ("unit", "USD"),
        ("unit", "U" * 65),
        ("unit", "USDT\x00"),
        ("origin", True),
        ("origin", "0"),
        ("origin", Number("0")),
        ("origin", Decimal("NaN")),
        ("origin", Decimal("sNaN")),
        ("origin", Decimal("Infinity")),
        ("origin", Decimal("1e-29")),
        ("origin", Decimal("79228162514264337593543950336")),
        ("step", Decimal(0)),
        ("step", Decimal(-1)),
        ("step", Decimal("1e-29")),
        ("step", Decimal("1e4300")),
        ("step", Decimal("1e-4300")),
        ("step", Decimal("79228162514264337593543950336")),
    ],
)
def test_definition_rejects_invalid_exact_domains(field: str, value: object) -> None:
    with pytest.raises(InvalidProfileGrid) as caught:
        replace(resolve_profile_grid(*KEY), **{field: value})
    assert str(caught.value) == "PROFILE_GRID_INVALID"
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


@pytest.mark.parametrize(
    "origin,step",
    [
        ("-0.000", "10.000"),
        ("-1.2300", "1e-28"),
        ("-" + MAX, MAX),
        (MAX, "0.00000000000000000000000000010"),
    ],
)
def test_decimal_normalization_is_context_independent(origin: str, step: str) -> None:
    baseline = ProfileGridDefinition(*KEY, Decimal(origin), Decimal(step), "USDT")
    with localcontext() as context:
        context.prec = 1
        context.traps[Inexact] = context.traps[Rounded] = True
        actual = ProfileGridDefinition(*KEY, Decimal(origin), Decimal(step), "USDT")
    assert actual == baseline
    assert actual.origin.as_tuple() == baseline.origin.as_tuple()
    if Decimal(origin).is_zero():
        assert actual.origin.as_tuple() == Decimal(0).as_tuple()
    assert actual.step.as_tuple().digits[-1] != 0


def test_safe_identifier_boundaries_and_quote_registry() -> None:
    product = "BINANCE:" + "A" * 47 + "USDT-SPOT"
    assert len(product) == 64
    definition = ProfileGridDefinition(
        product, "g" * 64, "v" * 32, Decimal(0), Decimal(1), "USDT"
    )
    assert definition.product_id == product
    assert (
        ProfileGridDefinition(
            "BINANCE:ETHUSDC-SPOT", "g", "v", Decimal(0), Decimal(1), "USDC"
        ).unit
        == "USDC"
    )
    with pytest.raises(InvalidProfileGrid):
        replace(definition, product_id="BINANCE:" + "A" * 48 + "USDT-SPOT")


def test_isolated_import_does_not_load_persistence_or_native() -> None:
    probe = subprocess.run(
        [
            sys.executable,
            "-c",
            """
import sys
from src.core.market_data.profiles.grid import resolve_profile_grid
resolve_profile_grid('BINANCE:BTCUSDT-SPOT', 'btc_spot_usdt_10_v1', 'vp-v1')
assert not any(name.startswith(('sqlalchemy', 'fluxtrade_core')) or name.endswith('.orm')
               for name in sys.modules)
""",
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert probe.returncode == 0, probe.stderr
