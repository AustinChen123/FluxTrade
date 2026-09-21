from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
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
