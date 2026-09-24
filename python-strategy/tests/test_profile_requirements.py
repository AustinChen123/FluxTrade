from dataclasses import FrozenInstanceError, asdict, replace
import hashlib

import pytest

from src.core.market_data.profiles.requirements import ProfileRequirement


VALUE = ProfileRequirement(
    "BINANCE:BTCUSDT-SPOT", "base", "output", "vp-v1", 7, "strict"
)
TEXT_FIELDS = (
    "product_id",
    "base_grid_id",
    "output_grid_id",
    "algorithm_version",
    "freshness_policy_id",
)


def test_canonical_golden_and_value_semantics():
    expected = (
        b'{"algorithm_version":"vp-v1","base_grid_id":"base","freshness_policy_id":"strict",'
        b'"output_grid_id":"output","product_id":"BINANCE:BTCUSDT-SPOT","schema_version":1,"window_days":7}'
    )
    assert VALUE.canonical_bytes == expected
    assert VALUE.digest == hashlib.sha256(expected).hexdigest()
    equivalent = ProfileRequirement(**asdict(VALUE))
    assert equivalent == VALUE and equivalent is not VALUE
    assert equivalent.canonical_bytes == VALUE.canonical_bytes
    assert equivalent.digest == VALUE.digest
    assert not hasattr(VALUE, "__dict__")
    with pytest.raises(FrozenInstanceError):
        setattr(VALUE, "window_days", 1)


@pytest.mark.parametrize("days", [1, 7, 30, 90])
def test_valid_day_boundaries(days):
    assert replace(VALUE, window_days=days).window_days == days


@pytest.mark.parametrize(
    "bad", [0, 91, -1, True, None, "7", 7.0, type("Int", (int,), {})(7)]
)
def test_exact_day_domain(bad):
    with pytest.raises(ValueError):
        replace(VALUE, window_days=bad)


@pytest.mark.parametrize("field", TEXT_FIELDS)
@pytest.mark.parametrize("bad", [None, True, 1, "", "é", "x\x00", "x/y", "x y"])
def test_invalid_text_domain(field, bad):
    with pytest.raises(ValueError):
        replace(VALUE, **{field: bad})


@pytest.mark.parametrize("field", TEXT_FIELDS)
def test_exact_string_subclass(field):
    bad = type("String", (str,), {})(getattr(VALUE, field))
    with pytest.raises(ValueError):
        replace(VALUE, **{field: bad})


@pytest.mark.parametrize(
    "field,maximum",
    [
        ("base_grid_id", 64),
        ("output_grid_id", 64),
        ("algorithm_version", 32),
        ("freshness_policy_id", 64),
    ],
)
def test_safe_identifier_lengths_and_unregistered_intent(field, maximum):
    assert replace(VALUE, **{field: "a" * maximum})
    with pytest.raises(ValueError):
        replace(VALUE, **{field: "a" * (maximum + 1)})


def test_product_length_and_canonical_syntax():
    prefix, suffix = "BINANCE:", "USDT-SPOT"
    assert replace(
        VALUE, product_id=prefix + "A" * (64 - len(prefix + suffix)) + suffix
    )
    for bad in (
        prefix + "A" * (65 - len(prefix + suffix)) + suffix,
        "binance:BTCUSDT-SPOT",
        "BTCUSDT",
        "BINANCE:BTCUSDT",
    ):
        with pytest.raises(ValueError):
            replace(VALUE, product_id=bad)


@pytest.mark.parametrize(
    "field,value",
    [
        ("product_id", "BINANCE:ETHUSDT-SPOT"),
        ("base_grid_id", "other_base"),
        ("output_grid_id", "other_output"),
        ("algorithm_version", "vp-v2"),
        ("window_days", 30),
        ("freshness_policy_id", "other_policy"),
    ],
)
def test_each_legal_field_changes_identity(field, value):
    changed = replace(VALUE, **{field: value})
    assert changed.canonical_bytes != VALUE.canonical_bytes
    assert changed.digest != VALUE.digest
