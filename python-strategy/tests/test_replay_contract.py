"""SCENARIO_ONLY / NOT_HISTORICAL_ADMISSIBLE input boundary fixtures."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal as D

import pytest

from src.core.product_registry import InstrumentSpec
from src.core.replay_contract import (
    AccountKey,
    EffectiveRange,
    EffectiveTimeline,
    InstrumentSpecVersion,
    MarginTier,
    RuleDataVersion,
)


def spec_version(product="BTC", *, second=False):
    return InstrumentSpecVersion(
        f"{product}-USDT-SWAP",
        "spec-v2" if second else "spec-v1",
        EffectiveRange(2000, 3000) if second else EffectiveRange(0, 2000),
        InstrumentSpec(
            product_id=f"{product}-USDT-SWAP",
            exchange="synthetic",
            symbol=f"{product}-USDT-SWAP",
            base=product,
            quote="USDT",
            quantity_step=D("0.01"),
            min_quantity=D("0.01"),
            multiplier=D("0.01") if product == "BTC" else D("0.1"),
            price_tick=D("1" if second else "0.1")
            if product == "BTC"
            else D("0.1" if second else "0.01"),
        ),
    )


def rule_version(product="BTC", *, second=False):
    bounds = (("0", "1000"), ("1000.01", "5000"), ("5000.01", "20000"))
    if product == "ETH":
        bounds = (("0", "5000"), ("5000.01", "10000"), ("10000.01", "25000"))
    rates = (
        ("0.004", "0.01", "100"),
        ("0.005", "0.015", "66.66"),
        ("0.0075", "0.02", "50"),
    )
    if second:
        rates = (
            ("0.0045", "0.011", "90"),
            ("0.0055", "0.016", "60"),
            ("0.008", "0.021", "45"),
        )
    return RuleDataVersion(
        f"{product}-USDT-SWAP",
        "tier-v2" if second else "tier-v1",
        EffectiveRange(1000, 3000) if second else EffectiveRange(0, 1000),
        tuple(
            MarginTier(i, *(D(v) for v in (*bound, *rate)))
            for i, (bound, rate) in enumerate(zip(bounds, rates, strict=True), 1)
        ),
        pool_id="synthetic-usdt-pool-v1",
    )


@pytest.mark.parametrize("product", ["BTC", "ETH"])
@pytest.mark.parametrize(
    "factory,boundary", [(spec_version, 2000), (rule_version, 1000)]
)
def test_frozen_scenario_before_equal_after(product, factory, boundary):
    first, second = factory(product), factory(product, second=True)
    timeline = EffectiveTimeline(
        first.product_id, EffectiveRange(0, 3000), (first, second)
    )
    assert timeline.resolve(0) == first
    assert timeline.resolve(boundary - 1) == first
    assert timeline.resolve(boundary) == second
    assert timeline.resolve(boundary + 1) == second
    assert timeline.resolve(2999) == second
    for outside in (3000, 3001):
        with pytest.raises(ValueError, match="outside"):
            timeline.resolve(outside)


@pytest.mark.parametrize("value", [True, False, 1.0, D("1"), "1", None])
def test_timestamp_rejects_non_exact_int(value):
    with pytest.raises(ValueError):
        EffectiveRange(value, 10)
    with pytest.raises(ValueError):
        EffectiveRange(0, value if value is not None else False)
    first = rule_version()
    with pytest.raises(ValueError):
        EffectiveTimeline(first.product_id, first.effective, (first,)).resolve(value)


def test_negative_timestamps_rejected_by_shared_validator():
    with pytest.raises(ValueError, match="timestamp must be a nonnegative"):
        EffectiveRange(-1, 10)
    with pytest.raises(ValueError, match="timestamp must be a nonnegative"):
        EffectiveRange(0, -1)
    first = rule_version()
    timeline = EffectiveTimeline(first.product_id, first.effective, (first,))
    with pytest.raises(ValueError, match="timestamp must be a nonnegative"):
        timeline.resolve(-1)
    assert timeline.resolve(0) == first


@pytest.mark.parametrize("pool_id", ["", " ", " pool", "pool ", None])
def test_rule_pool_identity_must_be_normalized(pool_id):
    with pytest.raises(ValueError, match="identity"):
        replace(rule_version(), pool_id=pool_id)


def test_rule_pool_identity_is_required_and_cannot_mix():
    first = rule_version()
    with pytest.raises(TypeError, match="pool_id"):
        RuleDataVersion(
            first.product_id, first.version_id, first.effective, first.tiers
        )
    second = replace(rule_version(second=True), pool_id="another-pool")
    with pytest.raises(ValueError, match="pool identity"):
        EffectiveTimeline(first.product_id, EffectiveRange(0, 3000), (first, second))
    assert (
        first.pool_id == rule_version(second=True).pool_id == "synthetic-usdt-pool-v1"
    )


@pytest.mark.parametrize("start,end", [(1, 1), (2, 1)])
def test_empty_or_reversed_range_rejected(start, end):
    with pytest.raises(ValueError):
        EffectiveRange(start, end)


@pytest.mark.parametrize("value", ["", " ", " A", "A ", None, 1])
def test_identity_rejected_without_silent_normalization(value):
    with pytest.raises(ValueError):
        AccountKey(value, "test", "A01")
    with pytest.raises(ValueError):
        replace(rule_version(), version_id=value)
    with pytest.raises(ValueError):
        replace(spec_version(), product_id=value)


def test_account_identity_is_scoped_and_immutable():
    account = AccountKey("okx-scenario", "test", "A01")
    assert account != replace(account, account_id="B01")
    assert account != replace(account, subaccount_id="sub")
    assert account != replace(account, environment="other")
    with pytest.raises(ValueError):
        replace(account, subaccount_id=" ")
    with pytest.raises(FrozenInstanceError):
        account.account_id = "B01"


@pytest.mark.parametrize(
    "field",
    [
        "quantity_step",
        "price_tick",
        "min_quantity",
        "multiplier",
        "min_notional",
        "tick_value",
        "capital_per_contract",
    ],
)
@pytest.mark.parametrize("value", [0.1, 1, "1", D("NaN"), D("Infinity"), D("-1")])
def test_spec_numeric_boundary(field, value):
    version = spec_version()
    with pytest.raises(ValueError):
        replace(version, spec=replace(version.spec, **{field: value}))


@pytest.mark.parametrize(
    "field", ["quantity_step", "price_tick", "min_quantity", "multiplier"]
)
@pytest.mark.parametrize("value", [None, D("0")])
def test_spec_required_values(field, value):
    version = spec_version()
    with pytest.raises(ValueError):
        replace(version, spec=replace(version.spec, **{field: value}))


def test_product_mismatch_and_nonnegative_notional():
    version = spec_version()
    with pytest.raises(ValueError, match="identity mismatch"):
        replace(version, spec=spec_version("ETH").spec)
    assert replace(version, spec=replace(version.spec, min_notional=D("0")))


@pytest.mark.parametrize("field", ["fee_model", "capital_model", "market_type"])
def test_spec_cannot_retain_mutable_or_unknown_model(field):
    version = spec_version()
    for value in ([], "unknown"):
        with pytest.raises(ValueError):
            replace(version, spec=replace(version.spec, **{field: value}))


@pytest.mark.parametrize(
    "field",
    [
        "min_contracts",
        "max_contracts",
        "maintenance_margin_rate",
        "initial_margin_rate",
        "max_leverage",
    ],
)
@pytest.mark.parametrize("value", [0.1, 1, "1", D("NaN"), D("Infinity"), D("-1")])
def test_tier_numeric_boundary(field, value):
    with pytest.raises(ValueError):
        replace(rule_version().tiers[0], **{field: value})


@pytest.mark.parametrize(
    "change",
    [
        {"tier": True},
        {"tier": 0},
        {"tier": D("1")},
        {"max_contracts": D("0")},
        {"maintenance_margin_rate": D("0")},
        {"initial_margin_rate": D("0")},
        {"max_leverage": D("0")},
        {"min_contracts": D("1001")},
    ],
)
def test_invalid_tier_domains(change):
    with pytest.raises(ValueError):
        replace(rule_version().tiers[0], **change)


@pytest.mark.parametrize(
    "case", ["empty", "mutable", "reversed", "duplicate", "overlap"]
)
def test_tier_table_rejects_ambiguous_input(case):
    version = rule_version()
    a, b, c = version.tiers
    tiers = {
        "empty": (),
        "mutable": list(version.tiers),
        "reversed": (b, a),
        "duplicate": (a, replace(b, tier=1)),
        "overlap": (a, replace(b, min_contracts=a.max_contracts), c),
    }[case]
    with pytest.raises(ValueError):
        replace(version, tiers=tiers)


@pytest.mark.parametrize(
    "case",
    [
        "empty",
        "mutable",
        "first",
        "gap",
        "overlap",
        "reverse",
        "duplicate",
        "wrong_key",
        "short",
        "long",
        "open_middle",
        "open_finite",
        "finite_open",
        "mixed",
    ],
)
def test_timeline_rejects_invalid_coverage(case):
    a, b = rule_version(), rule_version(second=True)
    coverage = EffectiveRange(0, 3000)
    versions = {
        "empty": (),
        "mutable": [a, b],
        "first": (replace(a, effective=EffectiveRange(1, 1000)), b),
        "gap": (a, replace(b, effective=EffectiveRange(1001, 3000))),
        "overlap": (a, replace(b, effective=EffectiveRange(999, 3000))),
        "reverse": (b, a),
        "duplicate": (a, replace(b, version_id=a.version_id)),
        "wrong_key": (a, replace(b, product_id="ETH-USDT-SWAP")),
        "short": (a, replace(b, effective=EffectiveRange(1000, 2999))),
        "long": (a, replace(b, effective=EffectiveRange(1000, 3001))),
        "open_middle": (replace(a, effective=EffectiveRange(0, None)), b),
        "open_finite": (a, replace(b, effective=EffectiveRange(1000, None))),
        "finite_open": (a, b),
        "mixed": (a, spec_version(second=True)),
    }[case]
    if case == "finite_open":
        coverage = EffectiveRange(0, None)
    with pytest.raises(ValueError):
        EffectiveTimeline(a.product_id, coverage, versions)


def test_unbounded_tail_and_frozen_inputs():
    a = rule_version()
    b = replace(rule_version(second=True), effective=EffectiveRange(1000, None))
    timeline = EffectiveTimeline(a.product_id, EffectiveRange(0, None), (a, b))
    assert timeline.resolve(10**30) is b
    for obj, field, value in [
        (timeline, "versions", ()),
        (a, "tiers", ()),
        (a.tiers[0], "tier", 9),
        (a.effective, "start", 1),
        (spec_version(), "version_id", "changed"),
    ]:
        assert not hasattr(obj, "__dict__")
        with pytest.raises(FrozenInstanceError):
            setattr(obj, field, value)
    spec = spec_version().spec
    with pytest.raises(FrozenInstanceError):
        spec.price_tick = D("2")
