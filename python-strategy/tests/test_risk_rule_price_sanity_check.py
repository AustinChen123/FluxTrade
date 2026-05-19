"""Tests for price sanity risk rule."""

from __future__ import annotations

from decimal import Decimal

from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.risk_rules.price_sanity_check import PriceSanityCheckRule


def _rule() -> PriceSanityCheckRule:
    return PriceSanityCheckRule(
        RiskConfig(max_price_deviation_from_mid_pct=Decimal("0.03"))
    )


def test_price_sanity_passes_at_upper_threshold(signal_factory) -> None:
    signal = signal_factory(price=Decimal("103"))

    status, reason = _rule().evaluate(
        signal,
        best_bid=Decimal("99"),
        best_ask=Decimal("101"),
    )

    assert status == RuleStatus.PASS
    assert reason is None


def test_price_sanity_passes_at_lower_threshold(signal_factory) -> None:
    signal = signal_factory(price=Decimal("97"))

    status, reason = _rule().evaluate(
        signal,
        best_bid=Decimal("99"),
        best_ask=Decimal("101"),
    )

    assert status == RuleStatus.PASS
    assert reason is None


def test_price_sanity_rejects_above_threshold(signal_factory) -> None:
    signal = signal_factory(price=Decimal("103.01"))

    status, reason = _rule().evaluate(
        signal,
        best_bid=Decimal("99"),
        best_ask=Decimal("101"),
    )

    assert status == RuleStatus.REJECT
    assert reason == "price_sanity_check_failed: deviation=0.0301% > 0.03%"


def test_price_sanity_rejects_below_threshold(signal_factory) -> None:
    signal = signal_factory(price=Decimal("96.99"))

    status, reason = _rule().evaluate(
        signal,
        best_bid=Decimal("99"),
        best_ask=Decimal("101"),
    )

    assert status == RuleStatus.REJECT
    assert reason == "price_sanity_check_failed: deviation=0.0301% > 0.03%"


def test_price_sanity_passes_market_order(signal_factory) -> None:
    signal = signal_factory(price=None)

    status, reason = _rule().evaluate(
        signal,
        best_bid=Decimal("99"),
        best_ask=Decimal("101"),
    )

    assert status == RuleStatus.PASS
    assert reason is None


def test_price_sanity_rejects_missing_market(signal_factory) -> None:
    signal = signal_factory(price=Decimal("100"))

    status, reason = _rule().evaluate(
        signal,
        best_bid=None,
        best_ask=Decimal("101"),
    )

    assert status == RuleStatus.REJECT
    assert reason == "price_sanity_check_missing_market"


def test_price_sanity_rejects_crossed_market(signal_factory) -> None:
    signal = signal_factory(price=Decimal("100"))

    status, reason = _rule().evaluate(
        signal,
        best_bid=Decimal("101"),
        best_ask=Decimal("99"),
    )

    assert status == RuleStatus.REJECT
    assert reason == "price_sanity_check_invalid_market"
