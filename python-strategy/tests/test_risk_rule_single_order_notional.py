"""Tests for the single-order notional risk rule."""

from __future__ import annotations

from decimal import Decimal

from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.risk_rules.single_order_notional import SingleOrderNotionalRule


def _rule() -> SingleOrderNotionalRule:
    return SingleOrderNotionalRule(
        RiskConfig(max_single_order_notional_pct=Decimal("0.05"))
    )


def test_single_order_notional_passes_at_exact_limit(signal_factory) -> None:
    signal = signal_factory(price=Decimal("50000"), quantity=Decimal("0.1"))

    status, reason = _rule().evaluate(signal, nav=Decimal("100000"))

    assert status == RuleStatus.PASS
    assert reason is None


def test_single_order_notional_passes_below_limit(signal_factory) -> None:
    signal = signal_factory(price=Decimal("49999.90"), quantity=Decimal("0.1"))

    status, reason = _rule().evaluate(signal, nav=Decimal("100000"))

    assert status == RuleStatus.PASS
    assert reason is None


def test_single_order_notional_rejects_above_limit(signal_factory) -> None:
    signal = signal_factory(price=Decimal("50000.10"), quantity=Decimal("0.1"))

    status, reason = _rule().evaluate(signal, nav=Decimal("100000"))

    assert status == RuleStatus.REJECT
    assert reason == "single_order_notional_exceeded: 5000.010 > 5000.00"


def test_single_order_notional_passes_market_order(signal_factory) -> None:
    signal = signal_factory(price=None, quantity=Decimal("100"))

    status, reason = _rule().evaluate(signal, nav=Decimal("100000"))

    assert status == RuleStatus.PASS
    assert reason is None


def test_single_order_notional_rejects_missing_quantity(signal_factory) -> None:
    signal = signal_factory(price=Decimal("50000"), quantity=None)

    status, reason = _rule().evaluate(signal, nav=Decimal("100000"))

    assert status == RuleStatus.REJECT
    assert reason == "single_order_notional_missing_quantity"


def test_single_order_notional_rejects_non_positive_nav(signal_factory) -> None:
    signal = signal_factory(price=Decimal("50000"), quantity=Decimal("0.1"))

    status, reason = _rule().evaluate(signal, nav=Decimal("0"))

    assert status == RuleStatus.REJECT
    assert reason == "single_order_notional_invalid_nav: 0"
