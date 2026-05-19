"""Tests for daily loss circuit-breaker risk rule."""

from __future__ import annotations

from decimal import Decimal

from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.risk_rules.daily_loss_circuit_breaker import (
    DailyLossCircuitBreakerRule,
)


def _rule() -> DailyLossCircuitBreakerRule:
    return DailyLossCircuitBreakerRule(
        RiskConfig(daily_loss_circuit_breaker_pct=Decimal("0.05"))
    )


def test_daily_loss_circuit_breaker_passes_when_nav_is_up() -> None:
    status, reason = _rule().evaluate(
        start_nav=Decimal("100000"),
        current_nav=Decimal("101000"),
    )

    assert status == RuleStatus.PASS
    assert reason is None


def test_daily_loss_circuit_breaker_passes_below_threshold() -> None:
    status, reason = _rule().evaluate(
        start_nav=Decimal("100000"),
        current_nav=Decimal("95000"),
    )

    assert status == RuleStatus.PASS
    assert reason is None


def test_daily_loss_circuit_breaker_triggers_above_threshold() -> None:
    status, reason = _rule().evaluate(
        start_nav=Decimal("100000"),
        current_nav=Decimal("94990"),
    )

    assert status == RuleStatus.CIRCUIT_BREAKER_TRIGGERED
    assert reason == "daily_loss_circuit_breaker_triggered: loss=5.01% > 5.00%"


def test_daily_loss_circuit_breaker_rejects_invalid_start_nav() -> None:
    status, reason = _rule().evaluate(
        start_nav=Decimal("0"),
        current_nav=Decimal("100"),
    )

    assert status == RuleStatus.REJECT
    assert reason == "daily_loss_invalid_start_nav: 0"
