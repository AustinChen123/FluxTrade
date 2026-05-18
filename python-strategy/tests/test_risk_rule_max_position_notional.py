"""Tests for max-position notional risk rule."""

from __future__ import annotations

from decimal import Decimal

from src.core.models import PositionSide, SignalType
from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.risk_rules.max_position_notional import MaxPositionNotionalRule


def _rule() -> MaxPositionNotionalRule:
    return MaxPositionNotionalRule(RiskConfig(max_position_notional=Decimal("100000")))


def test_max_position_notional_passes_without_position_under_limit(signal_factory) -> None:
    signal = signal_factory(
        signal_type=SignalType.LONG,
        price=Decimal("50000"),
        quantity=Decimal("2"),
    )

    status, reason = _rule().evaluate(signal, None, mid_price=Decimal("50000"))

    assert status == RuleStatus.PASS
    assert reason is None


def test_max_position_notional_rejects_without_position_over_limit(signal_factory) -> None:
    signal = signal_factory(
        signal_type=SignalType.LONG,
        price=Decimal("50000.01"),
        quantity=Decimal("2"),
    )

    status, reason = _rule().evaluate(signal, None, mid_price=Decimal("50000"))

    assert status == RuleStatus.REJECT
    assert reason == "max_position_notional_exceeded: 100000.02 > 100000"


def test_max_position_notional_rejects_same_side_add_over_limit(
    signal_factory,
    position_factory,
) -> None:
    position = position_factory(
        side=PositionSide.LONG,
        quantity=Decimal("1.5"),
        entry_price=Decimal("40000"),
    )
    signal = signal_factory(
        signal_type=SignalType.LONG,
        price=Decimal("50000"),
        quantity=Decimal("0.6"),
    )

    status, reason = _rule().evaluate(
        signal,
        position,
        mid_price=Decimal("50000"),
    )

    assert status == RuleStatus.REJECT
    assert reason == "max_position_notional_exceeded: 105000.0 > 100000"


def test_max_position_notional_allows_opposite_side_reduction(
    signal_factory,
    position_factory,
) -> None:
    position = position_factory(
        side=PositionSide.LONG,
        quantity=Decimal("2"),
        entry_price=Decimal("50000"),
    )
    signal = signal_factory(
        signal_type=SignalType.SHORT,
        price=Decimal("50000"),
        quantity=Decimal("1"),
    )

    status, reason = _rule().evaluate(
        signal,
        position,
        mid_price=Decimal("50000"),
    )

    assert status == RuleStatus.PASS
    assert reason is None


def test_max_position_notional_checks_remaining_notional_after_flip(
    signal_factory,
    position_factory,
) -> None:
    position = position_factory(
        side=PositionSide.LONG,
        quantity=Decimal("1"),
        entry_price=Decimal("50000"),
    )
    signal = signal_factory(
        signal_type=SignalType.SHORT,
        price=Decimal("50000"),
        quantity=Decimal("4"),
    )

    status, reason = _rule().evaluate(
        signal,
        position,
        mid_price=Decimal("50000"),
    )

    assert status == RuleStatus.REJECT
    assert reason == "max_position_notional_exceeded: 150000 > 100000"


def test_max_position_notional_uses_mid_price_for_market_order(signal_factory) -> None:
    signal = signal_factory(
        signal_type=SignalType.SHORT,
        price=None,
        quantity=Decimal("1.5"),
    )

    status, reason = _rule().evaluate(signal, None, mid_price=Decimal("60000"))

    assert status == RuleStatus.PASS
    assert reason is None


def test_max_position_notional_allows_exit_without_quantity(signal_factory) -> None:
    signal = signal_factory(signal_type=SignalType.EXIT_LONG, quantity=None)

    status, reason = _rule().evaluate(signal, None, mid_price=Decimal("50000"))

    assert status == RuleStatus.PASS
    assert reason is None


def test_max_position_notional_rejects_missing_quantity(signal_factory) -> None:
    signal = signal_factory(signal_type=SignalType.LONG, price=Decimal("50000"))

    status, reason = _rule().evaluate(signal, None, mid_price=Decimal("50000"))

    assert status == RuleStatus.REJECT
    assert reason == "max_position_notional_missing_quantity"


def test_max_position_notional_rejects_invalid_mid_price(signal_factory) -> None:
    signal = signal_factory(
        signal_type=SignalType.LONG,
        price=Decimal("50000"),
        quantity=Decimal("1"),
    )

    status, reason = _rule().evaluate(signal, None, mid_price=Decimal("0"))

    assert status == RuleStatus.REJECT
    assert reason == "max_position_notional_invalid_mid_price: 0"
