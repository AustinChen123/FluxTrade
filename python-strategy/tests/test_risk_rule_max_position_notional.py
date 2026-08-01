"""Tests for max-position notional risk rule."""

from __future__ import annotations

from decimal import Decimal

from src.core.models import PositionSide, SignalType
from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.risk_rules.max_position_notional import MaxPositionNotionalRule
from src.core.product_registry import InstrumentSpec


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


def test_max_position_notional_applies_to_legacy_value_limit(signal_factory) -> None:
    signal = signal_factory(
        signal_type=SignalType.LONG,
        price=None,
        value=Decimal("50000.01"),
        quantity=Decimal("2"),
    )

    status, reason = _rule().evaluate(signal, None, mid_price=Decimal("50000"))

    assert status == RuleStatus.REJECT
    assert reason == "max_position_notional_exceeded: 100000.02 > 100000"


def test_max_position_notional_rejects_invalid_legacy_value(signal_factory) -> None:
    signal = signal_factory(
        signal_type=SignalType.LONG,
        price=None,
        quantity=Decimal("1"),
    ).model_copy(update={"value": Decimal("NaN")})

    status, reason = _rule().evaluate(signal, None, mid_price=Decimal("50000"))

    assert status == RuleStatus.REJECT
    assert reason == (
        "invalid_signal_order_intent: signal.value must be finite and greater than zero"
    )


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
        value=None,
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


def test_max_position_notional_multiplier_covers_add_reduce_and_flip(
    signal_factory,
    position_factory,
) -> None:
    spec = InstrumentSpec(
        product_id="BINANCE:BTCUSDT-PERP",
        exchange="test",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        multiplier=Decimal("2"),
    )
    position = position_factory(
        side=PositionSide.LONG,
        quantity=Decimal("1"),
        entry_price=Decimal("25000"),
    )

    cases = [
        (SignalType.LONG, Decimal("1"), RuleStatus.PASS),
        (SignalType.LONG, Decimal("1.01"), RuleStatus.REJECT),
        (SignalType.SHORT, Decimal("1"), RuleStatus.PASS),
        (SignalType.SHORT, Decimal("3"), RuleStatus.PASS),
        (SignalType.SHORT, Decimal("3.01"), RuleStatus.REJECT),
    ]
    for signal_type, quantity, expected in cases:
        signal = signal_factory(
            signal_type=signal_type,
            price=Decimal("25000"),
            quantity=quantity,
        )
        status, _ = _rule().evaluate(
            signal,
            position,
            mid_price=Decimal("25000"),
            instrument_spec=spec,
        )
        assert status == expected
