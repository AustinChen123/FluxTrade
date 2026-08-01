"""State-matrix tests for the authoritative Signal order classifier."""

from __future__ import annotations

from decimal import Decimal

import pytest

from src.core.models import SignalType
from src.core.signal_order_intent import (
    InvalidSignalOrderIntent,
    normalize_signal_quantity,
    resolve_signal_order_intent,
)


@pytest.mark.parametrize(
    ("price", "value", "expected_type", "expected_price", "expected_source"),
    [
        (Decimal("100"), None, "limit", Decimal("100"), "signal.price"),
        (Decimal("100"), Decimal("200"), "limit", Decimal("100"), "signal.price"),
        (Decimal("100"), Decimal("-1"), "limit", Decimal("100"), "signal.price"),
        (None, Decimal("200"), "limit", Decimal("200"), "signal.value"),
        (None, None, "market", None, "market"),
    ],
)
@pytest.mark.parametrize(
    "signal_type",
    [
        SignalType.LONG,
        SignalType.SHORT,
        SignalType.EXIT_LONG,
        SignalType.EXIT_SHORT,
        SignalType.NO_SIGNAL,
    ],
)
def test_resolve_signal_order_intent_preserves_legacy_state_matrix(
    signal_factory,
    signal_type,
    price,
    value,
    expected_type,
    expected_price,
    expected_source,
) -> None:
    signal = signal_factory(signal_type=signal_type, price=price, value=value)

    intent = resolve_signal_order_intent(signal)

    assert intent.order_type == expected_type
    assert intent.limit_price == expected_price
    assert intent.price_source == expected_source
    assert intent.uses_legacy_value_fallback is (expected_source == "signal.value")


@pytest.mark.parametrize(
    ("price", "value", "field"),
    [
        (Decimal("0"), None, "signal.price"),
        (Decimal("-1"), Decimal("200"), "signal.price"),
        (Decimal("NaN"), None, "signal.price"),
        (Decimal("Infinity"), None, "signal.price"),
        (None, Decimal("0"), "signal.value"),
        (None, Decimal("-1"), "signal.value"),
        (None, Decimal("NaN"), "signal.value"),
        (None, Decimal("Infinity"), "signal.value"),
    ],
)
def test_resolve_signal_order_intent_rejects_explicit_invalid_price(
    signal_factory,
    price,
    value,
    field,
) -> None:
    signal = signal_factory(price=None, value=None).model_copy(
        update={"price": price, "value": value}
    )

    with pytest.raises(
        InvalidSignalOrderIntent,
        match=rf"{field} must be finite and greater than zero",
    ):
        resolve_signal_order_intent(signal)


@pytest.mark.parametrize("signal_type", [SignalType.LONG, SignalType.SHORT])
@pytest.mark.parametrize("quantity", [None, Decimal("0"), Decimal("-1")])
def test_normalize_signal_quantity_applies_entry_default(
    signal_factory,
    signal_type,
    quantity,
) -> None:
    signal = signal_factory(signal_type=signal_type, quantity=quantity)

    normalized = normalize_signal_quantity(
        signal,
        default_entry_quantity=Decimal("2"),
    )

    assert normalized.quantity == Decimal("2")


@pytest.mark.parametrize("signal_type", [SignalType.LONG, SignalType.SHORT])
def test_normalize_signal_quantity_preserves_positive_entry_quantity(
    signal_factory,
    signal_type,
) -> None:
    signal = signal_factory(signal_type=signal_type, quantity=Decimal("3"))

    normalized = normalize_signal_quantity(
        signal,
        default_entry_quantity=Decimal("NaN"),
    )

    assert normalized is signal
    assert normalized.quantity == Decimal("3")


@pytest.mark.parametrize(
    "signal_type",
    [SignalType.EXIT_LONG, SignalType.EXIT_SHORT],
)
@pytest.mark.parametrize("quantity", [None, Decimal("0"), Decimal("-1")])
def test_normalize_signal_quantity_does_not_default_exit_quantity(
    signal_factory,
    signal_type,
    quantity,
) -> None:
    signal = signal_factory(signal_type=signal_type, quantity=quantity)

    normalized = normalize_signal_quantity(
        signal,
        default_entry_quantity=Decimal("2"),
    )

    assert normalized is signal
    assert normalized.quantity == quantity


@pytest.mark.parametrize(
    "signal_type",
    [
        SignalType.LONG,
        SignalType.SHORT,
        SignalType.EXIT_LONG,
        SignalType.EXIT_SHORT,
    ],
)
@pytest.mark.parametrize(
    "quantity",
    [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")],
)
def test_normalize_signal_quantity_rejects_nonfinite_quantity(
    signal_factory,
    signal_type,
    quantity,
) -> None:
    signal = signal_factory(signal_type=signal_type).model_copy(
        update={"quantity": quantity}
    )

    with pytest.raises(
        InvalidSignalOrderIntent,
        match=r"signal.quantity must be finite",
    ):
        normalize_signal_quantity(
            signal,
            default_entry_quantity=Decimal("2"),
        )


@pytest.mark.parametrize(
    "quantity",
    [Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")],
)
def test_normalize_signal_quantity_ignores_nonfinite_no_signal(
    signal_factory,
    quantity,
) -> None:
    signal = signal_factory(signal_type=SignalType.NO_SIGNAL).model_copy(
        update={"quantity": quantity}
    )

    normalized = normalize_signal_quantity(
        signal,
        default_entry_quantity=Decimal("NaN"),
    )

    assert normalized is signal


@pytest.mark.parametrize(
    "default_quantity",
    [Decimal("0"), Decimal("-1"), Decimal("NaN"), Decimal("Infinity")],
)
def test_normalize_signal_quantity_rejects_invalid_entry_default(
    signal_factory,
    default_quantity,
) -> None:
    signal = signal_factory(signal_type=SignalType.LONG, quantity=None)

    with pytest.raises(
        InvalidSignalOrderIntent,
        match=r"default_entry_quantity must be finite and greater than zero",
    ):
        normalize_signal_quantity(
            signal,
            default_entry_quantity=default_quantity,
        )
