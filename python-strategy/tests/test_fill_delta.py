from decimal import Decimal

import pytest

from src.core.fill_delta import (
    FillDeltaState,
    classify_fill_delta,
    delta_price_from_cumulative_average,
    fill_delta_from_cumulative,
    snapshot_fill_delta,
)


@pytest.mark.parametrize(
    (
        "local_filled",
        "local_average_price",
        "cumulative_filled",
        "cumulative_average_price",
        "expected_state",
        "expected_quantity",
        "expected_price",
    ),
    [
        (
            Decimal("0"),
            None,
            None,
            None,
            FillDeltaState.NO_FILL,
            None,
            None,
        ),
        (
            Decimal("0"),
            None,
            Decimal("0"),
            None,
            FillDeltaState.NO_FILL,
            None,
            None,
        ),
        (
            Decimal("0.10"),
            Decimal("100"),
            Decimal("0.04"),
            Decimal("100"),
            FillDeltaState.LOCAL_OVERSTATED,
            Decimal("-0.06"),
            Decimal("100"),
        ),
        (
            Decimal("0.10"),
            Decimal("100"),
            Decimal("0.10"),
            Decimal("100"),
            FillDeltaState.CONVERGED,
            Decimal("0"),
            Decimal("100"),
        ),
        (
            Decimal("0.04"),
            Decimal("100"),
            Decimal("0.10"),
            None,
            FillDeltaState.DELTA_UNPRICED,
            Decimal("0.06"),
            None,
        ),
        (
            Decimal("0"),
            None,
            Decimal("0.04"),
            Decimal("101"),
            FillDeltaState.DELTA_PRICED,
            Decimal("0.04"),
            Decimal("101"),
        ),
        (
            Decimal("0.04"),
            Decimal("100"),
            Decimal("0.10"),
            Decimal("102.4"),
            FillDeltaState.DELTA_PRICED,
            Decimal("0.06"),
            Decimal("104"),
        ),
    ],
)
def test_classify_fill_delta_matrix(
    local_filled,
    local_average_price,
    cumulative_filled,
    cumulative_average_price,
    expected_state,
    expected_quantity,
    expected_price,
):
    state, delta = classify_fill_delta(
        local_filled=local_filled,
        local_average_price=local_average_price,
        cumulative_filled=cumulative_filled,
        cumulative_average_price=cumulative_average_price,
        cumulative_fee=Decimal("0.001"),
    )

    assert state == expected_state
    if expected_quantity is None:
        assert delta is None
        return
    assert delta is not None
    assert delta["quantity"] == expected_quantity
    assert delta["price"] == expected_price


@pytest.mark.parametrize(
    ("local_filled", "expected_fee"),
    [
        (Decimal("0"), Decimal("0.001")),
        (Decimal("0.04"), None),
    ],
)
def test_snapshot_fill_delta_applies_cumulative_fee_only_to_first_recorded_fill(
    local_filled,
    expected_fee,
):
    delta = snapshot_fill_delta(
        local_filled=local_filled,
        local_average_price=Decimal("100") if local_filled > 0 else None,
        cumulative_filled=Decimal("0.10"),
        cumulative_average_price=Decimal("101"),
        cumulative_fee=Decimal("0.001"),
    )

    assert delta is not None
    assert "fee" in delta
    assert delta["fee"] == expected_fee


def test_fill_delta_from_cumulative_recomputes_delta_price():
    delta = fill_delta_from_cumulative(
        local_filled=Decimal("0.04"),
        local_average_price=Decimal("100"),
        cumulative_filled=Decimal("0.10"),
        cumulative_average_price=Decimal("102.4"),
    )

    assert delta == {"quantity": Decimal("0.06"), "price": Decimal("104")}


@pytest.mark.parametrize("local_average_price", [None, Decimal("0")])
def test_delta_price_from_cumulative_average_requires_local_average_for_catch_up(
    local_average_price,
):
    price = delta_price_from_cumulative_average(
        local_filled=Decimal("0.04"),
        local_average_price=local_average_price,
        cumulative_filled=Decimal("0.10"),
        cumulative_average_price=Decimal("102.4"),
        delta=Decimal("0.06"),
    )

    assert price is None
