from decimal import Decimal
from enum import Enum
from typing import Optional


class FillDeltaState(str, Enum):
    NO_FILL = "no_fill"
    CONVERGED = "converged"
    DELTA_PRICED = "delta_priced"
    DELTA_UNPRICED = "delta_unpriced"
    LOCAL_OVERSTATED = "local_overstated"


def snapshot_fill_delta(
    *,
    local_filled: Decimal,
    local_average_price: Decimal | None,
    cumulative_filled: Decimal | None,
    cumulative_average_price: Decimal | None,
    cumulative_fee: Decimal | None = None,
) -> Optional[dict[str, Optional[Decimal]]]:
    if cumulative_filled is None or cumulative_filled <= 0:
        return None
    fill_delta = fill_delta_from_cumulative(
        local_filled=local_filled,
        local_average_price=local_average_price,
        cumulative_filled=cumulative_filled,
        cumulative_average_price=cumulative_average_price,
    )
    fill_delta["fee"] = cumulative_fee if local_filled <= 0 else None
    return fill_delta


def classify_fill_delta(
    *,
    local_filled: Decimal,
    local_average_price: Decimal | None,
    cumulative_filled: Decimal | None,
    cumulative_average_price: Decimal | None,
    cumulative_fee: Decimal | None = None,
) -> tuple[FillDeltaState, Optional[dict[str, Optional[Decimal]]]]:
    fill_delta = snapshot_fill_delta(
        local_filled=local_filled,
        local_average_price=local_average_price,
        cumulative_filled=cumulative_filled,
        cumulative_average_price=cumulative_average_price,
        cumulative_fee=cumulative_fee,
    )
    if fill_delta is None:
        return FillDeltaState.NO_FILL, None
    if fill_delta["quantity"] < 0:
        return FillDeltaState.LOCAL_OVERSTATED, fill_delta
    if fill_delta["quantity"] == 0:
        return FillDeltaState.CONVERGED, fill_delta
    if fill_delta["price"] is None:
        return FillDeltaState.DELTA_UNPRICED, fill_delta
    return FillDeltaState.DELTA_PRICED, fill_delta


def fill_delta_from_cumulative(
    *,
    local_filled: Decimal,
    local_average_price: Decimal | None,
    cumulative_filled: Decimal,
    cumulative_average_price: Decimal | None,
) -> dict[str, Decimal | None]:
    delta = cumulative_filled - local_filled
    if delta <= 0:
        return {"quantity": delta, "price": cumulative_average_price}
    if cumulative_average_price is None:
        return {"quantity": delta, "price": None}
    return {
        "quantity": delta,
        "price": delta_price_from_cumulative_average(
            local_filled=local_filled,
            local_average_price=local_average_price,
            cumulative_filled=cumulative_filled,
            cumulative_average_price=cumulative_average_price,
            delta=delta,
        ),
    }


def delta_price_from_cumulative_average(
    *,
    local_filled: Decimal,
    local_average_price: Decimal | None,
    cumulative_filled: Decimal,
    cumulative_average_price: Decimal,
    delta: Decimal,
) -> Decimal | None:
    if local_filled <= 0:
        return cumulative_average_price
    if local_average_price is None or local_average_price <= 0:
        return None
    cumulative_cost = cumulative_filled * cumulative_average_price
    local_cost = local_filled * local_average_price
    return (cumulative_cost - local_cost) / delta
