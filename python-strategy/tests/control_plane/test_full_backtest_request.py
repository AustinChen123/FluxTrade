from __future__ import annotations

from decimal import Decimal, localcontext

import pytest
from pydantic import ValidationError

from src.control_plane.full_backtest_request import FullBacktestFees, _strict_text


def _payload() -> dict[str, object]:
    return {
        "kind": "full_strategy_backtest",
        "dataset_id": "sealed-mnq",
        "strategy_id": "catalog::GoldenCross",
        "artifact_version": "v1.2.3",
        "start": 0,
        "end": 253_402_300_799_999,
        "initial_balance": "10000.0000000000000000000001",
        "instrument": {
            "product_id": "RITHMIC:MNQ-CONTINUOUS",
            "exchange": "rithmic",
            "symbol": "MNQ",
            "base": "MNQ",
            "quote": "USD",
            "quantity_step": "0.0001",
            "price_tick": "0.25",
            "min_notional": "0.0000000000000000000003",
            "min_quantity": "0.0002",
            "multiplier": "2",
            "tick_value": "0.50",
            "fee_model": "per_contract",
            "capital_model": "per_contract",
            "capital_per_contract": "8000",
            "session_calendar_id": "CME_GLOBEX",
            "market_type": "continuous_future",
        },
        "fees": {"maker": "0.0002", "taker": "0.0006"},
        "drawdown_limit": None,
    }


def test_text_is_strict_and_preserves_nonblank_value() -> None:
    assert _strict_text(" x ", "name") == " x "
    with pytest.raises(ValueError):
        _strict_text(" ", "name")
    with pytest.raises(ValueError):
        _strict_text(1, "name")
    with pytest.raises(ValueError):
        _strict_text("a/b", "name", path_free=True)
    with pytest.raises(ValueError):
        _strict_text(r"a\b", "name", path_free=True)


def test_fees_accept_exact_decimal_and_decimal_strings_under_low_context() -> None:
    with localcontext() as context:
        context.prec = 2
        fees = FullBacktestFees.model_validate(
            {"maker": Decimal("0.0002000000000000000001"), "taker": "0.0006"}
        )

    assert fees.maker == Decimal("0.0002000000000000000001")
    assert fees.taker == Decimal("0.0006")


@pytest.mark.parametrize("value", [1, 1.25, True, "NaN", "Infinity", "-Infinity"])
@pytest.mark.parametrize("field", ["maker", "taker"])
def test_fee_inputs_reject_non_decimal_types_and_nonfinite_values(
    field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        FullBacktestFees.model_validate({"maker": "0", "taker": "0"} | {field: value})


@pytest.mark.parametrize("field", ["maker", "taker"])
def test_fee_inputs_must_be_nonnegative_and_dto_is_immutable(field: str) -> None:
    with pytest.raises(ValidationError):
        FullBacktestFees.model_validate({"maker": "0", "taker": "0"} | {field: "-0.1"})

    fees = FullBacktestFees.model_validate({"maker": "0", "taker": "0"})
    with pytest.raises(ValidationError):
        setattr(fees, field, Decimal("1"))

    with pytest.raises(ValidationError):
        FullBacktestFees.model_validate(
            {"maker": "0", "taker": "0", "unexpected": "value"}
        )
