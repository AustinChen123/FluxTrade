from __future__ import annotations

from decimal import Decimal
from typing import cast

import pytest
from pydantic import ValidationError

from src.control_plane.full_backtest_request import FullBacktestInstrument
from src.core.product_registry import (
    CapitalModel,
    FeeModel,
    InstrumentSpec,
    MarketType,
)
from test_full_backtest_request import _payload


def _instrument() -> dict[str, object]:
    return cast(dict[str, object], _payload()["instrument"])


def _replace(field: str, value: object) -> dict[str, object]:
    return _instrument() | {field: value}


@pytest.mark.parametrize(
    "field",
    [
        "quantity_step",
        "price_tick",
        "min_notional",
        "min_quantity",
        "multiplier",
        "tick_value",
        "capital_per_contract",
    ],
)
@pytest.mark.parametrize("value", [1, 1.25, True, "NaN", "Infinity", "-Infinity"])
def test_optional_financial_fields_require_finite_decimal_inputs(
    field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        FullBacktestInstrument.model_validate(_replace(field, value))


def test_optional_instrument_decimals_allow_null_and_no_new_sign_policy() -> None:
    parsed = FullBacktestInstrument.model_validate(
        _instrument()
        | {
            "quantity_step": None,
            "price_tick": None,
            "min_notional": "-0.25",
            "min_quantity": "-0.5",
            "multiplier": None,
            "tick_value": "-1.75",
            "capital_model": "notional",
            "capital_per_contract": None,
        }
    )
    assert parsed.quantity_step is None
    assert parsed.price_tick is None
    assert parsed.min_notional == Decimal("-0.25")
    assert parsed.min_quantity == Decimal("-0.5")
    assert parsed.tick_value == Decimal("-1.75")


@pytest.mark.parametrize("field", ["product_id", "exchange", "symbol", "base", "quote"])
@pytest.mark.parametrize("value", ["", "   ", 12])
def test_instrument_identity_text_is_required_nonblank_and_strict(
    field: str, value: object
) -> None:
    with pytest.raises(ValidationError):
        FullBacktestInstrument.model_validate(_replace(field, value))


@pytest.mark.parametrize("value", ["", " ", 12])
def test_session_calendar_is_null_or_nonblank_text(value: object) -> None:
    with pytest.raises(ValidationError):
        FullBacktestInstrument.model_validate(_replace("session_calendar_id", value))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("fee_model", "unknown"),
        ("fee_model", FeeModel.PER_CONTRACT),
        ("capital_model", "unknown"),
        ("capital_model", CapitalModel.PER_CONTRACT),
        ("market_type", "unknown"),
        ("market_type", MarketType.CONTINUOUS_FUTURE),
    ],
)
def test_enum_values_require_known_strings(field: str, value: object) -> None:
    with pytest.raises(ValidationError):
        FullBacktestInstrument.model_validate(_replace(field, value))


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("multiplier", "0"),
        ("multiplier", "-1"),
        ("quantity_step", "0"),
        ("price_tick", "-0.1"),
        ("capital_per_contract", "0"),
    ],
)
def test_existing_positive_instrument_constraints(field: str, value: str) -> None:
    instrument = _instrument() | {
        "capital_model": "per_contract",
        "capital_per_contract": "10",
        field: value,
    }
    with pytest.raises(ValidationError):
        FullBacktestInstrument.model_validate(instrument)


@pytest.mark.parametrize(
    ("capital_model", "capital_per_contract"),
    [("per_contract", None), ("per_contract", "0"), ("notional", "10")],
)
def test_existing_capital_model_constraints(
    capital_model: str, capital_per_contract: str | None
) -> None:
    with pytest.raises(ValidationError):
        FullBacktestInstrument.model_validate(
            _instrument()
            | {
                "capital_model": capital_model,
                "capital_per_contract": capital_per_contract,
            }
        )


@pytest.mark.parametrize("field", ["quantity_step", "price_tick"])
def test_dated_future_requires_existing_order_increments(field: str) -> None:
    instrument = _instrument() | {
        "product_id": "RITHMIC:MNQ-202509",
        "market_type": "dated_future",
    }
    instrument.pop(field)
    with pytest.raises(ValidationError):
        FullBacktestInstrument.model_validate(instrument)


def test_product_market_type_must_match() -> None:
    with pytest.raises(ValidationError):
        FullBacktestInstrument.model_validate(_replace("market_type", "spot"))


def test_instrument_conversion_preserves_all_declared_fields() -> None:
    parsed = FullBacktestInstrument.model_validate(_instrument())
    assert parsed.to_instrument_spec() == InstrumentSpec(
        product_id="RITHMIC:MNQ-CONTINUOUS",
        exchange="rithmic",
        symbol="MNQ",
        base="MNQ",
        quote="USD",
        quantity_step=Decimal("0.0001"),
        price_tick=Decimal("0.25"),
        min_notional=Decimal("0.0000000000000000000003"),
        min_quantity=Decimal("0.0002"),
        multiplier=Decimal("2"),
        tick_value=Decimal("0.50"),
        fee_model=FeeModel.PER_CONTRACT,
        capital_model=CapitalModel.PER_CONTRACT,
        capital_per_contract=Decimal("8000"),
        session_calendar_id="CME_GLOBEX",
        market_type=MarketType.CONTINUOUS_FUTURE,
    )


def test_instrument_is_immutable_and_forbids_unknown_fields() -> None:
    parsed = FullBacktestInstrument.model_validate(_instrument())
    with pytest.raises(ValidationError):
        parsed.quote = "EUR"  # type: ignore[misc]
    with pytest.raises(ValidationError):
        FullBacktestInstrument.model_validate(_replace("extra", "value"))
