"""Unit tests for JSONB Decimal serialization helpers."""

from __future__ import annotations

import copy
from decimal import Decimal, InvalidOperation

import pytest

from src.core.jsonb_helpers import (
    decimal_to_jsonb_str,
    deserialize_payload_with_decimals,
    jsonb_to_decimal,
    serialize_payload_with_decimals,
)


# ---------------------------------------------------------------------------
# decimal_to_jsonb_str
# ---------------------------------------------------------------------------


def test_decimal_to_jsonb_str_positive():
    assert decimal_to_jsonb_str(Decimal("123.456")) == "123.456"


def test_decimal_to_jsonb_str_negative():
    # Decimal.__str__ may use scientific notation for very small/large values;
    # what matters is lossless round-trip, not the textual form.
    original = Decimal("-0.00000001")
    serialized = decimal_to_jsonb_str(original)
    assert Decimal(serialized) == original
    assert decimal_to_jsonb_str(Decimal("-12.5")) == "-12.5"


def test_decimal_to_jsonb_str_zero():
    assert decimal_to_jsonb_str(Decimal("0")) == "0"


def test_decimal_to_jsonb_str_high_precision():
    # 28-digit precision, default Decimal context
    high = Decimal("1.2345678901234567890123456789")
    assert decimal_to_jsonb_str(high) == "1.2345678901234567890123456789"


def test_decimal_to_jsonb_str_scientific_input():
    # Decimal preserves whatever notation it's constructed with
    assert decimal_to_jsonb_str(Decimal("1E+10")) == "1E+10"


def test_decimal_to_jsonb_str_rejects_non_decimal():
    with pytest.raises(TypeError):
        decimal_to_jsonb_str("123.456")  # type: ignore[arg-type]


def test_decimal_to_jsonb_str_rejects_float():
    with pytest.raises(TypeError):
        decimal_to_jsonb_str(123.456)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# jsonb_to_decimal
# ---------------------------------------------------------------------------


def test_jsonb_to_decimal_none():
    assert jsonb_to_decimal(None) is None


def test_jsonb_to_decimal_valid_string():
    assert jsonb_to_decimal("42.42") == Decimal("42.42")


def test_jsonb_to_decimal_negative_string():
    assert jsonb_to_decimal("-7.5") == Decimal("-7.5")


def test_jsonb_to_decimal_empty_string_raises():
    with pytest.raises(InvalidOperation):
        jsonb_to_decimal("")


def test_jsonb_to_decimal_invalid_string_raises():
    with pytest.raises(InvalidOperation):
        jsonb_to_decimal("not-a-number")


def test_jsonb_to_decimal_rejects_non_string():
    with pytest.raises(TypeError):
        jsonb_to_decimal(123)  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# serialize_payload_with_decimals
# ---------------------------------------------------------------------------


def test_serialize_flat_dict():
    payload = {"price": Decimal("100.5"), "qty": Decimal("0.01"), "symbol": "BTC"}
    result = serialize_payload_with_decimals(payload)
    assert result == {"price": "100.5", "qty": "0.01", "symbol": "BTC"}


def test_serialize_nested_dict():
    payload = {
        "order": {
            "price": Decimal("50000"),
            "fees": {"maker": Decimal("0.0001"), "taker": Decimal("0.0004")},
        }
    }
    result = serialize_payload_with_decimals(payload)
    assert result == {
        "order": {
            "price": "50000",
            "fees": {"maker": "0.0001", "taker": "0.0004"},
        }
    }


def test_serialize_list_of_decimals():
    payload = {"prices": [Decimal("1"), Decimal("2"), Decimal("3.14")]}
    result = serialize_payload_with_decimals(payload)
    assert result == {"prices": ["1", "2", "3.14"]}


def test_serialize_preserves_non_decimal_types():
    payload = {
        "active": True,
        "count": 7,
        "ratio": 0.5,  # float allowed in non-monetary context
        "name": "alpha",
        "missing": None,
        "amount": Decimal("9.99"),
    }
    result = serialize_payload_with_decimals(payload)
    assert result["active"] is True
    assert result["count"] == 7
    assert result["ratio"] == 0.5
    assert result["name"] == "alpha"
    assert result["missing"] is None
    assert result["amount"] == "9.99"


def test_serialize_does_not_mutate_input():
    payload = {"price": Decimal("100"), "nested": {"qty": Decimal("1")}}
    snapshot = copy.deepcopy(payload)
    serialize_payload_with_decimals(payload)
    assert payload == snapshot


def test_serialize_rejects_non_dict():
    with pytest.raises(TypeError):
        serialize_payload_with_decimals([Decimal("1")])  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# deserialize_payload_with_decimals
# ---------------------------------------------------------------------------


def test_deserialize_targeted_keys():
    payload = {"price": "100.5", "symbol": "BTC", "qty": "0.01"}
    result = deserialize_payload_with_decimals(payload, {"price", "qty"})
    assert result == {"price": Decimal("100.5"), "symbol": "BTC", "qty": Decimal("0.01")}


def test_deserialize_skips_unspecified_keys():
    payload = {"price": "100.5", "note": "1.23"}
    result = deserialize_payload_with_decimals(payload, {"price"})
    assert result["price"] == Decimal("100.5")
    assert result["note"] == "1.23"  # untouched, still str


def test_deserialize_nested_keys():
    payload = {
        "order": {
            "price": "50000",
            "meta": {"fee": "0.0001", "label": "maker"},
        }
    }
    result = deserialize_payload_with_decimals(payload, {"price", "fee"})
    assert result["order"]["price"] == Decimal("50000")
    assert result["order"]["meta"]["fee"] == Decimal("0.0001")
    assert result["order"]["meta"]["label"] == "maker"


def test_deserialize_does_not_mutate_input():
    payload = {"price": "100", "nested": {"qty": "1"}}
    snapshot = copy.deepcopy(payload)
    deserialize_payload_with_decimals(payload, {"price", "qty"})
    assert payload == snapshot


def test_deserialize_rejects_non_dict():
    with pytest.raises(TypeError):
        deserialize_payload_with_decimals(["100"], {"price"})  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# Round-trip
# ---------------------------------------------------------------------------


def test_round_trip_preserves_high_precision():
    original = {
        "price": Decimal("1.2345678901234567890123456789"),
        "fees": {"maker": Decimal("-0.00000001"), "taker": Decimal("0.0004")},
        "history": [Decimal("1"), Decimal("2.5")],
    }
    serialized = serialize_payload_with_decimals(original)
    decimal_keys = {"price", "maker", "taker"}
    restored = deserialize_payload_with_decimals(serialized, decimal_keys)

    assert restored["price"] == original["price"]
    assert restored["fees"]["maker"] == original["fees"]["maker"]
    assert restored["fees"]["taker"] == original["fees"]["taker"]
    # history not in decimal_keys → remains strings
    assert restored["history"] == ["1", "2.5"]
