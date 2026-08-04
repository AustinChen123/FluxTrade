from copy import deepcopy
from decimal import Decimal
from typing import cast

import pytest
from pydantic import ValidationError

from src.core.backtest.endpoint_state import ReplayEndpointState
from src.validation.trading_outcome import TradingOutcome


def _payload() -> dict[str, object]:
    return {
        "signals": (
            {
                "strategy_id": "s",
                "product_id": "MNQ",
                "timeframe": "5m",
                "timestamp_ms": 1,
                "signal_type": "LONG",
                "value": Decimal("1.0"),
                "quantity": Decimal("2.00"),
                "price": None,
                "stop_loss": None,
                "take_profit": None,
                "trailing_distance": None,
                "metadata_json": {"z": Decimal("-0.00"), "a": [True, None]},
            },
        ),
        "order_observations": (
            {
                "logical_order_id": "order-1",
                "parent_logical_order_id": None,
                "linked_logical_order_id": None,
                "strategy_id": "s",
                "product_id": "MNQ",
                "timestamp_ms": 2,
                "phase": "submitted",
                "status": "NEW",
                "order_type": "LIMIT",
                "side": "buy",
                "quantity": Decimal("2"),
                "filled_quantity": Decimal("0"),
                "price": Decimal("100"),
                "trigger_price": None,
                "trailing_distance": None,
            },
        ),
        "fills": (
            {
                "logical_order_id": "order-1",
                "strategy_id": "s",
                "product_id": "MNQ",
                "timestamp_ms": 3,
                "fill_type": "entry",
                "side": "buy",
                "price": Decimal("100"),
                "quantity": Decimal("2"),
                "fee": Decimal("1"),
            },
        ),
        "endpoint_state": ReplayEndpointState(
            positions=(),
            working_orders=(),
            final_mark=Decimal("100"),
            end_timestamp=3,
            halted_early=False,
        ),
        "financial": {
            "fees": Decimal("1"),
            "realized_pnl": Decimal("0"),
            "unrealized_pnl": Decimal("2"),
            "equity": Decimal("1002"),
        },
        "journal": (
            {
                "strategy_id": "s",
                "timestamp_ms": 4,
                "tag": "fill",
                "logical_trade_id": "trade-1",
                "data_json": {"price": Decimal("100")},
            },
        ),
    }


def _items(payload: dict[str, object], section: str) -> tuple[dict[str, object], ...]:
    return cast(tuple[dict[str, object], ...], payload[section])


def _outcome(payload: dict[str, object] | None = None) -> TradingOutcome:
    return TradingOutcome.model_validate(payload or _payload())


def test_canonical_decimal_mapping_and_first_difference() -> None:
    equivalent = deepcopy(_payload())
    cast(dict[str, object], equivalent["financial"])["equity"] = Decimal("1002.000")
    _items(equivalent, "signals")[0]["metadata_json"] = {
        "a": [True, None],
        "z": Decimal("0"),
    }
    expected, actual = _outcome(), _outcome(equivalent)
    assert expected.canonical_bytes() == actual.canonical_bytes()
    assert expected.sha256() == actual.sha256()
    assert expected.first_difference(actual) is None
    changed = deepcopy(_payload())
    _items(changed, "fills")[0]["fee"] = Decimal("2")
    actual = _outcome(changed)
    difference = expected.first_difference(actual)
    assert expected.sha256() != actual.sha256() and difference is not None
    assert (
        difference.path,
        difference.kind,
        difference.expected_json,
        difference.actual_json,
    ) == ("$.fills[0].fee", "value", '["decimal","1"]', '["decimal","2"]')


def test_dynamic_int_and_decimal_are_collision_free() -> None:
    integer, decimal = _payload(), _payload()
    _items(integer, "journal")[0]["data_json"] = 1
    _items(decimal, "journal")[0]["data_json"] = Decimal("1")
    assert _outcome(integer).sha256() != _outcome(decimal).sha256()
    assert _outcome(integer).first_difference(_outcome(decimal)) is not None


@pytest.mark.parametrize(
    "section", ["signals", "order_observations", "fills", "journal"]
)
def test_causal_order_and_length_are_observable(section: str) -> None:
    payload = _payload()
    items = list(_items(payload, section))
    items.append(deepcopy(items[0]))
    items[1]["timestamp_ms"] = 99
    payload[section] = tuple(items)
    swapped = deepcopy(payload)
    swapped[section] = tuple(reversed(_items(swapped, section)))
    difference = _outcome(payload).first_difference(_outcome(swapped))
    assert difference is not None and difference.path == f"$.{section}[0].timestamp_ms"
    shorter = deepcopy(payload)
    shorter[section] = _items(shorter, section)[:-1]
    difference = _outcome(payload).first_difference(_outcome(shorter))
    assert difference is not None and difference.kind == "length"


@pytest.mark.parametrize(
    "section,fields",
    [
        (
            "signals",
            (
                "value",
                "quantity",
                "price",
                "stop_loss",
                "take_profit",
                "trailing_distance",
            ),
        ),
        (
            "order_observations",
            (
                "quantity",
                "filled_quantity",
                "price",
                "trigger_price",
                "trailing_distance",
            ),
        ),
        ("fills", ("price", "quantity", "fee")),
        ("financial", ("fees", "realized_pnl", "unrealized_pnl", "equity")),
    ],
)
def test_financial_fields_reject_float_and_non_finite_decimal(
    section: str, fields: tuple[str, ...]
) -> None:
    for field in fields:
        for invalid in (1.0, Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")):
            payload = _payload()
            target = (
                cast(dict[str, object], payload[section])
                if section == "financial"
                else _items(payload, section)[0]
            )
            target[field] = invalid
            with pytest.raises(ValidationError):
                _outcome(payload)


@pytest.mark.parametrize(
    "invalid", [1.0, Decimal("NaN"), {"x": 1.0}, {1: "x"}, {1}, b"x", object()]
)
@pytest.mark.parametrize(
    "section,field", [("signals", "metadata_json"), ("journal", "data_json")]
)
def test_dynamic_values_fail_closed(invalid: object, section: str, field: str) -> None:
    payload = _payload()
    _items(payload, section)[0][field] = invalid
    with pytest.raises(ValidationError):
        _outcome(payload)


@pytest.mark.parametrize(
    "section",
    [
        "signals",
        "order_observations",
        "fills",
        "endpoint_state",
        "financial",
        "journal",
    ],
)
def test_every_top_level_section_is_required(section: str) -> None:
    missing = _payload()
    del missing[section]
    with pytest.raises(ValidationError):
        _outcome(missing)


@pytest.mark.parametrize(
    "section,field",
    [
        ("signals", field)
        for field in (
            "value",
            "quantity",
            "price",
            "stop_loss",
            "take_profit",
            "trailing_distance",
        )
    ]
    + [
        ("order_observations", field)
        for field in (
            "parent_logical_order_id",
            "linked_logical_order_id",
            "price",
            "trigger_price",
            "trailing_distance",
        )
    ]
    + [("journal", "logical_trade_id")],
)
def test_every_nullable_observation_field_is_required(section: str, field: str) -> None:
    missing = _payload()
    del _items(missing, section)[0][field]
    with pytest.raises(ValidationError):
        _outcome(missing)


def test_empty_and_null_dynamic_values_and_empty_sequence_are_distinct() -> None:
    digests = set()
    for value in ({}, "", None, [], ()):
        payload = _payload()
        _items(payload, "journal")[0]["data_json"] = value
        digests.add(_outcome(payload).sha256())
    assert len(digests) == 5
    empty = deepcopy(_payload())
    empty["signals"] = ()
    difference = _outcome().first_difference(_outcome(empty))
    assert difference is not None and difference.kind == "length"


def test_identity_controls() -> None:
    raw = _payload()
    _items(raw, "fills")[0]["provider_order_id"] = "random"
    with pytest.raises(ValidationError):
        _outcome(raw)
    blank = _payload()
    _items(blank, "fills")[0]["logical_order_id"] = ""
    with pytest.raises(ValidationError):
        _outcome(blank)
