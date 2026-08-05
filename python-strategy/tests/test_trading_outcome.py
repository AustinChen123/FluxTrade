from copy import deepcopy
from decimal import Decimal, DecimalTuple, getcontext, localcontext
from typing import cast

import pytest
from pydantic import ValidationError

from src.core.backtest.endpoint_state import (
    EndpointOrder,
    EndpointPosition,
    ReplayEndpointState,
)
from src.core.models import OrderSide, PositionSide
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
    return TradingOutcome.model_validate(_payload() if payload is None else payload)


def test_outcome_preserves_explicit_empty_mapping() -> None:
    with pytest.raises(ValidationError) as exc_info:
        _outcome({})

    errors = exc_info.value.errors()
    assert len(errors) == 6
    assert {error["loc"] for error in errors} == {
        ("signals",),
        ("order_observations",),
        ("fills",),
        ("endpoint_state",),
        ("financial",),
        ("journal",),
    }
    assert all(error["type"] == "missing" for error in errors)


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
    _items(changed, "fills")[0]["fee"] = Decimal("-2")
    actual = _outcome(changed)
    difference = expected.first_difference(actual)
    assert expected.sha256() != actual.sha256() and difference is not None
    assert (
        difference.path,
        difference.kind,
        difference.expected_json,
        difference.actual_json,
    ) == (
        "$.fills[0].fee",
        "value",
        '["decimal",0,"1",0]',
        '["decimal",1,"2",0]',
    )


class _ForgedTupleDecimal(Decimal):
    def as_tuple(self) -> DecimalTuple:
        return Decimal("999.00").as_tuple()

    def is_finite(self) -> bool:
        return False

    def is_zero(self) -> bool:
        return True


def test_outcome_projects_decimal_subclass_to_base_value() -> None:
    payload = _payload()
    cast(dict[str, object], payload["financial"])["equity"] = _ForgedTupleDecimal(
        "1.25"
    )
    outcome = _outcome(payload)

    assert outcome.financial.equity == Decimal("1.25")
    assert outcome.financial.equity != Decimal("999.00")
    assert type(outcome.financial.equity) is Decimal


def test_extreme_exponents_are_exact_compact_and_digest_distinct() -> None:
    outcomes = []
    for value in (Decimal("1E+1000000"), Decimal("1E-1000000")):
        payload = _payload()
        cast(dict[str, object], payload["financial"])["equity"] = value
        outcomes.append(_outcome(payload))

    assert tuple(outcome.financial.equity.as_tuple() for outcome in outcomes) == (
        Decimal("1E+1000000").as_tuple(),
        Decimal("1E-1000000").as_tuple(),
    )
    assert all(len(outcome.canonical_bytes()) < 2048 for outcome in outcomes)
    assert outcomes[0].sha256() != outcomes[1].sha256()
    assert TradingOutcome.schema_version == "fluxtrade.trading_outcome.v2"


def test_long_decimal_identity_is_context_independent_and_collision_free() -> None:
    exact = Decimal(
        "123456789012345678901234567890123456789012345678901234567890.123456789"
    )
    adjacent = Decimal(
        "123456789012345678901234567890123456789012345678901234567890.123456788"
    )
    global_context = str(getcontext())
    identities: list[tuple[bytes, str]] = []

    for precision in (6, 28, 50):
        with localcontext() as context:
            context.prec = precision
            exact_payload = _payload()
            cast(dict[str, object], exact_payload["financial"])["equity"] = exact
            adjacent_payload = _payload()
            cast(dict[str, object], adjacent_payload["financial"])["equity"] = adjacent
            outcome = _outcome(exact_payload)
            changed = _outcome(adjacent_payload)

            assert outcome.financial.equity == exact
            assert changed.financial.equity == adjacent
            assert outcome.financial.equity != changed.financial.equity
            assert outcome.sha256() != changed.sha256()
            difference = outcome.first_difference(changed)
            assert difference is not None and difference.path == "$.financial.equity"
            identities.append((outcome.canonical_bytes(), outcome.sha256()))

    assert len(set(identities)) == 1
    assert str(getcontext()) == global_context


def test_dynamic_int_and_decimal_are_collision_free() -> None:
    integer, decimal = _payload(), _payload()
    _items(integer, "journal")[0]["data_json"] = 1
    _items(decimal, "journal")[0]["data_json"] = Decimal("1")
    assert _outcome(integer).sha256() != _outcome(decimal).sha256()
    assert _outcome(integer).first_difference(_outcome(decimal)) is not None


def test_non_empty_endpoint_semantics_change_digest() -> None:
    position = EndpointPosition(
        strategy_id="s",
        product_id="MNQ",
        side=PositionSide.LONG,
        quantity=Decimal("1"),
        average_entry_price=Decimal("100"),
    )
    order = EndpointOrder(
        strategy_id="s",
        product_id="MNQ",
        side=OrderSide.SELL,
        order_type="LIMIT",
        quantity=Decimal("1"),
        timestamp=3,
        price=Decimal("101"),
    )
    payload = _payload()
    payload["endpoint_state"] = ReplayEndpointState(
        positions=(position,),
        working_orders=(order,),
        final_mark=Decimal("100"),
        end_timestamp=3,
    )
    expected = _outcome(payload)

    for field, changed, path in (
        (
            "positions",
            position.model_copy(update={"quantity": Decimal("2")}),
            "$.endpoint_state.positions[0].quantity",
        ),
        (
            "working_orders",
            order.model_copy(update={"price": Decimal("102")}),
            "$.endpoint_state.working_orders[0].price",
        ),
    ):
        changed_payload = deepcopy(payload)
        endpoint_state = changed_payload["endpoint_state"]
        assert isinstance(endpoint_state, ReplayEndpointState)
        changed_payload["endpoint_state"] = endpoint_state.model_copy(
            update={field: (changed,)}
        )
        actual = _outcome(changed_payload)
        difference = expected.first_difference(actual)
        assert expected.sha256() != actual.sha256()
        assert difference is not None and difference.path == path


@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
def test_trading_outcome_revalidates_endpoint_instance_content(method: str) -> None:
    valid = ReplayEndpointState(
        final_mark=Decimal("100"), end_timestamp=3, halted_early=False
    )
    if method == "model_copy":
        corrupted = valid.model_copy(update={"final_mark": 100.0})
    else:
        corrupted = ReplayEndpointState.model_construct(
            positions=(),
            working_orders=(),
            final_mark=100.0,
            end_timestamp=3,
            halted_early=False,
        )
    assert isinstance(corrupted.final_mark, float)
    payload = _payload()
    payload["endpoint_state"] = corrupted

    with pytest.raises(ValidationError):
        _outcome(payload)


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
