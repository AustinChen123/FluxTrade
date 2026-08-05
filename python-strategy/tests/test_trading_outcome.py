from copy import deepcopy
from collections.abc import Callable, Iterator
from decimal import Decimal, DecimalTuple, getcontext, localcontext
from types import SimpleNamespace
from typing import Literal, cast

import pytest
from pydantic import BaseModel, ValidationError

from src.core.backtest.endpoint_state import (
    EndpointOrder,
    EndpointPosition,
    ReplayEndpointState,
)
from src.core.models import OrderSide, PositionSide
from src.validation.trading_outcome import (
    FillObservation,
    FinancialOutcome,
    JournalObservation,
    OrderObservation,
    SignalObservation,
    TradingOutcome,
)


_OBSERVATION_FIELDS = {
    "signals": (
        SignalObservation,
        (
            "strategy_id",
            "product_id",
            "timeframe",
            "timestamp_ms",
            "signal_type",
            "value",
            "quantity",
            "price",
            "stop_loss",
            "take_profit",
            "trailing_distance",
            "metadata_json",
        ),
    ),
    "order_observations": (
        OrderObservation,
        (
            "logical_order_id",
            "parent_logical_order_id",
            "linked_logical_order_id",
            "strategy_id",
            "product_id",
            "timestamp_ms",
            "phase",
            "status",
            "order_type",
            "side",
            "quantity",
            "filled_quantity",
            "price",
            "trigger_price",
            "trailing_distance",
        ),
    ),
    "fills": (
        FillObservation,
        (
            "logical_order_id",
            "strategy_id",
            "product_id",
            "timestamp_ms",
            "fill_type",
            "side",
            "price",
            "quantity",
            "fee",
        ),
    ),
    "financial": (
        FinancialOutcome,
        ("fees", "realized_pnl", "unrealized_pnl", "equity"),
    ),
    "journal": (
        JournalObservation,
        ("strategy_id", "timestamp_ms", "tag", "logical_trade_id", "data_json"),
    ),
}
_PLAIN_STRING_FIELDS = {
    "signals": ("strategy_id", "product_id", "timeframe", "signal_type"),
    "order_observations": (
        "strategy_id",
        "product_id",
        "phase",
        "status",
        "order_type",
        "side",
    ),
    "fills": ("strategy_id", "product_id", "fill_type", "side"),
    "journal": ("strategy_id", "tag"),
}
_IDENTITY_FIELDS = {
    "order_observations": (
        "logical_order_id",
        "parent_logical_order_id",
        "linked_logical_order_id",
    ),
    "fills": ("logical_order_id",),
    "journal": ("logical_trade_id",),
}
_TIMESTAMP_FIELDS = {
    section: ("timestamp_ms",)
    for section in ("signals", "order_observations", "fills", "journal")
}
_MONEY_FIELDS = {
    "signals": (
        "value",
        "quantity",
        "price",
        "stop_loss",
        "take_profit",
        "trailing_distance",
    ),
    "order_observations": (
        "quantity",
        "filled_quantity",
        "price",
        "trigger_price",
        "trailing_distance",
    ),
    "fills": ("price", "quantity", "fee"),
    "financial": ("fees", "realized_pnl", "unrealized_pnl", "equity"),
}
_JSON_FIELDS = {"signals": ("metadata_json",), "journal": ("data_json",)}
_OBSERVATION_MODEL_CASES = [
    pytest.param("signals", SignalObservation, id="SignalObservation"),
    pytest.param("order_observations", OrderObservation, id="OrderObservation"),
    pytest.param("fills", FillObservation, id="FillObservation"),
    pytest.param("financial", FinancialOutcome, id="FinancialOutcome"),
    pytest.param("journal", JournalObservation, id="JournalObservation"),
]


def _cases(fields: dict[str, tuple[str, ...]]) -> list[tuple[str, str]]:
    return [(section, field) for section, names in fields.items() for field in names]


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


def _observation(outcome: TradingOutcome, section: str) -> BaseModel:
    value = getattr(outcome, section)
    return value if section == "financial" else value[0]


def _payload_with_instance(section: str, instance: BaseModel) -> dict[str, object]:
    payload = _payload()
    payload[section] = instance if section == "financial" else (instance,)
    return payload


def _corrupt(model: BaseModel, method: str, field: str, value: object) -> BaseModel:
    if method == "model_copy":
        return model.model_copy(update={field: value})
    values = model.model_dump()
    values[field] = value
    return type(model).model_construct(**values)


def _without(model: BaseModel, field: str) -> BaseModel:
    values = model.model_dump()
    del values[field]
    return type(model).model_construct(**values)


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


def test_final_mark_decimal_changes_digest_at_exact_path() -> None:
    expected = _outcome()
    payload = _payload()
    payload["endpoint_state"] = expected.endpoint_state.model_copy(
        update={"final_mark": Decimal("101")}
    )
    actual = _outcome(payload)
    assert expected.endpoint_state.final_mark == Decimal("100")
    assert actual.endpoint_state.final_mark == Decimal("101")
    assert expected.sha256() != actual.sha256()
    difference = expected.first_difference(actual)
    assert difference is not None
    assert difference.path == "$.endpoint_state.final_mark"
    assert difference.kind == "value"
    assert difference.expected_json == '["decimal",0,"1",2]'
    assert difference.actual_json == '["decimal",0,"101",0]'


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


def _endpoint_outcome(
    owner: str | None = None,
    field: str | None = None,
    changed: object = None,
    project: bool = True,
) -> TradingOutcome:
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
        trigger_price=Decimal("99"),
        trailing_distance=Decimal("1"),
    )
    state = ReplayEndpointState(
        positions=(position,),
        working_orders=(order,),
        final_mark=Decimal("100"),
        end_timestamp=3,
    )
    if owner == "position" and project:
        assert field is not None
        state = state.model_copy(
            update={"positions": (position.model_copy(update={field: changed}),)}
        )
    elif owner == "order" and project:
        assert field is not None
        state = state.model_copy(
            update={"working_orders": (order.model_copy(update={field: changed}),)}
        )
    elif owner == "state" and project:
        assert field is not None
        if field == "positions":
            changed = (
                *state.positions,
                position.model_copy(update={"strategy_id": "s2"}),
            )
        elif field == "working_orders":
            changed = (
                *state.working_orders,
                order.model_copy(update={"strategy_id": "s2"}),
            )
        state = state.model_copy(update={field: changed})
    payload = _payload()
    payload["endpoint_state"] = state
    return _outcome(payload)


_ENDPOINT_FIELD_LEDGER = {
    "position": dict(
        strategy_id="s2",
        product_id="NQ",
        side=PositionSide.SHORT,
        quantity=Decimal("2"),
        average_entry_price=Decimal("101"),
    ),
    "order": dict(
        strategy_id="s2",
        product_id="NQ",
        side=OrderSide.BUY,
        order_type="MARKET",
        quantity=Decimal("2"),
        timestamp=4,
        price=Decimal("102"),
        trigger_price=Decimal("98"),
        trailing_distance=Decimal("2"),
    ),
    "state": dict(
        positions=None,
        working_orders=None,
        final_mark=Decimal("101"),
        end_timestamp=4,
        halted_early=True,
    ),
}


def test_endpoint_field_ledger_matches_imported_models() -> None:
    assert set(_ENDPOINT_FIELD_LEDGER["position"]) == set(EndpointPosition.model_fields)
    assert set(_ENDPOINT_FIELD_LEDGER["order"]) == set(EndpointOrder.model_fields)
    assert set(_ENDPOINT_FIELD_LEDGER["state"]) == set(ReplayEndpointState.model_fields)


@pytest.mark.parametrize(
    "owner,field,changed",
    [
        (owner, field, changed)
        for owner, fields in _ENDPOINT_FIELD_LEDGER.items()
        for field, changed in fields.items()
    ],
)
def test_every_endpoint_field_changes_digest_at_exact_path(
    owner: str, field: str, changed: object
) -> None:
    expected, actual = _endpoint_outcome(), _endpoint_outcome(owner, field, changed)
    prefix = {"position": "positions[0]", "order": "working_orders[0]"}.get(owner)
    path = (
        f"$.endpoint_state.{field}"
        if prefix is None
        else f"$.endpoint_state.{prefix}.{field}"
    )
    difference = expected.first_difference(actual)
    omitted = _endpoint_outcome(owner, field, changed, project=False)
    assert omitted.sha256() == expected.sha256()
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


def test_observation_field_ledger_exactly_matches_imported_models() -> None:
    assert sum(len(fields) for _, fields in _OBSERVATION_FIELDS.values()) == 45
    for model, fields in _OBSERVATION_FIELDS.values():
        assert set(fields) == set(model.model_fields)


@pytest.mark.parametrize(
    "section,field",
    [
        (section, field)
        for section, (_, fields) in _OBSERVATION_FIELDS.items()
        for field in fields
    ],
)
def test_every_valid_observation_field_changes_digest_at_exact_path(
    section: str, field: str
) -> None:
    payload = _payload()
    target = (
        cast(dict[str, object], payload[section])
        if section == "financial"
        else _items(payload, section)[0]
    )
    value = target[field]
    if (section, field) in _cases(_JSON_FIELDS):
        target[field] = {"changed": True}
    elif (section, field) in _cases(_MONEY_FIELDS):
        target[field] = Decimal("7") if value is None else cast(Decimal, value) + 1
    elif (section, field) in _cases(_TIMESTAMP_FIELDS):
        target[field] = cast(int, value) + 1
    else:
        target[field] = "changed" if value is None else f"{value}-changed"
    expected, actual = _outcome(), _outcome(payload)
    difference = expected.first_difference(actual)
    prefix = f"$.{section}" if section == "financial" else f"$.{section}[0]"
    assert expected.sha256() != actual.sha256()
    assert difference is not None and difference.path == f"{prefix}.{field}"


@pytest.mark.parametrize(
    "section,field",
    [
        (section, field)
        for section, (_, fields) in _OBSERVATION_FIELDS.items()
        for field in fields
    ],
)
def test_missing_observation_field_fails_at_outcome_construction(
    section: str, field: str
) -> None:
    model = _observation(_outcome(), section)
    with pytest.raises(ValidationError):
        _outcome(_payload_with_instance(section, _without(model, field)))


@pytest.mark.parametrize("section,field", _cases(_PLAIN_STRING_FIELDS))
@pytest.mark.parametrize("invalid", [1, True, b"s", object(), "\ud800"])
@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
def test_corrupted_plain_strings_fail_at_outcome_construction(
    section: str, field: str, invalid: object, method: str
) -> None:
    model = _observation(_outcome(), section)
    corrupted = _corrupt(model, method, field, invalid)
    with pytest.raises(ValidationError):
        _outcome(_payload_with_instance(section, corrupted))


@pytest.mark.parametrize("section,field", _cases(_IDENTITY_FIELDS))
@pytest.mark.parametrize("invalid", [" ", 1, True, b"id", "\ud800"])
@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
def test_corrupted_identities_fail_at_outcome_construction(
    section: str, field: str, invalid: object, method: str
) -> None:
    model = _observation(_outcome(), section)
    corrupted = _corrupt(model, method, field, invalid)
    with pytest.raises(ValidationError):
        _outcome(_payload_with_instance(section, corrupted))


@pytest.mark.parametrize("section,field", _cases(_TIMESTAMP_FIELDS))
@pytest.mark.parametrize("invalid", [True, 1.0, "1"])
@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
def test_corrupted_timestamps_fail_at_outcome_construction(
    section: str, field: str, invalid: object, method: str
) -> None:
    model = _observation(_outcome(), section)
    corrupted = _corrupt(model, method, field, invalid)
    with pytest.raises(ValidationError):
        _outcome(_payload_with_instance(section, corrupted))


@pytest.mark.parametrize("section,field", _cases(_MONEY_FIELDS))
@pytest.mark.parametrize(
    "invalid",
    [1.0, True, "1", Decimal("NaN"), Decimal("Infinity"), Decimal("-Infinity")],
)
@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
def test_corrupted_money_fails_at_outcome_construction(
    section: str, field: str, invalid: object, method: str
) -> None:
    model = _observation(_outcome(), section)
    corrupted = _corrupt(model, method, field, invalid)
    with pytest.raises(ValidationError):
        _outcome(_payload_with_instance(section, corrupted))


@pytest.mark.parametrize("section,field", _cases(_JSON_FIELDS))
@pytest.mark.parametrize(
    "invalid",
    [
        {"raw": True},
        1.0,
        '["map",broken]',
        '["decimal",0,"10",-1]',
        '["map",[["b",["null"]],["a",["null"]]]]',
    ],
)
@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
def test_corrupted_canonical_json_fails_at_outcome_construction(
    section: str, field: str, invalid: object, method: str
) -> None:
    model = _observation(_outcome(), section)
    corrupted = _corrupt(model, method, field, invalid)
    with pytest.raises(ValidationError):
        _outcome(_payload_with_instance(section, corrupted))


_UNREPRESENTABLE_CANONICAL_JSON = [
    '["decimal",0,"1",' + "9" * 4000 + "]",
    '["decimal",0,"1",-' + "9" * 4000 + "]",
    '["list",[' * 1100 + '["null"]' + "]]" * 1100,
]


@pytest.mark.parametrize(
    "invalid",
    _UNREPRESENTABLE_CANONICAL_JSON,
    ids=["positive_exponent", "negative_exponent", "deep_nesting"],
)
@pytest.mark.parametrize(
    "section,field", [("signals", "metadata_json"), ("journal", "data_json")]
)
@pytest.mark.parametrize("method", ["model_copy", "model_construct"])
def test_unrepresentable_canonical_json_fails_at_outcome_construction(
    invalid: str, section: str, field: str, method: str
) -> None:
    model = _observation(_outcome(), section)
    corrupted = _corrupt(model, method, field, invalid)
    with pytest.raises(ValidationError):
        _outcome(_payload_with_instance(section, corrupted))


class _AdversarialTuple(tuple[object, ...]):
    exposed: tuple[object, ...]

    def __new__(
        cls, stored: tuple[object, ...], exposed: tuple[object, ...]
    ) -> "_AdversarialTuple":
        instance = super().__new__(cls, stored)
        instance.exposed = exposed
        return instance

    def __iter__(self) -> Iterator[object]:
        return iter(self.exposed)


class _FalseyModelExtra(dict[str, object]):
    def __bool__(self) -> bool:
        return False


def _endpoint_child(
    model: type[EndpointPosition] | type[EndpointOrder],
) -> EndpointPosition | EndpointOrder:
    if model is EndpointPosition:
        return EndpointPosition(
            strategy_id="s",
            product_id="MNQ",
            side=PositionSide.LONG,
            quantity=Decimal("1"),
            average_entry_price=Decimal("100"),
        )
    return EndpointOrder(
        strategy_id="s",
        product_id="MNQ",
        side=OrderSide.BUY,
        order_type="MARKET",
        quantity=Decimal("1"),
        timestamp=1,
    )


class _ForgedInt(int):
    def __format__(self, format_spec: str) -> str:
        return "forged"


class _ForgedList(list[object]):
    def __iter__(self) -> Iterator[object]:
        return iter(("exposed",))


class _ForgedTuple(tuple[object, ...]):
    def __iter__(self) -> Iterator[object]:
        return iter(("exposed",))


class _ForgedMapping(dict[str, object]):
    def __iter__(self) -> Iterator[str]:
        return iter(("exposed",))

    def __getitem__(self, key: str) -> object:
        return "value"


def _forged_int() -> object:
    return _ForgedInt(1)


def _forged_list() -> object:
    return _ForgedList(["stored"])


def _forged_tuple() -> object:
    return _ForgedTuple(("stored",))


def _forged_mapping() -> object:
    return _ForgedMapping(stored="value")


def _surrogate_raw() -> object:
    return "\ud800"


def _surrogate_key() -> object:
    return {"\ud800": "value"}


def _surrogate_value() -> object:
    return {"key": "\ud800"}


def _cyclic_list() -> object:
    value: list[object] = []
    value.append(value)
    return value


def _cyclic_mapping() -> object:
    value: dict[str, object] = {}
    value["cycle"] = value
    return value


def _deep_list() -> object:
    value: object = None
    for _ in range(1500):
        value = [value]
    return value


_DYNAMIC_JSON_FACTORIES = [
    pytest.param(_forged_int, id="forged_int"),
    pytest.param(_forged_list, id="forged_list"),
    pytest.param(_forged_tuple, id="forged_tuple"),
    pytest.param(_forged_mapping, id="forged_mapping"),
    pytest.param(_surrogate_raw, id="surrogate_raw"),
    pytest.param(_surrogate_key, id="surrogate_key"),
    pytest.param(_surrogate_value, id="surrogate_value"),
    pytest.param(_cyclic_list, id="cyclic_list"),
    pytest.param(_cyclic_mapping, id="cyclic_mapping"),
    pytest.param(_deep_list, id="deep_list_1500"),
]


@pytest.mark.parametrize(
    "section", ["signals", "order_observations", "fills", "journal"]
)
def test_sequence_container_and_child_subclasses_fail_closed(section: str) -> None:
    valid = _outcome()
    child = getattr(valid, section)[0]
    subclass_type = type(f"{type(child).__name__}Subclass", (type(child),), {})
    subclass = subclass_type.model_construct(**child.model_dump())
    payload = _payload()
    payload[section] = (subclass,)
    with pytest.raises(ValidationError):
        _outcome(payload)

    payload[section] = _AdversarialTuple((child,), (child,))
    with pytest.raises(ValidationError):
        _outcome(payload)

    values = child.model_dump()
    del values[next(iter(type(child).model_fields))]
    corrupted = type(child).model_construct(**values)
    payload[section] = _AdversarialTuple((corrupted,), (child,))
    with pytest.raises(ValidationError):
        _outcome(payload)


@pytest.mark.parametrize(
    "section", ["signals", "order_observations", "fills", "journal"]
)
def test_sequence_lists_fail_with_tuple_type(section: str) -> None:
    payload = _payload()
    payload[section] = list(_items(payload, section))
    with pytest.raises(ValidationError) as exc_info:
        _outcome(payload)
    assert exc_info.value.errors()[0]["type"] == "tuple_type"


@pytest.mark.parametrize("variant", ["exact", "model_construct"])
@pytest.mark.parametrize(
    "section,field,model_class",
    [
        pytest.param(
            "signals", "metadata_json", SignalObservation, id="SignalObservation"
        ),
        pytest.param(
            "journal", "data_json", JournalObservation, id="JournalObservation"
        ),
    ],
)
def test_direct_exact_canonical_json_revalidation_preserves_identity(
    section: str,
    field: str,
    model_class: type[BaseModel],
    variant: str,
) -> None:
    expected = _outcome()
    original = _observation(expected, section)
    candidate = (
        original
        if variant == "exact"
        else model_class.model_construct(**original.__dict__)
    )
    direct = model_class.model_validate(candidate)
    tagged = getattr(original, field)
    assert getattr(direct, field) == tagged

    actual = _outcome(_payload_with_instance(section, direct))
    assert actual.canonical_bytes() == expected.canonical_bytes()
    assert actual.sha256() == expected.sha256()

    raw_values = original.model_dump()
    raw_values[field] = tagged
    raw = model_class.model_validate(raw_values)
    assert getattr(raw, field) != tagged
    assert getattr(raw, field).startswith('["string",')


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
        (section, field)
        for section, (_, fields) in _OBSERVATION_FIELDS.items()
        for field in fields
    ],
)
def test_every_raw_observation_field_is_required_at_exact_path(
    section: str, field: str
) -> None:
    missing = _payload()
    target = (
        cast(dict[str, object], missing[section])
        if section == "financial"
        else _items(missing, section)[0]
    )
    del target[field]
    expected_location = (
        (section, field) if section == "financial" else (section, 0, field)
    )
    with pytest.raises(ValidationError) as exc_info:
        _outcome(missing)
    assert [(error["type"], error["loc"]) for error in exc_info.value.errors()] == [
        ("missing", expected_location)
    ]


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


@pytest.mark.parametrize(
    "section,field", _cases(_PLAIN_STRING_FIELDS) + _cases(_IDENTITY_FIELDS)
)
def test_strict_false_rejects_observation_string_coercion(
    section: str, field: str
) -> None:
    payload = _payload()
    _items(payload, section)[0][field] = b"coercible"
    with pytest.raises(ValidationError):
        TradingOutcome.model_validate(payload, strict=False)


@pytest.mark.parametrize("invalid", ["1", True], ids=["string", "bool"])
@pytest.mark.parametrize("section,field", _cases(_TIMESTAMP_FIELDS))
def test_strict_false_rejects_observation_timestamp_coercion(
    section: str, field: str, invalid: object
) -> None:
    payload = _payload()
    _items(payload, section)[0][field] = invalid
    with pytest.raises(ValidationError):
        TradingOutcome.model_validate(payload, strict=False)


@pytest.mark.parametrize(
    "section", ["signals", "order_observations", "fills", "journal"]
)
def test_strict_false_rejects_sequence_list_coercion(section: str) -> None:
    payload = _payload()
    payload[section] = list(_items(payload, section))
    with pytest.raises(ValidationError):
        TradingOutcome.model_validate(payload, strict=False)


@pytest.mark.parametrize("extra", ["allow", "ignore"])
@pytest.mark.parametrize("section,model_class", _OBSERVATION_MODEL_CASES)
def test_observation_call_time_extra_fails_closed(
    section: str, model_class: type[BaseModel], extra: Literal["allow", "ignore"]
) -> None:
    values = _observation(_outcome(), section).model_dump()
    values["unexpected"] = "accepted"
    with pytest.raises(ValidationError):
        model_class.model_validate(values, extra=extra)


@pytest.mark.parametrize("extra", ["allow", "ignore"])
def test_outcome_call_time_extra_fails_closed(
    extra: Literal["allow", "ignore"],
) -> None:
    payload = _payload()
    payload["unexpected"] = "accepted"
    with pytest.raises(ValidationError):
        TradingOutcome.model_validate(payload, extra=extra)


@pytest.mark.parametrize("section,model_class", _OBSERVATION_MODEL_CASES)
def test_observation_from_attributes_fails_closed(
    section: str, model_class: type[BaseModel]
) -> None:
    attributes = SimpleNamespace(**_observation(_outcome(), section).model_dump())
    with pytest.raises(ValidationError):
        model_class.model_validate(attributes, from_attributes=True)


def test_outcome_from_attributes_fails_closed() -> None:
    with pytest.raises(ValidationError):
        TradingOutcome.model_validate(
            SimpleNamespace(**_payload()), from_attributes=True
        )


@pytest.mark.parametrize("section,model_class", _OBSERVATION_MODEL_CASES)
def test_exact_observation_model_extra_fails_at_outcome_construction(
    section: str, model_class: type[BaseModel]
) -> None:
    exact = _observation(_outcome(), section)
    object.__setattr__(exact, "__pydantic_extra__", {"unexpected": "accepted"})
    assert exact.model_extra == {"unexpected": "accepted"}
    assert "unexpected" not in exact.__dict__
    with pytest.raises(ValidationError):
        _outcome(_payload_with_instance(section, exact))


def test_direct_exact_observation_rejects_falsey_model_extra() -> None:
    exact = _outcome().signals[0]
    hidden = _FalseyModelExtra(unexpected="accepted")
    object.__setattr__(exact, "__pydantic_extra__", hidden)
    assert exact.model_extra == {"unexpected": "accepted"}
    assert not exact.model_extra
    with pytest.raises(ValidationError):
        SignalObservation.model_validate(exact)


@pytest.mark.parametrize("section", ["signals", "financial", "endpoint_state"])
def test_outcome_rejects_falsey_model_extra_on_exact_child(section: str) -> None:
    valid = _outcome()
    child = {
        "signals": valid.signals[0],
        "financial": valid.financial,
        "endpoint_state": valid.endpoint_state,
    }[section]
    hidden = _FalseyModelExtra(unexpected="accepted")
    object.__setattr__(child, "__pydantic_extra__", hidden)
    assert child.model_extra == {"unexpected": "accepted"}
    assert not child.model_extra
    payload = _payload()
    payload[section] = (child,) if section == "signals" else child
    with pytest.raises(ValidationError):
        _outcome(payload)


@pytest.mark.parametrize(
    ("model", "field"),
    [
        (EndpointPosition, "positions"),
        (EndpointOrder, "working_orders"),
    ],
)
@pytest.mark.parametrize(
    "extra",
    [
        pytest.param(None, id="none"),
        pytest.param({}, id="empty"),
        pytest.param(_FalseyModelExtra(unexpected="accepted"), id="falsey_nonempty"),
        pytest.param({"unexpected": "accepted"}, id="truthy_nonempty"),
    ],
)
def test_endpoint_child_model_extra_matrix_at_outcome_construction(
    model: type[EndpointPosition] | type[EndpointOrder],
    field: str,
    extra: dict[str, object] | None,
) -> None:
    child = _endpoint_child(model)
    endpoint_state = ReplayEndpointState.model_validate({field: (child,)})
    payload = _payload()
    payload["endpoint_state"] = endpoint_state
    expected = _outcome(payload)
    object.__setattr__(getattr(endpoint_state, field)[0], "__pydantic_extra__", extra)
    if extra is None:
        actual = _outcome(payload)
        assert actual.canonical_bytes() == expected.canonical_bytes()
        assert actual.sha256() == expected.sha256()
    else:
        with pytest.raises(ValidationError):
            _outcome(payload)


def test_model_validate_json_strict_false_rejects_timestamp_string() -> None:
    payload = (
        '{"strategy_id":"s","product_id":"MNQ","timeframe":"5m",'
        '"timestamp_ms":"1","signal_type":"LONG","value":null,"quantity":null,'
        '"price":null,"stop_loss":null,"take_profit":null,'
        '"trailing_distance":null,"metadata_json":{}}'
    )
    with pytest.raises(ValidationError):
        SignalObservation.model_validate_json(payload, strict=False)


@pytest.mark.parametrize("factory", _DYNAMIC_JSON_FACTORIES)
@pytest.mark.parametrize(
    "section,field", [("signals", "metadata_json"), ("journal", "data_json")]
)
def test_dynamic_json_adversaries_fail_at_outcome_construction(
    section: str, field: str, factory: Callable[[], object]
) -> None:
    payload = _payload()
    _items(payload, section)[0][field] = factory()
    with pytest.raises(ValidationError):
        _outcome(payload)


_MULTIBYTE = "strategy-策略-交易-ß-🙂"
_MULTIBYTE_FIELDS = _cases(_PLAIN_STRING_FIELDS) + _cases(_IDENTITY_FIELDS)


@pytest.mark.parametrize("section,field", _MULTIBYTE_FIELDS)
@pytest.mark.parametrize(
    "projection", ["raw", "exact", "model_copy", "model_construct"]
)
def test_observation_strings_preserve_valid_multibyte_utf8(
    section: str, field: str, projection: str
) -> None:
    payload = _payload()
    if projection == "raw":
        _items(payload, section)[0][field] = _MULTIBYTE
    else:
        if projection == "exact":
            _items(payload, section)[0][field] = _MULTIBYTE
            projected = _observation(_outcome(payload), section)
        else:
            original = _observation(_outcome(), section)
            projected = _corrupt(original, projection, field, _MULTIBYTE)
        payload = _payload_with_instance(section, projected)
    assert getattr(_observation(_outcome(payload), section), field) == _MULTIBYTE


@pytest.mark.parametrize(
    "value,expected",
    [
        (_MULTIBYTE, f'["string","{_MULTIBYTE}"]'),
        ({_MULTIBYTE: None}, f'["map",[["{_MULTIBYTE}",["null"]]]]'),
        ({"key": _MULTIBYTE}, f'["map",[["key",["string","{_MULTIBYTE}"]]]]'),
    ],
)
@pytest.mark.parametrize("section,field", _cases(_JSON_FIELDS))
@pytest.mark.parametrize(
    "projection", ["raw", "exact", "model_copy", "model_construct"]
)
def test_dynamic_json_preserves_valid_multibyte_utf8_bytes(
    section: str, field: str, value: object, expected: str, projection: str
) -> None:
    payload = _payload()
    if projection in {"raw", "exact"}:
        _items(payload, section)[0][field] = value
        if projection == "exact":
            payload = _payload_with_instance(
                section, _observation(_outcome(payload), section)
            )
    else:
        original = _observation(_outcome(), section)
        payload = _payload_with_instance(
            section, _corrupt(original, projection, field, expected)
        )
    outcome = _outcome(payload)
    assert getattr(_observation(outcome, section), field).encode() == expected.encode()
    assert _MULTIBYTE.encode() in outcome.canonical_bytes()
