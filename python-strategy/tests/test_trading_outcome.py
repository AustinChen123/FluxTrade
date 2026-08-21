from copy import deepcopy
from collections.abc import Callable, Iterator
from decimal import Decimal, DecimalTuple, getcontext, localcontext
from enum import Enum, StrEnum
from types import SimpleNamespace
from typing import Literal, cast

import pytest
from pydantic import BaseModel, ValidationError

from src.core.backtest.endpoint_state import (
    EndpointOrder,
    EndpointPosition,
    ReplayEndpointState,
)
from src.core.models import OrderSide, PositionSide, SignalType
from src.validation.trading_outcome import (
    FillObservation,
    FinancialOutcome,
    JournalObservation,
    OrderObservation,
    SignalObservation,
    TradingOutcome,
)


class _CustomStringEnum(str, Enum):
    VALUE = "custom-enum"


class _CustomStrEnum(StrEnum):
    VALUE = "custom-str-enum"


_HOSTILE_STRING_CALLS: list[str] = []


class _HostileStringMetaBase(type):
    def __eq__(cls, other: object) -> bool:
        _HOSTILE_STRING_CALLS.append("metaclass_eq")
        return type.__eq__(cls, other)

    def __hash__(cls) -> int:
        _HOSTILE_STRING_CALLS.append("metaclass_hash")
        return type.__hash__(cls)

    def __getattribute__(cls, name: str) -> object:
        _HOSTILE_STRING_CALLS.append("metaclass_getattribute")
        return type.__getattribute__(cls, name)


def _hostile_type_name(cls: type) -> str:
    _HOSTILE_STRING_CALLS.append("metaclass_name")
    return "hostile-name"


_HostileStringMeta = type(
    "_HostileStringMeta",
    (_HostileStringMetaBase,),
    {"__name__": property(_hostile_type_name)},
)


class _HostileMetaclassString(str, metaclass=_HostileStringMeta):
    @property
    def value(self) -> str:
        _HOSTILE_STRING_CALLS.append("instance_value")
        return str(self)


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
    "section,field",
    [
        pytest.param("signals", "metadata_json", id="SignalObservation"),
        pytest.param("journal", "data_json", id="JournalObservation"),
    ],
)
@pytest.mark.parametrize(
    "position",
    ["root", "list_item", "tuple_item", "nested_mapping_value"],
)
def test_dynamic_string_subclass_fails_before_hostile_operations(
    section: str, field: str, position: str
) -> None:
    hostile = _HostileIdentity("valid-looking")
    values = {
        "root": hostile,
        "list_item": [hostile],
        "tuple_item": (hostile,),
        "nested_mapping_value": {"nested": hostile},
    }
    payload = _payload()
    _items(payload, section)[0][field] = values[position]

    with pytest.raises(ValidationError):
        _outcome(payload)
    assert hostile.calls == []


@pytest.mark.parametrize(
    "value,expected",
    [
        ("", b'["string",""]'),
        (" padded ASCII ", b'["string"," padded ASCII "]'),
        (" 兩端留白 ", '["string"," 兩端留白 "]'.encode()),
        ("Grüße世界", '["string","Grüße世界"]'.encode()),
    ],
)
@pytest.mark.parametrize(
    "section,field",
    [
        pytest.param("signals", "metadata_json", id="SignalObservation"),
        pytest.param("journal", "data_json", id="JournalObservation"),
    ],
)
def test_dynamic_exact_base_strings_preserve_canonical_bytes(
    section: str, field: str, value: str, expected: bytes
) -> None:
    payload = _payload()
    _items(payload, section)[0][field] = value
    observation = _observation(_outcome(payload), section)
    assert getattr(observation, field).encode() == expected


@pytest.mark.parametrize(
    "value,expected",
    [
        (PositionSide.LONG, b'["string","LONG"]'),
        (PositionSide.SHORT, b'["string","SHORT"]'),
        (OrderSide.BUY, b'["string","buy"]'),
        (OrderSide.SELL, b'["string","sell"]'),
    ],
)
def test_declared_side_enums_preserve_exact_base_string_bytes(
    value: PositionSide | OrderSide, expected: bytes
) -> None:
    payload = _payload()
    _items(payload, "journal")[0]["data_json"] = value
    assert _outcome(payload).journal[0].data_json.encode() == expected


@pytest.mark.parametrize(
    "value", [_CustomStringEnum.VALUE, _CustomStrEnum.VALUE, SignalType.LONG]
)
@pytest.mark.parametrize(
    "section,field",
    [("signals", "metadata_json"), ("journal", "data_json")],
)
def test_undeclared_string_enums_fail_closed(
    section: str, field: str, value: str
) -> None:
    payload = _payload()
    _items(payload, section)[0][field] = value
    with pytest.raises(ValidationError):
        _outcome(payload)


@pytest.mark.parametrize(
    "section,field",
    [("signals", "metadata_json"), ("journal", "data_json")],
)
def test_hostile_metaclass_string_rejects_without_dispatch(
    section: str, field: str
) -> None:
    hostile = _HostileMetaclassString("valid-looking")
    payload = _payload()
    _items(payload, section)[0][field] = hostile
    _HOSTILE_STRING_CALLS.clear()

    with pytest.raises(ValidationError):
        _outcome(payload)
    assert _HOSTILE_STRING_CALLS == []


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


class _SemanticTradingOutcome(TradingOutcome):
    semantic_field: str


class _PassiveTradingOutcome(TradingOutcome):
    pass


class _HostileTradingOutcome(TradingOutcome):
    calls: list[str] = []

    def _projection(self) -> dict[str, object]:
        self.calls.append("projection")
        return _outcome()._projection()


class _HostileCanonicalBytesTradingOutcome(TradingOutcome):
    calls: list[str] = []

    def canonical_bytes(self) -> bytes:
        self.calls.append("canonical_bytes")
        return b"hostile"


_SUBCLASS_ERROR = "TradingOutcome subclasses are unsupported"
_EXTRA_ERROR = "TradingOutcome contains unexpected fields"
_OBSERVATION_ERROR = "TradingOutcome observations must have exact canonical types"
_SUMMARY_ERROR = "TradingOutcome summaries must have exact canonical types"
_MONEY_ERROR = "financial values must be finite Decimal instances"
_GOLDEN_SHA256 = "05faee5ac0518a1f060775e78993d701b272487391753bbb2c5f4ff95d311a09"
_GOLDEN_BYTES = b'["map",[["endpoint_state",["map",[["end_timestamp",["int","3"]],["final_mark",["decimal",0,"1",2]],["halted_early",["bool",false]],["positions",["tuple",[]]],["working_orders",["tuple",[]]]]]],["fills",["tuple",[["map",[["fee",["decimal",0,"1",0]],["fill_type",["string","entry"]],["logical_order_id",["string","order-1"]],["price",["decimal",0,"1",2]],["product_id",["string","MNQ"]],["quantity",["decimal",0,"2",0]],["side",["string","buy"]],["strategy_id",["string","s"]],["timestamp_ms",["int","3"]]]]]]],["financial",["map",[["equity",["decimal",0,"1002",0]],["fees",["decimal",0,"1",0]],["realized_pnl",["decimal",0,"0",0]],["unrealized_pnl",["decimal",0,"2",0]]]]],["journal",["tuple",[["map",[["data_json",["string","[\\"map\\",[[\\"price\\",[\\"decimal\\",0,\\"1\\",2]]]]"]],["logical_trade_id",["string","trade-1"]],["strategy_id",["string","s"]],["tag",["string","fill"]],["timestamp_ms",["int","4"]]]]]]],["order_observations",["tuple",[["map",[["filled_quantity",["decimal",0,"0",0]],["linked_logical_order_id",["null"]],["logical_order_id",["string","order-1"]],["order_type",["string","LIMIT"]],["parent_logical_order_id",["null"]],["phase",["string","submitted"]],["price",["decimal",0,"1",2]],["product_id",["string","MNQ"]],["quantity",["decimal",0,"2",0]],["side",["string","buy"]],["status",["string","NEW"]],["strategy_id",["string","s"]],["timestamp_ms",["int","2"]],["trailing_distance",["null"]],["trigger_price",["null"]]]]]]],["schema_version",["string","fluxtrade.trading_outcome.v2"]],["signals",["tuple",[["map",[["metadata_json",["string","[\\"map\\",[[\\"a\\",[\\"list\\",[[\\"bool\\",true],[\\"null\\"]]]],[\\"z\\",[\\"decimal\\",0,\\"0\\",0]]]]"]],["price",["null"]],["product_id",["string","MNQ"]],["quantity",["decimal",0,"2",0]],["signal_type",["string","LONG"]],["stop_loss",["null"]],["strategy_id",["string","s"]],["take_profit",["null"]],["timeframe",["string","5m"]],["timestamp_ms",["int","1"]],["trailing_distance",["null"]],["value",["decimal",0,"1",0]]]]]]]]]'
_SHAPE_CASES = [
    pytest.param("signals", "list", _OBSERVATION_ERROR, id="signals_list"),
    pytest.param("signals", "raw", _OBSERVATION_ERROR, id="signals_raw_child"),
    pytest.param("order_observations", "list", _OBSERVATION_ERROR, id="orders_list"),
    pytest.param("order_observations", "raw", _OBSERVATION_ERROR, id="orders_raw"),
    pytest.param("fills", "list", _OBSERVATION_ERROR, id="fills_list"),
    pytest.param("fills", "raw", _OBSERVATION_ERROR, id="fills_raw_child"),
    pytest.param("journal", "list", _OBSERVATION_ERROR, id="journal_list"),
    pytest.param("journal", "raw", _OBSERVATION_ERROR, id="journal_raw_child"),
    pytest.param("endpoint_state", "raw", _SUMMARY_ERROR, id="endpoint_raw_dict"),
    pytest.param("financial", "raw", _SUMMARY_ERROR, id="financial_raw_dict"),
]


def test_valid_fixture_matches_literal_canonical_identity() -> None:
    valid = _outcome()
    assert valid.canonical_bytes() == _GOLDEN_BYTES
    assert valid.sha256() == _GOLDEN_SHA256


def _direct_shape_corruption(field: str, variant: str) -> TradingOutcome:
    valid = _outcome()
    if variant == "list":
        changed = list(getattr(valid, field))
    elif field in {"endpoint_state", "financial"}:
        changed = getattr(valid, field).model_dump()
    else:
        changed = (_items(_payload(), field)[0],)
    return valid.model_copy(update={field: changed})


def _assert_direct_identity_error(
    outcome: TradingOutcome,
    method: Literal["canonical_bytes", "sha256"],
    message: str,
) -> None:
    with pytest.raises(ValueError) as exc_info:
        getattr(outcome, method)()
    assert (type(exc_info.value), str(exc_info.value)) == (ValueError, message)


@pytest.mark.parametrize("field,variant,message", _SHAPE_CASES)
@pytest.mark.parametrize("method", ["canonical_bytes", "sha256"])
def test_direct_identity_rejects_invalid_graph_shape(
    field: str,
    variant: str,
    message: str,
    method: Literal["canonical_bytes", "sha256"],
) -> None:
    _assert_direct_identity_error(
        _direct_shape_corruption(field, variant), method, message
    )


@pytest.mark.parametrize(
    "field", list(TradingOutcome.model_fields), ids=list(TradingOutcome.model_fields)
)
@pytest.mark.parametrize("method", ["canonical_bytes", "sha256"])
def test_direct_identity_rejects_missing_top_level_field(
    field: str, method: Literal["canonical_bytes", "sha256"]
) -> None:
    values = dict(_outcome().__dict__)
    del values[field]
    corrupt = TradingOutcome.model_construct(**values)
    message = (
        _SUMMARY_ERROR
        if field in {"endpoint_state", "financial"}
        else _OBSERVATION_ERROR
    )
    _assert_direct_identity_error(corrupt, method, message)


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param(_FalseyModelExtra(unexpected="accepted"), id="falsey"),
        pytest.param({"unexpected": "accepted"}, id="truthy"),
    ],
)
@pytest.mark.parametrize("method", ["canonical_bytes", "sha256"])
def test_direct_identity_rejects_model_extra(
    extra: dict[str, object], method: Literal["canonical_bytes", "sha256"]
) -> None:
    corrupt = _outcome()
    object.__setattr__(corrupt, "__pydantic_extra__", extra)
    assert corrupt.model_extra is extra
    _assert_direct_identity_error(corrupt, method, _EXTRA_ERROR)


_CONTENT_CASES = [
    pytest.param(
        "signals", "value", 1.0, ("signals", 0, "value"), _MONEY_ERROR, id="signals"
    ),
    pytest.param(
        "order_observations",
        "quantity",
        1.0,
        ("order_observations", 0, "quantity"),
        _MONEY_ERROR,
        id="orders",
    ),
    pytest.param(
        "fills", "price", 1.0, ("fills", 0, "price"), _MONEY_ERROR, id="fills"
    ),
    pytest.param(
        "journal",
        "strategy_id",
        None,
        ("journal", 0, "strategy_id"),
        None,
        id="journal_missing",
    ),
    pytest.param(
        "endpoint_state",
        "quantity",
        1.0,
        ("endpoint_state", "positions", 0, "quantity"),
        "endpoint position quantity must be a Decimal",
        id="endpoint",
    ),
    pytest.param(
        "financial",
        "equity",
        1002.0,
        ("financial", "equity"),
        _MONEY_ERROR,
        id="financial",
    ),
]


def _direct_content_corruption(
    owner: str, child_field: str, invalid: object, method: str
) -> TradingOutcome:
    valid = _endpoint_outcome() if owner == "endpoint_state" else _outcome()
    if owner == "journal":
        journal = valid.journal[0]
        values = {
            field: value
            for field, value in journal.__dict__.items()
            if field != child_field
        }
        if method == "model_copy":
            changed = journal.model_copy()
            object.__setattr__(changed, "__dict__", values)
        else:
            changed = JournalObservation.model_construct(**values)
        return valid.model_copy(update={"journal": (changed,)})
    if owner == "endpoint_state":
        position = valid.endpoint_state.positions[0]
        changed = _corrupt(position, method, child_field, invalid)
        endpoint = valid.endpoint_state.model_copy(update={"positions": (changed,)})
        return valid.model_copy(update={"endpoint_state": endpoint})
    child = getattr(valid, owner)
    if owner != "financial":
        child = child[0]
    changed = _corrupt(child, method, child_field, invalid)
    return valid.model_copy(
        update={owner: changed if owner == "financial" else (changed,)}
    )


@pytest.mark.parametrize(
    "owner,child_field,invalid,expected_loc,cause_message", _CONTENT_CASES
)
@pytest.mark.parametrize("child_method", ["model_copy", "model_construct"])
@pytest.mark.parametrize("identity_method", ["canonical_bytes", "sha256"])
def test_direct_identity_revalidates_exact_child_content(
    owner: str,
    child_field: str,
    invalid: object,
    expected_loc: tuple[str | int, ...],
    cause_message: str | None,
    child_method: str,
    identity_method: Literal["canonical_bytes", "sha256"],
) -> None:
    corrupt = _direct_content_corruption(owner, child_field, invalid, child_method)
    with pytest.raises(ValidationError) as exc_info:
        getattr(corrupt, identity_method)()
    errors = exc_info.value.errors(include_url=False)
    assert len(errors) == 1
    error = errors[0]
    assert error["loc"] == expected_loc
    if cause_message is None:
        assert (error["type"], error["msg"], error.get("ctx")) == (
            "missing",
            "Field required",
            None,
        )
    else:
        context = error.get("ctx")
        assert context is not None
        cause = context["error"]
        assert (error["type"], error["msg"]) == (
            "value_error",
            f"Value error, {cause_message}",
        )
        assert (type(cause), str(cause)) == (ValueError, cause_message)


class _HostileIdentity(str):
    calls: list[str]

    def __new__(cls, value: str) -> "_HostileIdentity":
        instance = super().__new__(cls, value)
        instance.calls = []
        return instance

    def strip(self, chars: str | None = None) -> str:
        self.calls.append("strip")
        return str.strip(self, chars)

    def encode(self, encoding: str = "utf-8", errors: str = "strict") -> bytes:
        self.calls.append("encode")
        return str.encode(self, encoding, errors)

    def __hash__(self) -> int:
        self.calls.append("hash")
        return str.__hash__(self)

    def __eq__(self, other: object) -> bool:
        self.calls.append("eq")
        return str.__eq__(self, other)

    def __repr__(self) -> str:
        self.calls.append("repr")
        return str.__repr__(self)

    def __format__(self, format_spec: str) -> str:
        self.calls.append("format")
        return str.__format__(self, format_spec)


@pytest.mark.parametrize("owner", ["signals", "order_observations", "fills", "journal"])
@pytest.mark.parametrize("child_method", ["model_copy", "model_construct"])
@pytest.mark.parametrize("identity_method", ["canonical_bytes", "sha256"])
def test_direct_revalidation_rejects_string_subclass_before_hostile_operations(
    owner: str,
    child_method: str,
    identity_method: Literal["canonical_bytes", "sha256"],
) -> None:
    valid = _outcome()
    hostile = _HostileIdentity("valid-looking")
    child = getattr(valid, owner)[0]
    changed = _corrupt(child, child_method, "strategy_id", hostile)
    corrupt = valid.model_copy(update={owner: (changed,)})
    with pytest.raises(ValidationError) as exc_info:
        getattr(corrupt, identity_method)()
    assert hostile.calls == []
    errors = exc_info.value.errors(include_input=False, include_url=False)
    assert hostile.calls == [] and len(errors) == 1
    error = errors[0]
    context = error.get("ctx")
    assert error["loc"] == (owner, 0) and context is not None
    cause = context["error"]
    assert (error["type"], error["msg"]) == (
        "value_error",
        "Value error, strategy_id must be a string",
    )
    assert (type(cause), str(cause)) == (
        ValueError,
        "strategy_id must be a string",
    )
    assert hostile.calls == []


@pytest.mark.parametrize("method", ["canonical_bytes", "sha256"])
def test_semantic_subclass_cannot_emit_identity(
    method: Literal["canonical_bytes", "sha256"],
) -> None:
    valid = _outcome()
    subclass = _SemanticTradingOutcome.model_construct(
        **valid.__dict__, semantic_field="semantic"
    )
    _assert_direct_identity_error(subclass, method, _SUBCLASS_ERROR)


@pytest.mark.parametrize("side", ["self", "actual"])
def test_semantic_subclass_cannot_compare_identity(side: str) -> None:
    valid = _outcome()
    subclass = _SemanticTradingOutcome.model_construct(
        **valid.__dict__, semantic_field="semantic"
    )
    with pytest.raises(ValueError) as exc_info:
        subclass.first_difference(valid) if side == "self" else valid.first_difference(
            subclass
        )
    assert (type(exc_info.value), str(exc_info.value)) == (ValueError, _SUBCLASS_ERROR)


@pytest.mark.parametrize("method", ["canonical_bytes", "sha256"])
def test_passive_subclass_cannot_emit_identity(
    method: Literal["canonical_bytes", "sha256"],
) -> None:
    valid = _outcome()
    passive = _PassiveTradingOutcome.model_construct(**valid.__dict__)
    _assert_direct_identity_error(passive, method, _SUBCLASS_ERROR)


@pytest.mark.parametrize("side", ["self", "actual"])
def test_passive_subclass_cannot_compare_identity(side: str) -> None:
    valid = _outcome()
    passive = _PassiveTradingOutcome.model_construct(**valid.__dict__)
    with pytest.raises(ValueError, match=f"^{_SUBCLASS_ERROR}$"):
        passive.first_difference(valid) if side == "self" else valid.first_difference(
            passive
        )


def test_sha256_cannot_dispatch_hostile_canonical_bytes_override() -> None:
    valid = _outcome()
    hostile = _HostileCanonicalBytesTradingOutcome.model_construct(
        **valid.__dict__, calls=[]
    )
    with pytest.raises(ValueError, match=f"^{_SUBCLASS_ERROR}$"):
        hostile.sha256()
    assert hostile.calls == []


@pytest.mark.parametrize("method", ["canonical_bytes", "sha256"])
def test_hostile_subclass_cannot_dispatch_identity_projection(method: str) -> None:
    valid = _outcome()
    hostile = _HostileTradingOutcome.model_construct(**valid.__dict__, calls=[])
    with pytest.raises(ValueError, match=f"^{_SUBCLASS_ERROR}$"):
        getattr(hostile, method)()
    assert hostile.calls == []


@pytest.mark.parametrize("side", ["self", "actual"])
def test_hostile_subclass_cannot_dispatch_comparison_projection(side: str) -> None:
    valid = _outcome()
    hostile = _HostileTradingOutcome.model_construct(**valid.__dict__, calls=[])
    with pytest.raises(ValueError, match=f"^{_SUBCLASS_ERROR}$"):
        hostile.first_difference(valid) if side == "self" else valid.first_difference(
            hostile
        )
    assert hostile.calls == []


def _shadow_projection(outcome: TradingOutcome, calls: list[str]) -> None:
    def hostile() -> dict[str, object]:
        calls.append("projection")
        return {}

    object.__setattr__(outcome, "_projection", hostile)


def _shadow_canonical_bytes(outcome: TradingOutcome, calls: list[str]) -> None:
    def hostile() -> bytes:
        calls.append("canonical_bytes")
        return b"hostile"

    object.__setattr__(outcome, "canonical_bytes", hostile)


def _assert_shadow_rejection(exc_info: pytest.ExceptionInfo[ValidationError]) -> None:
    errors = exc_info.value.errors(include_url=False)
    assert len(errors) == 1
    error = errors[0]
    assert (error["type"], error["loc"], error["msg"]) == (
        "value_error",
        (),
        "Value error, canonical models forbid unexpected fields",
    )
    context = error.get("ctx")
    assert context is not None
    cause = context["error"]
    assert (type(cause), str(cause)) == (
        ValueError,
        "canonical models forbid unexpected fields",
    )


@pytest.mark.parametrize("method", ["canonical_bytes", "sha256"])
def test_exact_instance_shadow_cannot_dispatch_identity_projection(method: str) -> None:
    hostile = _outcome()
    calls: list[str] = []
    _shadow_projection(hostile, calls)
    with pytest.raises(ValidationError) as exc_info:
        getattr(hostile, method)()
    assert calls == []
    _assert_shadow_rejection(exc_info)
    assert calls == []


@pytest.mark.parametrize("side", ["self", "actual"])
def test_exact_instance_shadow_cannot_dispatch_comparison_projection(side: str) -> None:
    valid, hostile = _outcome(), _outcome()
    calls: list[str] = []
    _shadow_projection(hostile, calls)
    with pytest.raises(ValidationError) as exc_info:
        hostile.first_difference(valid) if side == "self" else valid.first_difference(
            hostile
        )
    assert calls == []
    _assert_shadow_rejection(exc_info)
    assert calls == []


def test_sha256_rejects_exact_instance_canonical_bytes_shadow_without_dispatch() -> (
    None
):
    hostile = _outcome()
    calls: list[str] = []
    _shadow_canonical_bytes(hostile, calls)
    with pytest.raises(ValidationError) as exc_info:
        hostile.sha256()
    assert calls == []
    _assert_shadow_rejection(exc_info)
    assert calls == []


@pytest.mark.parametrize("side", ["self", "actual"])
def test_first_difference_rejects_canonical_bytes_shadow_without_dispatch(
    side: str,
) -> None:
    valid, hostile = _outcome(), _outcome()
    calls: list[str] = []
    _shadow_canonical_bytes(hostile, calls)
    with pytest.raises(ValidationError) as exc_info:
        hostile.first_difference(valid) if side == "self" else valid.first_difference(
            hostile
        )
    assert calls == []
    _assert_shadow_rejection(exc_info)
    assert calls == []


@pytest.mark.parametrize("side", ["self", "actual"])
def test_first_difference_rejects_falsey_model_extra(side: str) -> None:
    valid, corrupt = _outcome(), _outcome()
    extra = _FalseyModelExtra(unexpected="accepted")
    object.__setattr__(corrupt, "__pydantic_extra__", extra)
    with pytest.raises(ValueError) as exc_info:
        corrupt.first_difference(valid) if side == "self" else valid.first_difference(
            corrupt
        )
    assert (type(exc_info.value), str(exc_info.value)) == (ValueError, _EXTRA_ERROR)


@pytest.mark.parametrize("side", ["self", "actual"])
def test_first_difference_rejects_corrupt_summary(side: str) -> None:
    valid = _outcome()
    corrupt = valid.model_copy(update={"financial": valid.financial.model_dump()})
    with pytest.raises(ValueError) as exc_info:
        corrupt.first_difference(valid) if side == "self" else valid.first_difference(
            corrupt
        )
    assert (type(exc_info.value), str(exc_info.value)) == (ValueError, _SUMMARY_ERROR)
