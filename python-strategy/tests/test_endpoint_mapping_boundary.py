from collections import UserDict
from collections.abc import Callable, Iterator, Mapping
from decimal import Decimal
from types import MappingProxyType
from typing import Literal, cast

import pytest
from pydantic import ValidationError

from src.core.backtest.endpoint_state import (
    EndpointOrder,
    EndpointPosition,
    ReplayEndpointState,
)
from src.core.models import OrderSide, PositionSide

EndpointModel = type[EndpointPosition] | type[EndpointOrder] | type[ReplayEndpointState]
ExtraMode = Literal["omitted", "forbid", "allow", "ignore"]
StrictMode = Literal["default", "true", "false"]


def _position_values() -> dict[str, object]:
    return {
        "strategy_id": "alpha",
        "product_id": "RITHMIC:MNQ-202609",
        "side": PositionSide.LONG,
        "quantity": Decimal("2"),
        "average_entry_price": Decimal("21001.25"),
    }


def _order_payload() -> dict[str, object]:
    return {
        "strategy_id": "alpha",
        "product_id": "RITHMIC:MNQ-202609",
        "side": OrderSide.BUY,
        "order_type": "LIMIT",
        "quantity": Decimal("3"),
        "timestamp": 123,
        "price": Decimal("21002.50"),
    }


def _state_payload() -> dict[str, object]:
    return {"halted_early": False}


EXPECTED_PROJECTIONS: dict[EndpointModel, dict[str, object]] = {
    EndpointPosition: _position_values(),
    EndpointOrder: {
        **_order_payload(),
        "trigger_price": None,
        "trailing_distance": None,
    },
    ReplayEndpointState: {
        "positions": (),
        "working_orders": (),
        "final_mark": None,
        "end_timestamp": None,
        "halted_early": False,
    },
}


MODEL_CASES: tuple[
    tuple[EndpointModel, Callable[[], dict[str, object]], str, object], ...
] = (
    (EndpointPosition, _position_values, "strategy_id", "omega"),
    (EndpointOrder, _order_payload, "strategy_id", "omega"),
    (ReplayEndpointState, _state_payload, "halted_early", True),
)
OWNER_CASES = tuple((model, values) for model, values, *_ in MODEL_CASES)
EXTRA_MODES: tuple[ExtraMode, ...] = ("omitted", "forbid", "allow", "ignore")
STRICT_MODES: tuple[StrictMode, ...] = ("default", "true", "false")


def _validate(
    model: EndpointModel,
    payload: object,
    extra: ExtraMode,
    strict: StrictMode = "default",
):
    if extra == "omitted":
        if strict == "default":
            return model.model_validate(payload)
        return model.model_validate(payload, strict=strict == "true")
    if strict == "default":
        return model.model_validate(payload, extra=extra)
    return model.model_validate(payload, extra=extra, strict=strict == "true")


def _root_error(exc: ValidationError, *, include: bool = True) -> Mapping[str, object]:
    errors = exc.errors(include_input=include)
    assert len(errors) == 1
    assert errors[0]["loc"] == ()
    return errors[0]


class IteratorHiddenDict(dict[str, object]):
    def __iter__(self) -> Iterator[str]:
        return (key for key in dict.__iter__(self) if key != "unexpected")


class CombinedHiddenDict(IteratorHiddenDict):
    def keys(self):
        return {key: None for key in self}.keys()

    def items(self):
        return {key: dict.__getitem__(self, key) for key in self}.items()

    def __getitem__(self, key: str) -> object:
        if key == "unexpected":
            raise KeyError(key)
        return dict.__getitem__(self, key)


class HostileField(str):
    impersonated: str
    fail_hash: bool
    armed: bool
    calls: dict[str, int]

    def __new__(cls, raw: str, impersonated: str, fail_hash: bool):
        instance = super().__new__(cls, raw)
        instance.impersonated = impersonated
        instance.fail_hash = fail_hash
        instance.armed = False
        instance.calls = dict.fromkeys(("hash", "equality", "repr", "format"), 0)
        return instance

    def __hash__(self) -> int:
        self.calls["hash"] += 1
        if self.armed and self.fail_hash:
            raise RuntimeError("hostile hash")
        return hash(self.impersonated if self.armed else str(self))

    def __eq__(self, other: object) -> bool:
        self.calls["equality"] += 1
        if self.armed:
            return other == self.impersonated
        return str.__eq__(self, other)

    def __repr__(self) -> str:
        self.calls["repr"] += 1
        return str.__repr__(self)

    def __format__(self, format_spec: str) -> str:
        self.calls["format"] += 1
        return str.__format__(self, format_spec)


class HostileValue:
    def __init__(self) -> None:
        self.calls = dict.fromkeys(("str", "bool", "repr", "format"), 0)

    def __str__(self) -> str:
        self.calls["str"] += 1
        return "hostile"

    def __bool__(self) -> bool:
        self.calls["bool"] += 1
        return True

    def __repr__(self) -> str:
        self.calls["repr"] += 1
        return "HostileValue()"

    def __format__(self, format_spec: str) -> str:
        self.calls["format"] += 1
        return format("hostile", format_spec)


class HostilePayload(dict[object, object]):
    lookups: list[object]

    def __getitem__(self, key: object) -> object:
        self.lookups.append(key)
        return dict.__getitem__(self, key)


class StatefulMapping(Mapping[str, object]):
    def __init__(self, first: dict[str, object], second: dict[str, object]) -> None:
        self.views = (first, second)
        self.iterations = 0
        self.lookups: dict[str, int] = {}

    def __iter__(self) -> Iterator[str]:
        view = self.views[min(self.iterations, 1)]
        self.iterations += 1
        return iter(view)

    def __len__(self) -> int:
        return len(self.views[min(max(self.iterations - 1, 0), 1)])

    def __getitem__(self, key: str) -> object:
        view = self.views[min(max(self.iterations - 1, 0), 1)]
        self.lookups[key] = self.lookups.get(key, 0) + 1
        return view[key]


class DuplicateKeyMapping(Mapping[str, object]):
    def __init__(self, values: dict[str, object], repeated: str) -> None:
        self.data = values
        self.repeated = repeated
        self.iterations = 0
        self.lookups: dict[str, int] = {}

    def __iter__(self) -> Iterator[str]:
        self.iterations += 1
        return iter((*self.data, self.repeated))

    def __len__(self) -> int:
        return len(self.data) + 1

    def __getitem__(self, key: str) -> object:
        self.lookups[key] = self.lookups.get(key, 0) + 1
        return self.data[key]


class ExplodingMapping(Mapping[str, object]):
    def __init__(
        self,
        values: dict[str, object],
        operation: Literal["iterator", "getitem"],
        error: Exception,
    ) -> None:
        self.data = values
        self.operation = operation
        self.error = error
        self.iterations = 0
        self.lookups = 0

    def __iter__(self) -> Iterator[str]:
        self.iterations += 1
        if self.operation == "iterator":
            raise self.error
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def __getitem__(self, key: str) -> object:
        self.lookups += 1
        if self.operation == "getitem":
            raise self.error
        return self.data[key]


def _assert_boundary_error(call: Callable[[], object], message: str) -> None:
    with pytest.raises(ValidationError) as exc_info:
        call()
    assert message in str(exc_info.value)
    _root_error(exc_info.value)


def _assert_projection(result: object, fields: dict[str, object], cell: object) -> None:
    assert {field: getattr(result, field) for field in fields} == fields, cell


@pytest.mark.parametrize(("model", "values", "field", "second"), MODEL_CASES)
@pytest.mark.parametrize("extra", EXTRA_MODES)
@pytest.mark.parametrize("strict", STRICT_MODES)
@pytest.mark.parametrize(
    "attack",
    (
        "hidden_iterator",
        "hidden_combined",
        "non_exact_spoof",
        "non_exact_hash",
        "stateful",
        "user_dict",
        "mapping_proxy",
        "valid_dict",
    ),
)
def test_endpoint_mapping_owner_core_matrix(
    model: EndpointModel,
    values: Callable[[], dict[str, object]],
    field: str,
    second: object,
    extra: ExtraMode,
    strict: StrictMode,
    attack: str,
) -> None:
    source = values()
    cell = (model, extra, strict, attack)
    if attack.startswith("hidden_"):
        payload_type = (
            IteratorHiddenDict if attack.endswith("iterator") else CombinedHiddenDict
        )
        payload = payload_type({**source, "unexpected": "hidden"})
        assert dict(dict.items(payload))["unexpected"] == "hidden"
        _assert_boundary_error(
            lambda: _validate(model, payload, extra, strict),
            "unexpected endpoint fields",
        )
        return
    if attack.startswith("non_exact_"):
        source.pop(field)
        key = HostileField(
            f"hidden_{field}" if attack.endswith("spoof") else field,
            field,
            attack.endswith("hash"),
        )
        hostile_value = HostileValue()
        payload = HostilePayload({**source, key: hostile_value})
        payload.lookups = []
        key.armed = True
        initial_calls = key.calls.copy()
        with pytest.raises(ValidationError) as exc_info:
            _validate(model, payload, extra, strict)
        error = _root_error(exc_info.value, include=False)
        assert error["type"] == "value_error", cell
        cause = cast(dict[str, object], error["ctx"])["error"]
        assert type(cause) is ValueError, cell
        assert str(cause) == "endpoint field names must be exact strings", cell
        assert key.calls == initial_calls
        assert payload.lookups == []
        assert hostile_value.calls == dict.fromkeys(
            ("str", "bool", "repr", "format"), 0
        )
        return
    if attack == "stateful":
        payload = StatefulMapping(source, {**source, field: second})
        if strict != "false":
            with pytest.raises(ValidationError) as exc_info:
                _validate(model, payload, extra, strict)
            assert _root_error(exc_info.value)["type"] == "model_type"
            assert payload.iterations == 1
            return
        result = _validate(model, payload, extra, strict)
        _assert_projection(result, EXPECTED_PROJECTIONS[model], cell)
        assert payload.iterations == 1
        assert payload.lookups == dict.fromkeys(source, 1)
    elif attack in {"user_dict", "mapping_proxy"}:
        payload = (
            UserDict(source) if attack == "user_dict" else MappingProxyType(source)
        )
        if strict != "false":
            with pytest.raises(ValidationError) as exc_info:
                _validate(model, payload, extra, strict)
            assert _root_error(exc_info.value)["type"] == "model_type"
            return
        result = _validate(model, payload, extra, strict)
        _assert_projection(result, EXPECTED_PROJECTIONS[model], cell)
    else:
        result = _validate(model, source, extra, strict)
        _assert_projection(result, EXPECTED_PROJECTIONS[model], cell)
    assert result.model_extra == ({} if extra == "allow" else None)


@pytest.mark.parametrize(("model", "values"), OWNER_CASES)
@pytest.mark.parametrize("extra", EXTRA_MODES)
def test_duplicate_keys_preserve_first_observation_and_one_lookup(
    model: EndpointModel,
    values: Callable[[], dict[str, object]],
    extra: ExtraMode,
) -> None:
    source = values()
    field = next(iter(source))
    payload = DuplicateKeyMapping(source, field)
    result = _validate(model, payload, extra, "false")
    assert getattr(result, field) == source[field]
    assert payload.iterations == 1
    assert payload.lookups == dict.fromkeys(source, 1)


@pytest.mark.parametrize(("model", "values", "field", "second"), MODEL_CASES)
@pytest.mark.parametrize("extra", EXTRA_MODES)
def test_plain_dict_source_mutation_does_not_change_model(
    model: EndpointModel,
    values: Callable[[], dict[str, object]],
    field: str,
    second: object,
    extra: ExtraMode,
) -> None:
    source = values()
    result = _validate(model, source, extra, "false")
    first = source[field]
    source[field] = second
    assert getattr(result, field) == first


class IteratorFailure(RuntimeError):
    pass


class LookupFailure(KeyError):
    pass


class BoundaryFailure(ValueError):
    pass


@pytest.mark.parametrize(("model", "values"), OWNER_CASES)
@pytest.mark.parametrize("operation", ["iterator", "getitem"])
@pytest.mark.parametrize(
    "error_type", [IteratorFailure, LookupFailure, BoundaryFailure]
)
def test_mapping_exception_identity_is_preserved(
    model: EndpointModel,
    values: Callable[[], dict[str, object]],
    operation: Literal["iterator", "getitem"],
    error_type: type[Exception],
) -> None:
    error = error_type("original")
    payload = ExplodingMapping(values(), operation, error)
    if issubclass(error_type, ValueError):
        with pytest.raises(ValidationError) as exc_info:
            _validate(model, payload, "ignore", "false")
        assert _root_error(exc_info.value)["ctx"] == {"error": error}
    else:
        with pytest.raises(error_type) as exc_info:
            _validate(model, payload, "ignore", "false")
        assert exc_info.value is error
    assert payload.iterations == 1
    assert payload.lookups == (0 if operation == "iterator" else 1)


def _nested_values() -> tuple[dict[str, object], EndpointPosition, EndpointOrder]:
    position = EndpointPosition.model_validate(_position_values())
    order = EndpointOrder.model_validate(_order_payload())
    return (
        {
            "positions": (position,),
            "working_orders": (order,),
            "final_mark": Decimal("21003.75"),
            "end_timestamp": 456,
            "halted_early": False,
        },
        position,
        order,
    )


@pytest.mark.parametrize("extra", EXTRA_MODES)
@pytest.mark.parametrize("strict", STRICT_MODES)
@pytest.mark.parametrize("mapping_type", [UserDict, MappingProxyType])
def test_non_empty_nested_state_preserves_parent_revalidation(
    extra: ExtraMode,
    strict: StrictMode,
    mapping_type: Callable[[dict[str, object]], Mapping[str, object]],
) -> None:
    values, source_position, source_order = _nested_values()
    payload = mapping_type(values)
    backing = values
    if isinstance(payload, UserDict):
        backing = cast(dict[str, object], payload.data)
    backing_snapshot = backing.copy()
    position_projection = _position_values()
    order_projection = EXPECTED_PROJECTIONS[EndpointOrder]
    child_extra = (source_position.model_extra, source_order.model_extra)
    cell = (extra, strict, mapping_type)
    if strict != "false":
        with pytest.raises(ValidationError) as exc_info:
            _validate(ReplayEndpointState, payload, extra, strict)
        assert _root_error(exc_info.value)["type"] == "model_type"
        assert backing == backing_snapshot
        assert cast(tuple[object, ...], backing["positions"])[0] is source_position
        assert cast(tuple[object, ...], backing["working_orders"])[0] is source_order
        _assert_projection(source_position, position_projection, cell)
        _assert_projection(source_order, order_projection, cell)
        assert (source_position.model_extra, source_order.model_extra) == child_extra
        return
    validated = _validate(ReplayEndpointState, payload, extra, strict)
    result = cast(ReplayEndpointState, validated)
    position = result.positions[0]
    order = result.working_orders[0]
    assert type(position) is EndpointPosition
    assert type(order) is EndpointOrder
    assert position is not source_position
    assert order is not source_order
    _assert_projection(position, position_projection, cell)
    _assert_projection(order, order_projection, cell)
    _assert_projection(
        result,
        {**values, "positions": (position,), "working_orders": (order,)},
        cell,
    )
    expected_extra = {} if extra == "allow" else None
    assert result.model_extra == expected_extra
    assert position.model_extra == expected_extra
    assert order.model_extra == expected_extra
    object.__setattr__(source_position, "strategy_id", "mutated")
    object.__setattr__(source_order, "strategy_id", "mutated")
    assert position.strategy_id == "alpha"
    assert order.strategy_id == "alpha"


@pytest.mark.parametrize(
    "model", [EndpointPosition, EndpointOrder, ReplayEndpointState]
)
def test_non_mapping_is_rejected_by_endpoint_owner(model: EndpointModel) -> None:
    _assert_boundary_error(
        lambda: _validate(model, object(), "ignore"),
        "endpoint model input must be a mapping",
    )
