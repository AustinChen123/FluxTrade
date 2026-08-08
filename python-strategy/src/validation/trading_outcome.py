"""Canonical, Decimal-safe outcome contract for replay parity."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from decimal import Decimal
from typing import Annotated, ClassVar, Self

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    ModelWrapValidatorHandler,
    field_validator,
    model_validator,
)
from pydantic_core import PydanticCustomError

from src.core.backtest.endpoint_state import ReplayEndpointState
from src.core.canonical_mapping import snapshot_mapping

__all__ = ["OutcomeDifference", "TradingOutcome"]


def _decimal(value: object) -> Decimal:
    if not isinstance(value, Decimal):
        raise ValueError("financial values must be finite Decimal instances")
    value = Decimal(value)
    if not value.is_finite():
        raise ValueError("financial values must be finite Decimal instances")
    if value.is_zero():
        return Decimal(0)
    sign, digits, exponent = value.as_tuple()
    assert isinstance(exponent, int)
    trailing_zeros = next(
        index for index, digit in enumerate(reversed(digits)) if digit
    )
    coefficient = digits[:-trailing_zeros] if trailing_zeros else digits
    return Decimal((sign, coefficient, exponent + trailing_zeros))


def _identity(value: str) -> str:
    if not value.strip():
        raise ValueError("logical identities must be non-empty")
    value.encode("utf-8")
    return value


def _json(value: object) -> str:
    try:
        return _json_value(value)
    except (RecursionError, UnicodeEncodeError) as error:
        raise ValueError("canonical value is not representable") from error


def _json_value(value: object) -> str:
    if value is None:
        return '["null"]'
    if isinstance(value, bool):
        return f'["bool",{str(value).lower()}]'
    if type(value) is int:
        return f'["int","{value}"]'
    if isinstance(value, Decimal):
        sign, digits, exponent = _decimal(value).as_tuple()
        assert isinstance(exponent, int)
        coefficient = "".join(str(digit) for digit in digits)
        return f'["decimal",{sign},"{coefficient}",{exponent}]'
    if isinstance(value, str):
        str.encode(value, "utf-8")
        return f'["string",{json.dumps(value, ensure_ascii=False)}]'
    if type(value) is list:
        return '["list",[' + ",".join(_json_value(item) for item in value) + "]]"
    if type(value) is tuple:
        return '["tuple",[' + ",".join(_json_value(item) for item in value) + "]]"
    if type(value) is dict:
        if not all(type(key) is str for key in value):
            raise ValueError("canonical mappings require string keys")
        for key in value:
            str.encode(key, "utf-8")
        return (
            '["map",['
            + ",".join(
                f"[{json.dumps(key, ensure_ascii=False)},{_json_value(value[key])}]"
                for key in sorted(value)
            )
            + "]]"
        )
    raise ValueError(f"unsupported canonical value: {type(value).__name__}")


Money = Annotated[Decimal, BeforeValidator(_decimal)]
Identity = Annotated[str, AfterValidator(_identity)]
CanonicalJson = Annotated[str, BeforeValidator(_json)]


def _reject_json_number(_: str) -> object:
    raise ValueError("canonical tagged JSON cannot contain raw numbers")


def _decode_canonical_json(value: object) -> object:
    if type(value) is not str:
        raise ValueError("stored canonical JSON must be a string")
    try:
        encoded = json.loads(
            value,
            parse_float=_reject_json_number,
            parse_constant=_reject_json_number,
        )
    except (ArithmeticError, RecursionError) as error:
        raise ValueError("unrepresentable stored canonical JSON") from error
    except (json.JSONDecodeError, ValueError) as error:
        raise ValueError("invalid stored canonical JSON") from error

    def decode(node: object) -> object:
        if type(node) is not list or not node or type(node[0]) is not str:
            raise ValueError("invalid canonical tag shape")
        tag = node[0]
        if tag == "null" and len(node) == 1:
            return None
        if tag == "bool" and len(node) == 2 and type(node[1]) is bool:
            return node[1]
        if (
            tag == "int"
            and len(node) == 2
            and type(node[1]) is str
            and re.fullmatch(r"-?(?:0|[1-9][0-9]*)", node[1])
        ):
            return int(node[1])
        if (
            tag == "decimal"
            and len(node) == 4
            and type(node[1]) is int
            and node[1] in (0, 1)
            and type(node[2]) is str
            and re.fullmatch(r"[0-9]+", node[2])
            and type(node[3]) is int
        ):
            return Decimal((node[1], tuple(map(int, node[2])), node[3]))
        if tag == "string" and len(node) == 2 and type(node[1]) is str:
            return node[1]
        if tag in ("list", "tuple") and len(node) == 2 and type(node[1]) is list:
            items = [decode(item) for item in node[1]]
            return items if tag == "list" else tuple(items)
        if tag == "map" and len(node) == 2 and type(node[1]) is list:
            result: dict[str, object] = {}
            for pair in node[1]:
                if (
                    type(pair) is not list
                    or len(pair) != 2
                    or type(pair[0]) is not str
                    or pair[0] in result
                ):
                    raise ValueError("invalid canonical map entry")
                result[pair[0]] = decode(pair[1])
            return result
        raise ValueError("invalid canonical tag")

    try:
        decoded = decode(encoded)
        if _json(decoded) != value:
            raise ValueError("stored canonical JSON is not byte-identical")
    except (ArithmeticError, RecursionError) as error:
        raise ValueError("unrepresentable stored canonical JSON") from error
    return decoded


def _project_observation(value: object, expected: type[BaseModel]) -> object:
    if type(value) is not expected:
        if isinstance(value, expected):
            raise ValueError(f"{expected.__name__} subclasses are unsupported")
        return value
    if isinstance(value, BaseModel) and value.model_extra is not None:
        raise ValueError(f"{expected.__name__} contains unexpected fields")
    projected = dict(value.__dict__)
    for field in ("metadata_json", "data_json"):
        if field in projected:
            projected[field] = _decode_canonical_json(projected[field])
    return projected


def _project_sequence(value: object, expected: type[BaseModel]) -> object:
    if type(value) is not tuple:
        raise PydanticCustomError("tuple_type", "Input should be a valid tuple")
    return tuple(_project_observation(item, expected) for item in value)


class _Observation(BaseModel):
    model_config = ConfigDict(
        frozen=True, strict=True, extra="forbid", revalidate_instances="always"
    )
    _strings: ClassVar[frozenset[str]] = frozenset()
    _identities: ClassVar[frozenset[str]] = frozenset()
    _timestamps: ClassVar[frozenset[str]] = frozenset()
    _canonical_json: ClassVar[frozenset[str]] = frozenset()

    @model_validator(mode="wrap")
    @classmethod
    def project_exact_instance(
        cls, value: object, handler: ModelWrapValidatorHandler[Self]
    ) -> Self:
        if not isinstance(value, _Observation):
            return handler(value)
        if type(value) is not cls:
            raise ValueError(f"{cls.__name__} subclasses are unsupported")
        if value.model_extra is not None:
            raise ValueError(f"{cls.__name__} contains unexpected fields")
        values = dict(value.__dict__)
        for field in cls._canonical_json & values.keys():
            values[field] = _decode_canonical_json(values[field])
        return handler(values)

    @model_validator(mode="before")
    @classmethod
    def validate_raw_mapping(cls, value: object) -> object:
        if not isinstance(value, Mapping):
            raise ValueError("canonical models require mapping input")
        values = snapshot_mapping(
            value,
            invalid_key_error="canonical model field names must be exact strings",
        )
        if set(values) - set(cls.model_fields):
            raise ValueError("canonical models forbid unexpected fields")
        for field in cls._strings & values.keys():
            if type(values[field]) is not str:
                raise ValueError(f"{field} must be a string")
            values[field].encode("utf-8")
        for field in cls._identities & values.keys():
            if values[field] is not None and type(values[field]) is not str:
                raise ValueError(f"{field} must be a string or null")
        for field in cls._timestamps & values.keys():
            if type(values[field]) is not int:
                raise ValueError(f"{field} must be an integer")
        return values


class SignalObservation(_Observation):
    _strings = frozenset({"strategy_id", "product_id", "timeframe", "signal_type"})
    _timestamps = frozenset({"timestamp_ms"})
    _canonical_json = frozenset({"metadata_json"})
    strategy_id: str
    product_id: str
    timeframe: str
    timestamp_ms: int
    signal_type: str
    value: Money | None
    quantity: Money | None
    price: Money | None
    stop_loss: Money | None
    take_profit: Money | None
    trailing_distance: Money | None
    metadata_json: CanonicalJson


class OrderObservation(_Observation):
    _strings = frozenset(
        {
            "logical_order_id",
            "strategy_id",
            "product_id",
            "phase",
            "status",
            "order_type",
            "side",
        }
    )
    _identities = frozenset({"parent_logical_order_id", "linked_logical_order_id"})
    _timestamps = frozenset({"timestamp_ms"})
    logical_order_id: Identity
    parent_logical_order_id: Identity | None
    linked_logical_order_id: Identity | None
    strategy_id: str
    product_id: str
    timestamp_ms: int
    phase: str
    status: str
    order_type: str
    side: str
    quantity: Money
    filled_quantity: Money
    price: Money | None
    trigger_price: Money | None
    trailing_distance: Money | None


class FillObservation(_Observation):
    _strings = frozenset(
        {"logical_order_id", "strategy_id", "product_id", "fill_type", "side"}
    )
    _timestamps = frozenset({"timestamp_ms"})
    logical_order_id: Identity
    strategy_id: str
    product_id: str
    timestamp_ms: int
    fill_type: str
    side: str
    price: Money
    quantity: Money
    fee: Money


class FinancialOutcome(_Observation):
    fees: Money
    realized_pnl: Money
    unrealized_pnl: Money
    equity: Money


class JournalObservation(_Observation):
    _strings = frozenset({"strategy_id", "tag"})
    _identities = frozenset({"logical_trade_id"})
    _timestamps = frozenset({"timestamp_ms"})
    _canonical_json = frozenset({"data_json"})
    strategy_id: str
    timestamp_ms: int
    tag: str
    logical_trade_id: Identity | None
    data_json: CanonicalJson


class OutcomeDifference(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")
    path: str
    kind: str
    expected_json: str
    actual_json: str


def _difference(
    expected: object, actual: object, path: str = "$"
) -> OutcomeDifference | None:
    if isinstance(expected, dict) and isinstance(actual, dict):
        for key in expected:
            difference = _difference(expected[key], actual[key], f"{path}.{key}")
            if difference:
                return difference
        return None
    if isinstance(expected, (list, tuple)) and isinstance(actual, (list, tuple)):
        for index, (left, right) in enumerate(zip(expected, actual, strict=False)):
            difference = _difference(left, right, f"{path}[{index}]")
            if difference:
                return difference
        if len(expected) != len(actual):
            return OutcomeDifference(
                path=path,
                kind="length",
                expected_json=_json(expected),
                actual_json=_json(actual),
            )
        return None
    if expected != actual or type(expected) is not type(actual):
        return OutcomeDifference(
            path=path,
            kind="value",
            expected_json=_json(expected),
            actual_json=_json(actual),
        )
    return None


class TradingOutcome(_Observation):
    schema_version: ClassVar[str] = "fluxtrade.trading_outcome.v2"

    signals: tuple[SignalObservation, ...]
    order_observations: tuple[OrderObservation, ...]
    fills: tuple[FillObservation, ...]
    endpoint_state: ReplayEndpointState
    financial: FinancialOutcome
    journal: tuple[JournalObservation, ...]

    @field_validator("signals", mode="before")
    @classmethod
    def reproject_signals(cls, value: object) -> object:
        return _project_sequence(value, SignalObservation)

    @field_validator("order_observations", mode="before")
    @classmethod
    def reproject_orders(cls, value: object) -> object:
        return _project_sequence(value, OrderObservation)

    @field_validator("fills", mode="before")
    @classmethod
    def reproject_fills(cls, value: object) -> object:
        return _project_sequence(value, FillObservation)

    @field_validator("endpoint_state", mode="before")
    @classmethod
    def reproject_endpoint_state(cls, value: object) -> object:
        return _project_observation(value, ReplayEndpointState)

    @field_validator("financial", mode="before")
    @classmethod
    def reproject_financial(cls, value: object) -> object:
        return _project_observation(value, FinancialOutcome)

    @field_validator("journal", mode="before")
    @classmethod
    def reproject_journal(cls, value: object) -> object:
        return _project_sequence(value, JournalObservation)

    def _projection(self) -> dict[str, object]:
        if type(self) is not TradingOutcome:
            raise ValueError("TradingOutcome subclasses are unsupported")
        if self.model_extra is not None:
            raise ValueError("TradingOutcome contains unexpected fields")
        values = self.__dict__
        sequences = (
            (values.get("signals"), SignalObservation),
            (values.get("order_observations"), OrderObservation),
            (values.get("fills"), FillObservation),
            (values.get("journal"), JournalObservation),
        )
        if any(
            type(items) is not tuple
            or any(type(item) is not expected for item in items)
            for items, expected in sequences
        ):
            raise ValueError(
                "TradingOutcome observations must have exact canonical types"
            )
        if (
            type(values.get("endpoint_state")) is not ReplayEndpointState
            or type(values.get("financial")) is not FinancialOutcome
        ):
            raise ValueError("TradingOutcome summaries must have exact canonical types")
        validated = TradingOutcome.model_validate(dict(values))
        return {
            "schema_version": self.schema_version,
            **validated.model_dump(exclude_computed_fields=True),
        }

    def canonical_bytes(self) -> bytes:
        return _json(self._projection()).encode()

    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def first_difference(self, actual: TradingOutcome) -> OutcomeDifference | None:
        return _difference(self._projection(), actual._projection())
