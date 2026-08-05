"""Canonical, Decimal-safe outcome contract for replay parity."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from decimal import Decimal
from typing import Annotated, ClassVar

from pydantic import AfterValidator, BaseModel, BeforeValidator, ConfigDict

from src.core.backtest.endpoint_state import ReplayEndpointState

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
    return value


def _json(value: object) -> str:
    if value is None:
        return '["null"]'
    if isinstance(value, bool):
        return f'["bool",{str(value).lower()}]'
    if isinstance(value, int):
        return f'["int","{value}"]'
    if isinstance(value, Decimal):
        sign, digits, exponent = _decimal(value).as_tuple()
        assert isinstance(exponent, int)
        coefficient = "".join(str(digit) for digit in digits)
        return f'["decimal",{sign},"{coefficient}",{exponent}]'
    if isinstance(value, str):
        return f'["string",{json.dumps(value, ensure_ascii=False)}]'
    if isinstance(value, (list, tuple)):
        tag = "list" if isinstance(value, list) else "tuple"
        return f'["{tag}",[' + ",".join(_json(item) for item in value) + "]]"
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise ValueError("canonical mappings require string keys")
        return (
            '["map",['
            + ",".join(
                f"[{json.dumps(key, ensure_ascii=False)},{_json(value[key])}]"
                for key in sorted(value)
            )
            + "]]"
        )
    raise ValueError(f"unsupported canonical value: {type(value).__name__}")


Money = Annotated[Decimal, BeforeValidator(_decimal)]
Identity = Annotated[str, AfterValidator(_identity)]
CanonicalJson = Annotated[str, BeforeValidator(_json)]


class _Observation(BaseModel):
    model_config = ConfigDict(frozen=True, strict=True, extra="forbid")


class SignalObservation(_Observation):
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

    def _projection(self) -> dict[str, object]:
        return {
            "schema_version": self.schema_version,
            **self.model_dump(exclude_computed_fields=True),
        }

    def canonical_bytes(self) -> bytes:
        return _json(self._projection()).encode()

    def sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()

    def first_difference(self, actual: TradingOutcome) -> OutcomeDifference | None:
        return _difference(self._projection(), actual._projection())
