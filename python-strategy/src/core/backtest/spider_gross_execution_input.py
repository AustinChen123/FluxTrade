"""Closed semantic identity for original GRID with paid gross response."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from hashlib import sha256
from typing import cast

from src.core.backtest.spider_execution_input import (
    SpiderExecutionInput,
    decode_spider_execution_input,
    encode_spider_execution_input,
    validate_spider_execution_input,
)
from src.core.backtest.spider_historical_input import HistoricalInputError
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.decimal_math import canonical_decimal_text


SCHEMA_VERSION = "spider_gross_execution_input_v1"
POLICY_ID = "SPIDER_ORIGINAL_GROSS_PROTECTED_PAID_V1"
NATIVE_PROFILE_ID = "SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1"
ADMISSION_POLICY_ID = "CURRENT_MARK_GROSS_CAP_TWO_V1"
RESPONSE_POLICY_ID = "PROPORTIONAL_PAID_GROSS_TARGET_V1"
_KEYS = frozenset({
    "schema_version", "policy_id", "native_profile_id", "admission_policy_id",
    "response_policy_id", "response_parameters", "authorization_scope", "model_id",
    "execution_input",
})


@dataclass(frozen=True, slots=True)
class GrossResponseParameters:
    trigger_cap: Decimal = Decimal("2")
    target_cap: Decimal = Decimal("1.5")
    allocation: str = "GROSS_PROPORTIONAL_V1"
    quantity_rounding: str = "CEIL_STEP_NO_MINIMUM_UPLIFT_V1"


@dataclass(frozen=True, slots=True)
class GrossAuthorizationScope:
    strategy_id: str
    ordered_products: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SpiderGrossExecutionInput:
    schema_version: str
    policy_id: str
    native_profile_id: str
    admission_policy_id: str
    response_policy_id: str
    response_parameters: GrossResponseParameters
    authorization_scope: GrossAuthorizationScope
    model_id: None
    execution_input: SpiderExecutionInput


def _object(value: object) -> dict[str, object]:
    if type(value) is not dict or set(cast(dict[object, object], value)) != _KEYS:
        raise HistoricalInputError("invalid gross execution input shape")
    return cast(dict[str, object], value)


def _parameters_value(value: GrossResponseParameters) -> dict[str, object]:
    return {
        "trigger_cap": value.trigger_cap,
        "target_cap": value.target_cap,
        "allocation": value.allocation,
        "quantity_rounding": value.quantity_rounding,
    }


def _scope_value(value: GrossAuthorizationScope) -> dict[str, object]:
    return {
        "kind": "RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1",
        "strategy_id": value.strategy_id,
        "ordered_products": list(value.ordered_products),
    }


def _parameter_decimal(value: object) -> Decimal:
    if type(value) is not str:
        raise HistoricalInputError("invalid gross response decimal parameter")
    try:
        parsed = Decimal(value)
    except InvalidOperation as error:
        raise HistoricalInputError("invalid gross response decimal parameter") from error
    if not parsed.is_finite() or canonical_decimal_text(parsed) != value:
        raise HistoricalInputError("noncanonical gross response decimal parameter")
    return parsed


def validate_spider_gross_execution_input(value: SpiderGrossExecutionInput) -> str:
    """Validate exact gross response identity and return its semantic SHA-256."""
    if type(value) is not SpiderGrossExecutionInput:
        raise HistoricalInputError("invalid gross execution DTO")
    if (
        value.schema_version != SCHEMA_VERSION
        or value.policy_id != POLICY_ID
        or value.native_profile_id != NATIVE_PROFILE_ID
        or value.admission_policy_id != ADMISSION_POLICY_ID
        or value.response_policy_id != RESPONSE_POLICY_ID
        or value.model_id is not None
    ):
        raise HistoricalInputError("unsupported gross execution identity")
    if type(value.response_parameters) is not GrossResponseParameters:
        raise HistoricalInputError("invalid gross response parameters")
    parameters = value.response_parameters
    if (
        type(parameters.trigger_cap) is not Decimal
        or parameters.trigger_cap != Decimal("2")
        or type(parameters.target_cap) is not Decimal
        or parameters.target_cap != Decimal("1.5")
        or type(parameters.allocation) is not str
        or parameters.allocation != "GROSS_PROPORTIONAL_V1"
        or type(parameters.quantity_rounding) is not str
        or parameters.quantity_rounding != "CEIL_STEP_NO_MINIMUM_UPLIFT_V1"
    ):
        raise HistoricalInputError("unsupported gross response parameters")
    if type(value.authorization_scope) is not GrossAuthorizationScope:
        raise HistoricalInputError("invalid gross authorization scope")
    scope = value.authorization_scope
    if (
        type(scope.strategy_id) is not str
        or not scope.strategy_id
        or type(scope.ordered_products) is not tuple
        or not scope.ordered_products
        or any(type(product) is not str or not product for product in scope.ordered_products)
    ):
        raise HistoricalInputError("invalid gross authorization scope")
    execution_hash = validate_spider_execution_input(value.execution_input)
    history = value.execution_input.historical_input
    if scope.strategy_id != history.strategy_identity or scope.ordered_products != history.ordered_products:
        raise HistoricalInputError("gross authorization scope mismatch")
    return sha256(canonical_bytes({
        "schema_version": value.schema_version,
        "policy_id": value.policy_id,
        "native_profile_id": value.native_profile_id,
        "admission_policy_id": value.admission_policy_id,
        "response_policy_id": value.response_policy_id,
        "response_parameters": _parameters_value(parameters),
        "authorization_scope": _scope_value(scope),
        "model_id": value.model_id,
        "execution_input_sha256": execution_hash,
    })).hexdigest()


def encode_spider_gross_execution_input(value: SpiderGrossExecutionInput) -> bytes:
    validate_spider_gross_execution_input(value)
    nested = decode_canonical(encode_spider_execution_input(value.execution_input))
    return canonical_bytes({
        "schema_version": value.schema_version,
        "policy_id": value.policy_id,
        "native_profile_id": value.native_profile_id,
        "admission_policy_id": value.admission_policy_id,
        "response_policy_id": value.response_policy_id,
        "response_parameters": _parameters_value(value.response_parameters),
        "authorization_scope": _scope_value(value.authorization_scope),
        "model_id": value.model_id,
        "execution_input": nested,
    })


def decode_spider_gross_execution_input(raw: bytes) -> SpiderGrossExecutionInput:
    try:
        row = _object(decode_canonical(raw))
        parameter_row = row["response_parameters"]
        if type(parameter_row) is not dict or set(cast(dict[object, object], parameter_row)) != {
            "trigger_cap", "target_cap", "allocation", "quantity_rounding",
        }:
            raise HistoricalInputError("invalid gross response parameters")
        parameters = cast(dict[str, object], parameter_row)
        scope_value = row["authorization_scope"]
        if type(scope_value) is not dict or set(cast(dict[object, object], scope_value)) != {
            "kind", "strategy_id", "ordered_products",
        }:
            raise HistoricalInputError("invalid gross authorization scope")
        scope = cast(dict[str, object], scope_value)
        products = scope["ordered_products"]
        nested = row["execution_input"]
        if (type(products) is not list or type(nested) is not dict
                or scope["kind"] != "RUN_STRATEGY_OWNED_LOTS_AND_ORDERS_V1"
                or row["model_id"] is not None):
            raise HistoricalInputError("invalid gross execution nested input")
        value = SpiderGrossExecutionInput(
            cast(str, row["schema_version"]), cast(str, row["policy_id"]),
            cast(str, row["native_profile_id"]), cast(str, row["admission_policy_id"]),
            cast(str, row["response_policy_id"]),
            GrossResponseParameters(
                _parameter_decimal(parameters["trigger_cap"]),
                _parameter_decimal(parameters["target_cap"]),
                cast(str, parameters["allocation"]),
                cast(str, parameters["quantity_rounding"]),
            ),
            GrossAuthorizationScope(
                cast(str, scope["strategy_id"]),
                tuple(cast(list[str], products)),
            ),
            None,
            decode_spider_execution_input(canonical_bytes(nested)),
        )
        validate_spider_gross_execution_input(value)
        if encode_spider_gross_execution_input(value) != raw:
            raise HistoricalInputError("noncanonical gross execution input encoding")
        return value
    except HistoricalInputError:
        raise
    except (TypeError, ValueError, UnicodeError) as error:
        raise HistoricalInputError("invalid gross execution input encoding") from error


__all__ = [
    "ADMISSION_POLICY_ID", "GrossAuthorizationScope", "GrossResponseParameters",
    "NATIVE_PROFILE_ID", "POLICY_ID", "RESPONSE_POLICY_ID", "SCHEMA_VERSION",
    "SpiderGrossExecutionInput", "decode_spider_gross_execution_input",
    "encode_spider_gross_execution_input", "validate_spider_gross_execution_input",
]
