"""One server-owned profile for compiling sealed GoldenCross GA requests."""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping
from decimal import Decimal, InvalidOperation
from enum import Enum
from functools import lru_cache
from hashlib import sha256
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict

from src.control_plane.models import (
    BacktestInstrumentConfig,
    EvolutionConfig,
    ParameterSearchDimension,
    ParameterSearchJobRequest,
    ParameterSearchSpace,
    SealedDatasetMarketData,
)
from src.core.data_sources.research_database import ResearchDatasetMetadata
from src.core.decimal_math import canonical_decimal_text
from src.core.product_registry import (
    CapitalModel,
    FeeModel,
    quantize_order_values,
    validate_product_id,
)
from src.strategies.golden_cross import GoldenCrossStrategy


_PROFILE_ID = "golden_cross_research_v1"
_STRATEGY_SUBJECT = "builtin:golden_cross"
_FITNESS_PROFILE_ID = "mark_to_market_pnl_v1"
_COST_PROFILE_ID = "explicit_accounting_v1"
_MIN_TIMESTAMP = 0
_MAX_TIMESTAMP = 253_402_300_799_999
_IDENTITY_FIELDS = (
    "parameter_search_profile_id",
    "strategy_subject",
    "fitness_profile_id",
    "cost_profile_id",
    "profile_revision",
    "strategy_version",
)
_REQUIRED_FIELDS = frozenset(
    {
        *_IDENTITY_FIELDS,
        "dataset_id",
        "start_time",
        "end_time",
        "initial_balance",
        "fees",
        "instrument",
    }
)
_OPTIONAL_FIELDS = frozenset(
    {"parameters", "population_size", "max_generations", "seed"}
)
_INSTRUMENT_FIELDS = frozenset(
    {
        "multiplier",
        "quantity_step",
        "price_tick",
        "fee_model",
        "capital_model",
        "capital_per_contract",
    }
)
_INSTRUMENT_DECIMAL_FIELDS = frozenset(
    {"multiplier", "quantity_step", "price_tick", "capital_per_contract"}
)
_INSTRUMENT_NULLABLE_DECIMAL_FIELDS = frozenset(
    {"quantity_step", "price_tick", "capital_per_contract"}
)
_MISSING = object()
_DEFAULT_SHORT_RANGE = {"min": 5, "max": 50, "step": 5}
_DEFAULT_LONG_RANGE = {"min": 60, "max": 200, "step": 10}
_DEFAULT_QUANTITY = Decimal("0.01")
_DEFAULT_POPULATION_SIZE = 32
_DEFAULT_MAX_GENERATIONS = 10
_DEFAULT_SEED = 0
_EVOLUTION_DEFAULTS = {
    "tournament_size": 2,
    "elite_count": 1,
    "crossover_probability": Decimal("0.9"),
    "mutation_probability": Decimal("0.1"),
    "mutation_sigma_steps": Decimal("1"),
}


class GaProfileBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    parameter_search_profile_id: str
    profile_revision: str
    strategy_subject: str
    strategy_version: str
    dataset_id: str
    dataset_checksum: str
    fitness_profile_id: str
    cost_profile_id: str
    input_digest: str


class CompiledGaProfileRequest(ParameterSearchJobRequest):
    """Typed internal extension; legacy request models stay unchanged."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    ga_binding: GaProfileBinding


def get_golden_cross_profile() -> dict[str, Any]:
    """Return a detached description of the sole accepted profile."""
    return json.loads(_profile_json())


def compile_golden_cross_request(
    payload: Mapping[str, object],
    dataset_metadata: ResearchDatasetMetadata,
) -> CompiledGaProfileRequest:
    """Validate profile input and compile it into the sealed GA request."""
    if not isinstance(payload, Mapping):
        raise ValueError("profile input must be an object")
    supplied = dict(payload)
    unknown = set(supplied) - _REQUIRED_FIELDS - _OPTIONAL_FIELDS
    missing = _REQUIRED_FIELDS - set(supplied)
    if unknown:
        raise ValueError("profile input contains unsupported fields")
    if missing:
        raise ValueError("profile input is missing required fields")

    profile = get_golden_cross_profile()
    expected_identity = {
        "parameter_search_profile_id": _PROFILE_ID,
        "strategy_subject": _STRATEGY_SUBJECT,
        "fitness_profile_id": _FITNESS_PROFILE_ID,
        "cost_profile_id": _COST_PROFILE_ID,
        "profile_revision": profile["profile_revision"],
        "strategy_version": profile["strategy_version"],
    }
    if any(
        type(supplied[name]) is not str or supplied[name] != expected
        for name, expected in expected_identity.items()
    ):
        raise ValueError("profile identity or revision does not match")
    if not isinstance(dataset_metadata, ResearchDatasetMetadata):
        raise ValueError("sealed dataset metadata is required")
    if supplied["dataset_id"] != dataset_metadata.id:
        raise ValueError("dataset_id does not match sealed metadata")

    start_time = _strict_int(supplied["start_time"], "start_time")
    end_time = _strict_int(supplied["end_time"], "end_time")
    if not (_MIN_TIMESTAMP <= start_time <= end_time <= _MAX_TIMESTAMP):
        raise ValueError("requested window is invalid")
    if start_time < dataset_metadata.start_time or end_time > dataset_metadata.end_time:
        raise ValueError("requested window exceeds sealed dataset coverage")

    parameters = _compile_parameters(supplied.get("parameters", _MISSING))
    population_size = _bounded_int(
        supplied.get("population_size", _DEFAULT_POPULATION_SIZE),
        "population_size",
        2,
        256,
    )
    max_generations = _bounded_int(
        supplied.get("max_generations", _DEFAULT_MAX_GENERATIONS),
        "max_generations",
        1,
        100,
    )
    seed = _bounded_int(supplied.get("seed", _DEFAULT_SEED), "seed", 0, 2_147_483_647)
    cardinality = _range_cardinality(parameters["short_window"]) * _range_cardinality(
        parameters["long_window"]
    )
    if population_size > cardinality:
        raise ValueError("population_size exceeds search-space cardinality")

    initial_balance = _decimal(supplied["initial_balance"], "initial_balance")
    if initial_balance <= 0:
        raise ValueError("initial_balance must be positive")
    fees = _mapping(supplied["fees"], "fees")
    if set(fees) != {"maker", "taker"}:
        raise ValueError("fees must contain exactly maker and taker")
    maker_fee = _decimal(fees["maker"], "fees.maker")
    taker_fee = _decimal(fees["taker"], "fees.taker")
    if maker_fee < 0 or taker_fee < 0:
        raise ValueError("fees must be non-negative")

    quantity = parameters["quantity"]
    instrument = _compile_instrument(supplied["instrument"])
    spec = instrument.to_instrument_spec(dataset_metadata.product_id)
    validate_product_id(dataset_metadata.product_id)
    if spec.min_quantity is not None and quantity < spec.min_quantity:
        raise ValueError("quantity is below the configured minimum")
    quantized = quantize_order_values(
        quantity=quantity,
        price=None,
        spec=spec,
    )
    if quantized.quantity != quantity:
        raise ValueError("quantity does not align to instrument quantity_step")

    search_space = ParameterSearchSpace(
        parameters={
            "short_window": ParameterSearchDimension(
                type="integer", **parameters["short_window"]
            ),
            "long_window": ParameterSearchDimension(
                type="integer", **parameters["long_window"]
            ),
            "quantity": ParameterSearchDimension(
                type="decimal", min=quantity, max=quantity, step=quantity
            ),
        }
    )
    evolution = EvolutionConfig(
        population_size=population_size,
        max_generations=max_generations,
        **_EVOLUTION_DEFAULTS,
    )
    base_request: dict[str, object] = {
        "kind": "parameter_search",
        "strategy_type": "golden_cross",
        "strategy_id": "golden_cross",
        "product_id": dataset_metadata.product_id,
        "timeframe": dataset_metadata.timeframe,
        "start_time": start_time,
        "end_time": end_time,
        "market_data": SealedDatasetMarketData(
            kind="sealed_dataset", dataset_id=dataset_metadata.id
        ),
        "objective": "maximize_score",
        "seed": seed,
        "backtest": {
            "candles_csv_path": None,
            "initial_balance": initial_balance,
            "maker_fee": maker_fee,
            "taker_fee": taker_fee,
            "instrument": instrument,
            "write_reports": False,
        },
        "search_space": search_space,
        "evolution": evolution,
    }
    binding_values: dict[str, object] = {
        "parameter_search_profile_id": _PROFILE_ID,
        "profile_revision": profile["profile_revision"],
        "strategy_subject": _STRATEGY_SUBJECT,
        "strategy_version": profile["strategy_version"],
        "dataset_id": dataset_metadata.id,
        "dataset_checksum": dataset_metadata.checksum_sha256,
        "fitness_profile_id": _FITNESS_PROFILE_ID,
        "cost_profile_id": _COST_PROFILE_ID,
        "input_digest": "",
    }
    compiled = CompiledGaProfileRequest.model_validate(
        {**base_request, "ga_binding": binding_values}
    )
    digest = _input_digest(compiled)
    binding = compiled.ga_binding.model_copy(update={"input_digest": digest})
    return compiled.model_copy(update={"ga_binding": binding})


@lru_cache(maxsize=1)
def _profile_json() -> str:
    module = sys.modules.get(GoldenCrossStrategy.__module__)
    module_path = getattr(module, "__file__", None)
    if not isinstance(module_path, str):
        raise RuntimeError("builtin GoldenCross source path is unavailable")
    try:
        strategy_version = sha256(Path(module_path).read_bytes()).hexdigest()
    except OSError:
        raise RuntimeError("builtin GoldenCross source cannot be read") from None

    instrument_defaults = BacktestInstrumentConfig()
    definition: dict[str, object] = {
        "parameter_search_profile_id": _PROFILE_ID,
        "strategy_subject": _STRATEGY_SUBJECT,
        "strategy_version": strategy_version,
        "fitness_profile_id": _FITNESS_PROFILE_ID,
        "cost_profile_id": _COST_PROFILE_ID,
        "accepted_fields": {
            "parameter_search_profile_id": {
                "required": True,
                "type": "string",
                "const": _PROFILE_ID,
                "immutable": "server_profile",
            },
            "strategy_subject": {
                "required": True,
                "type": "string",
                "const": _STRATEGY_SUBJECT,
                "immutable": "server_profile",
            },
            "fitness_profile_id": {
                "required": True,
                "type": "string",
                "const": _FITNESS_PROFILE_ID,
                "immutable": "server_profile",
            },
            "cost_profile_id": {
                "required": True,
                "type": "string",
                "const": _COST_PROFILE_ID,
                "immutable": "server_profile",
            },
            "profile_revision": {
                "required": True,
                "type": "sha256",
                "immutable": "server_profile",
            },
            "strategy_version": {
                "required": True,
                "type": "sha256",
                "immutable": "imported_builtin_source",
            },
            "dataset_id": {
                "required": True,
                "type": "string",
                "source": "sealed_metadata.id",
                "immutable": "compiled_binding",
            },
            "start_time": {
                "required": True,
                "type": "strict_integer_utc_ms",
                "minimum": _MIN_TIMESTAMP,
                "maximum": _MAX_TIMESTAMP,
                "constraint": "within sealed coverage and <= end_time",
                "immutable": "compiled_binding",
            },
            "end_time": {
                "required": True,
                "type": "strict_integer_utc_ms",
                "minimum": _MIN_TIMESTAMP,
                "maximum": _MAX_TIMESTAMP,
                "constraint": "inclusive; within sealed coverage and >= start_time",
                "immutable": "compiled_binding",
            },
            "initial_balance": {
                "required": True,
                "type": "finite_decimal_or_decimal_string",
                "exclusive_minimum": "0",
                "immutable": "compiled_binding",
            },
            "fees": {
                "required": True,
                "type": "object",
                "immutable": "compiled_binding",
                "fields": {
                    name: {
                        "required": True,
                        "type": "finite_decimal_or_decimal_string",
                        "minimum": "0",
                        "immutable": "compiled_binding",
                    }
                    for name in ("maker", "taker")
                },
            },
            "instrument": {
                "required": True,
                "type": "object",
                "immutable": "compiled_binding",
                "constraint": "dated_future products require quantity_step and price_tick",
                "fields": {
                    "multiplier": {
                        "required": False,
                        "type": "finite_decimal_or_decimal_string",
                        "default": canonical_decimal_text(
                            instrument_defaults.multiplier
                        ),
                        "exclusive_minimum": "0",
                        "immutable": "compiled_binding",
                    },
                    "quantity_step": {
                        "required": False,
                        "type": "finite_decimal_or_decimal_string_or_null",
                        "default": None,
                        "exclusive_minimum_when_set": "0",
                        "constraint": "required for dated_future products",
                        "immutable": "compiled_binding",
                    },
                    "price_tick": {
                        "required": False,
                        "type": "finite_decimal_or_decimal_string_or_null",
                        "default": None,
                        "exclusive_minimum_when_set": "0",
                        "constraint": "required for dated_future products",
                        "immutable": "compiled_binding",
                    },
                    "fee_model": {
                        "required": False,
                        "type": "string_enum",
                        "default": instrument_defaults.fee_model.value,
                        "choices": sorted(item.value for item in FeeModel),
                        "immutable": "compiled_binding",
                    },
                    "capital_model": {
                        "required": False,
                        "type": "string_enum",
                        "default": instrument_defaults.capital_model.value,
                        "choices": sorted(item.value for item in CapitalModel),
                        "immutable": "compiled_binding",
                    },
                    "capital_per_contract": {
                        "required": False,
                        "type": "finite_decimal_or_decimal_string_or_null",
                        "default": None,
                        "exclusive_minimum_when_set": "0",
                        "constraint": "positive and required only when capital_model is per_contract",
                        "immutable": "compiled_binding",
                    },
                },
            },
            "parameters": {
                "required": False,
                "type": "object",
                "fields": {
                    "short_window": {
                        "required": False,
                        "type": "integer_range",
                        "default": dict(_DEFAULT_SHORT_RANGE),
                        "constraint": "min <= max; step > 0",
                        "fields": {
                            "min": {
                                "required": True,
                                "type": "strict_integer",
                                "minimum": 1,
                                "maximum": 10000,
                                "immutable": "compiled_binding",
                            },
                            "max": {
                                "required": True,
                                "type": "strict_integer",
                                "minimum": 1,
                                "maximum": 10000,
                                "immutable": "compiled_binding",
                            },
                            "step": {
                                "required": True,
                                "type": "strict_integer",
                                "exclusive_minimum": 0,
                                "immutable": "compiled_binding",
                            },
                        },
                        "immutable": "compiled_binding",
                    },
                    "long_window": {
                        "required": False,
                        "type": "integer_range",
                        "default": dict(_DEFAULT_LONG_RANGE),
                        "constraint": "min <= max; step > 0",
                        "fields": {
                            "min": {
                                "required": True,
                                "type": "strict_integer",
                                "minimum": 2,
                                "maximum": 10000,
                                "immutable": "compiled_binding",
                            },
                            "max": {
                                "required": True,
                                "type": "strict_integer",
                                "minimum": 2,
                                "maximum": 10000,
                                "immutable": "compiled_binding",
                            },
                            "step": {
                                "required": True,
                                "type": "strict_integer",
                                "exclusive_minimum": 0,
                                "immutable": "compiled_binding",
                            },
                        },
                        "immutable": "compiled_binding",
                    },
                    "quantity": {
                        "required": False,
                        "type": "finite_decimal_or_decimal_string",
                        "default": canonical_decimal_text(_DEFAULT_QUANTITY),
                        "exclusive_minimum": "0",
                        "immutable": "compiled_binding",
                    },
                },
                "dependency": "short_window.max < long_window.min",
                "quantity_constraint": "must align to instrument.quantity_step when configured",
            },
            "population_size": {
                "required": False,
                "type": "strict_integer",
                "default": _DEFAULT_POPULATION_SIZE,
                "minimum": 2,
                "maximum": 256,
                "constraint": "must not exceed parameter Cartesian cardinality",
                "immutable": "compiled_binding",
            },
            "max_generations": {
                "required": False,
                "type": "strict_integer",
                "default": _DEFAULT_MAX_GENERATIONS,
                "minimum": 1,
                "maximum": 100,
                "immutable": "compiled_binding",
            },
            "seed": {
                "required": False,
                "type": "strict_integer",
                "default": _DEFAULT_SEED,
                "minimum": 0,
                "maximum": 2147483647,
                "immutable": "compiled_binding",
            },
        },
        "compiled_fields": {
            "kind": "parameter_search",
            "strategy_type": "golden_cross",
            "strategy_id": "golden_cross",
            "objective": "maximize_score",
            "market_data": "sealed_dataset",
            "write_reports": False,
            "capital_allocation": None,
            "evaluation_set": None,
            "fitness": None,
        },
        "evolution_defaults": {
            key: canonical_decimal_text(value) if isinstance(value, Decimal) else value
            for key, value in _EVOLUTION_DEFAULTS.items()
        },
    }
    revision = sha256(_canonical_json(definition).encode("utf-8")).hexdigest()
    return _canonical_json({**definition, "profile_revision": revision})


def _compile_parameters(value: object) -> dict[str, Any]:
    raw = {} if value is _MISSING else _mapping(value, "parameters")
    if set(raw) - {"short_window", "long_window", "quantity"}:
        raise ValueError("parameters contain unsupported fields")
    short_range = _integer_range(
        raw.get("short_window", _DEFAULT_SHORT_RANGE),
        "short_window",
        1,
        10_000,
    )
    long_range = _integer_range(
        raw.get("long_window", _DEFAULT_LONG_RANGE),
        "long_window",
        2,
        10_000,
    )
    if short_range["max"] >= long_range["min"]:
        raise ValueError("short_window.max must be less than long_window.min")
    quantity = _decimal(raw.get("quantity", _DEFAULT_QUANTITY), "parameters.quantity")
    if quantity <= 0:
        raise ValueError("quantity must be positive")
    return {
        "short_window": short_range,
        "long_window": long_range,
        "quantity": quantity,
    }


def _integer_range(
    value: object, name: str, minimum: int, maximum: int
) -> dict[str, int]:
    raw = _mapping(value, name)
    if set(raw) != {"min", "max", "step"}:
        raise ValueError(f"{name} requires min, max and step")
    bounds = {key: _strict_int(raw[key], f"{name}.{key}") for key in raw}
    if (
        bounds["min"] < minimum
        or bounds["max"] > maximum
        or bounds["min"] > bounds["max"]
        or bounds["step"] <= 0
    ):
        raise ValueError(f"{name} range is outside its supported domain")
    return bounds


def _compile_instrument(value: object) -> BacktestInstrumentConfig:
    raw = _mapping(value, "instrument")
    unknown = set(raw) - _INSTRUMENT_FIELDS
    if unknown:
        raise ValueError("instrument contains unsupported fields")
    normalized = dict(raw)
    for name in _INSTRUMENT_DECIMAL_FIELDS.intersection(raw):
        field_value = raw[name]
        if field_value is None and name in _INSTRUMENT_NULLABLE_DECIMAL_FIELDS:
            continue
        normalized[name] = _decimal(field_value, f"instrument.{name}")
    for name, enum_type in (("fee_model", FeeModel), ("capital_model", CapitalModel)):
        if name in raw:
            enum_value = raw[name]
            if type(enum_value) is not str:
                raise ValueError(f"instrument.{name} must be a string")
            try:
                normalized[name] = enum_type(enum_value)
            except ValueError:
                raise ValueError(f"instrument.{name} is unsupported") from None
    return BacktestInstrumentConfig.model_validate(normalized)


def _decimal(value: object, field_name: str) -> Decimal:
    if type(value) is Decimal:
        parsed = value
    elif type(value) is str:
        try:
            parsed = Decimal(value)
        except InvalidOperation:
            raise ValueError(f"{field_name} must be a finite Decimal") from None
    else:
        raise ValueError(f"{field_name} must be a Decimal or decimal string")
    if not parsed.is_finite():
        raise ValueError(f"{field_name} must be finite")
    return parsed


def _strict_int(value: object, field_name: str) -> int:
    if type(value) is not int:
        raise ValueError(f"{field_name} must be an integer")
    return value


def _bounded_int(value: object, field_name: str, minimum: int, maximum: int) -> int:
    parsed = _strict_int(value, field_name)
    if not minimum <= parsed <= maximum:
        raise ValueError(f"{field_name} is outside its supported domain")
    return parsed


def _mapping(value: object, field_name: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be an object")
    if any(type(key) is not str for key in value):
        raise ValueError(f"{field_name} keys must be strings")
    return value


def _range_cardinality(bounds: Mapping[str, int]) -> int:
    return ((bounds["max"] - bounds["min"]) // bounds["step"]) + 1


def _input_digest(request: CompiledGaProfileRequest) -> str:
    payload = request.model_dump(mode="python")
    binding = payload["ga_binding"]
    assert isinstance(binding, dict)
    binding.pop("input_digest")
    evolution = payload["evolution"]
    assert isinstance(evolution, dict)
    evolution.pop("epoch_id")
    return sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _canonical_json(value: object) -> str:
    return json.dumps(
        _canonical_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _canonical_value(value: object) -> object:
    if isinstance(value, Decimal):
        return canonical_decimal_text(value)
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, BaseModel):
        return _canonical_value(value.model_dump(mode="python"))
    if isinstance(value, Mapping):
        return {str(key): _canonical_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_canonical_value(item) for item in value]
    return value
