"""Pure artifact_encoding_v1 codec and closed attempt/status structures.

Canonical JSON helpers do not validate artifact semantics. Only
validate_artifact validates the two schemas implemented here; nothing admits a run.
"""

import json
import re
from collections.abc import Sequence
from decimal import Decimal
from typing import NamedTuple, cast

from src.core.decimal_math import canonical_decimal_text

_IDENTITIES = "profile_id account_key scenario_plan_id scenario_plan_sha256 program_sha256 native_artifact_sha256 policy_source_sha256 ordering_contract_id cost_contract_id funding_exclusion terminal_policy artifact_encoding".split()
_HASHES = "scenario_plan_sha256 program_sha256 native_artifact_sha256 policy_source_sha256".split()
_HASH_NAMES = "SCENARIO_PLAN PROGRAM NATIVE_ARTIFACT POLICY_SOURCE_MANIFEST".split()
_KINDS = "SOURCE_GROUP_RESULT SNAPSHOT_FACT DELIVERY_ATTEMPT CALLBACK_RESULT".split()
_P3_COVERAGE_KINDS = "HISTORICAL_MARKET_STEP HISTORICAL_MARKET_CACHE HISTORICAL_TIMER".split()
_TERMINALS = "SCHEDULED_MTM LEGAL_NATIVE_LIQUIDATION_FINAL_EVENT O03_NON_ATOMIC_COMPLETE".split()
_FAILURES = "UNSUPPORTED_CONFIGURATION PERSISTENCE_FAILED CALLBACK_FAILED NATIVE_FAULT NATIVE_POISONED SCHEDULER_FAILED ENDPOINT_RECONCILIATION_FAILED ARTIFACT_WRITE_FAILED PUBLICATION_FAILED PUBLICATION_DURABILITY_UNKNOWN UNEXPECTED_EXCEPTION".split()
_P1_RUN_CONTRACT = "SPIDER_SYNTHETIC_P1_RUN_V1"
_P2_RUN_CONTRACT = "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1"
_P3_RUN_CONTRACT = "SPIDER_HISTORICAL_RESEARCH_RUN_V1"
_P3_SELECTOR = "SPIDER_HISTORICAL_RESEARCH_RUN_V1"
_HISTORICAL_MODELS = ("OHLC4_OPEN_HIGH_LOW_CLOSE_V1", "OHLC4_OPEN_LOW_HIGH_CLOSE_V1")


class ConfigurationContext(NamedTuple):
    schema_version: str
    config_id: str
    configuration_sha256: str
    products: tuple[str, ...]


class HistoricalContext(NamedTuple):
    schema_version: str
    research_classification: str
    historical_input_sha256: str
    path_pair_sha256: str
    source_sha256: str
    model_sha256: str
    assumption_sha256: str
    coverage_sha256: str
    model_id: str
    model_version: int


def _require(condition: bool) -> None:
    if not condition:
        raise ValueError("INVALID_ARTIFACT")


def _object(value: object, required: str, optional: str = "") -> dict[str, object]:
    _require(type(value) is dict)
    row = cast(dict[str, object], value)
    keys = set(required.split())
    _require(keys <= row.keys() and row.keys() <= keys | set(optional.split()))
    return row


def _text(value: object, pattern: str | None = None) -> str:
    _require(type(value) is str)
    text = cast(str, value)
    _require(bool(text) and text.strip() == text)
    _require(pattern is None or re.fullmatch(pattern, text) is not None)
    return text


def _integer(value: object) -> int:
    _require(type(value) is int)
    number = cast(int, value)
    _require(number >= 0)
    return number


def _boolean(value: object) -> bool:
    _require(type(value) is bool)
    return cast(bool, value)


def _list(value: object) -> list[object]:
    _require(type(value) is list)
    return cast(list[object], value)


def _enum(value: object, choices: Sequence[str]) -> str:
    text = _text(value)
    _require(text in choices)
    return text


def configuration_context(value: object) -> ConfigurationContext:
    """Validate and detach configured-product identity without retaining config."""
    if type(value) is ConfigurationContext:
        context = cast(ConfigurationContext, value)
        schema_version = context.schema_version
        config_id = context.config_id
        configuration_sha256 = context.configuration_sha256
        raw_products: object = context.products
        _require(type(raw_products) is tuple)
    else:
        row = _object(value, "schema_version config_id configuration_sha256 products")
        schema_version = row["schema_version"]
        config_id = row["config_id"]
        configuration_sha256 = row["configuration_sha256"]
        raw_products = row["products"]
        _require(type(raw_products) is list)
    _enum(schema_version, ["spider_configuration_context_v1"])
    validated_config_id = _text(config_id)
    validated_hash = _text(configuration_sha256, r"[0-9a-f]{64}")
    products = tuple(_text(product) for product in cast(Sequence[object], raw_products))
    _require(bool(products) and len(products) == len(set(products)))
    if type(value) is ConfigurationContext:
        return value
    return ConfigurationContext(
        cast(str, schema_version), validated_config_id, validated_hash, products
    )


def _configuration_context(value: object) -> dict[str, object]:
    context = configuration_context(value)
    return {
        "schema_version": context.schema_version,
        "config_id": context.config_id,
        "configuration_sha256": context.configuration_sha256,
        "products": list(context.products),
    }


def historical_context(value: object) -> HistoricalContext:
    """Validate and detach the frozen historical evidence identity context."""
    if type(value) is HistoricalContext:
        context = cast(HistoricalContext, value)
        row: dict[str, object] = context._asdict()
    else:
        row = _object(value, "schema_version research_classification historical_input_sha256 path_pair_sha256 source_sha256 model_sha256 assumption_sha256 coverage_sha256 model_id model_version")
    _enum(row["schema_version"], ["spider_historical_context_v1"])
    _enum(row["research_classification"], ["RESEARCH_ONLY"])
    for field in ("historical_input_sha256", "path_pair_sha256", "source_sha256", "model_sha256", "assumption_sha256", "coverage_sha256"):
        _text(row[field], r"[0-9a-f]{64}")
    model_id = _enum(row["model_id"], _HISTORICAL_MODELS)
    model_version = row["model_version"]
    _require(type(model_version) is int and model_version == 1)
    if type(value) is HistoricalContext:
        return value
    return HistoricalContext(
        cast(str, row["schema_version"]), cast(str, row["research_classification"]),
        cast(str, row["historical_input_sha256"]), cast(str, row["path_pair_sha256"]),
        cast(str, row["source_sha256"]), cast(str, row["model_sha256"]),
        cast(str, row["assumption_sha256"]), cast(str, row["coverage_sha256"]),
        model_id, cast(int, model_version),
    )


def _historical_context(value: object) -> dict[str, object]:
    return historical_context(value)._asdict()


def _artifact_contexts(value: object) -> tuple[ConfigurationContext | None, HistoricalContext | None]:
    _require(type(value) is dict)
    row = cast(dict[str, object], value)
    configuration = configuration_context(row["configuration_context"]) if "configuration_context" in row else None
    historical = historical_context(row["historical_context"]) if "historical_context" in row else None
    _require(historical is None or configuration is not None)
    return configuration, historical


def _terminal_policy(value: object, *, historical: bool) -> None:
    if historical:
        _require(value == "MTM_PRESERVE_OPEN_V1")
    else:
        _enum(value, _TERMINALS)


def _optional_context(row: dict[str, object]) -> dict[str, object] | None:
    if "configuration_context" not in row:
        return None
    return _configuration_context(row["configuration_context"])


def _optional_historical_context(row: dict[str, object]) -> dict[str, object] | None:
    if "historical_context" not in row:
        return None
    return _historical_context(row["historical_context"])


def _decimal_text(value: object) -> str:
    text = _text(value, r"-?(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?")
    _require(text != "-0")
    return text


def _json_value(value: object) -> object:
    if type(value) is Decimal:
        return canonical_decimal_text(cast(Decimal, value))
    if type(value) is dict:
        row = cast(dict[object, object], value)
        _require(all(type(key) is str for key in row))
        return {cast(str, key): _json_value(item) for key, item in row.items()}
    if type(value) is list:
        return [_json_value(item) for item in cast(list[object], value)]
    _require(value is None or type(value) in (str, int, bool))
    return value


def canonical_bytes(value: object) -> bytes:
    """Encode a semantic JSON value without a record terminator."""
    return json.dumps(_json_value(value), sort_keys=True, ensure_ascii=True, separators=(",", ":")).encode("ascii")


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    row: dict[str, object] = {}
    for key, value in pairs:
        _require(key not in row)
        row[key] = value
    return row


def _invalid_number(value: str) -> object:
    raise ValueError("INVALID_ARTIFACT")


def decode_canonical(raw: bytes) -> object:
    """Decode canonical semantic bytes; Decimal strings remain strings."""
    _require(type(raw) is bytes)
    value: object = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs,
                               parse_float=_invalid_number, parse_constant=_invalid_number)
    _require(canonical_bytes(value) == raw)
    return value


def encode_jsonl(rows: list[object]) -> bytes:
    """Frame JSON objects only; the caller separately validates each schema."""
    encoded: list[bytes] = []
    for row in _list(rows):
        _require(type(row) is dict)
        encoded.append(canonical_bytes(row) + b"\n")
    return b"".join(encoded)


def decode_jsonl(raw: bytes) -> list[dict[str, object]]:
    """Reject blank/partial lines; perform no artifact-schema dispatch."""
    _require(type(raw) is bytes and (not raw or raw.endswith(b"\n")))
    rows: list[dict[str, object]] = []
    for line in raw.split(b"\n")[:-1]:
        value = decode_canonical(line)
        _require(type(value) is dict)
        rows.append(cast(dict[str, object], value))
    return rows


def _boundary(value: object) -> None:
    if value is not None:
        row = _object(value, "ordinal barrier_id journal_seq")
        _require(_integer(row["ordinal"]) > 0 and _integer(row["journal_seq"]) > 0)
        _text(row["barrier_id"])


def _attempt(row: dict[str, object]) -> None:
    _object(row, "schema_version run_id run_contract_id registration_state requested_scenario_selector registration_failure input_contract_hashes planned_coverage " + " ".join(_IDENTITIES), "configuration_context historical_context")
    context = _optional_context(row)
    historical = _optional_historical_context(row)
    run_contract = _enum(row["run_contract_id"], [_P1_RUN_CONTRACT, _P2_RUN_CONTRACT, _P3_RUN_CONTRACT])
    _text(row["requested_scenario_selector"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
    state = _enum(row["registration_state"], ["VALIDATED", "REJECTED"])
    hashes, coverage = _list(row["input_contract_hashes"]), _list(row["planned_coverage"])
    if state == "REJECTED":
        _require(row["registration_failure"] == "UNSUPPORTED_CONFIGURATION")
        _require(all(row[key] is None for key in _IDENTITIES) and not hashes and not coverage)
        _require(run_contract != _P3_RUN_CONTRACT and historical is None)
        _require(run_contract == _P2_RUN_CONTRACT or context is None)
        return
    _require(row["registration_failure"] is None)
    if run_contract == _P1_RUN_CONTRACT:
        _require(context is None and historical is None)
    elif run_contract == _P2_RUN_CONTRACT:
        _require(context is not None)
        _require(historical is None)
    else:
        _require(context is not None and historical is not None)
    for key in _IDENTITIES:
        if key != "account_key":
            _text(row[key])
    if run_contract == _P1_RUN_CONTRACT:
        _enum(row["profile_id"], ["SYNTHETIC_MIN_CASH_V1", "SYNTHETIC_P1_LIQUIDATION_V1", "SYNTHETIC_P1_O03_V1"])
    elif run_contract == _P2_RUN_CONTRACT:
        _require(row["profile_id"] == "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1")
    else:
        _require(row["profile_id"] == "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1")
    account = _object(row["account_key"], "venue environment account", "subaccount")
    for key, value in account.items():
        if key != "subaccount" or value is not None:
            _text(value)
    if run_contract == _P3_RUN_CONTRACT:
        _require(account["venue"] == "SPIDER_HISTORICAL_RESEARCH")
        _require(account["environment"] == "RESEARCH_ONLY")
        _require("subaccount" not in account or account["subaccount"] is None)
        assert historical is not None
        _require(row["requested_scenario_selector"] == _P3_SELECTOR)
        _require(row["scenario_plan_id"] == _P3_SELECTOR)
        _require(row["scenario_plan_sha256"] == historical["historical_input_sha256"])
    for key in _HASHES:
        _text(row[key], "[0-9a-f]{64}")
    _require(len(hashes) == len(_HASH_NAMES))
    for item, name in zip(hashes, _HASH_NAMES, strict=True):
        entry = _object(item, "name sha256")
        _require(entry["name"] == name)
        _text(entry["sha256"], "[0-9a-f]{64}")
    expected_ordering = "HISTORICAL_ORDER_V1" if run_contract == _P3_RUN_CONTRACT else "S_order_v1"
    expected_cost = "SPIDER_HISTORICAL_RESEARCH_COSTS_V1" if run_contract == _P3_RUN_CONTRACT else "SPIDER_SYNTHETIC_COSTS_V1"
    _require(row["ordering_contract_id"] == expected_ordering)
    _require(row["cost_contract_id"] == expected_cost)
    expected_funding = (
        "HISTORICAL_FUNDING_DISABLED" if run_contract == _P3_RUN_CONTRACT
        else "SYNTHETIC_P1_NO_FUNDING_INPUT_OR_CLAIM" if run_contract == _P1_RUN_CONTRACT
        else "SYNTHETIC_P2_NO_FUNDING_INPUT_OR_CLAIM"
    )
    _require(row["funding_exclusion"] == expected_funding)
    _require(row["artifact_encoding"] == "artifact_encoding_v1")
    if run_contract == _P3_RUN_CONTRACT:
        _require(row["terminal_policy"] == "MTM_PRESERVE_OPEN_V1")
        _require(bool(coverage))
    else:
        _enum(row["terminal_policy"], _TERMINALS)
    seen: set[str] = set()
    for ordinal, item in enumerate(coverage, 1):
        entry = _object(item, "ordinal barrier_id record_kind")
        _require(_integer(entry["ordinal"]) == ordinal)
        barrier = _text(entry["barrier_id"])
        _require(barrier not in seen)
        seen.add(barrier)
        _enum(entry["record_kind"], _P3_COVERAGE_KINDS if run_contract == _P3_RUN_CONTRACT else _KINDS)


def _status(row: dict[str, object]) -> None:
    _object(row, "schema_version run_id state processed_boundary persisted_boundary failure_reason primary_failure", "configuration_context historical_context")
    configuration = _optional_context(row)
    historical = _optional_historical_context(row)
    _require(historical is None or configuration is not None)
    state = _enum(row["state"], ["RUNNING", "FAILED", "COMPLETE"])
    _boundary(row["processed_boundary"])
    _boundary(row["persisted_boundary"])
    if state != "FAILED":
        _require(row["failure_reason"] is None and row["primary_failure"] is None)
        return
    _enum(row["failure_reason"], _FAILURES)
    if row["primary_failure"] is not None:
        failure = _object(row["primary_failure"], "kind reason")
        _enum(failure["kind"], ["NATIVE", "CALLBACK", "SCHEDULER", "PERSISTENCE", "PUBLICATION"])
        _text(failure["reason"], r"[A-Z][A-Z0-9_]{0,127}")


def validate_artifact(value: object) -> None:
    """Validate attempt/status structure, not protocol equality or admission."""
    _require(type(value) is dict)
    row = cast(dict[str, object], value)
    _text(row.get("run_id"), r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    schema = _enum(row.get("schema_version"), ["spider_attempt_v1", "spider_status_v1"])
    (_attempt if schema == "spider_attempt_v1" else _status)(row)


def encode_artifact(value: object) -> bytes:
    validate_artifact(value)
    serialized = value
    row = cast(dict[str, object], value)
    if "configuration_context" in row:
        serialized = {**row, "configuration_context": _configuration_context(row["configuration_context"])}
    if "historical_context" in row:
        serialized = {**cast(dict[str, object], serialized), "historical_context": _historical_context(row["historical_context"])}
    return canonical_bytes(serialized) + b"\n"


def decode_artifact(raw: bytes) -> dict[str, object]:
    rows = decode_jsonl(raw)
    _require(len(rows) == 1)
    validate_artifact(rows[0])
    return rows[0]
