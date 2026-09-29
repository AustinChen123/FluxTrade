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
_TERMINALS = "SCHEDULED_MTM LEGAL_NATIVE_LIQUIDATION_FINAL_EVENT O03_NON_ATOMIC_COMPLETE".split()
_FAILURES = "UNSUPPORTED_CONFIGURATION PERSISTENCE_FAILED CALLBACK_FAILED NATIVE_FAULT NATIVE_POISONED SCHEDULER_FAILED ENDPOINT_RECONCILIATION_FAILED ARTIFACT_WRITE_FAILED PUBLICATION_FAILED PUBLICATION_DURABILITY_UNKNOWN UNEXPECTED_EXCEPTION".split()


class ConfigurationContext(NamedTuple):
    schema_version: str
    config_id: str
    configuration_sha256: str
    products: tuple[str, ...]


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
    _object(row, "schema_version run_id run_contract_id registration_state requested_scenario_selector registration_failure input_contract_hashes planned_coverage " + " ".join(_IDENTITIES))
    _require(row["run_contract_id"] == "SPIDER_SYNTHETIC_P1_RUN_V1")
    _text(row["requested_scenario_selector"], r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
    state = _enum(row["registration_state"], ["VALIDATED", "REJECTED"])
    hashes, coverage = _list(row["input_contract_hashes"]), _list(row["planned_coverage"])
    if state == "REJECTED":
        _require(row["registration_failure"] == "UNSUPPORTED_CONFIGURATION")
        _require(all(row[key] is None for key in _IDENTITIES) and not hashes and not coverage)
        return
    _require(row["registration_failure"] is None)
    for key in _IDENTITIES:
        if key != "account_key":
            _text(row[key])
    _enum(row["profile_id"], ["SYNTHETIC_MIN_CASH_V1", "SYNTHETIC_P1_LIQUIDATION_V1", "SYNTHETIC_P1_O03_V1"])
    account = _object(row["account_key"], "venue environment account", "subaccount")
    for key, value in account.items():
        if key != "subaccount" or value is not None:
            _text(value)
    for key in _HASHES:
        _text(row[key], "[0-9a-f]{64}")
    _require(len(hashes) == len(_HASH_NAMES))
    for item, name in zip(hashes, _HASH_NAMES, strict=True):
        entry = _object(item, "name sha256")
        _require(entry["name"] == name)
        _text(entry["sha256"], "[0-9a-f]{64}")
    _require(row["ordering_contract_id"] == "S_order_v1")
    _require(row["cost_contract_id"] == "SPIDER_SYNTHETIC_COSTS_V1")
    _require(row["funding_exclusion"] == "SYNTHETIC_P1_NO_FUNDING_INPUT_OR_CLAIM")
    _require(row["artifact_encoding"] == "artifact_encoding_v1")
    _enum(row["terminal_policy"], _TERMINALS)
    seen: set[str] = set()
    for ordinal, item in enumerate(coverage, 1):
        entry = _object(item, "ordinal barrier_id record_kind")
        _require(_integer(entry["ordinal"]) == ordinal)
        barrier = _text(entry["barrier_id"])
        _require(barrier not in seen)
        seen.add(barrier)
        _enum(entry["record_kind"], _KINDS)


def _status(row: dict[str, object]) -> None:
    _object(row, "schema_version run_id state processed_boundary persisted_boundary failure_reason primary_failure")
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
    return canonical_bytes(value) + b"\n"


def decode_artifact(raw: bytes) -> dict[str, object]:
    rows = decode_jsonl(raw)
    _require(len(rows) == 1)
    validate_artifact(rows[0])
    return rows[0]
