"""Pure closed-host input and pre-owner admission for paid Spider research."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import cast

from src.core.backtest.spider_historical_input import (
    HistoricalInputError,
    HistoricalRunInput,
    decode_historical_run_input,
    encode_historical_run_input,
    validate_historical_input,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical


SCHEMA_VERSION = "spider_execution_input_v1"
POLICY_ID = "SPIDER_HOST_ORIGINAL_PAID_V1"
ENDPOINT_POLICY_ID = "PAID_CLOSE_RESEARCH_V1"
BASE_POLICY_ID = "SPIDER_GRID_ORIGINAL_V1"
BASE_POLICY_VERSION = "1"
BASE_POLICY_SOURCE_SHA256 = "4dbf2bdbee87d4100004fe5b14a93f597e64a9bca6525ed2902afd3476ee40ec"
_KEYS = frozenset({
    "schema_version", "policy_id", "endpoint_policy_id", "historical_input",
    "policy_end_ms", "generation",
})


@dataclass(frozen=True, slots=True)
class SpiderExecutionInput:
    schema_version: str
    policy_id: str
    endpoint_policy_id: str
    historical_input: HistoricalRunInput
    policy_end_ms: int
    generation: int = 0


def _object(value: object, label: str) -> dict[str, object]:
    if type(value) is not dict or set(cast(dict[object, object], value)) != _KEYS:
        raise HistoricalInputError(f"invalid {label} shape")
    return cast(dict[str, object], value)


def _admit_initial_ownership(run: HistoricalRunInput) -> None:
    state = decode_canonical(run.initial_account_state)
    if type(state) is not dict:
        raise HistoricalInputError("invalid initial account state")
    row = cast(dict[str, object], state)
    orders = row.get("orders")
    positions = row.get("positions")
    if type(orders) is not list or type(positions) is not list:
        raise HistoricalInputError("invalid initial account ownership state")
    for order_value in cast(list[object], orders):
        if type(order_value) is not dict or cast(dict[str, object], order_value).get("strategy_id") != run.strategy_identity:
            raise HistoricalInputError("initial order ownership mismatch")
    for position_value in cast(list[object], positions):
        if type(position_value) is not dict:
            raise HistoricalInputError("invalid initial position ownership")
        lots = cast(dict[str, object], position_value).get("lots")
        if type(lots) is not list or not lots:
            raise HistoricalInputError("invalid initial position lots")
        for lot_value in cast(list[object], lots):
            if type(lot_value) is not dict or cast(dict[str, object], lot_value).get("strategy_id") != run.strategy_identity:
                raise HistoricalInputError("initial position lot ownership mismatch")


def validate_spider_execution_input(value: SpiderExecutionInput) -> str:
    """Validate the exact closed host contract and return its semantic SHA-256."""
    if type(value) is not SpiderExecutionInput:
        raise HistoricalInputError("invalid Spider execution input DTO")
    if (value.schema_version != SCHEMA_VERSION or value.policy_id != POLICY_ID
            or value.endpoint_policy_id != ENDPOINT_POLICY_ID):
        raise HistoricalInputError("unsupported Spider execution identity")
    if type(value.policy_end_ms) is not int or type(value.generation) is not int or value.generation != 0:
        raise HistoricalInputError("invalid policy cutoff or generation")
    run = value.historical_input
    base_hash = validate_historical_input(run)
    if (run.policy_id != BASE_POLICY_ID or run.policy_version != BASE_POLICY_VERSION
            or run.policy_source_sha256 != BASE_POLICY_SOURCE_SHA256):
        raise HistoricalInputError("unsupported original P3 policy identity")
    if not run.range_start_ms <= value.policy_end_ms < run.range_end_ms:
        raise HistoricalInputError("policy cutoff outside historical settlement range")
    _admit_initial_ownership(run)
    projection = {
        "schema_version": value.schema_version,
        "policy_id": value.policy_id,
        "endpoint_policy_id": value.endpoint_policy_id,
        "historical_input_sha256": base_hash,
        "policy_end_ms": value.policy_end_ms,
        "generation": value.generation,
    }
    return sha256(canonical_bytes(projection)).hexdigest()


def encode_spider_execution_input(value: SpiderExecutionInput) -> bytes:
    """Encode the wrapper with the existing P3 canonical object embedded."""
    validate_spider_execution_input(value)
    base = decode_canonical(encode_historical_run_input(value.historical_input))
    return canonical_bytes({
        "schema_version": value.schema_version,
        "policy_id": value.policy_id,
        "endpoint_policy_id": value.endpoint_policy_id,
        "historical_input": base,
        "policy_end_ms": value.policy_end_ms,
        "generation": value.generation,
    })


def decode_spider_execution_input(raw: bytes) -> SpiderExecutionInput:
    """Decode exact wrapper JSON, reuse the immutable P3 decoder, and recheck bytes."""
    try:
        decoded = decode_canonical(raw)
        row = _object(decoded, "Spider execution input")
        if (row["schema_version"] != SCHEMA_VERSION or row["policy_id"] != POLICY_ID
                or row["endpoint_policy_id"] != ENDPOINT_POLICY_ID):
            raise HistoricalInputError("unsupported Spider execution identity")
        nested = row["historical_input"]
        if type(nested) is not dict:
            raise HistoricalInputError("historical_input must be a P3 JSON object")
        run = decode_historical_run_input(canonical_bytes(nested))
        cutoff = row["policy_end_ms"]
        generation = row["generation"]
        if type(cutoff) is not int or type(generation) is not int:
            raise HistoricalInputError("invalid policy cutoff or generation")
        value = SpiderExecutionInput(
            cast(str, row["schema_version"]), cast(str, row["policy_id"]),
            cast(str, row["endpoint_policy_id"]), run, cutoff, generation,
        )
        validate_spider_execution_input(value)
        if encode_spider_execution_input(value) != raw:
            raise HistoricalInputError("noncanonical Spider execution input encoding")
        return value
    except HistoricalInputError:
        raise
    except (TypeError, ValueError, UnicodeError) as error:
        raise HistoricalInputError("invalid Spider execution input encoding") from error
