"""Closed input identity for the source-derived floating-exit paid host."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from hashlib import sha256
from typing import cast

from src.core.backtest.spider_bayes_model import MODEL_ID
from src.core.backtest.spider_bayes_replay import (
    PRODUCTS,
    PROJECTION_ID,
    _prepare_closed15m_source14_validated,
    prepare_closed15m_source14,
)
from src.core.backtest.spider_execution_input import (
    SpiderExecutionInput,
    _decode_spider_execution_input_validated,
    _encode_spider_execution_input_validated,
    decode_spider_execution_input,
    encode_spider_execution_input,
    validate_spider_execution_input,
)
from src.core.backtest.spider_historical_input import (
    HistoricalInputError,
    ValidatedHistoricalInput,
    _decode_historical_run_input_candidate,
    _is_approved_historical_input_tree,
    decode_p2_configuration,
)
from src.core.backtest.spider_floating_exit_policy import (
    BAYES_REMAINING_COST_RISK_V1,
    FIXED_REMAINING_COST_TP_V1,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical


SCHEMA_VERSION = "spider_floating_exit_input_v1"
POLICY_ID = "SPIDER_SOURCE_FLOATING_EXIT_PAID_V1"
SOURCE_STUDY_SHA256 = "cc5bd45253f8d1f999c4437c944e5bf066fd8c61c30fbae64f0b68f47fb2da6c"
_DECISION_POLICIES = frozenset((FIXED_REMAINING_COST_TP_V1, BAYES_REMAINING_COST_RISK_V1))
_KEYS = frozenset({"schema_version", "policy_id", "decision_policy_id", "model_id", "execution_input"})
_EXPECTED_TAKER_FEE = Decimal("0.0006")


@dataclass(frozen=True, slots=True)
class SpiderFloatingExitInput:
    schema_version: str
    policy_id: str
    decision_policy_id: str
    model_id: str
    execution_input: SpiderExecutionInput


def _object(value: object) -> dict[str, object]:
    if type(value) is not dict or set(cast(dict[object, object], value)) != _KEYS:
        raise HistoricalInputError("invalid floating-exit input shape")
    return cast(dict[str, object], value)


def validate_spider_floating_exit_input(value: SpiderFloatingExitInput) -> str:
    """Validate candidate identity and all source/native inputs before owner creation."""
    return _validate_spider_floating_exit_input(value)


def _validate_spider_floating_exit_input(
    value: SpiderFloatingExitInput,
    execution_hash: str | None = None,
    token: ValidatedHistoricalInput | None = None,
) -> str:
    if type(value) is not SpiderFloatingExitInput:
        raise HistoricalInputError("invalid floating-exit input DTO")
    if (type(value.schema_version) is not str or value.schema_version != SCHEMA_VERSION
            or type(value.policy_id) is not str or value.policy_id != POLICY_ID
            or type(value.decision_policy_id) is not str
            or value.decision_policy_id not in _DECISION_POLICIES
            or type(value.model_id) is not str or value.model_id != MODEL_ID):
        raise HistoricalInputError("unsupported floating-exit identity")
    if execution_hash is None:
        execution_hash = validate_spider_execution_input(value.execution_input)
    run = value.execution_input.historical_input
    if token is not None and token.run is not run:
        raise HistoricalInputError("validated historical input token mismatch")
    projection = (
        _prepare_closed15m_source14_validated(token)
        if token is not None else prepare_closed15m_source14(run)
    )
    if projection.products != PRODUCTS:
        raise HistoricalInputError("unsupported source observation identity")
    configuration = decode_p2_configuration(
        run.configuration_bytes, run.configuration_sha256, run.ordered_products,
    )
    products = cast(list[dict[str, object]], configuration["products"])
    if len(products) != len(PRODUCTS) or any(
        product.get("product_id") != expected
        or product.get("taker_fee_rate") != str(_EXPECTED_TAKER_FEE)
        for product, expected in zip(products, PRODUCTS, strict=True)
    ):
        raise HistoricalInputError("unsupported configured taker fee profile")
    semantics = {
        "schema_version": value.schema_version,
        "policy_id": value.policy_id,
        "decision_policy_id": value.decision_policy_id,
        "model_id": value.model_id,
        "observation_projection_id": PROJECTION_ID,
        "source_study_sha256": SOURCE_STUDY_SHA256,
        "execution_input_sha256": execution_hash,
    }
    return sha256(canonical_bytes(semantics)).hexdigest()


def _encode_spider_floating_exit_input_validated(
    value: SpiderFloatingExitInput, token: ValidatedHistoricalInput,
) -> bytes:
    nested = decode_canonical(_encode_spider_execution_input_validated(value.execution_input, token))
    return canonical_bytes({
        "schema_version": value.schema_version,
        "policy_id": value.policy_id,
        "decision_policy_id": value.decision_policy_id,
        "model_id": value.model_id,
        "execution_input": nested,
    })


def encode_spider_floating_exit_input(value: SpiderFloatingExitInput) -> bytes:
    validate_spider_floating_exit_input(value)
    nested = decode_canonical(encode_spider_execution_input(value.execution_input))
    return canonical_bytes({
        "schema_version": value.schema_version,
        "policy_id": value.policy_id,
        "decision_policy_id": value.decision_policy_id,
        "model_id": value.model_id,
        "execution_input": nested,
    })


def decode_spider_floating_exit_input(raw: bytes) -> SpiderFloatingExitInput:
    try:
        row = _object(decode_canonical(raw))
        nested = row["execution_input"]
        if type(nested) is not dict:
            raise HistoricalInputError("execution_input must be a canonical paid object")
        value = SpiderFloatingExitInput(
            cast(str, row["schema_version"]), cast(str, row["policy_id"]),
            cast(str, row["decision_policy_id"]), cast(str, row["model_id"]),
            decode_spider_execution_input(canonical_bytes(nested)),
        )
        validate_spider_floating_exit_input(value)
        if encode_spider_floating_exit_input(value) != raw:
            raise HistoricalInputError("noncanonical floating-exit input encoding")
        return value
    except HistoricalInputError:
        raise
    except (TypeError, ValueError, UnicodeError) as error:
        raise HistoricalInputError("invalid floating-exit input encoding") from error


def _decode_spider_floating_exit_input_validated(
    raw: bytes,
) -> tuple[SpiderFloatingExitInput, ValidatedHistoricalInput | None, str, str | None]:
    try:
        row = _object(decode_canonical(raw))
        nested = row.get("execution_input")
        nested_paid = (
            cast(dict[str, object], nested).get("historical_input")
            if type(nested) is dict else None
        )
        candidate = None
        if type(nested_paid) is dict:
            try:
                candidate = _decode_historical_run_input_candidate(canonical_bytes(nested_paid))
            except Exception:
                pass
        if candidate is None or not _is_approved_historical_input_tree(candidate):
            value = decode_spider_floating_exit_input(raw)
            return value, None, validate_spider_floating_exit_input(value), None
        execution, token, execution_hash = _decode_spider_execution_input_validated(
            canonical_bytes(nested),
        )
        if token is None:
            value = decode_spider_floating_exit_input(raw)
            return value, None, validate_spider_floating_exit_input(value), None
        value = SpiderFloatingExitInput(
            cast(str, row["schema_version"]), cast(str, row["policy_id"]),
            cast(str, row["decision_policy_id"]), cast(str, row["model_id"]), execution,
        )
        contract_hash = _validate_spider_floating_exit_input(value, execution_hash, token)
        if _encode_spider_floating_exit_input_validated(value, token) != raw:
            raise HistoricalInputError("noncanonical floating-exit input encoding")
        return value, token, contract_hash, execution_hash
    except HistoricalInputError:
        raise
    except (TypeError, ValueError, UnicodeError) as error:
        raise HistoricalInputError("invalid floating-exit input encoding") from error


__all__ = [
    "SCHEMA_VERSION", "POLICY_ID", "SOURCE_STUDY_SHA256", "SpiderFloatingExitInput",
    "decode_spider_floating_exit_input", "encode_spider_floating_exit_input",
    "validate_spider_floating_exit_input",
]
