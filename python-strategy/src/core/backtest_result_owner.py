"""Canonical result values and input identity for completed backtests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import NamedTuple

from src.core.decimal_math import canonical_decimal_text
from src.core.product_registry import CapitalModel, FeeModel, InstrumentSpec, MarketType


@dataclass(frozen=True, slots=True)
class BacktestResultRunIdentity:
    job_id: str
    strategy_id: str
    artifact_version: str
    catalog_sha256: str
    dataset_id: str
    dataset_checksum_sha256: str
    product_id: str
    timeframe: str
    currency: str
    start: int
    end: int
    initial_balance: Decimal
    instrument: InstrumentSpec
    maker_fee: Decimal
    taker_fee: Decimal
    drawdown_limit: Decimal | None
    execution_timeframe: str | None


def _decimal(value: object, name: str) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise ValueError(f"{name} must be a finite Decimal")
    return value


def _timestamp(value: object, name: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a nonnegative integer UTC millisecond")
    try:
        _ = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)
    except OverflowError as exc:
        raise ValueError(f"{name} is outside UTC year 1..9999") from exc
    return value


def _instrument_payload(instrument: InstrumentSpec) -> dict[str, object]:
    if type(instrument) is not InstrumentSpec:
        raise ValueError("instrument must be an InstrumentSpec")
    decimal_fields = {
        "quantity_step",
        "price_tick",
        "min_notional",
        "min_quantity",
        "multiplier",
        "tick_value",
        "capital_per_contract",
    }
    text_fields = {"product_id", "exchange", "symbol", "base", "quote"}
    enum_fields = {
        "fee_model": FeeModel,
        "capital_model": CapitalModel,
        "market_type": MarketType,
    }
    payload: dict[str, object] = {}
    for field in fields(instrument):
        value = getattr(instrument, field.name)
        if field.name in decimal_fields:
            if value is None:
                payload[field.name] = None
            else:
                payload[field.name] = canonical_decimal_text(
                    _decimal(value, field.name)
                )
        elif field.name in enum_fields:
            expected_enum = enum_fields[field.name]
            if value is not None and type(value) is not expected_enum:
                raise ValueError(f"unsupported instrument enum field: {field.name}")
            payload[field.name] = None if value is None else value.value
        elif field.name in text_fields:
            if type(value) is not str:
                raise ValueError(f"instrument field must be text: {field.name}")
            payload[field.name] = value
        elif field.name == "session_calendar_id":
            if value is not None and type(value) is not str:
                raise ValueError("session_calendar_id must be text or null")
            payload[field.name] = value
        else:
            raise ValueError(f"unsupported instrument field: {field.name}")
    return payload


def validate_run_identity(identity: BacktestResultRunIdentity) -> None:
    if type(identity) is not BacktestResultRunIdentity:
        raise ValueError("identity must be a BacktestResultRunIdentity")
    for name in (
        "job_id",
        "strategy_id",
        "artifact_version",
        "dataset_id",
        "product_id",
        "timeframe",
        "currency",
    ):
        value = getattr(identity, name)
        if type(value) is not str or not value.strip():
            raise ValueError(f"{name} must be nonempty text")
    for name in ("catalog_sha256", "dataset_checksum_sha256"):
        value = getattr(identity, name)
        if (
            type(value) is not str
            or len(value) != 64
            or any(char not in "0123456789abcdef" for char in value)
        ):
            raise ValueError(f"{name} must be lowercase SHA-256 hex")
    if _timestamp(identity.start, "start") > _timestamp(identity.end, "end"):
        raise ValueError("requested window is reversed")
    if _decimal(identity.initial_balance, "initial_balance") <= 0:
        raise ValueError("initial_balance must be positive")
    if type(identity.instrument) is not InstrumentSpec:
        raise ValueError("instrument must be an InstrumentSpec")
    if identity.instrument.product_id != identity.product_id:
        raise ValueError("instrument product_id does not match run identity")
    if identity.instrument.quote and identity.currency != identity.instrument.quote:
        raise ValueError("currency must match instrument quote")
    if _decimal(identity.maker_fee, "maker_fee") < 0:
        raise ValueError("maker_fee must be nonnegative")
    if _decimal(identity.taker_fee, "taker_fee") < 0:
        raise ValueError("taker_fee must be nonnegative")
    if (
        identity.drawdown_limit is not None
        and _decimal(identity.drawdown_limit, "drawdown_limit") < 0
    ):
        raise ValueError("drawdown_limit must be nonnegative")
    if identity.execution_timeframe is not None and (
        type(identity.execution_timeframe) is not str
        or not identity.execution_timeframe.strip()
    ):
        raise ValueError("execution_timeframe must be nonempty text or null")
    _instrument_payload(identity.instrument)


def canonical_input_payload(identity: BacktestResultRunIdentity) -> dict[str, object]:
    """Return the exact version-one identity payload after strict validation."""
    validate_run_identity(identity)
    return {
        "subject": {
            "kind": "STRATEGY_ARTIFACT",
            "id": f"{identity.strategy_id}:{identity.artifact_version}",
            "digest": identity.catalog_sha256,
        },
        "dataset": {
            "id": identity.dataset_id,
            "checksum_sha256": identity.dataset_checksum_sha256,
        },
        "product_id": identity.product_id,
        "timeframe": identity.timeframe,
        "currency": identity.currency,
        "start": identity.start,
        "end": identity.end,
        "initial_balance": canonical_decimal_text(identity.initial_balance),
        "instrument": _instrument_payload(identity.instrument),
        "fees": {
            "maker": canonical_decimal_text(identity.maker_fee),
            "taker": canonical_decimal_text(identity.taker_fee),
        },
        "drawdown_limit": (
            None
            if identity.drawdown_limit is None
            else canonical_decimal_text(identity.drawdown_limit)
        ),
        "execution_timeframe": identity.execution_timeframe,
    }


def canonical_input_digest(identity: BacktestResultRunIdentity) -> str:
    encoded = json.dumps(
        canonical_input_payload(identity),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class BacktestResultSummaryValues(NamedTuple):
    initial_balance: Decimal
    total_pnl: Decimal
    net_pnl: Decimal
    return_pct: Decimal
    max_drawdown: Decimal
    sharpe: Decimal
    sortino: Decimal
    calmar: Decimal


class EquitySampleValues(NamedTuple):
    sequence: int
    timestamp: int
    equity: Decimal
    drawdown: Decimal


class ClosedTradeValues(NamedTuple):
    sequence: int
    entry_time: int
    exit_time: int
    side: str
    quantity: Decimal
    entry_price: Decimal
    exit_price: Decimal
    fee: Decimal
    pnl: Decimal


class MonthlyReturnValues(NamedTuple):
    month: str
    return_pct: Decimal


@dataclass(frozen=True, slots=True)
class PnlDistributionValues:
    sequence: int
    lower: Decimal | None
    upper: Decimal | None
    count: int


@dataclass(frozen=True, slots=True)
class BacktestResultProjection:
    identity: BacktestResultRunIdentity
    completed_at: int
    summary: BacktestResultSummaryValues
    equity: tuple[EquitySampleValues, ...]
    closed_trades: tuple[ClosedTradeValues, ...]
    monthly_returns: tuple[MonthlyReturnValues, ...]
    pnl_distribution: tuple[PnlDistributionValues, ...]
    input_digest: str
    result_digest: str
    input_payload_json: str
    result_payload_json: str
