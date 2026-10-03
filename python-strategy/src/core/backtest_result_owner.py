"""Canonical result values and input identity for completed backtests."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from typing import NamedTuple

from src.core.backtest_result_persistence import FullBacktestOutcome
from src.core.decimal_math import (
    canonical_decimal_text,
    decimal_from_fraction_significant,
    exact_decimal_add,
    exact_decimal_subtract,
)
from src.core.models import PositionSide
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


def _month(timestamp: int) -> str:
    date = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=timestamp)
    return f"{date.year:04d}-{date.month:02d}"


def _project_result(
    identity: BacktestResultRunIdentity,
    outcome: FullBacktestOutcome,
    *,
    completed_at: int,
) -> BacktestResultProjection:
    validate_run_identity(identity)
    if type(outcome) is not FullBacktestOutcome:
        raise ValueError("outcome must be a FullBacktestOutcome")
    if outcome.initial_balance != identity.initial_balance:
        raise ValueError("outcome initial_balance must match run identity")
    _timestamp(completed_at, "completed_at")
    summary = _summary(identity, outcome)
    equity = _project_equity(identity, outcome)
    trades = _project_trades(identity, outcome)
    monthly = _project_monthly(identity, outcome)
    distribution = _project_distribution(outcome)
    input_json = json.dumps(
        canonical_input_payload(identity),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    result_json = json.dumps(
        _result_payload(summary, equity, trades, monthly, distribution),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return BacktestResultProjection(
        identity=identity,
        completed_at=completed_at,
        summary=summary,
        equity=equity,
        closed_trades=trades,
        monthly_returns=monthly,
        pnl_distribution=distribution,
        input_digest=hashlib.sha256(input_json.encode("utf-8")).hexdigest(),
        result_digest=hashlib.sha256(result_json.encode("utf-8")).hexdigest(),
        input_payload_json=input_json,
        result_payload_json=result_json,
    )


def _summary(
    identity: BacktestResultRunIdentity,
    outcome: FullBacktestOutcome,
) -> BacktestResultSummaryValues:
    if outcome.max_drawdown < 0:
        raise ValueError("max_drawdown must be nonnegative")
    return BacktestResultSummaryValues(
        identity.initial_balance,
        outcome.total_pnl,
        outcome.mark_to_market_pnl,
        decimal_from_fraction_significant(
            Fraction(outcome.mark_to_market_pnl) / Fraction(identity.initial_balance),
            precision=28,
        ),
        outcome.max_drawdown,
        outcome.trade_sharpe,
        outcome.sortino_ratio,
        outcome.calmar_ratio,
    )


def _project_equity(
    identity: BacktestResultRunIdentity,
    outcome: FullBacktestOutcome,
) -> tuple[EquitySampleValues, ...]:
    peak = identity.initial_balance
    previous: int | None = None
    rows: list[EquitySampleValues] = []
    for sequence, (timestamp, equity) in enumerate(outcome.equity_samples):
        _timestamp(timestamp, "equity sample timestamp")
        _decimal(equity, "equity")
        if not identity.start <= timestamp <= identity.end:
            raise ValueError("equity sample is outside requested window")
        if previous is not None and timestamp <= previous:
            raise ValueError("equity samples must be strictly increasing")
        peak = max(peak, equity)
        rows.append(
            EquitySampleValues(
                sequence,
                timestamp,
                equity,
                exact_decimal_subtract(peak, equity),
            )
        )
        previous = timestamp
    return tuple(rows)


def _project_trades(
    identity: BacktestResultRunIdentity,
    outcome: FullBacktestOutcome,
) -> tuple[ClosedTradeValues, ...]:
    rows: list[ClosedTradeValues] = []
    for sequence, trade in enumerate(outcome.closed_trades):
        entry = _timestamp(trade.entry_time, "trade entry_time")
        exit_time = _timestamp(trade.exit_time, "trade exit_time")
        if not identity.start <= entry <= exit_time <= identity.end:
            raise ValueError("trade timestamps must fit requested window")
        if trade.side not in (PositionSide.LONG, PositionSide.SHORT):
            raise ValueError("trade side must be LONG or SHORT")
        quantity = _decimal(trade.quantity, "trade quantity")
        if quantity <= 0:
            raise ValueError("trade quantity must be positive")
        rows.append(
            ClosedTradeValues(
                sequence,
                entry,
                exit_time,
                trade.side.value,
                quantity,
                _decimal(trade.entry_price, "entry_price"),
                _decimal(trade.exit_price, "exit_price"),
                _decimal(trade.fee, "fee"),
                _decimal(trade.pnl, "pnl"),
            )
        )
    return tuple(rows)


def _project_monthly(
    identity: BacktestResultRunIdentity,
    outcome: FullBacktestOutcome,
) -> tuple[MonthlyReturnValues, ...]:
    totals: dict[str, Decimal] = {}
    for trade in outcome.closed_trades:
        month = _month(trade.exit_time)
        totals[month] = exact_decimal_add(totals.get(month, Decimal(0)), trade.pnl)
    return tuple(
        MonthlyReturnValues(
            month,
            decimal_from_fraction_significant(
                Fraction(total) / Fraction(identity.initial_balance), precision=28
            ),
        )
        for month, total in sorted(totals.items())
    )


def _project_distribution(
    outcome: FullBacktestOutcome,
) -> tuple[PnlDistributionValues, ...]:
    if not outcome.closed_trades:
        return ()
    losses = sum(trade.pnl < 0 for trade in outcome.closed_trades)
    return (
        PnlDistributionValues(0, None, Decimal(0), losses),
        PnlDistributionValues(1, Decimal(0), None, len(outcome.closed_trades) - losses),
    )


def _result_payload(
    summary: BacktestResultSummaryValues,
    equity: tuple[EquitySampleValues, ...],
    trades: tuple[ClosedTradeValues, ...],
    monthly: tuple[MonthlyReturnValues, ...],
    distribution: tuple[PnlDistributionValues, ...],
) -> dict[str, object]:
    return {
        "summary": {
            name: canonical_decimal_text(getattr(summary, name))
            for name in (
                "initial_balance",
                "total_pnl",
                "net_pnl",
                "return_pct",
                "max_drawdown",
                "sharpe",
                "sortino",
                "calmar",
            )
        },
        "equity": [
            {
                "sequence": row.sequence,
                "timestamp": row.timestamp,
                "equity": canonical_decimal_text(row.equity),
                "drawdown": canonical_decimal_text(row.drawdown),
            }
            for row in equity
        ],
        "closed_trades": [
            {
                "sequence": row.sequence,
                "entry_time": row.entry_time,
                "exit_time": row.exit_time,
                "side": row.side,
                "quantity": canonical_decimal_text(row.quantity),
                "entry_price": canonical_decimal_text(row.entry_price),
                "exit_price": canonical_decimal_text(row.exit_price),
                "fee": canonical_decimal_text(row.fee),
                "pnl": canonical_decimal_text(row.pnl),
            }
            for row in trades
        ],
        "monthly_returns": [
            {"month": row.month, "return_pct": canonical_decimal_text(row.return_pct)}
            for row in monthly
        ],
        "pnl_distribution": [
            {
                "sequence": row.sequence,
                "lower": None
                if row.lower is None
                else canonical_decimal_text(row.lower),
                "upper": None
                if row.upper is None
                else canonical_decimal_text(row.upper),
                "count": row.count,
            }
            for row in distribution
        ],
    }
