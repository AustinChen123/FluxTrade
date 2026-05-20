from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class BacktestJobRequest(BaseModel):
    """Request payload for a CSV-signal backtest job."""

    model_config = ConfigDict(populate_by_name=True)

    kind: Literal["csv_signal_backtest"] = "csv_signal_backtest"
    strategy_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    timeframe: str = Field(min_length=1)
    candles_csv_path: str = Field(min_length=1)
    signals_csv_path: str = Field(min_length=1)
    start_time: int
    end_time: int
    initial_balance: Decimal = Decimal("10000")
    maker_fee: Decimal = Decimal("0")
    taker_fee: Decimal = Decimal("0")
    write_reports: bool = False

    @field_validator("end_time")
    @classmethod
    def validate_time_range(cls, value: int, info) -> int:
        start_time = info.data.get("start_time")
        if start_time is not None and value < start_time:
            raise ValueError("end_time must be greater than or equal to start_time")
        return value

    @field_validator("candles_csv_path", "signals_csv_path")
    @classmethod
    def validate_path(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("path cannot be blank")
        return str(Path(value))

    @field_validator("initial_balance")
    @classmethod
    def validate_initial_balance(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("initial_balance must be positive")
        return value

    @field_validator("maker_fee", "taker_fee")
    @classmethod
    def validate_fee(cls, value: Decimal) -> Decimal:
        if value < 0:
            raise ValueError("fee cannot be negative")
        return value


class JobRecord(BaseModel):
    """Control-plane job state exposed by the API layer."""

    model_config = ConfigDict(populate_by_name=True)

    id: str
    kind: str
    status: JobStatus
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None = None
    finished_at: datetime | None = None
    request: dict[str, Any]
    result: dict[str, Any] | None = None
    error: str | None = None

    @classmethod
    def new(cls, *, job_id: str, kind: str, request: BaseModel) -> "JobRecord":
        now = datetime.now(UTC)
        return cls(
            id=job_id,
            kind=kind,
            status=JobStatus.QUEUED,
            created_at=now,
            updated_at=now,
            request=request.model_dump(mode="json"),
        )


class StrategyCommandRequest(BaseModel):
    """Operator command for a strategy instance."""

    command: Literal["START", "STOP", "RESUME", "FORCE_RECOVER", "RELOAD"]
    reason: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)
