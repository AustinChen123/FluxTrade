from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.core.evaluation_set import EvaluationDataset, EvaluationSet


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


class ParameterCandidate(BaseModel):
    """Candidate parameter pack for a parameter-search job."""

    candidate_id: str = Field(min_length=1)
    param_pack: dict[str, Any] = Field(default_factory=dict)


class ParameterSearchDimension(BaseModel):
    """One parameter dimension for generated parameter-search candidates."""

    type: Literal["integer", "decimal", "categorical"]
    min: int | Decimal | None = None
    max: int | Decimal | None = None
    step: int | Decimal | None = None
    choices: list[int | Decimal | str | bool] | None = None

    @field_validator("min", "max", "step", mode="before")
    @classmethod
    def reject_boolean_bounds(cls, value):
        if isinstance(value, bool):
            raise ValueError("numeric dimension bounds cannot be boolean")
        return value

    @model_validator(mode="after")
    def validate_dimension(self) -> "ParameterSearchDimension":
        if self.type == "categorical":
            if not self.choices:
                raise ValueError("categorical dimensions require non-empty choices")
            return self

        if self.min is None or self.max is None or self.step is None:
            raise ValueError(f"{self.type} dimensions require min, max, and step")
        if self.choices is not None:
            raise ValueError(f"{self.type} dimensions cannot define choices")

        if self.type == "integer":
            min_value = _require_int(self.min, "min")
            max_value = _require_int(self.max, "max")
            step_value = _require_int(self.step, "step")
            if step_value <= 0:
                raise ValueError("integer dimension step must be positive")
            if min_value > max_value:
                raise ValueError("integer dimension min must be less than or equal to max")
            return self

        min_decimal = Decimal(str(self.min))
        max_decimal = Decimal(str(self.max))
        step_decimal = Decimal(str(self.step))
        if step_decimal <= 0:
            raise ValueError("decimal dimension step must be positive")
        if min_decimal > max_decimal:
            raise ValueError("decimal dimension min must be less than or equal to max")
        return self


class ParameterSearchSpace(BaseModel):
    """Parameter dimensions used to generate candidate parameter packs."""

    parameters: dict[str, ParameterSearchDimension] = Field(min_length=1)

    @field_validator("parameters")
    @classmethod
    def validate_parameter_names(
        cls,
        value: dict[str, ParameterSearchDimension],
    ) -> dict[str, ParameterSearchDimension]:
        for name in value:
            if not name.strip():
                raise ValueError("parameter names cannot be blank")
        return value


class CsvSignalBacktestEvaluationConfig(BaseModel):
    """Backtest settings for CSV-signal parameter candidate evaluation."""

    candles_csv_path: str = Field(min_length=1)
    initial_balance: Decimal = Decimal("10000")
    maker_fee: Decimal = Decimal("0")
    taker_fee: Decimal = Decimal("0")
    write_reports: bool = False

    @field_validator("candles_csv_path")
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


class EvaluationDatasetConfig(BaseModel):
    """One dataset interval for multi-regime parameter evaluation."""

    dataset_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    timeframe: str = Field(min_length=1)
    start_time: int
    end_time: int
    warmup_start_time: int | None = None
    backtest: CsvSignalBacktestEvaluationConfig | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("dataset_id", "product_id", "timeframe")
    @classmethod
    def strip_required_string(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("field cannot be blank")
        return stripped

    @field_validator("end_time")
    @classmethod
    def validate_time_range(cls, value: int, info) -> int:
        start_time = info.data.get("start_time")
        if start_time is not None and value <= start_time:
            raise ValueError("end_time must be greater than start_time")
        return value

    @model_validator(mode="after")
    def validate_warmup_range(self) -> "EvaluationDatasetConfig":
        if (
            self.warmup_start_time is not None
            and self.warmup_start_time > self.start_time
        ):
            raise ValueError("warmup_start_time must be <= start_time")
        return self

    def to_core_dataset(self) -> EvaluationDataset:
        return EvaluationDataset(
            dataset_id=self.dataset_id,
            product_id=self.product_id,
            timeframe=self.timeframe,
            start_time=self.start_time,
            end_time=self.end_time,
            warmup_start_time=self.warmup_start_time,
            metadata=self.metadata,
        )


class EvaluationSetConfig(BaseModel):
    """Control-plane payload for a group of evaluation datasets."""

    datasets: list[EvaluationDatasetConfig] = Field(min_length=1)

    @field_validator("datasets")
    @classmethod
    def validate_unique_dataset_ids(
        cls,
        value: list[EvaluationDatasetConfig],
    ) -> list[EvaluationDatasetConfig]:
        dataset_ids = [dataset.dataset_id for dataset in value]
        if len(dataset_ids) != len(set(dataset_ids)):
            raise ValueError("dataset_id values must be unique")
        return value

    def to_core_evaluation_set(self) -> EvaluationSet:
        return EvaluationSet(dataset.to_core_dataset() for dataset in self.datasets)


class ParameterSearchJobRequest(BaseModel):
    """Request payload for evaluating strategy parameter candidates."""

    kind: Literal["parameter_search"] = "parameter_search"
    strategy_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    timeframe: str = Field(min_length=1)
    start_time: int
    end_time: int
    objective: Literal[
        "maximize_score",
        "maximize_return",
        "minimize_drawdown",
    ] = "maximize_score"
    seed: int | None = None
    backtest: CsvSignalBacktestEvaluationConfig | None = None
    evaluation_set: EvaluationSetConfig | None = None
    candidates: list[ParameterCandidate] | None = Field(default=None, min_length=1)
    search_space: ParameterSearchSpace | None = None
    candidate_sample_count: int | None = Field(default=None, ge=1, le=10_000)

    @field_validator("end_time")
    @classmethod
    def validate_time_range(cls, value: int, info) -> int:
        start_time = info.data.get("start_time")
        if start_time is not None and value < start_time:
            raise ValueError("end_time must be greater than or equal to start_time")
        return value

    @field_validator("candidates")
    @classmethod
    def validate_unique_candidates(
        cls,
        value: list[ParameterCandidate] | None,
    ) -> list[ParameterCandidate] | None:
        if value is None:
            return value
        candidate_ids = [candidate.candidate_id for candidate in value]
        if len(candidate_ids) != len(set(candidate_ids)):
            raise ValueError("candidate_id values must be unique")
        return value

    @model_validator(mode="after")
    def validate_candidate_source(self) -> "ParameterSearchJobRequest":
        has_candidates = self.candidates is not None
        has_search_space = self.search_space is not None
        if has_candidates == has_search_space:
            raise ValueError("provide exactly one of candidates or search_space")
        if has_candidates and self.candidate_sample_count is not None:
            raise ValueError("candidate_sample_count requires search_space")
        if has_search_space and self.candidate_sample_count is None:
            raise ValueError("candidate_sample_count is required with search_space")
        if self.evaluation_set is not None:
            warmup_dataset_ids = [
                dataset.dataset_id
                for dataset in self.evaluation_set.datasets
                if dataset.warmup_start_time is not None
            ]
            if warmup_dataset_ids:
                datasets = ", ".join(warmup_dataset_ids)
                raise ValueError(
                    "parameter_search evaluation_set does not support "
                    f"warmup_start_time yet: {datasets}"
                )
        return self


def _require_int(value: int | Decimal, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"integer dimension {field_name} must be an integer")
    decimal_value = Decimal(str(value))
    if decimal_value != decimal_value.to_integral_value():
        raise ValueError(f"integer dimension {field_name} must be an integer")
    return int(decimal_value)


class ParameterEvaluationResult(BaseModel):
    """Evaluator output for one parameter candidate."""

    candidate_id: str = Field(min_length=1)
    score_total: Decimal
    max_drawdown: Decimal = Decimal("0")
    metrics: dict[str, Any] = Field(default_factory=dict)


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


class GenePromotionRequest(BaseModel):
    """Operator request to promote a parameter gene to champion."""

    reason: str | None = None
    actor: str = "control_plane"
