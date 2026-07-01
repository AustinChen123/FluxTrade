from __future__ import annotations

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from src.control_plane.models import (
    CsvSignalBacktestEvaluationConfig,
    EvaluationSetConfig,
    ParameterSearchDimension,
    ParameterSearchJobRequest,
    ParameterSearchSpace,
    ResearchRunnerEvaluationConfig,
)


class IntegerSearchRange(BaseModel):
    """Inclusive integer range for generated parameter-search dimensions."""

    min: int
    max: int
    step: int = 1

    @field_validator("min", "max", "step", mode="before")
    @classmethod
    def reject_boolean_values(cls, value):
        if isinstance(value, bool):
            raise ValueError("integer range values cannot be boolean")
        return value

    @model_validator(mode="after")
    def validate_range(self) -> "IntegerSearchRange":
        if self.step <= 0:
            raise ValueError("integer range step must be positive")
        if self.min > self.max:
            raise ValueError("integer range min must be less than or equal to max")
        return self

    def to_dimension(self) -> ParameterSearchDimension:
        return ParameterSearchDimension(
            type="integer",
            min=self.min,
            max=self.max,
            step=self.step,
        )


class DecimalSearchRange(BaseModel):
    """Inclusive decimal range for generated parameter-search dimensions."""

    min: Decimal
    max: Decimal
    step: Decimal

    @field_validator("min", "max", "step", mode="before")
    @classmethod
    def reject_boolean_values(cls, value):
        if isinstance(value, bool):
            raise ValueError("decimal range values cannot be boolean")
        return value

    @model_validator(mode="after")
    def validate_range(self) -> "DecimalSearchRange":
        if self.step <= 0:
            raise ValueError("decimal range step must be positive")
        if self.min > self.max:
            raise ValueError("decimal range min must be less than or equal to max")
        return self

    def to_dimension(self) -> ParameterSearchDimension:
        return ParameterSearchDimension(
            type="decimal",
            min=self.min,
            max=self.max,
            step=self.step,
        )


class GoldenCrossParameterSearchPreset(BaseModel):
    """Convenience payload for GoldenCross parameter-search jobs."""

    preset: Literal["golden_cross"] = "golden_cross"
    strategy_id: str = Field(default="golden_cross", min_length=1)
    product_id: str = Field(min_length=1)
    timeframe: str = Field(min_length=1)
    start_time: int
    end_time: int
    short_window: IntegerSearchRange
    long_window: IntegerSearchRange
    quantity: DecimalSearchRange = Field(
        default_factory=lambda: DecimalSearchRange(
            min=Decimal("0.01"),
            max=Decimal("0.01"),
            step=Decimal("0.01"),
        )
    )
    objective: Literal[
        "maximize_score",
        "maximize_return",
        "minimize_drawdown",
    ] = "maximize_score"
    seed: int | None = None
    candidate_sample_count: int = Field(ge=1, le=10_000)
    backtest: CsvSignalBacktestEvaluationConfig | None = None
    research_runner: ResearchRunnerEvaluationConfig | None = None
    evaluation_set: EvaluationSetConfig | None = None

    @field_validator("strategy_id", "product_id", "timeframe")
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
        if start_time is not None and value < start_time:
            raise ValueError("end_time must be greater than or equal to start_time")
        return value

    @model_validator(mode="after")
    def validate_golden_cross_ranges(self) -> "GoldenCrossParameterSearchPreset":
        if self.short_window.max >= self.long_window.min:
            raise ValueError(
                "golden_cross preset requires short_window.max < long_window.min "
                "because generated search spaces cannot express cross-parameter "
                "window constraints"
            )
        if self.quantity.min <= 0:
            raise ValueError("quantity range min must be positive")
        return self

    def to_parameter_search_request(self) -> ParameterSearchJobRequest:
        return ParameterSearchJobRequest(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=self.timeframe,
            start_time=self.start_time,
            end_time=self.end_time,
            objective=self.objective,
            seed=self.seed,
            backtest=self.backtest,
            research_runner=self.research_runner,
            evaluation_set=self.evaluation_set,
            search_space=ParameterSearchSpace(
                parameters={
                    "short_window": self.short_window.to_dimension(),
                    "long_window": self.long_window.to_dimension(),
                    "quantity": self.quantity.to_dimension(),
                }
            ),
            candidate_sample_count=self.candidate_sample_count,
        )
