from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from src.control_plane.fitness import (
    DEFAULT_WALK_FORWARD_FITNESS,
    validate_fitness_expression,
)
from src.core.evaluation_set import EvaluationDataset, EvaluationSet
from src.core.product_registry import (
    CapitalModel,
    FeeModel,
    InstrumentSpec,
    is_dated_future_product_id,
)


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class BacktestInstrumentConfig(BaseModel):
    """Instrument accounting metadata required by non-spot backtests."""

    multiplier: Decimal = Decimal("1")
    quantity_step: Decimal | None = None
    price_tick: Decimal | None = None
    fee_model: FeeModel = FeeModel.PERCENTAGE_NOTIONAL
    capital_model: CapitalModel = CapitalModel.NOTIONAL
    capital_per_contract: Decimal | None = None

    @field_validator("multiplier")
    @classmethod
    def validate_multiplier(cls, value: Decimal) -> Decimal:
        if value <= 0:
            raise ValueError("multiplier must be positive")
        return value

    @field_validator("quantity_step", "price_tick")
    @classmethod
    def validate_optional_positive_decimal(
        cls,
        value: Decimal | None,
    ) -> Decimal | None:
        if value is not None and value <= 0:
            raise ValueError("value must be positive")
        return value

    @model_validator(mode="after")
    def validate_capital_model(self) -> "BacktestInstrumentConfig":
        if self.capital_model == CapitalModel.PER_CONTRACT:
            if self.capital_per_contract is None or self.capital_per_contract <= 0:
                raise ValueError(
                    "capital_per_contract must be positive for per_contract capital"
                )
        elif self.capital_per_contract is not None:
            raise ValueError("capital_per_contract requires per_contract capital_model")
        return self

    def to_instrument_spec(self, product_id: str) -> InstrumentSpec:
        return InstrumentSpec(
            product_id=product_id,
            exchange=product_id.partition(":")[0].lower(),
            symbol=product_id,
            base=product_id,
            quote="",
            quantity_step=self.quantity_step,
            price_tick=self.price_tick,
            multiplier=self.multiplier,
            fee_model=self.fee_model,
            capital_model=self.capital_model,
            capital_per_contract=self.capital_per_contract,
        )


class PartialBacktestInstrumentConfig(BaseModel):
    """Per-dataset instrument fields validated after merging with shared settings."""

    multiplier: Decimal | None = None
    quantity_step: Decimal | None = None
    price_tick: Decimal | None = None
    fee_model: FeeModel | None = None
    capital_model: CapitalModel | None = None
    capital_per_contract: Decimal | None = None

    @field_validator(
        "multiplier",
        "quantity_step",
        "price_tick",
        "capital_per_contract",
    )
    @classmethod
    def validate_positive_decimal(
        cls,
        value: Decimal | None,
    ) -> Decimal | None:
        if value is not None and value <= 0:
            raise ValueError("value must be positive")
        return value


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
    instrument: BacktestInstrumentConfig | None = None
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

    @model_validator(mode="after")
    def validate_dated_future_rules(self) -> "BacktestJobRequest":
        _require_dated_future_rules(
            self.product_id,
            instrument_configured=self.instrument is not None,
            quantity_step=self.instrument.quantity_step if self.instrument else None,
            price_tick=self.instrument.price_tick if self.instrument else None,
        )
        return self


class ParameterCandidate(BaseModel):
    """Candidate parameter pack for a parameter-search job."""

    candidate_id: str = Field(min_length=1, max_length=64)
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


class EvolutionConfig(BaseModel):
    """Deterministic generation-based parameter evolution settings."""

    population_size: int = Field(ge=2, le=10_000)
    max_generations: int = Field(ge=1, le=10_000)
    tournament_size: int = Field(default=2, ge=2)
    elite_count: int = Field(default=1, ge=1)
    crossover_probability: Decimal = Decimal("0.9")
    mutation_probability: Decimal = Decimal("0.1")
    mutation_sigma_steps: Decimal = Decimal("1")
    epoch_id: str | None = Field(default=None, min_length=1, max_length=64)

    @field_validator("crossover_probability", "mutation_probability")
    @classmethod
    def validate_probability(cls, value: Decimal) -> Decimal:
        if not value.is_finite() or value < 0 or value > 1:
            raise ValueError("probability must be finite and between zero and one")
        return value

    @field_validator("mutation_sigma_steps")
    @classmethod
    def validate_mutation_sigma(cls, value: Decimal) -> Decimal:
        if not value.is_finite() or value <= 0:
            raise ValueError("mutation_sigma_steps must be finite and positive")
        return value

    @model_validator(mode="after")
    def validate_population_settings(self) -> "EvolutionConfig":
        if self.tournament_size > self.population_size:
            raise ValueError("tournament_size cannot exceed population_size")
        if self.elite_count >= self.population_size:
            raise ValueError("elite_count must be smaller than population_size")
        return self


class CsvSignalBacktestEvaluationConfig(BaseModel):
    """Backtest accounting plus the default CSV candle-source reference."""

    candles_csv_path: str | None = None
    initial_balance: Decimal = Decimal("10000")
    maker_fee: Decimal = Decimal("0")
    taker_fee: Decimal = Decimal("0")
    instrument: BacktestInstrumentConfig | None = None
    write_reports: bool = False

    @field_validator("candles_csv_path")
    @classmethod
    def validate_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
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


class PartialCsvSignalBacktestEvaluationConfig(BaseModel):
    """Per-dataset backtest overrides merged with shared settings."""

    candles_csv_path: str | None = None
    initial_balance: Decimal | None = None
    maker_fee: Decimal | None = None
    taker_fee: Decimal | None = None
    instrument: PartialBacktestInstrumentConfig | None = None
    write_reports: bool | None = None

    @field_validator("candles_csv_path")
    @classmethod
    def validate_path(cls, value: str | None) -> str | None:
        if value is None:
            return value
        if not value.strip():
            raise ValueError("path cannot be blank")
        return str(Path(value))

    @field_validator("initial_balance")
    @classmethod
    def validate_initial_balance(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and value <= 0:
            raise ValueError("initial_balance must be positive")
        return value

    @field_validator("maker_fee", "taker_fee")
    @classmethod
    def validate_fee(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and value < 0:
            raise ValueError("fee cannot be negative")
        return value


class ResearchRunnerEvaluationConfig(BaseModel):
    """Research-runner settings for parameter candidate evaluation."""

    capital_allocation: Decimal | None = None

    @field_validator("capital_allocation")
    @classmethod
    def validate_capital_allocation(cls, value: Decimal | None) -> Decimal | None:
        if value is not None and value <= 0:
            raise ValueError("capital_allocation must be positive")
        return value


class EvaluationDatasetConfig(BaseModel):
    """One dataset interval for multi-regime parameter evaluation."""

    dataset_id: str = Field(min_length=1)
    product_id: str = Field(min_length=1)
    timeframe: str = Field(min_length=1)
    start_time: int
    end_time: int
    warmup_start_time: int | None = None
    backtest: PartialCsvSignalBacktestEvaluationConfig | None = None
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


class WalkForwardEvaluationConfig(BaseModel):
    """Generate equal, non-overlapping scoring folds from one time range."""

    product_id: str = Field(min_length=1)
    timeframe: str = Field(min_length=1)
    start_time: int
    end_time: int
    fold_duration_ms: int = Field(gt=0)
    warmup_duration_ms: int = Field(default=0, ge=0)
    dataset_id_prefix: str = Field(default="wf", min_length=1)

    @field_validator("product_id", "timeframe", "dataset_id_prefix")
    @classmethod
    def strip_required_string(cls, value: str) -> str:
        stripped = value.strip()
        if not stripped:
            raise ValueError("field cannot be blank")
        return stripped

    def to_core_evaluation_set(self) -> EvaluationSet:
        return EvaluationSet.walk_forward(
            product_id=self.product_id,
            timeframe=self.timeframe,
            start_time=self.start_time,
            end_time=self.end_time,
            fold_duration_ms=self.fold_duration_ms,
            warmup_duration_ms=self.warmup_duration_ms,
            dataset_id_prefix=self.dataset_id_prefix,
        )


class EvaluationSetConfig(BaseModel):
    """Control-plane payload for a group of evaluation datasets."""

    datasets: list[EvaluationDatasetConfig] = Field(default_factory=list)
    walk_forward: WalkForwardEvaluationConfig | None = None

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

    @model_validator(mode="after")
    def validate_dataset_source(self) -> "EvaluationSetConfig":
        if self.datasets and self.walk_forward is not None:
            raise ValueError("provide datasets or walk_forward, not both")
        if not self.datasets and self.walk_forward is None:
            raise ValueError("evaluation_set requires datasets or walk_forward")
        return self

    @property
    def resolved_datasets(self) -> list[EvaluationDatasetConfig]:
        if self.walk_forward is None:
            return self.datasets
        return [
            EvaluationDatasetConfig(
                dataset_id=dataset.dataset_id,
                product_id=dataset.product_id,
                timeframe=dataset.timeframe,
                start_time=dataset.start_time,
                end_time=dataset.end_time,
                warmup_start_time=dataset.warmup_start_time,
                metadata=dict(dataset.metadata),
            )
            for dataset in self.walk_forward.to_core_evaluation_set()
        ]

    def to_core_evaluation_set(self) -> EvaluationSet:
        return EvaluationSet(
            dataset.to_core_dataset() for dataset in self.resolved_datasets
        )


class FitnessConfig(BaseModel):
    """Registered Decimal expression used to score walk-forward candidates."""

    expression: str = DEFAULT_WALK_FORWARD_FITNESS
    independent_trials: int | None = Field(
        default=None,
        ge=1,
        le=1_000_000_000,
    )

    @field_validator("expression")
    @classmethod
    def validate_expression(cls, value: str) -> str:
        return validate_fitness_expression(value)


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
    research_runner: ResearchRunnerEvaluationConfig | None = None
    evaluation_set: EvaluationSetConfig | None = None
    fitness: FitnessConfig | None = None
    candidates: list[ParameterCandidate] | None = Field(default=None, min_length=1)
    search_space: ParameterSearchSpace | None = None
    candidate_sample_count: int | None = Field(default=None, ge=1, le=10_000)
    evolution: EvolutionConfig | None = None

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
        if self.evolution is not None:
            if not has_search_space:
                raise ValueError("evolution requires search_space")
            if self.candidate_sample_count is not None:
                raise ValueError("candidate_sample_count is not used by evolution")
            assert self.search_space is not None
            if any(
                dimension.type == "categorical"
                and any(
                    isinstance(choice, Decimal)
                    for choice in dimension.choices or []
                )
                for dimension in self.search_space.parameters.values()
            ):
                raise ValueError(
                    "evolution categorical choices cannot contain Decimal values"
                )
        else:
            if has_candidates and self.candidate_sample_count is not None:
                raise ValueError("candidate_sample_count requires search_space")
            if has_search_space and self.candidate_sample_count is None:
                raise ValueError("candidate_sample_count is required with search_space")
        if self.fitness is not None:
            if self.evaluation_set is None:
                raise ValueError("fitness requires evaluation_set")
            if self.objective != "maximize_score":
                raise ValueError("fitness requires objective=maximize_score")
            _require_non_overlapping_scoring_folds(
                self.evaluation_set.resolved_datasets
            )
        if self.evaluation_set is not None:
            if self.backtest is None:
                missing_backtest_dataset_ids = [
                    dataset.dataset_id
                    for dataset in self.evaluation_set.resolved_datasets
                    if dataset.backtest is None
                    or dataset.backtest.candles_csv_path is None
                ]
                if missing_backtest_dataset_ids:
                    datasets = ", ".join(missing_backtest_dataset_ids)
                    raise ValueError(
                        "evaluation_set datasets require candles_csv_path when "
                        f"shared backtest is not provided: {datasets}"
                    )
            for dataset in self.evaluation_set.resolved_datasets:
                override = dataset.backtest.instrument if dataset.backtest else None
                shared = self.backtest.instrument if self.backtest else None
                _require_dated_future_rules(
                    dataset.product_id,
                    instrument_configured=shared is not None or override is not None,
                    quantity_step=(
                        override.quantity_step
                        if override and override.quantity_step is not None
                        else shared.quantity_step if shared else None
                    ),
                    price_tick=(
                        override.price_tick
                        if override and override.price_tick is not None
                        else shared.price_tick if shared else None
                    ),
                )
        else:
            instrument = self.backtest.instrument if self.backtest else None
            _require_dated_future_rules(
                self.product_id,
                instrument_configured=instrument is not None,
                quantity_step=instrument.quantity_step if instrument else None,
                price_tick=instrument.price_tick if instrument else None,
            )
        return self


def _require_non_overlapping_scoring_folds(
    datasets: list[EvaluationDatasetConfig],
) -> None:
    grouped: dict[tuple[str, str], list[EvaluationDatasetConfig]] = {}
    for dataset in datasets:
        grouped.setdefault(
            (dataset.product_id, dataset.timeframe),
            [],
        ).append(dataset)
    for group in grouped.values():
        ordered = sorted(group, key=lambda dataset: (dataset.start_time, dataset.end_time))
        for previous, current in zip(ordered, ordered[1:]):
            if current.start_time <= previous.end_time:
                raise ValueError(
                    "fitness scoring folds overlap: "
                    f"{previous.dataset_id}, {current.dataset_id}"
                )


def _require_dated_future_rules(
    product_id: str,
    *,
    instrument_configured: bool,
    quantity_step: Decimal | None,
    price_tick: Decimal | None,
) -> None:
    if not is_dated_future_product_id(product_id):
        return
    if not instrument_configured:
        raise ValueError(
            f"dated future {product_id} requires instrument configuration"
        )
    missing = [
        name
        for name, value in (
            ("quantity_step", quantity_step),
            ("price_tick", price_tick),
        )
        if value is None
    ]
    if missing:
        raise ValueError(
            f"dated future {product_id} requires {', '.join(missing)}"
        )


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
