"""Control-plane APIs for backtest and strategy operations."""

from src.control_plane.app import ControlPlaneApp, HttpResponse
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.gene_control import GeneControlService
from src.control_plane.jobs import InMemoryJobStore, JobStatus, JobStore, SqliteJobStore
from src.control_plane.models import (
    BacktestJobRequest,
    CsvSignalBacktestEvaluationConfig,
    EvaluationDatasetConfig,
    EvaluationSetConfig,
    GenePromotionRequest,
    JobRecord,
    PartialCsvSignalBacktestEvaluationConfig,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
    ParameterSearchDimension,
    ParameterSearchSpace,
    ResearchRunnerEvaluationConfig,
)
from src.control_plane.parameter_search import (
    CsvSignalBacktestParameterEvaluator,
    GoldenCrossFastFitnessParameterEvaluator,
    GoldenCrossResearchParameterEvaluator,
    ParameterSearchEvaluator,
    ParameterSearchJobExecutor,
    ResearchBacktestParameterEvaluator,
)
from src.control_plane.presets import (
    DecimalSearchRange,
    GoldenCrossParameterSearchPreset,
    IntegerSearchRange,
)
from src.control_plane.strategy_control import StrategyControlService
from src.control_plane.strategy_state_query import StrategyStateQueryService

__all__ = [
    "BacktestJobExecutor",
    "BacktestJobRequest",
    "ControlPlaneApp",
    "CsvSignalBacktestEvaluationConfig",
    "CsvSignalBacktestParameterEvaluator",
    "DecimalSearchRange",
    "EvaluationDatasetConfig",
    "EvaluationSetConfig",
    "GeneControlService",
    "GoldenCrossFastFitnessParameterEvaluator",
    "GoldenCrossParameterSearchPreset",
    "GoldenCrossResearchParameterEvaluator",
    "GenePromotionRequest",
    "HttpResponse",
    "InMemoryJobStore",
    "JobRecord",
    "JobStatus",
    "JobStore",
    "PartialCsvSignalBacktestEvaluationConfig",
    "ParameterCandidate",
    "ParameterEvaluationResult",
    "ParameterSearchJobRequest",
    "ParameterSearchDimension",
    "ParameterSearchEvaluator",
    "ParameterSearchJobExecutor",
    "ParameterSearchSpace",
    "ResearchBacktestParameterEvaluator",
    "ResearchRunnerEvaluationConfig",
    "IntegerSearchRange",
    "SqliteJobStore",
    "StrategyControlService",
    "StrategyStateQueryService",
]
