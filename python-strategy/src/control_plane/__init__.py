"""Control-plane APIs for backtest and strategy operations."""

from src.control_plane.app import ControlPlaneApp, HttpResponse
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.evaluation_data import (
    CsvEvaluationDataSourceProvider,
    EvaluationDataSourceProvider,
)
from src.control_plane.gene_control import GeneControlService
from src.control_plane.jobs import InMemoryJobStore, JobStatus, JobStore, SqliteJobStore
from src.control_plane.models import (
    BacktestJobRequest,
    CsvSignalBacktestEvaluationConfig,
    EvaluationDatasetConfig,
    EvaluationSetConfig,
    EvolutionConfig,
    FitnessConfig,
    GenePromotionRequest,
    JobRecord,
    PartialCsvSignalBacktestEvaluationConfig,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
    ParameterSearchDimension,
    ParameterSearchSpace,
    ResearchRunnerEvaluationConfig,
    WalkForwardEvaluationConfig,
)
from src.control_plane.parameter_evaluation import (
    CsvSignalBacktestParameterEvaluator,
    GoldenCrossFastFitnessParameterEvaluator,
    GoldenCrossResearchParameterEvaluator,
    ParameterSearchEvaluatorRegistry,
    ParameterSearchEvaluator,
    UnsupportedParameterSearchError,
    ResearchBacktestParameterEvaluator,
)
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.control_plane.presets import (
    DecimalSearchRange,
    GoldenCrossParameterSearchPreset,
    IntegerSearchRange,
)
from src.control_plane.strategy_control import (
    RedisStrategyCommandRouter,
    StrategyControlService,
    StrategyControlUnavailable,
)
from src.control_plane.strategy_state_query import StrategyStateQueryService

__all__ = [
    "BacktestJobExecutor",
    "BacktestJobRequest",
    "ControlPlaneApp",
    "CsvSignalBacktestEvaluationConfig",
    "CsvSignalBacktestParameterEvaluator",
    "CsvEvaluationDataSourceProvider",
    "DecimalSearchRange",
    "EvaluationDatasetConfig",
    "EvaluationDataSourceProvider",
    "EvaluationSetConfig",
    "EvolutionConfig",
    "FitnessConfig",
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
    "ParameterSearchEvaluatorRegistry",
    "UnsupportedParameterSearchError",
    "ParameterSearchJobExecutor",
    "ParameterSearchSpace",
    "ResearchBacktestParameterEvaluator",
    "ResearchRunnerEvaluationConfig",
    "RedisStrategyCommandRouter",
    "IntegerSearchRange",
    "SqliteJobStore",
    "StrategyControlService",
    "StrategyControlUnavailable",
    "StrategyStateQueryService",
    "WalkForwardEvaluationConfig",
]
