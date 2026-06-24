"""Control-plane APIs for backtest and strategy operations."""

from src.control_plane.app import ControlPlaneApp, HttpResponse
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.gene_control import GeneControlService
from src.control_plane.jobs import InMemoryJobStore, JobStatus, JobStore, SqliteJobStore
from src.control_plane.models import (
    BacktestJobRequest,
    CsvSignalBacktestEvaluationConfig,
    GenePromotionRequest,
    JobRecord,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
    ParameterSearchDimension,
    ParameterSearchSpace,
)
from src.control_plane.parameter_search import (
    CsvSignalBacktestParameterEvaluator,
    GoldenCrossFastFitnessParameterEvaluator,
    GoldenCrossResearchParameterEvaluator,
    ParameterSearchEvaluator,
    ParameterSearchJobExecutor,
    ResearchBacktestParameterEvaluator,
)
from src.control_plane.strategy_control import StrategyControlService
from src.control_plane.strategy_state_query import StrategyStateQueryService

__all__ = [
    "BacktestJobExecutor",
    "BacktestJobRequest",
    "ControlPlaneApp",
    "CsvSignalBacktestEvaluationConfig",
    "CsvSignalBacktestParameterEvaluator",
    "GeneControlService",
    "GoldenCrossFastFitnessParameterEvaluator",
    "GoldenCrossResearchParameterEvaluator",
    "GenePromotionRequest",
    "HttpResponse",
    "InMemoryJobStore",
    "JobRecord",
    "JobStatus",
    "JobStore",
    "ParameterCandidate",
    "ParameterEvaluationResult",
    "ParameterSearchJobRequest",
    "ParameterSearchDimension",
    "ParameterSearchEvaluator",
    "ParameterSearchJobExecutor",
    "ParameterSearchSpace",
    "ResearchBacktestParameterEvaluator",
    "SqliteJobStore",
    "StrategyControlService",
    "StrategyStateQueryService",
]
