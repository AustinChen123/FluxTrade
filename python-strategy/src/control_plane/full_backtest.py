"""Resolve a verified full-backtest subject and sealed data boundary."""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.control_plane.full_backtest_request import FullStrategyBacktestRequest
from src.core.data_provider import timeframe_to_ms
from src.core.data_sources.research_database import (
    ResearchDatabaseDataSource,
    ResearchDatasetMetadata,
)
from src.core.research_datasets import ResearchDatasetIntegrityError
from src.strategies.base import BaseStrategy, StrategyRequirements

_SUBJECT_UNAVAILABLE = "browser_result_subject_unavailable"
_DATASET_UNAVAILABLE = "browser_result_dataset_unavailable"
_BACKEND_UNAVAILABLE = "browser_result_backend_unavailable"
_SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")

ResolutionErrorCode = Literal[
    "browser_result_subject_unavailable",
    "browser_result_dataset_unavailable",
    "browser_result_backend_unavailable",
]


class FullBacktestResolutionError(RuntimeError):
    """One fixed, detail-free failure class for full-backtest resolution."""

    def __init__(self, code: ResolutionErrorCode) -> None:
        self.code = code
        super().__init__(code)


def _resolve_subject(
    request: FullStrategyBacktestRequest,
    strategy_loader: Callable[[], Mapping[str, object]],
) -> tuple[BaseStrategy, str, str]:
    """Resolve one fresh strategy class against the request's immutable identity."""
    try:
        strategies = strategy_loader()
        artifact = strategies.get(request.strategy_id)
        if (
            not isinstance(artifact, type)
            or not issubclass(artifact, BaseStrategy)
            or artifact is BaseStrategy
        ):
            raise ValueError

        artifact_version = getattr(artifact, "__fluxtrade_artifact_version__", None)
        catalog_sha256 = getattr(artifact, "__fluxtrade_catalog_sha256__", None)
        if (
            type(artifact_version) is not str
            or artifact_version != request.artifact_version
            or type(catalog_sha256) is not str
            or _SHA256_PATTERN.fullmatch(catalog_sha256) is None
        ):
            raise ValueError

        strategy = artifact(request.strategy_id, request.instrument.product_id)
        if (
            not isinstance(strategy, BaseStrategy)
            or strategy.strategy_id != request.strategy_id
            or strategy.product_id != request.instrument.product_id
        ):
            raise ValueError
        requirements = strategy.requirements
        if (
            not isinstance(requirements, StrategyRequirements)
            or requirements.product_id != strategy.product_id
            or type(requirements.timeframe) is not str
            or not requirements.timeframe.strip()
        ):
            raise ValueError
        if timeframe_to_ms(requirements.timeframe) <= 0:
            raise ValueError
        return strategy, catalog_sha256, requirements.timeframe
    except Exception:
        raise FullBacktestResolutionError(_SUBJECT_UNAVAILABLE) from None


@dataclass(frozen=True, slots=True)
class ResolvedFullBacktest:
    request: FullStrategyBacktestRequest
    strategy: BaseStrategy
    dataset: ResearchDatasetMetadata
    decision_timeframe: str
    execution_timeframe: str | None
    source_range: tuple[int, int]
    catalog_sha256: str


def resolve_full_backtest(
    request: FullStrategyBacktestRequest,
    *,
    strategy_loader: Callable[[], Mapping[str, object]],
    session_factory: Callable[[], Session],
) -> ResolvedFullBacktest:
    """Resolve a fresh verified strategy and sealed source without running it."""
    strategy, catalog_sha256, decision_timeframe = _resolve_subject(
        request, strategy_loader
    )
    try:
        source = ResearchDatabaseDataSource(
            request.dataset_id, session_factory=session_factory
        )
        dataset = source.get_dataset_metadata()
        source_range = source.get_available_range(dataset.product_id, dataset.timeframe)
    except ResearchDatasetIntegrityError:
        raise FullBacktestResolutionError(_DATASET_UNAVAILABLE) from None
    except SQLAlchemyError:
        raise FullBacktestResolutionError(_BACKEND_UNAVAILABLE) from None
    except Exception:
        raise FullBacktestResolutionError(_BACKEND_UNAVAILABLE) from None

    if (
        dataset.id != request.dataset_id
        or dataset.product_id != request.instrument.product_id
        or source_range is None
        or request.start < source_range[0]
        or request.end > source_range[1]
    ):
        raise FullBacktestResolutionError(_DATASET_UNAVAILABLE)

    execution_timeframe = request.execution_timeframe
    if execution_timeframe is None:
        if dataset.timeframe != decision_timeframe:
            raise FullBacktestResolutionError(_DATASET_UNAVAILABLE)
    else:
        if dataset.timeframe != execution_timeframe:
            raise FullBacktestResolutionError(_DATASET_UNAVAILABLE)
        try:
            execution_ms = timeframe_to_ms(execution_timeframe)
            decision_ms = timeframe_to_ms(decision_timeframe)
        except (IndexError, TypeError, ValueError):
            raise FullBacktestResolutionError(_DATASET_UNAVAILABLE) from None
        if (
            execution_ms <= 0
            or execution_ms >= decision_ms
            or decision_ms % execution_ms != 0
        ):
            raise FullBacktestResolutionError(_DATASET_UNAVAILABLE)

    return ResolvedFullBacktest(
        request=request,
        strategy=strategy,
        dataset=dataset,
        decision_timeframe=decision_timeframe,
        execution_timeframe=execution_timeframe,
        source_range=source_range,
        catalog_sha256=catalog_sha256,
    )
