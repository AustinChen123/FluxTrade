"""Registered market-data boundary for parameter evaluation."""

from __future__ import annotations

from pathlib import Path
from typing import Hashable, Protocol

from src.control_plane.models import ParameterSearchJobRequest
from src.core.data_sources.csv_source import CsvDataSource
from src.core.data_sources.research_database import ResearchDatabaseDataSource
from src.core.interfaces.data_source import IDataSource


class EvaluationDataSourceProvider(Protocol):
    """Create replay sources without coupling evaluators to their storage type."""

    def create(self, request: ParameterSearchJobRequest) -> IDataSource: ...

    def cache_key(self, request: ParameterSearchJobRequest) -> Hashable: ...


class CsvEvaluationDataSourceProvider:
    """Default provider for the current path-based request contract."""

    def create(self, request: ParameterSearchJobRequest) -> IDataSource:
        if request.backtest is None:
            raise ValueError("backtest settings are required for CSV market data")
        if request.backtest.candles_csv_path is None:
            raise ValueError("CSV market data requires candles_csv_path")
        return CsvDataSource(
            file_path=request.backtest.candles_csv_path,
            product_id=request.product_id,
            timeframe=request.timeframe,
        )

    def cache_key(self, request: ParameterSearchJobRequest) -> Hashable:
        if request.backtest is None:
            raise ValueError("backtest settings are required for CSV market data")
        if request.backtest.candles_csv_path is None:
            raise ValueError("CSV market data requires candles_csv_path")
        path = Path(request.backtest.candles_csv_path)
        stat = path.stat()
        return str(path.resolve()), stat.st_mtime_ns, stat.st_size


class DatabaseEvaluationDataSourceProvider:
    """Provide one explicitly bound, sealed research dataset to an evaluation."""

    def __init__(self, dataset_id: str, session_factory=None) -> None:
        if not isinstance(dataset_id, str) or not dataset_id.strip():
            raise ValueError("dataset_id must be non-empty")
        self._dataset_id = dataset_id
        self._source = ResearchDatabaseDataSource(
            dataset_id,
            session_factory=session_factory,
        )

    def create(self, request: ParameterSearchJobRequest) -> IDataSource:
        self._validate_request(request)
        return self._source

    def cache_key(self, request: ParameterSearchJobRequest) -> Hashable:
        self._validate_request(request)
        return (
            self._dataset_id,
            request.product_id,
            request.timeframe,
            request.start_time,
            request.end_time,
        )

    def _validate_request(self, request: ParameterSearchJobRequest) -> None:
        if request.start_time > request.end_time:
            raise ValueError("evaluation range must not be reversed")
        available_range = self._source.get_available_range(
            request.product_id,
            request.timeframe,
        )
        if available_range is None:
            raise ValueError("dataset product/timeframe does not match request")
        if (
            request.start_time < available_range[0]
            or request.end_time > available_range[1]
        ):
            raise ValueError("requested range is outside sealed dataset coverage")
