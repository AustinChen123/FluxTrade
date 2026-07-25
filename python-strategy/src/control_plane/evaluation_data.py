"""Registered market-data boundary for parameter evaluation."""

from __future__ import annotations

from pathlib import Path
from typing import Hashable, Protocol

from src.control_plane.models import ParameterSearchJobRequest
from src.core.data_sources.csv_source import CsvDataSource
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
