from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import AbstractContextManager
from decimal import Decimal
from typing import Any, Callable

from sqlalchemy.orm import Session

from src.control_plane.jobs import InMemoryJobStore, JobStore
from src.control_plane.models import BacktestJobRequest, JobRecord
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.csv_source import CsvDataSource
from src.strategies.csv_signal_strategy import CsvSignalStrategy


SessionFactory = Callable[[], AbstractContextManager[Session]]


class BacktestJobExecutor:
    """Submit and run backtest jobs through the existing BacktestRunner."""

    def __init__(
        self,
        store: JobStore | None = None,
        *,
        db_session_factory: SessionFactory | None = None,
        max_workers: int = 2,
        run_inline: bool = False,
    ) -> None:
        self.store = store or InMemoryJobStore()
        self._db_session_factory = db_session_factory
        self._run_inline = run_inline
        self._executor = None if run_inline else ThreadPoolExecutor(max_workers=max_workers)

    def submit_backtest(self, request: BacktestJobRequest) -> JobRecord:
        job = self.store.create(kind=request.kind, request=request)
        if self._run_inline:
            return self._run_job(job.id, request)
        assert self._executor is not None
        self._executor.submit(self._run_job, job.id, request)
        return job

    def shutdown(self, wait: bool = True) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=wait, cancel_futures=False)

    def _run_job(self, job_id: str, request: BacktestJobRequest) -> JobRecord:
        self.store.mark_running(job_id)
        try:
            result = self._run_backtest(request)
        except Exception as exc:
            return self.store.mark_failed(job_id, str(exc))
        return self.store.mark_succeeded(job_id, result)

    def _run_backtest(self, request: BacktestJobRequest) -> dict[str, Any]:
        data_source = CsvDataSource(
            file_path=request.candles_csv_path,
            product_id=request.product_id,
            timeframe=request.timeframe,
        )
        strategy = CsvSignalStrategy(
            strategy_id=request.strategy_id,
            csv_path=request.signals_csv_path,
            product_id=request.product_id,
            timeframe=request.timeframe,
        )
        runner_kwargs: dict[str, Any] = {}
        if self._db_session_factory is not None:
            runner_kwargs["db_session_factory"] = self._db_session_factory

        runner = BacktestRunner(
            start_time=request.start_time,
            end_time=request.end_time,
            product_id=request.product_id,
            timeframe=request.timeframe,
            initial_balance=float(request.initial_balance),
            data_source=data_source,
            fee_config={
                "maker": float(request.maker_fee),
                "taker": float(request.taker_fee),
            },
            report_config={
                "csv_trades": request.write_reports,
                "markdown_report": request.write_reports,
                "equity_curve": request.write_reports,
                "journal_export": request.write_reports,
            },
            **runner_kwargs,
        )
        runner.add_strategy(strategy)
        result = runner.run()
        return _json_safe(result)


def _json_safe(value: Any) -> Any:
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    if hasattr(value, "model_dump"):
        return _json_safe(value.model_dump(mode="json"))
    return value
