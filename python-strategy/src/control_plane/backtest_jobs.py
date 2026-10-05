from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, wait as wait_futures
from contextlib import AbstractContextManager
from collections.abc import Mapping
from dataclasses import dataclass, fields, is_dataclass
from decimal import Decimal
from threading import Lock
from typing import Any, Callable, cast

from sqlalchemy.orm import Session

from src.control_plane.jobs import InMemoryJobStore, JobStore
from src.control_plane.models import (
    BacktestInstrumentConfig,
    BacktestJobRequest,
    JobRecord,
    JobStatus,
)
from src.control_plane.full_backtest import (
    FullBacktestExecutionError,
    FullBacktestResolutionError,
    ResolvedFullBacktest,
    resolve_full_backtest,
    run_full_backtest,
)
from src.control_plane.full_backtest_request import (
    BacktestRequest,
    FullStrategyBacktestRequest,
    parse_backtest_request,
)
from src.core.backtest_result_owner import (
    BacktestResultPersistenceError,
    BacktestResultPersistenceOwner,
    BacktestResultRunIdentity,
)
from src.core.backtest_runner import BacktestRunner
from src.core.data_sources.csv_source import CsvDataSource
from src.core.interfaces.data_source import IDataSource
from src.strategies.csv_signal_strategy import CsvSignalStrategy


SessionFactory = Callable[[], AbstractContextManager[Session]]


@dataclass(frozen=True, slots=True)
class BacktestRunSpec:
    """Internal runner input; market data may be supplied by an injected provider."""

    strategy_id: str
    product_id: str
    timeframe: str
    signals_csv_path: str
    start_time: int
    end_time: int
    initial_balance: Decimal
    maker_fee: Decimal
    taker_fee: Decimal
    instrument: BacktestInstrumentConfig | None
    write_reports: bool
    candles_csv_path: str | None = None


class BacktestJobExecutor:
    """Submit and run backtest jobs through the existing BacktestRunner."""

    def __init__(
        self,
        store: JobStore | None = None,
        *,
        db_session_factory: SessionFactory | None = None,
        max_workers: int = 2,
        run_inline: bool = False,
        recover_interrupted: bool = False,
        strategy_loader: Callable[[], Mapping[str, object]] | None = None,
    ) -> None:
        self.store = store or InMemoryJobStore()
        if recover_interrupted:
            self.store.mark_interrupted_active_jobs(
                "Job interrupted before control plane startup"
            )
        self._db_session_factory = db_session_factory
        self._strategy_loader = strategy_loader
        self._run_inline = run_inline
        self._executor = None if run_inline else ThreadPoolExecutor(max_workers=max_workers)
        self._futures: dict[str, Future[JobRecord]] = {}
        self._futures_lock = Lock()
        self._closed = False

    def submit_backtest(self, request: BacktestRequest) -> JobRecord:
        with self._futures_lock:
            if self._closed:
                raise RuntimeError("backtest executor is shut down")
            job = self.store.create(kind=request.kind, request=request)
            if self._run_inline:
                future = None
            else:
                assert self._executor is not None
                future = self._executor.submit(self._run_job, job.id, request)
                self._futures[job.id] = future
        if future is None:
            return self._run_job(job.id, request)
        return job

    def cancel_backtest(self, job_id: str, reason: str | None = None) -> JobRecord:
        with self._futures_lock:
            job = self.store.get(job_id)
            if job is None:
                raise KeyError(job_id)
            if job.status == JobStatus.RUNNING:
                raise ValueError("running jobs cannot be cancelled")
            if job.status != JobStatus.QUEUED:
                raise ValueError(
                    f"{job.status.value.lower()} jobs cannot be cancelled"
                )
            future = self._futures.get(job_id)
            if future is not None:
                if not future.cancel():
                    raise ValueError("job already started")
                self._futures.pop(job_id, None)
            return self.store.mark_cancelled(
                job_id,
                reason or "cancelled by operator",
            )

    def retry_backtest(self, job_id: str) -> JobRecord:
        job = self.store.get(job_id)
        if job is None:
            raise KeyError(job_id)
        if job.kind not in {"csv_signal_backtest", "full_strategy_backtest"}:
            raise ValueError(f"unsupported job kind: {job.kind}")
        if job.status not in {JobStatus.FAILED, JobStatus.CANCELLED}:
            raise ValueError(f"{job.status.value.lower()} jobs cannot be retried")

        request = parse_backtest_request(job.request)
        if request.kind != job.kind:
            raise ValueError(f"unsupported job kind: {job.kind}")
        return self.submit_backtest(request)

    def shutdown(
        self,
        wait: bool = True,
        *,
        timeout: float | None = None,
        cancel_futures: bool = False,
    ) -> bool:
        with self._futures_lock:
            self._closed = True
            if self._executor is None:
                return True
            futures = tuple(self._futures.values())
            self._executor.shutdown(
                wait=False,
                cancel_futures=cancel_futures,
            )
        if not wait:
            return all(future.done() for future in futures)
        _, pending = wait_futures(futures, timeout=timeout)
        if pending:
            return False
        self._executor.shutdown(wait=True)
        return True

    def _run_job(self, job_id: str, request: BacktestRequest) -> JobRecord:
        try:
            current = self.store.get(job_id)
            if current is not None and current.status == JobStatus.CANCELLED:
                return current
            self.store.mark_running(job_id)
            if isinstance(request, FullStrategyBacktestRequest):
                try:
                    result = self._run_full_strategy_request(job_id, request)
                except FullBacktestResolutionError as exc:
                    return self.store.mark_failed(job_id, exc.code)
                except FullBacktestExecutionError as exc:
                    return self.store.mark_failed(job_id, exc.code)
                except BacktestResultPersistenceError:
                    return self.store.mark_failed(
                        job_id, "browser_result_persistence_failed"
                    )
                except Exception:
                    return self.store.mark_failed(
                        job_id, "browser_result_execution_failed"
                    )
            else:
                try:
                    result = self.run_backtest_request(request)
                except Exception as exc:
                    return self.store.mark_failed(job_id, str(exc))
            return self.store.mark_succeeded(job_id, result)
        finally:
            with self._futures_lock:
                self._futures.pop(job_id, None)

    def _run_full_strategy_request(
        self, job_id: str, request: FullStrategyBacktestRequest
    ) -> dict[str, str]:
        if self._strategy_loader is None:
            raise FullBacktestResolutionError("browser_result_subject_unavailable")
        if self._db_session_factory is None:
            raise FullBacktestResolutionError("browser_result_backend_unavailable")

        session_factory = cast(Callable[[], Session], self._db_session_factory)
        resolved: ResolvedFullBacktest = resolve_full_backtest(
            request,
            strategy_loader=self._strategy_loader,
            session_factory=session_factory,
        )
        outcome = run_full_backtest(resolved, session_factory=session_factory)
        identity = BacktestResultRunIdentity(
            job_id=job_id,
            strategy_id=request.strategy_id,
            artifact_version=request.artifact_version,
            catalog_sha256=resolved.catalog_sha256,
            dataset_id=resolved.dataset.id,
            dataset_checksum_sha256=resolved.dataset.checksum_sha256,
            product_id=resolved.dataset.product_id,
            timeframe=resolved.decision_timeframe,
            currency=request.instrument.quote,
            start=request.start,
            end=request.end,
            initial_balance=request.initial_balance,
            instrument=request.instrument.to_instrument_spec(),
            maker_fee=request.fees.maker,
            taker_fee=request.fees.taker,
            drawdown_limit=request.drawdown_limit,
            execution_timeframe=resolved.execution_timeframe,
        )
        receipt = BacktestResultPersistenceOwner(session_factory).persist(
            identity, outcome
        )
        return {
            "job_id": receipt.job_id,
            "input_digest": receipt.input_digest,
            "result_digest": receipt.result_digest,
        }

    def run_backtest_request(
        self,
        request: BacktestJobRequest | BacktestRunSpec,
        *,
        max_drawdown_limit: float | None = 0.20,
        data_source: IDataSource | None = None,
    ) -> dict[str, Any]:
        if data_source is None:
            if request.candles_csv_path is None:
                raise ValueError("CSV backtest requires candles_csv_path")
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
            initial_balance=request.initial_balance,
            max_drawdown_limit=max_drawdown_limit,
            data_source=data_source,
            fee_config={
                "maker": float(request.maker_fee),
                "taker": float(request.taker_fee),
            },
            instrument_spec=(
                request.instrument.to_instrument_spec(request.product_id)
                if request.instrument is not None
                else None
            ),
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
    if is_dataclass(value) and not isinstance(value, type):
        return {
            field.name: _json_safe(getattr(value, field.name))
            for field in fields(value)
        }
    return value
