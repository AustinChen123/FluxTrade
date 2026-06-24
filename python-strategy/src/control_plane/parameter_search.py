from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Protocol

from sqlalchemy.orm import Session

from src.control_plane.backtest_jobs import BacktestJobExecutor, SessionFactory, _json_safe
from src.control_plane.jobs import InMemoryJobStore, JobStore
from src.control_plane.models import (
    BacktestJobRequest,
    JobRecord,
    JobStatus,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.search_space import resolve_parameter_candidates
from src.core.data_sources.csv_source import CsvDataSource
from src.core.data_sources.memory import MemoryDataSource
from src.core.golden_cross_fast_fitness import GoldenCrossFastFitnessEvaluator
from src.core.models import GeneRole
from src.core.orm_models import EvolutionEpoch, GeneRecord
from src.core.precision import PrecisionCodec
from src.core.research_backtest_runner import ResearchBacktestRunner
from src.strategies.base import BaseStrategy
from src.strategies.golden_cross import GoldenCrossStrategy


class ParameterSearchEvaluator(Protocol):
    """Evaluation boundary for candidate parameter packs."""

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult: ...


class CsvSignalBacktestParameterEvaluator:
    """Evaluate candidates by running CSV-signal backtests.

    Each candidate must include ``signals_csv_path`` in ``param_pack``. The shared
    candle CSV and fees live in ``ParameterSearchJobRequest.backtest``.
    """

    def __init__(self, db_session_factory: SessionFactory | None = None) -> None:
        self._backtest_executor = BacktestJobExecutor(
            db_session_factory=db_session_factory,
            run_inline=True,
        )

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        if request.backtest is None:
            raise ValueError("backtest settings are required for CSV-signal evaluation")

        signals_csv_path = candidate.param_pack.get("signals_csv_path")
        if not isinstance(signals_csv_path, str) or not signals_csv_path.strip():
            raise ValueError("candidate param_pack.signals_csv_path is required")

        backtest_request = BacktestJobRequest(
            strategy_id=f"{request.strategy_id}_{candidate.candidate_id}",
            product_id=request.product_id,
            timeframe=request.timeframe,
            candles_csv_path=request.backtest.candles_csv_path,
            signals_csv_path=signals_csv_path,
            start_time=request.start_time,
            end_time=request.end_time,
            initial_balance=request.backtest.initial_balance,
            maker_fee=request.backtest.maker_fee,
            taker_fee=request.backtest.taker_fee,
            write_reports=request.backtest.write_reports,
        )
        result = self._backtest_executor.run_backtest_request(backtest_request)
        score = _result_decimal(result, "total_pnl")
        max_drawdown = _result_decimal(result, "max_drawdown")
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            max_drawdown=max_drawdown,
            metrics=result,
        )


ResearchStrategyFactory = Callable[[str, str, str, dict[str, Any]], BaseStrategy]


class ResearchBacktestParameterEvaluator:
    """Evaluate candidates with the in-memory research backtest runner."""

    def __init__(
        self,
        strategy_factory: ResearchStrategyFactory,
        *,
        preload_candles: bool = True,
        precision_codec: PrecisionCodec | None = None,
    ) -> None:
        self._strategy_factory = strategy_factory
        self._preload_candles = preload_candles
        self._precision_codec = precision_codec
        self._candle_cache: dict[tuple, list] = {}
        self._prepared_scaled_cache: dict[tuple, list] = {}

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        if request.backtest is None:
            raise ValueError("backtest settings are required for research evaluation")

        data_source, prepared_scaled_candles = self._replay_inputs_for(request)
        runner = ResearchBacktestRunner(
            start_time=request.start_time,
            end_time=request.end_time,
            product_id=request.product_id,
            timeframe=request.timeframe,
            initial_balance=float(request.backtest.initial_balance),
            data_source=data_source,
            fee_config={
                "maker": float(request.backtest.maker_fee),
                "taker": float(request.backtest.taker_fee),
            },
            precision_codec=self._precision_codec,
            prepared_scaled_candles=prepared_scaled_candles,
        )
        strategy = self._strategy_factory(
            f"{request.strategy_id}_{candidate.candidate_id}",
            request.product_id,
            request.timeframe,
            candidate.param_pack,
        )
        runner.add_strategy(strategy)

        result = runner.run()
        metrics = {
            key: value
            for key, value in result.items()
            if key not in {"closed_trades", "raw_trades"}
        }
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=_result_decimal(result, "total_pnl"),
            max_drawdown=_result_decimal(result, "max_drawdown"),
            metrics=_json_safe(metrics),
        )

    def _replay_inputs_for(self, request: ParameterSearchJobRequest):
        assert request.backtest is not None
        csv_source = CsvDataSource(
            file_path=request.backtest.candles_csv_path,
            product_id=request.product_id,
            timeframe=request.timeframe,
        )
        if not self._preload_candles:
            return csv_source, None

        cache_key = _candle_cache_key(request)
        candles = self._candle_cache.get(cache_key)
        if candles is None:
            candles = list(
                csv_source.get_candles(
                    request.product_id,
                    request.timeframe,
                    request.start_time,
                    request.end_time,
                )
            )
            self._candle_cache[cache_key] = candles

        prepared_scaled_candles = None
        if self._precision_codec is not None:
            prepared_scaled_candles = self._prepared_scaled_cache.get(cache_key)
            if prepared_scaled_candles is None:
                prepared_scaled_candles = ResearchBacktestRunner.prepare_scaled_candles(
                    candles,
                    self._precision_codec,
                )
                self._prepared_scaled_cache[cache_key] = prepared_scaled_candles
        return MemoryDataSource(candles), prepared_scaled_candles


class GoldenCrossResearchParameterEvaluator(ResearchBacktestParameterEvaluator):
    """Research evaluator for GoldenCrossStrategy parameter packs."""

    def __init__(self, precision_codec: PrecisionCodec | None = None) -> None:
        super().__init__(
            _golden_cross_strategy_factory,
            precision_codec=precision_codec,
        )


class GoldenCrossFastFitnessParameterEvaluator:
    """Evaluate GoldenCross candidates through the numeric fitness path."""

    def __init__(self) -> None:
        self._fitness_cache: dict[tuple, GoldenCrossFastFitnessEvaluator] = {}

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        if request.backtest is None:
            raise ValueError("backtest settings are required for fast fitness evaluation")

        evaluator = self._evaluator_for(request)
        result = evaluator.evaluate(
            short_window=int(candidate.param_pack["short_window"]),
            long_window=int(candidate.param_pack["long_window"]),
            quantity=Decimal(str(candidate.param_pack.get("quantity", "0.01"))),
        )
        metrics = {
            "total_pnl": result.total_pnl,
            "max_drawdown": result.max_drawdown,
            "total_trades": result.total_trades,
            "raw_trade_count": result.raw_trade_count,
            "win_rate": result.win_rate,
            "profit_factor": result.profit_factor,
            "gross_profit": result.gross_profit,
            "gross_loss": result.gross_loss,
            "fitness_mode": "golden_cross_fast",
        }
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=result.total_pnl,
            max_drawdown=result.max_drawdown,
            metrics=_json_safe(metrics),
        )

    def _evaluator_for(
        self,
        request: ParameterSearchJobRequest,
    ) -> GoldenCrossFastFitnessEvaluator:
        assert request.backtest is not None
        cache_key = _candle_cache_key(request)
        evaluator = self._fitness_cache.get(cache_key)
        if evaluator is None:
            data_source = CsvDataSource(
                file_path=request.backtest.candles_csv_path,
                product_id=request.product_id,
                timeframe=request.timeframe,
            )
            df = data_source.get_candles_df(
                request.product_id,
                request.timeframe,
                request.start_time,
                request.end_time,
            )
            evaluator = GoldenCrossFastFitnessEvaluator.from_dataframe(
                df,
                initial_balance=request.backtest.initial_balance,
                taker_fee=request.backtest.taker_fee,
            )
            self._fitness_cache[cache_key] = evaluator
        return evaluator


def _golden_cross_strategy_factory(
    strategy_id: str,
    product_id: str,
    timeframe: str,
    param_pack: dict[str, Any],
) -> BaseStrategy:
    try:
        short_window = int(param_pack["short_window"])
        long_window = int(param_pack["long_window"])
    except KeyError as exc:
        raise ValueError("candidate param_pack requires short_window and long_window") from exc

    return GoldenCrossStrategy(
        strategy_id,
        product_id,
        short_window=short_window,
        long_window=long_window,
        timeframe=timeframe,
        quantity=Decimal(str(param_pack.get("quantity", "0.01"))),
    )


def _candle_cache_key(request: ParameterSearchJobRequest) -> tuple:
    assert request.backtest is not None
    path = Path(request.backtest.candles_csv_path)
    stat = path.stat()
    return (
        str(path.resolve()),
        stat.st_mtime_ns,
        stat.st_size,
        request.product_id,
        request.timeframe,
        request.start_time,
        request.end_time,
    )


class ParameterSearchJobExecutor:
    """Submit and run parameter-search jobs through an injected evaluator."""

    def __init__(
        self,
        evaluator: ParameterSearchEvaluator,
        store: JobStore | None = None,
        *,
        max_workers: int = 2,
        run_inline: bool = False,
        recover_interrupted: bool = False,
        db_session_factory: SessionFactory | None = None,
    ) -> None:
        self.evaluator = evaluator
        self.store = store or InMemoryJobStore()
        if recover_interrupted:
            self.store.mark_interrupted_active_jobs(
                "Job interrupted before control plane startup"
            )
        self._run_inline = run_inline
        self._executor = None if run_inline else ThreadPoolExecutor(max_workers=max_workers)
        self._futures: dict[str, Future[JobRecord]] = {}
        self._futures_lock = Lock()
        self._db_session_factory = db_session_factory

    def submit_search(self, request: ParameterSearchJobRequest) -> JobRecord:
        job = self.store.create(kind=request.kind, request=request)
        if self._run_inline:
            return self._run_job(job.id, request)
        assert self._executor is not None
        future = self._executor.submit(self._run_job, job.id, request)
        with self._futures_lock:
            self._futures[job.id] = future
            if future.done():
                self._futures.pop(job.id, None)
        return job

    def cancel_search(self, job_id: str, reason: str | None = None) -> JobRecord:
        job = self.store.get(job_id)
        if job is None:
            raise KeyError(job_id)
        if job.status == JobStatus.RUNNING:
            raise ValueError("running jobs cannot be cancelled")
        if job.status != JobStatus.QUEUED:
            raise ValueError(f"{job.status.value.lower()} jobs cannot be cancelled")

        with self._futures_lock:
            future = self._futures.pop(job_id, None)
        if future is not None and not future.cancel():
            raise ValueError("job already started")
        return self.store.mark_cancelled(job_id, reason or "cancelled by operator")

    def retry_search(self, job_id: str) -> JobRecord:
        job = self.store.get(job_id)
        if job is None:
            raise KeyError(job_id)
        if job.kind != "parameter_search":
            raise ValueError(f"unsupported job kind: {job.kind}")
        if job.status not in {JobStatus.FAILED, JobStatus.CANCELLED}:
            raise ValueError(f"{job.status.value.lower()} jobs cannot be retried")

        request = ParameterSearchJobRequest.model_validate(job.request)
        return self.submit_search(request)

    def shutdown(self, wait: bool = True) -> None:
        if self._executor is not None:
            self._executor.shutdown(wait=wait, cancel_futures=False)

    def _run_job(self, job_id: str, request: ParameterSearchJobRequest) -> JobRecord:
        try:
            current = self.store.get(job_id)
            if current is not None and current.status == JobStatus.CANCELLED:
                return current
            self.store.mark_running(job_id)
            try:
                result = self._run_search(request)
            except Exception as exc:
                return self.store.mark_failed(job_id, str(exc))
            return self.store.mark_succeeded(job_id, result)
        finally:
            with self._futures_lock:
                self._futures.pop(job_id, None)

    def _run_search(self, request: ParameterSearchJobRequest) -> dict[str, object]:
        candidates = resolve_parameter_candidates(request)
        evaluations = [
            self.evaluator.evaluate(request, candidate)
            for candidate in candidates
        ]
        best = _select_best_candidate(request, evaluations)
        epoch_id = None
        if self._db_session_factory is not None:
            epoch_id = _record_evolution_epoch(
                self._db_session_factory,
                request,
                candidates,
                evaluations,
                best,
            )
        return _json_safe(
            {
                "strategy_id": request.strategy_id,
                "product_id": request.product_id,
                "timeframe": request.timeframe,
                "objective": request.objective,
                "seed": request.seed,
                "epoch_id": epoch_id,
                "resolved_candidates": candidates,
                "evaluations": evaluations,
                "best_candidate": best,
                "best_candidate_param_pack": _param_pack_for_candidate(
                    candidates,
                    best.candidate_id,
                ),
            }
        )


def _select_best_candidate(
    request: ParameterSearchJobRequest,
    evaluations: list[ParameterEvaluationResult],
) -> ParameterEvaluationResult:
    if request.objective in {"maximize_score", "maximize_return"}:
        return max(evaluations, key=lambda result: result.score_total)
    if request.objective == "minimize_drawdown":
        return min(
            evaluations,
            key=lambda result: (result.max_drawdown, -_decimal_key(result.score_total)),
        )
    raise ValueError(f"unsupported objective: {request.objective}")


def _decimal_key(value: Decimal) -> Decimal:
    return value


def _result_decimal(result: dict[str, Any], key: str) -> Decimal:
    value = result.get(key, "0")
    return Decimal(str(value))


def _param_pack_for_candidate(
    candidates: list[ParameterCandidate],
    candidate_id: str,
) -> dict[str, Any]:
    for candidate in candidates:
        if candidate.candidate_id == candidate_id:
            return candidate.param_pack
    raise ValueError(f"missing parameter pack for candidate: {candidate_id}")


def _record_evolution_epoch(
    session_factory: SessionFactory,
    request: ParameterSearchJobRequest,
    candidates: list[ParameterCandidate],
    evaluations: list[ParameterEvaluationResult],
    best: ParameterEvaluationResult,
) -> str:
    epoch_id = f"epoch_{datetime.now(UTC).strftime('%Y%m%d%H%M%S%f')}"
    started_at = datetime.now(UTC)
    finished_at = datetime.now(UTC)
    with session_factory() as session:
        _insert_evolution_epoch(
            session,
            epoch_id,
            request,
            candidates,
            best,
            started_at,
            finished_at,
        )
        for evaluation in evaluations:
            candidate = next(
                candidate
                for candidate in candidates
                if candidate.candidate_id == evaluation.candidate_id
            )
            session.add(
                GeneRecord(
                    strategy_id=request.strategy_id,
                    role=GeneRole.CHALLENGER.value,
                    param_pack=candidate.param_pack,
                    score_total=evaluation.score_total,
                    score_breakdown=evaluation.metrics,
                    max_drawdown=evaluation.max_drawdown,
                    epoch_id=epoch_id,
                )
            )
        session.commit()
    return epoch_id


def _insert_evolution_epoch(
    session: Session,
    epoch_id: str,
    request: ParameterSearchJobRequest,
    candidates: list[ParameterCandidate],
    best: ParameterEvaluationResult,
    started_at: datetime,
    finished_at: datetime,
) -> None:
    session.add(
        EvolutionEpoch(
            id=epoch_id,
            strategy_id=request.strategy_id,
            started_at=started_at,
            finished_at=finished_at,
            pop_size=len(candidates),
            max_generations=1,
            generations_run=1,
            best_score=best.score_total,
            seed=request.seed or 0,
            config_json={
                "objective": request.objective,
                "candidate_ids": [
                    candidate.candidate_id for candidate in candidates
                ],
            },
            status="completed",
            eval_pair=request.product_id,
            eval_start_date=datetime.fromtimestamp(
                request.start_time / 1000,
                tz=UTC,
            ).date(),
            eval_end_date=datetime.fromtimestamp(
                request.end_time / 1000,
                tz=UTC,
            ).date(),
            eval_timeframe=request.timeframe,
        )
    )
