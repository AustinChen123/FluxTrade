from __future__ import annotations

from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from threading import Lock
from typing import Any, Callable, Iterable, Protocol
from uuid import uuid4

from sqlalchemy.orm import Session

from src.control_plane.backtest_jobs import BacktestJobExecutor, SessionFactory, _json_safe
from src.control_plane.evolution import (
    canonical_param_key,
    initial_population,
    next_population,
)
from src.control_plane.jobs import InMemoryJobStore, JobStore
from src.control_plane.models import (
    BacktestJobRequest,
    CsvSignalBacktestEvaluationConfig,
    EvaluationDatasetConfig,
    JobRecord,
    JobStatus,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.search_space import resolve_parameter_candidates
from src.core.capital_allocator import CapitalAllocator
from src.core.data_sources.csv_source import CsvDataSource
from src.core.data_sources.memory import MemoryDataSource
from src.core.golden_cross_fast_fitness import GoldenCrossFastFitnessEvaluator
from src.core.models import GeneRole
from src.core.orm_models import EvolutionEpoch, GeneRecord
from src.core.precision import PrecisionCodec
from src.core.product_registry import CapitalModel
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


@dataclass(frozen=True)
class _EvolutionCheckpoint:
    generations_run: int
    population: list[ParameterCandidate]
    evaluations: list[ParameterEvaluationResult]
    evaluation_cache: dict[
        tuple[tuple[str, str], ...],
        ParameterEvaluationResult,
    ]


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
        _reject_research_runner_settings(request, "CSV-signal evaluation")
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
            instrument=request.backtest.instrument,
            write_reports=request.backtest.write_reports,
        )
        result = self._backtest_executor.run_backtest_request(
            backtest_request,
            max_drawdown_limit=None if request.evolution is not None else 0.20,
        )
        score = _result_decimal(result, "total_pnl")
        max_drawdown = _result_decimal(result, "max_drawdown")
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            max_drawdown=_drawdown_loss_magnitude(max_drawdown),
            metrics=_normalize_metrics_drawdown(result),
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
        strategy = self._strategy_factory(
            f"{request.strategy_id}_{candidate.candidate_id}",
            request.product_id,
            request.timeframe,
            candidate.param_pack,
        )
        capital_allocator = _capital_allocator_for(request, strategy.strategy_id)
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
            capital_allocator=capital_allocator,
            instrument_spec=(
                request.backtest.instrument.to_instrument_spec(request.product_id)
                if request.backtest.instrument is not None
                else None
            ),
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
            max_drawdown=_drawdown_loss_magnitude(
                _result_decimal(result, "max_drawdown")
            ),
            metrics=_normalize_metrics_drawdown(metrics),
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
        _reject_research_runner_settings(request, "fast fitness evaluation")
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
            max_drawdown=_drawdown_loss_magnitude(result.max_drawdown),
            metrics=_normalize_metrics_drawdown(metrics),
        )

    def _evaluator_for(
        self,
        request: ParameterSearchJobRequest,
    ) -> GoldenCrossFastFitnessEvaluator:
        assert request.backtest is not None
        cache_key = _fitness_cache_key(request)
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
                instrument_spec=(
                    request.backtest.instrument.to_instrument_spec(request.product_id)
                    if request.backtest.instrument is not None
                    else None
                ),
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


def _fitness_cache_key(request: ParameterSearchJobRequest) -> tuple:
    assert request.backtest is not None
    instrument = request.backtest.instrument
    return (
        _candle_cache_key(request),
        request.backtest.initial_balance,
        request.backtest.taker_fee,
        instrument.multiplier if instrument is not None else None,
        instrument.fee_model if instrument is not None else None,
        instrument.capital_model if instrument is not None else None,
        instrument.capital_per_contract if instrument is not None else None,
    )


def _capital_allocator_for(
    request: ParameterSearchJobRequest,
    strategy_id: str,
) -> CapitalAllocator | None:
    if request.research_runner is None:
        return None
    capital_allocation = request.research_runner.capital_allocation
    if capital_allocation is None:
        return None
    if request.backtest is None:
        raise ValueError("backtest settings are required for capital allocation")
    initial_balance = request.backtest.initial_balance
    if capital_allocation > initial_balance:
        raise ValueError(
            "research_runner.capital_allocation cannot exceed backtest.initial_balance"
        )

    allocator = CapitalAllocator(total_balance=initial_balance)
    allocator.allocate(strategy_id, capital_allocation)
    return allocator


def _reject_research_runner_settings(
    request: ParameterSearchJobRequest,
    evaluator_name: str,
) -> None:
    if request.research_runner is None:
        return
    raise ValueError(
        "research_runner settings require ResearchBacktestParameterEvaluator; "
        f"{evaluator_name} does not support them"
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
        self._active_evolution_epochs: set[str] = set()
        self._active_evolution_lock = Lock()
        self._db_session_factory = db_session_factory

    def submit_search(self, request: ParameterSearchJobRequest) -> JobRecord:
        if request.evolution is not None and request.evolution.epoch_id is None:
            request = request.model_copy(
                update={
                    "evolution": request.evolution.model_copy(
                        update={"epoch_id": f"epoch_{uuid4().hex}"}
                    )
                }
            )
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
        if request.evolution is not None:
            return self._run_evolution(request)
        candidates = resolve_parameter_candidates(request)
        if request.evaluation_set is not None:
            evaluations = [
                _evaluate_candidate_across_datasets(
                    self.evaluator,
                    request,
                    candidate,
                )
                for candidate in candidates
            ]
        else:
            evaluations = [
                _normalize_evaluation_result(
                    self.evaluator.evaluate(request, candidate)
                )
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
                "research_runner": _research_runner_result_payload(request),
                "evaluation_set": _evaluation_set_result_payload(request),
                "resolved_candidates": candidates,
                "evaluations": evaluations,
                "best_candidate": best,
                "best_candidate_param_pack": _param_pack_for_candidate(
                    candidates,
                    best.candidate_id,
                ),
            }
        )

    def _run_evolution(
        self,
        request: ParameterSearchJobRequest,
    ) -> dict[str, object]:
        if self._db_session_factory is None:
            raise ValueError("evolution requires db_session_factory")
        assert request.evolution is not None
        assert request.evolution.epoch_id is not None
        assert request.search_space is not None
        seed = request.seed if request.seed is not None else 0
        epoch_id = request.evolution.epoch_id

        self._claim_evolution_epoch(epoch_id)
        try:
            _ensure_evolution_epoch(self._db_session_factory, request)
        except Exception:
            self._release_evolution_epoch(epoch_id)
            raise
        try:
            checkpoint = _load_evolution_checkpoint(
                self._db_session_factory,
                request,
            )
            population = checkpoint.population
            evaluations = checkpoint.evaluations
            evaluation_cache = checkpoint.evaluation_cache
            next_generation = checkpoint.generations_run

            if next_generation == 0:
                population = initial_population(
                    request.search_space,
                    request.evolution,
                    seed=seed,
                )

            while next_generation < request.evolution.max_generations:
                if next_generation > 0:
                    population = next_population(
                        request.search_space,
                        request.evolution,
                        population,
                        evaluations,
                        objective=request.objective,
                        seed=seed,
                        generation_index=next_generation,
                    )
                evaluations = [
                    _evaluate_evolution_candidate(
                        self.evaluator,
                        request,
                        candidate,
                        evaluation_cache,
                    )
                    for candidate in population
                ]
                _persist_evolution_generation(
                    self._db_session_factory,
                    request,
                    next_generation,
                    population,
                    evaluations,
                )
                next_generation += 1

            best = _select_best_candidate(request, evaluations)
            _mark_evolution_completed(
                self._db_session_factory,
                epoch_id,
                best.score_total,
            )
            return _evolution_result_payload(
                request,
                population,
                evaluations,
                best,
                next_generation,
            )
        except Exception:
            _mark_evolution_aborted(self._db_session_factory, epoch_id)
            raise
        finally:
            self._release_evolution_epoch(epoch_id)

    def _claim_evolution_epoch(self, epoch_id: str) -> None:
        with self._active_evolution_lock:
            if epoch_id in self._active_evolution_epochs:
                raise ValueError("evolution epoch is already running")
            self._active_evolution_epochs.add(epoch_id)

    def _release_evolution_epoch(self, epoch_id: str) -> None:
        with self._active_evolution_lock:
            self._active_evolution_epochs.discard(epoch_id)


def _ensure_evolution_epoch(
    session_factory: SessionFactory,
    request: ParameterSearchJobRequest,
) -> None:
    assert request.evolution is not None
    assert request.evolution.epoch_id is not None
    epoch_id = request.evolution.epoch_id
    expected_config = _evolution_config_payload(request)
    with session_factory() as session:
        epoch = session.get(EvolutionEpoch, epoch_id)
        if epoch is None:
            session.add(
                EvolutionEpoch(
                    id=epoch_id,
                    strategy_id=request.strategy_id,
                    started_at=datetime.now(UTC),
                    finished_at=None,
                    pop_size=request.evolution.population_size,
                    max_generations=request.evolution.max_generations,
                    generations_run=0,
                    best_score=None,
                    seed=request.seed if request.seed is not None else 0,
                    config_json=expected_config,
                    status="running",
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
            session.commit()
            return
        _validate_evolution_epoch(epoch, request, expected_config)
        if epoch.status != "completed":
            epoch.status = "running"
            epoch.finished_at = None
            session.commit()


def _validate_evolution_epoch(
    epoch: EvolutionEpoch,
    request: ParameterSearchJobRequest,
    expected_config: dict[str, Any],
) -> None:
    assert request.evolution is not None
    expected = (
        request.strategy_id,
        request.product_id,
        request.timeframe,
        request.evolution.population_size,
        request.evolution.max_generations,
        request.seed if request.seed is not None else 0,
        expected_config,
    )
    actual = (
        epoch.strategy_id,
        epoch.eval_pair,
        epoch.eval_timeframe,
        epoch.pop_size,
        epoch.max_generations,
        epoch.seed,
        epoch.config_json,
    )
    if actual != expected:
        raise ValueError("evolution checkpoint does not match request")


def _load_evolution_checkpoint(
    session_factory: SessionFactory,
    request: ParameterSearchJobRequest,
) -> _EvolutionCheckpoint:
    assert request.evolution is not None
    assert request.evolution.epoch_id is not None
    with session_factory() as session:
        epoch = session.get(EvolutionEpoch, request.evolution.epoch_id)
        if epoch is None:
            raise ValueError("evolution epoch was not created")
        generations_run = epoch.generations_run or 0
        if not 0 <= generations_run <= request.evolution.max_generations:
            raise ValueError("evolution checkpoint generation is out of range")
        records = (
            session.query(GeneRecord)
            .filter(GeneRecord.epoch_id == epoch.id)
            .order_by(GeneRecord.generation_index, GeneRecord.candidate_id)
            .all()
        )

    expected_generation_counts = {
        generation_index: request.evolution.population_size
        for generation_index in range(generations_run)
    }
    actual_generation_counts = Counter(
        record.generation_index for record in records
    )
    if actual_generation_counts != expected_generation_counts:
        raise ValueError("evolution checkpoint contains an incomplete generation")

    evaluation_cache = {}
    population = []
    evaluations = []
    for record in records:
        param_pack = _restore_param_pack(record.param_pack, request)
        evaluation = ParameterEvaluationResult(
            candidate_id=record.candidate_id,
            score_total=record.score_total,
            max_drawdown=record.max_drawdown,
            metrics=record.score_breakdown,
        )
        evaluation_cache[canonical_param_key(param_pack)] = evaluation
        if record.generation_index == generations_run - 1:
            population.append(
                ParameterCandidate(
                    candidate_id=record.candidate_id,
                    param_pack=param_pack,
                )
            )
            evaluations.append(evaluation)
    return _EvolutionCheckpoint(
        generations_run=generations_run,
        population=population,
        evaluations=evaluations,
        evaluation_cache=evaluation_cache,
    )


def _evaluate_evolution_candidate(
    evaluator: ParameterSearchEvaluator,
    request: ParameterSearchJobRequest,
    candidate: ParameterCandidate,
    cache: dict[tuple[tuple[str, str], ...], ParameterEvaluationResult],
) -> ParameterEvaluationResult:
    key = canonical_param_key(candidate.param_pack)
    cached = cache.get(key)
    if cached is not None:
        return cached.model_copy(update={"candidate_id": candidate.candidate_id})
    if request.evaluation_set is not None:
        evaluation = _evaluate_candidate_across_datasets(
            evaluator,
            request,
            candidate,
        )
    else:
        evaluation = _normalize_evaluation_result(
            evaluator.evaluate(request, candidate)
        )
    cache[key] = evaluation
    return evaluation


def _persist_evolution_generation(
    session_factory: SessionFactory,
    request: ParameterSearchJobRequest,
    generation_index: int,
    population: list[ParameterCandidate],
    evaluations: list[ParameterEvaluationResult],
) -> None:
    assert request.evolution is not None
    assert request.evolution.epoch_id is not None
    best = _select_best_candidate(request, evaluations)
    evaluation_by_id = {
        evaluation.candidate_id: evaluation for evaluation in evaluations
    }
    with session_factory() as session:
        epoch = session.get(EvolutionEpoch, request.evolution.epoch_id)
        if epoch is None:
            raise ValueError("evolution epoch was not created")
        if (epoch.generations_run or 0) != generation_index:
            raise ValueError("evolution checkpoint generation changed")
        for candidate in population:
            evaluation = evaluation_by_id[candidate.candidate_id]
            session.add(
                GeneRecord(
                    strategy_id=request.strategy_id,
                    role=GeneRole.CHALLENGER.value,
                    param_pack=_json_safe(candidate.param_pack),
                    score_total=evaluation.score_total,
                    score_breakdown=_json_safe(evaluation.metrics),
                    max_drawdown=evaluation.max_drawdown,
                    generation_index=generation_index,
                    candidate_id=candidate.candidate_id,
                    epoch_id=epoch.id,
                )
            )
        epoch.generations_run = generation_index + 1
        epoch.best_score = best.score_total
        session.commit()


def _mark_evolution_completed(
    session_factory: SessionFactory,
    epoch_id: str,
    best_score: Decimal,
) -> None:
    with session_factory() as session:
        epoch = session.get(EvolutionEpoch, epoch_id)
        if epoch is None:
            raise ValueError("evolution epoch was not created")
        epoch.status = "completed"
        epoch.finished_at = datetime.now(UTC)
        epoch.best_score = best_score
        session.commit()


def _mark_evolution_aborted(
    session_factory: SessionFactory,
    epoch_id: str,
) -> None:
    with session_factory() as session:
        epoch = session.get(EvolutionEpoch, epoch_id)
        if epoch is None or epoch.status == "completed":
            return
        epoch.status = "aborted"
        epoch.finished_at = datetime.now(UTC)
        session.commit()


def _evolution_result_payload(
    request: ParameterSearchJobRequest,
    population: list[ParameterCandidate],
    evaluations: list[ParameterEvaluationResult],
    best: ParameterEvaluationResult,
    generations_run: int,
) -> dict[str, object]:
    assert request.evolution is not None
    return _json_safe(
        {
            "strategy_id": request.strategy_id,
            "product_id": request.product_id,
            "timeframe": request.timeframe,
            "objective": request.objective,
            "seed": request.seed,
            "epoch_id": request.evolution.epoch_id,
            "generations_run": generations_run,
            "resolved_candidates": population,
            "evaluations": evaluations,
            "best_candidate": best,
            "best_candidate_param_pack": _param_pack_for_candidate(
                population,
                best.candidate_id,
            ),
        }
    )


def _evolution_config_payload(
    request: ParameterSearchJobRequest,
) -> dict[str, Any]:
    return request.model_dump(mode="json")


def _restore_param_pack(
    raw_param_pack: dict[str, Any],
    request: ParameterSearchJobRequest,
) -> dict[str, Any]:
    assert request.search_space is not None
    restored = {}
    for name, dimension in request.search_space.parameters.items():
        value = raw_param_pack[name]
        if dimension.type == "decimal":
            restored[name] = Decimal(str(value))
        elif dimension.type == "integer":
            restored[name] = int(value)
        else:
            restored[name] = value
    return restored


def _select_best_candidate(
    request: ParameterSearchJobRequest,
    evaluations: list[ParameterEvaluationResult],
) -> ParameterEvaluationResult:
    if request.objective in {"maximize_score", "maximize_return"}:
        return max(evaluations, key=lambda result: result.score_total)
    if request.objective == "minimize_drawdown":
        return min(
            evaluations,
            key=lambda result: (
                _drawdown_risk_key(result.max_drawdown),
                -_decimal_key(result.score_total),
            ),
        )
    raise ValueError(f"unsupported objective: {request.objective}")


def _evaluate_candidate_across_datasets(
    evaluator: ParameterSearchEvaluator,
    request: ParameterSearchJobRequest,
    candidate: ParameterCandidate,
) -> ParameterEvaluationResult:
    assert request.evaluation_set is not None
    dataset_results: dict[str, dict[str, Any]] = {}
    dataset_scores: dict[str, Decimal] = {}
    dataset_drawdowns: dict[str, Decimal] = {}

    for dataset in request.evaluation_set.datasets:
        dataset_request = _request_for_evaluation_dataset(request, dataset)
        evaluation = _normalize_evaluation_result(
            evaluator.evaluate(dataset_request, candidate)
        )
        dataset_results[dataset.dataset_id] = evaluation.metrics
        dataset_scores[dataset.dataset_id] = evaluation.score_total
        dataset_drawdowns[dataset.dataset_id] = evaluation.max_drawdown

    return ParameterEvaluationResult(
        candidate_id=candidate.candidate_id,
        score_total=sum(dataset_scores.values(), Decimal("0")),
        max_drawdown=_worst_drawdown(dataset_drawdowns.values()),
        metrics=_json_safe(
            {
                "evaluation_mode": "evaluation_set",
                "aggregation": "sum_score_worst_drawdown",
                "dataset_scores": dataset_scores,
                "dataset_drawdowns": dataset_drawdowns,
                "datasets": dataset_results,
            }
        ),
    )


def _worst_drawdown(drawdowns: Iterable[Decimal]) -> Decimal:
    return max(drawdowns, key=_drawdown_risk_key, default=Decimal("0"))


def _normalize_evaluation_result(
    evaluation: ParameterEvaluationResult,
) -> ParameterEvaluationResult:
    max_drawdown = _drawdown_loss_magnitude(evaluation.max_drawdown)
    metrics = _normalize_metrics_drawdown(evaluation.metrics)
    if max_drawdown == evaluation.max_drawdown and metrics == evaluation.metrics:
        return evaluation
    return evaluation.model_copy(
        update={
            "max_drawdown": max_drawdown,
            "metrics": metrics,
        }
    )


def _normalize_metrics_drawdown(metrics: dict[str, Any]) -> dict[str, Any]:
    if "max_drawdown" not in metrics:
        return _json_safe(metrics)

    normalized = dict(metrics)
    normalized["max_drawdown"] = _drawdown_loss_magnitude(
        Decimal(str(normalized["max_drawdown"]))
    )
    return _json_safe(normalized)


def _drawdown_risk_key(drawdown: Decimal) -> Decimal:
    return abs(drawdown)


def _drawdown_loss_magnitude(drawdown: Decimal) -> Decimal:
    return abs(drawdown)


def _request_for_evaluation_dataset(
    request: ParameterSearchJobRequest,
    dataset: EvaluationDatasetConfig,
) -> ParameterSearchJobRequest:
    backtest = _backtest_for_evaluation_dataset(request, dataset)
    if backtest is None:
        raise ValueError(
            f"evaluation dataset {dataset.dataset_id} requires backtest settings"
        )
    return request.model_copy(
        update={
            "product_id": dataset.product_id,
            "timeframe": dataset.timeframe,
            "start_time": dataset.start_time,
            "end_time": dataset.end_time,
            "backtest": backtest,
            "evaluation_set": None,
        },
        deep=True,
    )


def _backtest_for_evaluation_dataset(
    request: ParameterSearchJobRequest,
    dataset: EvaluationDatasetConfig,
) -> CsvSignalBacktestEvaluationConfig | None:
    if dataset.backtest is None:
        return request.backtest
    overrides = _dataset_backtest_override_values(dataset)
    if request.backtest is None:
        return CsvSignalBacktestEvaluationConfig.model_validate(overrides)

    shared = request.backtest.model_dump()
    instrument_overrides = overrides.pop("instrument", None)
    if instrument_overrides is not None:
        instrument = {
            **(shared.get("instrument") or {}),
            **instrument_overrides,
        }
        if (
            instrument_overrides.get("capital_model") == CapitalModel.NOTIONAL
            and "capital_per_contract" not in instrument_overrides
        ):
            instrument["capital_per_contract"] = None
        shared["instrument"] = instrument

    return CsvSignalBacktestEvaluationConfig.model_validate(
        {
            **shared,
            **overrides,
        }
    )


def _evaluation_set_result_payload(
    request: ParameterSearchJobRequest,
) -> dict[str, Any] | None:
    if request.evaluation_set is None:
        return None
    return {
        "datasets": [
            {
                "dataset_id": dataset.dataset_id,
                "product_id": dataset.product_id,
                "timeframe": dataset.timeframe,
                "start_time": dataset.start_time,
                "end_time": dataset.end_time,
                "warmup_start_time": dataset.warmup_start_time,
                "metadata": dataset.metadata,
                "backtest": _dataset_backtest_override_payload(dataset),
                "resolved_backtest": _resolved_dataset_backtest_payload(request, dataset),
            }
            for dataset in request.evaluation_set.datasets
        ]
    }


def _research_runner_result_payload(
    request: ParameterSearchJobRequest,
) -> dict[str, Any] | None:
    if request.research_runner is None:
        return None
    return _json_safe(request.research_runner.model_dump(mode="json", exclude_none=True))


def _dataset_backtest_override_payload(
    dataset: EvaluationDatasetConfig,
) -> dict[str, Any] | None:
    if dataset.backtest is None:
        return None
    return _json_safe(_dataset_backtest_override_values(dataset))


def _dataset_backtest_override_values(
    dataset: EvaluationDatasetConfig,
) -> dict[str, Any]:
    if dataset.backtest is None:
        return {}
    return dataset.backtest.model_dump(exclude_unset=True, exclude_none=True)


def _resolved_dataset_backtest_payload(
    request: ParameterSearchJobRequest,
    dataset: EvaluationDatasetConfig,
) -> dict[str, Any] | None:
    backtest = _backtest_for_evaluation_dataset(request, dataset)
    if backtest is None:
        return None
    return _json_safe(backtest.model_dump(mode="json", exclude_none=True))


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
                    param_pack=_json_safe(candidate.param_pack),
                    score_total=evaluation.score_total,
                    score_breakdown=_json_safe(evaluation.metrics),
                    max_drawdown=evaluation.max_drawdown,
                    generation_index=0,
                    candidate_id=candidate.candidate_id,
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
                "research_runner": _research_runner_result_payload(request),
                "evaluation_set": _evaluation_set_result_payload(request),
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
