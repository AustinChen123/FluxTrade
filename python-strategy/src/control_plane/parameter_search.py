from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from threading import Lock
from typing import Any, Iterable
from uuid import uuid4

from sqlalchemy.orm import Session

from src.control_plane.backtest_jobs import (
    SessionFactory,
    _json_safe,
)
from src.control_plane.evolution import (
    canonical_param_key,
    initial_population,
    next_population,
)
from src.control_plane.evolution_persistence import (
    _ensure_evolution_epoch,
    _load_evolution_checkpoint,
    _mark_evolution_aborted,
    _mark_evolution_completed,
    _persist_evolution_generation,
)
from src.control_plane.fitness import (
    WalkForwardMetricData,
    calculate_registered_fitness_inputs,
    deflated_sharpe_probability,
    evaluate_fitness_expression,
    expected_maximum_sharpe,
    fitness_metric_contract,
)
from src.control_plane.jobs import InMemoryJobStore, JobStore
from src.control_plane.models import (
    CsvSignalBacktestEvaluationConfig,
    EvaluationDatasetConfig,
    JobRecord,
    JobStatus,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.parameter_evaluation import (
    CsvSignalBacktestParameterEvaluator as CsvSignalBacktestParameterEvaluator,
    GoldenCrossFastFitnessParameterEvaluator as GoldenCrossFastFitnessParameterEvaluator,
    GoldenCrossResearchParameterEvaluator as GoldenCrossResearchParameterEvaluator,
    ParameterSearchEvaluator,
    ResearchBacktestParameterEvaluator as ResearchBacktestParameterEvaluator,
    WalkForwardWarmupEvaluator,
    _normalize_evaluation_result,
)
from src.control_plane.parameter_selection import (
    _drawdown_risk_key,
    _select_best_candidate,
)
from src.control_plane.search_space import resolve_parameter_candidates
from src.core.models import GeneRole
from src.core.orm_models import EvolutionEpoch, GeneRecord
from src.core.product_registry import CapitalModel


_FITNESS_SCORE_QUANTUM = Decimal("0.00000001")
_FITNESS_SCORE_MAX = Decimal("9999999999.99999999")


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
        evaluations = _apply_walk_forward_fitness(request, evaluations)
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
                evaluations = _apply_walk_forward_fitness(
                    request,
                    evaluations,
                    benchmark_evaluations=list(evaluation_cache.values()),
                )
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


def _evaluate_candidate_across_datasets(
    evaluator: ParameterSearchEvaluator,
    request: ParameterSearchJobRequest,
    candidate: ParameterCandidate,
) -> ParameterEvaluationResult:
    assert request.evaluation_set is not None
    dataset_results: dict[str, dict[str, Any]] = {}
    dataset_scores: dict[str, Decimal] = {}
    dataset_drawdowns: dict[str, Decimal] = {}

    for dataset in request.evaluation_set.resolved_datasets:
        dataset_request = _request_for_evaluation_dataset(request, dataset)
        if dataset.warmup_start_time is None:
            raw_evaluation = evaluator.evaluate(dataset_request, candidate)
        else:
            if not isinstance(evaluator, WalkForwardWarmupEvaluator):
                raise ValueError(
                    "evaluator does not support walk-forward warmup: "
                    f"{dataset.dataset_id}"
                )
            raw_evaluation = evaluator.evaluate_with_warmup(
                dataset_request,
                candidate,
                warmup_start_time=dataset.warmup_start_time,
            )
        evaluation = _normalize_evaluation_result(raw_evaluation)
        dataset_results[dataset.dataset_id] = evaluation.metrics
        dataset_scores[dataset.dataset_id] = evaluation.score_total
        dataset_drawdowns[dataset.dataset_id] = evaluation.max_drawdown

    metrics: dict[str, Any] = {
        "evaluation_mode": "evaluation_set",
        "aggregation": "sum_score_worst_drawdown",
        "dataset_scores": dataset_scores,
        "dataset_drawdowns": dataset_drawdowns,
        "datasets": dataset_results,
    }
    if request.fitness is not None:
        fitness_inputs, fitness_statistics = _walk_forward_inputs(
            request,
            dataset_scores,
            dataset_drawdowns,
            dataset_results,
        )
        metrics["fitness_inputs"] = fitness_inputs
        metrics["fitness_statistics"] = fitness_statistics

    return ParameterEvaluationResult(
        candidate_id=candidate.candidate_id,
        score_total=sum(dataset_scores.values(), Decimal("0")),
        max_drawdown=_worst_drawdown(dataset_drawdowns.values()),
        metrics=_json_safe(metrics),
    )


def _worst_drawdown(drawdowns: Iterable[Decimal]) -> Decimal:
    return max(drawdowns, key=_drawdown_risk_key, default=Decimal("0"))


def _apply_walk_forward_fitness(
    request: ParameterSearchJobRequest,
    evaluations: list[ParameterEvaluationResult],
    *,
    benchmark_evaluations: list[ParameterEvaluationResult] | None = None,
) -> list[ParameterEvaluationResult]:
    if request.fitness is None:
        return evaluations
    if not evaluations:
        return evaluations

    benchmark_population = benchmark_evaluations or evaluations
    candidate_sharpes = [
        Decimal(str(evaluation.metrics["fitness_inputs"]["daily_sharpe"]))
        for evaluation in benchmark_population
    ]
    independent_trials = request.fitness.independent_trials or _default_trial_count(
        request,
        evaluations,
    )
    benchmark_sharpe = expected_maximum_sharpe(
        candidate_sharpes,
        independent_trials=independent_trials,
    )

    scored = []
    for evaluation in evaluations:
        inputs = {
            name: Decimal(str(value))
            for name, value in evaluation.metrics["fitness_inputs"].items()
        }
        statistics = evaluation.metrics["fitness_statistics"]
        inputs["deflated_sharpe"] = deflated_sharpe_probability(
            observed_sharpe=inputs["daily_sharpe"],
            benchmark_sharpe=benchmark_sharpe,
            observations=int(statistics["observations"]),
            skewness=Decimal(str(statistics["skewness"])),
            kurtosis=Decimal(str(statistics["kurtosis"])),
        )
        score = _canonical_fitness_score(
            evaluate_fitness_expression(
                request.fitness.expression,
                inputs,
            )
        )
        if not score.is_finite():
            raise ValueError("walk-forward fitness must be finite")
        metrics = {
            **evaluation.metrics,
            "aggregation": "registered_walk_forward_fitness",
            "fitness_inputs": inputs,
            "fitness": {
                "expression": request.fitness.expression,
                "independent_trials": independent_trials,
                "benchmark_sharpe": benchmark_sharpe,
                "metric_contract": fitness_metric_contract(),
                "inputs": inputs,
                "score": score,
            },
        }
        scored.append(
            evaluation.model_copy(
                update={
                    "score_total": score,
                    "metrics": _json_safe(metrics),
                }
            )
        )
    return scored


def _default_trial_count(
    request: ParameterSearchJobRequest,
    evaluations: list[ParameterEvaluationResult],
) -> int:
    if request.evolution is not None:
        return request.evolution.population_size * request.evolution.max_generations
    return len(evaluations)


def _canonical_fitness_score(score: Decimal) -> Decimal:
    if not score.is_finite():
        raise ValueError("walk-forward fitness must be finite")
    if abs(score) > _FITNESS_SCORE_MAX:
        raise ValueError("walk-forward fitness exceeds Numeric(18,8) range")
    return score.quantize(_FITNESS_SCORE_QUANTUM)


def _walk_forward_inputs(
    request: ParameterSearchJobRequest,
    dataset_scores: dict[str, Decimal],
    dataset_drawdowns: dict[str, Decimal],
    dataset_results: dict[str, dict[str, Any]],
) -> tuple[dict[str, Decimal], dict[str, Decimal | int]]:
    assert request.evaluation_set is not None
    scores = list(dataset_scores.values())
    returns = []
    drawdown_percentages = []
    daily_sharpes = []
    annualized_sharpes = []
    trade_counts = []
    pooled_moments = _empty_return_moments()
    yearly_returns: dict[str, Decimal] = {}
    mean_r_values = []

    for dataset in request.evaluation_set.resolved_datasets:
        backtest = _backtest_for_evaluation_dataset(request, dataset)
        if backtest is None:
            raise ValueError(
                f"evaluation dataset {dataset.dataset_id} requires backtest settings"
            )
        initial_balance = backtest.initial_balance
        returns.append(dataset_scores[dataset.dataset_id] / initial_balance)
        drawdown_percentages.append(
            dataset_drawdowns[dataset.dataset_id] / initial_balance
        )

        metrics = dataset_results[dataset.dataset_id]
        missing = {
            "daily_return_moments",
            "closed_trade_count",
            "equity_sample_count",
            "yearly_mark_to_market_returns",
        } - set(metrics)
        if missing:
            names = ", ".join(sorted(missing))
            raise ValueError(
                f"walk-forward dataset {dataset.dataset_id} missing metrics: {names}"
            )
        if int(metrics["equity_sample_count"]) < 1:
            raise ValueError(
                f"walk-forward dataset {dataset.dataset_id} has no scoring candles"
            )
        fold_moments = _normalize_return_moments(metrics["daily_return_moments"])
        _add_return_moments(pooled_moments, fold_moments)
        fold_statistics = _return_statistics(fold_moments)
        daily_sharpes.append(fold_statistics["sharpe"])
        annualized_sharpes.append(
            fold_statistics["sharpe"] * Decimal(365).sqrt()
        )
        trade_count = int(metrics["closed_trade_count"])
        trade_counts.append(trade_count)
        for year, value in metrics["yearly_mark_to_market_returns"].items():
            year = str(year)
            if len(year) != 4 or not year.isdigit():
                raise ValueError(
                    f"invalid yearly return key for {dataset.dataset_id}: {year}"
                )
            yearly_return = Decimal(str(value))
            yearly_returns[year] = (
                (Decimal("1") + yearly_returns.get(year, Decimal("0")))
                * (Decimal("1") + yearly_return)
                - Decimal("1")
            )
        if "mean_r" in metrics:
            mean_r_values.append(Decimal(str(metrics["mean_r"])))

    pooled_statistics = _return_statistics(pooled_moments)

    inputs = calculate_registered_fitness_inputs(
        WalkForwardMetricData(
            scores=tuple(scores),
            returns=tuple(returns),
            drawdown_percentages=tuple(drawdown_percentages),
            daily_sharpes=tuple(daily_sharpes),
            annualized_sharpes=tuple(annualized_sharpes),
            trade_counts=tuple(trade_counts),
            pooled_daily_sharpe=pooled_statistics["sharpe"],
            yearly_returns=yearly_returns,
            mean_r_values=tuple(mean_r_values),
        )
    )

    statistics: dict[str, Decimal | int] = {
        "observations": pooled_moments["count"],
        "skewness": pooled_statistics["skewness"],
        "kurtosis": pooled_statistics["kurtosis"],
    }
    return inputs, statistics


def _empty_return_moments() -> dict[str, Decimal | int]:
    return {
        "count": 0,
        "sum": Decimal("0"),
        "sum_squares": Decimal("0"),
        "sum_cubes": Decimal("0"),
        "sum_fourth": Decimal("0"),
    }


def _normalize_return_moments(value: Any) -> dict[str, Decimal | int]:
    if not isinstance(value, dict):
        raise ValueError("daily_return_moments must be an object")
    required = {"count", "sum", "sum_squares", "sum_cubes", "sum_fourth"}
    missing = required - set(value)
    if missing:
        raise ValueError(
            "daily_return_moments missing fields: "
            + ", ".join(sorted(missing))
        )
    count = int(value["count"])
    if count < 0:
        raise ValueError("daily_return_moments count cannot be negative")
    normalized: dict[str, Decimal | int] = {"count": count}
    for name in required - {"count"}:
        decimal_value = Decimal(str(value[name]))
        if not decimal_value.is_finite():
            raise ValueError("daily_return_moments values must be finite")
        normalized[name] = decimal_value
    return normalized


def _add_return_moments(
    target: dict[str, Decimal | int],
    source: dict[str, Decimal | int],
) -> None:
    target["count"] = int(target["count"]) + int(source["count"])
    for name in ("sum", "sum_squares", "sum_cubes", "sum_fourth"):
        target[name] = Decimal(target[name]) + Decimal(source[name])


def _return_statistics(
    moments: dict[str, Decimal | int],
) -> dict[str, Decimal]:
    count = int(moments["count"])
    if count < 2:
        return {
            "sharpe": Decimal("0"),
            "skewness": Decimal("0"),
            "kurtosis": Decimal("3"),
        }
    sample_count = Decimal(count)
    total = Decimal(moments["sum"])
    raw_second = Decimal(moments["sum_squares"]) / sample_count
    raw_third = Decimal(moments["sum_cubes"]) / sample_count
    raw_fourth = Decimal(moments["sum_fourth"]) / sample_count
    mean = total / sample_count
    second_moment = max(raw_second - mean * mean, Decimal("0"))
    sample_variance = second_moment * sample_count / Decimal(count - 1)
    sharpe = mean / sample_variance.sqrt() if sample_variance > 0 else Decimal("0")
    if second_moment == 0:
        return {
            "sharpe": sharpe,
            "skewness": Decimal("0"),
            "kurtosis": Decimal("3"),
        }
    third_moment = raw_third - Decimal("3") * mean * raw_second + Decimal("2") * (
        mean**3
    )
    fourth_moment = (
        raw_fourth
        - Decimal("4") * mean * raw_third
        + Decimal("6") * mean * mean * raw_second
        - Decimal("3") * (mean**4)
    )
    return {
        "sharpe": sharpe,
        "skewness": third_moment / (second_moment.sqrt() ** 3),
        "kurtosis": fourth_moment / (second_moment**2),
    }


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
        "walk_forward": (
            None
            if request.evaluation_set.walk_forward is None
            else _json_safe(
                request.evaluation_set.walk_forward.model_dump(
                    mode="json",
                    exclude_none=True,
                )
            )
        ),
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
            for dataset in request.evaluation_set.resolved_datasets
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
