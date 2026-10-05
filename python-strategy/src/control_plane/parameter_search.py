from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor, wait as wait_futures
from datetime import UTC, datetime
from decimal import Decimal
from threading import Lock
from typing import Any, Iterable, cast
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
from src.control_plane.evaluation_data import SealedDatasetRejectedError
from src.control_plane.evolution_persistence import (
    _ensure_evolution_epoch,
    _load_evolution_checkpoint,
    _mark_evolution_aborted,
    _mark_evolution_completed,
    _persist_evolution_generation,
)
from src.control_plane.invalidation import ControlPlaneInvalidationHub
from src.control_plane.ga_lifecycle import GaJobRecord, GaJobStatus
from src.control_plane.ga_profile import (
    CompiledGaProfileRequest,
    _input_digest,
    get_golden_cross_profile,
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
    ParameterSearchEvaluatorRegistry,
    ParameterSearchRequestValidator,
    ResearchBacktestParameterEvaluator as ResearchBacktestParameterEvaluator,
    WalkForwardWarmupEvaluator,
    _normalize_evaluation_result,
)
from src.control_plane.parameter_fitness import (
    _add_return_moments as _add_return_moments,
    _apply_walk_forward_fitness as _apply_walk_forward_fitness,
    _canonical_fitness_score as _canonical_fitness_score,
    _default_trial_count as _default_trial_count,
    _empty_return_moments as _empty_return_moments,
    _normalize_return_moments as _normalize_return_moments,
    _return_statistics as _return_statistics,
    _walk_forward_inputs as _calculate_walk_forward_inputs,
)
from src.control_plane.parameter_selection import (
    _drawdown_risk_key,
    _select_best_candidate,
)
from src.control_plane.search_space import resolve_parameter_candidates
from src.core.models import GeneRole
from src.core.data_sources.research_database import ResearchDatabaseDataSource
from src.core.orm_models import EvolutionEpoch, GeneRecord
from src.core.product_registry import CapitalModel


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
        invalidation_hub: ControlPlaneInvalidationHub | None = None,
    ) -> None:
        self.evaluator = evaluator
        self.store = store or InMemoryJobStore()
        if recover_interrupted:
            self.store.mark_interrupted_active_jobs(
                "Job interrupted before control plane startup"
            )
            self.store.interrupt_ga_jobs("control_plane_interrupted")
        self._run_inline = run_inline
        self._executor = None if run_inline else ThreadPoolExecutor(max_workers=max_workers)
        self._futures: dict[str, Future[JobRecord]] = {}
        self._ga_futures: set[Future[GaJobRecord]] = set()
        self._ga_futures_lock = Lock()
        self._futures_lock = Lock()
        self._closed = False
        self._ga_job_locks: dict[str, Lock] = {}
        self._active_evolution_epochs: set[str] = set()
        self._active_evolution_lock = Lock()
        self._db_session_factory = db_session_factory
        self._invalidation_hub = invalidation_hub

    def submit_search(self, request: ParameterSearchJobRequest) -> JobRecord:
        if request.market_data is not None and not isinstance(
            self.evaluator,
            ParameterSearchEvaluatorRegistry,
        ):
            raise SealedDatasetRejectedError(
                "sealed dataset requires an explicitly configured evaluator registry"
            )
        if isinstance(self.evaluator, ParameterSearchRequestValidator):
            self.evaluator.validate_request(request)
        if request.evolution is not None and request.evolution.epoch_id is None:
            request = request.model_copy(
                update={
                    "evolution": request.evolution.model_copy(
                        update={"epoch_id": f"epoch_{uuid4().hex}"}
                    )
                }
            )
        with self._futures_lock:
            if self._closed:
                raise RuntimeError("parameter-search executor is shut down")
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

    def cancel_search(self, job_id: str, reason: str | None = None) -> JobRecord:
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
            with self._ga_futures_lock:
                ga_futures = tuple(self._ga_futures)
            futures = tuple(
                cast(Future[Any], future)
                for future in (*self._futures.values(), *ga_futures)
            )
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

    def dispatch_ga_job(
        self,
        job_id: str,
        expected_version: int,
    ) -> GaJobRecord | Future[GaJobRecord]:
        """Dispatch an existing queued GA record through the shared executor."""
        if not isinstance(job_id, str) or not job_id.strip():
            raise ValueError("job_id must be non-empty")
        with self._futures_lock:
            if self._closed:
                raise RuntimeError("parameter-search executor is shut down")
            dispatch_lock = self._ga_job_locks.setdefault(job_id, Lock())
            if self._run_inline:
                return self._run_ga_job(job_id, expected_version, dispatch_lock)
            assert self._executor is not None
            future = self._executor.submit(
                self._run_ga_job,
                job_id,
                expected_version,
                dispatch_lock,
            )
            with self._ga_futures_lock:
                self._ga_futures.add(future)
        future.add_done_callback(self._discard_ga_future)
        return future

    def _discard_ga_future(self, future: Future[GaJobRecord]) -> None:
        with self._ga_futures_lock:
            self._ga_futures.discard(future)

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
                invalidation_hub=self._invalidation_hub,
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
        ga_job: GaJobRecord | None = None,
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
            if ga_job is not None:
                with self._db_session_factory() as session:
                    epoch_exists = session.get(EvolutionEpoch, epoch_id) is not None
                if not epoch_exists and ga_job.completed_generation != -1:
                    raise ValueError("evolution checkpoint is missing for job progress")
            _ensure_evolution_epoch(
                self._db_session_factory,
                request,
                invalidation_hub=self._invalidation_hub,
            )
        except Exception:
            self._release_evolution_epoch(epoch_id)
            raise
        try:
            checkpoint = _load_evolution_checkpoint(
                self._db_session_factory,
                request,
                restore_exact_metrics=ga_job is not None,
            )
            population = checkpoint.population
            evaluations = checkpoint.evaluations
            evaluation_cache = checkpoint.evaluation_cache
            next_generation = checkpoint.generations_run

            if (
                ga_job is not None
                and next_generation != ga_job.completed_generation + 1
            ):
                raise ValueError("evolution checkpoint does not match job progress")

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
                    invalidation_hub=self._invalidation_hub,
                )
                if (
                    ga_job is not None
                    and next_generation == request.evolution.max_generations - 1
                ):
                    _mark_evolution_completed(
                        self._db_session_factory,
                        epoch_id,
                        _select_best_candidate(request, evaluations).score_total,
                        invalidation_hub=self._invalidation_hub,
                    )
                next_generation += 1

                if ga_job is not None:
                    ga_job = self.store.apply_ga_worker_envelope(
                        ga_job.id,
                        epoch_id=epoch_id,
                        expected_completed_generation=next_generation - 2,
                        action="ack_generation",
                        payload={
                            "completed_generation": next_generation - 1,
                            "checkpoint_epoch_id": epoch_id,
                        },
                    )
                    if ga_job.status != GaJobStatus.RUNNING:
                        break

            best = _select_best_candidate(request, evaluations)
            if ga_job is None:
                _mark_evolution_completed(
                    self._db_session_factory,
                    epoch_id,
                    best.score_total,
                    invalidation_hub=self._invalidation_hub,
                )
            elif next_generation >= request.evolution.max_generations:
                with self._db_session_factory() as session:
                    epoch = session.get(EvolutionEpoch, epoch_id)
                    already_completed = (
                        epoch is not None and epoch.status == "completed"
                    )
                if not already_completed:
                    _mark_evolution_completed(
                        self._db_session_factory,
                        epoch_id,
                        best.score_total,
                        invalidation_hub=self._invalidation_hub,
                    )
                if (
                    ga_job.completed_generation == request.evolution.max_generations - 1
                    and ga_job.status
                    in {
                        GaJobStatus.RUNNING,
                        GaJobStatus.PAUSING,
                        GaJobStatus.CANCELLING,
                    }
                ):
                    ga_job = self.store.apply_ga_worker_envelope(
                        ga_job.id,
                        epoch_id=epoch_id,
                        expected_completed_generation=ga_job.completed_generation,
                        action="finalize",
                    )
            return _evolution_result_payload(
                request,
                population,
                evaluations,
                best,
                next_generation,
            )
        except Exception:
            try:
                _mark_evolution_aborted(
                    self._db_session_factory,
                    epoch_id,
                    invalidation_hub=self._invalidation_hub,
                )
            except Exception:
                pass
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

    def _run_ga_job(
        self,
        job_id: str,
        expected_version: int,
        dispatch_lock: Lock,
    ) -> GaJobRecord:
        with dispatch_lock:
            claimed = self.store.transition_ga_job(job_id, "claim", expected_version)
            try:
                request = _validated_ga_worker_request(
                    claimed,
                    self.evaluator,
                    self._db_session_factory,
                )
                self._run_evolution(request, claimed)
                current = self.store.get_ga_job(job_id)
                if current is None:
                    raise ValueError("GA job disappeared during execution")
                return current
            except Exception as exc:
                current = self.store.get_ga_job(job_id)
                if current is not None and current.status in {
                    GaJobStatus.RUNNING,
                    GaJobStatus.PAUSING,
                    GaJobStatus.CANCELLING,
                }:
                    try:
                        self.store.apply_ga_worker_envelope(
                            job_id,
                            epoch_id=current.epoch_id,
                            expected_completed_generation=current.completed_generation,
                            action="fail",
                            payload={"error": str(exc) or type(exc).__name__},
                        )
                    except Exception:
                        pass
                raise


def _validated_ga_worker_request(
    job: GaJobRecord,
    evaluator: ParameterSearchEvaluator,
    session_factory: SessionFactory | None,
) -> CompiledGaProfileRequest:
    if session_factory is None:
        raise ValueError("GA worker requires db_session_factory")
    request = CompiledGaProfileRequest.model_validate(job.request)
    binding = request.ga_binding
    profile = get_golden_cross_profile()
    if (
        binding.parameter_search_profile_id != profile["parameter_search_profile_id"]
        or binding.profile_revision != profile["profile_revision"]
        or binding.strategy_subject != profile["strategy_subject"]
        or binding.strategy_version != profile["strategy_version"]
        or binding.fitness_profile_id != profile["fitness_profile_id"]
        or binding.cost_profile_id != profile["cost_profile_id"]
        or binding.input_digest != _input_digest(request)
    ):
        raise ValueError("GA profile binding no longer matches the request")
    if (
        request.evolution is None
        or request.evolution.epoch_id != job.epoch_id
        or request.market_data is None
        or request.market_data.dataset_id != binding.dataset_id
        or request.strategy_type != "golden_cross"
    ):
        raise ValueError("GA request identity does not match the job")
    metadata = ResearchDatabaseDataSource(
        binding.dataset_id,
        session_factory=session_factory,
    ).get_dataset_metadata()
    if (
        metadata.id != binding.dataset_id
        or metadata.checksum_sha256 != binding.dataset_checksum
        or metadata.product_id != request.product_id
        or metadata.timeframe != request.timeframe
        or request.start_time < metadata.start_time
        or request.end_time > metadata.end_time
    ):
        raise ValueError("sealed dataset no longer matches the GA request")
    if not isinstance(evaluator, ParameterSearchEvaluatorRegistry):
        raise ValueError("GA worker requires the configured evaluator registry")
    evaluator.validate_request(request)
    return request


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


def _walk_forward_inputs(
    request: ParameterSearchJobRequest,
    dataset_scores: dict[str, Decimal],
    dataset_drawdowns: dict[str, Decimal],
    dataset_results: dict[str, dict[str, Any]],
) -> tuple[dict[str, Decimal], dict[str, Decimal | int]]:
    return _calculate_walk_forward_inputs(
        request,
        dataset_scores,
        dataset_drawdowns,
        dataset_results,
        backtest_resolver=_backtest_for_evaluation_dataset,
    )


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
    *,
    invalidation_hub: ControlPlaneInvalidationHub | None = None,
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
    if invalidation_hub is not None:
        invalidation_hub.publish_committed("evolution_epoch", epoch_id, 1)
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
            revision=1,
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
