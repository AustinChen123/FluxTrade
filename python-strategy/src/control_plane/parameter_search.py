from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from decimal import Decimal
from threading import Lock
from typing import Protocol

from src.control_plane.backtest_jobs import _json_safe
from src.control_plane.jobs import InMemoryJobStore, JobStore
from src.control_plane.models import (
    JobRecord,
    JobStatus,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)


class ParameterSearchEvaluator(Protocol):
    """Evaluation boundary for candidate parameter packs."""

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult: ...


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
        evaluations = [
            self.evaluator.evaluate(request, candidate)
            for candidate in request.candidates
        ]
        best = _select_best_candidate(request, evaluations)
        return _json_safe(
            {
                "strategy_id": request.strategy_id,
                "product_id": request.product_id,
                "timeframe": request.timeframe,
                "objective": request.objective,
                "seed": request.seed,
                "evaluations": evaluations,
                "best_candidate": best,
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
