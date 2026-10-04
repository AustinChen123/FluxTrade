from __future__ import annotations

import sqlite3
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from dataclasses import replace
from threading import Barrier, Event
from typing import Any, cast

import pytest

from src.control_plane.ga_lifecycle import GaJobStatus, GaJobStoreError
from src.control_plane.ga_lifecycle import GaJobRecord
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.jobs import SqliteJobStore
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.control_plane.ga_profile import (
    compile_golden_cross_request,
    get_golden_cross_profile,
)
from src.control_plane.backtest_jobs import SessionFactory
from src.control_plane.models import (
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.core.data_sources.research_database import (
    ResearchDatabaseDataSource,
    ResearchDatasetMetadata,
)
from src.control_plane.parameter_evaluation import ParameterSearchEvaluatorRegistry
import src.control_plane.jobs as jobs_module
import src.control_plane.parameter_search as parameter_search_module
from src.control_plane.evolution_persistence import _checkpoint_decimal_metric


def _request(max_generations: int = 2, epoch_id: str = "epoch-a") -> dict[str, Any]:
    return {
        "strategy": {"id": "golden_cross", "version": "1"},
        "evolution": {"epoch_id": epoch_id, "max_generations": max_generations},
        "seed": 17,
    }


class _UnusedEvaluator:
    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=Decimal("0"),
            max_drawdown=Decimal("0"),
            metrics={},
        )


@pytest.fixture(params=["memory", "sqlite"])
def worker_store(request, tmp_path):
    return (
        InMemoryJobStore()
        if request.param == "memory"
        else SqliteJobStore(tmp_path / "jobs.sqlite")
    )


def test_executor_can_dispatch_an_existing_ga_job() -> None:
    executor = ParameterSearchJobExecutor(
        evaluator=_UnusedEvaluator(),
        store=InMemoryJobStore(),
        run_inline=True,
    )

    assert callable(getattr(executor, "dispatch_ga_job", None))


def test_worker_ack_uses_current_row_and_exact_checkpoint(worker_store) -> None:
    queued = worker_store.create_ga_job(request=_request(), epoch_id="epoch-a")
    running = worker_store.transition_ga_job(queued.id, "claim", queued.version)

    acknowledged = worker_store.apply_ga_worker_envelope(
        queued.id,
        epoch_id="epoch-a",
        expected_completed_generation=-1,
        action="ack_generation",
        payload={"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
    )
    assert acknowledged.status == GaJobStatus.RUNNING
    assert acknowledged.version == running.version + 1
    assert acknowledged.completed_generation == 0

    with pytest.raises(GaJobStoreError) as error:
        worker_store.apply_ga_worker_envelope(
            queued.id,
            epoch_id="epoch-a",
            expected_completed_generation=-1,
            action="ack_generation",
            payload={"completed_generation": 1, "checkpoint_epoch_id": "epoch-a"},
        )
    assert error.value.code == "job_version_conflict"
    assert worker_store.get_ga_job(queued.id) == acknowledged


@pytest.mark.parametrize(
    ("case", "expected_code"),
    [
        ("wrong-epoch", "job_version_conflict"),
        ("unknown-action", "job_version_conflict"),
        ("prior-generation", "job_version_conflict"),
    ],
)
def test_worker_envelope_rejects_stale_or_invalid_identity_without_mutation(
    worker_store, case: str, expected_code: str
) -> None:
    queued = worker_store.create_ga_job(request=_request(), epoch_id="epoch-a")
    running = worker_store.transition_ga_job(queued.id, "claim", queued.version)
    if case == "prior-generation":
        running = worker_store.apply_ga_worker_envelope(
            running.id,
            epoch_id="epoch-a",
            expected_completed_generation=-1,
            action="ack_generation",
            payload={
                "completed_generation": 0,
                "checkpoint_epoch_id": "epoch-a",
            },
        )
    before = worker_store.get_ga_job(queued.id)
    assert before is not None
    epoch_id = "other-epoch" if case == "wrong-epoch" else "epoch-a"
    action = "unknown" if case == "unknown-action" else "ack_generation"
    expected_generation = -1 if case != "prior-generation" else -1

    with pytest.raises(GaJobStoreError) as error:
        worker_store.apply_ga_worker_envelope(
            queued.id,
            epoch_id=epoch_id,
            expected_completed_generation=expected_generation,
            action=action,
            payload={
                "completed_generation": 1,
                "checkpoint_epoch_id": "epoch-a",
            },
        )
    assert error.value.code == expected_code
    assert worker_store.get_ga_job(queued.id) == before


@pytest.mark.parametrize(
    ("action", "expected_status"),
    [("finalize", GaJobStatus.SUCCEEDED), ("fail", GaJobStatus.FAILED)],
)
def test_worker_envelope_forwards_finalize_and_failure(
    worker_store, action: str, expected_status: GaJobStatus
) -> None:
    queued = worker_store.create_ga_job(
        request=_request(max_generations=1 if action == "finalize" else 2),
        epoch_id="epoch-a",
    )
    running = worker_store.transition_ga_job(queued.id, "claim", queued.version)
    if action == "finalize":
        pending = worker_store.transition_ga_job(running.id, "pause", running.version)
        assert pending.status == GaJobStatus.PAUSING
        paused = worker_store.apply_ga_worker_envelope(
            running.id,
            epoch_id="epoch-a",
            expected_completed_generation=-1,
            action="ack_generation",
            payload={
                "completed_generation": 0,
                "checkpoint_epoch_id": "epoch-a",
            },
        )
        assert paused.status == GaJobStatus.PAUSED
        resumed = worker_store.transition_ga_job(paused.id, "resume", paused.version)
        running = worker_store.transition_ga_job(resumed.id, "claim", resumed.version)
    completed = worker_store.apply_ga_worker_envelope(
        running.id,
        epoch_id="epoch-a",
        expected_completed_generation=running.completed_generation,
        action=action,
        payload={"error": "worker failed"} if action == "fail" else None,
    )
    assert completed.status == expected_status
    assert completed.version == running.version + 1


@pytest.mark.parametrize(
    ("action", "expected_status"),
    [
        ("pause", GaJobStatus.PAUSED),
        ("cancel", GaJobStatus.CANCELLED),
    ],
)
def test_two_sqlite_instances_serialize_operator_action_and_worker_ack(
    tmp_path, action: str, expected_status: GaJobStatus
) -> None:
    path = tmp_path / f"worker-race-{action}.sqlite"
    store_a = SqliteJobStore(path)
    store_b = SqliteJobStore(path)
    queued = store_a.create_ga_job(request=_request(), epoch_id="epoch-a")
    running = store_a.transition_ga_job(queued.id, "claim", queued.version)
    start = Barrier(3)

    def operator_action():
        start.wait(timeout=3)
        return store_a.transition_ga_job(running.id, action, running.version)

    def worker_ack():
        start.wait(timeout=3)
        return store_b.apply_ga_worker_envelope(
            running.id,
            epoch_id="epoch-a",
            expected_completed_generation=-1,
            action="ack_generation",
            payload={
                "completed_generation": 0,
                "checkpoint_epoch_id": "epoch-a",
            },
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        operator_future = pool.submit(operator_action)
        worker_future = pool.submit(worker_ack)
        start.wait(timeout=3)
        try:
            operator_result = operator_future.result(timeout=5)
        except GaJobStoreError as error:
            assert error.code == "job_version_conflict"
            operator_result = None
        worker_future.result(timeout=5)

    current = SqliteJobStore(path).get_ga_job(queued.id)
    assert current is not None
    assert current.completed_generation == 0
    if operator_result is None:
        assert current.status == GaJobStatus.RUNNING
        assert current.version == running.version + 1
    else:
        assert current.status == expected_status
        assert current.version == running.version + 2


@pytest.mark.parametrize(
    ("action", "expected_status"),
    [("pause", GaJobStatus.PAUSED), ("cancel", GaJobStatus.CANCELLED)],
)
def test_worker_final_ack_observes_boundary_priority(
    worker_store, action: str, expected_status: GaJobStatus
) -> None:
    queued = worker_store.create_ga_job(
        request=_request(max_generations=1), epoch_id="epoch-a"
    )
    running = worker_store.transition_ga_job(queued.id, "claim", queued.version)
    pending = worker_store.transition_ga_job(running.id, action, running.version)

    final = worker_store.apply_ga_worker_envelope(
        queued.id,
        epoch_id="epoch-a",
        expected_completed_generation=-1,
        action="ack_generation",
        payload={"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
    )
    assert pending.status in {GaJobStatus.PAUSING, GaJobStatus.CANCELLING}
    assert final.status == expected_status
    assert final.completed_generation == 0


@pytest.mark.parametrize("action", ["pause", "cancel"])
def test_worker_dispatch_does_not_claim_queued_operator_action(action: str) -> None:
    store = InMemoryJobStore()
    queued = store.create_ga_job(request=_request(), epoch_id="epoch-a")
    changed = store.transition_ga_job(queued.id, action, queued.version)
    executor = ParameterSearchJobExecutor(
        _UnusedEvaluator(), store=store, run_inline=True
    )

    with pytest.raises(GaJobStoreError):
        executor.dispatch_ga_job(queued.id, queued.version)
    assert store.get_ga_job(queued.id) == changed


def test_worker_dispatch_rejects_duplicate_stale_claim() -> None:
    store = InMemoryJobStore()
    queued = store.create_ga_job(request=_request(), epoch_id="epoch-a")
    running = store.transition_ga_job(queued.id, "claim", queued.version)
    executor = ParameterSearchJobExecutor(
        _UnusedEvaluator(), store=store, run_inline=True
    )

    with pytest.raises(GaJobStoreError) as error:
        executor.dispatch_ga_job(queued.id, queued.version)
    assert error.value.code == "job_version_conflict"
    assert store.get_ga_job(queued.id) == running


def test_worker_recovery_interrupts_only_persisted_active_ga(tmp_path) -> None:
    path = tmp_path / "jobs.sqlite"
    store = SqliteJobStore(path)
    queued = store.create_ga_job(
        request=_request(epoch_id="queued-epoch"), epoch_id="queued-epoch"
    )
    paused_seed = store.create_ga_job(
        request=_request(epoch_id="paused-epoch"), epoch_id="paused-epoch"
    )
    paused = store.transition_ga_job(paused_seed.id, "pause", paused_seed.version)
    running_seed = store.create_ga_job(
        request=_request(epoch_id="running-epoch"), epoch_id="running-epoch"
    )
    running = store.transition_ga_job(running_seed.id, "claim", running_seed.version)

    recovered = SqliteJobStore(path)
    ParameterSearchJobExecutor(
        _UnusedEvaluator(),
        store=recovered,
        run_inline=True,
        recover_interrupted=True,
    )

    recovered_queued = recovered.get_ga_job(queued.id)
    recovered_paused = recovered.get_ga_job(paused.id)
    assert (
        recovered_queued is not None and recovered_queued.status == GaJobStatus.QUEUED
    )
    assert (
        recovered_paused is not None and recovered_paused.status == GaJobStatus.PAUSED
    )
    interrupted = recovered.get_ga_job(running.id)
    assert interrupted is not None
    assert interrupted.status == GaJobStatus.FAILED
    assert interrupted.version == running.version + 1
    assert interrupted.error == "control_plane_interrupted"


def test_shutdown_cancels_pending_ga_future_without_claiming_row() -> None:
    store = InMemoryJobStore()
    queued = store.create_ga_job(request=_request(), epoch_id="epoch-a")
    executor = ParameterSearchJobExecutor(
        _UnusedEvaluator(), store=store, max_workers=1
    )
    entered = Event()
    release = Event()
    assert executor._executor is not None
    blocker = executor._executor.submit(lambda: (entered.set(), release.wait(5)))
    assert entered.wait(2)
    pending = executor.dispatch_ga_job(queued.id, queued.version)
    assert isinstance(pending, Future)
    assert executor.shutdown(wait=False, cancel_futures=True) is True
    assert pending.cancelled()
    assert store.get_ga_job(queued.id) == queued
    with pytest.raises(RuntimeError, match="shut down"):
        executor.dispatch_ga_job(queued.id, queued.version)
    release.set()
    assert blocker.result(timeout=2) == (None, True)


def test_immediate_resume_dispatch_waits_for_old_worker_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryJobStore()
    queued = store.create_ga_job(
        request=_request(max_generations=2), epoch_id="epoch-a"
    )
    executor = ParameterSearchJobExecutor(
        _UnusedEvaluator(), store=store, max_workers=2
    )
    generation_started = Event()
    allow_ack = Event()
    ack_visible = Event()
    release_cleanup = Event()
    resumed_generation_started = Event()
    generations: list[int] = []

    def run_generation(request, job):
        generations.append(job.completed_generation)
        if job.completed_generation == -1:
            generation_started.set()
            assert allow_ack.wait(3)
            acknowledged = store.apply_ga_worker_envelope(
                job.id,
                epoch_id=job.epoch_id,
                expected_completed_generation=-1,
                action="ack_generation",
                payload={
                    "completed_generation": 0,
                    "checkpoint_epoch_id": job.epoch_id,
                },
            )
            assert acknowledged.status == GaJobStatus.PAUSED
            ack_visible.set()
            assert release_cleanup.wait(3)
            return {}
        resumed_generation_started.set()
        acknowledged = store.apply_ga_worker_envelope(
            job.id,
            epoch_id=job.epoch_id,
            expected_completed_generation=0,
            action="ack_generation",
            payload={"completed_generation": 1, "checkpoint_epoch_id": job.epoch_id},
        )
        assert acknowledged.status == GaJobStatus.SUCCEEDED
        return {}

    monkeypatch.setattr(
        parameter_search_module,
        "_validated_ga_worker_request",
        lambda *_args: object(),
    )
    monkeypatch.setattr(executor, "_run_evolution", run_generation)
    try:
        first_future = executor.dispatch_ga_job(queued.id, queued.version)
        assert isinstance(first_future, Future)
        assert generation_started.wait(2)
        running = store.get_ga_job(queued.id)
        assert running is not None and running.status == GaJobStatus.RUNNING
        store.transition_ga_job(running.id, "pause", running.version)
        allow_ack.set()
        assert ack_visible.wait(2)
        paused = store.get_ga_job(queued.id)
        assert paused is not None and paused.status == GaJobStatus.PAUSED
        assert not first_future.done()
        resumed = store.transition_ga_job(paused.id, "resume", paused.version)
        completed = executor.dispatch_ga_job(resumed.id, resumed.version)
        assert isinstance(completed, Future)
        assert not resumed_generation_started.wait(0.1)
        assert generations == [-1]
        release_cleanup.set()
        first_future.result(timeout=3)
        assert resumed_generation_started.wait(2)
        assert completed.result(timeout=3).status == GaJobStatus.SUCCEEDED
    finally:
        allow_ack.set()
        release_cleanup.set()
        assert executor.shutdown(wait=True, timeout=3)


def test_shutdown_tracks_every_duplicate_same_job_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    store = InMemoryJobStore()
    queued = store.create_ga_job(request=_request(), epoch_id="epoch-a")
    executor = ParameterSearchJobExecutor(
        _UnusedEvaluator(), store=store, max_workers=1
    )
    entered = Event()
    release = Event()

    monkeypatch.setattr(
        parameter_search_module,
        "_validated_ga_worker_request",
        lambda *_args: object(),
    )

    def run_until_released(_request, _job):
        entered.set()
        assert release.wait(3)

    monkeypatch.setattr(executor, "_run_evolution", run_until_released)
    shutdown_complete = True
    try:
        first = executor.dispatch_ga_job(queued.id, queued.version)
        assert isinstance(first, Future)
        assert entered.wait(2)
        running = store.get_ga_job(queued.id)
        assert running is not None and running.status == GaJobStatus.RUNNING
        second = executor.dispatch_ga_job(running.id, running.version)
        assert isinstance(second, Future)
        assert second.cancel()
        assert not first.done()
        assert second.cancelled()
        shutdown_complete = executor.shutdown(wait=False)
    finally:
        release.set()
        assert executor._executor is not None
        executor._executor.shutdown(wait=True)
    assert not shutdown_complete
    assert first.done()


def test_invalid_profile_binding_fails_before_dataset_or_evaluation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = ResearchDatasetMetadata(
        id="worker-binding",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="15m",
        checksum_sha256="a" * 64,
        start_time=1_704_067_200_000,
        end_time=1_704_067_800_000,
    )
    profile = get_golden_cross_profile()
    compiled = compile_golden_cross_request(
        {
            "parameter_search_profile_id": profile["parameter_search_profile_id"],
            "strategy_subject": profile["strategy_subject"],
            "fitness_profile_id": profile["fitness_profile_id"],
            "cost_profile_id": profile["cost_profile_id"],
            "profile_revision": profile["profile_revision"],
            "strategy_version": profile["strategy_version"],
            "dataset_id": metadata.id,
            "start_time": metadata.start_time,
            "end_time": metadata.end_time,
            "initial_balance": "10000",
            "fees": {"maker": "0.001", "taker": "0.002"},
            "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
            "parameters": {
                "short_window": {"min": 1, "max": 2, "step": 1},
                "long_window": {"min": 3, "max": 4, "step": 1},
                "quantity": "0.01",
            },
            "population_size": 2,
            "max_generations": 1,
            "seed": 1,
        },
        metadata,
    )
    assert compiled.evolution is not None
    bad_binding = compiled.ga_binding.model_copy(update={"input_digest": "0" * 64})
    request = compiled.model_copy(update={"ga_binding": bad_binding})
    assert request.evolution is not None
    epoch_id = "worker-binding-epoch"
    request = request.model_copy(
        update={
            "evolution": request.evolution.model_copy(update={"epoch_id": epoch_id})
        }
    )
    store = InMemoryJobStore()
    queued = store.create_ga_job(
        request=request.model_dump(mode="json"),
        epoch_id=epoch_id,
    )
    evaluated = False

    class NeverEvaluate:
        def evaluate(self, request, candidate):
            nonlocal evaluated
            evaluated = True
            raise AssertionError("invalid binding reached evaluator")

    registry = ParameterSearchEvaluatorRegistry({"golden_cross": NeverEvaluate()})
    executor = ParameterSearchJobExecutor(
        registry,
        store=store,
        run_inline=True,
        db_session_factory=cast(SessionFactory, lambda: None),
    )
    with pytest.raises(ValueError, match="profile binding"):
        executor.dispatch_ga_job(queued.id, queued.version)
    failed = store.get_ga_job(queued.id)
    assert failed is not None and failed.status == GaJobStatus.FAILED
    assert failed.completed_generation == -1
    assert not evaluated

    valid_request = compiled.model_copy(
        update={
            "evolution": compiled.evolution.model_copy(
                update={"epoch_id": "dataset-preflight-epoch"}
            )
        }
    )
    dataset_job = store.create_ga_job(
        request=valid_request.model_dump(mode="json"),
        epoch_id="dataset-preflight-epoch",
    )

    def reject_dataset(_source) -> None:
        raise ValueError("sealed dataset unavailable")

    monkeypatch.setattr(
        parameter_search_module.ResearchDatabaseDataSource,
        "get_dataset_metadata",
        reject_dataset,
    )
    with pytest.raises(ValueError, match="sealed dataset unavailable"):
        executor.dispatch_ga_job(dataset_job.id, dataset_job.version)
    dataset_failed = store.get_ga_job(dataset_job.id)
    assert dataset_failed is not None
    assert dataset_failed.status == GaJobStatus.FAILED
    assert dataset_failed.completed_generation == -1
    assert not evaluated


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("checksum_sha256", "c" * 64),
        ("product_id", "BINANCE:ETHUSDT-PERP"),
        ("timeframe", "1h"),
        ("start_time", 1_704_067_200_001),
        ("end_time", 1_704_067_799_999),
    ],
    ids=("checksum", "product", "timeframe", "start", "end"),
)
def test_worker_rejects_successful_dataset_metadata_mismatch_before_epoch(
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: object,
) -> None:
    metadata = ResearchDatasetMetadata(
        id="worker-metadata-match",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="15m",
        checksum_sha256="a" * 64,
        start_time=1_704_067_200_000,
        end_time=1_704_067_800_000,
    )
    profile = get_golden_cross_profile()
    compiled = compile_golden_cross_request(
        {
            "parameter_search_profile_id": profile["parameter_search_profile_id"],
            "strategy_subject": profile["strategy_subject"],
            "fitness_profile_id": profile["fitness_profile_id"],
            "cost_profile_id": profile["cost_profile_id"],
            "profile_revision": profile["profile_revision"],
            "strategy_version": profile["strategy_version"],
            "dataset_id": metadata.id,
            "start_time": metadata.start_time,
            "end_time": metadata.end_time,
            "initial_balance": "10000",
            "fees": {"maker": "0.001", "taker": "0.002"},
            "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
            "parameters": {
                "short_window": {"min": 1, "max": 2, "step": 1},
                "long_window": {"min": 3, "max": 4, "step": 1},
                "quantity": "0.01",
            },
            "population_size": 2,
            "max_generations": 1,
            "seed": 1,
        },
        metadata,
    )
    assert compiled.evolution is not None
    epoch_id = "worker-metadata-mismatch"
    request = compiled.model_copy(
        update={
            "evolution": compiled.evolution.model_copy(update={"epoch_id": epoch_id})
        }
    )
    returned_metadata = replace(metadata, **{field: value})
    monkeypatch.setattr(
        ResearchDatabaseDataSource,
        "get_dataset_metadata",
        lambda _source: returned_metadata,
    )
    epoch_creation_attempted = False
    evaluated = False

    def track_epoch_creation(*_args, **_kwargs):
        nonlocal epoch_creation_attempted
        epoch_creation_attempted = True
        raise AssertionError("metadata mismatch reached PostgreSQL epoch creation")

    monkeypatch.setattr(
        parameter_search_module, "_ensure_evolution_epoch", track_epoch_creation
    )

    class NeverEvaluate:
        def evaluate(self, request, candidate):
            nonlocal evaluated
            evaluated = True
            raise AssertionError("metadata mismatch reached evaluation")

    store = InMemoryJobStore()
    queued = store.create_ga_job(
        request=request.model_dump(mode="json"), epoch_id=epoch_id
    )
    executor = ParameterSearchJobExecutor(
        ParameterSearchEvaluatorRegistry({"golden_cross": NeverEvaluate()}),
        store=store,
        run_inline=True,
        db_session_factory=cast(SessionFactory, lambda: None),
    )
    with pytest.raises(ValueError, match="sealed dataset no longer matches"):
        executor.dispatch_ga_job(queued.id, queued.version)
    failed = store.get_ga_job(queued.id)
    assert failed is not None and failed.status == GaJobStatus.FAILED
    assert failed.completed_generation == -1
    assert not epoch_creation_attempted
    assert not evaluated


def test_worker_rejects_stale_profile_revision_before_dataset_or_epoch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = ResearchDatasetMetadata(
        id="worker-profile-revision",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="15m",
        checksum_sha256="d" * 64,
        start_time=1_704_067_200_000,
        end_time=1_704_067_800_000,
    )
    profile = get_golden_cross_profile()
    compiled = compile_golden_cross_request(
        {
            "parameter_search_profile_id": profile["parameter_search_profile_id"],
            "strategy_subject": profile["strategy_subject"],
            "fitness_profile_id": profile["fitness_profile_id"],
            "cost_profile_id": profile["cost_profile_id"],
            "profile_revision": profile["profile_revision"],
            "strategy_version": profile["strategy_version"],
            "dataset_id": metadata.id,
            "start_time": metadata.start_time,
            "end_time": metadata.end_time,
            "initial_balance": "10000",
            "fees": {"maker": "0.001", "taker": "0.002"},
            "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
            "parameters": {
                "short_window": {"min": 1, "max": 2, "step": 1},
                "long_window": {"min": 3, "max": 4, "step": 1},
                "quantity": "0.01",
            },
            "population_size": 2,
            "max_generations": 1,
            "seed": 1,
        },
        metadata,
    )
    assert compiled.evolution is not None
    epoch_id = "worker-profile-revision-epoch"
    request = compiled.model_copy(
        update={
            "evolution": compiled.evolution.model_copy(update={"epoch_id": epoch_id})
        }
    )
    store = InMemoryJobStore()
    queued = store.create_ga_job(
        request=request.model_dump(mode="json"), epoch_id=epoch_id
    )
    changed_profile = {**profile, "profile_revision": "stale-revision"}
    monkeypatch.setattr(
        parameter_search_module,
        "get_golden_cross_profile",
        lambda: changed_profile,
    )
    monkeypatch.setattr(
        ResearchDatabaseDataSource,
        "get_dataset_metadata",
        lambda _source: pytest.fail("stale profile reached source metadata"),
    )
    monkeypatch.setattr(
        parameter_search_module,
        "_ensure_evolution_epoch",
        lambda *_args: pytest.fail("stale profile created an epoch"),
    )

    class NeverEvaluate:
        def evaluate(self, request, candidate):
            pytest.fail("stale profile reached evaluation")

    executor = ParameterSearchJobExecutor(
        ParameterSearchEvaluatorRegistry({"golden_cross": NeverEvaluate()}),
        store=store,
        run_inline=True,
        db_session_factory=cast(SessionFactory, lambda: None),
    )
    with pytest.raises(ValueError, match="profile binding"):
        executor.dispatch_ga_job(queued.id, queued.version)
    failed = store.get_ga_job(queued.id)
    assert failed is not None and failed.status == GaJobStatus.FAILED
    assert failed.completed_generation == -1


def test_worker_rejects_request_epoch_mismatch_before_source_lookup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    metadata = ResearchDatasetMetadata(
        id="worker-request-epoch",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="15m",
        checksum_sha256="e" * 64,
        start_time=1_704_067_200_000,
        end_time=1_704_067_800_000,
    )
    profile = get_golden_cross_profile()
    compiled = compile_golden_cross_request(
        {
            "parameter_search_profile_id": profile["parameter_search_profile_id"],
            "strategy_subject": profile["strategy_subject"],
            "fitness_profile_id": profile["fitness_profile_id"],
            "cost_profile_id": profile["cost_profile_id"],
            "profile_revision": profile["profile_revision"],
            "strategy_version": profile["strategy_version"],
            "dataset_id": metadata.id,
            "start_time": metadata.start_time,
            "end_time": metadata.end_time,
            "initial_balance": "10000",
            "fees": {"maker": "0.001", "taker": "0.002"},
            "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
            "parameters": {
                "short_window": {"min": 1, "max": 2, "step": 1},
                "long_window": {"min": 3, "max": 4, "step": 1},
                "quantity": "0.01",
            },
            "population_size": 2,
            "max_generations": 1,
            "seed": 1,
        },
        metadata,
    )
    assert compiled.evolution is not None
    epoch_id = "worker-request-epoch"
    request = compiled.model_copy(
        update={
            "evolution": compiled.evolution.model_copy(update={"epoch_id": epoch_id})
        }
    )
    store = InMemoryJobStore()
    queued = store.create_ga_job(
        request=request.model_dump(mode="json"), epoch_id=epoch_id
    )
    wrong_epoch_job = GaJobRecord(
        id=queued.id,
        status=queued.status,
        version=queued.version,
        request=queued.request,
        epoch_id="different-job-epoch",
        completed_generation=queued.completed_generation,
    )
    monkeypatch.setattr(
        ResearchDatabaseDataSource,
        "get_dataset_metadata",
        lambda _source: pytest.fail("epoch mismatch reached source metadata"),
    )
    with pytest.raises(ValueError, match="request identity"):
        parameter_search_module._validated_ga_worker_request(
            wrong_epoch_job,
            ParameterSearchEvaluatorRegistry({"golden_cross": _UnusedEvaluator()}),
            cast(SessionFactory, lambda: None),
        )


def test_worker_preserves_original_error_when_abort_marking_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "golden_cross",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "15m",
            "start_time": 1,
            "end_time": 2,
            "search_space": {
                "parameters": {
                    "short_window": {
                        "type": "integer",
                        "min": 1,
                        "max": 1,
                        "step": 1,
                    },
                    "long_window": {
                        "type": "integer",
                        "min": 2,
                        "max": 2,
                        "step": 1,
                    },
                    "quantity": {
                        "type": "decimal",
                        "min": "0.01",
                        "max": "0.01",
                        "step": "0.01",
                    },
                }
            },
            "evolution": {
                "epoch_id": "abort-preserves-checkpoint-error",
                "population_size": 2,
                "max_generations": 1,
            },
        }
    )
    executor = ParameterSearchJobExecutor(
        _UnusedEvaluator(),
        run_inline=True,
        db_session_factory=cast(SessionFactory, lambda: None),
    )
    monkeypatch.setattr(
        parameter_search_module, "_ensure_evolution_epoch", lambda *_args: None
    )

    def fail_checkpoint(*_args, **_kwargs):
        raise OSError("original-checkpoint-read-error")

    def fail_abort(*_args, **_kwargs):
        raise RuntimeError("secondary-abort-write-error")

    monkeypatch.setattr(
        parameter_search_module, "_load_evolution_checkpoint", fail_checkpoint
    )
    monkeypatch.setattr(parameter_search_module, "_mark_evolution_aborted", fail_abort)
    with pytest.raises(OSError, match="original-checkpoint-read-error"):
        executor._run_evolution(request)


@pytest.mark.parametrize(
    ("metrics", "name", "nonnegative"),
    [
        ({}, "mark_to_market_pnl", False),
        ({"mark_to_market_pnl": 1}, "mark_to_market_pnl", False),
        ({"mark_to_market_pnl": "NaN"}, "mark_to_market_pnl", False),
        ({"max_drawdown": "-0.1"}, "max_drawdown", True),
        ({"max_drawdown": "Infinity"}, "max_drawdown", True),
    ],
    ids=(
        "missing",
        "not-text",
        "nonfinite-score",
        "negative-drawdown",
        "infinite-drawdown",
    ),
)
def test_exact_checkpoint_metrics_reject_invalid_text_without_fallback(
    metrics: dict[str, Any],
    name: str,
    nonnegative: bool,
) -> None:
    with pytest.raises(ValueError, match="evolution checkpoint metric"):
        _checkpoint_decimal_metric(metrics, name, nonnegative=nonnegative)


def test_sqlite_worker_envelope_write_failure_rolls_back_after_update(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "worker-rollback.sqlite"
    store = SqliteJobStore(path)
    queued = store.create_ga_job(request=_request(), epoch_id="epoch-a")
    running = store.transition_ga_job(queued.id, "claim", queued.version)
    original_write = jobs_module._write_ga_record
    wrote = False

    def write_then_fail(conn: sqlite3.Connection, updated, version: int) -> None:
        nonlocal wrote
        original_write(conn, updated, version)
        wrote = True
        raise RuntimeError("after_worker_update")

    monkeypatch.setattr(jobs_module, "_write_ga_record", write_then_fail)
    with pytest.raises(RuntimeError, match="after_worker_update"):
        store.apply_ga_worker_envelope(
            queued.id,
            epoch_id="epoch-a",
            expected_completed_generation=-1,
            action="ack_generation",
            payload={"completed_generation": 0, "checkpoint_epoch_id": "epoch-a"},
        )
    assert wrote
    reopened = SqliteJobStore(path).get_ga_job(queued.id)
    assert reopened == running
