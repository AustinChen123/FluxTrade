from __future__ import annotations

import multiprocessing
from concurrent.futures import Future
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import pytest
import sqlalchemy as sa
from sqlalchemy import event, select
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from src.control_plane.evaluation_data import (
    DatabaseEvaluationDataSourceProvider,
    RequestEvaluationDataSourceProvider,
)
from src.control_plane.backtest_jobs import _json_safe
from src.control_plane.ga_profile import (
    compile_golden_cross_request,
    get_golden_cross_profile,
)
from src.control_plane.evolution import (
    canonical_param_key,
    initial_population,
    next_population,
)
from src.control_plane.evolution_persistence import (
    _ensure_evolution_epoch,
    _load_evolution_checkpoint,
)
from src.control_plane.ga_lifecycle import GaJobStatus
from src.control_plane.jobs import SqliteJobStore
from src.control_plane.models import ParameterEvaluationResult
from src.control_plane.parameter_selection import _select_best_candidate
from src.control_plane.parameter_evaluation import (
    GoldenCrossResearchParameterEvaluator,
    ParameterSearchEvaluatorRegistry,
)
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.core.data_sources.research_database import (
    ResearchDatabaseDataSource,
    ResearchDatasetMetadata,
)
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy
from src.core.research_datasets import ResearchDatasetImporter, ResearchDatasetSpec
from test_migrations import _target_url, _upgrade, fresh_pg_db as _fresh_pg_db

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db

_PRODUCT = "BINANCE:BTCUSDT-PERP"
_TIMEFRAME = "15m"
_START = 1_700_000_000_000
_DATASET = "ga-worker-native-pg"


@pytest.mark.parametrize(
    ("scores", "drawdowns"),
    [
        (("1.000000001", "1.000000004"), ("0.500000001", "0.500000001")),
        (("1.000000001", "1.000000001"), ("2.000000004", "2.000000001")),
    ],
    ids=("near-equal-score", "near-equal-drawdown"),
)
def test_pg_checkpoint_restores_exact_metrics_for_selection_and_cache(
    fresh_pg_db: str,
    scores: tuple[str, str],
    drawdowns: tuple[str, str],
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db), poolclass=NullPool)
    sessions = sessionmaker(bind=engine)
    with sessions() as session:
        session.add(Strategy(id="golden_cross", name="Golden Cross"))
        session.commit()
    metadata = ResearchDatasetMetadata(
        id="precision-checkpoint",
        product_id=_PRODUCT,
        timeframe=_TIMEFRAME,
        checksum_sha256="b" * 64,
        start_time=_START,
        end_time=_START + 900_000,
    )
    profile = get_golden_cross_profile()
    payload = {
        **_profile_input(metadata),
        "profile_revision": profile["profile_revision"],
    }
    request = compile_golden_cross_request(payload, metadata)
    assert request.fitness is None
    assert request.evolution is not None and request.search_space is not None
    epoch_id = "precision-checkpoint-epoch"
    request = request.model_copy(
        update={
            "evolution": request.evolution.model_copy(update={"epoch_id": epoch_id})
        }
    )
    assert request.evolution is not None and request.search_space is not None
    evolution = request.evolution
    search_space = request.search_space
    _ensure_evolution_epoch(sessions, request)
    population = initial_population(
        search_space,
        evolution,
        seed=request.seed or 0,
    )
    raw_evaluations = [
        ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=Decimal(score),
            max_drawdown=Decimal(drawdown),
            metrics={
                "mark_to_market_pnl": score,
                "max_drawdown": drawdown,
            },
        )
        for candidate, score, drawdown in zip(
            population,
            scores,
            drawdowns,
            strict=True,
        )
    ]
    with sessions() as session:
        epoch = session.get(EvolutionEpoch, epoch_id)
        assert epoch is not None
        epoch.generations_run = 1
        for candidate, evaluation in zip(population, raw_evaluations, strict=True):
            session.add(
                GeneRecord(
                    strategy_id="golden_cross",
                    role="challenger",
                    param_pack=_json_safe(candidate.param_pack),
                    score_total=evaluation.score_total,
                    score_breakdown=evaluation.metrics,
                    max_drawdown=evaluation.max_drawdown,
                    generation_index=0,
                    candidate_id=candidate.candidate_id,
                    epoch_id=epoch_id,
                )
            )
        session.commit()
        stored_rows = session.scalars(
            select(GeneRecord)
            .where(GeneRecord.epoch_id == epoch_id)
            .order_by(GeneRecord.candidate_id)
        ).all()
        assert len(stored_rows) == 2
        assert {row.score_total for row in stored_rows} == {Decimal("1.00000000")}
        assert len({row.max_drawdown for row in stored_rows}) == 1

    legacy_checkpoint = _load_evolution_checkpoint(sessions, request)
    assert {evaluation.score_total for evaluation in legacy_checkpoint.evaluations} == {
        Decimal("1.00000000")
    }
    checkpoint = _load_evolution_checkpoint(
        sessions,
        request,
        restore_exact_metrics=True,
    )
    expected_best = _select_best_candidate(request, raw_evaluations)
    restored_best = _select_best_candidate(request, checkpoint.evaluations)
    assert restored_best.candidate_id == expected_best.candidate_id
    if scores[0] != scores[1]:
        assert restored_best.candidate_id == population[1].candidate_id
    for candidate, expected in zip(population, raw_evaluations, strict=True):
        cached = checkpoint.evaluation_cache[canonical_param_key(candidate.param_pack)]
        assert cached.score_total == expected.score_total
        assert cached.max_drawdown == expected.max_drawdown
    restored_next = next_population(
        search_space,
        evolution,
        checkpoint.population,
        checkpoint.evaluations,
        objective=request.objective,
        seed=request.seed or 0,
        generation_index=1,
    )
    expected_next = next_population(
        search_space,
        evolution,
        population,
        raw_evaluations,
        objective=request.objective,
        seed=request.seed or 0,
        generation_index=1,
    )
    assert expected_next[0].param_pack == population[1].param_pack
    assert restored_next == expected_next

    with sessions() as session:
        first_row = session.scalars(
            select(GeneRecord)
            .where(GeneRecord.epoch_id == epoch_id)
            .order_by(GeneRecord.candidate_id)
        ).first()
        assert first_row is not None
        invalid_metrics = dict(first_row.score_breakdown)
        if scores[0] != scores[1]:
            invalid_metrics.pop("mark_to_market_pnl")
        else:
            invalid_metrics["max_drawdown"] = "-0.1"
        first_row.score_breakdown = invalid_metrics
        session.commit()
    with pytest.raises(ValueError, match="evolution checkpoint metric"):
        _load_evolution_checkpoint(
            sessions,
            request,
            restore_exact_metrics=True,
        )
    engine.dispose()


def _write_candles(path: Path) -> None:
    closes = ("100", "100", "100", "110", "120", "90", "80", "85", "95")
    rows = ["timestamp,open,high,low,close,volume"]
    rows.extend(
        f"{_START + index * 900_000},{close},{close},{close},{close},100"
        for index, close in enumerate(closes)
    )
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")


def _profile_input(metadata, *, max_generations: int = 2) -> dict[str, object]:
    profile = get_golden_cross_profile()
    return {
        "parameter_search_profile_id": profile["parameter_search_profile_id"],
        "strategy_subject": profile["strategy_subject"],
        "fitness_profile_id": profile["fitness_profile_id"],
        "cost_profile_id": profile["cost_profile_id"],
        "profile_revision": profile["profile_revision"],
        "strategy_version": profile["strategy_version"],
        "dataset_id": metadata.id,
        "start_time": metadata.start_time,
        "end_time": metadata.end_time,
        "initial_balance": "10000.123456789012345678901234",
        "fees": {"maker": "0.000001", "taker": "0.000123"},
        "instrument": {"quantity_step": "0.001", "price_tick": "0.01"},
        "parameters": {
            "short_window": {"min": 1, "max": 2, "step": 1},
            "long_window": {"min": 3, "max": 4, "step": 1},
            "quantity": "0.01",
        },
        "population_size": 2,
        "max_generations": max_generations,
        "seed": 17,
    }


class _GatedEvaluator:
    def __init__(self, delegate, entered, release) -> None:
        self._delegate = delegate
        self._entered = entered
        self._release = release
        self._first = True

    def evaluate(self, request, candidate):
        result = self._delegate.evaluate(request, candidate)
        if self._first:
            self._first = False
            self._entered.set()
            if not self._release.wait(30):
                raise TimeoutError("test generation gate timed out")
        return result


class _FailIfEvaluated:
    def __init__(self, delegate) -> None:
        self._delegate = delegate

    def evaluate(self, request, candidate):
        raise AssertionError("completed checkpoint must resume without evaluation")


class _FailAtCandidate:
    def __init__(self, delegate) -> None:
        self._delegate = delegate

    def evaluate(self, request, candidate):
        raise RuntimeError("injected_candidate_failure")


def _worker_process(
    database_url: str,
    sqlite_path: str,
    job_id: str,
    version: int,
    dataset_id: str,
    result_queue,
    entered=None,
    release=None,
    forbid_evaluation: bool = False,
    fail_generation_ack: bool = False,
    fail_evaluation: bool = False,
    failure_phase: str | None = None,
    failure_reached=None,
    failure_release=None,
) -> None:
    engine = sa.create_engine(database_url, poolclass=NullPool)
    sessions = sessionmaker(bind=engine)
    store = SqliteJobStore(sqlite_path)
    job = store.get_ga_job(job_id)
    assert job is not None
    failure_listener = None
    if failure_phase is not None:
        assert failure_phase in {"generation", "completion"}
        assert failure_reached is not None and failure_release is not None

        def fail_after_flush(session, _flush_context) -> None:
            if failure_phase == "generation":
                matched = any(
                    isinstance(row, GeneRecord) and row.epoch_id == job.epoch_id
                    for row in session.new
                )
            else:
                matched = any(
                    isinstance(epoch, EvolutionEpoch)
                    and epoch.id == job.epoch_id
                    and epoch.status == "completed"
                    for epoch in session.dirty
                )
            if matched:
                failure_reached.set()
                if not failure_release.wait(30):
                    raise TimeoutError("test transaction gate timed out")
                raise RuntimeError(f"injected_{failure_phase}_commit_failure")

        failure_listener = fail_after_flush
        event.listen(Session, "after_flush", failure_listener)
    if fail_generation_ack:
        apply_envelope = store.apply_ga_worker_envelope

        def fail_after_generation_commit(job_id, **kwargs):
            if kwargs.get("action") == "ack_generation":
                raise RuntimeError("injected_generation_ack_failure")
            return apply_envelope(job_id, **kwargs)

        setattr(store, "apply_ga_worker_envelope", fail_after_generation_commit)
    evaluator = GoldenCrossResearchParameterEvaluator(
        data_source_provider=DatabaseEvaluationDataSourceProvider(
            dataset_id,
            session_factory=sessions,
        )
    )
    if entered is not None and release is not None:
        evaluator = _GatedEvaluator(evaluator, entered, release)
    elif forbid_evaluation:
        evaluator = _FailIfEvaluated(evaluator)
    elif fail_evaluation:
        evaluator = _FailAtCandidate(evaluator)
    registry = ParameterSearchEvaluatorRegistry(
        {"golden_cross": evaluator},
        data_source_provider=RequestEvaluationDataSourceProvider(
            session_factory=sessions
        ),
    )
    executor = ParameterSearchJobExecutor(
        registry,
        store=store,
        max_workers=1,
        db_session_factory=sessions,
    )
    try:
        future = executor.dispatch_ga_job(job_id, version)
        assert isinstance(future, Future)
        result = future.result(timeout=60)
        result_queue.put(
            (result.status.value, result.version, result.completed_generation)
        )
    except Exception as exc:
        result_queue.put(("error", type(exc).__name__, str(exc)))
    finally:
        if failure_listener is not None:
            event.remove(Session, "after_flush", failure_listener)
        executor.shutdown(wait=True, timeout=30)
        engine.dispose()


def test_native_pg_worker_pause_process_restart_resume_matches_uninterrupted(
    fresh_pg_db: str,
    tmp_path: Path,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db), poolclass=NullPool)
    sessions = sessionmaker(bind=engine)
    dataset_path = tmp_path / "candles.csv"
    _write_candles(dataset_path)
    imported = ResearchDatasetImporter(sessions).import_csv(
        dataset_path,
        ResearchDatasetSpec(
            dataset_id=_DATASET,
            product_id=_PRODUCT,
            timeframe=_TIMEFRAME,
            source="ga-worker-test",
            revision="v1",
        ),
    )
    with sessions() as session:
        if session.get(Strategy, "golden_cross") is None:
            session.add(Strategy(id="golden_cross", name="Golden Cross"))
            session.commit()
    sqlite_path = tmp_path / "jobs.sqlite3"
    store = SqliteJobStore(sqlite_path)
    metadata = ResearchDatabaseDataSource(
        _DATASET, session_factory=sessions
    ).get_dataset_metadata()
    compiled = compile_golden_cross_request(_profile_input(metadata), metadata)
    request = compiled.model_dump(mode="json")
    first, replayed = store.submit_ga_job_command(
        actor="test-operator",
        idempotency_key="paused-run",
        request=request,
    )
    assert not replayed
    original_binding = first.request["ga_binding"]
    ctx = multiprocessing.get_context("spawn")
    entered = ctx.Event()
    release = ctx.Event()
    results = ctx.Queue()
    processes = []
    first_process = ctx.Process(
        target=_worker_process,
        args=(
            _target_url(fresh_pg_db),
            str(sqlite_path),
            first.id,
            first.version,
            _DATASET,
            results,
            entered,
            release,
        ),
    )
    try:
        first_process.start()
        processes.append(first_process)
        if not entered.wait(45):
            first_process.join(1)
            if not results.empty():
                raise AssertionError(
                    f"worker failed before evaluation: {results.get()}"
                )
            raise AssertionError("native evaluator did not reach the generation gate")
        running = store.get_ga_job(first.id)
        assert running is not None and running.status == GaJobStatus.RUNNING
        pausing = store.transition_ga_job(running.id, "pause", running.version)
        assert pausing.status == GaJobStatus.PAUSING
        release.set()
        first_process.join(60)
        assert first_process.exitcode == 0
        assert results.get(timeout=3) == ("PAUSED", pausing.version + 1, 0)

        reopened_store = SqliteJobStore(sqlite_path)
        paused = reopened_store.get_ga_job(first.id)
        assert paused is not None and paused.status == GaJobStatus.PAUSED
        assert paused.completed_generation == 0
        assert paused.request["ga_binding"] == original_binding
        assert paused.epoch_id == first.epoch_id

        resumed = reopened_store.transition_ga_job(paused.id, "resume", paused.version)
        resume_process = ctx.Process(
            target=_worker_process,
            args=(
                _target_url(fresh_pg_db),
                str(sqlite_path),
                resumed.id,
                resumed.version,
                _DATASET,
                results,
            ),
        )
        resume_process.start()
        processes.append(resume_process)
        resume_process.join(60)
        assert resume_process.exitcode == 0
        resume_result = results.get(timeout=3)
        assert resume_result[0] == "SUCCEEDED", resume_result
        completed = SqliteJobStore(sqlite_path).get_ga_job(first.id)
        assert completed is not None
        assert completed.status == GaJobStatus.SUCCEEDED
        assert completed.completed_generation == 1

        second, replayed = SqliteJobStore(sqlite_path).submit_ga_job_command(
            actor="test-operator",
            idempotency_key="uninterrupted-run",
            request=request,
        )
        assert not replayed and second.epoch_id != first.epoch_id
        full_process = ctx.Process(
            target=_worker_process,
            args=(
                _target_url(fresh_pg_db),
                str(sqlite_path),
                second.id,
                second.version,
                _DATASET,
                results,
            ),
        )
        full_process.start()
        processes.append(full_process)
        full_process.join(60)
        assert full_process.exitcode == 0
        assert results.get(timeout=3)[0] == "SUCCEEDED"

        with sessions() as session:
            resumed_epoch = session.get(EvolutionEpoch, first.epoch_id)
            full_epoch = session.get(EvolutionEpoch, second.epoch_id)
            assert resumed_epoch is not None and resumed_epoch.status == "completed"
            assert full_epoch is not None and full_epoch.status == "completed"
            resumed_rows = session.scalars(
                select(GeneRecord)
                .where(GeneRecord.epoch_id == first.epoch_id)
                .order_by(GeneRecord.generation_index, GeneRecord.candidate_id)
            ).all()
            full_rows = session.scalars(
                select(GeneRecord)
                .where(GeneRecord.epoch_id == second.epoch_id)
                .order_by(GeneRecord.generation_index, GeneRecord.candidate_id)
            ).all()

            def project(rows):
                return [
                    (
                        row.generation_index,
                        row.candidate_id,
                        row.param_pack,
                        row.score_total,
                        row.score_breakdown,
                        row.max_drawdown,
                    )
                    for row in rows
                ]

            assert project(resumed_rows) == project(full_rows)
        assert len(resumed_rows) == 4
        assert len({row.candidate_id for row in resumed_rows}) == 4
        assert all(isinstance(row.score_total, Decimal) for row in resumed_rows)
        for row in resumed_rows:
            pnl_text = row.score_breakdown.get("mark_to_market_pnl")
            assert isinstance(pnl_text, str)
            assert Decimal(pnl_text).is_finite()
        persisted_config = cast(dict[str, Any], resumed_epoch.config_json)
        assert persisted_config["backtest"]["maker_fee"] == "0.000001"
        assert persisted_config["backtest"]["taker_fee"] == "0.000123"
        assert imported.checksum_sha256 == original_binding["dataset_checksum"]

        mismatch_request = compile_golden_cross_request(
            _profile_input(metadata, max_generations=2), metadata
        )
        assert mismatch_request.evolution is not None
        mismatch_epoch_id = "worker-checkpoint-mismatch"
        mismatch_request = mismatch_request.model_copy(
            update={
                "evolution": mismatch_request.evolution.model_copy(
                    update={"epoch_id": mismatch_epoch_id}
                )
            }
        )
        _ensure_evolution_epoch(sessions, mismatch_request)
        mismatch_job = store.create_ga_job(
            request=mismatch_request.model_dump(mode="json"),
            epoch_id=mismatch_epoch_id,
        )
        mismatch_running = store.transition_ga_job(
            mismatch_job.id, "claim", mismatch_job.version
        )
        mismatch_running = store.apply_ga_worker_envelope(
            mismatch_running.id,
            epoch_id=mismatch_epoch_id,
            expected_completed_generation=-1,
            action="ack_generation",
            payload={
                "completed_generation": 0,
                "checkpoint_epoch_id": mismatch_epoch_id,
            },
        )
        mismatch_pausing = store.transition_ga_job(
            mismatch_running.id, "pause", mismatch_running.version
        )
        mismatch_running = store.apply_ga_worker_envelope(
            mismatch_pausing.id,
            epoch_id=mismatch_epoch_id,
            expected_completed_generation=0,
            action="ack_generation",
            payload={
                "completed_generation": 1,
                "checkpoint_epoch_id": mismatch_epoch_id,
            },
        )
        assert mismatch_running.status == GaJobStatus.PAUSED
        mismatch_queued = store.transition_ga_job(
            mismatch_running.id, "resume", mismatch_running.version
        )
        mismatch_process = ctx.Process(
            target=_worker_process,
            args=(
                _target_url(fresh_pg_db),
                str(sqlite_path),
                mismatch_job.id,
                mismatch_queued.version,
                _DATASET,
                results,
                None,
                None,
                True,
            ),
        )
        mismatch_process.start()
        processes.append(mismatch_process)
        mismatch_process.join(60)
        assert mismatch_process.exitcode == 0
        mismatch_result = results.get(timeout=3)
        assert mismatch_result == (
            "error",
            "ValueError",
            "evolution checkpoint does not match job progress",
        )
        mismatch_failed = SqliteJobStore(sqlite_path).get_ga_job(mismatch_job.id)
        assert mismatch_failed is not None
        assert mismatch_failed.status == GaJobStatus.FAILED
        assert mismatch_failed.completed_generation == 1
        with sessions() as session:
            mismatch_epoch = session.get(EvolutionEpoch, mismatch_epoch_id)
            mismatch_rows = session.scalars(
                select(GeneRecord).where(GeneRecord.epoch_id == mismatch_epoch_id)
            ).all()
        assert mismatch_epoch is not None and mismatch_epoch.status == "aborted"
        assert mismatch_rows == []

        failed_ack_job, replayed = SqliteJobStore(sqlite_path).submit_ga_job_command(
            actor="test-operator",
            idempotency_key="generation-ack-failure",
            request=request,
        )
        assert not replayed
        ack_failure_process = ctx.Process(
            target=_worker_process,
            args=(
                _target_url(fresh_pg_db),
                str(sqlite_path),
                failed_ack_job.id,
                failed_ack_job.version,
                _DATASET,
                results,
                None,
                None,
                False,
                True,
            ),
        )
        ack_failure_process.start()
        processes.append(ack_failure_process)
        ack_failure_process.join(60)
        assert ack_failure_process.exitcode == 0
        ack_failure = results.get(timeout=3)
        assert ack_failure == (
            "error",
            "RuntimeError",
            "injected_generation_ack_failure",
        )
        failed_ack_record = SqliteJobStore(sqlite_path).get_ga_job(failed_ack_job.id)
        assert failed_ack_record is not None
        assert failed_ack_record.status == GaJobStatus.FAILED
        assert failed_ack_record.completed_generation == -1
        with sessions() as session:
            committed_before_ack = session.get(EvolutionEpoch, failed_ack_job.epoch_id)
            assert committed_before_ack is not None
            assert committed_before_ack.status == "aborted"
            committed_rows = session.scalars(
                select(GeneRecord).where(GeneRecord.epoch_id == failed_ack_job.epoch_id)
            ).all()
        assert len(committed_rows) == 2
        assert {row.generation_index for row in committed_rows} == {0}

        failed_evaluation_job, replayed = SqliteJobStore(
            sqlite_path
        ).submit_ga_job_command(
            actor="test-operator",
            idempotency_key="candidate-failure-before-persist",
            request=request,
        )
        assert not replayed
        failed_evaluation_process = ctx.Process(
            target=_worker_process,
            args=(
                _target_url(fresh_pg_db),
                str(sqlite_path),
                failed_evaluation_job.id,
                failed_evaluation_job.version,
                _DATASET,
                results,
                None,
                None,
                False,
                False,
                True,
            ),
        )
        failed_evaluation_process.start()
        processes.append(failed_evaluation_process)
        failed_evaluation_process.join(60)
        assert failed_evaluation_process.exitcode == 0
        evaluation_failure = results.get(timeout=3)
        assert evaluation_failure == (
            "error",
            "RuntimeError",
            "injected_candidate_failure",
        )
        failed_evaluation_record = SqliteJobStore(sqlite_path).get_ga_job(
            failed_evaluation_job.id
        )
        assert failed_evaluation_record is not None
        assert failed_evaluation_record.status == GaJobStatus.FAILED
        assert failed_evaluation_record.completed_generation == -1
        with sessions() as session:
            failed_epoch = session.get(EvolutionEpoch, failed_evaluation_job.epoch_id)
            assert failed_epoch is not None and failed_epoch.status == "aborted"
            assert (
                session.scalars(
                    select(GeneRecord).where(
                        GeneRecord.epoch_id == failed_evaluation_job.epoch_id
                    )
                ).all()
                == []
            )

        one_generation = compile_golden_cross_request(
            _profile_input(metadata, max_generations=1), metadata
        )
        final_job, replayed = SqliteJobStore(sqlite_path).submit_ga_job_command(
            actor="test-operator",
            idempotency_key="final-paused-run",
            request=one_generation.model_dump(mode="json"),
        )
        assert not replayed
        final_entered = ctx.Event()
        final_release = ctx.Event()
        final_gate_process = ctx.Process(
            target=_worker_process,
            args=(
                _target_url(fresh_pg_db),
                str(sqlite_path),
                final_job.id,
                final_job.version,
                _DATASET,
                results,
                final_entered,
                final_release,
            ),
        )
        final_gate_process.start()
        processes.append(final_gate_process)
        assert final_entered.wait(45)
        active = SqliteJobStore(sqlite_path).get_ga_job(final_job.id)
        assert active is not None and active.status == GaJobStatus.RUNNING
        final_pause = SqliteJobStore(sqlite_path).transition_ga_job(
            active.id, "pause", active.version
        )
        final_release.set()
        final_gate_process.join(60)
        assert final_gate_process.exitcode == 0
        assert results.get(timeout=3) == ("PAUSED", final_pause.version + 1, 0)
        final_checkpoint = SqliteJobStore(sqlite_path).get_ga_job(final_job.id)
        assert final_checkpoint is not None
        assert final_checkpoint.status == GaJobStatus.PAUSED
        with sessions() as session:
            final_epoch = session.get(EvolutionEpoch, final_job.epoch_id)
            assert final_epoch is not None and final_epoch.status == "completed"
            final_rows_before = session.scalars(
                select(GeneRecord)
                .where(GeneRecord.epoch_id == final_job.epoch_id)
                .order_by(GeneRecord.candidate_id)
            ).all()
        assert len(final_rows_before) == 2

        final_resumed = SqliteJobStore(sqlite_path).transition_ga_job(
            final_checkpoint.id, "resume", final_checkpoint.version
        )
        final_resume_process = ctx.Process(
            target=_worker_process,
            args=(
                _target_url(fresh_pg_db),
                str(sqlite_path),
                final_resumed.id,
                final_resumed.version,
                _DATASET,
                results,
                None,
                None,
                True,
            ),
        )
        final_resume_process.start()
        processes.append(final_resume_process)
        final_resume_process.join(60)
        assert final_resume_process.exitcode == 0
        final_resume_result = results.get(timeout=3)
        assert final_resume_result[0] == "SUCCEEDED", final_resume_result
        with sessions() as session:
            final_rows_after = session.scalars(
                select(GeneRecord)
                .where(GeneRecord.epoch_id == final_job.epoch_id)
                .order_by(GeneRecord.candidate_id)
            ).all()
        assert [row.candidate_id for row in final_rows_after] == [
            row.candidate_id for row in final_rows_before
        ]

        for max_generations in (1, 2):
            cancel_request = compile_golden_cross_request(
                _profile_input(metadata, max_generations=max_generations), metadata
            )
            cancel_job, replayed = SqliteJobStore(sqlite_path).submit_ga_job_command(
                actor="test-operator",
                idempotency_key=f"running-cancel-{max_generations}",
                request=cancel_request.model_dump(mode="json"),
            )
            assert not replayed
            cancel_entered = ctx.Event()
            cancel_release = ctx.Event()
            cancel_process = ctx.Process(
                target=_worker_process,
                args=(
                    _target_url(fresh_pg_db),
                    str(sqlite_path),
                    cancel_job.id,
                    cancel_job.version,
                    _DATASET,
                    results,
                    cancel_entered,
                    cancel_release,
                ),
            )
            cancel_process.start()
            processes.append(cancel_process)
            assert cancel_entered.wait(45)
            cancel_active = SqliteJobStore(sqlite_path).get_ga_job(cancel_job.id)
            assert cancel_active is not None
            assert cancel_active.status == GaJobStatus.RUNNING
            cancelling = SqliteJobStore(sqlite_path).transition_ga_job(
                cancel_active.id, "cancel", cancel_active.version
            )
            assert cancelling.status == GaJobStatus.CANCELLING
            cancel_release.set()
            cancel_process.join(60)
            assert cancel_process.exitcode == 0
            assert results.get(timeout=3) == (
                "CANCELLED",
                cancelling.version + 1,
                0,
            )
            cancelled = SqliteJobStore(sqlite_path).get_ga_job(cancel_job.id)
            assert cancelled is not None and cancelled.status == GaJobStatus.CANCELLED
            assert cancelled.completed_generation == 0
            cancel_epoch = None
            with sessions() as session:
                cancel_rows = session.scalars(
                    select(GeneRecord)
                    .where(GeneRecord.epoch_id == cancel_job.epoch_id)
                    .order_by(GeneRecord.generation_index, GeneRecord.candidate_id)
                ).all()
                if max_generations == 1:
                    cancel_epoch = session.get(EvolutionEpoch, cancel_job.epoch_id)
            assert len(cancel_rows) == 2
            assert {row.generation_index for row in cancel_rows} == {0}
            if max_generations == 1:
                assert cancel_epoch is not None and cancel_epoch.status == "completed"
    finally:
        release.set()
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(5)
        engine.dispose()


@pytest.mark.parametrize(
    ("failure_phase", "pending_action"),
    [
        ("generation", None),
        ("completion", "pause"),
        ("completion", "cancel"),
    ],
    ids=(
        "generation-rollback",
        "completion-pause-priority",
        "completion-cancel-priority",
    ),
)
def test_native_pg_worker_transaction_failures_preserve_committed_checkpoint(
    fresh_pg_db: str,
    tmp_path: Path,
    failure_phase: str,
    pending_action: str | None,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db), poolclass=NullPool)
    sessions = sessionmaker(bind=engine)
    dataset_path = tmp_path / "candles.csv"
    _write_candles(dataset_path)
    ResearchDatasetImporter(sessions).import_csv(
        dataset_path,
        ResearchDatasetSpec(
            dataset_id=_DATASET,
            product_id=_PRODUCT,
            timeframe=_TIMEFRAME,
            source="ga-worker-transaction-failure",
            revision="v1",
        ),
    )
    with sessions() as session:
        if session.get(Strategy, "golden_cross") is None:
            session.add(Strategy(id="golden_cross", name="Golden Cross"))
            session.commit()

    sqlite_path = tmp_path / "jobs.sqlite3"
    store = SqliteJobStore(sqlite_path)
    metadata = ResearchDatabaseDataSource(
        _DATASET, session_factory=sessions
    ).get_dataset_metadata()
    max_generations = 2 if failure_phase == "generation" else 1
    compiled = compile_golden_cross_request(
        _profile_input(metadata, max_generations=max_generations), metadata
    )
    request = compiled.model_dump(mode="json")
    job, replayed = store.submit_ga_job_command(
        actor="test-operator",
        idempotency_key=f"{failure_phase}-{pending_action or 'none'}",
        request=request,
    )
    assert not replayed
    ctx = multiprocessing.get_context("spawn")
    results = ctx.Queue()
    processes = []

    if failure_phase == "generation":
        first_entered = ctx.Event()
        first_release = ctx.Event()
        first = ctx.Process(
            target=_worker_process,
            args=(
                _target_url(fresh_pg_db),
                str(sqlite_path),
                job.id,
                job.version,
                _DATASET,
                results,
                first_entered,
                first_release,
            ),
        )
        first.start()
        processes.append(first)
        assert first_entered.wait(45)
        active = store.get_ga_job(job.id)
        assert active is not None and active.status == GaJobStatus.RUNNING
        pausing = store.transition_ga_job(active.id, "pause", active.version)
        first_release.set()
        first.join(60)
        assert first.exitcode == 0
        assert results.get(timeout=3) == ("PAUSED", pausing.version + 1, 0)
        paused = SqliteJobStore(sqlite_path).get_ga_job(job.id)
        assert paused is not None and paused.status == GaJobStatus.PAUSED
        assert paused.completed_generation == 0
        job = SqliteJobStore(sqlite_path).transition_ga_job(
            paused.id, "resume", paused.version
        )

    failure_reached = ctx.Event()
    failure_release = ctx.Event()
    process = ctx.Process(
        target=_worker_process,
        args=(
            _target_url(fresh_pg_db),
            str(sqlite_path),
            job.id,
            job.version,
            _DATASET,
            results,
            None,
            None,
            False,
            False,
            False,
            failure_phase,
            failure_reached,
            failure_release,
        ),
    )
    try:
        process.start()
        processes.append(process)
        assert failure_reached.wait(45), "worker did not flush the gated transaction"
        active = SqliteJobStore(sqlite_path).get_ga_job(job.id)
        assert active is not None and active.status == GaJobStatus.RUNNING
        if failure_phase == "generation":
            assert active.completed_generation == 0
        else:
            assert active.completed_generation == -1
            assert pending_action in {"pause", "cancel"}
            pending = SqliteJobStore(sqlite_path).transition_ga_job(
                active.id, pending_action, active.version
            )
            assert pending.status in {
                GaJobStatus.PAUSING,
                GaJobStatus.CANCELLING,
            }

        with sessions() as session:
            epoch = session.get(EvolutionEpoch, job.epoch_id)
            rows = session.scalars(
                select(GeneRecord)
                .where(GeneRecord.epoch_id == job.epoch_id)
                .order_by(GeneRecord.generation_index, GeneRecord.candidate_id)
            ).all()
            assert epoch is not None and epoch.status == "running"
            assert epoch.generations_run == 1
            assert len(rows) == 2
            assert {row.generation_index for row in rows} == {0}

        failure_release.set()
        process.join(60)
        assert process.exitcode == 0
        assert results.get(timeout=3) == (
            "error",
            "RuntimeError",
            f"injected_{failure_phase}_commit_failure",
        )
        failed = SqliteJobStore(sqlite_path).get_ga_job(job.id)
        assert failed is not None and failed.status == GaJobStatus.FAILED
        assert failed.completed_generation == (
            0 if failure_phase == "generation" else -1
        )
        assert failed.status not in {GaJobStatus.PAUSED, GaJobStatus.CANCELLED}
        with sessions() as session:
            epoch = session.get(EvolutionEpoch, job.epoch_id)
            rows = session.scalars(
                select(GeneRecord)
                .where(GeneRecord.epoch_id == job.epoch_id)
                .order_by(GeneRecord.generation_index, GeneRecord.candidate_id)
            ).all()
        assert epoch is not None and epoch.status == "aborted"
        assert len(rows) == 2
        assert {row.generation_index for row in rows} == {0}
    finally:
        failure_release.set()
        for child in processes:
            if child.is_alive():
                child.terminate()
                child.join(5)
        engine.dispose()
