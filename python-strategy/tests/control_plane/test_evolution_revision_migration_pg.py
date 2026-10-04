from __future__ import annotations

import logging
import json
from decimal import Decimal
from typing import cast

import pytest
import sqlalchemy as sa
from sqlalchemy import event, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool, QueuePool

from src.control_plane.evolution import initial_population
from src.control_plane.evolution_persistence import (
    _ensure_evolution_epoch,
    _mark_evolution_aborted,
    _mark_evolution_completed,
    _persist_evolution_generation,
)
from src.control_plane.invalidation import ControlPlaneInvalidationHub
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.models import (
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy
from test_migrations import (
    _downgrade,
    _target_url,
    _upgrade,
    fresh_pg_db as _fresh_pg_db,
)

pytest_plugins = ["test_migrations"]
pytestmark = pytest.mark.integration
fresh_pg_db = _fresh_pg_db


def _ga_request(epoch_id: str) -> ParameterSearchJobRequest:
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "revision-test",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "15m",
            "start_time": 1_700_000_000_000,
            "end_time": 1_700_086_400_000,
            "seed": 17,
            "search_space": {
                "parameters": {
                    "value": {"type": "integer", "min": 0, "max": 2, "step": 1}
                }
            },
            "evolution": {
                "population_size": 2,
                "max_generations": 1,
                "tournament_size": 2,
                "elite_count": 1,
                "crossover_probability": "0.9",
                "mutation_probability": "0.5",
                "mutation_sigma_steps": "2",
                "epoch_id": epoch_id,
            },
        }
    )
    return request


def _read_event(stream) -> dict[str, object]:
    frame = stream.next_frame(timeout=0)
    assert frame is not None
    return json.loads(frame.split(b"data: ", 1)[1].strip())


def _observed_epoch_hub(engine: Engine, sessions: sessionmaker[Session]):
    class ObservedHub(ControlPlaneInvalidationHub):
        def __init__(self) -> None:
            super().__init__()
            self.publications: list[tuple[str, str, int]] = []

        def publish_committed(
            self, resource: str, identity: str, revision: int
        ) -> None:
            assert resource == "evolution_epoch"
            assert cast(QueuePool, engine.pool).checkedout() == 0
            with sessions() as independent:
                committed = independent.scalar(
                    select(EvolutionEpoch)
                    .where(EvolutionEpoch.id == identity)
                    .with_for_update(nowait=True)
                )
                assert committed is not None
                assert committed.revision == revision
            self.publications.append((resource, identity, revision))
            super().publish_committed(resource, identity, revision)

    return ObservedHub()


def test_evolution_revision_migration_preserves_rows_across_down_up(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "e83d14a79b2c")
    engine = sa.create_engine(_target_url(fresh_pg_db), poolclass=NullPool)
    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO strategy (id, name) VALUES ('revision-test', 'Revision test')"
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO evolution_epochs (
                    id, strategy_id, started_at, finished_at, pop_size,
                    max_generations, generations_run, best_score, seed,
                    config_json, status, eval_pair, eval_start_date,
                    eval_end_date, eval_timeframe
                ) VALUES (
                    'revision-epoch', 'revision-test', now(), now(), 2,
                    3, 1, 1.25, 7, jsonb_build_object('seed', 7), 'completed',
                    'BINANCE:BTCUSDT-PERP', '2023-11-14', '2023-11-15', '15m'
                )
                """
            )
        )
        connection.execute(
            text(
                """
                INSERT INTO gene_records (
                    strategy_id, role, param_pack, score_total, score_breakdown,
                    max_drawdown, epoch_id, generation_index, candidate_id
                ) VALUES (
                    'revision-test', 'challenger', jsonb_build_object('value', 1), 1.25,
                    jsonb_build_object('mark_to_market_pnl', '1.25'), 0.5,
                    'revision-epoch', 0, 'revision-candidate'
                )
                """
            )
        )

    _upgrade(fresh_pg_db, "f27c8d14a9b1")
    with engine.connect() as connection:
        first = connection.execute(
            text(
                "SELECT revision, config_json, best_score FROM evolution_epochs "
                "WHERE id='revision-epoch'"
            )
        ).one()
        assert first.revision == 1
        assert first.config_json == {"seed": 7}
        assert isinstance(first.best_score, Decimal)
        assert first.best_score == Decimal("1.25")
        retained_epoch = (first.config_json, first.best_score)
        retained_gene = connection.execute(
            text(
                "SELECT param_pack, score_total, score_breakdown, max_drawdown "
                "FROM gene_records WHERE epoch_id='revision-epoch'"
            )
        ).one()
        assert isinstance(retained_gene.score_total, Decimal)
        assert isinstance(retained_gene.max_drawdown, Decimal)
        assert retained_gene.param_pack == {"value": 1}
        assert retained_gene.score_total == Decimal("1.25")
        assert (
            connection.scalar(
                text(
                    "SELECT count(*) FROM gene_records WHERE epoch_id='revision-epoch'"
                )
            )
            == 1
        )
    with pytest.raises(IntegrityError):
        with engine.begin() as connection:
            connection.execute(
                text("UPDATE evolution_epochs SET revision=0 WHERE id='revision-epoch'")
            )

    _downgrade(fresh_pg_db, "e83d14a79b2c")
    with engine.connect() as connection:
        columns = {
            column["name"]
            for column in sa.inspect(connection).get_columns("evolution_epochs")
        }
        assert "revision" not in columns
        constraints = {
            constraint["name"]
            for constraint in sa.inspect(connection).get_check_constraints(
                "evolution_epochs"
            )
        }
        assert "ck_evolution_epoch_revision_positive" not in constraints
        downgraded_epoch = connection.execute(
            text(
                "SELECT config_json, best_score FROM evolution_epochs "
                "WHERE id='revision-epoch'"
            )
        ).one()
        assert (
            downgraded_epoch.config_json,
            downgraded_epoch.best_score,
        ) == retained_epoch
        downgraded_gene = connection.execute(
            text(
                "SELECT param_pack, score_total, score_breakdown, max_drawdown "
                "FROM gene_records WHERE epoch_id='revision-epoch'"
            )
        ).one()
        assert tuple(downgraded_gene) == tuple(retained_gene)
        assert (
            connection.scalar(
                text(
                    "SELECT count(*) FROM gene_records WHERE epoch_id='revision-epoch'"
                )
            )
            == 1
        )
    _upgrade(fresh_pg_db, "f27c8d14a9b1")
    with engine.connect() as connection:
        upgraded_epoch = connection.execute(
            text(
                "SELECT revision, config_json, best_score FROM evolution_epochs "
                "WHERE id='revision-epoch'"
            )
        ).one()
        assert upgraded_epoch.revision == 1
        assert (upgraded_epoch.config_json, upgraded_epoch.best_score) == retained_epoch
        constraints = {
            constraint["name"]
            for constraint in sa.inspect(connection).get_check_constraints(
                "evolution_epochs"
            )
        }
        assert "ck_evolution_epoch_revision_positive" in constraints
        upgraded_gene = connection.execute(
            text(
                "SELECT param_pack, score_total, score_breakdown, max_drawdown "
                "FROM gene_records WHERE epoch_id='revision-epoch'"
            )
        ).one()
        assert tuple(upgraded_gene) == tuple(retained_gene)
    engine.dispose()


def test_postgres_epoch_writes_publish_committed_revisions_and_suppress_rollback(
    fresh_pg_db: str,
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    sessions = sessionmaker(bind=engine)
    with sessions() as session:
        session.add(Strategy(id="revision-test", name="Revision test"))
        session.commit()
    hub = _observed_epoch_hub(engine, sessions)
    stream = hub.subscribe()
    request = _ga_request("epoch-postgres-events")

    _ensure_evolution_epoch(sessions, request, invalidation_hub=hub)
    assert _read_event(stream)["revision"] == 1
    _ensure_evolution_epoch(sessions, request, invalidation_hub=hub)
    assert _read_event(stream)["revision"] == 2
    assert request.search_space is not None and request.evolution is not None
    population = initial_population(
        request.search_space,
        request.evolution,
        seed=request.seed or 0,
    )
    evaluations = [
        ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=Decimal(index + 1),
            max_drawdown=Decimal("0"),
            metrics={"mark_to_market_pnl": str(index + 1), "max_drawdown": "0"},
        )
        for index, candidate in enumerate(population)
    ]

    def fail_after_generation_flush(session, _context):
        if any(isinstance(row, GeneRecord) for row in session.new):
            raise RuntimeError("injected commit failure")

    event.listen(Session, "after_flush", fail_after_generation_flush)
    try:
        with pytest.raises(RuntimeError, match="injected commit failure"):
            _persist_evolution_generation(
                sessions,
                request,
                0,
                population,
                evaluations,
                invalidation_hub=hub,
            )
    finally:
        event.remove(Session, "after_flush", fail_after_generation_flush)
    with sessions() as session:
        epoch = session.get(EvolutionEpoch, "epoch-postgres-events")
        assert epoch is not None and epoch.revision == 2
        assert (
            session.scalars(
                select(GeneRecord).where(GeneRecord.epoch_id == epoch.id)
            ).all()
            == []
        )
    assert stream.next_frame(timeout=0) is None

    _persist_evolution_generation(
        sessions,
        request,
        0,
        population,
        evaluations,
        invalidation_hub=hub,
    )
    assert _read_event(stream)["revision"] == 3
    _mark_evolution_completed(
        sessions,
        "epoch-postgres-events",
        evaluations[0].score_total,
        invalidation_hub=hub,
    )
    assert _read_event(stream)["revision"] == 4
    _mark_evolution_aborted(
        sessions,
        "epoch-postgres-events",
        invalidation_hub=hub,
    )
    assert stream.next_frame(timeout=0) is None

    abort_request = _ga_request("epoch-aborted-events")
    _ensure_evolution_epoch(sessions, abort_request, invalidation_hub=hub)
    assert _read_event(stream)["identity"] == "epoch-aborted-events"
    _mark_evolution_aborted(
        sessions,
        "epoch-aborted-events",
        invalidation_hub=hub,
    )
    aborted = _read_event(stream)
    assert aborted["identity"] == "epoch-aborted-events" and aborted["revision"] == 2
    with sessions() as session:
        epoch = session.get(EvolutionEpoch, "epoch-aborted-events")
        assert epoch is not None and epoch.status == "aborted" and epoch.revision == 2
    assert hub.publications == [
        ("evolution_epoch", "epoch-postgres-events", 1),
        ("evolution_epoch", "epoch-postgres-events", 2),
        ("evolution_epoch", "epoch-postgres-events", 3),
        ("evolution_epoch", "epoch-postgres-events", 4),
        ("evolution_epoch", "epoch-aborted-events", 1),
        ("evolution_epoch", "epoch-aborted-events", 2),
    ]
    hub.close()
    engine.dispose()


def test_legacy_search_epoch_creation_publishes_only_after_commit(
    fresh_pg_db: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _upgrade(fresh_pg_db, "head")
    engine = sa.create_engine(_target_url(fresh_pg_db))
    sessions = sessionmaker(bind=engine)
    with sessions() as session:
        session.add(Strategy(id="revision-test", name="Revision test"))
        session.commit()

    class Evaluator:
        def __init__(self) -> None:
            self.calls = 0

        def evaluate(self, request, candidate):
            del request
            self.calls += 1
            return ParameterEvaluationResult(
                candidate_id=candidate.candidate_id,
                score_total=Decimal("1.25"),
                metrics={"mark_to_market_pnl": "1.25"},
            )

    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "revision-test",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "15m",
            "start_time": 1_700_000_000_000,
            "end_time": 1_700_086_400_000,
            "seed": 19,
            "search_space": {
                "parameters": {
                    "value": {"type": "integer", "min": 1, "max": 2, "step": 1}
                }
            },
            "candidate_sample_count": 2,
        }
    )
    hub = _observed_epoch_hub(engine, sessions)
    stream = hub.subscribe()
    executor = ParameterSearchJobExecutor(
        Evaluator(),
        store=InMemoryJobStore(),
        run_inline=True,
        db_session_factory=sessions,
        invalidation_hub=hub,
    )
    result = executor.submit_search(request)
    assert result.status.value == "SUCCEEDED" and result.result is not None
    epoch_id = result.result["epoch_id"]
    event_record = _read_event(stream)
    assert event_record == {
        "schema_version": 1,
        "resource": "evolution_epoch",
        "identity": epoch_id,
        "revision": 1,
    }
    with sessions() as session:
        epoch = session.get(EvolutionEpoch, epoch_id)
        assert epoch is not None and epoch.revision == 1 and epoch.status == "completed"
        genes = session.scalars(
            select(GeneRecord).where(GeneRecord.epoch_id == epoch_id)
        ).all()
        assert len(genes) == 2
    assert stream.next_frame(timeout=0) is None
    assert hub.publications == [("evolution_epoch", epoch_id, 1)]

    rollback_hub = _observed_epoch_hub(engine, sessions)
    rollback_stream = rollback_hub.subscribe()

    def fail_legacy_epoch_after_flush(session, _context):
        if any(isinstance(row, GeneRecord) for row in session.new):
            raise RuntimeError("injected legacy epoch commit failure")

    event.listen(Session, "after_flush", fail_legacy_epoch_after_flush)
    try:
        failed = ParameterSearchJobExecutor(
            Evaluator(),
            store=InMemoryJobStore(),
            run_inline=True,
            db_session_factory=sessions,
            invalidation_hub=rollback_hub,
        ).submit_search(request.model_copy(update={"seed": 23}))
    finally:
        event.remove(Session, "after_flush", fail_legacy_epoch_after_flush)
    assert failed.status.value == "FAILED"
    assert rollback_hub.publications == []
    assert rollback_stream.next_frame(timeout=0) is None
    with sessions() as session:
        assert session.scalar(select(sa.func.count()).select_from(EvolutionEpoch)) == 1
        assert session.scalar(select(sa.func.count()).select_from(GeneRecord)) == 2
    rollback_hub.close()

    closed_hub = ControlPlaneInvalidationHub()
    closed_hub.close()
    warning_calls: list[tuple[object, tuple[object, ...], dict[str, object]]] = []
    invalidation_logger = logging.getLogger("src.control_plane.invalidation")
    original_warning = invalidation_logger.warning

    def capture_fixed_warning(message, *args, **kwargs):
        warning_calls.append((message, args, kwargs))
        original_warning(message, *args, **kwargs)

    monkeypatch.setattr(invalidation_logger, "warning", capture_fixed_warning)
    closed_hub_evaluator = Evaluator()
    closed_result = ParameterSearchJobExecutor(
        closed_hub_evaluator,
        store=InMemoryJobStore(),
        run_inline=True,
        db_session_factory=sessions,
        invalidation_hub=closed_hub,
    ).submit_search(request.model_copy(update={"seed": 29}))
    assert closed_result.status.value == "SUCCEEDED"
    assert closed_result.result is not None
    assert len(closed_result.result["evaluations"]) == 2
    assert closed_hub_evaluator.calls == 2
    assert warning_calls == [("invalidation_publish_failed", (), {})]
    assert all(
        epoch_id not in str(message) for message, _args, _kwargs in warning_calls
    )
    with sessions() as session:
        assert session.scalar(select(sa.func.count()).select_from(EvolutionEpoch)) == 2
        assert session.scalar(select(sa.func.count()).select_from(GeneRecord)) == 4
    hub.close()
    engine.dispose()
