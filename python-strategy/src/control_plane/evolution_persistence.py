"""Durable evolution checkpoint persistence."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from src.control_plane.backtest_jobs import SessionFactory, _json_safe
from src.control_plane.evolution import canonical_param_key
from src.control_plane.fitness import fitness_metric_contract
from src.control_plane.models import (
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.parameter_selection import _select_best_candidate
from src.core.models import GeneRole
from src.core.orm_models import EvolutionEpoch, GeneRecord


@dataclass(frozen=True)
class _EvolutionCheckpoint:
    generations_run: int
    population: list[ParameterCandidate]
    evaluations: list[ParameterEvaluationResult]
    evaluation_cache: dict[
        tuple[tuple[str, str], ...],
        ParameterEvaluationResult,
    ]


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


def _evolution_config_payload(
    request: ParameterSearchJobRequest,
) -> dict[str, Any]:
    payload = request.model_dump(mode="json")
    if payload.get("strategy_type") is None:
        payload.pop("strategy_type", None)
    if request.fitness is not None:
        payload["fitness_metric_contract"] = fitness_metric_contract()
    return payload


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
