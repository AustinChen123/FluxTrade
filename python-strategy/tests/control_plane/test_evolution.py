from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from threading import Event

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from src.control_plane.jobs import InMemoryJobStore, SqliteJobStore
from src.control_plane.fitness import expected_maximum_sharpe
from src.control_plane.evolution import (
    _value_index,
    canonical_param_key,
    initial_population,
    next_population,
)
from src.control_plane.models import (
    EvolutionConfig,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
    ParameterSearchSpace,
)
from src.control_plane.parameter_search import (
    CsvSignalBacktestParameterEvaluator,
    ParameterSearchJobExecutor,
    _ensure_evolution_epoch,
)
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"


def _daily_return_moments(*values: Decimal) -> dict[str, Decimal | int]:
    return {
        "count": len(values),
        "sum": sum(values, Decimal("0")),
        "sum_squares": sum((value**2 for value in values), Decimal("0")),
        "sum_cubes": sum((value**3 for value in values), Decimal("0")),
        "sum_fourth": sum((value**4 for value in values), Decimal("0")),
    }


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


class _KnownOptimumEvaluator:
    def __init__(self, *, fail_after: int | None = None) -> None:
        self.fail_after = fail_after
        self.calls = Counter()
        self.total_calls = 0

    def evaluate(self, request, candidate):
        value = int(candidate.param_pack["value"])
        self.total_calls += 1
        if self.fail_after is not None and self.total_calls > self.fail_after:
            raise RuntimeError("injected evolution interruption")
        self.calls[value] += 1
        score = Decimal("100") - Decimal(abs(value - 7))
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            max_drawdown=Decimal(abs(value - 7)),
            metrics={"value": value, "score": score},
        )


class _BlockingKnownOptimumEvaluator(_KnownOptimumEvaluator):
    def __init__(self) -> None:
        super().__init__()
        self.started = Event()
        self.release = Event()

    def evaluate(self, request, candidate):
        self.started.set()
        if not self.release.wait(timeout=5):
            raise RuntimeError("timed out waiting to release evaluator")
        return super().evaluate(request, candidate)


class _WalkForwardKnownOptimumEvaluator(_KnownOptimumEvaluator):
    def evaluate(self, request, candidate):
        evaluation = super().evaluate(request, candidate)
        year = "2020" if request.start_time % 120_000 == 0 else "2021"
        return evaluation.model_copy(
            update={
                "metrics": {
                    **evaluation.metrics,
                    "daily_return_moments": _daily_return_moments(
                        evaluation.score_total / Decimal("10000"),
                        evaluation.score_total / Decimal("20000"),
                    ),
                    "closed_trade_count": 100,
                    "equity_sample_count": 2,
                    "monthly_returns": {
                        f"{year}-01": evaluation.score_total,
                    },
                    "total_trades": 100,
                    "yearly_mark_to_market_returns": {
                        year: evaluation.score_total / Decimal("10000")
                    },
                }
            }
        )


def _session_factory(tmp_path, name: str):
    engine = create_engine(
        f"sqlite:///{tmp_path / name}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Strategy.__table__,
        EvolutionEpoch.__table__,
        GeneRecord.__table__,
    ]:
        table.create(engine, checkfirst=True)
    factory = sessionmaker(bind=engine)
    with factory() as session:
        session.add(Strategy(id="known_optimum", name="Known optimum"))
        session.commit()
    return factory


def _request(*, seed: int = 17) -> ParameterSearchJobRequest:
    return ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "known_optimum",
            "product_id": PRODUCT_ID,
            "timeframe": "1m",
            "start_time": 1_700_000_000_000,
            "end_time": 1_700_086_400_000,
            "seed": seed,
            "search_space": {
                "parameters": {
                    "value": {
                        "type": "integer",
                        "min": 0,
                        "max": 10,
                        "step": 1,
                    }
                }
            },
            "evolution": {
                "population_size": 6,
                "max_generations": 30,
                "tournament_size": 3,
                "elite_count": 1,
                "crossover_probability": "0.9",
                "mutation_probability": "0.5",
                "mutation_sigma_steps": "2",
            },
        }
    )


def _gene_snapshot(factory):
    with factory() as session:
        records = (
            session.query(GeneRecord)
            .order_by(GeneRecord.generation_index, GeneRecord.candidate_id)
            .all()
        )
        return [
            (
                record.generation_index,
                record.candidate_id,
                record.param_pack,
                record.score_total,
                record.max_drawdown,
            )
            for record in records
        ]


def test_evolution_converges_to_known_optimum_within_thirty_generations(tmp_path):
    factory = _session_factory(tmp_path, "convergence.db")
    request = _request()
    assert all(
        candidate.param_pack["value"] != 7
        for candidate in initial_population(
            request.search_space,
            request.evolution,
            seed=request.seed,
        )
    )
    executor = ParameterSearchJobExecutor(
        evaluator=_KnownOptimumEvaluator(),
        run_inline=True,
        db_session_factory=factory,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result["generations_run"] == 30
    assert job.result["best_candidate_param_pack"] == {"value": 7}
    with factory() as session:
        epoch = session.get(EvolutionEpoch, job.result["epoch_id"])
        assert epoch.status == "completed"
        assert epoch.generations_run == 30


def test_evolution_seed_is_bit_for_bit_reproducible(tmp_path):
    first_factory = _session_factory(tmp_path, "first.db")
    second_factory = _session_factory(tmp_path, "second.db")
    first = ParameterSearchJobExecutor(
        evaluator=_KnownOptimumEvaluator(),
        run_inline=True,
        db_session_factory=first_factory,
    ).submit_search(_request(seed=23))
    second = ParameterSearchJobExecutor(
        evaluator=_KnownOptimumEvaluator(),
        run_inline=True,
        db_session_factory=second_factory,
    ).submit_search(_request(seed=23))

    assert first.status.value == second.status.value == "SUCCEEDED"
    assert _gene_snapshot(first_factory) == _gene_snapshot(second_factory)


def test_evolution_retry_resumes_without_reevaluating_checkpointed_genes(tmp_path):
    factory = _session_factory(tmp_path, "resume.db")
    evaluator = _KnownOptimumEvaluator(fail_after=6)
    store = InMemoryJobStore()
    executor = ParameterSearchJobExecutor(
        evaluator=evaluator,
        store=store,
        run_inline=True,
        db_session_factory=factory,
    )

    failed = executor.submit_search(_request(seed=29))

    assert failed.status.value == "FAILED"
    with factory() as session:
        epoch_id = failed.request["evolution"]["epoch_id"]
        epoch = session.get(EvolutionEpoch, epoch_id)
        assert epoch.status == "aborted"
        assert 0 < epoch.generations_run < epoch.max_generations
        checkpointed_values = {
            record.param_pack["value"]
            for record in session.query(GeneRecord)
            .filter(GeneRecord.epoch_id == epoch_id)
            .all()
        }
    calls_before_resume = dict(evaluator.calls)

    evaluator.fail_after = None
    resumed = executor.retry_search(failed.id)

    assert resumed.status.value == "SUCCEEDED"
    assert resumed.result["epoch_id"] == epoch_id
    for value in checkpointed_values:
        assert evaluator.calls[int(value)] == calls_before_resume[int(value)]


def test_evolution_resume_survives_durable_job_store_reopen(tmp_path):
    factory = _session_factory(tmp_path, "durable_resume.db")
    jobs_path = tmp_path / "control_plane_jobs.db"
    evaluator = _KnownOptimumEvaluator(fail_after=6)
    request = _request(seed=31).model_copy(
        update={
            "evolution": _request(seed=31).evolution.model_copy(
                update={"max_generations": 4}
            )
        }
    )
    failed = ParameterSearchJobExecutor(
        evaluator=evaluator,
        store=SqliteJobStore(jobs_path),
        run_inline=True,
        db_session_factory=factory,
    ).submit_search(request)
    assert failed.status.value == "FAILED"

    with factory() as session:
        epoch_id = failed.request["evolution"]["epoch_id"]
        checkpointed_values = {
            int(record.param_pack["value"])
            for record in session.query(GeneRecord)
            .filter(GeneRecord.epoch_id == epoch_id)
            .all()
        }

    resumed_evaluator = _KnownOptimumEvaluator()
    resumed = ParameterSearchJobExecutor(
        evaluator=resumed_evaluator,
        store=SqliteJobStore(jobs_path),
        run_inline=True,
        db_session_factory=factory,
    ).retry_search(failed.id)

    assert resumed.status.value == "SUCCEEDED"
    assert resumed.result["epoch_id"] == epoch_id
    assert checkpointed_values.isdisjoint(resumed_evaluator.calls)


def test_walk_forward_fitness_resume_survives_durable_store_reopen(tmp_path):
    factory = _session_factory(tmp_path, "walk_forward_resume.db")
    jobs_path = tmp_path / "walk_forward_jobs.db"
    payload = _request(seed=41).model_dump(mode="json", exclude_none=True)
    payload["backtest"] = {
        "candles_csv_path": "data/folds.csv",
        "initial_balance": "10000",
    }
    payload["evaluation_set"] = {
        "walk_forward": {
            "product_id": PRODUCT_ID,
            "timeframe": "1m",
            "start_time": 1_700_000_040_000,
            "end_time": 1_700_000_159_999,
            "fold_duration_ms": 60_000,
        }
    }
    payload["fitness"] = {}
    payload["evolution"].update(
        {
            "population_size": 4,
            "max_generations": 2,
            "tournament_size": 2,
        }
    )
    request = ParameterSearchJobRequest.model_validate(payload)
    evaluator = _WalkForwardKnownOptimumEvaluator(fail_after=0)
    failed = ParameterSearchJobExecutor(
        evaluator=evaluator,
        store=SqliteJobStore(jobs_path),
        run_inline=True,
        db_session_factory=factory,
    ).submit_search(request)

    assert failed.status.value == "FAILED"

    resumed = ParameterSearchJobExecutor(
        evaluator=_WalkForwardKnownOptimumEvaluator(),
        store=SqliteJobStore(jobs_path),
        run_inline=True,
        db_session_factory=factory,
    ).retry_search(failed.id)

    assert resumed.status.value == "SUCCEEDED"
    with factory() as session:
        records = (
            session.query(GeneRecord)
            .filter(GeneRecord.epoch_id == resumed.result["epoch_id"])
            .all()
        )
    assert records
    assert all(
        record.score_breakdown["aggregation"]
        == "registered_walk_forward_fitness"
        for record in records
    )
    assert all(
        record.score_breakdown["fitness"]["independent_trials"] == 8
        for record in records
    )
    assert all(
        record.score_breakdown["fitness"]["metric_contract"]["version"]
        == "walk_forward_fitness_v2"
        for record in records
    )
    with factory() as session:
        epoch = session.get(EvolutionEpoch, resumed.result["epoch_id"])
        assert (
            epoch.config_json["fitness_metric_contract"]["version"]
            == "walk_forward_fitness_v2"
        )
    unique_sharpes = {
        canonical_param_key(record.param_pack): Decimal(
            record.score_breakdown["fitness_inputs"]["daily_sharpe"]
        )
        for record in records
    }
    expected_benchmark = expected_maximum_sharpe(
        list(unique_sharpes.values()),
        independent_trials=8,
    )
    final_generation = max(record.generation_index for record in records)
    assert {
        Decimal(record.score_breakdown["fitness"]["benchmark_sharpe"])
        for record in records
        if record.generation_index == final_generation
    } == {expected_benchmark}


def test_duplicate_epoch_run_is_rejected_without_aborting_active_run(tmp_path):
    factory = _session_factory(tmp_path, "duplicate_epoch.db")
    evaluator = _BlockingKnownOptimumEvaluator()
    executor = ParameterSearchJobExecutor(
        evaluator=evaluator,
        run_inline=True,
        db_session_factory=factory,
    )
    base_request = _request(seed=37)
    request = base_request.model_copy(
        update={
            "evolution": base_request.evolution.model_copy(
                update={
                    "epoch_id": "epoch_active",
                    "max_generations": 1,
                }
            )
        }
    )

    with ThreadPoolExecutor(max_workers=1) as pool:
        active = pool.submit(executor._run_evolution, request)
        assert evaluator.started.wait(timeout=5)

        with pytest.raises(ValueError, match="already running"):
            executor._run_evolution(request)

        with factory() as session:
            epoch = session.get(EvolutionEpoch, "epoch_active")
            assert epoch.status == "running"
            assert epoch.generations_run == 0

        evaluator.release.set()
        result = active.result(timeout=5)

    assert result["generations_run"] == 1
    with factory() as session:
        assert session.get(EvolutionEpoch, "epoch_active").status == "completed"


@pytest.mark.parametrize(
    "evolution_update",
    [
        {"population_size": 1},
        {"population_size": 4, "tournament_size": 5},
        {"population_size": 4, "elite_count": 4},
        {"population_size": 4, "crossover_probability": "-0.1"},
        {"population_size": 4, "crossover_probability": "1.1"},
        {"population_size": 4, "mutation_probability": "NaN"},
        {"population_size": 4, "mutation_sigma_steps": "0"},
    ],
)
def test_evolution_config_rejects_invalid_state_matrix(evolution_update):
    payload = {
        "population_size": 4,
        "max_generations": 2,
        **evolution_update,
    }
    with pytest.raises(ValidationError):
        EvolutionConfig.model_validate(payload)


@pytest.mark.parametrize("field", ["score_total", "max_drawdown"])
@pytest.mark.parametrize("value", ["NaN", "Infinity", "-Infinity"])
def test_parameter_evaluation_rejects_non_finite_fitness(field, value):
    payload = {
        "candidate_id": "candidate",
        "score_total": "1",
        "max_drawdown": "0",
        field: value,
    }

    with pytest.raises(ValidationError, match="finite number"):
        ParameterEvaluationResult.model_validate(payload)


def test_evolution_request_rejects_ambiguous_candidate_sources():
    payload = _request().model_dump(mode="json", exclude_none=True)
    payload["candidate_sample_count"] = 4
    with pytest.raises(ValidationError, match="candidate_sample_count"):
        ParameterSearchJobRequest.model_validate(payload)

    payload = _request().model_dump(mode="json", exclude_none=True)
    payload.pop("search_space")
    payload["candidates"] = [{"candidate_id": "one", "param_pack": {"value": 1}}]
    with pytest.raises(ValidationError, match="evolution requires search_space"):
        ParameterSearchJobRequest.model_validate(payload)


def test_candidate_id_rejects_values_that_do_not_fit_gene_storage():
    with pytest.raises(ValidationError, match="at most 64"):
        ParameterCandidate(candidate_id="x" * 65, param_pack={})


def test_evolution_request_accepts_walk_forward_warmup_boundary():
    payload = _request().model_dump(mode="json", exclude_none=True)
    payload["backtest"] = {"candles_csv_path": "data/fold.csv"}
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "warmup",
                "product_id": PRODUCT_ID,
                "timeframe": "1m",
                "start_time": payload["start_time"],
                "end_time": payload["end_time"],
                "warmup_start_time": payload["start_time"] - 60_000,
            }
        ]
    }

    request = ParameterSearchJobRequest.model_validate(payload)

    assert request.evaluation_set is not None
    assert request.evaluation_set.datasets[0].warmup_start_time == (
        payload["start_time"] - 60_000
    )


def test_categorical_dimensions_reject_non_durable_decimal_values():
    payload = _request().model_dump(mode="python")
    payload["search_space"] = {
        "parameters": {
            "unstable": {
                "type": "categorical",
                "choices": [Decimal("0.1"), Decimal("0.2")],
            }
        }
    }

    with pytest.raises(ValidationError, match="cannot contain Decimal"):
        ParameterSearchJobRequest.model_validate(payload)


def test_evolution_reuses_search_space_expansion_limit():
    search_space = ParameterSearchSpace.model_validate(
        {
            "parameters": {
                "too_wide": {
                    "type": "integer",
                    "min": 0,
                    "max": 100_000,
                    "step": 1,
                }
            }
        }
    )
    config = EvolutionConfig(population_size=2, max_generations=1)

    with pytest.raises(ValueError, match="too many values"):
        initial_population(search_space, config, seed=1)


def test_evolution_csv_evaluation_disables_early_stop_and_normalizes_drawdown(
    monkeypatch,
):
    payload = _request().model_dump(mode="python")
    payload["backtest"] = {
        "candles_csv_path": "/tmp/candles.csv",
        "initial_balance": "1000",
    }
    payload["search_space"] = {
        "parameters": {
            "signals_csv_path": {
                "type": "categorical",
                "choices": ["/tmp/one.csv", "/tmp/two.csv"],
            }
        }
    }
    request = ParameterSearchJobRequest.model_validate(payload)
    evaluator = CsvSignalBacktestParameterEvaluator()
    captured = {}

    def run_backtest(backtest_request, *, max_drawdown_limit):
        captured["max_drawdown_limit"] = max_drawdown_limit
        return {
            "total_pnl": "5",
            "max_drawdown": "-125.50",
        }

    monkeypatch.setattr(
        evaluator._backtest_executor,
        "run_backtest_request",
        run_backtest,
    )

    result = evaluator.evaluate(
        request,
        ParameterCandidate(
            candidate_id="candidate",
            param_pack={"signals_csv_path": "/tmp/one.csv"},
        ),
    )

    assert captured["max_drawdown_limit"] is None
    assert result.max_drawdown == Decimal("125.50")


def test_evolution_persists_drawdown_as_positive_loss_magnitude(tmp_path):
    class NegativeDrawdownEvaluator:
        def evaluate(self, request, candidate):
            return ParameterEvaluationResult(
                candidate_id=candidate.candidate_id,
                score_total=Decimal("1"),
                max_drawdown=Decimal("-1250.125"),
                metrics={"max_drawdown": Decimal("-1250.125")},
            )

    factory = _session_factory(tmp_path, "drawdown_sign.db")
    base_request = _request()
    request = base_request.model_copy(
        update={
            "evolution": base_request.evolution.model_copy(
                update={"max_generations": 1}
            )
        }
    )
    job = ParameterSearchJobExecutor(
        evaluator=NegativeDrawdownEvaluator(),
        run_inline=True,
        db_session_factory=factory,
    ).submit_search(request)

    assert job.status.value == "SUCCEEDED"
    with factory() as session:
        records = (
            session.query(GeneRecord)
            .filter(GeneRecord.epoch_id == job.result["epoch_id"])
            .all()
        )
    assert {record.max_drawdown for record in records} == {
        Decimal("1250.12500000")
    }


def test_categorical_identity_distinguishes_boolean_and_integer_values():
    values = [True, 1, "1"]

    assert _value_index(values, True) == 0
    assert _value_index(values, 1) == 1
    assert canonical_param_key({"value": True}) != canonical_param_key({"value": 1})


def test_evolution_operators_keep_every_dimension_on_registered_domain():
    search_space = ParameterSearchSpace.model_validate(
        {
            "parameters": {
                "integer_gene": {
                    "type": "integer",
                    "min": 1,
                    "max": 5,
                    "step": 1,
                },
                "decimal_gene": {
                    "type": "decimal",
                    "min": "0.1",
                    "max": "0.5",
                    "step": "0.1",
                },
                "categorical_gene": {
                    "type": "categorical",
                    "choices": ["a", "b", "c"],
                },
            }
        }
    )
    config = EvolutionConfig(
        population_size=6,
        max_generations=10,
        tournament_size=3,
        elite_count=1,
        crossover_probability=Decimal("1"),
        mutation_probability=Decimal("1"),
        mutation_sigma_steps=Decimal("2"),
    )
    population = initial_population(search_space, config, seed=41)
    for generation_index in range(1, 10):
        evaluations = [
            ParameterEvaluationResult(
                candidate_id=candidate.candidate_id,
                score_total=Decimal(candidate.param_pack["integer_gene"]),
                max_drawdown=Decimal("0"),
                metrics={},
            )
            for candidate in population
        ]
        population = next_population(
            search_space,
            config,
            population,
            evaluations,
            objective="maximize_score",
            seed=41,
            generation_index=generation_index,
        )
        for candidate in population:
            assert candidate.param_pack["integer_gene"] in range(1, 6)
            assert candidate.param_pack["decimal_gene"] in {
                Decimal("0.1"),
                Decimal("0.2"),
                Decimal("0.3"),
                Decimal("0.4"),
                Decimal("0.5"),
            }
            assert candidate.param_pack["categorical_gene"] in {"a", "b", "c"}


def test_mismatched_resume_request_does_not_mutate_existing_epoch(tmp_path):
    factory = _session_factory(tmp_path, "mismatch.db")
    request = _request().model_copy(
        update={
            "evolution": _request().evolution.model_copy(
                update={"epoch_id": "epoch_fixed"}
            )
        }
    )
    _ensure_evolution_epoch(factory, request)
    mismatched = request.model_copy(
        update={
            "evolution": request.evolution.model_copy(
                update={"mutation_sigma_steps": Decimal("3")}
            )
        }
    )
    job = ParameterSearchJobExecutor(
        evaluator=_KnownOptimumEvaluator(),
        run_inline=True,
        db_session_factory=factory,
    ).submit_search(mismatched)

    assert job.status.value == "FAILED"
    with factory() as session:
        epoch = session.get(EvolutionEpoch, "epoch_fixed")
        assert epoch.status == "running"
        assert epoch.generations_run == 0


def test_resume_rejects_changed_backtest_window(tmp_path):
    factory = _session_factory(tmp_path, "window_mismatch.db")
    request = _request().model_copy(
        update={
            "evolution": _request().evolution.model_copy(
                update={"epoch_id": "epoch_window"}
            )
        }
    )
    _ensure_evolution_epoch(factory, request)
    changed = request.model_copy(update={"end_time": request.end_time + 60_000})

    with pytest.raises(ValueError, match="does not match request"):
        _ensure_evolution_epoch(factory, changed)

    with factory() as session:
        epoch = session.get(EvolutionEpoch, "epoch_window")
        assert epoch.status == "running"
        assert epoch.generations_run == 0


def test_resume_rejects_uneven_generation_checkpoint(tmp_path):
    factory = _session_factory(tmp_path, "uneven_checkpoint.db")
    executor = ParameterSearchJobExecutor(
        evaluator=_KnownOptimumEvaluator(),
        run_inline=True,
        db_session_factory=factory,
    )
    request = _request().model_copy(
        update={
            "evolution": _request().evolution.model_copy(
                update={"max_generations": 2}
            )
        }
    )
    completed = executor.submit_search(request)
    assert completed.status.value == "SUCCEEDED"

    with factory() as session:
        first = (
            session.query(GeneRecord)
            .filter(
                GeneRecord.epoch_id == completed.result["epoch_id"],
                GeneRecord.generation_index == 0,
            )
            .first()
        )
        first.generation_index = 1
        session.commit()

    resumed = executor.submit_search(
        ParameterSearchJobRequest.model_validate(completed.request)
    )

    assert resumed.status.value == "FAILED"
    assert "incomplete generation" in resumed.error
