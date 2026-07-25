from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from src.control_plane.models import (
    EvaluationSetConfig,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.parameter_search import (
    ParameterSearchJobExecutor,
    ResearchBacktestParameterEvaluator,
)
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy, SystemEvent
from src.strategies.base import BaseStrategy, StrategyRequirements


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


class _NoopEvaluator:
    def __init__(self):
        self.requests = []

    def evaluate(self, request, candidate):
        self.requests.append((request, candidate))
        score = Decimal(str(candidate.param_pack["score"]))
        if request.start_time == 10:
            score += Decimal("1")
            drawdown = Decimal("-2")
        else:
            score += Decimal("2")
            drawdown = Decimal("-5")
        if "drawdown" in candidate.param_pack:
            drawdown = Decimal(str(candidate.param_pack["drawdown"]))
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            max_drawdown=drawdown,
            metrics={
                "product_id": request.product_id,
                "timeframe": request.timeframe,
                "start_time": request.start_time,
                "max_drawdown": drawdown,
            },
        )


class _DrawdownEvaluator:
    def evaluate(self, request, candidate):
        drawdowns = candidate.param_pack["drawdowns_by_start_time"]
        drawdown = Decimal(str(drawdowns[str(request.start_time)]))
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=Decimal(str(candidate.param_pack.get("score", "0"))),
            max_drawdown=drawdown,
            metrics={"max_drawdown": drawdown},
        )


class _WarmupAwareEvaluator(_NoopEvaluator):
    def __init__(self):
        super().__init__()
        self.warmups = []

    def evaluate_with_warmup(
        self,
        request,
        candidate,
        *,
        warmup_start_time,
    ):
        self.warmups.append(
            (warmup_start_time, request.start_time, request.end_time)
        )
        return self.evaluate(request, candidate)


class _WalkForwardFitnessEvaluator:
    def evaluate(self, request, candidate):
        fragile = candidate.candidate_id == "fragile"
        first_fold = request.start_time == 10
        if fragile:
            score = Decimal("100") if first_fold else Decimal("-10")
            sharpe = Decimal("2") if first_fold else Decimal("-1")
            drawdown = Decimal("50") if first_fold else Decimal("100")
        else:
            score = Decimal("40")
            sharpe = Decimal("1")
            drawdown = Decimal("20")
        year = "2020" if first_fold else "2021"
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            max_drawdown=drawdown,
            metrics={
                "daily_return_moments": _daily_return_moments(
                    Decimal("0.01"),
                    Decimal("0.02"),
                ),
                "closed_trade_count": 100,
                "equity_sample_count": 2,
                "max_drawdown": drawdown,
                "monthly_returns": {f"{year}-01": score},
                "total_trades": 100,
                "trade_pnl_quality": sharpe,
                "yearly_mark_to_market_returns": {
                    year: score / Decimal("10000")
                },
            },
        )


class _BalanceScaledFitnessEvaluator:
    def evaluate(self, request, candidate):
        assert request.backtest is not None
        score = request.backtest.initial_balance / Decimal("10")
        year = "2020" if request.start_time == 10 else "2021"
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            max_drawdown=Decimal("0"),
            metrics={
                "daily_return_moments": _daily_return_moments(
                    Decimal("0.01"),
                    Decimal("0.02"),
                ),
                "closed_trade_count": 2,
                "equity_sample_count": 2,
                "monthly_returns": {f"{year}-01": score},
                "total_trades": 2,
                "yearly_mark_to_market_returns": {
                    year: score / request.backtest.initial_balance
                },
            },
        )


class _EmptyScoringFitnessEvaluator:
    def evaluate(self, request, candidate):
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=Decimal("0"),
            max_drawdown=Decimal("0"),
            metrics={
                "daily_return_moments": _daily_return_moments(),
                "closed_trade_count": 0,
                "equity_sample_count": 0,
                "total_trades": 0,
                "yearly_mark_to_market_returns": {},
            },
        )


class _WarmupRecordingStrategy(BaseStrategy):
    def __init__(self):
        super().__init__("warmup", PRODUCT_ID)
        self.timestamps = []
        self._in_position = False
        self.active_trade = None

    @property
    def requirements(self):
        return StrategyRequirements(self.product_id, "5m", 2)

    def on_candle(self, candle, context=None):
        self.timestamps.append(candle.timestamp)
        self._in_position = True
        self.active_trade = {"entry": candle.close}
        return None

    def snapshot_walk_forward_trade_state(self):
        return self._in_position, self.active_trade

    def restore_walk_forward_trade_state(self, state):
        self._in_position, self.active_trade = state


class BaseStrategyWithoutWarmupContract(BaseStrategy):
    def __init__(self):
        super().__init__("no_contract", PRODUCT_ID)

    @property
    def requirements(self):
        return StrategyRequirements(self.product_id, "5m", 2)

    def on_candle(self, candle, context=None):
        return None


def _base_search_request() -> dict:
    return {
        "strategy_id": "golden_cross",
        "product_id": PRODUCT_ID,
        "timeframe": "5m",
        "start_time": 1_700_000_000_000,
        "end_time": 1_700_086_400_000,
        "candidates": [
            {
                "candidate_id": "baseline",
                "param_pack": {"short_window": 5, "long_window": 20, "score": 1},
            },
        ],
    }


def _sqlite_gene_registry_session_factory(tmp_path):
    engine = create_engine(
        f"sqlite:///{tmp_path / 'evaluation_set_gene_registry.db'}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Strategy.__table__,
        SystemEvent.__table__,
        EvolutionEpoch.__table__,
        GeneRecord.__table__,
    ]:
        table.create(engine, checkfirst=True)

    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add(Strategy(id="golden_cross", name="Golden Cross"))
        session.commit()
    return session_factory


def _dataset_payload(
    dataset_id: str,
    *,
    start_time: int,
    end_time: int,
    metadata: dict | None = None,
    backtest: dict | None = None,
    resolved_backtest: dict | None = None,
) -> dict:
    return {
        "dataset_id": dataset_id,
        "product_id": PRODUCT_ID,
        "timeframe": "5m",
        "start_time": start_time,
        "end_time": end_time,
        "warmup_start_time": None,
        "metadata": metadata or {},
        "backtest": backtest,
        "resolved_backtest": resolved_backtest,
    }


def test_parameter_search_accepts_evaluation_set_payload():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/BTCUSDT_5m.csv",
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "trend",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 1_700_000_000_000,
                "end_time": 1_700_086_400_000,
                "metadata": {"regime": "trend"},
            },
            {
                "dataset_id": "chop",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 1_700_086_400_000,
                "end_time": 1_700_172_800_000,
                "metadata": {"regime": "chop"},
            },
        ],
    }

    request = ParameterSearchJobRequest.model_validate(payload)

    assert request.evaluation_set is not None
    assert [dataset.dataset_id for dataset in request.evaluation_set.datasets] == [
        "trend",
        "chop",
    ]
    assert request.evaluation_set.datasets[1].metadata["regime"] == "chop"


def test_evaluation_set_config_converts_to_core_model():
    config = EvaluationSetConfig.model_validate(
        {
            "datasets": [
                {
                    "dataset_id": "with_warmup",
                    "product_id": PRODUCT_ID,
                    "timeframe": "5m",
                    "start_time": 10,
                    "end_time": 20,
                    "warmup_start_time": 5,
                    "metadata": {"regime": "trend"},
                },
            ],
        }
    )

    evaluation_set = config.to_core_evaluation_set()
    dataset = evaluation_set["with_warmup"]

    assert dataset.product_id == PRODUCT_ID
    assert dataset.replay_start_time == 5
    assert dataset.metadata["regime"] == "trend"


def test_evaluation_set_config_generates_walk_forward_folds():
    config = EvaluationSetConfig.model_validate(
        {
            "walk_forward": {
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 1_000,
                "end_time": 3_499,
                "fold_duration_ms": 1_000,
                "warmup_duration_ms": 500,
                "dataset_id_prefix": "fold",
            }
        }
    )

    assert [
        (
            dataset.dataset_id,
            dataset.warmup_start_time,
            dataset.start_time,
            dataset.end_time,
        )
        for dataset in config.resolved_datasets
    ] == [
        ("fold_0000", 500, 1_000, 1_999),
        ("fold_0001", 1_500, 2_000, 2_999),
    ]
    round_trip = EvaluationSetConfig.model_validate(config.model_dump(mode="json"))
    assert round_trip.datasets == []
    assert [
        dataset.dataset_id for dataset in round_trip.resolved_datasets
    ] == ["fold_0000", "fold_0001"]


def test_evaluation_set_config_requires_one_dataset_source():
    with pytest.raises(
        ValidationError,
        match="evaluation_set requires datasets or walk_forward",
    ):
        EvaluationSetConfig.model_validate({})

    with pytest.raises(
        ValidationError,
        match="provide datasets or walk_forward, not both",
    ):
        EvaluationSetConfig.model_validate(
            {
                "datasets": [
                    {
                        "dataset_id": "manual",
                        "product_id": PRODUCT_ID,
                        "timeframe": "5m",
                        "start_time": 1,
                        "end_time": 2,
                    }
                ],
                "walk_forward": {
                    "product_id": PRODUCT_ID,
                    "timeframe": "5m",
                    "start_time": 1,
                    "end_time": 10,
                    "fold_duration_ms": 5,
                },
            }
        )


def test_evaluation_set_rejects_duplicate_dataset_ids():
    with pytest.raises(ValidationError, match="dataset_id values must be unique"):
        EvaluationSetConfig.model_validate(
            {
                "datasets": [
                    {
                        "dataset_id": "duplicate",
                        "product_id": PRODUCT_ID,
                        "timeframe": "5m",
                        "start_time": 1,
                        "end_time": 2,
                    },
                    {
                        "dataset_id": "duplicate",
                        "product_id": PRODUCT_ID,
                        "timeframe": "5m",
                        "start_time": 2,
                        "end_time": 3,
                    },
                ],
            }
        )


def test_evaluation_dataset_rejects_invalid_warmup_range():
    with pytest.raises(ValidationError, match="warmup_start_time must be <= start_time"):
        EvaluationSetConfig.model_validate(
            {
                "datasets": [
                    {
                        "dataset_id": "bad_warmup",
                        "product_id": PRODUCT_ID,
                        "timeframe": "5m",
                        "start_time": 10,
                        "end_time": 20,
                        "warmup_start_time": 11,
                    },
                ],
            }
        )


def test_parameter_search_accepts_warmup_datasets_for_capable_evaluators():
    payload = _base_search_request()
    payload["backtest"] = {"candles_csv_path": "data/fold.csv"}
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "with_warmup",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "warmup_start_time": 5,
            },
        ],
    }

    request = ParameterSearchJobRequest.model_validate(payload)

    assert request.evaluation_set is not None
    assert request.evaluation_set.datasets[0].warmup_start_time == 5


def test_parameter_search_routes_warmup_only_to_capable_evaluator():
    payload = _base_search_request()
    payload["backtest"] = {"candles_csv_path": "data/fold.csv"}
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "fold",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "warmup_start_time": 5,
                "start_time": 10,
                "end_time": 20,
            },
        ],
    }
    evaluator = _WarmupAwareEvaluator()
    executor = ParameterSearchJobExecutor(evaluator=evaluator, run_inline=True)

    job = executor.submit_search(ParameterSearchJobRequest.model_validate(payload))

    assert job.status.value == "SUCCEEDED"
    assert evaluator.warmups == [(5, 10, 20)]


def test_parameter_search_rejects_silently_ignored_warmup():
    payload = _base_search_request()
    payload["backtest"] = {"candles_csv_path": "data/fold.csv"}
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "fold",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "warmup_start_time": 5,
                "start_time": 10,
                "end_time": 20,
            },
        ],
    }
    executor = ParameterSearchJobExecutor(evaluator=_NoopEvaluator(), run_inline=True)

    job = executor.submit_search(ParameterSearchJobRequest.model_validate(payload))

    assert job.status.value == "FAILED"
    assert job.error == "evaluator does not support walk-forward warmup: fold"


def test_research_warmup_excludes_scoring_boundary_and_restores_trade_state(tmp_path):
    csv_path = tmp_path / "warmup.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "5,1,1,1,1,1\n"
        "9,1,1,1,1,1\n"
        "10,1,1,1,1,1\n"
        "11,1,1,1,1,1\n"
    )
    payload = _base_search_request()
    payload["start_time"] = 10_000
    payload["end_time"] = 11_000
    payload["backtest"] = {"candles_csv_path": str(csv_path)}
    request = ParameterSearchJobRequest.model_validate(payload)
    strategy = _WarmupRecordingStrategy()

    ResearchBacktestParameterEvaluator(
        lambda *_args: strategy,
    )._warm_up_strategy(
        request,
        strategy,
        warmup_start_time=5_000,
    )

    assert strategy.timestamps == [5_000, 9_000]
    assert strategy._in_position is False
    assert strategy.active_trade is None


def test_research_warmup_rejects_strategy_without_complete_state_contract(tmp_path):
    csv_path = tmp_path / "warmup.csv"
    csv_path.write_text(
        "timestamp,open,high,low,close,volume\n"
        "5,1,1,1,1,1\n"
        "9,1,1,1,1,1\n"
    )
    payload = _base_search_request()
    payload["start_time"] = 10_000
    payload["end_time"] = 11_000
    payload["backtest"] = {"candles_csv_path": str(csv_path)}
    request = ParameterSearchJobRequest.model_validate(payload)

    with pytest.raises(
        NotImplementedError,
        match="walk-forward trade-state isolation",
    ):
        ResearchBacktestParameterEvaluator(
            lambda *_args: BaseStrategyWithoutWarmupContract(),
        )._warm_up_strategy(
            request,
            BaseStrategyWithoutWarmupContract(),
            warmup_start_time=5_000,
        )


def test_parameter_search_rejects_fitness_without_evaluation_set():
    payload = _base_search_request()
    payload["fitness"] = {"expression": "return_mean"}

    with pytest.raises(ValidationError, match="fitness requires evaluation_set"):
        ParameterSearchJobRequest.model_validate(payload)


def test_parameter_search_rejects_fitness_with_non_score_objective():
    payload = _base_search_request()
    payload["objective"] = "minimize_drawdown"
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "fold",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {"candles_csv_path": "data/fold.csv"},
            },
        ],
    }
    payload["fitness"] = {"expression": "return_mean"}

    with pytest.raises(
        ValidationError,
        match="fitness requires objective=maximize_score",
    ):
        ParameterSearchJobRequest.model_validate(payload)


def test_parameter_search_rejects_overlapping_fitness_scoring_folds():
    payload = _base_search_request()
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "first",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {"candles_csv_path": "data/first.csv"},
            },
            {
                "dataset_id": "second",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 20,
                "end_time": 30,
                "warmup_start_time": 5,
                "backtest": {"candles_csv_path": "data/second.csv"},
            },
        ],
    }
    payload["fitness"] = {"expression": "return_mean"}

    with pytest.raises(ValidationError, match="fitness scoring folds overlap"):
        ParameterSearchJobRequest.model_validate(payload)


def test_parameter_search_allows_overlapping_warmup_only():
    payload = _base_search_request()
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "first",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {"candles_csv_path": "data/first.csv"},
            },
            {
                "dataset_id": "second",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 21,
                "end_time": 30,
                "warmup_start_time": 5,
                "backtest": {"candles_csv_path": "data/second.csv"},
            },
        ],
    }
    payload["fitness"] = {"expression": "return_mean"}

    request = ParameterSearchJobRequest.model_validate(payload)

    assert request.evaluation_set is not None


def test_parameter_search_limits_independent_trials():
    payload = _base_search_request()
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "fold",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {"candles_csv_path": "data/fold.csv"},
            },
        ],
    }
    payload["fitness"] = {
        "expression": "return_mean",
        "independent_trials": 1_000_000_001,
    }

    with pytest.raises(ValidationError, match="less than or equal to 1000000000"):
        ParameterSearchJobRequest.model_validate(payload)


def test_parameter_search_rejects_unregistered_fitness_expression():
    payload = _base_search_request()
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "fold",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {"candles_csv_path": "data/fold.csv"},
            },
        ],
    }
    payload["fitness"] = {"expression": "__import__('os')"}

    with pytest.raises(ValidationError, match="fitness function is not registered"):
        ParameterSearchJobRequest.model_validate(payload)


def test_walk_forward_fitness_prefers_stable_candidate():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/folds.csv",
        "initial_balance": "10000",
    }
    payload["candidates"] = [
        {"candidate_id": "fragile", "param_pack": {}},
        {"candidate_id": "stable", "param_pack": {}},
    ]
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "fold_2020",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
            },
            {
                "dataset_id": "fold_2021",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 21,
                "end_time": 30,
            },
        ],
    }
    payload["fitness"] = {}
    executor = ParameterSearchJobExecutor(
        evaluator=_WalkForwardFitnessEvaluator(),
        run_inline=True,
    )

    job = executor.submit_search(ParameterSearchJobRequest.model_validate(payload))

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    assert job.result["best_candidate"]["candidate_id"] == "stable"
    evaluations = {
        item["candidate_id"]: item for item in job.result["evaluations"]
    }
    stable_fitness = evaluations["stable"]["metrics"]["fitness"]
    assert stable_fitness["expression"].startswith("deflated_sharpe")
    assert (
        evaluations["stable"]["metrics"]["fitness_inputs"]
        == stable_fitness["inputs"]
    )
    assert stable_fitness["inputs"]["deflated_sharpe"] != "0"
    assert stable_fitness["inputs"]["return_worst"] == "0.004"
    assert stable_fitness["inputs"]["year_concentration"] == "0.5"
    assert stable_fitness["inputs"]["trade_count_min"] == "100"
    assert stable_fitness["inputs"]["trade_count_mean"] == "100"
    assert stable_fitness["inputs"]["trade_count_total"] == "200"
    assert evaluations["fragile"]["metrics"]["dataset_scores"] == {
        "fold_2020": "100",
        "fold_2021": "-10",
    }


def test_year_concentration_uses_returns_across_different_fold_balances():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/folds.csv",
        "initial_balance": "10000",
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "small",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {"initial_balance": "1000"},
            },
            {
                "dataset_id": "large",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 21,
                "end_time": 30,
                "backtest": {"initial_balance": "10000"},
            },
        ],
    }
    payload["fitness"] = {}

    job = ParameterSearchJobExecutor(
        evaluator=_BalanceScaledFitnessEvaluator(),
        run_inline=True,
    ).submit_search(ParameterSearchJobRequest.model_validate(payload))

    assert job.status.value == "SUCCEEDED"
    assert (
        job.result["evaluations"][0]["metrics"]["fitness"]["inputs"][
            "year_concentration"
        ]
        == "0.5"
    )


def test_walk_forward_fitness_rejects_fold_without_scoring_candles():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/folds.csv",
        "initial_balance": "10000",
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "empty",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
            }
        ],
    }
    payload["fitness"] = {}

    job = ParameterSearchJobExecutor(
        evaluator=_EmptyScoringFitnessEvaluator(),
        run_inline=True,
    ).submit_search(ParameterSearchJobRequest.model_validate(payload))

    assert job.status.value == "FAILED"
    assert job.error == "walk-forward dataset empty has no scoring candles"


def test_parameter_search_accepts_shared_backtest_with_evaluation_set():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/BTCUSDT_5m.csv",
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "full_history",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 1,
                "end_time": 2,
            },
        ],
    }

    request = ParameterSearchJobRequest.model_validate(payload)

    assert request.backtest is not None
    assert request.evaluation_set is not None


def test_dated_future_parameter_search_requires_complete_resolved_rules():
    payload = _base_search_request()
    payload["product_id"] = "RITHMIC:MNQ-202509"
    payload["backtest"] = {
        "candles_csv_path": "data/mnq.csv",
        "instrument": {"quantity_step": "1"},
    }

    with pytest.raises(ValidationError, match="requires price_tick"):
        ParameterSearchJobRequest.model_validate(payload)


def test_dated_future_dataset_merges_shared_and_override_rules():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/mnq.csv",
        "instrument": {"quantity_step": "1"},
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "mnq",
                "product_id": "RITHMIC:MNQ-202509",
                "timeframe": "1m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {"instrument": {"price_tick": "0.25"}},
            },
        ],
    }

    request = ParameterSearchJobRequest.model_validate(payload)

    assert request.evaluation_set.datasets[0].backtest.instrument.price_tick == Decimal(
        "0.25"
    )


def test_parameter_search_merges_dataset_backtest_overrides_with_shared_settings():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/shared.csv",
        "initial_balance": "25000",
        "maker_fee": "0.0002",
        "taker_fee": "0.0006",
        "write_reports": True,
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "shared_file",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
            },
            {
                "dataset_id": "override_file",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 20,
                "end_time": 30,
                "backtest": {
                    "candles_csv_path": "data/override.csv",
                    "maker_fee": "0.0004",
                    "write_reports": False,
                },
            },
        ],
    }
    request = ParameterSearchJobRequest.model_validate(payload)
    evaluator = _NoopEvaluator()
    executor = ParameterSearchJobExecutor(
        evaluator=evaluator,
        run_inline=True,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert len(evaluator.requests) == 2
    shared_request = evaluator.requests[0][0]
    override_request = evaluator.requests[1][0]
    assert shared_request.backtest.candles_csv_path == "data/shared.csv"
    assert override_request.backtest.candles_csv_path == "data/override.csv"
    assert override_request.backtest.initial_balance == Decimal("25000")
    assert override_request.backtest.maker_fee == Decimal("0.0004")
    assert override_request.backtest.taker_fee == Decimal("0.0006")
    assert override_request.backtest.write_reports is False


def test_parameter_search_merges_partial_instrument_overrides_by_field():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/shared.csv",
        "instrument": {
            "multiplier": "5",
            "fee_model": "per_contract",
            "capital_model": "per_contract",
            "capital_per_contract": "500",
        },
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "multiplier",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {"instrument": {"multiplier": "2"}},
            },
            {
                "dataset_id": "capital",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 20,
                "end_time": 30,
                "backtest": {"instrument": {"capital_per_contract": "750"}},
            },
            {
                "dataset_id": "notional",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 30,
                "end_time": 40,
                "backtest": {"instrument": {"capital_model": "notional"}},
            },
        ],
    }
    evaluator = _NoopEvaluator()
    executor = ParameterSearchJobExecutor(evaluator=evaluator, run_inline=True)

    job = executor.submit_search(ParameterSearchJobRequest.model_validate(payload))

    assert job.status.value == "SUCCEEDED"
    multiplier, capital, notional = [
        request.backtest.instrument for request, _ in evaluator.requests
    ]
    assert multiplier.multiplier == Decimal("2")
    assert multiplier.fee_model.value == "per_contract"
    assert multiplier.capital_model.value == "per_contract"
    assert multiplier.capital_per_contract == Decimal("500")
    assert capital.multiplier == Decimal("5")
    assert capital.capital_model.value == "per_contract"
    assert capital.capital_per_contract == Decimal("750")
    assert notional.multiplier == Decimal("5")
    assert notional.fee_model.value == "per_contract"
    assert notional.capital_model.value == "notional"
    assert notional.capital_per_contract is None


def test_parameter_search_allows_non_path_dataset_backtest_overrides():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/shared.csv",
        "initial_balance": "25000",
        "maker_fee": "0.0002",
        "taker_fee": "0.0006",
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "fee_override",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {
                    "maker_fee": "0.0008",
                    "write_reports": True,
                },
            },
        ],
    }

    request = ParameterSearchJobRequest.model_validate(payload)
    evaluator = _NoopEvaluator()
    executor = ParameterSearchJobExecutor(
        evaluator=evaluator,
        run_inline=True,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    dataset_request = evaluator.requests[0][0]
    assert dataset_request.backtest.candles_csv_path == "data/shared.csv"
    assert dataset_request.backtest.initial_balance == Decimal("25000")
    assert dataset_request.backtest.maker_fee == Decimal("0.0008")
    assert dataset_request.backtest.taker_fee == Decimal("0.0006")
    assert dataset_request.backtest.write_reports is True


def test_parameter_search_ignores_null_dataset_backtest_overrides():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/shared.csv",
        "initial_balance": "25000",
        "maker_fee": "0.0002",
        "taker_fee": "0.0006",
        "write_reports": True,
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "null_override",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {
                    "candles_csv_path": None,
                    "initial_balance": None,
                    "maker_fee": "0.0008",
                    "write_reports": None,
                },
            },
        ],
    }
    request = ParameterSearchJobRequest.model_validate(payload)
    evaluator = _NoopEvaluator()
    executor = ParameterSearchJobExecutor(
        evaluator=evaluator,
        run_inline=True,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    dataset_request = evaluator.requests[0][0]
    assert dataset_request.backtest.candles_csv_path == "data/shared.csv"
    assert dataset_request.backtest.initial_balance == Decimal("25000")
    assert dataset_request.backtest.maker_fee == Decimal("0.0008")
    assert dataset_request.backtest.taker_fee == Decimal("0.0006")
    assert dataset_request.backtest.write_reports is True
    assert job.result["evaluation_set"]["datasets"][0]["backtest"] == {
        "maker_fee": "0.0008",
    }
    assert job.result["evaluation_set"]["datasets"][0]["resolved_backtest"] == {
        "candles_csv_path": "data/shared.csv",
        "initial_balance": "25000",
        "maker_fee": "0.0008",
        "taker_fee": "0.0006",
        "write_reports": True,
    }


def test_parameter_search_rejects_partial_dataset_backtest_without_shared_path():
    payload = _base_search_request()
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "missing_path",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "backtest": {
                    "maker_fee": "0.0008",
                },
            },
        ],
    }

    with pytest.raises(
        ValidationError,
        match=(
            "evaluation_set datasets require candles_csv_path when shared "
            "backtest is not provided: missing_path"
        ),
    ):
        ParameterSearchJobRequest.model_validate(payload)


def test_parameter_search_aggregates_candidate_across_evaluation_set():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/BTCUSDT_5m.csv",
    }
    payload["candidates"] = [
        {
            "candidate_id": "candidate_a",
            "param_pack": {"short_window": 5, "long_window": 20, "score": 1},
        },
        {
            "candidate_id": "candidate_b",
            "param_pack": {"short_window": 10, "long_window": 30, "score": 3},
        },
    ]
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "trend",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
            },
            {
                "dataset_id": "chop",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 20,
                "end_time": 30,
            },
        ],
    }
    request = ParameterSearchJobRequest.model_validate(payload)
    evaluator = _NoopEvaluator()
    executor = ParameterSearchJobExecutor(
        evaluator=evaluator,
        run_inline=True,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert len(evaluator.requests) == 4
    assert job.result is not None
    assert job.result["best_candidate"]["candidate_id"] == "candidate_b"
    assert job.result["best_candidate"]["score_total"] == "9"
    assert job.result["best_candidate"]["max_drawdown"] == "5"
    resolved_backtest = {
        "candles_csv_path": "data/BTCUSDT_5m.csv",
        "initial_balance": "10000",
        "maker_fee": "0",
        "taker_fee": "0",
        "write_reports": False,
    }
    assert job.result["evaluation_set"]["datasets"] == [
        _dataset_payload(
            "trend",
            start_time=10,
            end_time=20,
            resolved_backtest=resolved_backtest,
        ),
        _dataset_payload(
            "chop",
            start_time=20,
            end_time=30,
            resolved_backtest=resolved_backtest,
        ),
    ]
    assert job.result["evaluations"][0]["metrics"]["evaluation_mode"] == "evaluation_set"
    assert job.result["evaluations"][0]["metrics"]["dataset_scores"] == {
        "trend": "2",
        "chop": "3",
    }
    assert job.result["evaluations"][0]["metrics"]["dataset_drawdowns"] == {
        "trend": "2",
        "chop": "5",
    }


def test_parameter_search_persists_evaluation_set_traceability(tmp_path):
    session_factory = _sqlite_gene_registry_session_factory(tmp_path)
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/BTCUSDT_5m.csv",
        "initial_balance": "25000",
        "maker_fee": "0.0002",
        "taker_fee": "0.0006",
        "write_reports": True,
    }
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "trend",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
                "metadata": {"regime": "trend"},
            },
            {
                "dataset_id": "selloff",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 20,
                "end_time": 30,
                "metadata": {"regime": "selloff"},
                "backtest": {
                    "candles_csv_path": "data/selloff.csv",
                    "maker_fee": "0.0004",
                    "write_reports": False,
                },
            },
        ],
    }
    request = ParameterSearchJobRequest.model_validate(payload)
    executor = ParameterSearchJobExecutor(
        evaluator=_NoopEvaluator(),
        run_inline=True,
        db_session_factory=session_factory,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    assert job.result["epoch_id"].startswith("epoch_")
    with session_factory() as session:
        epoch = session.get(EvolutionEpoch, job.result["epoch_id"])
        gene = (
            session.query(GeneRecord)
            .filter(GeneRecord.epoch_id == job.result["epoch_id"])
            .one()
        )

    trend_resolved_backtest = {
        "candles_csv_path": "data/BTCUSDT_5m.csv",
        "initial_balance": "25000",
        "maker_fee": "0.0002",
        "taker_fee": "0.0006",
        "write_reports": True,
    }
    selloff_backtest_override = {
        "candles_csv_path": "data/selloff.csv",
        "maker_fee": "0.0004",
        "write_reports": False,
    }
    selloff_resolved_backtest = {
        **trend_resolved_backtest,
        **selloff_backtest_override,
    }
    assert epoch.config_json["evaluation_set"]["datasets"] == [
        _dataset_payload(
            "trend",
            start_time=10,
            end_time=20,
            metadata={"regime": "trend"},
            resolved_backtest=trend_resolved_backtest,
        ),
        _dataset_payload(
            "selloff",
            start_time=20,
            end_time=30,
            metadata={"regime": "selloff"},
            backtest=selloff_backtest_override,
            resolved_backtest=selloff_resolved_backtest,
        ),
    ]
    assert gene.max_drawdown == Decimal("5.00000000")
    assert gene.score_breakdown["evaluation_mode"] == "evaluation_set"
    assert gene.score_breakdown["dataset_scores"] == {
        "trend": "2",
        "selloff": "3",
    }
    assert gene.score_breakdown["dataset_drawdowns"] == {
        "trend": "2",
        "selloff": "5",
    }
    assert gene.score_breakdown["datasets"]["trend"]["max_drawdown"] == "2"


def test_parameter_search_uses_worst_positive_drawdown_across_evaluation_set():
    payload = _base_search_request()
    payload["backtest"] = {
        "candles_csv_path": "data/BTCUSDT_5m.csv",
    }
    payload["candidates"] = [
        {
            "candidate_id": "risk_positive",
            "param_pack": {
                "score": 1,
                "drawdowns_by_start_time": {"10": "0.02", "20": "0.30"},
            },
        },
    ]
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "trend",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 10,
                "end_time": 20,
            },
            {
                "dataset_id": "selloff",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 20,
                "end_time": 30,
            },
        ],
    }
    request = ParameterSearchJobRequest.model_validate(payload)
    executor = ParameterSearchJobExecutor(
        evaluator=_DrawdownEvaluator(),
        run_inline=True,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    assert job.result["best_candidate"]["max_drawdown"] == "0.30"
    assert job.result["evaluations"][0]["metrics"]["dataset_drawdowns"] == {
        "trend": "0.02",
        "selloff": "0.30",
    }


def test_parameter_search_minimizes_drawdown_magnitude_for_negative_values():
    payload = _base_search_request()
    payload["objective"] = "minimize_drawdown"
    payload["candidates"] = [
        {
            "candidate_id": "small_loss",
            "param_pack": {"score": 1, "drawdown": "-0.02"},
        },
        {
            "candidate_id": "large_loss",
            "param_pack": {"score": 100, "drawdown": "-0.30"},
        },
    ]
    request = ParameterSearchJobRequest.model_validate(payload)
    executor = ParameterSearchJobExecutor(
        evaluator=_NoopEvaluator(),
        run_inline=True,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    assert job.result["best_candidate"]["candidate_id"] == "small_loss"
    assert job.result["best_candidate"]["max_drawdown"] == "0.02"


def test_parameter_search_normalizes_single_dataset_drawdown_boundary():
    payload = _base_search_request()
    payload["candidates"] = [
        {
            "candidate_id": "signed_loss",
            "param_pack": {"score": 1, "drawdown": "-0.40"},
        },
    ]
    request = ParameterSearchJobRequest.model_validate(payload)
    executor = ParameterSearchJobExecutor(
        evaluator=_NoopEvaluator(),
        run_inline=True,
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    assert job.result["best_candidate"]["max_drawdown"] == "0.40"
    assert job.result["evaluations"][0]["max_drawdown"] == "0.40"
    assert job.result["evaluations"][0]["metrics"]["max_drawdown"] == "0.40"


def test_parameter_search_rejects_evaluation_dataset_without_backtest_settings():
    payload = _base_search_request()
    payload["evaluation_set"] = {
        "datasets": [
            {
                "dataset_id": "full_history",
                "product_id": PRODUCT_ID,
                "timeframe": "5m",
                "start_time": 1,
                "end_time": 2,
            },
        ],
    }

    with pytest.raises(
        ValidationError,
        match=(
            "evaluation_set datasets require candles_csv_path when shared "
            "backtest is not provided: full_history"
        ),
    ):
        ParameterSearchJobRequest.model_validate(payload)
