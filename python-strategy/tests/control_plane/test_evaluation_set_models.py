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
from src.control_plane.parameter_search import ParameterSearchJobExecutor
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy, SystemEvent


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"


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


def test_parameter_search_rejects_warmup_datasets_before_job_creation():
    payload = _base_search_request()
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

    with pytest.raises(
        ValidationError,
        match=(
            "parameter_search evaluation_set does not support "
            "warmup_start_time yet: with_warmup"
        ),
    ):
        ParameterSearchJobRequest.model_validate(payload)


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
