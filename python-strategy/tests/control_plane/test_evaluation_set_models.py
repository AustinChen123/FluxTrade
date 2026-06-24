from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.control_plane.models import (
    EvaluationSetConfig,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)
from src.control_plane.parameter_search import ParameterSearchJobExecutor


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"


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
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            max_drawdown=drawdown,
            metrics={
                "product_id": request.product_id,
                "timeframe": request.timeframe,
                "start_time": request.start_time,
            },
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


def test_parameter_search_accepts_evaluation_set_payload():
    payload = _base_search_request()
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
                "warmup_start_time": 1_700_000_000_000,
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
    assert request.evaluation_set.datasets[1].warmup_start_time == 1_700_000_000_000


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
    assert job.result["best_candidate"]["max_drawdown"] == "-5"
    assert job.result["evaluation_set"]["datasets"] == [
        {
            "dataset_id": "trend",
            "product_id": PRODUCT_ID,
            "timeframe": "5m",
            "start_time": 10,
            "end_time": 20,
            "warmup_start_time": None,
            "metadata": {},
        },
        {
            "dataset_id": "chop",
            "product_id": PRODUCT_ID,
            "timeframe": "5m",
            "start_time": 20,
            "end_time": 30,
            "warmup_start_time": None,
            "metadata": {},
        },
    ]
    assert job.result["evaluations"][0]["metrics"]["evaluation_mode"] == "evaluation_set"
    assert job.result["evaluations"][0]["metrics"]["dataset_scores"] == {
        "trend": "2",
        "chop": "3",
    }


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
    request = ParameterSearchJobRequest.model_validate(payload)
    executor = ParameterSearchJobExecutor(
        evaluator=_NoopEvaluator(),
        run_inline=True,
    )

    job = executor.submit_search(request)

    assert job.status.value == "FAILED"
    assert job.error == "evaluation dataset full_history requires backtest settings"
