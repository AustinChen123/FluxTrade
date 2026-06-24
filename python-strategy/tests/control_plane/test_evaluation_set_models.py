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
    def evaluate(self, request, candidate):
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=Decimal("0"),
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
                "param_pack": {"short_window": 5, "long_window": 20},
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


def test_parameter_search_rejects_backtest_and_evaluation_set_together():
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

    with pytest.raises(ValidationError, match="provide either backtest or evaluation_set"):
        ParameterSearchJobRequest.model_validate(payload)


def test_parameter_search_reports_evaluation_set_runtime_as_unsupported():
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
    assert job.error == "evaluation_set parameter search is not implemented yet"
