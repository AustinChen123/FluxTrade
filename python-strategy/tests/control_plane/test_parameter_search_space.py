from __future__ import annotations

from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.control_plane import (
    InMemoryJobStore,
    ParameterEvaluationResult,
    ParameterSearchJobExecutor,
)
from src.control_plane.models import ParameterCandidate, ParameterSearchJobRequest
from src.control_plane.search_space import generate_parameter_candidates


class _ScoreFromWindowEvaluator:
    def __init__(self) -> None:
        self.param_packs: list[dict] = []

    def evaluate(
        self,
        request: ParameterSearchJobRequest,
        candidate: ParameterCandidate,
    ) -> ParameterEvaluationResult:
        del request
        self.param_packs.append(candidate.param_pack)
        score = Decimal(str(candidate.param_pack["short_window"]))
        return ParameterEvaluationResult(
            candidate_id=candidate.candidate_id,
            score_total=score,
            metrics={"score": str(score)},
        )


def test_parameter_search_generates_full_grid_candidates_in_order():
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
                        "min": 5,
                        "max": 10,
                        "step": 5,
                    },
                    "long_window": {
                        "type": "integer",
                        "min": 15,
                        "max": 20,
                        "step": 5,
                    },
                    "quantity": {
                        "type": "decimal",
                        "min": "0.01",
                        "max": "0.01",
                        "step": "0.01",
                    },
                }
            },
            "candidate_sample_count": 4,
            "seed": 7,
        }
    )
    assert request.search_space is not None

    candidates = generate_parameter_candidates(
        request.search_space,
        sample_count=request.candidate_sample_count or 0,
        seed=request.seed,
    )

    assert [candidate.candidate_id for candidate in candidates] == [
        "generated_000001",
        "generated_000002",
        "generated_000003",
        "generated_000004",
    ]
    assert [candidate.param_pack for candidate in candidates] == [
        {"short_window": 5, "long_window": 15, "quantity": Decimal("0.01")},
        {"short_window": 5, "long_window": 20, "quantity": Decimal("0.01")},
        {"short_window": 10, "long_window": 15, "quantity": Decimal("0.01")},
        {"short_window": 10, "long_window": 20, "quantity": Decimal("0.01")},
    ]


def test_parameter_search_space_sampling_is_seeded_and_unique():
    payload = {
        "parameters": {
            "short_window": {"type": "integer", "min": 5, "max": 50, "step": 5},
            "mode": {"type": "categorical", "choices": ["fast", "safe"]},
        }
    }
    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": "golden_cross",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "15m",
            "start_time": 1,
            "end_time": 2,
            "search_space": payload,
            "candidate_sample_count": 5,
            "seed": 17,
        }
    )
    assert request.search_space is not None

    first = generate_parameter_candidates(
        request.search_space,
        sample_count=5,
        seed=request.seed,
    )
    second = generate_parameter_candidates(
        request.search_space,
        sample_count=5,
        seed=request.seed,
    )

    assert [candidate.param_pack for candidate in first] == [
        candidate.param_pack for candidate in second
    ]
    assert len({tuple(candidate.param_pack.items()) for candidate in first}) == 5


def test_parameter_search_request_requires_one_candidate_source():
    base_payload = {
        "strategy_id": "golden_cross",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "15m",
        "start_time": 1,
        "end_time": 2,
    }

    with pytest.raises(ValidationError, match="provide exactly one"):
        ParameterSearchJobRequest.model_validate(base_payload)

    with pytest.raises(ValidationError, match="provide exactly one"):
        ParameterSearchJobRequest.model_validate(
            {
                **base_payload,
                "candidates": [
                    {"candidate_id": "manual", "param_pack": {"score": "1"}}
                ],
                "search_space": {
                    "parameters": {
                        "score": {"type": "integer", "min": 1, "max": 2, "step": 1}
                    }
                },
                "candidate_sample_count": 1,
            }
        )

    with pytest.raises(ValidationError, match="candidate_sample_count is required"):
        ParameterSearchJobRequest.model_validate(
            {
                **base_payload,
                "search_space": {
                    "parameters": {
                        "score": {"type": "integer", "min": 1, "max": 2, "step": 1}
                    }
                },
            }
        )


def test_parameter_search_executor_evaluates_generated_candidates():
    store = InMemoryJobStore()
    evaluator = _ScoreFromWindowEvaluator()
    executor = ParameterSearchJobExecutor(
        evaluator,
        store=store,
        run_inline=True,
    )
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
                        "min": 5,
                        "max": 10,
                        "step": 5,
                    },
                    "long_window": {
                        "type": "integer",
                        "min": 20,
                        "max": 20,
                        "step": 5,
                    },
                }
            },
            "candidate_sample_count": 2,
        }
    )

    job = executor.submit_search(request)

    assert job.status.value == "SUCCEEDED"
    assert job.result is not None
    assert job.result["best_candidate"]["candidate_id"] == "generated_000002"
    assert evaluator.param_packs == [
        {"short_window": 5, "long_window": 20},
        {"short_window": 10, "long_window": 20},
    ]
