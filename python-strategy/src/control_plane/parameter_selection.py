"""Candidate selection shared by parameter-search orchestration and persistence."""

from decimal import Decimal

from src.control_plane.models import (
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)


def _select_best_candidate(
    request: ParameterSearchJobRequest,
    evaluations: list[ParameterEvaluationResult],
) -> ParameterEvaluationResult:
    if request.objective in {"maximize_score", "maximize_return"}:
        return max(evaluations, key=lambda result: result.score_total)
    if request.objective == "minimize_drawdown":
        return min(
            evaluations,
            key=lambda result: (
                _drawdown_risk_key(result.max_drawdown),
                -_decimal_key(result.score_total),
            ),
        )
    raise ValueError(f"unsupported objective: {request.objective}")


def _drawdown_risk_key(drawdown: Decimal) -> Decimal:
    return abs(drawdown)


def _decimal_key(value: Decimal) -> Decimal:
    return value
