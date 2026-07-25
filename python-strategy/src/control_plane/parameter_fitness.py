"""Walk-forward fitness aggregation for parameter-search evaluations."""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Callable

from src.control_plane.backtest_jobs import _json_safe
from src.control_plane.fitness import (
    WalkForwardMetricData,
    calculate_registered_fitness_inputs,
    deflated_sharpe_probability,
    evaluate_fitness_expression,
    expected_maximum_sharpe,
    fitness_metric_contract,
)
from src.control_plane.models import (
    CsvSignalBacktestEvaluationConfig,
    EvaluationDatasetConfig,
    ParameterEvaluationResult,
    ParameterSearchJobRequest,
)


_FITNESS_SCORE_QUANTUM = Decimal("0.00000001")
_FITNESS_SCORE_MAX = Decimal("9999999999.99999999")

_BacktestResolver = Callable[
    [ParameterSearchJobRequest, EvaluationDatasetConfig],
    CsvSignalBacktestEvaluationConfig | None,
]


def _apply_walk_forward_fitness(
    request: ParameterSearchJobRequest,
    evaluations: list[ParameterEvaluationResult],
    *,
    benchmark_evaluations: list[ParameterEvaluationResult] | None = None,
) -> list[ParameterEvaluationResult]:
    if request.fitness is None:
        return evaluations
    if not evaluations:
        return evaluations

    benchmark_population = benchmark_evaluations or evaluations
    candidate_sharpes = [
        Decimal(str(evaluation.metrics["fitness_inputs"]["daily_sharpe"]))
        for evaluation in benchmark_population
    ]
    independent_trials = request.fitness.independent_trials or _default_trial_count(
        request,
        evaluations,
    )
    benchmark_sharpe = expected_maximum_sharpe(
        candidate_sharpes,
        independent_trials=independent_trials,
    )

    scored = []
    for evaluation in evaluations:
        inputs = {
            name: Decimal(str(value))
            for name, value in evaluation.metrics["fitness_inputs"].items()
        }
        statistics = evaluation.metrics["fitness_statistics"]
        inputs["deflated_sharpe"] = deflated_sharpe_probability(
            observed_sharpe=inputs["daily_sharpe"],
            benchmark_sharpe=benchmark_sharpe,
            observations=int(statistics["observations"]),
            skewness=Decimal(str(statistics["skewness"])),
            kurtosis=Decimal(str(statistics["kurtosis"])),
        )
        score = _canonical_fitness_score(
            evaluate_fitness_expression(
                request.fitness.expression,
                inputs,
            )
        )
        if not score.is_finite():
            raise ValueError("walk-forward fitness must be finite")
        metrics = {
            **evaluation.metrics,
            "aggregation": "registered_walk_forward_fitness",
            "fitness_inputs": inputs,
            "fitness": {
                "expression": request.fitness.expression,
                "independent_trials": independent_trials,
                "benchmark_sharpe": benchmark_sharpe,
                "metric_contract": fitness_metric_contract(),
                "inputs": inputs,
                "score": score,
            },
        }
        scored.append(
            evaluation.model_copy(
                update={
                    "score_total": score,
                    "metrics": _json_safe(metrics),
                }
            )
        )
    return scored


def _default_trial_count(
    request: ParameterSearchJobRequest,
    evaluations: list[ParameterEvaluationResult],
) -> int:
    if request.evolution is not None:
        return request.evolution.population_size * request.evolution.max_generations
    return len(evaluations)


def _canonical_fitness_score(score: Decimal) -> Decimal:
    if not score.is_finite():
        raise ValueError("walk-forward fitness must be finite")
    if abs(score) > _FITNESS_SCORE_MAX:
        raise ValueError("walk-forward fitness exceeds Numeric(18,8) range")
    return score.quantize(_FITNESS_SCORE_QUANTUM)


def _walk_forward_inputs(
    request: ParameterSearchJobRequest,
    dataset_scores: dict[str, Decimal],
    dataset_drawdowns: dict[str, Decimal],
    dataset_results: dict[str, dict[str, Any]],
    *,
    backtest_resolver: _BacktestResolver,
) -> tuple[dict[str, Decimal], dict[str, Decimal | int]]:
    assert request.evaluation_set is not None
    scores = list(dataset_scores.values())
    returns = []
    drawdown_percentages = []
    daily_sharpes = []
    annualized_sharpes = []
    trade_counts = []
    pooled_moments = _empty_return_moments()
    yearly_returns: dict[str, Decimal] = {}
    mean_r_values = []

    for dataset in request.evaluation_set.resolved_datasets:
        backtest = backtest_resolver(request, dataset)
        if backtest is None:
            raise ValueError(
                f"evaluation dataset {dataset.dataset_id} requires backtest settings"
            )
        initial_balance = backtest.initial_balance
        returns.append(dataset_scores[dataset.dataset_id] / initial_balance)
        drawdown_percentages.append(
            dataset_drawdowns[dataset.dataset_id] / initial_balance
        )

        metrics = dataset_results[dataset.dataset_id]
        missing = {
            "daily_return_moments",
            "closed_trade_count",
            "equity_sample_count",
            "yearly_mark_to_market_returns",
        } - set(metrics)
        if missing:
            names = ", ".join(sorted(missing))
            raise ValueError(
                f"walk-forward dataset {dataset.dataset_id} missing metrics: {names}"
            )
        if int(metrics["equity_sample_count"]) < 1:
            raise ValueError(
                f"walk-forward dataset {dataset.dataset_id} has no scoring candles"
            )
        fold_moments = _normalize_return_moments(metrics["daily_return_moments"])
        _add_return_moments(pooled_moments, fold_moments)
        fold_statistics = _return_statistics(fold_moments)
        daily_sharpes.append(fold_statistics["sharpe"])
        annualized_sharpes.append(
            fold_statistics["sharpe"] * Decimal(365).sqrt()
        )
        trade_count = int(metrics["closed_trade_count"])
        trade_counts.append(trade_count)
        for year, value in metrics["yearly_mark_to_market_returns"].items():
            year = str(year)
            if len(year) != 4 or not year.isdigit():
                raise ValueError(
                    f"invalid yearly return key for {dataset.dataset_id}: {year}"
                )
            yearly_return = Decimal(str(value))
            yearly_returns[year] = (
                (Decimal("1") + yearly_returns.get(year, Decimal("0")))
                * (Decimal("1") + yearly_return)
                - Decimal("1")
            )
        if "mean_r" in metrics:
            mean_r_values.append(Decimal(str(metrics["mean_r"])))

    pooled_statistics = _return_statistics(pooled_moments)

    inputs = calculate_registered_fitness_inputs(
        WalkForwardMetricData(
            scores=tuple(scores),
            returns=tuple(returns),
            drawdown_percentages=tuple(drawdown_percentages),
            daily_sharpes=tuple(daily_sharpes),
            annualized_sharpes=tuple(annualized_sharpes),
            trade_counts=tuple(trade_counts),
            pooled_daily_sharpe=pooled_statistics["sharpe"],
            yearly_returns=yearly_returns,
            mean_r_values=tuple(mean_r_values),
        )
    )

    statistics: dict[str, Decimal | int] = {
        "observations": pooled_moments["count"],
        "skewness": pooled_statistics["skewness"],
        "kurtosis": pooled_statistics["kurtosis"],
    }
    return inputs, statistics


def _empty_return_moments() -> dict[str, Decimal | int]:
    return {
        "count": 0,
        "sum": Decimal("0"),
        "sum_squares": Decimal("0"),
        "sum_cubes": Decimal("0"),
        "sum_fourth": Decimal("0"),
    }


def _normalize_return_moments(value: Any) -> dict[str, Decimal | int]:
    if not isinstance(value, dict):
        raise ValueError("daily_return_moments must be an object")
    required = {"count", "sum", "sum_squares", "sum_cubes", "sum_fourth"}
    missing = required - set(value)
    if missing:
        raise ValueError(
            "daily_return_moments missing fields: "
            + ", ".join(sorted(missing))
        )
    count = int(value["count"])
    if count < 0:
        raise ValueError("daily_return_moments count cannot be negative")
    normalized: dict[str, Decimal | int] = {"count": count}
    for name in required - {"count"}:
        decimal_value = Decimal(str(value[name]))
        if not decimal_value.is_finite():
            raise ValueError("daily_return_moments values must be finite")
        normalized[name] = decimal_value
    return normalized


def _add_return_moments(
    target: dict[str, Decimal | int],
    source: dict[str, Decimal | int],
) -> None:
    target["count"] = int(target["count"]) + int(source["count"])
    for name in ("sum", "sum_squares", "sum_cubes", "sum_fourth"):
        target[name] = Decimal(target[name]) + Decimal(source[name])


def _return_statistics(
    moments: dict[str, Decimal | int],
) -> dict[str, Decimal]:
    count = int(moments["count"])
    if count < 2:
        return {
            "sharpe": Decimal("0"),
            "skewness": Decimal("0"),
            "kurtosis": Decimal("3"),
        }
    sample_count = Decimal(count)
    total = Decimal(moments["sum"])
    raw_second = Decimal(moments["sum_squares"]) / sample_count
    raw_third = Decimal(moments["sum_cubes"]) / sample_count
    raw_fourth = Decimal(moments["sum_fourth"]) / sample_count
    mean = total / sample_count
    second_moment = max(raw_second - mean * mean, Decimal("0"))
    sample_variance = second_moment * sample_count / Decimal(count - 1)
    sharpe = mean / sample_variance.sqrt() if sample_variance > 0 else Decimal("0")
    if second_moment == 0:
        return {
            "sharpe": sharpe,
            "skewness": Decimal("0"),
            "kurtosis": Decimal("3"),
        }
    third_moment = raw_third - Decimal("3") * mean * raw_second + Decimal("2") * (
        mean**3
    )
    fourth_moment = (
        raw_fourth
        - Decimal("4") * mean * raw_third
        + Decimal("6") * mean * mean * raw_second
        - Decimal("3") * (mean**4)
    )
    return {
        "sharpe": sharpe,
        "skewness": third_moment / (second_moment.sqrt() ** 3),
        "kurtosis": fourth_moment / (second_moment**2),
    }
