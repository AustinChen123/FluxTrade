from decimal import Decimal

import pytest

from src.control_plane.fitness import (
    FITNESS_METRIC_DEFINITIONS,
    REGISTERED_FITNESS_INPUTS,
    FitnessMetricDefinition,
    WalkForwardMetricData,
    calculate_registered_fitness_inputs,
    deflated_sharpe_probability,
    evaluate_fitness_expression,
    expected_maximum_sharpe,
    fitness_metric_contract,
    validate_fitness_expression,
)
from src.control_plane.parameter_search import _canonical_fitness_score


def test_registered_fitness_expression_uses_decimal_math():
    result = evaluate_fitness_expression(
        "power(return_mean, 2) + exp(0) + ln(1) + sqrt(4) - abs(-1)",
        {"return_mean": Decimal("0.25")},
    )

    assert result == Decimal("2.0625")


def test_fitness_expression_preserves_decimal_literal_text():
    result = evaluate_fitness_expression(
        "0.10000000000000001",
        {},
    )

    assert result == Decimal("0.10000000000000001")


def test_metric_registry_owns_names_calculators_and_contract():
    data = WalkForwardMetricData(
        scores=(Decimal("10"), Decimal("20")),
        returns=(Decimal("0.01"), Decimal("0.02")),
        drawdown_percentages=(Decimal("0.03"), Decimal("0.04")),
        daily_sharpes=(Decimal("1"), Decimal("2")),
        annualized_sharpes=(Decimal("19"), Decimal("38")),
        trade_counts=(2, 4),
        pooled_daily_sharpe=Decimal("1.5"),
        yearly_returns={"2025": Decimal("0.1")},
        mean_r_values=(),
    )

    inputs = calculate_registered_fitness_inputs(data)
    contract = fitness_metric_contract()

    assert set(contract["metrics"]) == REGISTERED_FITNESS_INPUTS
    assert set(inputs) == REGISTERED_FITNESS_INPUTS - {"worst_fold_mean_r"}
    assert len(FITNESS_METRIC_DEFINITIONS) == len(REGISTERED_FITNESS_INPUTS)
    sharpe_contract = contract["metrics"]["annualized_sharpe"]
    assert sharpe_contract["sampling_interval"] == "utc_calendar_day"
    assert sharpe_contract["periods_per_year"] == 365
    assert sharpe_contract["risk_free_rate_annual"] == "0"
    assert sharpe_contract["variance_ddof"] == 1
    assert (
        sharpe_contract["missing_day_policy"]
        == "carry_prior_equity_zero_return"
    )
    assert (
        contract["metrics"]["year_concentration"]["year_return_aggregation"]
        == "compound_fold_returns_within_calendar_year"
    )


def test_custom_metric_definition_carries_calculation_and_contract_together():
    definition = FitnessMetricDefinition(
        name="custom_stability",
        metric_id="custom_stability_v1",
        calculator=lambda data: data.returns[0] * Decimal("2"),
        source="fold_endpoint_returns",
        aggregation="first_fold_times_two",
    )
    data = WalkForwardMetricData(
        scores=(Decimal("1"),),
        returns=(Decimal("0.25"),),
        drawdown_percentages=(Decimal("0"),),
        daily_sharpes=(Decimal("0"),),
        annualized_sharpes=(Decimal("0"),),
        trade_counts=(1,),
        pooled_daily_sharpe=Decimal("0"),
        yearly_returns={},
        mean_r_values=(),
    )

    assert calculate_registered_fitness_inputs(
        data,
        definitions=(definition,),
    ) == {"custom_stability": Decimal("0.50")}
    assert fitness_metric_contract(definitions=(definition,))["metrics"] == {
        "custom_stability": {
            "metric_id": "custom_stability_v1",
            "source": "fold_endpoint_returns",
            "aggregation": "first_fold_times_two",
        }
    }


def test_metric_registry_rejects_duplicate_names_and_nonfinite_outputs():
    data = WalkForwardMetricData(
        scores=(Decimal("1"),),
        returns=(Decimal("0"),),
        drawdown_percentages=(Decimal("0"),),
        daily_sharpes=(Decimal("0"),),
        annualized_sharpes=(Decimal("0"),),
        trade_counts=(0,),
        pooled_daily_sharpe=Decimal("0"),
        yearly_returns={},
        mean_r_values=(),
    )
    first = FitnessMetricDefinition(
        "custom",
        "custom_v1",
        lambda _data: Decimal("0"),
        "test",
        "identity",
    )
    nonfinite = FitnessMetricDefinition(
        "nonfinite",
        "nonfinite_v1",
        lambda _data: Decimal("NaN"),
        "test",
        "identity",
    )

    with pytest.raises(ValueError, match="names must be unique"):
        fitness_metric_contract(definitions=(first, first))
    with pytest.raises(ValueError, match="must be finite"):
        calculate_registered_fitness_inputs(
            data,
            definitions=(nonfinite,),
        )


@pytest.mark.parametrize(
    "expression",
    [
        "__import__('os')",
        "return_mean.real",
        "return_mean[0]",
        "unknown_metric + 1",
        "min(value=return_mean)",
        "'not numeric'",
        "0x10",
    ],
)
def test_fitness_expression_rejects_unregistered_syntax(expression):
    with pytest.raises(ValueError):
        validate_fitness_expression(expression)


def test_fitness_expression_requires_referenced_input():
    with pytest.raises(ValueError, match="fitness input is unavailable: return_worst"):
        evaluate_fitness_expression(
            "return_mean + return_worst",
            {"return_mean": Decimal("0.1")},
        )


@pytest.mark.parametrize(
    "expression",
    [
        "exp(101)",
        "power(2, 101)",
        "2 ** 101",
    ],
)
def test_fitness_expression_limits_expensive_decimal_operations(expression):
    with pytest.raises(ValueError, match="too large"):
        evaluate_fitness_expression(expression, {})


def test_fitness_expression_limits_nested_power_results():
    with pytest.raises(ValueError, match="result is too large"):
        evaluate_fitness_expression("power(power(10, 100), 100)", {})


@pytest.mark.parametrize("expression", ["1e1001", "1e-1001"])
def test_fitness_expression_limits_decimal_literal_exponents(expression):
    with pytest.raises(ValueError, match="result is too large"):
        validate_fitness_expression(expression)
    with pytest.raises(ValueError, match="result is too large"):
        evaluate_fitness_expression(expression, {})


def test_fitness_expression_limits_direct_input_results():
    with pytest.raises(ValueError, match="result is too large"):
        evaluate_fitness_expression(
            "score_sum",
            {"score_sum": Decimal("1e1001")},
        )


@pytest.mark.parametrize(
    "expression",
    [
        "abs(1, 2)",
        "sqrt()",
        "power(2)",
        "min()",
    ],
)
def test_fitness_expression_validates_function_arity(expression):
    with pytest.raises(ValueError, match="requires"):
        validate_fitness_expression(expression)


def test_expected_maximum_sharpe_accounts_for_multiple_trials():
    benchmark = expected_maximum_sharpe(
        [Decimal("-1"), Decimal("0"), Decimal("1")],
        independent_trials=10,
    )

    assert benchmark > 0
    assert (
        expected_maximum_sharpe(
            [Decimal("1")],
            independent_trials=1,
        )
        == 0
    )


def test_expected_maximum_sharpe_moves_with_sample_mean():
    baseline = expected_maximum_sharpe(
        [Decimal("0"), Decimal("1"), Decimal("2")],
        independent_trials=10,
    )
    shifted = expected_maximum_sharpe(
        [Decimal("1"), Decimal("2"), Decimal("3")],
        independent_trials=10,
    )

    assert shifted - baseline == Decimal("1")


def test_fitness_score_is_canonical_before_selection_and_persistence():
    assert _canonical_fitness_score(Decimal("1.000000004")) == Decimal(
        "1.00000000"
    )
    assert _canonical_fitness_score(Decimal("1.000000006")) == Decimal(
        "1.00000001"
    )
    with pytest.raises(ValueError, match=r"Numeric\(18,8\)"):
        _canonical_fitness_score(Decimal("10000000000"))


def test_deflated_sharpe_probability_increases_with_observed_sharpe():
    weak = deflated_sharpe_probability(
        observed_sharpe=Decimal("0.2"),
        benchmark_sharpe=Decimal("0.1"),
        observations=100,
    )
    strong = deflated_sharpe_probability(
        observed_sharpe=Decimal("1.0"),
        benchmark_sharpe=Decimal("0.1"),
        observations=100,
    )

    assert Decimal("0.5") < weak < strong < Decimal("1")


def test_deflated_sharpe_requires_observations():
    assert (
        deflated_sharpe_probability(
            observed_sharpe=Decimal("1"),
            benchmark_sharpe=Decimal("0"),
            observations=1,
        )
        == 0
    )
