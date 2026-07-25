from decimal import Decimal

import pytest

from src.control_plane.fitness import (
    deflated_sharpe_probability,
    evaluate_fitness_expression,
    expected_maximum_sharpe,
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
