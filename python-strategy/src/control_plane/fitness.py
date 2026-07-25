"""Safe, registered fitness expressions for walk-forward evaluation."""

from __future__ import annotations

import ast
from decimal import Decimal
import re
from statistics import NormalDist
from typing import Callable, Mapping, Sequence


DEFAULT_WALK_FORWARD_FITNESS = (
    "deflated_sharpe + return_worst - drawdown_pct_worst "
    "- return_std - year_concentration"
)

REGISTERED_FITNESS_INPUTS = frozenset(
    {
        "deflated_sharpe",
        "annualized_sharpe",
        "annualized_sharpe_worst",
        "daily_sharpe",
        "daily_sharpe_worst",
        "drawdown_pct_worst",
        "fold_count",
        "return_mean",
        "return_std",
        "return_worst",
        "score_mean",
        "score_sum",
        "score_worst",
        "trade_count_min",
        "trade_count_mean",
        "trade_count_total",
        "worst_fold_mean_r",
        "year_concentration",
    }
)

_MAX_EXPRESSION_LENGTH = 512
_MAX_AST_NODES = 128
_MAX_RESULT_ADJUSTED_EXPONENT = 1_000
_DECIMAL_LITERAL = re.compile(
    r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?\Z"
)


def validate_fitness_expression(expression: str) -> str:
    """Validate and normalize a fitness expression without evaluating it."""
    normalized = expression.strip()
    if not normalized:
        raise ValueError("fitness expression cannot be blank")
    if len(normalized) > _MAX_EXPRESSION_LENGTH:
        raise ValueError("fitness expression is too long")
    tree = _parse_expression(normalized)
    nodes = list(ast.walk(tree))
    if len(nodes) > _MAX_AST_NODES:
        raise ValueError("fitness expression is too complex")
    _validate_node(tree, normalized)
    return normalized


def evaluate_fitness_expression(
    expression: str,
    inputs: Mapping[str, Decimal],
) -> Decimal:
    """Evaluate a validated expression against registered Decimal inputs."""
    normalized = validate_fitness_expression(expression)
    unknown_inputs = set(inputs) - REGISTERED_FITNESS_INPUTS
    if unknown_inputs:
        names = ", ".join(sorted(unknown_inputs))
        raise ValueError(f"unregistered fitness inputs: {names}")
    return _bounded_result(
        _evaluate_node(_parse_expression(normalized), inputs, normalized)
    )


def deflated_sharpe_probability(
    *,
    observed_sharpe: Decimal,
    benchmark_sharpe: Decimal,
    observations: int,
    skewness: Decimal = Decimal("0"),
    kurtosis: Decimal = Decimal("3"),
) -> Decimal:
    """Return the probabilistic Sharpe confidence against a deflated benchmark."""
    if observations < 2:
        return Decimal("0")
    variance_term = (
        Decimal("1")
        - skewness * observed_sharpe
        + ((kurtosis - Decimal("1")) / Decimal("4"))
        * observed_sharpe
        * observed_sharpe
    )
    if variance_term <= 0:
        raise ValueError("deflated Sharpe variance term must be positive")
    z_score = (
        (observed_sharpe - benchmark_sharpe)
        * Decimal(observations - 1).sqrt()
        / variance_term.sqrt()
    )
    return Decimal(str(NormalDist().cdf(float(z_score))))


def expected_maximum_sharpe(
    sharpes: Sequence[Decimal],
    *,
    independent_trials: int,
) -> Decimal:
    """Estimate the multiple-testing Sharpe benchmark from candidate dispersion."""
    if independent_trials < 1:
        raise ValueError("independent_trials must be positive")
    if independent_trials == 1 or len(sharpes) < 2:
        return Decimal("0")

    mean = sum(sharpes, Decimal("0")) / Decimal(len(sharpes))
    variance = sum(
        ((value - mean) * (value - mean) for value in sharpes),
        Decimal("0"),
    ) / Decimal(len(sharpes) - 1)
    if variance == 0:
        return mean

    standard_deviation = variance.sqrt()
    normal = NormalDist()
    gamma = Decimal("0.5772156649015328606")
    trial_count = Decimal(independent_trials)
    first_quantile = Decimal(
        str(normal.inv_cdf(float(Decimal("1") - Decimal("1") / trial_count)))
    )
    second_quantile = Decimal(
        str(
            normal.inv_cdf(
                float(
                    Decimal("1")
                    - Decimal("1") / (trial_count * Decimal(str(_EULER_NUMBER)))
                )
            )
        )
    )
    return mean + standard_deviation * (
        (Decimal("1") - gamma) * first_quantile + gamma * second_quantile
    )


_EULER_NUMBER = Decimal("2.7182818284590452354")


def _parse_expression(expression: str) -> ast.Expression:
    try:
        parsed = ast.parse(expression, mode="eval")
    except SyntaxError as exc:
        raise ValueError("invalid fitness expression syntax") from exc
    assert isinstance(parsed, ast.Expression)
    return parsed


def _validate_node(node: ast.AST, expression: str) -> None:
    if isinstance(node, ast.Expression):
        _validate_node(node.body, expression)
        return
    if isinstance(node, ast.BinOp):
        if not isinstance(node.op, (ast.Add, ast.Sub, ast.Mult, ast.Div, ast.Pow)):
            raise ValueError("unsupported fitness operator")
        _validate_node(node.left, expression)
        _validate_node(node.right, expression)
        return
    if isinstance(node, ast.UnaryOp):
        if not isinstance(node.op, (ast.UAdd, ast.USub)):
            raise ValueError("unsupported fitness unary operator")
        _validate_node(node.operand, expression)
        return
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name) or node.func.id not in _FUNCTIONS:
            raise ValueError("fitness function is not registered")
        if node.keywords:
            raise ValueError("fitness functions do not accept keyword arguments")
        minimum_args, maximum_args = _FUNCTION_ARITY[node.func.id]
        if len(node.args) < minimum_args or (
            maximum_args is not None and len(node.args) > maximum_args
        ):
            expected = (
                f"at least {minimum_args}"
                if maximum_args is None
                else str(minimum_args)
            )
            raise ValueError(
                f"fitness function {node.func.id} requires {expected} argument(s)"
            )
        for argument in node.args:
            _validate_node(argument, expression)
        return
    if isinstance(node, ast.Name):
        if node.id not in REGISTERED_FITNESS_INPUTS:
            raise ValueError(f"fitness input is not registered: {node.id}")
        return
    if isinstance(node, ast.Constant):
        if isinstance(node.value, bool) or not isinstance(node.value, (int, float)):
            raise ValueError("fitness constants must be numeric")
        _bounded_result(_decimal_constant(node, expression))
        return
    raise ValueError(f"unsupported fitness expression node: {type(node).__name__}")


def _evaluate_node(
    node: ast.AST,
    inputs: Mapping[str, Decimal],
    expression: str,
) -> Decimal:
    if isinstance(node, ast.Expression):
        return _evaluate_node(node.body, inputs, expression)
    if isinstance(node, ast.BinOp):
        left = _evaluate_node(node.left, inputs, expression)
        right = _evaluate_node(node.right, inputs, expression)
        if isinstance(node.op, ast.Add):
            return _bounded_result(left + right)
        if isinstance(node.op, ast.Sub):
            return _bounded_result(left - right)
        if isinstance(node.op, ast.Mult):
            return _bounded_result(left * right)
        if isinstance(node.op, ast.Div):
            return _bounded_result(left / right)
        if isinstance(node.op, ast.Pow):
            return _bounded_result(_power(left, right))
    if isinstance(node, ast.UnaryOp):
        operand = _evaluate_node(node.operand, inputs, expression)
        return _bounded_result(
            operand if isinstance(node.op, ast.UAdd) else -operand
        )
    if isinstance(node, ast.Call):
        assert isinstance(node.func, ast.Name)
        function = _FUNCTIONS[node.func.id]
        return _bounded_result(
            function(
                *(
                    _evaluate_node(argument, inputs, expression)
                    for argument in node.args
                )
            )
        )
    if isinstance(node, ast.Name):
        try:
            return _bounded_result(inputs[node.id])
        except KeyError as exc:
            raise ValueError(f"fitness input is unavailable: {node.id}") from exc
    if isinstance(node, ast.Constant):
        return _bounded_result(_decimal_constant(node, expression))
    raise ValueError("invalid fitness expression")


def _decimal_constant(node: ast.Constant, expression: str) -> Decimal:
    literal = ast.get_source_segment(expression, node)
    if literal is None or _DECIMAL_LITERAL.fullmatch(literal) is None:
        raise ValueError("fitness constants must use decimal literal syntax")
    return Decimal(literal)


def _bounded_result(value: Decimal) -> Decimal:
    if not value.is_finite():
        raise ValueError("fitness expression result must be finite")
    if value and abs(value.adjusted()) > _MAX_RESULT_ADJUSTED_EXPONENT:
        raise ValueError("fitness expression result is too large")
    return value


def _minimum(*values: Decimal) -> Decimal:
    if not values:
        raise ValueError("min requires at least one argument")
    return min(values)


def _maximum(*values: Decimal) -> Decimal:
    if not values:
        raise ValueError("max requires at least one argument")
    return max(values)


def _power(base: Decimal, exponent: Decimal) -> Decimal:
    if abs(exponent) > Decimal("100"):
        raise ValueError("fitness power exponent is too large")
    return base.__pow__(exponent)


def _absolute(value: Decimal) -> Decimal:
    return abs(value)


def _exponential(value: Decimal) -> Decimal:
    if abs(value) > Decimal("100"):
        raise ValueError("fitness exp input is too large")
    return value.exp()


_FUNCTIONS: Mapping[str, Callable[..., Decimal]] = {
    "abs": _absolute,
    "exp": _exponential,
    "ln": lambda value: value.ln(),
    "max": _maximum,
    "min": _minimum,
    "power": _power,
    "sqrt": lambda value: value.sqrt(),
}

_FUNCTION_ARITY: Mapping[str, tuple[int, int | None]] = {
    "abs": (1, 1),
    "exp": (1, 1),
    "ln": (1, 1),
    "max": (1, None),
    "min": (1, None),
    "power": (2, 2),
    "sqrt": (1, 1),
}
