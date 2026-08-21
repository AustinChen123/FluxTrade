"""Safe, registered fitness expressions for walk-forward evaluation."""

from __future__ import annotations

import ast
from dataclasses import dataclass, field
from decimal import Decimal
import re
from statistics import NormalDist
from types import MappingProxyType
from typing import Callable, Mapping, Sequence, TypedDict


DEFAULT_WALK_FORWARD_FITNESS = (
    "deflated_sharpe + return_worst - drawdown_pct_worst "
    "- return_std - year_concentration"
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


@dataclass(frozen=True, slots=True)
class WalkForwardMetricData:
    """Raw candidate aggregates exposed to registered metric calculators."""

    scores: tuple[Decimal, ...]
    returns: tuple[Decimal, ...]
    drawdown_percentages: tuple[Decimal, ...]
    daily_sharpes: tuple[Decimal, ...]
    annualized_sharpes: tuple[Decimal, ...]
    trade_counts: tuple[int, ...]
    pooled_daily_sharpe: Decimal
    yearly_returns: Mapping[str, Decimal]
    mean_r_values: tuple[Decimal, ...]
    deflated_sharpe: Decimal = Decimal("0")


FitnessMetricCalculator = Callable[[WalkForwardMetricData], Decimal | None]


class _FitnessMetricContract(TypedDict):
    version: str
    metrics: dict[str, dict[str, object]]


@dataclass(frozen=True, slots=True)
class FitnessMetricDefinition:
    """One trusted metric calculator and its persisted semantic contract."""

    name: str
    metric_id: str
    calculator: FitnessMetricCalculator
    source: str
    aggregation: str
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.name.isidentifier():
            raise ValueError("fitness metric name must be a valid identifier")
        if not self.metric_id.strip():
            raise ValueError("fitness metric_id cannot be blank")
        if not self.source.strip() or not self.aggregation.strip():
            raise ValueError("fitness metric source and aggregation cannot be blank")
        object.__setattr__(
            self,
            "metadata",
            MappingProxyType(dict(self.metadata)),
        )

    def contract(self) -> dict[str, object]:
        return {
            "metric_id": self.metric_id,
            "source": self.source,
            "aggregation": self.aggregation,
            **self.metadata,
        }


def calculate_registered_fitness_inputs(
    data: WalkForwardMetricData,
    *,
    definitions: Sequence[FitnessMetricDefinition] | None = None,
) -> dict[str, Decimal]:
    """Calculate inputs from the same registry used for validation and metadata."""
    resolved = _resolve_metric_definitions(definitions)
    inputs: dict[str, Decimal] = {}
    for definition in resolved:
        value = definition.calculator(data)
        if value is not None:
            inputs[definition.name] = _bounded_result(value)
    return inputs


def fitness_metric_contract(
    *,
    definitions: Sequence[FitnessMetricDefinition] | None = None,
) -> _FitnessMetricContract:
    """Return the durable contract for the active trusted metric registry."""
    resolved = _resolve_metric_definitions(definitions)
    return {
        "version": "walk_forward_fitness_v2",
        "metrics": {
            definition.name: definition.contract()
            for definition in resolved
        },
    }


def _resolve_metric_definitions(
    definitions: Sequence[FitnessMetricDefinition] | None,
) -> tuple[FitnessMetricDefinition, ...]:
    resolved = (
        FITNESS_METRIC_DEFINITIONS
        if definitions is None
        else tuple(definitions)
    )
    names = [definition.name for definition in resolved]
    if len(names) != len(set(names)):
        raise ValueError("fitness metric names must be unique")
    return resolved


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


def _mean(values: Sequence[Decimal]) -> Decimal:
    if not values:
        return Decimal("0")
    return sum(values, Decimal("0")) / Decimal(len(values))


def _population_standard_deviation(values: Sequence[Decimal]) -> Decimal:
    if len(values) < 2:
        return Decimal("0")
    mean = _mean(values)
    variance = _mean([(value - mean) ** 2 for value in values])
    return variance.sqrt()


def _positive_year_concentration(
    yearly_returns: Mapping[str, Decimal],
) -> Decimal:
    positive = [value for value in yearly_returns.values() if value > 0]
    total = sum(positive, Decimal("0"))
    if total <= 0:
        return Decimal("1")
    return max(positive) / total


def _worst_fold_mean_r(data: WalkForwardMetricData) -> Decimal | None:
    if len(data.mean_r_values) != len(data.scores):
        return None
    return min(data.mean_r_values)


_DAILY_SHARPE_SOURCE = "utc_daily_mark_to_market_simple_returns_ddof_1_rf_0"
_DAILY_SHARPE_METADATA = {
    "sampling_interval": "utc_calendar_day",
    "periods_per_year": 365,
    "risk_free_rate_annual": "0",
    "variance_estimator": "sample",
    "variance_ddof": 1,
    "missing_day_policy": "carry_prior_equity_zero_return",
    "boundary_policy": "full_scoring_utc_date_range_leading_and_trailing_carry",
    "zero_variance_policy": "zero",
}
FITNESS_METRIC_DEFINITIONS: tuple[FitnessMetricDefinition, ...] = (
    FitnessMetricDefinition(
        "deflated_sharpe",
        "deflated_sharpe_probability_v1",
        lambda data: data.deflated_sharpe,
        _DAILY_SHARPE_SOURCE,
        "all_evaluated_unique_genotypes",
        {
            **_DAILY_SHARPE_METADATA,
            "observations": "pooled_utc_daily_returns",
            "moments": "population_skewness_and_kurtosis_same_returns",
            "benchmark": "all_evaluated_unique_genotypes",
        },
    ),
    FitnessMetricDefinition(
        "annualized_sharpe",
        "annualized_sharpe_utc_daily_v1",
        lambda data: data.pooled_daily_sharpe * Decimal(365).sqrt(),
        _DAILY_SHARPE_SOURCE,
        "pooled_raw_return_moments_sqrt_365",
        _DAILY_SHARPE_METADATA,
    ),
    FitnessMetricDefinition(
        "annualized_sharpe_worst",
        "annualized_sharpe_worst_fold_v1",
        lambda data: min(data.annualized_sharpes),
        _DAILY_SHARPE_SOURCE,
        "minimum_fold",
        _DAILY_SHARPE_METADATA,
    ),
    FitnessMetricDefinition(
        "daily_sharpe",
        "daily_sharpe_utc_v1",
        lambda data: data.pooled_daily_sharpe,
        _DAILY_SHARPE_SOURCE,
        "pooled_raw_return_moments",
        _DAILY_SHARPE_METADATA,
    ),
    FitnessMetricDefinition(
        "daily_sharpe_worst",
        "daily_sharpe_worst_fold_v1",
        lambda data: min(data.daily_sharpes),
        _DAILY_SHARPE_SOURCE,
        "minimum_fold",
        _DAILY_SHARPE_METADATA,
    ),
    FitnessMetricDefinition(
        "drawdown_pct_worst",
        "drawdown_pct_worst_fold_v1",
        lambda data: max(data.drawdown_percentages),
        "fold_mark_to_market_max_drawdown_over_initial_balance",
        "maximum_fold",
    ),
    FitnessMetricDefinition(
        "fold_count",
        "walk_forward_fold_count_v1",
        lambda data: Decimal(len(data.scores)),
        "resolved_scoring_folds",
        "count",
    ),
    FitnessMetricDefinition(
        "return_mean",
        "fold_endpoint_return_mean_v1",
        lambda data: _mean(data.returns),
        "fold_endpoint_mark_to_market_pnl_over_initial_balance",
        "arithmetic_mean",
    ),
    FitnessMetricDefinition(
        "return_std",
        "fold_endpoint_return_population_std_v1",
        lambda data: _population_standard_deviation(data.returns),
        "fold_endpoint_mark_to_market_pnl_over_initial_balance",
        "population_std_ddof_0",
    ),
    FitnessMetricDefinition(
        "return_worst",
        "fold_endpoint_return_worst_v1",
        lambda data: min(data.returns),
        "fold_endpoint_mark_to_market_pnl_over_initial_balance",
        "minimum_fold",
    ),
    FitnessMetricDefinition(
        "score_mean",
        "fold_score_mean_v1",
        lambda data: _mean(data.scores),
        "fold_endpoint_mark_to_market_pnl",
        "arithmetic_mean",
    ),
    FitnessMetricDefinition(
        "score_sum",
        "fold_score_sum_v1",
        lambda data: sum(data.scores, Decimal("0")),
        "fold_endpoint_mark_to_market_pnl",
        "sum",
    ),
    FitnessMetricDefinition(
        "score_worst",
        "fold_score_worst_v1",
        lambda data: min(data.scores),
        "fold_endpoint_mark_to_market_pnl",
        "minimum_fold",
    ),
    FitnessMetricDefinition(
        "trade_count_min",
        "closed_trade_count_min_v1",
        lambda data: Decimal(min(data.trade_counts)),
        "closed_trade_count_including_breakeven",
        "minimum_fold",
    ),
    FitnessMetricDefinition(
        "trade_count_mean",
        "closed_trade_count_mean_v1",
        lambda data: _mean(
            [Decimal(trade_count) for trade_count in data.trade_counts]
        ),
        "closed_trade_count_including_breakeven",
        "arithmetic_mean",
    ),
    FitnessMetricDefinition(
        "trade_count_total",
        "closed_trade_count_total_v1",
        lambda data: Decimal(sum(data.trade_counts)),
        "closed_trade_count_including_breakeven",
        "sum",
    ),
    FitnessMetricDefinition(
        "worst_fold_mean_r",
        "worst_fold_mean_r_v1",
        _worst_fold_mean_r,
        "fold_mean_r",
        "minimum_fold_when_available_for_all_folds",
    ),
    FitnessMetricDefinition(
        "year_concentration",
        "positive_year_return_concentration_v1",
        lambda data: _positive_year_concentration(data.yearly_returns),
        "utc_daily_mark_to_market_returns",
        "largest_positive_year_over_total_positive_years",
        {
            "year_return_aggregation": (
                "compound_fold_returns_within_calendar_year"
            ),
            "formula": (
                "largest_positive_year_return_over_all_positive_year_returns"
            )
        },
    ),
)
REGISTERED_FITNESS_INPUTS = frozenset(
    definition.name for definition in FITNESS_METRIC_DEFINITIONS
)


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
