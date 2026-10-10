"""Pure Decimal forward kernel for the source-derived five-state model.

This model is a named encoding of the archived assumptions, not a fitted
market model or a trading policy. Callers provide the normalized observations.
"""

from __future__ import annotations

from collections.abc import Sequence
from decimal import (
    Clamped,
    Context,
    Decimal,
    DivisionByZero,
    FloatOperation,
    InvalidOperation,
    Overflow,
    ROUND_HALF_EVEN,
    localcontext,
)

MODEL_ID = "CHAT_BAYES_FORWARD_DECIMAL_V1"
STATES = ("MR", "UP", "DOWN", "STRESS", "FAIR")
BASKET_SIZE = 14

_ZERO = Decimal("0")
_ONE = Decimal("1")
_HALF = Decimal("0.5")
_TWO = Decimal("2")
_MR_REVERSION = Decimal("5")
_FAIR_MEAN_SCALE = Decimal("0.5")
_JUMP_SHIFT = Decimal("0.22")
_JUMP_EXTRA_SD = Decimal("0.06")
_MAX_JUMP_WEIGHT = Decimal("0.95")

PRIOR = (
    Decimal("0.52"), Decimal("0.10"), Decimal("0.15"),
    Decimal("0.06"), Decimal("0.17"),
)
TRANSITION_RATES = (
    (Decimal("0"), Decimal("0.22"), Decimal("0.28"), Decimal("0.10"), Decimal("0.12")),
    (Decimal("0.20"), Decimal("0"), Decimal("0.10"), Decimal("0.08"), Decimal("0.10")),
    (Decimal("0.20"), Decimal("0.10"), Decimal("0"), Decimal("0.10"), Decimal("0.08")),
    (Decimal("0.50"), Decimal("0.04"), Decimal("0.12"), Decimal("0"), Decimal("0.08")),
    (Decimal("0.12"), Decimal("0.14"), Decimal("0.18"), Decimal("0.07"), Decimal("0")),
)
SIGMA_COMMON = (
    Decimal("0.16"), Decimal("0.12"), Decimal("0.17"), Decimal("0.30"), Decimal("0.16"),
)
SIGMA_IDIOSYNCRATIC = (
    Decimal("0.12"), Decimal("0.11"), Decimal("0.13"), Decimal("0.19"), Decimal("0.12"),
)
DRIFT = (Decimal("0"), Decimal("0.12"), Decimal("-0.14"), Decimal("-0.30"), Decimal("0"))
JUMP_RATE = (Decimal("0"), Decimal("0.004"), Decimal("0.025"), Decimal("0.45"), Decimal("0"))


def _model_context() -> Context:
    context = Context(
        prec=60,
        rounding=ROUND_HALF_EVEN,
        Emin=-999999,
        Emax=999999,
        clamp=0,
    )
    for signal in context.traps:
        context.traps[signal] = signal in (
            InvalidOperation, DivisionByZero, Overflow, FloatOperation,
        )
    context.traps[Clamped] = False
    context.clear_flags()
    return context


with localcontext(_model_context()):
    _DT = Decimal("1") / Decimal("96")


def _transition_matrix() -> tuple[tuple[Decimal, ...], ...]:
    with localcontext(_model_context()):
        scaled = [
            [rate * _DT for rate in row]
            for row in TRANSITION_RATES
        ]
        for index, row in enumerate(scaled):
            row[index] = _ONE - sum(row, _ZERO)
            mass = sum(row, _ZERO)
            scaled[index] = [weight / mass for weight in row]
        return tuple(tuple(row) for row in scaled)


TRANSITION = _transition_matrix()


def initial_probabilities() -> tuple[Decimal, ...]:
    """Return the source prior without applying an initial emission."""
    return PRIOR


def _decimal(value: object, name: str) -> Decimal:
    if type(value) is not Decimal:
        raise TypeError(f"{name} must be Decimal")
    if not value.is_finite():
        raise ValueError(f"{name} must be finite")
    return value


def _normalized(values: Sequence[Decimal], name: str) -> tuple[Decimal, ...]:
    if len(values) != len(STATES):
        raise ValueError(f"{name} must contain five values")
    checked = tuple(_decimal(value, name) for value in values)
    if any(value < _ZERO for value in checked):
        raise ValueError(f"{name} values must be nonnegative")
    total = sum(checked, _ZERO)
    if total <= _ZERO:
        raise ValueError(f"{name} mass must be positive")
    return tuple(value / total for value in checked)


def _state_likelihoods(observation_old: Decimal, observation_new: Decimal) -> tuple[Decimal, ...]:
    change = observation_new - observation_old
    likelihoods = []
    for index, state in enumerate(STATES):
        sigma_common = SIGMA_COMMON[index]
        sigma_idiosyncratic = SIGMA_IDIOSYNCRATIC[index]
        variance = (
            sigma_common * sigma_common
            + sigma_idiosyncratic * sigma_idiosyncratic / Decimal(BASKET_SIZE)
        ) * _DT
        standard_deviation = variance.sqrt()
        if state == "MR":
            mean = -_MR_REVERSION * observation_old * _DT
        elif state == "FAIR":
            mean = -_FAIR_MEAN_SCALE * (
                sigma_common * sigma_common + sigma_idiosyncratic * sigma_idiosyncratic
            ) * _DT
        else:
            mean = DRIFT[index] * _DT

        centered = (change - mean) / standard_deviation
        base_component = (-(centered * centered) * _HALF).exp() / standard_deviation
        jump_weight = min(_MAX_JUMP_WEIGHT, JUMP_RATE[index] * _DT)
        jump_sd = (variance + _JUMP_EXTRA_SD * _JUMP_EXTRA_SD).sqrt()
        jump_centered = (change - mean + _JUMP_SHIFT) / jump_sd
        jump_component = (-(jump_centered * jump_centered) * _HALF).exp() / jump_sd
        likelihoods.append(
            (_ONE - jump_weight) * base_component + jump_weight * jump_component
        )
    return tuple(likelihoods)


def forward_step(
    previous_weights: Sequence[Decimal],
    observation_old: Decimal,
    observation_new: Decimal,
) -> tuple[Decimal, ...]:
    """Advance one fixed 15-minute forward-filter step.

    Observations are caller-supplied normalized-log basket levels. This pure
    kernel does not construct them from candles or retain model state.
    """
    old = _decimal(observation_old, "observation_old")
    new = _decimal(observation_new, "observation_new")
    if len(previous_weights) != len(STATES):
        raise ValueError("previous_weights must contain five values")

    with localcontext(_model_context()):
        weights = _normalized(previous_weights, "previous_weights")
        prediction = tuple(
            sum((weights[row] * TRANSITION[row][column] for row in range(len(STATES))), _ZERO)
            for column in range(len(STATES))
        )
        prediction = _normalized(prediction, "prediction")
        likelihoods = _state_likelihoods(old, new)
        weighted = tuple(prediction[index] * likelihoods[index] for index in range(len(STATES)))
        mass = sum(weighted, _ZERO)
        if mass == _ZERO:
            return prediction
        return tuple(value / mass for value in weighted)
