"""Causal numeric contract tests for the pure source-derived Decimal kernel."""

from __future__ import annotations

from decimal import (
    Context,
    Decimal,
    DivisionByZero,
    InvalidOperation,
    Overflow,
    ROUND_HALF_EVEN,
    localcontext,
)

import pytest

from src.core.backtest import spider_bayes_model as model


def _context() -> Context:
    return Context(
        prec=60, rounding=ROUND_HALF_EVEN, Emin=-999999, Emax=999999, clamp=0,
        traps=[InvalidOperation, DivisionByZero, Overflow],
    )


def _reference_transition() -> tuple[tuple[Decimal, ...], ...]:
    with localcontext(_context()):
        dt = Decimal("1") / Decimal("96")
        matrix = []
        for index, rates in enumerate(model.TRANSITION_RATES):
            row = [rate * dt for rate in rates]
            row[index] = Decimal("1") - sum(row, Decimal("0"))
            mass = sum(row, Decimal("0"))
            matrix.append(tuple(value / mass for value in row))
        return tuple(matrix)


def _reference_step(
    weights: tuple[Decimal, ...], observation_old: Decimal, observation_new: Decimal,
) -> tuple[Decimal, ...]:
    with localcontext(_context()):
        dt = Decimal("1") / Decimal("96")
        total = sum(weights, Decimal("0"))
        normalized = tuple(weight / total for weight in weights)
        predicted = tuple(
            sum((normalized[row] * model.TRANSITION[row][column] for row in range(5)), Decimal("0"))
            for column in range(5)
        )
        prediction_total = sum(predicted, Decimal("0"))
        predicted = tuple(value / prediction_total for value in predicted)
        change = observation_new - observation_old
        likelihoods = []
        for index, state in enumerate(model.STATES):
            common = model.SIGMA_COMMON[index]
            idio = model.SIGMA_IDIOSYNCRATIC[index]
            variance = (common * common + idio * idio / Decimal("14")) * dt
            sd = variance.sqrt()
            if state == "MR":
                mean = Decimal("-5") * observation_old * dt
            elif state == "FAIR":
                mean = Decimal("-0.5") * (common * common + idio * idio) * dt
            else:
                mean = model.DRIFT[index] * dt
            z = (change - mean) / sd
            normal = (-(z * z) / Decimal("2")).exp() / sd
            jump_weight = min(Decimal("0.95"), model.JUMP_RATE[index] * dt)
            jump_sd = (variance + Decimal("0.06") ** 2).sqrt()
            jump_z = (change - mean + Decimal("0.22")) / jump_sd
            jump = (-(jump_z * jump_z) / Decimal("2")).exp() / jump_sd
            likelihoods.append((Decimal("1") - jump_weight) * normal + jump_weight * jump)
        weighted = tuple(predicted[index] * likelihoods[index] for index in range(5))
        mass = sum(weighted, Decimal("0"))
        if mass == Decimal("0"):
            return predicted
        return tuple(value / mass for value in weighted)


def test_source_identity_prior_rates_transition_and_source_parameters_are_pinned():
    assert model.MODEL_ID == "CHAT_BAYES_FORWARD_DECIMAL_V1"
    assert model.STATES == ("MR", "UP", "DOWN", "STRESS", "FAIR")
    assert model.BASKET_SIZE == 14
    assert model.PRIOR == tuple(map(Decimal, ("0.52", "0.10", "0.15", "0.06", "0.17")))
    assert model.TRANSITION_RATES == (
        tuple(map(Decimal, ("0", ".22", ".28", ".10", ".12"))),
        tuple(map(Decimal, (".20", "0", ".10", ".08", ".10"))),
        tuple(map(Decimal, (".20", ".10", "0", ".10", ".08"))),
        tuple(map(Decimal, (".50", ".04", ".12", "0", ".08"))),
        tuple(map(Decimal, (".12", ".14", ".18", ".07", "0"))),
    )
    assert model.SIGMA_COMMON == tuple(map(Decimal, (".16", ".12", ".17", ".30", ".16")))
    assert model.SIGMA_IDIOSYNCRATIC == tuple(map(Decimal, (".12", ".11", ".13", ".19", ".12")))
    assert model.DRIFT == tuple(map(Decimal, ("0", ".12", "-.14", "-.30", "0")))
    assert model.JUMP_RATE == tuple(map(Decimal, ("0", ".004", ".025", ".45", "0")))
    assert model.TRANSITION == _reference_transition()
    assert model.initial_probabilities() is model.PRIOR


def test_emission_and_forward_step_match_independent_decimal_expression():
    previous = tuple(map(Decimal, (".40", ".15", ".20", ".05", ".20")))
    old = Decimal("0.0125")
    new = Decimal("0.014")
    observed = model.forward_step(previous, old, new)
    expected = _reference_step(previous, old, new)
    assert observed == expected
    assert type(observed) is tuple and len(observed) == 5
    assert all(type(value) is Decimal and value.is_finite() and value >= 0 for value in observed)


def test_forward_step_is_deterministic_immutable_and_causal():
    previous = [Decimal(".52"), Decimal(".10"), Decimal(".15"), Decimal(".06"), Decimal(".17")]
    original = tuple(previous)
    first = model.forward_step(previous, Decimal("0"), Decimal(".001"))
    repeated = model.forward_step(previous, Decimal("0"), Decimal(".001"))
    assert first == repeated
    assert tuple(previous) == original
    with pytest.raises(TypeError):
        first[0] = Decimal("0")  # type: ignore[index]

    extended = model.forward_step(first, Decimal(".001"), Decimal(".002"))
    changed_future = model.forward_step(first, Decimal(".001"), Decimal("-.4"))
    assert len(extended) == len(changed_future) == 5
    assert first == repeated


def test_model_context_is_independent_of_ambient_precision_rounding_and_traps():
    values = model.initial_probabilities()
    expected = model.forward_step(values, Decimal("0"), Decimal(".003"))
    with localcontext() as ambient:
        ambient.prec = 7
        ambient.rounding = "ROUND_DOWN"
        ambient.traps[InvalidOperation] = False
        ambient.traps[DivisionByZero] = False
        ambient.traps[Overflow] = False
        actual = model.forward_step(values, Decimal("0"), Decimal(".003"))
    assert actual == expected


def test_zero_weighted_likelihood_uses_normalized_prediction_fallback():
    observation_new = Decimal(
        "144.762572736090128282352200948651222316082598258526134670957"
    )
    with localcontext(model._model_context()):
        normalized = model._normalized(model.PRIOR, "test weights")
        predicted = tuple(
            sum((normalized[row] * model.TRANSITION[row][column] for row in range(5)), Decimal("0"))
            for column in range(5)
        )
        predicted = model._normalized(predicted, "test prediction")
        likelihoods = model._state_likelihoods(Decimal("0"), observation_new)
        assert any(value > 0 for value in likelihoods)
        assert sum((predicted[i] * likelihoods[i] for i in range(5)), Decimal("0")) == 0

    assert model.forward_step(model.PRIOR, Decimal("0"), observation_new) == predicted
    assert model.forward_step(model.PRIOR, Decimal("0"), Decimal("1000")) == predicted


@pytest.mark.parametrize(
    ("weights", "old", "new", "error"),
    [
        ((Decimal("1"),) * 4, Decimal("0"), Decimal("0"), ValueError),
        ((Decimal("1"),) * 6, Decimal("0"), Decimal("0"), ValueError),
        ((Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("-1")), Decimal("0"), Decimal("0"), ValueError),
        ((Decimal("0"),) * 5, Decimal("0"), Decimal("0"), ValueError),
        ((Decimal("NaN"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1")), Decimal("0"), Decimal("0"), ValueError),
        ((Decimal("Infinity"), Decimal("1"), Decimal("1"), Decimal("1"), Decimal("1")), Decimal("0"), Decimal("0"), ValueError),
        ((Decimal("1"), Decimal("1"), Decimal("1"), 1.0, Decimal("1")), Decimal("0"), Decimal("0"), TypeError),
        ((Decimal("1"), Decimal("1"), Decimal("1"), True, Decimal("1")), Decimal("0"), Decimal("0"), TypeError),
        ((Decimal("1"),) * 5, 0.0, Decimal("0"), TypeError),
        ((Decimal("1"),) * 5, Decimal("0"), True, TypeError),
        ((Decimal("1"),) * 5, Decimal("NaN"), Decimal("0"), ValueError),
        ((Decimal("1"),) * 5, Decimal("0"), Decimal("Infinity"), ValueError),
    ],
)
def test_invalid_observation_or_probability_input_is_rejected(weights, old, new, error):
    with pytest.raises(error):
        model.forward_step(weights, old, new)
