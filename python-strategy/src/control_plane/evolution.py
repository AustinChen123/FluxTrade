from __future__ import annotations

from decimal import Decimal, ROUND_CEILING, ROUND_HALF_EVEN
from hashlib import sha256
from random import Random
from typing import Any

from src.control_plane.models import (
    EvolutionConfig,
    ParameterCandidate,
    ParameterEvaluationResult,
    ParameterSearchSpace,
)
from src.control_plane.search_space import (
    generate_parameter_candidates,
    parameter_dimension_values,
)


_RANDOM_SCALE = 10**18
_SBX_ETA = Decimal("2")


def initial_population(
    search_space: ParameterSearchSpace,
    config: EvolutionConfig,
    *,
    seed: int,
) -> list[ParameterCandidate]:
    candidates = generate_parameter_candidates(
        search_space,
        sample_count=config.population_size,
        seed=seed,
    )
    return _with_generation_ids(candidates, generation_index=0)


def next_population(
    search_space: ParameterSearchSpace,
    config: EvolutionConfig,
    population: list[ParameterCandidate],
    evaluations: list[ParameterEvaluationResult],
    *,
    objective: str,
    seed: int,
    generation_index: int,
) -> list[ParameterCandidate]:
    if len(population) != config.population_size:
        raise ValueError("population size does not match evolution config")
    by_candidate_id = {result.candidate_id: result for result in evaluations}
    if set(by_candidate_id) != {candidate.candidate_id for candidate in population}:
        raise ValueError("population evaluations are incomplete")

    ranked = sorted(
        population,
        key=lambda candidate: _fitness_key(
            by_candidate_id[candidate.candidate_id],
            objective,
        ),
        reverse=True,
    )
    next_packs = [
        dict(candidate.param_pack) for candidate in ranked[: config.elite_count]
    ]
    rng = Random(_generation_seed(seed, generation_index))
    while len(next_packs) < config.population_size:
        first = _select_parent(
            population,
            by_candidate_id,
            config.tournament_size,
            objective,
            rng,
        )
        second = _select_parent(
            population,
            by_candidate_id,
            config.tournament_size,
            objective,
            rng,
        )
        child_packs = _crossover(
            first.param_pack,
            second.param_pack,
            search_space,
            config,
            rng,
        )
        for child_pack in child_packs:
            next_packs.append(
                _mutate(child_pack, search_space, config, rng)
            )
            if len(next_packs) == config.population_size:
                break

    return [
        ParameterCandidate(
            candidate_id=_candidate_id(generation_index, index),
            param_pack=param_pack,
        )
        for index, param_pack in enumerate(next_packs)
    ]


def canonical_param_key(param_pack: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    return tuple(
        (name, _canonical_value(param_pack[name]))
        for name in sorted(param_pack)
    )


def _select_parent(
    population: list[ParameterCandidate],
    evaluations: dict[str, ParameterEvaluationResult],
    tournament_size: int,
    objective: str,
    rng: Random,
) -> ParameterCandidate:
    contestants = rng.sample(population, tournament_size)
    return max(
        contestants,
        key=lambda candidate: _fitness_key(
            evaluations[candidate.candidate_id],
            objective,
        ),
    )


def _fitness_key(
    result: ParameterEvaluationResult,
    objective: str,
) -> tuple[Decimal, Decimal]:
    if objective in {"maximize_score", "maximize_return"}:
        return result.score_total, -abs(result.max_drawdown)
    if objective == "minimize_drawdown":
        return -abs(result.max_drawdown), result.score_total
    raise ValueError(f"unsupported objective: {objective}")


def _crossover(
    first: dict[str, Any],
    second: dict[str, Any],
    search_space: ParameterSearchSpace,
    config: EvolutionConfig,
    rng: Random,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if _random_decimal(rng) >= config.crossover_probability:
        return dict(first), dict(second)

    first_child = {}
    second_child = {}
    for name, dimension in search_space.parameters.items():
        values = parameter_dimension_values(dimension)
        first_index = _value_index(values, first[name])
        second_index = _value_index(values, second[name])
        if dimension.type == "categorical":
            if rng.randrange(2):
                first_index, second_index = second_index, first_index
            child_indexes = first_index, second_index
        else:
            child_indexes = _sbx_indexes(
                first_index,
                second_index,
                len(values),
                rng,
            )
        first_child[name] = values[child_indexes[0]]
        second_child[name] = values[child_indexes[1]]
    return first_child, second_child


def _sbx_indexes(
    first: int,
    second: int,
    value_count: int,
    rng: Random,
) -> tuple[int, int]:
    if first == second:
        return first, second
    random_value = _random_decimal(rng)
    exponent = Decimal("1") / (_SBX_ETA + Decimal("1"))
    if random_value <= Decimal("0.5"):
        beta = (Decimal("2") * random_value) ** exponent
    else:
        beta = (
            Decimal("1") / (Decimal("2") * (Decimal("1") - random_value))
        ) ** exponent
    first_decimal = Decimal(first)
    second_decimal = Decimal(second)
    midpoint = (first_decimal + second_decimal) / Decimal("2")
    half_distance = abs(first_decimal - second_decimal) / Decimal("2")
    low = _bounded_index(midpoint - beta * half_distance, value_count)
    high = _bounded_index(midpoint + beta * half_distance, value_count)
    return (low, high) if first <= second else (high, low)


def _mutate(
    param_pack: dict[str, Any],
    search_space: ParameterSearchSpace,
    config: EvolutionConfig,
    rng: Random,
) -> dict[str, Any]:
    mutated = dict(param_pack)
    for name, dimension in search_space.parameters.items():
        if _random_decimal(rng) >= config.mutation_probability:
            continue
        values = parameter_dimension_values(dimension)
        current_index = _value_index(values, mutated[name])
        if dimension.type == "categorical":
            alternatives = [
                index for index in range(len(values)) if index != current_index
            ]
            if alternatives:
                mutated[name] = values[rng.choice(alternatives)]
            continue
        offset = _discrete_gaussian_offset(
            config.mutation_sigma_steps,
            len(values),
            rng,
        )
        mutated[name] = values[
            min(max(current_index + offset, 0), len(values) - 1)
        ]
    return mutated


def _discrete_gaussian_offset(
    sigma_steps: Decimal,
    value_count: int,
    rng: Random,
) -> int:
    radius = int(
        min(
            Decimal(value_count - 1),
            sigma_steps * Decimal("6"),
        ).to_integral_value(rounding=ROUND_CEILING)
    )
    offsets = list(range(-radius, radius + 1))
    denominator = Decimal("2") * sigma_steps * sigma_steps
    weights = [
        (-(Decimal(offset * offset) / denominator)).exp()
        for offset in offsets
    ]
    target = _random_decimal(rng) * sum(weights, Decimal("0"))
    cumulative = Decimal("0")
    for offset, weight in zip(offsets, weights):
        cumulative += weight
        if target <= cumulative:
            return offset
    return offsets[-1]


def _with_generation_ids(
    candidates: list[ParameterCandidate],
    *,
    generation_index: int,
) -> list[ParameterCandidate]:
    return [
        candidate.model_copy(
            update={"candidate_id": _candidate_id(generation_index, index)}
        )
        for index, candidate in enumerate(candidates)
    ]


def _candidate_id(generation_index: int, index: int) -> str:
    return f"g{generation_index:06d}_c{index:06d}"


def _generation_seed(seed: int, generation_index: int) -> int:
    digest = sha256(f"{seed}:{generation_index}".encode()).digest()
    return int.from_bytes(digest[:16], "big")


def _random_decimal(rng: Random) -> Decimal:
    return Decimal(rng.randrange(_RANDOM_SCALE)) / Decimal(_RANDOM_SCALE)


def _bounded_index(value: Decimal, value_count: int) -> int:
    rounded = int(value.to_integral_value(rounding=ROUND_HALF_EVEN))
    return min(max(rounded, 0), value_count - 1)


def _value_index(values: list[Any], target: Any) -> int:
    for index, value in enumerate(values):
        if type(value) is type(target) and value == target:
            return index
    raise ValueError(f"parameter value is outside its registered domain: {target!r}")


def _canonical_value(value: Any) -> str:
    if isinstance(value, Decimal):
        rendered = format(value.normalize(), "f")
    else:
        rendered = repr(value)
    return f"{type(value).__name__}:{rendered}"
