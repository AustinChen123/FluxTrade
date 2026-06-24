from __future__ import annotations

from decimal import Decimal
from itertools import product
from math import prod
from random import Random
from typing import Any

from src.control_plane.models import (
    ParameterCandidate,
    ParameterSearchDimension,
    ParameterSearchJobRequest,
    ParameterSearchSpace,
)

_MAX_DIMENSION_VALUES = 100_000


def resolve_parameter_candidates(
    request: ParameterSearchJobRequest,
) -> list[ParameterCandidate]:
    """Return explicit candidates or deterministic generated candidates."""

    if request.candidates is not None:
        return request.candidates
    assert request.search_space is not None
    assert request.candidate_sample_count is not None
    return generate_parameter_candidates(
        request.search_space,
        sample_count=request.candidate_sample_count,
        seed=request.seed,
    )


def generate_parameter_candidates(
    search_space: ParameterSearchSpace,
    *,
    sample_count: int,
    seed: int | None = None,
) -> list[ParameterCandidate]:
    """Generate deterministic candidate packs from a finite search space."""

    if sample_count < 1:
        raise ValueError("sample_count must be positive")

    parameter_names = list(search_space.parameters)
    value_lists = [
        _dimension_values(search_space.parameters[name])
        for name in parameter_names
    ]
    total_combinations = prod(len(values) for values in value_lists)
    if sample_count > total_combinations:
        raise ValueError("sample_count exceeds available search-space combinations")

    if sample_count == total_combinations:
        combinations = product(*value_lists)
    else:
        rng = Random(seed)
        selected_indexes: set[tuple[int, ...]] = set()
        while len(selected_indexes) < sample_count:
            selected_indexes.add(
                tuple(rng.randrange(len(values)) for values in value_lists)
            )
        combinations = (
            tuple(values[index] for values, index in zip(value_lists, indexes))
            for indexes in sorted(selected_indexes)
        )

    return [
        ParameterCandidate(
            candidate_id=f"generated_{index:06d}",
            param_pack=dict(zip(parameter_names, combination)),
        )
        for index, combination in enumerate(combinations, start=1)
    ]


def _dimension_values(dimension: ParameterSearchDimension) -> list[Any]:
    if dimension.type == "categorical":
        assert dimension.choices is not None
        return list(dimension.choices)
    if dimension.type == "integer":
        return _integer_values(dimension)
    return _decimal_values(dimension)


def _integer_values(dimension: ParameterSearchDimension) -> list[int]:
    assert dimension.min is not None
    assert dimension.max is not None
    assert dimension.step is not None
    min_value = int(Decimal(str(dimension.min)))
    max_value = int(Decimal(str(dimension.max)))
    step_value = int(Decimal(str(dimension.step)))
    count = ((max_value - min_value) // step_value) + 1
    if count > _MAX_DIMENSION_VALUES:
        raise ValueError("integer dimension expands to too many values")
    return [min_value + (step_value * index) for index in range(count)]


def _decimal_values(dimension: ParameterSearchDimension) -> list[Decimal]:
    assert dimension.min is not None
    assert dimension.max is not None
    assert dimension.step is not None
    min_value = Decimal(str(dimension.min))
    max_value = Decimal(str(dimension.max))
    step_value = Decimal(str(dimension.step))

    values = []
    current = min_value
    while current <= max_value:
        values.append(current)
        if len(values) > _MAX_DIMENSION_VALUES:
            raise ValueError("decimal dimension expands to too many values")
        current += step_value
    return values
