from dataclasses import asdict, replace
from itertools import permutations
import json
from types import SimpleNamespace
from typing import cast

import pytest

from src.core.backtest.run_evidence import (
    configuration_sha256,
    strategy_configuration_contract,
)
from src.strategies.base import (
    BaseStrategy,
    StrategyRequirements,
    StrategyContextCapability,
)
from src.core.market_data.profiles.requirements import ProfileRequirement
from test_profile_requirements import VALUE


def contract(requirements):
    strategy = SimpleNamespace(
        strategy_id="test", requirements=requirements, replay_configuration=lambda: {}
    )
    return strategy_configuration_contract((cast(BaseStrategy, strategy),))


@pytest.mark.parametrize(
    "capabilities", [frozenset(), frozenset({StrategyContextCapability.ENTRY_RISK})]
)
def test_legacy_four_fields_and_positional_hash(capabilities):
    requirements = StrategyRequirements(VALUE.product_id, "1h", 3, capabilities)
    expected = {
        "product_id": VALUE.product_id,
        "timeframe": "1h",
        "lookback_window": 3,
        "required_context_capabilities": capabilities,
    }
    actual = contract(requirements)
    assert actual[0]["requirements"] == expected
    assert type(actual[0]["requirements"]) is dict
    assert requirements.required_context_capabilities is capabilities
    assert requirements.profile_requirements == ()
    assert configuration_sha256(actual) == configuration_sha256(
        ({**actual[0], "requirements": expected},)
    )
    assert configuration_sha256(actual) == configuration_sha256(
        contract(replace(requirements, profile_requirements=()))
    )


def test_permutations_full_projection_and_legal_coexistence():
    entries = (
        VALUE,
        replace(VALUE, window_days=30),
        replace(VALUE, output_grid_id="other"),
    )
    expected = tuple(sorted(entries, key=lambda entry: entry.canonical_bytes))
    hashes = set()
    for ordering in permutations(entries):
        requirements = StrategyRequirements(
            VALUE.product_id, "1h", 3, profile_requirements=ordering
        )
        assert requirements.profile_requirements == expected
        projected = contract(requirements)
        assert projected[0]["requirements"] == {
            "product_id": VALUE.product_id,
            "timeframe": "1h",
            "lookback_window": 3,
            "required_context_capabilities": frozenset(),
            "profile_requirements": [
                json.loads(entry.canonical_bytes) for entry in expected
            ],
        }
        hashes.add(configuration_sha256(projected))
    assert len(hashes) == 1


def test_exact_tuple_items_and_duplicates():
    child = type("Child", (ProfileRequirement,), {})(**asdict(VALUE))
    invalid = ([VALUE], type("Tuple", (tuple,), {})((VALUE,)), (child,), (None,))
    for entries in invalid:
        with pytest.raises(TypeError):
            StrategyRequirements(
                VALUE.product_id,
                "1h",
                3,
                profile_requirements=cast(tuple[ProfileRequirement, ...], entries),
            )
    for entries in ((VALUE, VALUE), (VALUE, replace(VALUE))):
        with pytest.raises(ValueError, match="duplicate profile requirements"):
            StrategyRequirements(
                VALUE.product_id, "1h", 3, profile_requirements=entries
            )
    with pytest.raises(TypeError):
        StrategyRequirements(VALUE.product_id, "1h", 3, cast(frozenset, (VALUE,)))


@pytest.mark.parametrize(
    "field,new",
    [
        ("product_id", "BINANCE:ETHUSDT-SPOT"),
        ("base_grid_id", "other_base"),
        ("output_grid_id", "other_output"),
        ("algorithm_version", "v2"),
        ("window_days", 90),
        ("freshness_policy_id", "other_policy"),
    ],
)
def test_each_profile_field_changes_configuration(field, new):
    requirements = StrategyRequirements(
        VALUE.product_id, "1h", 3, profile_requirements=(VALUE,)
    )
    changed = replace(
        requirements, profile_requirements=(replace(VALUE, **{field: new}),)
    )
    assert configuration_sha256(contract(requirements)) != configuration_sha256(
        contract(changed)
    )
