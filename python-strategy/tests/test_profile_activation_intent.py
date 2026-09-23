from dataclasses import FrozenInstanceError, replace
from typing import Any, cast
import ast
from pathlib import Path

import pytest

from src.core.strategy_activation_intent import (
    ProfileActivationAdmission as Admission,
    ProfileActivationIntent as Intent,
    ProfileActivationIntentError,
    classify_profile_activation_intent as classify,
)
from src.core.market_data.profiles.bootstrap_seed import BootstrapKey
from src.core.market_data.profiles.requirements import ProfileRequirement
from src.strategies.base import StrategyRequirements


def intent():
    product = "BINANCE:BTCUSDT-SPOT"
    key = BootstrapKey("live", "deployment", "strategy", "v1", "a" * 64, product, "1m")
    profile = ProfileRequirement(
        product,
        "btc_spot_usdt_10_v1",
        "btc_spot_usdt_10_v1",
        "vp-v1",
        1,
        "utc_complete_strict_v1",
    )
    requirements = StrategyRequirements(
        product, "1m", 2, profile_requirements=(profile,)
    )
    return Intent(key, requirements, 0)


def test_service_reexports_exact_contract_objects():
    from src.core import strategy_activation_intent as pure
    from src.core import strategy_activation_service as service

    for name in (
        "ProfileActivationIntentError",
        "ProfileActivationAdmission",
        "ProfileActivationIntent",
        "classify_profile_activation_intent",
    ):
        assert getattr(service, name) is getattr(pure, name)


def test_pure_contract_import_boundary():
    from src.core import strategy_activation_intent as pure

    tree = ast.parse(Path(pure.__file__).read_text())
    allowed = {
        "dataclasses.dataclass",
        "enum.Enum",
        "src.core.market_data.profiles.bootstrap_seed.BootstrapKey",
        "src.strategies.base.StrategyRequirements",
    }
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            pytest.fail("pure intent must use only its explicit domain imports")
        if isinstance(node, ast.ImportFrom):
            assert node.level == 0
            assert {f"{node.module}.{name.name}" for name in node.names} <= allowed


@pytest.mark.parametrize("drift", ["version", "config", "requirements", "state"])
def test_all_admissions_and_stale_precedence(drift):
    requested = intent()
    changes = {
        "version": dict(key=replace(requested.key, strategy_version="v2")),
        "config": dict(key=replace(requested.key, config_hash="f" * 64)),
        "requirements": dict(
            requirements=replace(requested.requirements, lookback_window=3)
        ),
        "state": dict(expected_state_version=1),
    }
    different = replace(requested, **changes[drift])
    assert classify(requested, current=requested, pending=None) is Admission.ADMIT
    assert (
        classify(requested, current=requested, pending=replace(requested))
        is Admission.IDEMPOTENT
    )
    assert (
        classify(requested, current=requested, pending=different) is Admission.CONFLICT
    )
    for pending in (None, requested, different):
        assert (
            classify(requested, current=different, pending=pending) is Admission.STALE
        )


@pytest.mark.parametrize("field", ["environment", "execution_scope_id", "strategy_id"])
@pytest.mark.parametrize("position", ["current", "pending"])
def test_cross_target_never_classified_as_stale(field, position):
    value = intent()
    args = dict(current=value, pending=value)
    args[position] = replace(value, key=replace(value.key, **{field: "other"}))
    with pytest.raises(
        ProfileActivationIntentError, match="^PROFILE_ACTIVATION_INTENT_INVALID$"
    ):
        classify(value, **args)


class Integer(int):
    pass


class KeySubclass(BootstrapKey):
    pass


class RequirementsSubclass(StrategyRequirements):
    pass


@pytest.mark.parametrize(
    "field,value",
    [
        ("expected_state_version", True),
        ("expected_state_version", Integer(0)),
        ("expected_state_version", -1),
        ("expected_state_version", 1 << 31),
        ("key", None),
        ("requirements", None),
    ],
)
def test_hostile_direct_fields(field, value):
    with pytest.raises(ProfileActivationIntentError):
        replace(intent(), **{field: value})


@pytest.mark.parametrize("lookback", [True, Integer(0), -1, 1 << 63])
def test_lookback_domain(lookback):
    value = intent()
    with pytest.raises(ProfileActivationIntentError):
        replace(
            value, requirements=replace(value.requirements, lookback_window=lookback)
        )


@pytest.mark.parametrize(
    "damage", ["key_subclass", "requirements_subclass", "empty", "product", "timeframe"]
)
def test_nested_exactness_and_matching(damage):
    value = intent()
    if damage == "key_subclass":
        key = value.key
        changes = dict(
            key=KeySubclass(
                key.environment,
                key.execution_scope_id,
                key.strategy_id,
                key.strategy_version,
                key.config_hash,
                key.product_id,
                key.timeframe,
            )
        )
    else:
        req = value.requirements
        requirements = (
            RequirementsSubclass(
                req.product_id,
                req.timeframe,
                req.lookback_window,
                profile_requirements=req.profile_requirements,
            )
            if damage == "requirements_subclass"
            else replace(
                req,
                **{
                    "empty": {"profile_requirements": ()},
                    "product": {"product_id": "BINANCE:ETHUSDT-SPOT"},
                    "timeframe": {"timeframe": "5m"},
                }[damage],
            )
        )
        changes = dict(requirements=requirements)
    with pytest.raises(ProfileActivationIntentError):
        replace(value, **changes)


@pytest.mark.parametrize("position", ["requested", "current", "pending"])
def test_classifier_exact_arguments(position):
    class Subclass(Intent):
        pass

    value = intent()
    for invalid in (True, object(), Subclass(value.key, value.requirements, 0)):
        args: dict[str, Any] = dict(requested=value, current=value, pending=None)
        args[position] = invalid
        with pytest.raises(ProfileActivationIntentError):
            classify(**args)


def test_normalized_equality_bounds_and_immutability():
    value = intent()
    first = value.requirements.profile_requirements[0]
    second = replace(first, window_days=2)
    a = replace(
        value,
        requirements=replace(value.requirements, profile_requirements=(first, second)),
    )
    b = replace(
        a, requirements=replace(a.requirements, profile_requirements=(second, first))
    )
    assert a == b and classify(a, current=b, pending=b) is Admission.IDEMPOTENT
    for lookback in (0, (1 << 63) - 1):
        assert replace(
            value,
            expected_state_version=(1 << 31) - 1,
            requirements=replace(value.requirements, lookback_window=lookback),
        )
    with pytest.raises(FrozenInstanceError):
        cast(Any, value).expected_state_version = 1
    assert not hasattr(value, "__dict__")
    assert value == intent()
