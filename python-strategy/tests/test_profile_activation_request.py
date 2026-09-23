from dataclasses import FrozenInstanceError, replace
from typing import Any, cast

import pytest

from src.core import strategy_activation_intent as owner
from src.core.strategy_activation_intent import (
    ProfileActivationRequest as Request,
    ProfileActivationCommand as Command,
    ProfileActivationRequestError as Error,
)
from src.strategies.base import StrategyContextCapability
from test_profile_activation_intent import intent


def request(count=2):
    value = intent()
    profile = replace(
        value.requirements.profile_requirements[0],
        base_grid_id="g",
        output_grid_id="g",
        algorithm_version="v1",
    )
    req = replace(
        value.requirements,
        required_context_capabilities=frozenset({StrategyContextCapability.ENTRY_RISK}),
        profile_requirements=tuple(
            replace(profile, window_days=i + 1) for i in range(count)
        ),
    )
    return Request("ops", "secret-key", Command.START, replace(value, requirements=req))


def test_literal_complete_golden():
    value = request()
    assert value.canonical_bytes == (
        b'{"actor":"ops","command":"START","command_ref":"65df24f19fcefc10773496948ddf2f6d2be461b95f13d50f4d21720432992b3d",'
        b'"idempotency_key":"secret-key","intent":{"expected_state_version":0,"key":{"config_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"contract_version":1,"environment":"live","execution_scope_id":"deployment","product_id":"BINANCE:BTCUSDT-SPOT",'
        b'"schema_version":1,"strategy_id":"strategy","strategy_version":"v1","timeframe":"1m"},'
        b'"requirements":{"lookback_window":2,"product_id":"BINANCE:BTCUSDT-SPOT","profile_requirements":['
        b'{"algorithm_version":"v1","base_grid_id":"g","freshness_policy_id":"utc_complete_strict_v1","output_grid_id":"g",'
        b'"product_id":"BINANCE:BTCUSDT-SPOT","schema_version":1,"window_days":1},'
        b'{"algorithm_version":"v1","base_grid_id":"g","freshness_policy_id":"utc_complete_strict_v1","output_grid_id":"g",'
        b'"product_id":"BINANCE:BTCUSDT-SPOT","schema_version":1,"window_days":2}],'
        b'"required_context_capabilities":["ENTRY_RISK"],"timeframe":"1m"}},"schema_version":1}'
    )
    assert (
        value.command_ref
        == value.request_id
        == "65df24f19fcefc10773496948ddf2f6d2be461b95f13d50f4d21720432992b3d"
    )
    assert (
        value.payload_digest
        == "24ad7e36e566c34304188764bf9fb373fc01d974e0dabd25a291ac6f6ef06567"
    )


@pytest.mark.parametrize(
    "field", ["environment", "execution_scope_id", "actor", "idempotency_key"]
)
def test_command_reference_scope(field):
    value = request()
    changed = (
        replace(value, **{field: "other"})
        if field in ("actor", "idempotency_key")
        else replace(
            value,
            intent=replace(
                value.intent, key=replace(value.intent.key, **{field: "other"})
            ),
        )
    )
    assert changed.command_ref != value.command_ref
    assert changed.payload_digest != value.payload_digest


@pytest.mark.parametrize(
    "field",
    [
        "command",
        "strategy_id",
        "strategy_version",
        "config_hash",
        "requirements",
        "state",
    ],
)
def test_payload_drift_does_not_change_command_reference(field):
    value = request()
    if field == "command":
        changed = replace(value, command=Command.RESUME)
    elif field == "requirements":
        changed = replace(
            value,
            intent=replace(
                value.intent,
                requirements=replace(value.intent.requirements, lookback_window=3),
            ),
        )
    elif field == "state":
        changed = replace(value, intent=replace(value.intent, expected_state_version=1))
    else:
        changed = replace(
            value,
            intent=replace(
                value.intent,
                key=replace(
                    value.intent.key,
                    **{field: "b" * 64 if field == "config_hash" else "other"},
                ),
            ),
        )
    assert changed.command_ref == value.command_ref
    assert changed.payload_digest != value.payload_digest


class Text(str):
    pass


@pytest.mark.parametrize("field,limit", [("actor", 64), ("idempotency_key", 128)])
def test_text_domain_and_bounds(field, limit):
    value = request()
    assert replace(value, **{field: "a" * limit})
    for invalid in (
        None,
        True,
        Text("ops"),
        "",
        "a" * (limit + 1),
        " ops",
        "ops ",
        "é",
        "a\x00",
        "_ops",
        "a/b",
    ):
        with pytest.raises(
            Error, match="^PROFILE_ACTIVATION_REQUEST_INVALID$"
        ) as caught:
            replace(value, **{field: invalid})
        assert caught.value.__cause__ is None


def test_exact_command_intent_frozen_hidden_key():
    value = request()

    class Subclass(owner.ProfileActivationIntent):
        pass

    for command in (None, True, "START", owner.ProfileActivationAdmission.ADMIT):
        with pytest.raises(Error):
            replace(value, command=cast(Any, command))
    for invalid in (
        None,
        True,
        Subclass(value.intent.key, value.intent.requirements, 0),
    ):
        with pytest.raises(Error):
            replace(value, intent=cast(Any, invalid))
    with pytest.raises(FrozenInstanceError):
        cast(Any, value).actor = "new"
    assert "secret-key" not in repr(value) and not hasattr(value, "__dict__")


def test_collection_permutation_and_admission_caps(monkeypatch):
    value = request()
    requirements = replace(
        value.intent.requirements,
        profile_requirements=value.intent.requirements.profile_requirements[::-1],
    )
    reordered = replace(value, intent=replace(value.intent, requirements=requirements))
    assert reordered.canonical_bytes == value.canonical_bytes
    assert reordered.payload_digest == value.payload_digest
    assert request(1) and request(32)
    with pytest.raises(Error):
        request(33)
    assert owner.MAX_ACTIVATION_REQUEST_BYTES == 65536
    monkeypatch.setattr(
        owner, "MAX_ACTIVATION_REQUEST_BYTES", len(value.canonical_bytes)
    )
    assert replace(value) == value
    monkeypatch.setattr(
        owner, "MAX_ACTIVATION_REQUEST_BYTES", len(value.canonical_bytes) - 1
    )
    with pytest.raises(Error):
        replace(value)
