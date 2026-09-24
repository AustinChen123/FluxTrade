from dataclasses import FrozenInstanceError, replace
from typing import Any, cast
import json
from unittest.mock import MagicMock

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


class Bytes(bytes):
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


def wire(change=lambda data: None):
    data = json.loads(request().canonical_bytes)
    change(data)
    return json.dumps(
        data, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode()


def test_decoder_roundtrip():
    value = request()
    decoded = Request.from_canonical_bytes(value.canonical_bytes)
    assert type(decoded) is Request and decoded == value
    assert decoded.canonical_bytes == value.canonical_bytes
    with pytest.raises(Error):
        Request.from_canonical_bytes(Bytes(value.canonical_bytes))


PATHS = [
    (),
    ("intent",),
    ("intent", "key"),
    ("intent", "requirements"),
    ("intent", "requirements", "profile_requirements", 0),
]


@pytest.mark.parametrize("path", PATHS)
@pytest.mark.parametrize("damage", ["missing", "unknown", "duplicate", "list"])
def test_exact_nested_shapes(path, damage):
    data = json.loads(request().canonical_bytes)
    node = data
    for part in path:
        node = node[part]
    key = next(iter(node))
    if damage == "missing":
        del node[key]
    elif damage == "unknown":
        node["SECRET"] = 1
    elif damage == "list":
        if not path:
            data = []
        else:
            parent = data
            for part in path[:-1]:
                parent = parent[part]
            parent[path[-1]] = []
    raw = json.dumps(data, sort_keys=True, separators=(",", ":")).encode()
    if damage == "duplicate":
        fragment = json.dumps(node, sort_keys=True, separators=(",", ":")).encode()
        duplicated = b"{" + json.dumps(key).encode() + b":null," + fragment[1:]
        raw = raw.replace(fragment, duplicated, 1)
    with pytest.raises(Error, match="^PROFILE_ACTIVATION_REQUEST_INVALID$") as caught:
        Request.from_canonical_bytes(raw)
    assert caught.value.__cause__ is None and "SECRET" not in str(caught.value)


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema_version",), True),
        (("intent", "key", "schema_version"), True),
        (("intent", "key", "contract_version"), True),
        (("intent", "requirements", "profile_requirements", 0, "schema_version"), True),
        (
            ("intent", "requirements", "required_context_capabilities"),
            ["ENTRY_RISK", "ENTRY_RISK"],
        ),
        (("intent", "requirements", "required_context_capabilities"), ["SECRET"]),
        (("intent", "requirements", "required_context_capabilities"), [True]),
        (("command_ref",), "a" * 64),
        (("actor",), "é"),
        (("actor",), "a\x00"),
        (("intent", "expected_state_version"), True),
        (("command",), True),
    ],
)
def test_decoder_domain_mutations(path, value):
    def change(data):
        node = data
        for part in path[:-1]:
            node = node[part]
        node[path[-1]] = value

    with pytest.raises(Error):
        Request.from_canonical_bytes(wire(change))


@pytest.mark.parametrize(
    "raw",
    [
        None,
        True,
        "{}",
        bytearray(b"{}"),
        b"",
        b"\xff",
        b"\xef\xbb\xbf{}",
        b'{"x":1.0}',
        b'{"x":NaN}',
        b'{"x":Infinity}',
        b'{"x":-Infinity}',
        b'{"actor":"\\ud800"}',
    ],
)
def test_decoder_raw_rejections(raw):
    with pytest.raises(Error):
        Request.from_canonical_bytes(raw)


@pytest.mark.parametrize(
    "damage", ["space", "keys", "profiles", "escape", "float", "surrogate"]
)
def test_noncanonical_valid_shape_rejected(damage):
    raw = request().canonical_bytes
    if damage == "space":
        raw += b" "
    elif damage == "keys":
        raw = json.dumps(dict(reversed(list(json.loads(raw).items())))).encode()
    elif damage == "profiles":
        raw = wire(
            lambda d: d["intent"]["requirements"]["profile_requirements"].reverse()
        )
    elif damage == "escape":
        raw = raw.replace(b'"ops"', b'"\\u006fps"')
    elif damage == "float":
        raw = raw.replace(b'"lookback_window":2', b'"lookback_window":2.0')
    else:
        raw = raw.replace(b'"ops"', b'"\\ud800"')
    with pytest.raises(Error):
        Request.from_canonical_bytes(raw)


def test_profile_count_admission_before_constructor(monkeypatch):
    def oversized(data):
        requirements = data["intent"]["requirements"]
        requirements["profile_requirements"] = (
            requirements["profile_requirements"][:1] * 33
        )

    raw = wire(oversized)
    constructor = MagicMock(side_effect=AssertionError("must not construct"))
    monkeypatch.setattr(owner, "ProfileRequirement", constructor)
    with pytest.raises(Error):
        Request.from_canonical_bytes(raw)
    constructor.assert_not_called()


def test_raw_size_gate_before_json_parser(monkeypatch):
    parser = MagicMock(side_effect=ValueError("SECRET"))
    monkeypatch.setattr(owner.json, "loads", parser)
    for size in (65536, 65537):
        parser.reset_mock()
        with pytest.raises(Error) as caught:
            Request.from_canonical_bytes(b" " * size)
        assert parser.call_count == int(size == 65536)
        assert str(caught.value) == "PROFILE_ACTIVATION_REQUEST_INVALID"
        assert caught.value.__cause__ is None


def test_decoder_does_not_swallow_base_exception(monkeypatch):
    error = KeyboardInterrupt()
    monkeypatch.setattr(owner.json, "loads", MagicMock(side_effect=error))
    with pytest.raises(KeyboardInterrupt) as caught:
        Request.from_canonical_bytes(b"{}")
    assert caught.value is error
