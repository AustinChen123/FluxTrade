from dataclasses import FrozenInstanceError, fields, replace
import hashlib
import json
from itertools import permutations
from typing import Any, cast

import pytest
from unittest.mock import Mock
from src.core.market_data.profiles import decision_application as owner

from src.core.market_data.profiles.decision_application import (
    DecisionApplicationError,
    MarketDataDecisionKey as Key,
    MarketDataDecisionOutcome as Outcome,
    MarketDataDecisionBatch as Batch,
)


def key():
    return Key.for_candle(
        environment="live",
        execution_scope_id="deployment",
        strategy_id="s",
        strategy_version="v1",
        config_hash="a" * 64,
        product_id="BINANCE:BTCUSDT-SPOT",
        timeframe="1m",
        bar_start_ms=0,
    )


def applied(value=None):
    value = value or key()
    return Outcome(value, "APPLIED", value.input_id, "b" * 64)


def batch(keys=(), outcomes=()):
    return Batch("live", "deployment", "BINANCE:BTCUSDT-SPOT", "1m", 0, keys, outcomes)


def test_batch_empty_literal_and_full_roundtrip():
    expected = b'{"bar_start_ms":0,"environment":"live","execution_scope_id":"deployment","outcomes":[],"participants":[],"product_id":"BINANCE:BTCUSDT-SPOT","schema_version":1,"timeframe":"1m"}'
    assert batch().canonical_bytes == expected
    assert Batch.from_canonical_bytes(expected) == batch()
    outcomes = [
        applied(),
        replace(
            applied(), signal_suppressed=True, suppression_reason="SNAPSHOT_REVOKED"
        ),
    ]
    for reason in ("INPUT_COMMIT_UNCONFIRMED", "INPUT_STORE_FAILED"):
        outcomes.extend(
            [
                Outcome(key(), "SKIPPED", None, None, reason),
                Outcome(key(), "SKIPPED", key().input_id, "b" * 64, reason),
            ]
        )
    for outcome in outcomes:
        value = batch((key(),), (outcome,))
        assert Batch.from_canonical_bytes(value.canonical_bytes) == value


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema_version",), True),
        (("schema_version",), 2),
        (("bar_start_ms",), True),
        (("environment",), "SECRET!"),
        (("participants", 0, "strategy_id"), "other"),
        (("outcomes", 0, "disposition"), "OTHER"),
        (("outcomes", 0, "input_digest"), "SECRET"),
        (("outcomes", 0, "signal_suppressed"), 0),
        (("outcomes", 0, "contract_version"), True),
        (("outcomes", 0, "key", "execution_scope_id"), "other"),
    ],
)
def test_batch_codec_hostile_fields(path, value):
    data = json.loads(batch((key(),), (applied(),)).canonical_bytes)
    target = data
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    with pytest.raises(DecisionApplicationError) as caught:
        Batch.from_canonical_bytes(owner._bytes(data))
    assert (
        str(caught.value) == "MARKET_DATA_DECISION_INVALID"
        and caught.value.__cause__ is None
    )


def test_batch_codec_nested_shapes_and_json_domain():
    raw = batch((key(),), (applied(),)).canonical_bytes
    for path in ((), ("participants", 0), ("outcomes", 0), ("outcomes", 0, "key")):
        for extra in (False, True):
            data = json.loads(raw)
            target = data
            for part in path:
                target = target[part]
            if extra:
                target["SECRET"] = 1
            else:
                target.pop(next(iter(target)))
            with pytest.raises(DecisionApplicationError):
                Batch.from_canonical_bytes(owner._bytes(data))
    for invalid in (
        b"\xff",
        raw + b" ",
        raw.replace(b'"strategy_id":"s"', b'"strategy_id":"s","strategy_id":"s"'),
        raw.replace(b'"bar_start_ms":0', b'"bar_start_ms":0.0'),
        raw.replace(b'"bar_start_ms":0', b'"bar_start_ms":NaN'),
        raw.replace(b'"bar_start_ms":0', b'"bar_start_ms":Infinity'),
    ):
        with pytest.raises(DecisionApplicationError):
            Batch.from_canonical_bytes(invalid)


def test_batch_count_admission_constructor_and_decoder(monkeypatch):
    assert owner.MAX_DECISION_BATCH_PARTICIPANTS == 256
    keys = tuple(replace(key(), strategy_id=f"s{i}") for i in range(3))
    value = batch(keys, tuple(applied(k) for k in keys))
    raw = value.canonical_bytes
    monkeypatch.setattr(owner, "MAX_DECISION_BATCH_PARTICIPANTS", 2)
    control = batch(keys[:2], tuple(applied(k) for k in keys[:2]))
    assert Batch.from_canonical_bytes(control.canonical_bytes) == control
    with pytest.raises(DecisionApplicationError):
        batch(keys, tuple(applied(k) for k in keys))
    original_shape = owner._shape
    spy = Mock(side_effect=AssertionError("typed reconstruction"))

    def shape(value, cls):
        if cls in (Key, Outcome):
            spy()
        return original_shape(value, cls)

    monkeypatch.setattr(owner, "_shape", shape)
    with pytest.raises(DecisionApplicationError):
        Batch.from_canonical_bytes(raw)
    spy.assert_not_called()


def test_batch_bytes_depth_nodes_admission(monkeypatch):
    assert (
        owner.MAX_DECISION_BATCH_BYTES,
        owner.MAX_DECISION_BATCH_DEPTH,
        owner.MAX_DECISION_BATCH_NODES,
    ) == (1048576, 16, 65536)
    raw = batch((key(),), (applied(),)).canonical_bytes
    assert Batch.from_canonical_bytes(raw)
    with monkeypatch.context() as patch:
        patch.setattr(owner, "MAX_DECISION_BATCH_BYTES", len(raw))
        assert Batch.from_canonical_bytes(raw)
        assert batch((key(),), (applied(),)).canonical_bytes == raw
        patch.setattr(owner, "MAX_DECISION_BATCH_BYTES", len(raw) - 1)
        with pytest.raises(DecisionApplicationError):
            batch((key(),), (applied(),))
        spy = Mock(side_effect=AssertionError("JSON parser"))
        patch.setattr(owner.json, "loads", spy)
        with pytest.raises(DecisionApplicationError):
            Batch.from_canonical_bytes(raw)
        spy.assert_not_called()
    data = json.loads(raw)
    nested = "SECRET"
    for _ in range(17):
        nested = [nested]
    data["outcomes"][0]["reason"] = nested
    spy = Mock(side_effect=AssertionError("typed reconstruction"))
    monkeypatch.setattr(owner, "_shape", spy)
    with pytest.raises(DecisionApplicationError):
        Batch.from_canonical_bytes(owner._bytes(data))
    spy.assert_not_called()


def test_batch_actual_byte_boundary_precedes_parser(monkeypatch):
    spy = Mock(side_effect=ValueError)
    monkeypatch.setattr(owner.json, "loads", spy)
    with pytest.raises(DecisionApplicationError):
        Batch.from_canonical_bytes(b" " * owner.MAX_DECISION_BATCH_BYTES)
    spy.assert_called_once()
    spy.reset_mock()
    with pytest.raises(DecisionApplicationError):
        Batch.from_canonical_bytes(b" " * (owner.MAX_DECISION_BATCH_BYTES + 1))
    spy.assert_not_called()


def test_batch_exact_node_boundary_is_symmetric(monkeypatch):
    raw = batch((key(),), (applied(),)).canonical_bytes

    def nodes(value):
        if type(value) is dict:
            return 1 + sum(1 + nodes(v) for v in value.values())
        if type(value) is list:
            return 1 + sum(nodes(v) for v in value)
        return 1

    count = nodes(json.loads(raw))
    monkeypatch.setattr(owner, "MAX_DECISION_BATCH_NODES", count)
    assert Batch.from_canonical_bytes(raw) == batch((key(),), (applied(),))
    monkeypatch.setattr(owner, "MAX_DECISION_BATCH_NODES", count - 1)
    with pytest.raises(DecisionApplicationError):
        batch((key(),), (applied(),))
    spy = Mock(side_effect=AssertionError("typed reconstruction"))
    monkeypatch.setattr(owner, "_shape", spy)
    with pytest.raises(DecisionApplicationError):
        Batch.from_canonical_bytes(raw)
    spy.assert_not_called()
    monkeypatch.setattr(owner, "MAX_DECISION_BATCH_NODES", 1)
    with pytest.raises(DecisionApplicationError):
        Batch.from_canonical_bytes(raw)
    spy.assert_not_called()


def test_key_canonical_identity_and_no_decision_time():
    value = key()
    assert "decision_time" not in value.canonical_bytes.decode()
    assert set(json.loads(value.canonical_bytes)) == {
        "schema_version",
        *[f.name for f in fields(value)],
    }
    assert value.input_id == hashlib.sha256(value.canonical_bytes).hexdigest()
    assert replace(value).digest == value.digest
    for field, replacement in (
        ("trigger_id", "1m:1"),
        ("trigger_id", "5m:0"),
        ("strategy_version", "v2"),
        ("config_hash", "c" * 64),
    ):
        assert replace(value, **{field: replacement}).digest != value.digest
    with pytest.raises(FrozenInstanceError):
        cast(Any, value).strategy_id = "other"
    assert not hasattr(value, "__dict__")


@pytest.mark.parametrize("disposition", ["APPLIED", "SKIPPED", "PARTIAL"])
@pytest.mark.parametrize(
    "reason",
    [None, "INPUT_STORE_FAILED", "INPUT_COMMIT_UNCONFIRMED", "PROFILE_NOT_READY"],
)
@pytest.mark.parametrize("refs", [False, True])
def test_disposition_reason_reference_matrix(disposition, reason, refs):
    value = key()
    args = (
        value,
        disposition,
        value.input_id if refs else None,
        "b" * 64 if refs else None,
        reason,
    )
    valid = (disposition == "APPLIED" and refs and reason is None) or (
        disposition == "SKIPPED"
        and reason in ("INPUT_STORE_FAILED", "INPUT_COMMIT_UNCONFIRMED")
    )
    if valid:
        assert Outcome(*args).reason == reason
    else:
        with pytest.raises(DecisionApplicationError):
            Outcome(*args)


@pytest.mark.parametrize("disposition", ["APPLIED", "SKIPPED"])
@pytest.mark.parametrize("suppressed", [False, True])
@pytest.mark.parametrize("reason", [None, "SNAPSHOT_REVOKED", "OTHER"])
def test_suppression_matrix(disposition, suppressed, reason):
    value = (
        applied()
        if disposition == "APPLIED"
        else Outcome(key(), "SKIPPED", None, None, "INPUT_STORE_FAILED")
    )
    valid = (not suppressed and reason is None) or (
        disposition == "APPLIED" and suppressed and reason == "SNAPSHOT_REVOKED"
    )
    if valid:
        replace(value, signal_suppressed=suppressed, suppression_reason=reason)
    else:
        with pytest.raises(DecisionApplicationError):
            replace(value, signal_suppressed=suppressed, suppression_reason=reason)


def test_batch_exact_participants_permutations_and_scope():
    first, second = key(), replace(key(), strategy_id="second")
    outcomes = (applied(first), applied(second))
    expected = batch((first, second), outcomes)
    for keys in permutations((first, second)):
        for values in permutations(outcomes):
            assert batch(keys, values).canonical_bytes == expected.canonical_bytes
            assert batch(keys, values).digest == expected.digest
    assert batch().participants == batch().outcomes == ()
    for keys, values in (
        ((first,), ()),
        ((), outcomes),
        ((first, first), (outcomes[0],)),
        ((first,), (outcomes[0], outcomes[0])),
        (
            (first, replace(first, strategy_version="v2")),
            (outcomes[0], applied(replace(first, strategy_version="v2"))),
        ),
    ):
        with pytest.raises(DecisionApplicationError):
            batch(keys, values)
    for field, value in (
        ("environment", "paper"),
        ("execution_scope_id", "other"),
        ("product_id", "BINANCE:ETHUSDT-SPOT"),
        ("timeframe", "5m"),
        ("bar_start_ms", 1),
    ):
        with pytest.raises(DecisionApplicationError):
            replace(expected, **{field: value})


@pytest.mark.parametrize(
    "bad", [True, "SECRET\x00", "中文", type("String", (str,), {})("s")]
)
def test_hostile_strings(bad):
    for field in (
        "environment",
        "strategy_id",
        "strategy_version",
        "execution_scope_id",
        "product_id",
        "config_hash",
        "trigger_kind",
        "trigger_id",
    ):
        with pytest.raises(DecisionApplicationError) as error:
            replace(key(), **{field: bad})
        assert (
            str(error.value) == "MARKET_DATA_DECISION_INVALID"
            and error.value.__cause__ is None
        )


def test_remaining_exact_domains():
    for changes in (
        dict(input_id=None),
        dict(input_digest=None),
        dict(input_id="c" * 64),
        dict(contract_version=True),
        dict(signal_suppressed=1),
    ):
        with pytest.raises(DecisionApplicationError):
            replace(applied(), **changes)
    for trigger in ("1m:00", "1m:-1", "1m:9223372036854775808", "1m:0:1", "1m:1.0"):
        with pytest.raises(DecisionApplicationError):
            replace(key(), trigger_id=trigger)
    with pytest.raises(DecisionApplicationError):
        replace(batch(), participants=[])


def test_literal_canonical_goldens():
    expected_key = (
        b'{"config_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"environment":"live","execution_scope_id":"deployment","product_id":"BINANCE:BTCUSDT-SPOT",'
        b'"schema_version":1,"strategy_id":"s","strategy_version":"v1","trigger_id":"1m:0","trigger_kind":"CANDLE"}'
    )
    expected_outcome = (
        b'{"contract_version":1,"disposition":"APPLIED","input_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
        b'"input_id":"4f179c0d2513ef607dcde21d3c9140aeb3c225c44dd03d35ef74864577df19b9",'
        b'"key":{"config_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"environment":"live","execution_scope_id":"deployment","product_id":"BINANCE:BTCUSDT-SPOT",'
        b'"strategy_id":"s","strategy_version":"v1","trigger_id":"1m:0","trigger_kind":"CANDLE"},'
        b'"reason":null,"signal_suppressed":false,"suppression_reason":null}'
    )
    expected_batch = (
        b'{"bar_start_ms":0,"environment":"live","execution_scope_id":"deployment","outcomes":['
        b'{"contract_version":1,"disposition":"APPLIED","input_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",'
        b'"input_id":"4f179c0d2513ef607dcde21d3c9140aeb3c225c44dd03d35ef74864577df19b9",'
        b'"key":{"config_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"environment":"live","execution_scope_id":"deployment","product_id":"BINANCE:BTCUSDT-SPOT",'
        b'"strategy_id":"s","strategy_version":"v1","trigger_id":"1m:0","trigger_kind":"CANDLE"},'
        b'"reason":null,"signal_suppressed":false,"suppression_reason":null}],'
        b'"participants":[{"config_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"environment":"live","execution_scope_id":"deployment","product_id":"BINANCE:BTCUSDT-SPOT",'
        b'"strategy_id":"s","strategy_version":"v1","trigger_id":"1m:0","trigger_kind":"CANDLE"}],'
        b'"product_id":"BINANCE:BTCUSDT-SPOT","schema_version":1,"timeframe":"1m"}'
    )
    for value, expected, digest in (
        (
            key(),
            expected_key,
            "4f179c0d2513ef607dcde21d3c9140aeb3c225c44dd03d35ef74864577df19b9",
        ),
        (
            applied(),
            expected_outcome,
            "63e44c31d61476902143cb8359cb94c0abfc87a2a6f9a6827bcf615ca1dc6e58",
        ),
        (
            batch((key(),), (applied(),)),
            expected_batch,
            "04289b78b76cfc89a9e5e1d721180d37bc16e83af1f1dc2384eb0bce77bf6a69",
        ),
    ):
        assert value.canonical_bytes == expected
        assert value.digest == digest


def test_same_key_different_input_digest_is_duplicate_outcome():
    first = applied()
    second = replace(first, input_digest="c" * 64)
    assert first.key == second.key and first.input_id == second.input_id
    assert first.digest != second.digest
    for outcome in (first, second):
        assert batch((first.key,), (outcome,)).outcomes == (outcome,)
    for order in permutations((first, second)):
        with pytest.raises(DecisionApplicationError):
            batch((first.key,), order)
