import json
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles import bootstrap_seed as owner
from src.core.market_data.profiles.decision_context import ProfileDecisionStatus
from test_profile_bootstrap_seed import SEED_GOLDEN, seed, suffix


def decode(raw):
    return owner.BootstrapSeed.from_canonical_bytes(raw, max_seed_candles=10)


@pytest.mark.parametrize("status", list(ProfileDecisionStatus))
@pytest.mark.parametrize("count", [0, 1, 2])
def test_roundtrip(status, count):
    value = seed(status, count=count)
    restored = decode(value.canonical_bytes)
    assert restored == value
    assert restored.digest == value.digest
    assert restored.key.seed_id == value.key.seed_id
    assert restored.canonical_bytes == value.canonical_bytes


def test_literal_golden():
    assert decode(SEED_GOLDEN) == seed(ProfileDecisionStatus.MISSING, count=1)


@pytest.mark.parametrize(
    "path,value",
    [
        (("schema_version",), True),
        (("contract_version",), 2),
        (("key", "contract_version"), True),
        (("key", "schema_version"), 2),
        (("key", "config_hash"), "SECRET"),
        (("cutover_ms",), 86460001),
        (("lookback",), 2),
        (("requirements", 0, "schema_version"), True),
        (("candles", 0, "modeled_evidence", "evidence_kind"), "LIVE"),
        (("candles", 0, "modeled_evidence", "schema_version"), True),
        (("candles", 0, "candle", "bar_start_ms"), True),
        *(
            (("candles", 0, "candle", "open"), value)
            for value in (
                True,
                10,
                "1e1",
                "10.0",
                "-0",
                "NaN",
                "Infinity",
                "1e-29",
                "SECRET",
            )
        ),
    ],
)
def test_hostile_fields(path, value):
    data = json.loads(SEED_GOLDEN)
    target = data
    for part in path[:-1]:
        target = target[part]
    target[path[-1]] = value
    with pytest.raises(
        owner.BootstrapSeedError, match="^PROFILE_BOOTSTRAP_INVALID$"
    ) as error:
        decode(owner._bytes(data))
    assert error.value.__cause__ is None


@pytest.mark.parametrize(
    "path",
    [
        (),
        ("key",),
        ("requirements", 0),
        ("candles", 0),
        ("candles", 0, "candle"),
        ("candles", 0, "modeled_evidence"),
        ("candles", 0, "modeled_evidence", "context"),
    ],
)
@pytest.mark.parametrize("mutation", ["extra", "missing"])
def test_exact_nested_keys(path, mutation):
    data = json.loads(SEED_GOLDEN)
    target = data
    for part in path:
        target = target[part]
    if mutation == "extra":
        target["SECRET"] = None
    else:
        target.pop(next(iter(target)))
    with pytest.raises(owner.BootstrapSeedError):
        decode(owner._bytes(data))


@pytest.mark.parametrize(
    "raw",
    [
        b"\xff",
        b"{}",
        b"[]",
        b"NaN",
        b"1.0",
        SEED_GOLDEN + b"\n",
        SEED_GOLDEN.replace(b'"lookback":1', b'"lookback":1,"lookback":1'),
        SEED_GOLDEN.replace(b'"close":"10"', b'"close":"10","close":"10"'),
        bytearray(SEED_GOLDEN),
        SEED_GOLDEN.decode(),
    ],
)
def test_raw_rejection(raw):
    with pytest.raises(owner.BootstrapSeedError):
        decode(raw)


def test_admission_before_parse_and_reconstruction(monkeypatch):
    assert decode(SEED_GOLDEN)
    monkeypatch.setattr(owner, "MAX_BOOTSTRAP_BYTES", len(SEED_GOLDEN))
    assert decode(SEED_GOLDEN)
    with monkeypatch.context() as patch:
        parser = Mock(side_effect=AssertionError("must not parse"))
        patch.setattr(owner.json, "loads", parser)
        with pytest.raises(owner.BootstrapSeedError):
            decode(SEED_GOLDEN + b" ")
        parser.assert_not_called()
    monkeypatch.setattr(owner, "MAX_BOOTSTRAP_BYTES", 32 * 1024 * 1024)
    trap = Mock(side_effect=AssertionError("must not reconstruct"))
    monkeypatch.setattr(owner, "_context", trap)
    data = json.loads(SEED_GOLDEN)
    nested = None
    for _ in range(17):
        nested = [nested]
    data["candles"][0]["modeled_evidence"]["context"] = nested
    with pytest.raises(owner.BootstrapSeedError):
        decode(owner._bytes(data))
    trap.assert_not_called()


def test_live_input_cannot_be_bootstrap_evidence():
    _, pinned = suffix(seed())
    assert pinned is not None
    with pytest.raises(owner.BootstrapSeedError):
        decode(pinned.canonical_bytes)
    data = json.loads(SEED_GOLDEN)
    data["candles"][0]["modeled_evidence"] = json.loads(pinned.canonical_bytes)
    with pytest.raises(owner.BootstrapSeedError):
        decode(owner._bytes(data))


@pytest.mark.parametrize(
    "bound", ["MAX_INPUT_BYTES", "MAX_INPUT_NODES", "MAX_INPUT_PROFILES"]
)
def test_evidence_admission_before_context(bound, monkeypatch):
    assert decode(SEED_GOLDEN)
    trap = Mock(side_effect=AssertionError("must not reconstruct"))
    monkeypatch.setattr(owner, "_context", trap)
    monkeypatch.setattr(owner, bound, 0)
    with pytest.raises(owner.BootstrapSeedError):
        decode(SEED_GOLDEN)
    trap.assert_not_called()


def test_exact_raw_and_caller_limit():
    class Bytes(bytes):
        pass

    class Integer(int):
        pass

    with pytest.raises(owner.BootstrapSeedError):
        decode(Bytes(SEED_GOLDEN))
    for limit in (True, Integer(10), 0, -1, 2**63):
        with pytest.raises(owner.BootstrapSeedError):
            owner.BootstrapSeed.from_canonical_bytes(
                SEED_GOLDEN, max_seed_candles=limit
            )
    with pytest.raises(owner.BootstrapSeedError):
        owner.BootstrapSeed.from_canonical_bytes(
            seed().canonical_bytes, max_seed_candles=1
        )
