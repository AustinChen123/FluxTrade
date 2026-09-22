from dataclasses import replace
import json
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles import decision_input as owner
from test_profile_context_enrichment import fixture, requirement, B, S, Collection, DAY
from test_profile_decision_application import key


def sample(basis=B.LIVE_OBSERVED, status=S.FRESH):
    item, req = fixture(basis, status)
    return owner.MarketDataDecisionInput(
        key(), (req,), DAY + 3, Collection(DAY + 3, (item,))
    )


@pytest.mark.parametrize("basis", list(B))
@pytest.mark.parametrize("status", list(S))
def test_full_matrix_roundtrip(basis, status):
    value = sample(basis, status)
    decoded = owner.MarketDataDecisionInput.from_canonical_bytes(value.canonical_bytes)
    assert decoded == value and decoded.input_digest == value.input_digest
    assert decoded.input_id == value.key.input_id
    policy = json.loads(value.policy_bytes)["policies"][0]
    assert policy["basis"] == basis.value
    assert policy["freshness_policy_id"] == (
        "utc_complete_strict_v1" if basis is B.LIVE_OBSERVED else None
    )
    assert policy["availability_policy_id"] == (
        "modeled" if basis is B.MODELED else None
    )


def test_literal_empty_golden():
    value = owner.MarketDataDecisionInput(key(), (), 0, Collection(0, ()))
    expected = (
        b'{"context":{"decision_time_ms":0,"profiles":[],"schema_version":1},"decision_time_ms":0,'
        b'"key":{"config_hash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"environment":"live","execution_scope_id":"deployment","product_id":"BINANCE:BTCUSDT-SPOT",'
        b'"schema_version":1,"strategy_id":"s","strategy_version":"v1","trigger_id":"1m:0","trigger_kind":"CANDLE"},'
        b'"policy":{"policies":[],"schema_version":1},"requirements":{"requirements":[],"schema_version":1},"schema_version":1}'
    )
    assert value.canonical_bytes == expected
    assert (
        value.input_digest
        == "aaa1808ac1a6499f3efc954c127272fbbfd43ced867f24114e325332580ce38f"
    )
    assert owner.MarketDataDecisionInput.from_canonical_bytes(expected) == value


def test_permutations_auxiliary_and_digest_domains():
    first = sample(status=S.MISSING)
    item = first.context.profiles[0]
    other = replace(
        item, request=replace(item.request, product_id="BINANCE:ETHUSDT-SPOT")
    )
    reqs = first.requirements + (requirement(other.request),)
    value = replace(
        first, requirements=reqs, context=Collection(DAY + 3, (item, other))
    )
    reverse = replace(
        value,
        requirements=tuple(reversed(reqs)),
        context=Collection(DAY + 3, (other, item)),
    )
    assert value == reverse and value.input_digest == reverse.input_digest
    assert value.key.product_id != other.request.product_id
    assert value.requirements_digest != first.requirements_digest
    assert value.policy_digest != first.policy_digest
    modeled = sample(B.MODELED, S.MISSING)
    modified = replace(
        modeled.context.profiles[0],
        request=replace(
            modeled.context.profiles[0].request, availability_policy_id="other_model"
        ),
    )
    changed = replace(modeled, context=Collection(DAY + 3, (modified,)))
    assert changed.requirements_digest == modeled.requirements_digest
    assert changed.policy_digest != modeled.policy_digest
    assert changed.input_digest != modeled.input_digest


@pytest.mark.parametrize(
    "raw",
    [
        b"\xff",
        b"{}",
        b"[]",
        b"null",
        b'{"x":1,"x":2}',
        b'{"x":NaN}',
        b'{"x":1.0}',
        b" " * (8 * 1024 * 1024 + 1),
    ],
)
def test_malformed_raw(raw):
    with pytest.raises(owner.DecisionInputError, match="^MARKET_DATA_INPUT_INVALID$"):
        owner.MarketDataDecisionInput.from_canonical_bytes(raw)


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown",
        "missing",
        "duplicate",
        "decimal",
        "identity",
        "policy",
        "depth",
        "schema",
        "time",
        "enum",
    ],
)
def test_nested_hostile_matrix(mutation):
    value = sample()
    body = json.loads(value.canonical_bytes)
    item = body["context"]["profiles"][0]
    if mutation == "unknown":
        item["extra"] = 1
    elif mutation == "missing":
        del item["reason"]
    elif mutation == "decimal":
        item["profile"]["base_volume"] = "1e0"
    elif mutation == "identity":
        item["profile"]["composite_id"] = "0" * 64
    elif mutation == "policy":
        body["policy"]["policies"][0]["availability_policy_id"] = "SECRET"
    elif mutation == "schema":
        body["schema_version"] = True
    elif mutation == "time":
        body["decision_time_ms"] = True
    elif mutation == "enum":
        item["status"] = "PARTIAL"
    elif mutation == "depth":
        for _ in range(17):
            body = {"nested": body}
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    if mutation == "duplicate":
        raw = raw.replace(b'"reason":null', b'"reason":null,"reason":null')
    with pytest.raises(owner.DecisionInputError) as error:
        owner.MarketDataDecisionInput.from_canonical_bytes(raw)
    assert "SECRET" not in str(error.value) and error.value.__cause__ is None


def test_collection_admission_exact_and_one_over():
    value = sample(status=S.MISSING)
    item = value.context.profiles[0]
    items = tuple(
        replace(item, request=replace(item.request, output_grid_id=f"grid_{i}"))
        for i in range(33)
    )
    reqs = tuple(requirement(item.request) for item in items)
    accepted = replace(
        value, requirements=reqs[:32], context=Collection(DAY + 3, items[:32])
    )
    assert (
        owner.MarketDataDecisionInput.from_canonical_bytes(accepted.canonical_bytes)
        == accepted
    )
    with pytest.raises(owner.DecisionInputError):
        replace(value, requirements=reqs, context=Collection(DAY + 3, items))
    with pytest.raises(owner.DecisionInputError):
        replace(value, requirements=value.requirements * 2)
    with pytest.raises(owner.DecisionInputError):
        owner.MarketDataDecisionInput.from_canonical_bytes(value.canonical_bytes + b" ")


@pytest.mark.parametrize(
    "bad", ["SECRET", "NaN", "Infinity", "-0", "1.00", "1e0", "\ud800"]
)
def test_decimal_failures_are_sanitized(bad):
    body = json.loads(sample().canonical_bytes)
    body["context"]["profiles"][0]["profile"]["base_volume"] = bad
    with pytest.raises(owner.DecisionInputError, match="^MARKET_DATA_INPUT_INVALID$"):
        owner.MarketDataDecisionInput.from_canonical_bytes(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        )


def test_bytes_admission_before_json_and_exact_encoding_cap(monkeypatch):
    parser = Mock(side_effect=AssertionError("parser forbidden"))
    with monkeypatch.context() as patch:
        patch.setattr(owner.json, "loads", parser)
        with pytest.raises(owner.DecisionInputError):
            owner.MarketDataDecisionInput.from_canonical_bytes(
                b" " * (owner.MAX_INPUT_BYTES + 1)
            )
        parser.assert_not_called()
    value = sample()
    size = len(value.canonical_bytes)
    monkeypatch.setattr(owner, "MAX_INPUT_BYTES", size)
    assert replace(value).canonical_bytes == value.canonical_bytes
    monkeypatch.setattr(owner, "MAX_INPUT_BYTES", size - 1)
    with pytest.raises(owner.DecisionInputError):
        replace(value)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("key", None),
        ("requirements", []),
        ("context", None),
        ("decision_time_ms", True),
        ("decision_time_ms", -1),
        ("decision_time_ms", 2**63),
    ],
)
def test_exact_envelope_domain(field, bad):
    with pytest.raises(owner.DecisionInputError):
        replace(sample(), **{field: bad})


def test_depth_admission_precedes_typed_reconstruction(monkeypatch):
    value = sample()
    reconstruct = Mock(wraps=owner._context)
    monkeypatch.setattr(owner, "_context", reconstruct)
    assert (
        owner.MarketDataDecisionInput.from_canonical_bytes(value.canonical_bytes)
        == value
    )
    reconstruct.assert_called_once()
    reconstruct.reset_mock()
    reconstruct.side_effect = AssertionError("typed reconstruction forbidden")
    body = json.loads(value.canonical_bytes)
    nested = None
    for _ in range(17):
        nested = [nested]
    # Preserve every envelope key; only an existing nested value exceeds depth.
    body["context"]["profiles"][0]["reason"] = nested
    raw = json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(owner.DecisionInputError):
        owner.MarketDataDecisionInput.from_canonical_bytes(raw)
    reconstruct.assert_not_called()


def test_bin_cap_constructor_and_decoder_are_causal(monkeypatch):
    assert owner.MAX_INPUT_BINS_PER_PROFILE == 100_000
    value = sample()
    item = value.context.profiles[0]
    assert item.profile is not None and item.profile.bins
    profile = item.profile
    extra = replace(profile.bins[-1], bin_index=profile.bins[-1].bin_index + 1)
    over_profile = replace(profile, bins=profile.bins + (extra,))
    over = replace(
        value, context=Collection(DAY + 3, (replace(item, profile=over_profile),))
    )
    raw = over.canonical_bytes
    assert len(raw) < owner.MAX_INPUT_BYTES
    monkeypatch.setattr(owner, "MAX_INPUT_BINS_PER_PROFILE", len(profile.bins))
    assert replace(value) == value
    assert (
        owner.MarketDataDecisionInput.from_canonical_bytes(value.canonical_bytes)
        == value
    )
    with pytest.raises(owner.DecisionInputError):
        replace(over)
    construct_bin = Mock(side_effect=AssertionError("bin construction forbidden"))
    forbidden_bin = type(
        "ForbiddenBin", (owner.ProfileBin,), {"__new__": construct_bin}
    )
    monkeypatch.setattr(owner, "ProfileBin", forbidden_bin)
    with pytest.raises(owner.DecisionInputError):
        owner.MarketDataDecisionInput.from_canonical_bytes(raw)
    construct_bin.assert_not_called()
