from dataclasses import replace
from hashlib import sha256
from pathlib import Path
import ast
import inspect
from typing import cast

import pytest

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.backtest.spider_historical_input import HistoricalInputError, encode_historical_run_input
from src.core.backtest.spider_execution_input import (
    ENDPOINT_POLICY_ID,
    POLICY_ID,
    SCHEMA_VERSION,
    BASE_POLICY_SOURCE_SHA256,
    SpiderExecutionInput,
    decode_spider_execution_input,
    encode_spider_execution_input,
    validate_spider_execution_input,
)
from test_spider_historical_input import _initial_account_state, _p2_configuration, _valid_run, _run_with_configuration


def _input(run=None, **changes):
    base = _valid_run() if run is None else run
    if base.policy_source_sha256 != BASE_POLICY_SOURCE_SHA256:
        base = replace(base, policy_source_sha256=BASE_POLICY_SOURCE_SHA256)
    return SpiderExecutionInput(SCHEMA_VERSION, POLICY_ID, ENDPOINT_POLICY_ID, base,
                                base.range_start_ms, 0, **changes)


def test_execution_input_roundtrip_embeds_exact_p3_object_and_preserves_base_bytes():
    run = _input().historical_input
    original = encode_historical_run_input(run)
    value = _input(run)
    encoded = encode_spider_execution_input(value)
    wire = cast(dict[str, object], decode_canonical(encoded))
    assert set(wire) == {"schema_version", "policy_id", "endpoint_policy_id", "historical_input", "policy_end_ms", "generation"}
    assert wire["historical_input"] == decode_canonical(original)
    decoded = decode_spider_execution_input(encoded)
    assert decoded == value
    assert encode_historical_run_input(decoded.historical_input) == original
    assert encode_spider_execution_input(decoded) == encoded


@pytest.mark.parametrize("mutation", ["missing", "extra", "nested_type", "schema", "host", "endpoint", "base_policy", "base_version", "base_source", "cutoff_bool", "cutoff_lower", "cutoff_upper", "generation_bool", "generation_nonzero"])
def test_execution_input_rejects_unknown_identity_or_malformed_contract(mutation):
    value = _input()
    wire = cast(dict[str, object], decode_canonical(encode_spider_execution_input(value)))
    if mutation == "missing":
        del wire["generation"]
    elif mutation == "extra":
        wire["extra"] = True
    elif mutation == "nested_type":
        wire["historical_input"] = encode_historical_run_input(value.historical_input).decode()
    elif mutation == "schema":
        wire["schema_version"] = "future"
    elif mutation == "host":
        wire["policy_id"] = "OTHER"
    elif mutation == "endpoint":
        wire["endpoint_policy_id"] = "OTHER"
    elif mutation in {"base_policy", "base_version", "base_source"}:
        base = cast(dict[str, object], wire["historical_input"])
        key = {"base_policy": "policy_id", "base_version": "policy_version", "base_source": "policy_source_sha256"}[mutation]
        base[key] = "0" * 64 if mutation == "base_source" else "OTHER"
    elif mutation == "cutoff_bool":
        wire["policy_end_ms"] = True
    elif mutation == "cutoff_lower":
        wire["policy_end_ms"] = value.historical_input.range_start_ms - 1
    elif mutation == "cutoff_upper":
        wire["policy_end_ms"] = value.historical_input.range_end_ms
    elif mutation == "generation_bool":
        wire["generation"] = True
    else:
        wire["generation"] = 1
    expected_message = "unsupported original P3 policy identity" if mutation == "base_source" else None
    with pytest.raises(HistoricalInputError, match=expected_message):
        decode_spider_execution_input(canonical_bytes(wire))


@pytest.mark.parametrize("mutation", ["duplicate_key", "float"])
def test_execution_input_rejects_ambiguous_or_float_json(mutation):
    raw = encode_spider_execution_input(_input())
    token = b'"generation":0'
    assert raw.count(token) == 1
    replacement = (b'"generation":0,"generation":0' if mutation == "duplicate_key"
                   else b'"generation":0.0')
    raw = raw.replace(token, replacement, 1)
    with pytest.raises(HistoricalInputError):
        decode_spider_execution_input(raw)


def test_execution_input_hash_tracks_semantics_but_not_transport_run_id():
    value = _input()
    baseline = validate_spider_execution_input(value)
    assert validate_spider_execution_input(replace(value, policy_end_ms=value.policy_end_ms + 1)) != baseline
    changed_run = replace(value.historical_input, run_id="different-transport-id")
    assert validate_spider_execution_input(replace(value, historical_input=changed_run)) == baseline
    changed_parameters = replace(value.historical_input, parameters=(("defaultN", "2"),))
    assert validate_spider_execution_input(replace(value, historical_input=changed_parameters)) != baseline


@pytest.mark.parametrize("kind", ["order", "lot", "empty_lots"])
def test_execution_input_rejects_mixed_initial_ownership(kind):
    run = _valid_run()
    config = _p2_configuration()
    if kind == "order":
        config["orders"] = [dict(intent_id="i", order_id="o", client_order_id="c", strategy_id="OTHER",
                                 product_id="A-USDT-SWAP", side="LONG", limit_price="100", reduce_only=False,
                                 original_quantity_contracts="1", filled_quantity_contracts="0",
                                 canceled_quantity_contracts="0", remaining_quantity_contracts="1", status="OPEN")]
    elif kind == "lot":
        config["positions"] = [dict(product_id="A-USDT-SWAP", side="LONG", quantity_contracts="1",
                                    lots=[dict(seed_execution_id="e", seed_sequence=0, strategy_id="OTHER",
                                               quantity_contracts="1", entry_price="100")])]
    else:
        config["positions"] = [dict(product_id="A-USDT-SWAP", side="LONG", quantity_contracts="1", lots=[])]
    raw = canonical_bytes(config)
    seeded = replace(_run_with_configuration(run, config), initial_account_state=_initial_account_state(
        orders=config["orders"], positions=config["positions"]))
    assert seeded.configuration_sha256 == sha256(raw).hexdigest()
    with pytest.raises(HistoricalInputError, match="ownership mismatch|position lots"):
        validate_spider_execution_input(_input(seeded))


def test_strategy_ownership_is_not_required_to_equal_host_policy_id():
    run = _valid_run()
    renamed = replace(run, strategy_identity="dedicated-strategy")
    assert validate_spider_execution_input(_input(renamed))


@pytest.mark.parametrize("kind", ["order", "lot"])
def test_execution_input_accepts_initial_state_owned_by_admitted_strategy(kind):
    run = _valid_run()
    config = _p2_configuration()
    if kind == "order":
        config["orders"] = [dict(intent_id="i", order_id="o", client_order_id="c",
                                 strategy_id=run.strategy_identity, product_id="A-USDT-SWAP",
                                 side="LONG", limit_price="100", reduce_only=False,
                                 original_quantity_contracts="1", filled_quantity_contracts="0",
                                 canceled_quantity_contracts="0", remaining_quantity_contracts="1", status="OPEN")]
    else:
        config["positions"] = [dict(product_id="A-USDT-SWAP", side="LONG", quantity_contracts="1",
                                    lots=[dict(seed_execution_id="e", seed_sequence=0,
                                               strategy_id=run.strategy_identity,
                                               quantity_contracts="1", entry_price="100")])]
    seeded = replace(_run_with_configuration(run, config), initial_account_state=_initial_account_state(
        orders=config["orders"], positions=config["positions"]))
    assert validate_spider_execution_input(_input(seeded))


def test_input_module_has_no_owner_or_runtime_construction_dependency():
    source_path = inspect.getsourcefile(validate_spider_execution_input)
    assert source_path is not None
    source = Path(source_path)
    tree = ast.parse(source.read_text())
    forbidden = {"spider_scenario_run", "spider_run_admission", "synthetic_scenario_codec", "fluxtrade_core"}
    imports = {alias.name for node in ast.walk(tree) if isinstance(node, ast.Import) for alias in node.names}
    imports.update(node.module or "" for node in ast.walk(tree) if isinstance(node, ast.ImportFrom))
    assert not any(any(name in imported for name in forbidden) for imported in imports)
