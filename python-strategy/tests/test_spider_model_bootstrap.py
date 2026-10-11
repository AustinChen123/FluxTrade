from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from hashlib import sha256
from typing import cast

import pytest

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.backtest.synthetic_scenario_codec import loaded_native_artifact_path
from src.core.backtest.spider_policy import DEFAULTS
from src.core.product_registry import InstrumentSpec, MarketType
from src.core.spider_management.configuration_request import (
    SpiderConfigurationRequest,
    SpiderConfigurationRequestKind,
    SpiderConfigurationRequestRecord,
    SpiderManualConfiguration,
    POLICY_SOURCE_SHA256,
)
from src.core.spider_management.model_bootstrap import (
    ModelBootstrapUnavailable,
    prepare_model_bootstrap,
)
from test_spider_historical_input import HASH, _policy_cache, _valid_run


def _confirmation(run, *, config_mutation=None):
    cache = cast(dict[str, object], decode_canonical(run.initial_policy_cache))
    parameters = {
        key: format(Decimal(value), "f")
        for key, value in DEFAULTS.items()
    }
    parameters.update({key: format(Decimal(value), "f") for key, value in run.parameters})
    configuration = {
        "schema_version": "spider_manual_configuration_v1",
        "policy_id": "SPIDER_HOST_ORIGINAL_PAID_V1",
        "policy_version": "1",
        "policy_source_sha256": POLICY_SOURCE_SHA256,
        "parameters": parameters,
        "ordered_products": list(run.ordered_products),
        "product_settings": [
            {
                "product_id": row["name"],
                "active": row["active"],
                "leverage": row["leverage"],
                "歩差": row["歩差"],
                "單數": row["單數"],
                "hold上限": row["hold上限"],
                "hold下限": row["hold下限"],
            }
            for row in cast(list[dict[str, str]], cache["rows"])
        ],
        "risk_selection": None,
    }
    if config_mutation is not None:
        config_mutation(configuration)
    manual = SpiderManualConfiguration.create(configuration)
    proposal = SpiderConfigurationRequest.from_fields(
        environment="test",
        execution_scope_id="spider-test",
        strategy_id="spider-grid",
        request_kind=SpiderConfigurationRequestKind.PROPOSAL,
        actor="operator-1",
        idempotency_key="proposal-1",
        expected_state_version=0,
        configuration=manual.value,
    )
    confirmation = SpiderConfigurationRequest.from_fields(
        environment="test",
        execution_scope_id="spider-test",
        strategy_id="spider-grid",
        request_kind=SpiderConfigurationRequestKind.CONFIRMATION,
        actor="operator-1",
        idempotency_key="confirmation-1",
        expected_state_version=0,
        configuration=manual.value,
        proposal_request_id=proposal.request_id,
    )
    return SpiderConfigurationRequestRecord(
        confirmation, datetime(2026, 10, 11, tzinfo=timezone.utc)
    )


def _admitted_run():
    return replace(_valid_run(), policy_source_sha256=POLICY_SOURCE_SHA256)


def _selection(run, confirmation):
    return {
        "schema_version": "SPIDER_RUNTIME_SELECTION_V1",
        "environment": "test",
        "execution_scope_id": "spider-test",
        "strategy_id": "spider-grid",
        "confirmation_request_id": confirmation.request.request_id,
        "configuration_sha256": confirmation.request.configuration.sha256,
        "host_contract_id": "SPIDER_DURABLE_HOST_V1",
        "host_contract_sha256": HASH,
        "account_profile": "okx",
        "account_id": run.account_key,
        "provider_profile_sha256": HASH,
        "build_policy_id": "BUILD_MODEL_GUARDED_V1",
        "time_policy_id": "UTC_SOURCE_TIME_V1",
        "time_zone": "UTC",
    }


def _product_specs(run, *, exchange="okx"):
    return tuple(
        (
            product,
            InstrumentSpec(
                product_id=f"{exchange.upper()}:{symbol}USDT-PERP",
                exchange=exchange,
                symbol=f"{symbol}/USDT:USDT",
                base=symbol,
                quote="USDT",
                quantity_step=spec.quantity_step,
                price_tick=spec.price_tick,
                min_quantity=spec.minimum_quantity,
                multiplier=spec.contract_value * spec.multiplier,
                market_type=MarketType.PERPETUAL,
            ),
        )
        for product, symbol, spec in zip(
            run.ordered_products, ("A", "B"), run.spec_before, strict=True
        )
    )


def _prepare(run, selection, confirmation, *, product_specs=None, selected_venue="okx"):
    return prepare_model_bootstrap(
        run,
        selection,
        confirmation,
        product_specifications=_product_specs(run) if product_specs is None else product_specs,
        selected_venue=selected_venue,
    )


def test_model_bootstrap_captures_initialized_source_and_native_basis():
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    inspection = cast(dict[str, object], prepared.owner_evidence["inspection"])

    assert prepared.runtime_selection["account_id"] == run.account_key
    assert inspection["account_key"] == {
        "venue": "SPIDER_HISTORICAL_RESEARCH",
        "environment": "RESEARCH_ONLY",
        "account": run.account_key,
    }
    assert prepared.bootstrap_basis["historical_input_sha256"] == prepared.historical_input_sha256
    assert prepared.bootstrap_basis["native_artifact_sha256"] == sha256(
        loaded_native_artifact_path_bytes()
    ).hexdigest()
    assert prepared.financial_cursor == (
        f"model-financial-v1:{prepared.bootstrap_digest}:"
        f"{inspection['account_version']}"
    )
    assert prepared.tape_cursor == f"model-tape-v1:{prepared.bootstrap_digest}:0"
    assert prepared.initial_manifest["historical_provenance"] == prepared.historical_provenance
    assert prepared.product_map["domain"] == "SPIDER_MODEL_PRODUCT_MAP_V1"
    assert prepared.bootstrap_basis["product_map_sha256"] == prepared.product_map_sha256
    assert prepared.initial_manifest["product_map"] == prepared.product_map

    before = cast(dict[str, object], decode_canonical(prepared.source_checkpoint_bytes))
    after = cast(dict[str, object], decode_canonical(prepared.build_checkpoint_bytes))
    emissions = cast(
        dict[str, object], decode_canonical(prepared.initial_source_events_bytes)
    )
    entries = cast(list[dict[str, object]], emissions["entries"])
    assert prepared.bootstrap_basis["source_checkpoint_sha256"] == (
        prepared.source_checkpoint_sha256
    )
    assert prepared.initial_manifest["source_checkpoint_sha256"] == (
        prepared.source_checkpoint_sha256
    )
    assert prepared.initial_manifest["build_checkpoint_sha256"] == (
        prepared.build_checkpoint_sha256
    )
    assert prepared.initial_manifest["initial_source_events_sha256"] == (
        prepared.initial_source_events_sha256
    )
    assert emissions["ordered_products"] == list(run.ordered_products)
    assert [entry["source_product_id"] for entry in entries] == list(
        run.ordered_products
    )
    for product, entry in zip(run.ordered_products, entries, strict=True):
        events = cast(list[dict[str, object]], entry["events"])
        send_events = [event for event in events if event["kind"] == "send"]
        assert send_events
        assert all(
            order["instId"] == product
            for event in send_events
            for order in cast(list[dict[str, object]], event["orders"])
        )
    assert cast(int, before["order_id"]) < cast(int, after["order_id"])
    assert before["replies"] != after["replies"]
    assert prepared.source_checkpoint_sha256 != prepared.build_checkpoint_sha256


def loaded_native_artifact_path_bytes():
    from pathlib import Path

    return Path(loaded_native_artifact_path()).read_bytes()


@pytest.mark.parametrize("order_id", [0, -1])
def test_model_bootstrap_rejects_unavailable_order_counter(order_id):
    run = _admitted_run()
    cache = _policy_cache(order_id=order_id)
    unavailable = replace(run, initial_policy_cache=cache)

    with pytest.raises(ModelBootstrapUnavailable, match="^model_source_unavailable$"):
        confirmation = _confirmation(unavailable)
        _prepare(unavailable, _selection(unavailable, confirmation), confirmation)


@pytest.mark.parametrize("field", ["歩差", "leverage"])
def test_model_bootstrap_normalizes_zero_initial_build_division(field):
    run = _admitted_run()
    cache = cast(dict[str, object], decode_canonical(run.initial_policy_cache))
    rows = cast(list[dict[str, object]], cache["rows"])
    rows[0][field] = "0"
    unavailable = replace(run, initial_policy_cache=canonical_bytes(cache))
    confirmation = _confirmation(unavailable)

    with pytest.raises(ModelBootstrapUnavailable, match="^model_source_unavailable$"):
        _prepare(
            unavailable,
            _selection(unavailable, confirmation),
            confirmation,
        )


def test_model_bootstrap_rejects_confirmation_configuration_mismatch():
    run = _admitted_run()
    confirmation = _confirmation(run)
    selection = _selection(run, confirmation)
    selection["configuration_sha256"] = "0" * 64

    with pytest.raises(ModelBootstrapUnavailable, match="^model_source_unavailable$"):
        _prepare(run, selection, confirmation)


@pytest.mark.parametrize(
    "field",
    ["environment", "execution_scope_id", "strategy_id", "confirmation_request_id", "configuration_sha256"],
)
def test_model_bootstrap_rejects_confirmation_scope_or_identity_mismatch(field):
    run = _admitted_run()
    confirmation = _confirmation(run)
    selection = _selection(run, confirmation)
    selection[field] = "other" if field != "configuration_sha256" else "0" * 64

    with pytest.raises(ModelBootstrapUnavailable, match="^model_source_unavailable$"):
        _prepare(run, selection, confirmation)


def test_model_bootstrap_rejects_selected_account_mismatch_before_initialization():
    run = _admitted_run()
    confirmation = _confirmation(run)
    selection = _selection(run, confirmation)
    selection["account_id"] = "another-account"

    with pytest.raises(ModelBootstrapUnavailable, match="^model_source_unavailable$"):
        _prepare(run, selection, confirmation)


def test_model_bootstrap_normalizes_admitted_nondecimal_parameter_to_unavailable():
    run = _admitted_run()
    confirmation = _confirmation(run)
    selection = _selection(run, confirmation)
    malformed = replace(run, parameters=(("defaultN", "not-a-number"),))

    with pytest.raises(ModelBootstrapUnavailable, match="^model_source_unavailable$"):
        _prepare(malformed, selection, confirmation)


@pytest.mark.parametrize("field", ["parameters", "product_settings"])
def test_model_bootstrap_rejects_unmatched_confirmed_manual_projection(field):
    run = _admitted_run()

    def mutate(configuration):
        if field == "parameters":
            configuration[field]["defaultN"] = "2"
        elif field == "product_settings":
            configuration[field][0]["leverage"] = "8"

    confirmation = _confirmation(run, config_mutation=mutate)
    with pytest.raises(ModelBootstrapUnavailable, match="^model_source_unavailable$"):
        _prepare(run, _selection(run, confirmation), confirmation)


def test_model_bootstrap_keeps_native_identity_independent_of_historical_run_id():
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    changed = replace(run, run_id="another-historical-run")
    other_confirmation = _confirmation(changed)
    other = _prepare(changed, _selection(changed, other_confirmation), other_confirmation)

    assert prepared.native_artifact_sha256 == other.native_artifact_sha256
    assert prepared.historical_input_sha256 == other.historical_input_sha256
    assert prepared.historical_input_artifact_sha256 != other.historical_input_artifact_sha256
    assert prepared.native_artifact_sha256 != prepared.historical_input_artifact_sha256
    assert prepared.initial_manifest["historical_input_sha256"] == prepared.historical_input_sha256


@pytest.mark.parametrize(
    "mutation",
    [
        "missing",
        "reordered",
        "duplicate_target",
        "wrong_venue",
        "wrong_market",
        "wrong_quantity_step",
        "wrong_price_tick",
        "wrong_min_quantity",
        "wrong_multiplier",
    ],
)
def test_model_bootstrap_rejects_incomplete_or_incompatible_product_map(mutation):
    run = _admitted_run()
    confirmation = _confirmation(run)
    specs = list(_product_specs(run))
    if mutation == "missing":
        specs.pop()
    elif mutation == "reordered":
        specs.reverse()
    elif mutation == "duplicate_target":
        specs[1] = (specs[1][0], specs[0][1])
    elif mutation == "wrong_venue":
        specs[0] = (specs[0][0], replace(specs[0][1], exchange="binance"))
    elif mutation == "wrong_market":
        specs[0] = (specs[0][0], InstrumentSpec(
            product_id="OKX:AUSDT-SPOT", exchange="okx", symbol="A/USDT",
            base="A", quote="USDT", quantity_step=Decimal("0.001"),
            price_tick=Decimal("0.01"), min_quantity=Decimal("0.001"),
            multiplier=Decimal("1"), market_type=MarketType.SPOT,
        ))
    else:
        field = {
            "wrong_quantity_step": "quantity_step",
            "wrong_price_tick": "price_tick",
            "wrong_min_quantity": "min_quantity",
            "wrong_multiplier": "multiplier",
        }[mutation]
        specs[0] = (specs[0][0], replace(specs[0][1], **{field: Decimal("2")}))
    with pytest.raises(ModelBootstrapUnavailable, match="^model_source_unavailable$"):
        _prepare(run, _selection(run, confirmation), confirmation, product_specs=tuple(specs))


def test_model_product_map_hash_is_closed_and_bound_into_bootstrap_digest():
    run = _admitted_run()
    confirmation = _confirmation(run)
    selection = _selection(run, confirmation)
    original = _prepare(run, selection, confirmation)
    changed_specs = list(_product_specs(run))
    changed_specs[0] = (changed_specs[0][0], replace(
        changed_specs[0][1], product_id="OKX:CUSDT-PERP", symbol="C/USDT:USDT", base="C"
    ))
    changed = _prepare(
        run, selection, confirmation, product_specs=tuple(changed_specs)
    )
    assert original.product_map_sha256 != changed.product_map_sha256
    assert original.bootstrap_digest != changed.bootstrap_digest
    assert original.initial_manifest["product_map_sha256"] == original.product_map_sha256


def test_model_product_map_requires_selected_composition_venue():
    run = _admitted_run()
    confirmation = _confirmation(run)
    with pytest.raises(ModelBootstrapUnavailable, match="^model_source_unavailable$"):
        _prepare(
            run,
            _selection(run, confirmation),
            confirmation,
            selected_venue="binance",
        )
