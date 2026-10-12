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
from src.core.spider_management.source_checkpoint_codec import (
    decode_source_market_observation,
    replace_checkpoint_markets,
    restore_source_checkpoint,
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

    assert prepared.source_checkpoint_sha256 == (
        "fed34bc9d78fcb7eca19f2eb3df16da2875f6feef32a0088fba5f6c7c97d7d82"
    )
    assert prepared.build_checkpoint_sha256 == (
        "39bac23e0e71237c4b3394d4e7a5da6a037d75b89ef85626caa0619bba88c586"
    )
    assert prepared.initial_source_events_sha256 == (
        "565e3e696dd35c831b2c4b9d26ffc35201bbcf55bac49b250ae5dea529b7de5c"
    )

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


def _restore_checkpoint(raw, run, prepared):
    return restore_source_checkpoint(
        raw,
        ordered_products=run.ordered_products,
        policy_id=run.policy_id,
        policy_version=run.policy_version,
        policy_source_sha256=run.policy_source_sha256,
        source_sha256=prepared.historical_provenance["source_sha256"],
        confirmation_request_id=cast(
            str, prepared.runtime_selection["confirmation_request_id"]
        ),
        configuration_sha256=cast(
            str, prepared.runtime_selection["configuration_sha256"]
        ),
        generation=1,
    )


def _source_market_payload(run, prepared, snapshot_id="a" * 64):
    checkpoint = cast(
        dict[str, object], decode_canonical(prepared.build_checkpoint_bytes)
    )
    original_markets = cast(list[dict[str, object]], checkpoint["markets"])
    markets = []
    for row in original_markets:
        fields = cast(dict[str, object], row["value"])
        updated = dict(fields)
        updated["price"] = {"tag": "DECIMAL", "value": "12.5"}
        updated["ratioHL"] = {"tag": "DECIMAL", "value": "0.75"}
        markets.append({"product_id": row["product_id"], "value": updated})
    return canonical_bytes({
        "schema_version": "SPIDER_SOURCE_MARKET_V1",
        "scope": {
            "environment": "test",
            "execution_scope_id": "spider-test",
            "strategy_id": "spider-grid",
        },
        "effective_identity": {
            "generation": 1,
            "confirmation_request_id": prepared.runtime_selection["confirmation_request_id"],
            "configuration_sha256": prepared.runtime_selection["configuration_sha256"],
        },
        "snapshot_id": snapshot_id,
        "observed_at_ms": 1_800_000_000_000,
        "snapshot_as_of_ms": 1_800_000_000_000,
        "provenance": {
            "kind": "MODEL",
            "evidence_sha256": prepared.historical_input_artifact_sha256,
        },
        "markets": markets,
    })


def test_source_market_codec_replaces_only_decimal_observation_fields():
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    raw = _source_market_payload(run, prepared)
    observation = decode_source_market_observation(
        raw,
        scope={
            "environment": "test",
            "execution_scope_id": "spider-test",
            "strategy_id": "spider-grid",
        },
        generation=1,
        confirmation_request_id=cast(
            str, prepared.runtime_selection["confirmation_request_id"]
        ),
        configuration_sha256=cast(
            str, prepared.runtime_selection["configuration_sha256"]
        ),
        evidence_sha256=prepared.historical_input_artifact_sha256,
        ordered_products=run.ordered_products,
        build_checkpoint_bytes=prepared.build_checkpoint_bytes,
    )
    assert tuple(observation.markets) == run.ordered_products
    assert all(row["price"] == Decimal("12.5") for row in observation.markets.values())
    assert all(row["ratioHL"] == Decimal("0.75") for row in observation.markets.values())

    updated = replace_checkpoint_markets(
        prepared.build_checkpoint_bytes,
        ordered_products=run.ordered_products,
        markets=observation.markets,
    )
    before = cast(dict[str, object], decode_canonical(prepared.build_checkpoint_bytes))
    after = cast(dict[str, object], decode_canonical(updated))
    assert after["markets"] != before["markets"]
    assert {key: value for key, value in after.items() if key != "markets"} == {
        key: value for key, value in before.items() if key != "markets"
    }


def test_source_market_codec_accepts_pre_epoch_utc_milliseconds():
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    value = cast(dict[str, object], decode_canonical(_source_market_payload(run, prepared)))
    value["observed_at_ms"] = -1
    value["snapshot_as_of_ms"] = -2
    decoded = decode_source_market_observation(
        canonical_bytes(value),
        scope={
            "environment": "test",
            "execution_scope_id": "spider-test",
            "strategy_id": "spider-grid",
        },
        generation=1,
        confirmation_request_id=cast(
            str, prepared.runtime_selection["confirmation_request_id"]
        ),
        configuration_sha256=cast(
            str, prepared.runtime_selection["configuration_sha256"]
        ),
        evidence_sha256=prepared.historical_input_artifact_sha256,
        ordered_products=run.ordered_products,
        build_checkpoint_bytes=prepared.build_checkpoint_bytes,
    )
    assert decoded.observed_at_ms == -1
    assert decoded.snapshot_as_of_ms == -2


@pytest.mark.parametrize("mutation", ["bool", "float", "string", "zero", "wrong"])
def test_source_market_codec_rejects_noncanonical_effective_generation(mutation):
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    value = cast(dict[str, object], decode_canonical(_source_market_payload(run, prepared)))
    if mutation == "bool":
        cast(dict[str, object], value["effective_identity"])["generation"] = True
        raw = canonical_bytes(value)
    elif mutation == "float":
        raw = canonical_bytes(value).replace(b'"generation":1}', b'"generation":1.0}')
        assert raw != canonical_bytes(value)
    elif mutation == "string":
        cast(dict[str, object], value["effective_identity"])["generation"] = "1"
        raw = canonical_bytes(value)
    elif mutation == "zero":
        cast(dict[str, object], value["effective_identity"])["generation"] = 0
        raw = canonical_bytes(value)
    else:
        cast(dict[str, object], value["effective_identity"])["generation"] = 2
        raw = canonical_bytes(value)

    with pytest.raises(ValueError, match="INVALID_SOURCE_MARKET"):
        decode_source_market_observation(
            raw,
            scope={
                "environment": "test",
                "execution_scope_id": "spider-test",
                "strategy_id": "spider-grid",
            },
            generation=1,
            confirmation_request_id=cast(
                str, prepared.runtime_selection["confirmation_request_id"]
            ),
            configuration_sha256=cast(
                str, prepared.runtime_selection["configuration_sha256"]
            ),
            evidence_sha256=prepared.historical_input_artifact_sha256,
            ordered_products=run.ordered_products,
            build_checkpoint_bytes=prepared.build_checkpoint_bytes,
        )


@pytest.mark.parametrize(
    "mutation",
    [
        "price_tag", "specification", "order", "scope", "provider_provenance",
        "float_time", "before_datetime_year_one",
    ],
)
def test_source_market_codec_rejects_unsupported_observation_cells(mutation):
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    raw_value = cast(dict[str, object], decode_canonical(_source_market_payload(run, prepared)))
    if mutation == "price_tag":
        market = cast(dict[str, object], cast(list[object], raw_value["markets"])[0])
        cast(dict[str, object], market["value"])["price"] = {
            "tag": "STRING", "value": "12.5"
        }
    elif mutation == "specification":
        market = cast(dict[str, object], cast(list[object], raw_value["markets"])[0])
        cast(dict[str, object], market["value"])["lotSz"] = {
            "tag": "STRING", "value": "999"
        }
    elif mutation == "order":
        cast(list[object], raw_value["markets"]).reverse()
    elif mutation == "scope":
        cast(dict[str, object], raw_value["scope"])["strategy_id"] = "other"
    elif mutation == "provider_provenance":
        cast(dict[str, object], raw_value["provenance"])["kind"] = "PROVIDER"
    elif mutation == "before_datetime_year_one":
        raw_value["observed_at_ms"] = -62_135_596_800_001
        raw_value["snapshot_as_of_ms"] = -62_135_596_800_001
    else:
        raw_value["observed_at_ms"] = True
    with pytest.raises(ValueError, match="INVALID_SOURCE_MARKET"):
        decode_source_market_observation(
            canonical_bytes(raw_value),
            scope={
                "environment": "test",
                "execution_scope_id": "spider-test",
                "strategy_id": "spider-grid",
            },
            generation=1,
            confirmation_request_id=cast(
                str, prepared.runtime_selection["confirmation_request_id"]
            ),
            configuration_sha256=cast(
                str, prepared.runtime_selection["configuration_sha256"]
            ),
            evidence_sha256=prepared.historical_input_artifact_sha256,
            ordered_products=run.ordered_products,
            build_checkpoint_bytes=prepared.build_checkpoint_bytes,
        )


def _scalar_tags(value):
    if type(value) is dict:
        if set(value) in ({"tag"}, {"tag", "value"}) and type(value.get("tag")) is str:
            yield value["tag"]
        for item in value.values():
            yield from _scalar_tags(item)
    elif type(value) is list:
        for item in value:
            yield from _scalar_tags(item)


def test_source_checkpoint_restore_roundtrips_initial_and_build_states():
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)

    observed_tags = set()
    for raw in (prepared.source_checkpoint_bytes, prepared.build_checkpoint_bytes):
        policy = _restore_checkpoint(raw, run, prepared)
        assert policy.events == []
        from src.core.spider_management.source_checkpoint_codec import encode_source_checkpoint

        assert encode_source_checkpoint(
            policy,
            ordered_products=run.ordered_products,
            policy_id=run.policy_id,
            policy_version=run.policy_version,
            policy_source_sha256=run.policy_source_sha256,
            source_sha256=cast(str, prepared.historical_provenance["source_sha256"]),
            confirmation_request_id=cast(
                str, prepared.runtime_selection["confirmation_request_id"]
            ),
            configuration_sha256=cast(
                str, prepared.runtime_selection["configuration_sha256"]
            ),
        ) == raw
        decoded = cast(dict[str, object], decode_canonical(raw))
        observed_tags.update(_scalar_tags(decoded))
        assert policy.order_id == decoded["order_id"]
        assert policy.now_ms == decoded["now_ms"]
        assert policy.shared_ms == decoded["shared_ms"]
        assert policy.last_market_ms == decoded["last_market_ms"]
        assert policy.last_earn_ms == decoded["last_earn_ms"]
        assert list(policy.markets) == list(run.ordered_products)

    assert {"NULL", "STRING", "DECIMAL", "INT64", "BOOL"} <= observed_tags


def test_source_checkpoint_restore_roundtrips_nondefault_sparse_policy_state():
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    original = cast(dict[str, object], decode_canonical(prepared.build_checkpoint_bytes))
    policy = _restore_checkpoint(prepared.build_checkpoint_bytes, run, prepared)
    policy.reset_ms = {run.ordered_products[1]: 22, run.ordered_products[0]: 11}
    policy.last_filled_price = {
        run.ordered_products[1]: {"buy": "2.50", "sell": "2.75"},
        run.ordered_products[0]: {"buy": "1.25", "sell": "1.50"},
    }
    policy.last_capital = Decimal("123.4500")
    policy.capital = {"earn": Decimal("0.25"), "usdt": None}
    policy.replies = {
        "failed-order": {
            "instId": run.ordered_products[1],
            "side": "sell",
            "sMsg": None,
        }
    }

    from src.core.spider_management.source_checkpoint_codec import encode_source_checkpoint

    raw = encode_source_checkpoint(
        policy,
        ordered_products=run.ordered_products,
        policy_id=run.policy_id,
        policy_version=run.policy_version,
        policy_source_sha256=run.policy_source_sha256,
        source_sha256=cast(str, prepared.historical_provenance["source_sha256"]),
        confirmation_request_id=cast(
            str, prepared.runtime_selection["confirmation_request_id"]
        ),
        configuration_sha256=cast(
            str, prepared.runtime_selection["configuration_sha256"]
        ),
    )
    restored = _restore_checkpoint(raw, run, prepared)

    assert restored.events == []
    assert list(restored.reset_ms.items()) == list(policy.reset_ms.items())
    assert list(restored.last_filled_price.items()) == list(policy.last_filled_price.items())
    assert restored.last_capital == Decimal("123.45")
    assert type(restored.last_capital) is Decimal
    assert list(restored.capital.items()) == [("earn", Decimal("0.25")), ("usdt", None)]
    assert type(restored.capital["earn"]) is Decimal
    assert list(restored.replies.items()) == list(policy.replies.items())
    assert "px" not in restored.replies["failed-order"]
    assert restored.replies["failed-order"]["sMsg"] is None
    assert type(restored.running) is bool
    assert type(restored.order_id) is int
    for counter in ("now_ms", "shared_ms", "last_market_ms", "last_earn_ms", "order_id"):
        assert getattr(restored, counter) == original[counter]
    assert encode_source_checkpoint(
        restored,
        ordered_products=run.ordered_products,
        policy_id=run.policy_id,
        policy_version=run.policy_version,
        policy_source_sha256=run.policy_source_sha256,
        source_sha256=cast(str, prepared.historical_provenance["source_sha256"]),
        confirmation_request_id=cast(
            str, prepared.runtime_selection["confirmation_request_id"]
        ),
        configuration_sha256=cast(
            str, prepared.runtime_selection["configuration_sha256"]
        ),
    ) == raw


@pytest.mark.parametrize(
    "field,scalar",
    [
        (field, scalar)
        for field in ("buy", "sell")
        for scalar in (
            {"tag": "NULL"},
            {"tag": "BOOL", "value": False},
            {"tag": "INT64", "value": 1},
            {"tag": "DECIMAL", "value": "1"},
        )
    ],
)
def test_source_checkpoint_restore_rejects_nonstring_last_filled_prices(field, scalar):
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    value = cast(dict[str, object], decode_canonical(prepared.build_checkpoint_bytes))
    value["last_filled_price"] = [
        {
            "product_id": run.ordered_products[0],
            "buy": {"tag": "STRING", "value": "1"},
            "sell": {"tag": "STRING", "value": "2"},
        }
    ]
    cast(list[dict[str, object]], value["last_filled_price"])[0][field] = scalar

    with pytest.raises(ValueError, match="^INVALID_SOURCE_CHECKPOINT$"):
        _restore_checkpoint(canonical_bytes(value), run, prepared)


@pytest.mark.parametrize(
    "mutation",
    [
        "unknown_top_level",
        "unknown_scalar_tag",
        "noncanonical_decimal",
        "integer_overflow",
        "bool_in_integer_field",
        "bool_generation",
        "generation_overflow",
        "order_id_zero",
        "last_market_overflow",
        "duplicate_parameter",
        "row_order",
        "market_order",
        "duplicate_sparse_key",
        "duplicate_reply_alias",
        "unknown_reset_product",
    ],
)
def test_source_checkpoint_restore_rejects_noncanonical_or_open_shape(mutation):
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    value = cast(dict[str, object], decode_canonical(prepared.build_checkpoint_bytes))
    if mutation == "unknown_top_level":
        value["unexpected"] = None
    elif mutation == "unknown_scalar_tag":
        parameters = cast(list[dict[str, object]], value["parameters"])
        parameters[0]["value"] = {"tag": "UNKNOWN"}
    elif mutation == "noncanonical_decimal":
        parameters = cast(list[dict[str, object]], value["parameters"])
        parameters[0]["value"] = {"tag": "DECIMAL", "value": "1.0"}
    elif mutation == "integer_overflow":
        value["now_ms"] = 2**63
    elif mutation == "bool_in_integer_field":
        value["now_ms"] = True
    elif mutation == "bool_generation":
        cast(dict[str, object], value["effective_identity"])["generation"] = True
    elif mutation == "generation_overflow":
        cast(dict[str, object], value["effective_identity"])["generation"] = 2**63
    elif mutation == "order_id_zero":
        value["order_id"] = 0
    elif mutation == "last_market_overflow":
        value["last_market_ms"] = -(2**63) - 1
    elif mutation == "duplicate_parameter":
        parameters = cast(list[object], value["parameters"])
        parameters.append(parameters[0])
    elif mutation == "row_order":
        rows = cast(list[object], value["rows"])
        rows.reverse()
    elif mutation == "market_order":
        markets = cast(list[object], value["markets"])
        markets.reverse()
    elif mutation == "duplicate_sparse_key":
        value["capital"] = [
            {"key": "earn", "value": {"tag": "NULL"}},
            {"key": "earn", "value": {"tag": "NULL"}},
        ]
    elif mutation == "duplicate_reply_alias":
        value["replies"] = [
            {"alias": "same", "value": []},
            {"alias": "same", "value": []},
        ]
    else:
        value["reset_ms"] = [{"product_id": "NOT_A_PRODUCT", "value": 1}]

    with pytest.raises(ValueError, match="^INVALID_SOURCE_CHECKPOINT$"):
        _restore_checkpoint(canonical_bytes(value), run, prepared)


@pytest.mark.parametrize(
    "identity,override",
    [
        ("policy_source_sha256", "0" * 64),
        ("policy_id", "OTHER_POLICY"),
        ("policy_version", "2"),
        ("source_sha256", "0" * 64),
        ("confirmation_request_id", "0" * 64),
        ("configuration_sha256", "0" * 64),
        ("generation", 2),
    ],
)
def test_source_checkpoint_restore_rejects_wrong_expected_identity(identity, override):
    run = _admitted_run()
    confirmation = _confirmation(run)
    prepared = _prepare(run, _selection(run, confirmation), confirmation)
    kwargs = {
        "ordered_products": run.ordered_products,
        "policy_id": run.policy_id,
        "policy_version": run.policy_version,
        "policy_source_sha256": run.policy_source_sha256,
        "source_sha256": prepared.historical_provenance["source_sha256"],
        "confirmation_request_id": prepared.runtime_selection["confirmation_request_id"],
        "configuration_sha256": prepared.runtime_selection["configuration_sha256"],
        "generation": 1,
    }
    kwargs[identity] = override
    with pytest.raises(ValueError, match="^INVALID_SOURCE_CHECKPOINT$"):
        restore_source_checkpoint(prepared.build_checkpoint_bytes, **kwargs)


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
