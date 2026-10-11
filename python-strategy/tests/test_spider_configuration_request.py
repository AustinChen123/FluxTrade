"""Pure contract tests for Spider manual configuration request evidence."""

from copy import deepcopy
from decimal import Decimal
from typing import cast

import pytest

from src.core.backtest.spider_policy import DEFAULTS
from src.core.spider_management.configuration_request import (
    SpiderConfigurationRequest,
    SpiderConfigurationRequestError,
    SpiderConfigurationRequestIntegrityError,
    SpiderConfigurationRequestKind,
    SpiderManualConfiguration,
)


def configuration(*, policy_id: str = "SPIDER_HOST_ORIGINAL_PAID_V1", product_order=("BTC-USDT-SWAP", "ETH-USDT-SWAP")):
    products = list(product_order)
    value = {
        "schema_version": "spider_manual_configuration_v1",
        "policy_id": policy_id,
        "policy_version": "1",
        "policy_source_sha256": "4dbf2bdbee87d4100004fe5b14a93f597e64a9bca6525ed2902afd3476ee40ec",
        "parameters": dict(DEFAULTS),
        "ordered_products": products,
        "product_settings": [
            {
                "product_id": product,
                "active": "true",
                "leverage": "1",
                "歩差": "0.01",
                "單數": "10",
                "hold上限": "2",
                "hold下限": "0.5",
            }
            for product in products
        ],
        "risk_selection": None,
    }
    if policy_id == "SPIDER_ORIGINAL_GROSS_PROTECTED_PAID_V1":
        value["risk_selection"] = {
            "admission_policy_id": "CURRENT_MARK_GROSS_CAP_TWO_V1",
            "response_policy_id": "PROPORTIONAL_PAID_GROSS_TARGET_V1",
            "response_parameters": {
                "trigger_cap": "2",
                "target_cap": "1.5",
                "allocation": "GROSS_PROPORTIONAL_V1",
                "quantity_rounding": "CEIL_STEP_NO_MINIMUM_UPLIFT_V1",
            },
        }
    return value


def request(
    kind=SpiderConfigurationRequestKind.PROPOSAL,
    *,
    config=None,
    key="config-v1",
    proposal_request_id=None,
    version=7,
):
    return SpiderConfigurationRequest.from_fields(
        environment="live",
        execution_scope_id="account-main",
        strategy_id="spider-main",
        request_kind=kind,
        actor="operator",
        idempotency_key=key,
        expected_state_version=version,
        configuration=configuration() if config is None else config,
        proposal_request_id=proposal_request_id,
    )


def test_configuration_round_trip_is_canonical_and_detached():
    source = configuration()
    first = SpiderManualConfiguration.create(source)
    source["parameters"]["TotalLimit"] = "99"
    assert cast(dict[str, object], first.value["parameters"])["TotalLimit"] == "1"
    assert SpiderManualConfiguration.from_canonical_bytes(first.canonical_bytes) == first


def test_product_order_is_semantic_and_request_identity_is_stable():
    first = request()
    reordered = request(config=configuration(product_order=("ETH-USDT-SWAP", "BTC-USDT-SWAP")))
    assert first.configuration.sha256 != reordered.configuration.sha256
    assert first.request_id == request().request_id
    assert first.request_id == reordered.request_id
    assert first.canonical_bytes != reordered.canonical_bytes
    assert first.canonical_bytes == SpiderConfigurationRequest.from_canonical_bytes(
        first.canonical_bytes
    ).canonical_bytes


def test_gross_configuration_requires_exact_protected_selection():
    assert SpiderManualConfiguration.create(
        configuration(policy_id="SPIDER_ORIGINAL_GROSS_PROTECTED_PAID_V1")
    )
    altered = configuration(policy_id="SPIDER_ORIGINAL_GROSS_PROTECTED_PAID_V1")
    cast(dict[str, object], altered["risk_selection"])["response_parameters"] = {
        "trigger_cap": "2",
        "target_cap": "1.4",
        "allocation": "GROSS_PROPORTIONAL_V1",
        "quantity_rounding": "CEIL_STEP_NO_MINIMUM_UPLIFT_V1",
    }
    with pytest.raises(SpiderConfigurationRequestError):
        SpiderManualConfiguration.create(altered)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value["parameters"].pop("TotalLimit"),
        lambda value: value["parameters"].update(TotalLimit=Decimal("1")),
        lambda value: value["parameters"].update(TotalLimit="NaN"),
        lambda value: value["parameters"].update(TotalLimit="01"),
        lambda value: value["ordered_products"].reverse(),
        lambda value: value["product_settings"][0].update(extra="x"),
        lambda value: value["product_settings"][0].update(active=True),
        lambda value: value.update(policy_source_sha256="0" * 64),
        lambda value: value.update(unknown="x"),
    ],
)
def test_manual_configuration_rejects_unsupported_or_noncanonical_values(mutate):
    value = deepcopy(configuration())
    mutate(value)
    with pytest.raises(SpiderConfigurationRequestError):
        SpiderManualConfiguration.create(value)


def test_request_confirmation_requires_explicit_proposal_identity():
    with pytest.raises(SpiderConfigurationRequestError):
        request(SpiderConfigurationRequestKind.CONFIRMATION)
    proposal = request()
    confirmed = request(
        SpiderConfigurationRequestKind.CONFIRMATION,
        key="confirm-v1",
        proposal_request_id=proposal.request_id,
    )
    assert confirmed.request_id != proposal.request_id
    assert confirmed.proposal_request_id == proposal.request_id


@pytest.mark.parametrize("raw", [None, b"", b"x" * 65537])
def test_canonical_decode_rejects_invalid_type_and_payload_bounds(raw):
    with pytest.raises(SpiderConfigurationRequestIntegrityError):
        SpiderManualConfiguration.from_canonical_bytes(raw)


def test_canonical_decode_maps_deep_recursion_to_integrity_error():
    raw = b"[" * 1100 + b"0" + b"]" * 1100
    with pytest.raises(SpiderConfigurationRequestIntegrityError):
        SpiderConfigurationRequest.from_canonical_bytes(raw)
