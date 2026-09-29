"""Frozen P2 section-6 plan input; it does not execute or complete a run."""

import hashlib
from typing import Any, cast

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

_PRODUCTS = (
    "BTC-USDT-SWAP",
    "ETH-USDT-SWAP",
    "SOL-USDT-SWAP",
    "BNB-USDT-SWAP",
    "XRP-USDT-SWAP",
    "DOGE-USDT-SWAP",
    "ARB-USDT-SWAP",
    "OP-USDT-SWAP",
    "NEAR-USDT-SWAP",
    "APT-USDT-SWAP",
    "SUI-USDT-SWAP",
    "ADA-USDT-SWAP",
)
_ACCOUNT = {"venue": "okx-scenario", "environment": "test", "account": "A"}
_FIRST_ORDER_ID = "scenario-order-v1:[17, 9a, 5a, ba, 18, 13, 08, a2, cd, 67, 71, 3c, ff, 56, 81, a4, 88, 71, 5d, 81, ea, 11, 59, 12, 5e, 82, 2e, 43, 5f, 4d, a2, 26]"


def _product(product: str, ordinal: int) -> dict[str, Any]:
    return {
        "product_id": product,
        "instrument_code": ordinal,
        "taker_fee_rate": "0.001",
        "liquidation_fee_rate": "0.001",
        "specs": [
            {
                "version": "scale-spec-v1",
                "valid_from": 0,
                "valid_to": None,
                "contract_value": "1",
                "multiplier": "1",
                "price_tick": "1",
                "quantity_step": "0.5",
                "minimum_quantity": "0.5",
            }
        ],
        "tiers": [
            {
                "version": "scale-tier-v1",
                "valid_from": 0,
                "valid_to": None,
                "rows": [
                    {
                        "minimum_contracts": "0",
                        "maximum_contracts": "100000",
                        "mmr": "0.005",
                        "imr": "0.1",
                        "max_leverage": "10",
                    }
                ],
            }
        ],
        "marks": [{"valid_from": 0, "valid_to": 3000, "mark": "100"}],
    }


def _configuration() -> dict[str, Any]:
    return {
        "schema_version": "synthetic_multi_product_config_v1",
        "config_id": "SYNTHETIC_P2_SCALE_12_V1",
        "seed_effective_at": 500,
        "cash": "121.2",
        "leverage": "10",
        "products": [
            _product(product, ordinal)
            for ordinal, product in enumerate(_PRODUCTS, start=1)
        ],
        "positions": [],
        "orders": [],
    }


def _group(
    *,
    event_id: str,
    at: int,
    ordinal: int,
    kind: str,
    payload: dict[str, Any],
    parents: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "scenario_group_v1",
        "group_id": f"G-{at}",
        "account_key": dict(_ACCOUNT),
        "ordering_contract_id": "S_order_v1",
        "group_effective_at": at,
        "declared_member_count": 1,
        "members": [
            {
                "kind": kind,
                "stamp": {
                    "event_id": event_id,
                    "effective_at": at,
                    "causal_parent_ids": list(parents or []),
                    "ordering_contract_id": "S_order_v1",
                    "scenario_ordinal": ordinal,
                },
                "payload": payload,
            }
        ],
    }


def _intent_step(
    at: int, product: str, rejection_label: str | None = None
) -> dict[str, Any]:
    step: dict[str, Any] = {
        "kind": "SOURCE_GROUP",
        "at": at,
        "group": _group(
            event_id=f"event-{at}",
            at=at,
            ordinal=60,
            kind="INTENT",
            payload={
                "intent_id": f"I-{at}",
                "client_order_id": f"C-{at}",
                "config_id": "SYNTHETIC_P2_SCALE_12_V1",
                "product_id": product,
                "strategy_id": "section6-scale",
                "side": "LONG",
                "order_type": "LIMIT",
                "quantity_contracts": "1",
                "limit_price": "100",
                "reduce_only": False,
                "requested_at": at,
            },
        ),
    }
    if rejection_label is not None:
        step["expected_rejection_label"] = rejection_label
    return step


def _recipe() -> list[dict[str, Any]]:
    steps = [
        _intent_step(500 + ordinal, product)
        for ordinal, product in enumerate(_PRODUCTS, start=1)
    ]
    steps.extend(
        [
            _intent_step(513, _PRODUCTS[0], "INSUFFICIENT_SHARED_EQUITY"),
            {
                "kind": "SOURCE_GROUP",
                "at": 514,
                "group": _group(
                    event_id="event-514",
                    at=514,
                    ordinal=30,
                    kind="EXECUTION",
                    payload={
                        "namespace": "test",
                        "product_id": _PRODUCTS[0],
                        "external_execution_id": "fill-514",
                        "order_id": _FIRST_ORDER_ID,
                        "side": "LONG",
                        "price": "100",
                        "quantity_contracts": "0.5",
                        "liquidity": "SYNTHETIC_TAKER",
                        "matching_effective_at": 514,
                        "candidate_id": "candidate-514",
                        "source_id": "source-514",
                        "visible_at": 514,
                        "expected_account_version": 12,
                        "expected_order_version": 1,
                        "spec_version": "scale-spec-v1",
                        "rule_data_version": "scale-tier-v1",
                    },
                ),
            },
            {
                "kind": "SOURCE_GROUP",
                "at": 515,
                "group": _group(
                    event_id="event-515",
                    at=515,
                    ordinal=40,
                    kind="CANCEL_REQUEST",
                    parents=["event-514"],
                    payload={
                        "targets": [
                            {
                                "target_order_id": _FIRST_ORDER_ID,
                                "reason": "EXPLICIT_SCENARIO",
                            }
                        ]
                    },
                ),
            },
            {
                "kind": "SOURCE_GROUP",
                "at": 516,
                "group": _group(
                    event_id="event-516",
                    at=516,
                    ordinal=50,
                    kind="CANCEL_EFFECT",
                    parents=["event-515"],
                    payload={
                        "effects": [
                            {
                                "detecting_event_id": "event-515",
                                "target_order_id": _FIRST_ORDER_ID,
                                "reason": "EXPLICIT_SCENARIO",
                            }
                        ]
                    },
                ),
            },
            {
                "kind": "DELIVERY",
                "at": 517,
                "projection": {
                    "schema_version": "delivery_projection_v1",
                    "reference": {"namespace": "SOURCE", "fact_id": "event-516"},
                    "payload_kind": "TRANSPORT_ACK",
                    "occurrence_index": 0,
                    "schedule_sequence": 2,
                    "visible_at": 517,
                    "transport": {
                        "route": "WS",
                        "operation": "CANCEL",
                        "client_order_id": "C-501",
                        "code": "0",
                    },
                },
            },
        ]
    )
    return steps


def _payload() -> dict[str, Any]:
    configuration = _configuration()
    return {
        "schema_version": "spider_configured_plan_input_v1",
        "scenario_plan_id": "SPIDER_P2_CONFIGURED_SCALE_V1",
        "native_profile": "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1",
        "account_key": dict(_ACCOUNT),
        "terminal_policy": "SCHEDULED_MTM",
        "configuration": configuration,
        "configuration_sha256": hashlib.sha256(
            canonical_bytes(configuration)
        ).hexdigest(),
        "products": list(_PRODUCTS),
        "recipe": _recipe(),
    }


_PLAN_INPUT_BYTES = canonical_bytes(_payload())
_PLAN_INPUT_SHA256 = hashlib.sha256(_PLAN_INPUT_BYTES).hexdigest()


def _configured_scale_plan_input() -> dict[str, Any]:
    """Return a detached copy of the frozen input payload."""
    return cast(dict[str, Any], decode_canonical(_PLAN_INPUT_BYTES))
