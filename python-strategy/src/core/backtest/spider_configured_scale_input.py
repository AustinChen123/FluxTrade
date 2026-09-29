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
_DOGE_ORDER_ID = "scenario-order-v1:[e8, dd, fc, 11, a7, a3, d8, 1e, 93, 5c, 69, dc, 3e, f8, 71, 12, 83, 76, 45, 7d, 9f, 76, 54, db, a0, c0, 3b, ff, ae, 79, a7, d5]"


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


def _execution_step(at: int, parent: str | None) -> dict[str, Any]:
    event_id = f"event-{at}"
    return {
        "kind": "SOURCE_GROUP",
        "at": at,
        "schedule_sequence": 0,
        "group": _group(
            event_id=event_id,
            at=at,
            ordinal=30,
            kind="EXECUTION",
            parents=[parent] if parent is not None else [],
            payload={
                "namespace": "test",
                "product_id": "DOGE-USDT-SWAP",
                "external_execution_id": f"fill-{at}",
                "order_id": _DOGE_ORDER_ID,
                "side": "LONG",
                "price": "100",
                "quantity_contracts": "0.5",
                "liquidity": "SYNTHETIC_TAKER",
                "matching_effective_at": at,
                "candidate_id": f"candidate-{at}",
                "source_id": f"source-{at}",
                "visible_at": at,
                "expected_account_version": 15 if at == 518 else 16,
                "expected_order_version": 1 if at == 518 else 2,
                "spec_version": "scale-spec-v1",
                "rule_data_version": "scale-tier-v1",
            },
        ),
    }


def _poll_stage(
    poll_id: str,
    key: str,
    kind: str,
    fixture: str,
    sequence: int,
    at: int,
) -> dict[str, Any]:
    snapshot_id = f"{poll_id}{key}"
    payload_kind = {
        "TRADING": "TRADING_SNAPSHOT",
        "POSITIONS": "POSITION_SNAPSHOT",
        "OPEN_ORDERS": "OPEN_ORDER_SNAPSHOT",
    }[kind]
    return {
        "capture_sequence": sequence,
        "snapshot_request": {
            "schema_version": "snapshot_request_v1",
            "account_key": dict(_ACCOUNT),
            "snapshot_id": snapshot_id,
            "snapshot_kind": kind,
            "capture_mode": "FROZEN_POLL_FIXTURE",
            "fixture_key": fixture,
            "captured_at": at,
            "continuation_id": poll_id,
        },
        "delivery_projection": {
            "schema_version": "delivery_projection_v1",
            "reference": {"namespace": "SNAPSHOT", "fact_id": snapshot_id},
            "payload_kind": payload_kind,
            "occurrence_index": 0,
            "schedule_sequence": sequence,
            "visible_at": at,
            "continuation_id": poll_id,
        },
    }


def _poll_plan(
    poll_id: str,
    issued_at: int,
    sequence: int,
    start: int,
    trading_fixture: str,
) -> dict[str, Any]:
    return {
        "account_key": dict(_ACCOUNT),
        "poll_id": poll_id,
        "issued_at": issued_at,
        "continuation_id": poll_id,
        "trading": _poll_stage(
            poll_id, "trading", "TRADING", trading_fixture, sequence, start
        ),
        "positions": _poll_stage(
            poll_id,
            "positions",
            "POSITIONS",
            "POLL_POSITIONS_EMPTY",
            sequence + 1,
            start + 2,
        ),
        "open_orders": _poll_stage(
            poll_id,
            "open_orders",
            "OPEN_ORDERS",
            "POLL_OPEN_ORDERS_EMPTY",
            sequence + 2,
            start + 4,
        ),
    }


def _empty_callback_plan(identity: str) -> dict[str, Any]:
    return {
        "delivery_id": identity,
        "expected_policy_events": [],
        "financial_items": [],
        "market_requests": [],
    }


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
            _execution_step(518, None),
            _execution_step(519, "event-518"),
            {
                "kind": "DELIVERY",
                "at": 100018,
                "projection": {
                    "schema_version": "delivery_projection_v1",
                    "reference": {"namespace": "SOURCE", "fact_id": "event-518"},
                    "payload_kind": "EXECUTION_FACT",
                    "occurrence_index": 0,
                    "schedule_sequence": 6,
                    "visible_at": 100018,
                },
            },
            {
                "kind": "POLL_BEGIN",
                "at": 100000,
                "plan": _poll_plan("Q1", 100000, 0, 100020, "POLL_Q1_S1"),
            },
            {
                "kind": "POLL_BEGIN",
                "at": 100001,
                "plan": _poll_plan("Q2", 100001, 3, 100010, "POLL_Q2_S2"),
            },
        ]
    )
    for step in steps:
        if step["kind"] == "SOURCE_GROUP":
            step.setdefault("schedule_sequence", 0)
    return steps


def _callback_plans() -> list[dict[str, Any]]:
    return [
        _empty_callback_plan(
            "0a8a37271c61a66b240643fc8d9f88ba6e6c534b898cff02906bedd665f65a5a"
        ),
        _empty_callback_plan(
            "1bb9face9ceb3ebd7ce8b6a0563bd98af203dd584cebef5d13ecfbe34399a126"
        ),
        _empty_callback_plan(
            "2ea7684018d26b983bdaeb77cf3328199fd1cdb8275aa5baa932eac5e646a5be"
        ),
        _empty_callback_plan(
            "0e10645813c9635ae226bbb928a2b12a97b553a5588da4486e4335bfe375a31e"
        ),
        _empty_callback_plan(
            "8a08c04f06607365d119435cdd419b4d4a5a31bc865f8065dcd8f7b68440d3cc"
        ),
        _empty_callback_plan(
            "abb4b7b0a891762cef3459945e4f4ade4255777affd4854eb89171798590617d"
        ),
        _empty_callback_plan(
            "10f5772ed0c36142e315e24201395e64f7b0740dce530b779483fff2eff54c74"
        ),
        _empty_callback_plan(
            "aeb806c3647e4e0fe89139d512fc0d5e71e5904f1e4edf978130c47bdfa319a1"
        ),
    ]


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
        "callback_plans": _callback_plans(),
    }


_PLAN_INPUT_BYTES = canonical_bytes(_payload())
_PLAN_INPUT_SHA256 = hashlib.sha256(_PLAN_INPUT_BYTES).hexdigest()


def _configured_scale_plan_input() -> dict[str, Any]:
    """Return a detached copy of the frozen input payload."""
    return cast(dict[str, Any], decode_canonical(_PLAN_INPUT_BYTES))
