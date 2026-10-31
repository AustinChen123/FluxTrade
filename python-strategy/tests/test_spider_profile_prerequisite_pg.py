"""Causal PostgreSQL checks for the optional pre-marker profile prerequisite."""

from decimal import Decimal
from hashlib import sha256
import os
import time
from typing import Any, cast

import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from src.core.adapters.okx_spider_profile import create_okx_spider_profile_prerequisite
from src.core.adapters.okx_spider_transport import (
    OkxSpiderBatchTransport,
    classify_okx_batch_response,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.orm_models import Order
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
    SpiderRuntimeTransmission,
)
from src.core.spider_management.runtime_owner import RuntimeScopeKey
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_financial_delivery_pg import (
    _install_owner,
    _saved_initial_place_request,
    _transport_enabled_owner,
)
from test_spider_operation_request_pg import operation_pg as _operation_pg
from test_spider_transmission_owner_pg import (
    _accepted_envelope,
    _execution_for_owner,
    _filled_event,
    _normal_n3_run,
    _saved_normal_place_group,
)

fresh_pg_db = _migration_fresh_pg_db
pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]

_ENVIRONMENT = "demo"
_ACCOUNT_REF = "demo-account-ref"
_UID = "demo-profile-uid"


def _step(values: list[str]) -> str:
    places = max(
        max(0, -int(Decimal(value).as_tuple().exponent)) for value in values
    )
    return format(Decimal(1).scaleb(-places), "f")


def _profile(request_payload: bytes, *, account_level: str = "3", instruments: object = ...):
    request = cast(dict[str, object], decode_canonical(request_payload))
    members = cast(list[dict[str, object]], request["members"])
    products_by_id: dict[str, dict[str, object]] = {}
    for member in members:
        product_id = cast(str, member["product_id"])
        wire = cast(dict[str, object], member["request_payload"])
        inst_id = cast(str, wire["instId"])
        product = products_by_id.setdefault(product_id, {
            "product_id": product_id,
            "instId": inst_id,
            "base_ccy": inst_id.removesuffix("-USDT-SWAP"),
            "ctVal": "1",
            "ctMult": "1",
            "lotSz": "1",
            "minSz": "1",
            "tickSz": "1",
        })
        if request["operation"] == "CANCEL":
            continue
        sizes = [
            cast(str, cast(dict[str, object], candidate["request_payload"])["sz"])
            for candidate in members if candidate["product_id"] == product_id
        ]
        product["lotSz"] = _step(sizes)
        product["minSz"] = product["lotSz"]
        prices = [
            cast(str, cast(dict[str, object], candidate["request_payload"])["px"])
            for candidate in members
            if candidate["product_id"] == product_id
            and cast(dict[str, object], candidate["request_payload"]).get("ordType") == "limit"
        ]
        if prices:
            product["tickSz"] = _step(prices)

    selection = {
        "scope": request["scope"],
        "executing_account_ref": request["executing_account_ref"],
        "expected_uid": _UID,
        "products": list(products_by_id.values()),
    }
    rows = [{
        "instId": product["instId"],
        "instType": "SWAP",
        "ctType": "linear",
        "settleCcy": "USDT",
        "ctValCcy": product["base_ccy"],
        "ctVal": product["ctVal"],
        "ctMult": product["ctMult"],
        "lotSz": product["lotSz"],
        "minSz": product["minSz"],
        "tickSz": product["tickSz"],
        "state": "live",
    } for product in products_by_id.values()]
    evidence = {
        "environment": "demo",
        "rest_host": "www.okx.com",
        "simulated_trading": True,
        "account_received_at_ms": time.time_ns() // 1_000_000,
        "instruments_received_at_ms": time.time_ns() // 1_000_000,
        "account": {"code": "0", "data": [{
            "uid": _UID,
            "acctLv": account_level,
            "posMode": "net_mode",
            "autoLoan": False,
        }]},
        "instruments": {"code": "0", "data": rows} if instruments is ... else instruments,
    }
    selection_bytes = canonical_bytes(selection)
    evidence_bytes = canonical_bytes(evidence)
    digest = sha256(canonical_bytes({
        "domain": "OKX_SPIDER_PROFILE_PREREQUISITE_V1",
        "selection": selection,
        "evidence": evidence,
    })).hexdigest()
    return create_okx_spider_profile_prerequisite(
        selection_bytes, evidence_bytes, digest
    )


class _CountingTransport:
    def __init__(self) -> None:
        self.place_calls = 0
        self.cancel_calls = 0

    def privatePostTradeBatchOrders(self, rows: list[dict[str, object]]) -> object:
        self.place_calls += 1
        return _accepted_envelope([
            {"wire_clOrdId": row["clOrdId"]} for row in rows
        ])

    def privatePostTradeCancelBatchOrders(self, rows: list[dict[str, object]]) -> object:
        self.cancel_calls += 1
        return {
            "code": "0",
            "data": [{"ordId": row["ordId"], "clOrdId": "", "sCode": "0"} for row in rows],
            "msg": "offline cancellation acknowledgement",
        }


def _snapshot(engine: Engine, scope: RuntimeScopeKey) -> tuple[object, ...]:
    with Session(engine) as session:
        runtime = session.execute(select(SpiderRuntimeScope).where(
            SpiderRuntimeScope.environment == scope.environment,
            SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
            SpiderRuntimeScope.strategy_id == scope.strategy_id,
        )).scalar_one()
        actions = tuple(session.execute(select(
            SpiderRuntimeAction.action_id,
            SpiderRuntimeAction.outcome_state,
            SpiderRuntimeAction.outcome_reference,
        ).where(
            SpiderRuntimeAction.strategy_id == scope.strategy_id,
        ).order_by(SpiderRuntimeAction.action_id)).all())
        return (
            runtime.last_tape_sequence,
            session.scalar(select(func.count()).select_from(SpiderRuntimeTransmission).where(
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id,
            )),
            session.scalar(select(func.count()).select_from(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id,
                SpiderRuntimeTapeIndex.kind == "SEND_MARKER",
            )),
            actions,
        )


@pytest.mark.parametrize("checker_kind", ["invalid_profile", "exception", "non_bool"])
def test_initial_profile_rejection_precedes_marker_and_transport(
    operation_pg: Engine, checker_kind: str
) -> None:
    engine = operation_pg
    owner, scope, _operation_id, _manager, _prepared, _initial, *_ = _install_owner(
        engine, environment=_ENVIRONMENT, execution_scope_id=_ACCOUNT_REF
    )
    source_group, request_payload, _members = _saved_initial_place_request(engine, scope)
    if checker_kind == "invalid_profile":
        profile_checker = _profile(request_payload, account_level="2")
    elif checker_kind == "exception":
        def exception_checker(_payload: bytes, _now_ms: int) -> bool:
            raise RuntimeError("must not escape owner")

        profile_checker = exception_checker
    else:
        def non_bool_checker(_payload: bytes, _now_ms: int) -> object:
            return 1

        profile_checker = non_bool_checker
    enabled = _transport_enabled_owner(
        owner, scope, profile_prerequisite=profile_checker
    )
    transport = _CountingTransport()
    before = _snapshot(engine, scope)
    result = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, transport)),
    )
    assert result.disposition == "REJECTED"
    assert transport.place_calls == transport.cancel_calls == 0
    assert _snapshot(engine, scope) == before


def test_initial_valid_profile_sends_once_and_saved_readback_bypasses_new_checker(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    owner, scope, _operation_id, _manager, _prepared, _initial, *_ = _install_owner(
        engine, environment=_ENVIRONMENT, execution_scope_id=_ACCOUNT_REF
    )
    source_group, request_payload, _members = _saved_initial_place_request(engine, scope)
    observed_times: list[int] = []
    prepared = _profile(request_payload)

    def profile_checker(payload: bytes, now_ms: int) -> bool:
        observed_times.append(now_ms)
        return prepared(payload, now_ms)

    enabled = _transport_enabled_owner(
        owner, scope, profile_prerequisite=profile_checker
    )
    transport = _CountingTransport()
    sent = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, transport)),
    )
    assert sent.disposition == "RECORDED"
    assert transport.place_calls == 1 and len(observed_times) == 1
    assert type(observed_times[0]) is int

    rejecting = _transport_enabled_owner(
        owner, scope, profile_prerequisite=lambda _payload, _now_ms: (_ for _ in ()).throw(
            RuntimeError("saved result must bypass checker")
        ),
    )
    repeated = rejecting.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, transport)),
    )
    assert repeated == sent
    assert transport.place_calls == 1


def _record_demo_full_decision(engine: Engine, mock_clock, mock_exchange_adapter):
    owner, scope, operation_id, order_manager, *_ = _install_owner(
        engine,
        admitted_run=_normal_n3_run(),
        environment=_ENVIRONMENT,
        execution_scope_id=_ACCOUNT_REF,
    )
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    target = next(
        member for member in members
        if cast(dict[str, object], member["request_payload"]).get("side") == "buy"
        and cast(dict[str, object], member["request_payload"]).get("px") == "50"
    )
    order = order_manager.repo.get_order(cast(str, target["local_order_id"]))
    assert order is not None
    execution: ExecutionEngine = _execution_for_owner(
        enabled, engine, mock_clock, mock_exchange_adapter, observe_order_events=True
    )
    filled_wire_id = cast(str, target["wire_clOrdId"])

    class FillBeforeAck:
        def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            assert execution.process_exchange_order_event(_filled_event(
                order,
                provider_id="demo-profile-initial-order",
                trade_id="demo-profile-initial-trade",
            ))["action"] == "applied"
            return {
                "code": "0",
                "data": [
                    {
                        "clOrdId": member["wire_clOrdId"],
                        "ordId": (
                            "demo-profile-initial-order"
                            if member["wire_clOrdId"] == filled_wire_id
                            else f"demo-profile-order-{ordinal}"
                        ),
                        "sCode": "0",
                    }
                    for ordinal, member in enumerate(members)
                ],
                "msg": "consistent offline acknowledgement",
            }

        def privatePostTradeCancelBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            raise AssertionError("initial PLACE cannot call cancel endpoint")

    result = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, FillBeforeAck())),
    )
    assert result.disposition == "RECORDED"
    with Session(engine) as session:
        rows = session.execute(select(SpiderRuntimeDelivery).where(
            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            SpiderRuntimeDelivery.decision_payload.is_not(None),
        ).order_by(SpiderRuntimeDelivery.sequence)).scalars().all()
        full = next(row for row in rows if type(row.decision_payload) is bytes and
                    cast(dict[str, object], decode_canonical(row.decision_payload))[
                        "full_callback_key"] is not None)
        full_delivery_id = full.delivery_id

        assert type(full.decision_payload) is bytes
        decision = cast(dict[str, object], decode_canonical(full.decision_payload))
        cancel_candidate = next(
            row for row in cast(list[dict[str, object]], decision["candidates"])
            if row.get("operation") == "CANCEL" and row.get("disposition") == "PLANNED"
        )
        cancel_action = session.get(
            SpiderRuntimeAction, cast(str, cancel_candidate["action_id"])
        )
        assert cancel_action is not None and cancel_action.parent_action_id is not None
        target_action = session.get(SpiderRuntimeAction, cancel_action.parent_action_id)
        assert target_action is not None and target_action.outcome_reference is not None
        target_order_id = target_action.local_order_id
        transmission = session.get(SpiderRuntimeTransmission, target_action.outcome_reference)
        assert transmission is not None and transmission.response_payload is not None
        outcomes = classify_okx_batch_response(
            bytes(transmission.request_payload), bytes(transmission.response_payload)
        )
        assert outcomes is not None
        outcome = outcomes[target_action.action_id]
        assert outcome[0] == "ACCEPTED" and outcome[2] is not None
        provider_order_id = outcome[2]

    ack_order = order_manager.repo.get_order(target_order_id)
    assert ack_order is not None
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=ack_order.product_id,
        client_order_id=ack_order.client_order_id,
        exchange_order_id=provider_order_id,
        cumulative_filled_quantity=Decimal("0"),
    ))["action"] == "applied"
    return owner, scope, operation_id, full_delivery_id


def test_normal_place_and_cancel_use_profile_before_marker_but_cancel_ignores_specs(
    operation_pg: Engine, mock_clock, mock_exchange_adapter
) -> None:
    engine = operation_pg
    owner, scope, _operation_id, delivery_id = _record_demo_full_decision(
        engine, mock_clock, mock_exchange_adapter
    )
    _group_id, place_payload, place_members = _saved_normal_place_group(
        engine, scope, delivery_id
    )
    before_place_rejection = _snapshot(engine, scope)
    rejected_transport = _CountingTransport()

    def reject_place(_payload: bytes, _now_ms: int) -> bool:
        return False

    rejected_place = _transport_enabled_owner(
        owner, scope, profile_prerequisite=reject_place
    )._transmit_normal_place_group(
        scope, place_payload, OkxSpiderBatchTransport(cast(Any, rejected_transport))
    )
    assert rejected_place.disposition == "REJECTED"
    assert rejected_transport.place_calls == rejected_transport.cancel_calls == 0
    assert _snapshot(engine, scope) == before_place_rejection

    place_checker = _profile(place_payload)
    place_owner = _transport_enabled_owner(
        owner, scope, profile_prerequisite=place_checker
    )
    transport = _CountingTransport()
    place_result = place_owner._transmit_normal_place_group(
        scope, place_payload, OkxSpiderBatchTransport(cast(Any, transport))
    )
    assert place_result.disposition == "RECORDED"
    assert transport.place_calls == 1
    repeated_place = _transport_enabled_owner(
        owner,
        scope,
        profile_prerequisite=lambda _payload, _now_ms: (_ for _ in ()).throw(
            RuntimeError("saved normal result must bypass checker")
        ),
    )._transmit_normal_place_group(
        scope, place_payload, OkxSpiderBatchTransport(cast(Any, transport))
    )
    assert repeated_place == place_result
    assert transport.place_calls == 1

    with Session(engine) as session:
        delivery = session.execute(select(SpiderRuntimeDelivery).where(
            SpiderRuntimeDelivery.environment == scope.environment,
            SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            SpiderRuntimeDelivery.delivery_id == delivery_id,
        )).scalar_one_or_none()
        assert delivery is not None and delivery.decision_payload is not None
        decision = cast(dict[str, object], decode_canonical(bytes(delivery.decision_payload)))
        cancel_candidate = next(
            row for row in cast(list[dict[str, object]], decision["candidates"])
            if row.get("operation") == "CANCEL" and row.get("disposition") == "PLANNED"
        )
        cancel_action = session.get(
            SpiderRuntimeAction, cast(str, cancel_candidate["action_id"])
        )
        assert cancel_action is not None and cancel_action.parent_action_id is not None
        target_action = session.get(SpiderRuntimeAction, cancel_action.parent_action_id)
        assert target_action is not None
        binding = session.get(SpiderRuntimeBinding, target_action.action_id)
        target_order = session.get(Order, target_action.local_order_id)
        runtime = session.execute(select(SpiderRuntimeScope).where(
            SpiderRuntimeScope.strategy_id == scope.strategy_id,
        )).scalar_one()
        plan = session.get(SpiderRuntimePlan, runtime.initial_plan_id)
        assert binding is not None and target_order is not None
        assert plan is not None
        intent = cast(dict[str, object], target_order.intent_payload)
        source = cast(dict[str, object], decode_canonical(bytes(target_action.original_payload)))
        cancel_member = {
            "cancel_action_id": cancel_action.action_id,
            "product_id": target_action.product_id,
            "target_place_action_id": target_action.action_id,
            "target_local_order_id": target_order.id,
            "target_binding_sha256": sha256(bytes.fromhex(cast(
                str, intent["provider_binding_bytes_hex"]
            ))).hexdigest(),
            "target_wire_clOrdId": intent["provider_wire_client_order_id"],
            "target_ordId": target_order.exchange_order_id,
            "request_payload": {
                "instId": source["instId"],
                "ordId": target_order.exchange_order_id,
            },
        }
        cancel_group_id = sha256(canonical_bytes({
            "domain": "SPIDER_TRANSMISSION_GROUP_V1",
            "scope": {
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "strategy_id": scope.strategy_id,
            },
            "operation_id": runtime.current_operation_id,
            "plan_id": plan.plan_id,
            "generation": runtime.generation,
            "configuration_sha256": runtime.configuration_sha256,
            "operation": "CANCEL",
            "ordered_action_ids": [cancel_action.action_id],
        })).hexdigest()
        body: dict[str, object] = {
            "schema_version": "SPIDER_TRANSPORT_BATCH_V1",
            "scope": {
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "strategy_id": scope.strategy_id,
            },
            "executing_account_ref": scope.execution_scope_id,
            "transmission_group_id": cancel_group_id,
            "operation": "CANCEL",
            "members": [cancel_member],
        }
        body["payload_sha256"] = sha256(canonical_bytes(body)).hexdigest()
        cancel_payload = canonical_bytes(body)

    cancel_checker = _profile(cancel_payload, instruments=None)
    before_cancel_rejection = _snapshot(engine, scope)

    def reject_cancel(_payload: bytes, _now_ms: int) -> bool:
        return False

    rejected_cancel = _transport_enabled_owner(
        owner, scope, profile_prerequisite=reject_cancel
    )._transmit_normal_cancel_group(
        scope, cancel_payload, OkxSpiderBatchTransport(cast(Any, transport))
    )
    assert rejected_cancel.disposition == "REJECTED"
    assert transport.cancel_calls == 0
    assert _snapshot(engine, scope) == before_cancel_rejection

    cancel_owner = _transport_enabled_owner(
        owner, scope, profile_prerequisite=cancel_checker
    )
    cancel_result = cancel_owner._transmit_normal_cancel_group(
        scope, cancel_payload, OkxSpiderBatchTransport(cast(Any, transport))
    )
    assert cancel_result.disposition == "RECORDED"
    assert transport.cancel_calls == 1
    repeated_cancel = _transport_enabled_owner(
        owner,
        scope,
        profile_prerequisite=lambda _payload, _now_ms: (_ for _ in ()).throw(
            RuntimeError("saved cancel result must bypass checker")
        ),
    )._transmit_normal_cancel_group(
        scope, cancel_payload, OkxSpiderBatchTransport(cast(Any, transport))
    )
    assert repeated_cancel == cancel_result
    assert transport.cancel_calls == 1
