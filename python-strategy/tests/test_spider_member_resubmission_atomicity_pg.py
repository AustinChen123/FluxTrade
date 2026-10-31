"""Actual PostgreSQL atomicity and independent-owner race proofs for RESUBMIT."""

from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from hashlib import sha256
import os
from threading import Barrier
from typing import Any, cast

import pytest
from sqlalchemy import Engine, event, func, select
from sqlalchemy.orm import Session

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.orm_models import Order, Trade
from src.core.spider_management.member_resubmission import (
    MemberResubmissionRequest,
    control_receipt_id as expected_control_receipt_id,
)
from src.core.spider_management.orm import (
    SpiderControlReceipt,
    SpiderMemberResubmission,
    SpiderMemberResubmissionRequest as SavedMemberResubmissionRequest,
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
    SpiderRuntimeTransmission,
)
from src.core.spider_management.runtime_owner import RuntimeScopeKey
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_control_command_pg import _command, _dispatch
from test_spider_financial_delivery_pg import (
    _install_owner,
    _saved_initial_place_request,
    _transport_enabled_owner,
)
from test_spider_operation_request_pg import operation_pg as _operation_pg
from test_spider_transmission_owner_pg import (
    _OfflinePlaceClient,
    _accepted_envelope,
    _run_with_initial_order_count,
)
from src.core.adapters.okx_spider_transport import OkxSpiderBatchTransport

fresh_pg_db = _migration_fresh_pg_db
operation_pg = _operation_pg
pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


def _request_for_target(
    engine: Engine,
    scope: RuntimeScopeKey,
    target_action_id: str,
    key: str,
) -> MemberResubmissionRequest:
    with Session(engine) as session:
        runtime = session.get(
            SpiderRuntimeScope,
            (scope.environment, scope.execution_scope_id, scope.strategy_id),
        )
        assert runtime is not None and runtime.initial_plan_id is not None
        request = MemberResubmissionRequest.from_object({
            "schema_version": "spider_member_resubmission_request_v1",
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "strategy_id": scope.strategy_id,
            "actor": "operator",
            "idempotency_key": key,
            "expected_version": runtime.state_version,
            "generation": runtime.generation,
            "configuration_sha256": runtime.configuration_sha256,
            "initial_plan_id": runtime.initial_plan_id,
            "target_action_id": target_action_id,
        })
    return request


def _unsent_owner(engine: Engine):
    owner, scope, operation_id, manager, _prepared, _initial, *_ = _install_owner(
        engine, admitted_run=_run_with_initial_order_count(1)
    )
    enabled = _transport_enabled_owner(owner, scope)
    source_group, source_payload, members = _saved_initial_place_request(engine, scope)
    assert members
    return owner, enabled, scope, operation_id, manager, source_group, source_payload, members[0]


def _record_request(owner, scope, engine, target_action_id: str, key: str):
    request = _request_for_target(engine, scope, target_action_id, key)
    assert owner.record_member_resubmission_request(request.canonical_request_bytes) is not None
    return request


def _snapshot(engine: Engine, scope: RuntimeScopeKey) -> tuple[object, ...]:
    """Snapshot durable attempt, source, financial, tape and cursor state."""
    with Session(engine) as session:
        runtime = session.get(
            SpiderRuntimeScope,
            (scope.environment, scope.execution_scope_id, scope.strategy_id),
        )
        assert runtime is not None
        plan_id = runtime.initial_plan_id
        assert plan_id is not None

        def mapped(model, where) -> tuple[tuple[object, ...], ...]:
            rows = session.execute(select(model).where(where)).scalars().all()
            keys = tuple(column.key for column in model.__table__.columns)
            return tuple(sorted(
                (tuple(getattr(row, key) for key in keys) for row in rows),
                key=repr,
            ))

        actions = mapped(SpiderRuntimeAction, SpiderRuntimeAction.plan_id == plan_id)
        bindings = mapped(
            SpiderRuntimeBinding,
            (SpiderRuntimeBinding.environment == scope.environment)
            & (SpiderRuntimeBinding.execution_scope_id == scope.execution_scope_id)
            & (SpiderRuntimeBinding.strategy_id == scope.strategy_id),
        )
        relations = mapped(
            SpiderMemberResubmission,
            (SpiderMemberResubmission.environment == scope.environment)
            & (SpiderMemberResubmission.execution_scope_id == scope.execution_scope_id)
            & (SpiderMemberResubmission.strategy_id == scope.strategy_id),
        )
        requests = mapped(
            SavedMemberResubmissionRequest,
            (SavedMemberResubmissionRequest.environment == scope.environment)
            & (SavedMemberResubmissionRequest.execution_scope_id == scope.execution_scope_id)
            & (SavedMemberResubmissionRequest.strategy_id == scope.strategy_id),
        )
        tape = mapped(
            SpiderRuntimeTapeIndex,
            (SpiderRuntimeTapeIndex.environment == scope.environment)
            & (SpiderRuntimeTapeIndex.execution_scope_id == scope.execution_scope_id)
            & (SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id),
        )
        receipts = mapped(
            SpiderControlReceipt,
            (SpiderControlReceipt.environment == scope.environment)
            & (SpiderControlReceipt.execution_scope_id == scope.execution_scope_id)
            & (SpiderControlReceipt.strategy_id == scope.strategy_id),
        )
        deliveries = mapped(
            SpiderRuntimeDelivery,
            (SpiderRuntimeDelivery.environment == scope.environment)
            & (SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id)
            & (SpiderRuntimeDelivery.strategy_id == scope.strategy_id),
        )
        transmissions = mapped(
            SpiderRuntimeTransmission,
            (SpiderRuntimeTransmission.environment == scope.environment)
            & (SpiderRuntimeTransmission.execution_scope_id == scope.execution_scope_id)
            & (SpiderRuntimeTransmission.strategy_id == scope.strategy_id),
        )
        order_ids = [row[0] for row in session.execute(
            select(SpiderRuntimeAction.local_order_id).where(
                SpiderRuntimeAction.plan_id == plan_id
            )
        ).all()]
        order_facts = tuple(session.execute(select(
            Order.id, Order.product_id, Order.side, Order.type, Order.price,
            Order.quantity, Order.status, Order.exchange_order_id,
            Order.filled_quantity, Order.filled_price,
        ).where(Order.id.in_(order_ids)).order_by(Order.id)).all())
        trade_facts = tuple(session.execute(select(
            Trade.id, Trade.order_id, Trade.exchange_trade_id, Trade.quantity,
            Trade.price, Trade.fee, Trade.fee_asset,
        ).where(Trade.order_id.in_(order_ids)).order_by(Trade.id)).all())
        return (
            runtime.last_received_sequence,
            runtime.last_consumed_sequence,
            runtime.last_tape_sequence,
            runtime.state_version,
            runtime.source_checkpoint_sha256,
            bytes(runtime.source_checkpoint),
            actions,
            bindings,
            relations,
            requests,
            tape,
            receipts,
            deliveries,
            transmissions,
            order_facts,
            trade_facts,
        )


def _command_for(scope: RuntimeScopeKey, request: MemberResubmissionRequest) -> dict[str, Any]:
    message = _command(
        scope, key=request.idempotency_key, version=request.expected_version
    )
    message["command"] = "RESUBMIT"
    message["params"]["actor"] = request.actor
    return message


def _child_place_request(engine: Engine, scope: RuntimeScopeKey, action_id: str) -> bytes:
    with Session(engine) as session:
        runtime = session.get(
            SpiderRuntimeScope,
            (scope.environment, scope.execution_scope_id, scope.strategy_id),
        )
        assert runtime is not None
        action = session.get(SpiderRuntimeAction, action_id)
        assert action is not None
        order = session.get(Order, action.local_order_id)
        assert order is not None and type(order.intent_payload) is dict
        intent = cast(dict[str, object], order.intent_payload)
        binding_bytes = bytes.fromhex(cast(str, intent["provider_binding_bytes_hex"]))
        wire_id = cast(str, intent["provider_wire_client_order_id"])
        original = cast(dict[str, object], decode_canonical(bytes(action.original_payload)))
        request_order = dict(original)
        request_order["clOrdId"] = wire_id
        group_id = sha256(canonical_bytes({
            "domain": "SPIDER_TRANSMISSION_GROUP_V1",
            "scope": {
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "strategy_id": scope.strategy_id,
            },
            "operation_id": runtime.current_operation_id,
            "plan_id": runtime.initial_plan_id,
            "generation": runtime.generation,
            "configuration_sha256": runtime.configuration_sha256,
            "operation": "PLACE",
            "ordered_action_ids": [action.action_id],
        })).hexdigest()
        member = {
            "action_id": action.action_id,
            "product_id": action.product_id,
            "local_order_id": order.id,
            "binding_sha256": sha256(binding_bytes).hexdigest(),
            "wire_clOrdId": wire_id,
            "ordId": order.exchange_order_id,
            "request_payload": request_order,
        }
    body: dict[str, object] = {
        "schema_version": "SPIDER_TRANSPORT_BATCH_V1",
        "scope": {
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "strategy_id": scope.strategy_id,
        },
        "executing_account_ref": scope.execution_scope_id,
        "transmission_group_id": group_id,
        "operation": "PLACE",
        "members": [member],
    }
    body["payload_sha256"] = sha256(canonical_bytes(body)).hexdigest()
    return canonical_bytes(body)


def test_unsent_member_resubmits_child_only_and_old_group_cannot_send(operation_pg: Engine) -> None:
    engine = operation_pg
    owner, enabled, scope, _operation_id, manager, source_group, source_payload, original = _unsent_owner(engine)
    target_id = cast(str, original["action_id"])
    source_group_id = cast(
        str, cast(dict[str, object], decode_canonical(source_payload))["transmission_group_id"]
    )
    before = _snapshot(engine, scope)
    assert before[13] == ()
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(SpiderRuntimeTapeIndex).where(
            SpiderRuntimeTapeIndex.kind == "SEND_MARKER"
        )) == 0
    request = _record_request(owner, scope, engine, target_id, "r2-unsent-explicit")
    with Session(engine) as session:
        original_action = session.get(SpiderRuntimeAction, target_id)
        original_order = session.get(Order, cast(str, original["local_order_id"]))
        assert original_action is not None and original_order is not None
        assert original_action.outcome_state == "UNSENT"
        original_terms = (original_order.product_id, original_order.side, original_order.type,
                          original_order.price, original_order.quantity, original_order.status)

    result = _dispatch(enabled, _command_for(scope, request))
    assert result.success is True
    proof = enabled._member_resubmission_readback(scope, request)
    assert proof is not None and proof["disposition"] == "PLANNED"
    child_id = cast(str, proof["resubmission_action_id"])
    assert proof["initial_member_id"] == target_id
    child_payload = _child_place_request(engine, scope, child_id)

    old_client = _OfflinePlaceClient({"code": "0", "data": [], "msg": "old action must not send"})
    old_result = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, source_payload,
        OkxSpiderBatchTransport(cast(Any, old_client)),
    )
    assert old_result.disposition in {"REJECTED", "UNKNOWN"}
    assert old_client.calls == 0

    child_body = cast(dict[str, object], decode_canonical(child_payload))
    child_members = cast(list[object], child_body["members"])
    child_member = cast(dict[str, object], child_members[0])
    child_client = _OfflinePlaceClient(_accepted_envelope([cast(dict[str, object], child_member)]))
    sent = enabled._transmit_normal_place_group(
        scope, child_payload, OkxSpiderBatchTransport(cast(Any, child_client))
    )
    assert sent.disposition == "RECORDED"
    assert child_client.calls == 1
    with Session(engine) as session:
        relation = session.get(SpiderMemberResubmission, request.request_id)
        child = session.get(SpiderRuntimeAction, child_id)
        child_binding = session.get(SpiderRuntimeBinding, child_id)
        child_order = None if child is None else session.get(Order, child.local_order_id)
        old_action = session.get(SpiderRuntimeAction, target_id)
        old_order = session.get(Order, cast(str, original["local_order_id"]))
        assert relation is not None and child is not None and child_binding is not None
        assert child_order is not None and old_action is not None and old_order is not None
        assert (relation.initial_member_id, relation.target_action_id,
                relation.resubmission_action_id) == (target_id, target_id, child_id)
        assert old_action.outcome_state == "UNSENT"
        assert (old_order.product_id, old_order.side, old_order.type,
                old_order.price, old_order.quantity, old_order.status) == original_terms
        assert child.parent_action_id == target_id
        child_original = cast(
            dict[str, object], decode_canonical(bytes(child.original_payload))
        )
        assert child_order.price == Decimal(cast(str, child_original["px"]))
        assert child_order.quantity == Decimal(cast(str, child_original["sz"]))
        assert child_order.id != old_order.id
        assert child_binding.action_id == child_id
        assert child.outcome_state == "ACCEPTED"
        saved_transmissions = session.execute(select(SpiderRuntimeTransmission)).scalars().all()
        assert len(saved_transmissions) == 1
        assert saved_transmissions[0].transmission_group_id != source_group_id
        assert session.scalar(select(func.count()).select_from(SpiderMemberResubmission)) == 1
        assert session.scalar(select(func.count()).select_from(SpiderRuntimeTapeIndex).where(
            SpiderRuntimeTapeIndex.kind == "RESUBMIT"
        )) == 1


@pytest.mark.parametrize(
    "fault",
    [
        "rollback_after_flush",
        "lost_commit_with_available_read",
        "lost_commit_and_unavailable_read",
    ],
)
def test_member_resubmission_txb_failure_proves_absence_or_same_commit(
    operation_pg: Engine,
    fault: str,
) -> None:
    engine = operation_pg
    owner, enabled, scope, _operation_id, _manager, _group, _payload, original = _unsent_owner(engine)
    request = _record_request(owner, scope, engine, cast(str, original["action_id"]), f"r2-{fault}")
    baseline = _snapshot(engine, scope)
    fired = {"count": 0, "saw_flushed_attempt": False, "committed": False}

    def rollback_after_flush(session: Session, _flush_context: object) -> None:
        if fault != "rollback_after_flush" or fired["count"]:
            return
        relation_count = session.connection().execute(
            select(func.count()).select_from(SpiderMemberResubmission).where(
                SpiderMemberResubmission.request_id == request.request_id
            )
        ).scalar_one()
        if relation_count == 1:
            action = session.connection().execute(
                select(SpiderRuntimeAction.action_id).where(
                    SpiderRuntimeAction.parent_action_id == original["action_id"]
                )
            ).scalar_one_or_none()
            binding = session.connection().execute(
                select(func.count()).select_from(SpiderRuntimeBinding).where(
                    SpiderRuntimeBinding.action_id == action
                )
            ).scalar_one() if action is not None else 0
            fired["saw_flushed_attempt"] = action is not None and binding == 1
            if fired["saw_flushed_attempt"]:
                fired["count"] += 1
                raise RuntimeError("injected after action, binding and relation flush")

    def arm_lost_commit(session: Session, _flush_context: object, _instances: object) -> None:
        if fault in {"lost_commit_with_available_read", "lost_commit_and_unavailable_read"} and any(
            isinstance(row, SpiderMemberResubmission) for row in session.new
        ):
            session.info["r2_resubmit_commit"] = True

    def lose_commit_response(session: Session) -> None:
        if fault in {"lost_commit_with_available_read", "lost_commit_and_unavailable_read"} and session.info.pop(
            "r2_resubmit_commit", False
        ):
            fired["count"] += 1
            fired["committed"] = True
            raise RuntimeError("injected lost RESUBMIT commit response")

    original_readback = enabled._member_resubmission_readback

    def temporarily_unavailable_readback(
        scope: RuntimeScopeKey,
        request: MemberResubmissionRequest,
    ) -> dict[str, object] | None:
        if fired["committed"]:
            return None
        return original_readback(scope, request)

    if fault == "rollback_after_flush":
        event.listen(Session, "after_flush_postexec", rollback_after_flush)
    else:
        event.listen(Session, "before_flush", arm_lost_commit)
        event.listen(Session, "after_commit", lose_commit_response)
        if fault == "lost_commit_and_unavailable_read":
            enabled._member_resubmission_readback = temporarily_unavailable_readback
    try:
        failed = _dispatch(enabled, _command_for(scope, request))
    finally:
        if fault == "rollback_after_flush":
            event.remove(Session, "after_flush_postexec", rollback_after_flush)
        else:
            event.remove(Session, "before_flush", arm_lost_commit)
            event.remove(Session, "after_commit", lose_commit_response)
            if fault == "lost_commit_and_unavailable_read":
                enabled._member_resubmission_readback = original_readback

    assert fired["count"] == 1
    failed_data = cast(dict[str, object], failed.data)
    if fault == "rollback_after_flush":
        assert failed.success is False
        assert fired["saw_flushed_attempt"] is True
        assert failed_data["disposition"] == "UNKNOWN"
        assert _snapshot(engine, scope) == baseline
        cold = _transport_enabled_owner(owner, scope)
        cold_proof = cold.member_resubmission_readback(request.canonical_request_bytes)
        assert cold_proof is not None and cold_proof["disposition"] == "RECORDED"
    else:
        assert fired["committed"] is True
        proof_unavailable = fault == "lost_commit_and_unavailable_read"
        assert failed.success is not proof_unavailable
        assert failed_data["disposition"] == ("UNKNOWN" if proof_unavailable else "ACCEPTED")
        committed_snapshot = _snapshot(engine, scope)
        assert committed_snapshot != baseline
        restored_proof = enabled.member_resubmission_readback(request.canonical_request_bytes)
        assert restored_proof is not None and restored_proof["disposition"] == "PLANNED"
        cold = _transport_enabled_owner(owner, scope)
        retry = _dispatch(cold, _command_for(scope, request))
        assert retry.success is True
        proof = cold._member_resubmission_readback(scope, request)
        assert proof is not None and proof["disposition"] == "PLANNED"
        assert _snapshot(engine, scope) == committed_snapshot
        assert proof["resubmission_action_id"] == restored_proof["resubmission_action_id"]
        assert proof["local_order_id"] == restored_proof["local_order_id"]
        assert proof["resubmission_action_id"] is not None
        assert proof["local_order_id"] is not None


def test_different_keys_from_independent_owners_share_one_frontier_attempt(operation_pg: Engine) -> None:
    engine = operation_pg
    owner, enabled_a, scope, _operation_id, _manager, _group, _payload, original = _unsent_owner(engine)
    enabled_b = _transport_enabled_owner(owner, scope)
    target_id = cast(str, original["action_id"])
    request_a = _record_request(owner, scope, engine, target_id, "r2-race-a")
    request_b = _record_request(owner, scope, engine, target_id, "r2-race-b")
    baseline = _snapshot(engine, scope)
    rendezvous = Barrier(2)
    row_lock_arrivals = {"count": 0}

    def wait_before_runtime_lock(
        _conn, _cursor, statement: str, _parameters, _context, _executemany
    ) -> None:
        normalized = statement.lower()
        if "for update" in normalized and "spider_runtime_scope" in normalized:
            row_lock_arrivals["count"] += 1
            rendezvous.wait(timeout=10)

    event.listen(engine, "before_cursor_execute", wait_before_runtime_lock)
    try:
        with ThreadPoolExecutor(max_workers=2) as workers:
            future_a = workers.submit(_dispatch, enabled_a, _command_for(scope, request_a))
            future_b = workers.submit(_dispatch, enabled_b, _command_for(scope, request_b))
            result_a = future_a.result(timeout=30)
            result_b = future_b.result(timeout=30)
    finally:
        event.remove(engine, "before_cursor_execute", wait_before_runtime_lock)
        rendezvous.abort()

    assert row_lock_arrivals["count"] == 2
    results = (result_a, result_b)
    winners = [result for result in results if result.success]
    losers = [result for result in results if not result.success]
    assert len(winners) == 1 and len(losers) == 1
    loser_data = cast(dict[str, object], losers[0].data)
    assert loser_data["disposition"] == "REJECTED"
    assert loser_data["reason_code"] in {
        "SPIDER_MEMBER_RESUBMISSION_STALE",
        "SPIDER_MEMBER_RESUBMISSION_NOT_ELIGIBLE",
    }

    after = _snapshot(engine, scope)
    assert after[0] == baseline[0] and after[1] == baseline[1]
    assert type(after[2]) is int and type(baseline[2]) is int
    assert after[2] == cast(int, baseline[2]) + 1
    assert after[3] == baseline[3]
    assert len(cast(tuple[object, ...], after[8])) == len(cast(tuple[object, ...], baseline[8])) + 1
    assert len(cast(tuple[object, ...], after[6])) == len(cast(tuple[object, ...], baseline[6])) + 1
    assert len(cast(tuple[object, ...], after[7])) == len(cast(tuple[object, ...], baseline[7])) + 1
    assert len(cast(tuple[object, ...], after[10])) == len(cast(tuple[object, ...], baseline[10])) + 1
    assert after[11] == baseline[11]
    assert after[12] == baseline[12]
    assert after[13] == baseline[13]
    assert after[15] == baseline[15]
    with Session(engine) as session:
        assert session.scalar(select(func.count()).select_from(SpiderRuntimeTransmission)) == 0
        assert session.scalar(select(func.count()).select_from(SpiderRuntimeTapeIndex).where(
            SpiderRuntimeTapeIndex.kind == "SEND_MARKER"
        )) == 0
    with Session(engine) as session:
        relations = session.execute(select(SpiderMemberResubmission)).scalars().all()
        assert len(relations) == 1
        relation = relations[0]
        assert relation.target_action_id == target_id
        assert relation.request_id in {request_a.request_id, request_b.request_id}
        winner_request = request_a if relation.request_id == request_a.request_id else request_b
        winner_owner = enabled_a if relation.request_id == request_a.request_id else enabled_b
        assert relation.control_receipt_id == expected_control_receipt_id(winner_request)
        winner_proof = winner_owner._member_resubmission_readback(scope, winner_request)
        assert winner_proof is not None
        assert winner_proof["resubmission_action_id"] == relation.resubmission_action_id
        child_action = session.get(SpiderRuntimeAction, relation.resubmission_action_id)
        root_action = session.get(SpiderRuntimeAction, relation.initial_member_id)
        child_order = None if child_action is None else session.get(Order, child_action.local_order_id)
        root_order = None if root_action is None else session.get(Order, root_action.local_order_id)
        assert child_action is not None and child_order is not None and root_order is not None
        before_orders = {cast(str, row[0]): row for row in cast(tuple[tuple[object, ...], ...], baseline[14])}
        after_orders = {cast(str, row[0]): row for row in cast(tuple[tuple[object, ...], ...], after[14])}
        assert {key: after_orders[key] for key in before_orders} == before_orders
        assert set(after_orders) - set(before_orders) == {child_order.id}
        assert child_order.id == child_action.local_order_id
        assert child_order.id != root_order.id
        assert (child_order.product_id, child_order.side, child_order.type,
                child_order.price, child_order.quantity) == (
            root_order.product_id, root_order.side, root_order.type,
            root_order.price, root_order.quantity,
        )
