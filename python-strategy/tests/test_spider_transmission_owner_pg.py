"""Causal PostgreSQL checks for the durable initial PLACE transmission owner."""

import logging
import os
from decimal import Decimal
from hashlib import sha256
from threading import RLock
from typing import Any, cast

import pytest
from sqlalchemy import delete, event, func, select, update
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from src.core.adapters.okx_spider_transport import (
    OkxSpiderBatchTransport,
    classify_okx_batch_response,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.orm_models import Order, Trade
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
    SpiderRuntimeTransmission,
)
from src.core.spider_management.runtime_owner import SpiderRuntimeOwner
from src.core.strategy_state_manager import StrategyStateManager
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_financial_delivery_pg import (
    _install_owner,
    _saved_initial_place_request,
    _transport_enabled_owner,
)
from test_spider_control_command_pg import _command, _dispatch, _owner_with_lifecycle
from test_spider_operation_request_pg import operation_pg as _operation_pg

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


class _OfflinePlaceClient:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response
        self.calls = 0

    def privatePostTradeBatchOrders(
        self, _orders: list[dict[str, object]]
    ) -> object:
        self.calls += 1
        return self.response

    def privatePostTradeCancelBatchOrders(
        self, _orders: list[dict[str, object]]
    ) -> object:
        raise AssertionError("PLACE transmission must not call the CANCEL endpoint")


def _accepted_envelope(members: list[dict[str, object]]) -> dict[str, object]:
    return {
        "code": "0",
        "data": [
            {
                "clOrdId": member["wire_clOrdId"],
                "ordId": f"synthetic-provider-order-{ordinal}",
                "sCode": "0",
            }
            for ordinal, member in enumerate(members)
        ],
        "msg": "synthetic bounded acknowledgement",
    }


def _saved_group_request(
    engine: Engine, scope, source_group: int
) -> tuple[bytes, list[dict[str, object]]]:
    with Session(engine) as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).scalar_one()
        plan = session.execute(
            select(SpiderRuntimePlan).where(
                SpiderRuntimePlan.plan_id == runtime.initial_plan_id
            )
        ).scalar_one()
        rows = session.execute(
            select(SpiderRuntimeAction, SpiderRuntimeBinding, Order)
            .join(SpiderRuntimeBinding, SpiderRuntimeBinding.action_id == SpiderRuntimeAction.action_id)
            .join(Order, Order.id == SpiderRuntimeAction.local_order_id)
            .where(
                SpiderRuntimeAction.plan_id == plan.plan_id,
                SpiderRuntimeAction.transmission_group == source_group,
            )
            .order_by(SpiderRuntimeAction.candidate_ordinal)
        ).all()
        assert rows
        action_ids = [row[0].action_id for row in rows]
        group_id = sha256(canonical_bytes({
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
            "operation": "PLACE",
            "ordered_action_ids": action_ids,
        })).hexdigest()
        members: list[dict[str, object]] = []
        for action, _binding, order in rows:
            intent = cast(dict[str, object], order.intent_payload)
            binding_bytes = bytes.fromhex(cast(str, intent["provider_binding_bytes_hex"]))
            source_order = cast(dict[str, object], decode_canonical(action.original_payload))
            request_order = dict(source_order)
            request_order["clOrdId"] = intent["provider_wire_client_order_id"]
            members.append({
                "action_id": action.action_id,
                "product_id": action.product_id,
                "local_order_id": order.id,
                "binding_sha256": sha256(binding_bytes).hexdigest(),
                "wire_clOrdId": intent["provider_wire_client_order_id"],
                "ordId": order.exchange_order_id,
                "request_payload": request_order,
            })
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
        "members": members,
    }
    body["payload_sha256"] = sha256(canonical_bytes(body)).hexdigest()
    return canonical_bytes(body), members


def _place_attempt_snapshot(engine: Engine, scope) -> tuple[object, ...]:
    with Session(engine) as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).scalar_one()
        plan = session.execute(
            select(SpiderRuntimePlan).where(
                SpiderRuntimePlan.plan_id == runtime.initial_plan_id
            )
        ).scalar_one()
        transmissions = session.execute(
            select(
                SpiderRuntimeTransmission.environment,
                SpiderRuntimeTransmission.execution_scope_id,
                SpiderRuntimeTransmission.strategy_id,
                SpiderRuntimeTransmission.transmission_group_id,
                SpiderRuntimeTransmission.operation_id,
                SpiderRuntimeTransmission.plan_id,
                SpiderRuntimeTransmission.generation,
                SpiderRuntimeTransmission.configuration_sha256,
                SpiderRuntimeTransmission.operation,
                SpiderRuntimeTransmission.request_payload,
                SpiderRuntimeTransmission.request_sha256,
                SpiderRuntimeTransmission.response_payload,
                SpiderRuntimeTransmission.response_sha256,
                SpiderRuntimeTransmission.response_failure,
                SpiderRuntimeTransmission.recorded_at,
                SpiderRuntimeTransmission.response_recorded_at,
            ).where(
                SpiderRuntimeTransmission.environment == scope.environment,
                SpiderRuntimeTransmission.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id,
            ).order_by(SpiderRuntimeTransmission.transmission_group_id)
        ).all()
        transmission_snapshot = tuple(
            tuple(
                bytes(value) if index in {9, 11} and value is not None else value
                for index, value in enumerate(row)
            )
            for row in transmissions
        )
        markers = session.execute(
            select(
                SpiderRuntimeTapeIndex.environment,
                SpiderRuntimeTapeIndex.execution_scope_id,
                SpiderRuntimeTapeIndex.strategy_id,
                SpiderRuntimeTapeIndex.ordinal,
                SpiderRuntimeTapeIndex.kind,
                SpiderRuntimeTapeIndex.delivery_sequence,
                SpiderRuntimeTapeIndex.transmission_group_id,
            ).where(
                SpiderRuntimeTapeIndex.environment == scope.environment,
                SpiderRuntimeTapeIndex.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id,
                SpiderRuntimeTapeIndex.kind == "SEND_MARKER",
            ).order_by(SpiderRuntimeTapeIndex.ordinal)
        ).all()
        actions = session.execute(
            select(
                SpiderRuntimeAction.action_id,
                SpiderRuntimeAction.outcome_state,
                SpiderRuntimeAction.outcome_reference,
            ).where(SpiderRuntimeAction.plan_id == plan.plan_id)
            .order_by(SpiderRuntimeAction.candidate_ordinal)
        ).all()
        return (
            runtime.last_received_sequence,
            runtime.last_tape_sequence,
            tuple(actions),
            transmission_snapshot,
            tuple(markers),
            (plan.plan_status, bytes(plan.missing_members)),
        )


def _financial_receipt_snapshot(session: Session, scope) -> tuple[tuple[object, ...], ...]:
    rows = session.execute(
        select(
            SpiderRuntimeDelivery.sequence,
            SpiderRuntimeDelivery.delivery_id,
            SpiderRuntimeDelivery.action_id,
            SpiderRuntimeDelivery.local_order_id,
            SpiderRuntimeDelivery.trade_id,
            SpiderRuntimeDelivery.content_sha256,
            SpiderRuntimeDelivery.source_payload,
            SpiderRuntimeDelivery.disposition,
            SpiderRuntimeDelivery.suppression_reason,
        ).where(
            SpiderRuntimeDelivery.environment == scope.environment,
            SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
        ).order_by(SpiderRuntimeDelivery.sequence)
    ).all()
    return tuple(
        (*row[:-3], bytes(row[6]), *row[-2:])
        for row in rows
    )


def test_marker_projection_failure_rolls_back_before_offline_transport_call(
    operation_pg: Engine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, _manager, _prepared, initial, *_ = _install_owner(engine)
    source_group, request_payload, _members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    monkeypatch.setattr(enabled, "_refresh_initial_place_projection", lambda *_args, **_kwargs: None)
    offline = _OfflinePlaceClient({"code": "0", "data": [], "msg": "never called"})

    result = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, offline)),
    )

    assert result.disposition == "UNKNOWN"
    assert offline.calls == 0
    monkeypatch.undo()
    assert enabled.readback(scope, operation_id) == initial
    with Session(engine) as session:
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeTransmission).where(
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id
            )
        ) == 0
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            )
        ) == 0
        assert session.scalar(
            select(SpiderRuntimeScope.last_tape_sequence).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ) == 0


def test_result_projection_failure_rolls_back_raw_outcome_but_keeps_committed_marker(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, _manager, _prepared, _initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    original_refresh = enabled._refresh_initial_place_projection
    persisted_refreshes = 0
    fault_fired = False

    def fail_after_result_projection(*args: Any, **kwargs: Any):
        nonlocal persisted_refreshes, fault_fired
        projected = original_refresh(*args, **kwargs)
        if kwargs.get("persist") is True:
            persisted_refreshes += 1
        if kwargs.get("persist") is True and persisted_refreshes == 2:
            assert projected is not None
            fault_fired = True
            raise RuntimeError("synthetic result write rollback")
        return projected

    enabled._refresh_initial_place_projection = fail_after_result_projection
    offline = _OfflinePlaceClient(_accepted_envelope(members))

    result = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, offline)),
    )

    assert persisted_refreshes == 2
    assert fault_fired is True
    assert offline.calls == 1
    assert result.disposition == "SEND_UNKNOWN"
    assert result.action_outcomes == tuple(
        (cast(str, member["action_id"]), "SEND_UNKNOWN") for member in members
    )
    assert enabled.readback(scope, operation_id) is not None
    with Session(engine) as session:
        marker = session.execute(
            select(SpiderRuntimeTransmission).where(
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        assert marker.response_payload is None
        assert marker.response_failure is None
        actions = list(session.scalars(
            select(SpiderRuntimeAction).where(
                SpiderRuntimeAction.plan_id == marker.plan_id,
                SpiderRuntimeAction.outcome_reference == marker.transmission_group_id,
            ).order_by(SpiderRuntimeAction.transmission_ordinal)
        ))
        assert [action.outcome_state for action in actions] == ["SEND_UNKNOWN"] * len(members)


def test_lost_result_commit_response_is_proved_by_original_group_readback(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, _manager, _prepared, _initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    commit_count = 0
    fault = {"fired": False}

    class LoseResultCommitResponseSession(Session):
        def close(self) -> None:
            commit_at_close = cast(int, self.info.get("observed_commit_count", 0))
            super().close()
            if commit_at_close == 2 and not fault["fired"]:
                fault["fired"] = True
                raise RuntimeError("synthetic committed result close loss")

    def record_commit(session: Session) -> None:
        nonlocal commit_count
        commit_count += 1
        session.info["observed_commit_count"] = commit_count

    event.listen(LoseResultCommitResponseSession, "after_commit", record_commit)
    try:
        session_factory = sessionmaker(engine, class_=LoseResultCommitResponseSession)
        lifecycle = RLock()
        order_events = RLock()
        enabled = SpiderRuntimeOwner(
            session_factory,
            owner._order_manager,
            owned_scopes=(scope,),
            model_sources_by_confirmation=owner._model_sources,
            lifecycle_lock=lambda _strategy_id: lifecycle,
            order_event_lock=order_events,
            transport_response_classifier=classify_okx_batch_response,
        )
        offline = _OfflinePlaceClient(_accepted_envelope(members))
        transport = OkxSpiderBatchTransport(cast(Any, offline))

        result = enabled.transmit_initial_place_group(
            scope,
            source_group,
            scope.execution_scope_id,
            request_payload,
            transport,
        )
        assert fault["fired"] is True
        # The marker and result writes commit, followed by one proof reader.
        assert commit_count == 3
        assert offline.calls == 1
        assert result.disposition == "RECORDED"
        assert result.action_outcomes == tuple(
            (cast(str, member["action_id"]), "ACCEPTED") for member in members
        )
        proved = enabled.readback(scope, operation_id)
        assert proved is not None
        retry = enabled.transmit_initial_place_group(
            scope,
            source_group,
            scope.execution_scope_id,
            request_payload,
            transport,
        )
        assert retry == result
        assert offline.calls == 1
    finally:
        event.remove(LoseResultCommitResponseSession, "after_commit", record_commit)


def test_stop_can_commit_between_durable_marker_and_provider_response(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    base_owner, scope, operation_id, _factory, initial = _owner_with_lifecycle(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = SpiderRuntimeOwner(
        base_owner._sessions,
        base_owner._order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation=base_owner._model_sources,
        strategy_state_manager=base_owner._strategy_state_manager,
        lifecycle_lock=base_owner._lifecycle_lock,
        order_event_lock=base_owner._order_event_lock,
        transport_response_classifier=classify_okx_batch_response,
    )
    stopped: list[object] = []

    class StopInsideOfflineCall:
        def __init__(self) -> None:
            self.calls = 0

        def privatePostTradeBatchOrders(
            self, _orders: list[dict[str, object]]
        ) -> object:
            self.calls += 1
            stop_result = _dispatch(
                enabled,
                _command(
                    scope,
                    key="stop-after-send-marker",
                    version=cast(int, initial["state_version"]),
                ),
            )
            stopped.append(stop_result)
            return _accepted_envelope(members)

        def privatePostTradeCancelBatchOrders(
            self, _orders: list[dict[str, object]]
        ) -> object:
            raise AssertionError("PLACE transmission must not call the CANCEL endpoint")

    client = StopInsideOfflineCall()
    outcome = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, client)),
    )
    assert client.calls == 1
    assert stopped and cast(Any, stopped[0]).success is True
    assert outcome.disposition == "RECORDED"
    assert outcome.action_outcomes == tuple(
        (cast(str, member["action_id"]), "ACCEPTED") for member in members
    )
    readback = enabled.readback(scope, operation_id)
    assert readback is not None
    assert readback["runtime_control"] == "STOP"
    assert readback["phase"] == "INCOMPLETE"
    assert readback["trading_ready"] is False
    retry = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, client)),
    )
    assert retry == outcome
    assert client.calls == 1
    with Session(engine) as session:
        runtime = session.execute(
            select(
                SpiderRuntimeScope.last_tape_sequence,
                SpiderRuntimeScope.control_barrier_sequence,
                SpiderRuntimeScope.control_mode,
            ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).one()
        assert runtime.control_mode == "STOP"
        assert runtime.control_barrier_sequence == runtime.last_tape_sequence == 1
        marker = session.execute(
            select(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        assert marker.kind == "SEND_MARKER"


def test_valid_but_unclassified_raw_response_is_durable_and_projection_is_checked(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, _manager, _prepared, _initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    raw: dict[str, object] = {
        "code": "1",
        "data": "new-provider-shape",
        "msg": "opaque",
    }
    offline = _OfflinePlaceClient(raw)
    enabled = _transport_enabled_owner(owner, scope)
    transport = OkxSpiderBatchTransport(cast(Any, offline))

    first = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload, transport
    )
    assert first.disposition == "RECORDED"
    assert first.action_outcomes == tuple(
        (cast(str, member["action_id"]), "SEND_UNKNOWN") for member in members
    )
    initial = _initial
    proved = enabled.readback(scope, operation_id)
    assert proved is not None
    assert proved["phase"] == "INCOMPLETE"
    assert proved["trading_ready"] is False
    missing = cast(list[dict[str, object]], proved["initial_plan_missing_members"])
    initial_missing = cast(list[dict[str, object]], initial["initial_plan_missing_members"])
    sent_ids = {cast(str, member["action_id"]) for member in members}
    all_initial_ids = {cast(str, row["initial_member_id"]) for row in initial_missing}
    unsent_ids = all_initial_ids - sent_ids
    missing_by_id = {
        cast(str, row["initial_member_id"]): row for row in missing
    }
    assert set(missing_by_id) == all_initial_ids
    assert all(
        missing_by_id[action_id]["last_outcome"] == "SEND_UNKNOWN"
        and missing_by_id[action_id]["rejection_reason"] is None
        for action_id in sent_ids
    )
    assert all(
        missing_by_id[action_id]["last_outcome"] == "UNSENT"
        and missing_by_id[action_id]["rejection_reason"] is None
        for action_id in unsent_ids
    )
    assert offline.calls == 1

    with Session(engine) as session:
        saved = session.execute(
            select(SpiderRuntimeTransmission).where(
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        assert saved.response_payload == canonical_bytes(raw)
        assert saved.response_failure is None
        first_action_id = cast(str, members[0]["action_id"])
    with engine.begin() as connection:
        connection.execute(
            update(SpiderRuntimeAction)
            .where(SpiderRuntimeAction.action_id == first_action_id)
            .values(outcome_state="ACCEPTED")
        )
    assert enabled.readback(scope, operation_id) is None
    with engine.begin() as connection:
        connection.execute(
            update(SpiderRuntimeAction)
            .where(SpiderRuntimeAction.action_id == first_action_id)
            .values(outcome_state="SEND_UNKNOWN")
        )
    assert enabled.readback(scope, operation_id) == proved
    assert offline.calls == 1


def test_changed_bytes_for_an_existing_group_are_unavailable_and_never_resent(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, _manager, _prepared, _initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    offline = _OfflinePlaceClient(_accepted_envelope(members))
    transport = OkxSpiderBatchTransport(cast(Any, offline))
    original = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload, transport
    )
    assert original.disposition == "RECORDED"
    before = enabled.readback(scope, operation_id)
    assert before is not None

    changed = cast(dict[str, object], decode_canonical(request_payload))
    changed["executing_account_ref"] = "different-synthetic-account"
    changed.pop("payload_sha256")
    changed["payload_sha256"] = sha256(canonical_bytes(changed)).hexdigest()
    changed_payload = canonical_bytes(changed)
    conflict = enabled.transmit_initial_place_group(
        scope,
        source_group,
        "different-synthetic-account",
        changed_payload,
        transport,
    )
    assert conflict.disposition == "UNKNOWN"
    assert conflict.transmission_group_id == original.transmission_group_id
    assert offline.calls == 1
    assert enabled.readback(scope, operation_id) == before
    with Session(engine) as session:
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeTransmission).where(
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id
            )
        ) == 1
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            )
        ) == 1


def test_all_saved_source_groups_complete_and_exact_first_retry_never_resends(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, _manager, _prepared, _initial, *_ = _install_owner(engine)
    with Session(engine) as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).scalar_one()
        group_sizes = session.execute(
            select(
                SpiderRuntimeAction.transmission_group,
                func.count(SpiderRuntimeAction.action_id),
            )
            .where(SpiderRuntimeAction.plan_id == runtime.initial_plan_id)
            .group_by(SpiderRuntimeAction.transmission_group)
            .order_by(SpiderRuntimeAction.transmission_group)
        ).all()
    assert len(group_sizes) == 2
    assert all(1 <= count <= 20 for _source_group, count in group_sizes)
    enabled = _transport_enabled_owner(owner, scope)

    class AcceptEachGroup:
        def __init__(self) -> None:
            self.calls = 0

        def privatePostTradeBatchOrders(self, orders: list[dict[str, object]]) -> object:
            self.calls += 1
            return {
                "code": "0",
                "data": [
                    {
                        "clOrdId": cast(str, order["clOrdId"]),
                        "ordId": f"synthetic-group-{self.calls}-{index}",
                        "sCode": "0",
                    }
                    for index, order in enumerate(orders)
                ],
                "msg": "accepted saved group",
            }

        def privatePostTradeCancelBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            raise AssertionError("PLACE transmission must not call the CANCEL endpoint")

    client = AcceptEachGroup()
    transport = OkxSpiderBatchTransport(cast(Any, client))
    saved: list[tuple[int, bytes, object]] = []
    for source_group_value, expected_count in group_sizes:
        source_group = cast(int, source_group_value)
        request_payload, members = _saved_group_request(engine, scope, source_group)
        assert len(members) == expected_count
        outcome = enabled.transmit_initial_place_group(
            scope,
            source_group,
            scope.execution_scope_id,
            request_payload,
            transport,
        )
        assert outcome.disposition == "RECORDED"
        assert outcome.action_outcomes == tuple(
            (cast(str, member["action_id"]), "ACCEPTED") for member in members
        )
        saved.append((source_group, request_payload, outcome))

    complete = enabled.readback(scope, operation_id)
    assert complete is not None
    assert complete["initial_plan_status"] == "COMPLETE"
    assert complete["initial_plan_missing_members"] == []
    assert complete["phase"] == "INCOMPLETE"
    assert complete["trading_ready"] is False
    first_group, first_payload, first_outcome = saved[0]
    retried = enabled.transmit_initial_place_group(
        scope,
        first_group,
        scope.execution_scope_id,
        first_payload,
        transport,
    )
    assert retried == first_outcome
    assert client.calls == 2
    assert enabled.readback(scope, operation_id) == complete
    with Session(engine) as session:
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeTransmission).where(
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id
            )
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            )
        ) == 2


def test_mixed_place_response_projects_only_the_exact_saved_group_and_retries_once(
    operation_pg: Engine,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, _manager, _prepared, initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    assert len(members) >= 3
    raw = {
        "code": "1",
        "data": [
            {
                "clOrdId": members[0]["wire_clOrdId"],
                "ordId": "mixed-accepted-provider-order",
                "sCode": "0",
            },
            {
                "clOrdId": members[1]["wire_clOrdId"],
                "ordId": "",
                "sCode": "51008",
            },
        ],
        "msg": "synthetic mixed member result",
    }
    enabled = _transport_enabled_owner(owner, scope)
    offline = _OfflinePlaceClient(raw)
    transport = OkxSpiderBatchTransport(cast(Any, offline))
    recorded = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload, transport
    )
    assert recorded.disposition == "RECORDED"
    assert recorded.action_outcomes == tuple(
        (
            cast(str, member["action_id"]),
            "ACCEPTED" if index == 0 else "REJECTED" if index == 1 else "SEND_UNKNOWN",
        )
        for index, member in enumerate(members)
    )
    readback = enabled.readback(scope, operation_id)
    assert readback is not None
    missing = cast(list[dict[str, object]], readback["initial_plan_missing_members"])
    initial_missing = cast(list[dict[str, object]], initial["initial_plan_missing_members"])
    by_id = {cast(str, item["initial_member_id"]): item for item in missing}
    sent_ids = {cast(str, member["action_id"]) for member in members}
    accepted_id = cast(str, members[0]["action_id"])
    assert accepted_id not in by_id
    assert by_id[cast(str, members[1]["action_id"])]["last_outcome"] == "REJECTED"
    assert by_id[cast(str, members[1]["action_id"])]["rejection_reason"] == "51008"
    assert all(
        by_id[cast(str, member["action_id"])]["last_outcome"] == "SEND_UNKNOWN"
        for member in members[2:]
    )
    assert all(
        by_id[cast(str, item["initial_member_id"])]["last_outcome"] == "UNSENT"
        for item in initial_missing
        if item["initial_member_id"] not in sent_ids
    )
    expected_missing_ids = [
        cast(str, item["initial_member_id"])
        for item in initial_missing
        if item["initial_member_id"] != accepted_id
    ]
    assert [cast(str, item["initial_member_id"]) for item in missing] == expected_missing_ids
    assert readback["phase"] == "INCOMPLETE" and readback["trading_ready"] is False
    retry = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload, transport
    )
    assert retry == recorded
    assert offline.calls == 1


@pytest.mark.parametrize("missing_record", ["binding", "receipt"])
def test_missing_covered_binding_and_financial_receipt_break_shared_proof(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    missing_record: str,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, order_manager, _prepared, _initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    offline = _OfflinePlaceClient(_accepted_envelope(members))
    transport = OkxSpiderBatchTransport(cast(Any, offline))
    recorded = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload, transport
    )
    assert recorded.disposition == "RECORDED"
    target = members[0]
    order = order_manager.repo.get_order(cast(str, target["local_order_id"]))
    assert order is not None
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id="synthetic-provider-order-0",
    ))["action"] == "applied"
    before_omission = enabled.readback(scope, operation_id)
    assert before_omission is not None
    initial_ids = {
        cast(str, item["initial_member_id"])
        for item in cast(list[dict[str, object]], before_omission["initial_plan_missing_members"])
    }
    assert cast(str, target["action_id"]) not in initial_ids
    with Session(engine) as session:
        saved_action = session.get(SpiderRuntimeAction, cast(str, target["action_id"]))
        saved_order = session.get(Order, order.id)
        assert saved_action is not None and saved_action.outcome_state == "ACCEPTED"
        assert saved_order is not None
        assert saved_order.exchange_order_id == "synthetic-provider-order-0"

    with engine.begin() as connection:
        if missing_record == "binding":
            connection.execute(
                delete(SpiderRuntimeBinding).where(
                    SpiderRuntimeBinding.action_id == target["action_id"]
                )
            )
        else:
            sequences = list(connection.execute(
                select(SpiderRuntimeDelivery.sequence).where(
                    SpiderRuntimeDelivery.local_order_id == order.id
                )
            ).scalars())
            assert sequences
            connection.execute(
                delete(SpiderRuntimeTapeIndex).where(
                    SpiderRuntimeTapeIndex.delivery_sequence.in_(sequences)
                )
            )
            connection.execute(
                delete(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.sequence.in_(sequences)
                )
            )
    assert enabled.readback(scope, operation_id) is None
    before_failed_attempt = _place_attempt_snapshot(engine, scope)
    retry = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload, transport
    )
    assert retry.disposition in {"UNKNOWN", "REJECTED"}
    assert offline.calls == 1
    assert _place_attempt_snapshot(engine, scope) == before_failed_attempt


@pytest.mark.parametrize("proof_field", ["manifest_hash", "checkpoint_hash", "source_events"])
@pytest.mark.parametrize("after_marker", [False, True])
def test_corrupt_initial_install_proof_blocks_marker_and_saved_readback(
    operation_pg: Engine,
    proof_field: str,
    after_marker: bool,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, _manager, _prepared, initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    offline = _OfflinePlaceClient(_accepted_envelope(members))
    transport = OkxSpiderBatchTransport(cast(Any, offline))
    if after_marker:
        recorded = enabled.transmit_initial_place_group(
            scope, source_group, scope.execution_scope_id, request_payload, transport
        )
        assert recorded.disposition == "RECORDED"
        assert offline.calls == 1

    with Session(engine) as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).scalar_one()
        plan = session.execute(
            select(SpiderRuntimePlan).where(
                SpiderRuntimePlan.plan_id == runtime.initial_plan_id
            )
        ).scalar_one()
        original_checkpoint_hash = runtime.source_checkpoint_sha256
        original_manifest_hash = plan.manifest_sha256
        original_manifest_payload = bytes(plan.manifest_payload)
        if proof_field == "manifest_hash":
            plan.manifest_sha256 = "f" * 64
        elif proof_field == "checkpoint_hash":
            runtime.source_checkpoint_sha256 = "f" * 64
        else:
            manifest = cast(dict[str, object], decode_canonical(original_manifest_payload))
            source_events = cast(dict[str, object], manifest["initial_source_events"])
            source_events["ordered_products"] = []
            changed_payload = canonical_bytes(manifest)
            plan.manifest_payload = changed_payload
            plan.manifest_sha256 = sha256(changed_payload).hexdigest()
        session.commit()

    assert enabled.readback(scope, operation_id) is None
    before_failed_attempt = _place_attempt_snapshot(engine, scope)
    attempt = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload, transport
    )
    if after_marker:
        assert attempt.disposition == "UNKNOWN"
    else:
        assert attempt.disposition in {"UNKNOWN", "REJECTED"}
    assert offline.calls == (1 if after_marker else 0)
    assert _place_attempt_snapshot(engine, scope) == before_failed_attempt
    if not after_marker:
        assert before_failed_attempt[3] == ()
        assert before_failed_attempt[4] == ()

    with Session(engine) as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).scalar_one()
        plan = session.execute(
            select(SpiderRuntimePlan).where(
                SpiderRuntimePlan.plan_id == runtime.initial_plan_id
            )
        ).scalar_one()
        runtime.source_checkpoint_sha256 = original_checkpoint_hash
        plan.manifest_sha256 = original_manifest_hash
        plan.manifest_payload = original_manifest_payload
        session.commit()
    recovered = enabled.readback(scope, operation_id)
    if after_marker:
        assert recovered is not None
    else:
        assert recovered == initial


def _execution_for_owner(owner, engine: Engine, clock, adapter) -> ExecutionEngine:
    return ExecutionEngine(
        db_session=None,
        clock=clock,
        adapter=adapter,
        db_session_factory=sessionmaker(engine),
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )


def _filled_event(order: Order, *, provider_id: str, trade_id: str) -> ExchangeOrderEvent:
    assert order.price is not None
    return ExchangeOrderEvent(
        status="filled",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id=provider_id,
        cumulative_filled_quantity=order.quantity,
        cumulative_average_price=cast(Decimal, order.price),
        last_fill_quantity=order.quantity,
        last_fill_price=cast(Decimal, order.price),
        fee=Decimal("0.01"),
        fee_asset="USDT",
        last_fill_exchange_trade_id=trade_id,
    )


def test_late_fill_after_saved_rejection_preserves_financial_facts_and_updates_projection(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, order_manager, _prepared, initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    rejected = {
        "code": "1",
        "data": [
            {"clOrdId": member["wire_clOrdId"], "ordId": "", "sCode": "51008"}
            for member in members
        ],
        "msg": "synthetic rejection before financial fill",
    }
    offline = _OfflinePlaceClient(rejected)
    enabled = _transport_enabled_owner(owner, scope)
    result = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, offline)),
    )
    assert result.disposition == "RECORDED"
    target = members[0]
    order = order_manager.repo.get_order(cast(str, target["local_order_id"]))
    assert order is not None
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)
    assert execution.process_exchange_order_event(_filled_event(
        order,
        provider_id="late-fill-provider-order",
        trade_id="late-fill-after-rejection",
    ))["action"] == "applied"

    readback = enabled.readback(scope, operation_id)
    assert readback is not None
    assert readback["phase"] == "INCOMPLETE"
    assert readback["trading_ready"] is False
    missing = cast(list[dict[str, object]], readback["initial_plan_missing_members"])
    by_member = {cast(str, row["initial_member_id"]): row for row in missing}
    sent_ids = {cast(str, member["action_id"]) for member in members}
    initial_missing = cast(list[dict[str, object]], initial["initial_plan_missing_members"])
    unsent_ids = {cast(str, row["initial_member_id"]) for row in initial_missing} - sent_ids
    assert by_member[cast(str, target["action_id"])]["last_outcome"] == "SEND_UNKNOWN"
    assert by_member[cast(str, target["action_id"])]["rejection_reason"] is None
    assert all(
        by_member[action_id]["last_outcome"] == "REJECTED"
        and by_member[action_id]["rejection_reason"] == "51008"
        for action_id in sent_ids - {cast(str, target["action_id"])}
    )
    assert all(by_member[action_id]["last_outcome"] == "UNSENT" for action_id in unsent_ids)
    with Session(engine) as session:
        trade = session.execute(
            select(Trade).where(Trade.exchange_trade_id == "late-fill-after-rejection")
        ).scalar_one()
        saved_order = session.get(Order, order.id)
        assert saved_order is not None
        assert trade.order_id == order.id
        assert trade.quantity == order.quantity
        assert trade.price == order.price
        assert trade.fee == Decimal("0.01")
        assert saved_order.filled_quantity == order.quantity
        assert offline.calls == 1

    before_duplicate_readback = enabled.readback(scope, operation_id)
    assert before_duplicate_readback == readback
    with Session(engine) as session:
        before_receipts = _financial_receipt_snapshot(session, scope)
        before_duplicate = (
            session.scalar(
                select(func.count()).select_from(Trade).where(Trade.order_id == order.id)
            ),
            session.scalar(
                select(func.count()).select_from(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.environment == scope.environment,
                    SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                )
            ),
            session.execute(
                select(
                    SpiderRuntimeScope.last_received_sequence,
                    SpiderRuntimeScope.last_tape_sequence,
                ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
            ).one(),
            session.execute(
                select(
                    Order.status,
                    Order.filled_quantity,
                    Order.filled_price,
                    Order.exchange_order_id,
                ).where(Order.id == order.id)
            ).one(),
        )
    duplicate = execution.process_exchange_order_event(_filled_event(
        order,
        provider_id="late-fill-provider-order",
        trade_id="late-fill-after-rejection",
    ))
    assert duplicate["action"] == "applied"
    assert duplicate["fill_quantity"] == Decimal("0")
    assert enabled.readback(scope, operation_id) == before_duplicate_readback
    with Session(engine) as session:
        after_receipts = _financial_receipt_snapshot(session, scope)
        after_duplicate = (
            session.scalar(
                select(func.count()).select_from(Trade).where(Trade.order_id == order.id)
            ),
            session.scalar(
                select(func.count()).select_from(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.environment == scope.environment,
                    SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                )
            ),
            session.execute(
                select(
                    SpiderRuntimeScope.last_received_sequence,
                    SpiderRuntimeScope.last_tape_sequence,
                ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
            ).one(),
            session.execute(
                select(
                    Order.status,
                    Order.filled_quantity,
                    Order.filled_price,
                    Order.exchange_order_id,
                ).where(Order.id == order.id)
            ).one(),
            session.execute(
                select(Trade.quantity, Trade.price, Trade.fee).where(
                    Trade.exchange_trade_id == "late-fill-after-rejection"
                )
            ).one(),
        )
    assert after_duplicate[:4] == before_duplicate
    assert after_duplicate[4] == (order.quantity, order.price, Decimal("0.01"))
    assert after_receipts == before_receipts
    assert offline.calls == 1


def test_late_fill_then_actual_stop_drains_receipts_without_changing_financial_facts(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
) -> None:
    engine = operation_pg
    install_owner, scope, operation_id, order_manager, _prepared, initial, confirmation, source = (
        _install_owner(engine)
    )
    assert install_owner.readback(scope, operation_id) == initial
    factory = sessionmaker(engine)

    class RedisProbe:
        def publish(self, _channel: str, _message: str) -> int:
            return 1

    lock = RLock()
    owner = SpiderRuntimeOwner(
        factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={confirmation.request_id: source},
        strategy_state_manager=StrategyStateManager(factory, RedisProbe()),
        lifecycle_lock=lambda _strategy_id: lock,
        order_event_lock=RLock(),
        transport_response_classifier=classify_okx_batch_response,
    )
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    offline = _OfflinePlaceClient({
        "code": "1",
        "data": [
            {"clOrdId": member["wire_clOrdId"], "ordId": "", "sCode": "51008"}
            for member in members
        ],
        "msg": "synthetic rejection before late fill and STOP",
    })
    result = owner.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, offline)),
    )
    assert result.disposition == "RECORDED"
    order = order_manager.repo.get_order(cast(str, members[0]["local_order_id"]))
    assert order is not None
    execution = _execution_for_owner(owner, engine, mock_clock, mock_exchange_adapter)
    assert execution.process_exchange_order_event(_filled_event(
        order,
        provider_id="stop-after-late-fill-provider-order",
        trade_id="stop-after-late-fill",
    ))["action"] == "applied"
    before_stop = owner.readback(scope, operation_id)
    assert before_stop is not None
    before_missing = before_stop["initial_plan_missing_members"]
    version = cast(int, initial["state_version"])
    stopped = _dispatch(owner, _command(scope, key="late-fill-stop", version=version))
    assert stopped.success is True and stopped.completed is True
    owner.restore_owned_runtimes()

    after_stop = owner.readback(scope, operation_id)
    assert after_stop is not None
    assert after_stop["runtime_control"] == "STOP"
    assert after_stop["phase"] == "INCOMPLETE"
    assert after_stop["trading_ready"] is False
    assert after_stop["initial_plan_missing_members"] == before_missing
    with Session(engine) as session:
        trade = session.execute(
            select(Trade).where(Trade.exchange_trade_id == "stop-after-late-fill")
        ).scalar_one()
        runtime = session.execute(
            select(
                SpiderRuntimeScope.last_received_sequence,
                SpiderRuntimeScope.last_consumed_sequence,
            ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).one()
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.disposition == "PENDING",
            )
        ) == 0
    assert trade.order_id == order.id
    assert trade.quantity == order.quantity
    assert trade.price == order.price
    assert trade.fee == Decimal("0.01")
    assert runtime.last_received_sequence == runtime.last_consumed_sequence
    assert offline.calls == 1


def test_fill_before_rejection_keeps_same_financial_fact_and_returns_unknown_projection(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, order_manager, _prepared, initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)
    target = members[0]
    order = order_manager.repo.get_order(cast(str, target["local_order_id"]))
    assert order is not None

    class FillBeforeRejectClient:
        def __init__(self) -> None:
            self.calls = 0

        def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            self.calls += 1
            assert execution.process_exchange_order_event(_filled_event(
                order,
                provider_id="fill-before-rejection-provider-order",
                trade_id="fill-before-rejection",
            ))["action"] == "applied"
            return {
                "code": "1",
                "data": [
                    {"clOrdId": member["wire_clOrdId"], "ordId": "", "sCode": "51008"}
                    for member in members
                ],
                "msg": "synthetic rejection after financial fill",
            }

        def privatePostTradeCancelBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            raise AssertionError("PLACE transmission must not call the CANCEL endpoint")

    client = FillBeforeRejectClient()
    result = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, client)),
    )
    assert client.calls == 1
    assert result.disposition == "RECORDED"
    target_id = cast(str, target["action_id"])
    assert result.action_outcomes == tuple(
        (cast(str, member["action_id"]), "SEND_UNKNOWN" if member["action_id"] == target_id else "REJECTED")
        for member in members
    )
    readback = enabled.readback(scope, operation_id)
    assert readback is not None
    missing = cast(list[dict[str, object]], readback["initial_plan_missing_members"])
    by_member = {cast(str, row["initial_member_id"]): row for row in missing}
    sent_ids = {cast(str, member["action_id"]) for member in members}
    initial_missing = cast(list[dict[str, object]], initial["initial_plan_missing_members"])
    unsent_ids = {cast(str, row["initial_member_id"]) for row in initial_missing} - sent_ids
    assert by_member[target_id]["last_outcome"] == "SEND_UNKNOWN"
    assert by_member[target_id]["rejection_reason"] is None
    assert all(
        by_member[action_id]["last_outcome"] == "REJECTED"
        and by_member[action_id]["rejection_reason"] == "51008"
        for action_id in sent_ids - {target_id}
    )
    assert all(by_member[action_id]["last_outcome"] == "UNSENT" for action_id in unsent_ids)
    with Session(engine) as session:
        trade = session.execute(
            select(Trade).where(Trade.exchange_trade_id == "fill-before-rejection")
        ).scalar_one()
        assert trade.order_id == order.id
        assert trade.quantity == order.quantity
        assert trade.price == order.price
        assert trade.fee == Decimal("0.01")


def test_valid_late_fill_commits_when_old_tape_proof_is_missing_but_readback_stays_unavailable(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, order_manager, _prepared, _initial, *_ = _install_owner(engine)
    order_id = cast(str, _saved_initial_place_request(engine, scope)[2][0]["local_order_id"])
    order = order_manager.repo.get_order(order_id)
    assert order is not None
    enabled = _transport_enabled_owner(owner, scope)
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)

    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id="preexisting-ack-provider-order",
    ))["action"] == "applied"
    assert enabled.readback(scope, operation_id) is not None
    with Session(engine) as session:
        ack_receipts = session.execute(
            select(SpiderRuntimeDelivery.sequence, SpiderRuntimeDelivery.kind)
            .where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.local_order_id == order_id,
            )
            .order_by(SpiderRuntimeDelivery.sequence)
        ).all()
        ack_frontier = session.execute(
            select(
                SpiderRuntimeScope.last_received_sequence,
                SpiderRuntimeScope.last_tape_sequence,
            ).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).one()
        assert [(row.sequence, row.kind) for row in ack_receipts] == [
            (1, "ORDER_STATE"),
            (2, "ORDER_STATE"),
        ]
        assert ack_frontier == (2, 2)
    with engine.begin() as connection:
        connection.execute(
            delete(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.environment == scope.environment,
                SpiderRuntimeTapeIndex.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id,
                SpiderRuntimeTapeIndex.kind == "DELIVERY",
            )
        )
        connection.execute(
            delete(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.sequence == 1,
            )
        )

    logger = logging.getLogger("src.core.spider_management.runtime_owner")
    monkeypatch.setattr(logger, "disabled", False)
    added_capture = caplog.handler not in logger.handlers
    if added_capture:
        logger.addHandler(caplog.handler)
    try:
        fill_result = execution.process_exchange_order_event(_filled_event(
            order,
            provider_id="preexisting-ack-provider-order",
            trade_id="financial-fill-with-missing-old-tape",
        ))
    finally:
        if added_capture:
            logger.removeHandler(caplog.handler)
    assert fill_result["action"] == "applied"
    assert "initial plan projection unavailable" in caplog.text
    assert enabled.readback(scope, operation_id) is None
    with Session(engine) as session:
        saved_trade = session.execute(
            select(Trade).where(
                Trade.exchange_trade_id == "financial-fill-with-missing-old-tape"
            )
        ).scalar_one()
        saved_order = session.get(Order, order_id)
        runtime = session.execute(
            select(
                SpiderRuntimeScope.last_received_sequence,
                SpiderRuntimeScope.last_tape_sequence,
            ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).one()
        surviving_receipts = session.execute(
            select(SpiderRuntimeDelivery.sequence, SpiderRuntimeDelivery.kind)
            .where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.local_order_id == order_id,
            )
            .order_by(SpiderRuntimeDelivery.sequence)
        ).all()
        assert saved_order is not None
        assert saved_trade.order_id == order_id
        assert saved_trade.quantity == order.quantity
        assert saved_trade.price == order.price
        assert saved_trade.fee == Decimal("0.01")
        assert saved_order.status == "FILLED"
        assert surviving_receipts == [(2, "ORDER_STATE"), (3, "FILL")]
        assert runtime.last_received_sequence == 3
        assert runtime.last_tape_sequence == 3


def test_projection_sql_failure_rolls_back_order_state_receipt_and_projection_txa(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, order_manager, _prepared, _initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    rejected = {
        "code": "1",
        "data": [
            {"clOrdId": member["wire_clOrdId"], "ordId": "", "sCode": "51008"}
            for member in members
        ],
        "msg": "synthetic rejection before projection rollback",
    }
    enabled = _transport_enabled_owner(owner, scope)
    transmission = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, _OfflinePlaceClient(rejected))),
    )
    assert transmission.disposition == "RECORDED"
    target = members[0]
    action_id = cast(str, target["action_id"])
    order_id = cast(str, target["local_order_id"])
    order = order_manager.repo.get_order(order_id)
    assert order is not None
    before_readback = enabled.readback(scope, operation_id)
    assert before_readback is not None
    with Session(engine) as session:
        runtime_before = session.execute(
            select(
                SpiderRuntimeScope.last_received_sequence,
                SpiderRuntimeScope.last_tape_sequence,
            ).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).one()
        delivery_before = session.execute(
            select(
                SpiderRuntimeDelivery.sequence,
                SpiderRuntimeDelivery.kind,
                SpiderRuntimeDelivery.content_sha256,
            ).where(SpiderRuntimeDelivery.strategy_id == scope.strategy_id)
            .order_by(SpiderRuntimeDelivery.sequence)
        ).all()
        tape_before = session.execute(
            select(
                SpiderRuntimeTapeIndex.ordinal,
                SpiderRuntimeTapeIndex.kind,
                SpiderRuntimeTapeIndex.delivery_sequence,
                SpiderRuntimeTapeIndex.transmission_group_id,
            ).where(SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id)
            .order_by(SpiderRuntimeTapeIndex.ordinal)
        ).all()
        projection_before = session.execute(
            select(
                SpiderRuntimeAction.outcome_state,
                SpiderRuntimePlan.plan_status,
                SpiderRuntimePlan.missing_members,
            )
            .join(
                SpiderRuntimePlan,
                SpiderRuntimePlan.plan_id == SpiderRuntimeAction.plan_id,
            )
            .where(SpiderRuntimeAction.action_id == action_id)
        ).one()
        trade_count_before = session.scalar(
            select(func.count()).select_from(Trade).where(Trade.order_id == order_id)
        )
    order_before = (
        order.status,
        order.filled_quantity,
        order.filled_price,
        order.exchange_order_id,
    )

    original_refresh = enabled._refresh_initial_place_projection
    projection_flush_fired = False

    def fail_after_projection_flush(*args: Any, **kwargs: Any):
        nonlocal projection_flush_fired
        projected = original_refresh(*args, **kwargs)
        if kwargs.get("persist") is True:
            assert projected is not None
            projected_by_member = {
                cast(str, row["initial_member_id"]): row for row in projected
            }
            assert projected_by_member[action_id]["last_outcome"] == "SEND_UNKNOWN"
            session = cast(Session, args[0])
            plan = cast(SpiderRuntimePlan, args[3])
            dirty_actions = [
                item for item in session.dirty
                if isinstance(item, SpiderRuntimeAction)
                and item.action_id == action_id
            ]
            assert len(dirty_actions) == 1
            assert plan in session.dirty
            # Force only the actual projection rows through SQL in this caller TX,
            # then fail before repository commit to prove whole-TX rollback.
            session.flush([plan, *dirty_actions])
            assert session.scalar(
                select(SpiderRuntimeAction.outcome_state).where(
                    SpiderRuntimeAction.action_id == action_id
                )
            ) == "SEND_UNKNOWN"
            projection_flush_fired = True
            raise RuntimeError("synthetic projection SQL failure")
        return projected

    enabled._refresh_initial_place_projection = fail_after_projection_flush
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)
    with pytest.raises(RuntimeError, match="synthetic projection SQL failure"):
        execution.process_exchange_order_event(ExchangeOrderEvent(
            status="open",
            product_id=order.product_id,
            client_order_id=order.client_order_id,
            exchange_order_id="provider-id-after-rejection",
        ))

    assert projection_flush_fired is True
    assert enabled.readback(scope, operation_id) == before_readback
    with Session(engine) as session:
        saved_order = session.get(Order, order_id)
        assert saved_order is not None
        assert (
            saved_order.status,
            saved_order.filled_quantity,
            saved_order.filled_price,
            saved_order.exchange_order_id,
        ) == order_before
        assert session.scalar(
            select(func.count()).select_from(Trade).where(Trade.order_id == order_id)
        ) == trade_count_before == 0
        runtime_after = session.execute(
            select(
                SpiderRuntimeScope.last_received_sequence,
                SpiderRuntimeScope.last_tape_sequence,
            ).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).one()
        assert runtime_after == runtime_before
        assert session.execute(
            select(
                SpiderRuntimeDelivery.sequence,
                SpiderRuntimeDelivery.kind,
                SpiderRuntimeDelivery.content_sha256,
            ).where(SpiderRuntimeDelivery.strategy_id == scope.strategy_id)
            .order_by(SpiderRuntimeDelivery.sequence)
        ).all() == delivery_before
        assert session.execute(
            select(
                SpiderRuntimeTapeIndex.ordinal,
                SpiderRuntimeTapeIndex.kind,
                SpiderRuntimeTapeIndex.delivery_sequence,
                SpiderRuntimeTapeIndex.transmission_group_id,
            ).where(SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id)
            .order_by(SpiderRuntimeTapeIndex.ordinal)
        ).all() == tape_before
        projection_after = session.execute(
            select(
                SpiderRuntimeAction.outcome_state,
                SpiderRuntimePlan.plan_status,
                SpiderRuntimePlan.missing_members,
            )
            .join(
                SpiderRuntimePlan,
                SpiderRuntimePlan.plan_id == SpiderRuntimeAction.plan_id,
            )
            .where(SpiderRuntimeAction.action_id == action_id)
        ).one()
        assert projection_after == projection_before
