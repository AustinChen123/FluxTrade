"""Causal PostgreSQL checks for the durable initial PLACE transmission owner."""

import logging
import os
from dataclasses import replace
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
from src.core.backtest.spider_historical_input import HistoricalRunInput
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.orm_models import Order, Trade
from src.core.spider_management import runtime_owner as runtime_owner_module
from src.core.spider_management.orm import (
    SpiderRuntimeAction,
    SpiderRuntimeBinding,
    SpiderRuntimeDelivery,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
    SpiderRuntimeTransmission,
)
from src.core.spider_management.runtime_owner import RuntimeScopeKey, SpiderRuntimeOwner
from src.core.strategy_state_manager import StrategyStateManager
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_financial_delivery_pg import (
    _install_owner,
    _saved_initial_place_request,
    _transport_enabled_owner,
)
from test_spider_control_command_pg import _RedisProbe, _command, _dispatch, _owner_with_lifecycle
from test_spider_operation_request_pg import operation_pg as _operation_pg
from test_spider_model_bootstrap import _admitted_run

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


def _run_with_initial_order_count(per_product: int) -> HistoricalRunInput:
    run = _admitted_run()
    cache = cast(dict[str, object], decode_canonical(run.initial_policy_cache))
    rows = cast(list[dict[str, object]], cache["rows"])
    for row in rows:
        row["單數"] = str(per_product)
    return replace(run, initial_policy_cache=canonical_bytes(cache))


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
    with Session(engine) as session:
        checkpoint_before_send = bytes(session.execute(
            select(SpiderRuntimeScope.source_checkpoint).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one())

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
        assert runtime.control_barrier_sequence == 1
        assert runtime.last_tape_sequence == 2
        tape = session.execute(
            select(SpiderRuntimeTapeIndex)
            .where(SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id)
            .order_by(SpiderRuntimeTapeIndex.ordinal)
        ).scalars().all()
        assert [(row.ordinal, row.kind) for row in tape] == [
            (1, "SEND_MARKER"),
            (2, "DELIVERY"),
        ]
        late_result = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.kind == "TRANSPORT_RESULT",
            )
        ).scalar_one()
        assert late_result.disposition == "SUPPRESSED"
        assert late_result.suppression_reason == "STOP_CONTROL"
        assert late_result.decision_payload is None
        assert late_result.decision_sha256 is None
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeAction).where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.trigger_delivery_id.is_not(None),
            )
        ) == 0
        assert bytes(session.execute(
            select(SpiderRuntimeScope.source_checkpoint).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()) == checkpoint_before_send


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
        ) == 2
        assert session.execute(
            select(SpiderRuntimeTapeIndex.kind)
            .where(SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id)
            .order_by(SpiderRuntimeTapeIndex.ordinal)
        ).scalars().all() == ["SEND_MARKER", "DELIVERY"]


@pytest.mark.parametrize(
    ("orders_per_product", "expected_group_sizes"),
    ((2, (4, 4)), (11, (20, 2, 20, 2))),
)
def test_all_saved_source_groups_complete_and_exact_first_retry_never_resends(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    orders_per_product: int,
    expected_group_sizes: tuple[int, ...],
) -> None:
    engine = operation_pg
    run = _run_with_initial_order_count(orders_per_product)
    owner, scope, operation_id, manager, prepared, initial, *_ = _install_owner(
        engine, admitted_run=run
    )
    source_events = cast(
        dict[str, object], decode_canonical(prepared.initial_source_events_bytes)
    )
    entries = cast(list[dict[str, object]], source_events["entries"])
    assert [entry["source_product_id"] for entry in entries] == list(run.ordered_products)
    original_groups: list[list[dict[str, object]]] = []
    for entry in entries:
        events = cast(list[dict[str, object]], entry["events"])
        send_events = [event for event in events if event.get("kind") == "send"]
        product_groups = [
            cast(list[dict[str, object]], event["orders"]) for event in send_events
        ]
        expected_per_product = (20, 2) if orders_per_product == 11 else (4,)
        assert tuple(len(group) for group in product_groups) == expected_per_product
        original_groups.extend(product_groups)
    assert tuple(len(group) for group in original_groups) == expected_group_sizes

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
        action_rows = session.execute(
            select(SpiderRuntimeAction, SpiderRuntimeBinding, Order)
            .join(
                SpiderRuntimeBinding,
                SpiderRuntimeBinding.action_id == SpiderRuntimeAction.action_id,
            )
            .join(Order, Order.id == SpiderRuntimeAction.local_order_id)
            .where(SpiderRuntimeAction.plan_id == runtime.initial_plan_id)
            .order_by(SpiderRuntimeAction.candidate_ordinal)
        ).all()
    assert tuple(count for _source_group, count in group_sizes) == expected_group_sizes
    assert len(action_rows) == sum(expected_group_sizes)
    mapped_products = {
        cast(str, entry["source_product_id"]): cast(
            str, cast(dict[str, object], entry["instrument_spec"])["product_id"]
        )
        for entry in cast(list[dict[str, object]], prepared.product_map["entries"])
    }
    candidate_index = 0
    for group_id, source_group in enumerate(original_groups, start=1):
        for ordinal, source_order in enumerate(source_group):
            action, binding, order = action_rows[candidate_index]
            candidate_index += 1
            assert action.transmission_group == group_id
            assert action.transmission_ordinal == ordinal
            assert bytes(action.original_payload) == canonical_bytes(source_order)
            assert action.payload_sha256 == sha256(bytes(action.original_payload)).hexdigest()
            assert binding.action_id == action.action_id
            assert binding.local_order_id == action.local_order_id == order.id
            assert binding.source_product_id == action.product_id == source_order["instId"]
            assert binding.source_client_order_id == source_order["clOrdId"]
            assert binding.canonical_product_id == order.product_id == mapped_products[
                cast(str, source_order["instId"])
            ]
            intent = cast(dict[str, object], order.intent_payload)
            assert intent["source_order"] == source_order
            assert intent["source_client_order_id"] == source_order["clOrdId"]
            assert intent["local_client_order_id"] == order.client_order_id
            assert intent["local_client_order_id"] != source_order["clOrdId"]
            binding_bytes = bytes.fromhex(cast(str, intent["provider_binding_bytes_hex"]))
            assert sha256(binding_bytes).hexdigest() == intent["provider_binding_sha256"]
            assert order.side == source_order["side"]
            assert order.price == Decimal(cast(str, source_order["px"]))
            assert order.quantity == Decimal(cast(str, source_order["sz"]))

    enabled = _transport_enabled_owner(owner, scope)

    class AcceptEachGroup:
        def __init__(self) -> None:
            self.calls = 0
            self.requests: list[list[dict[str, object]]] = []

        def privatePostTradeBatchOrders(self, orders: list[dict[str, object]]) -> object:
            self.calls += 1
            self.requests.append(orders)
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
    expected_outbound_batches: list[list[dict[str, object]]] = []
    for source_group_value, expected_count in group_sizes:
        source_group = cast(int, source_group_value)
        request_payload, members = _saved_group_request(engine, scope, source_group)
        assert len(members) == expected_count
        decoded_request = cast(dict[str, object], decode_canonical(bytes(request_payload)))
        durable_members = cast(list[dict[str, object]], decoded_request["members"])
        expected_outbound_batches.append([
            dict(cast(dict[str, object], member["request_payload"]))
            for member in durable_members
        ])
        source_orders = original_groups[source_group - 1]
        assert len(source_orders) == len(members)
        for member, source_order in zip(members, source_orders, strict=True):
            request_order = cast(dict[str, object], member["request_payload"])
            assert request_order == source_order | {"clOrdId": member["wire_clOrdId"]}
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

    assert client.calls == len(group_sizes)
    assert client.requests == expected_outbound_batches

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
    assert client.calls == len(group_sizes)
    assert enabled.readback(scope, operation_id) == complete
    with Session(engine) as session:
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeTransmission).where(
                SpiderRuntimeTransmission.strategy_id == scope.strategy_id
            )
        ) == len(group_sizes)
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            )
        ) == 2 * len(group_sizes)
        tape = session.execute(
            select(SpiderRuntimeTapeIndex.kind)
            .where(SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id)
            .order_by(SpiderRuntimeTapeIndex.ordinal)
        ).scalars().all()
        assert tape == [kind for _ in group_sizes for kind in ("SEND_MARKER", "DELIVERY")]

    if orders_per_product == 11:
        first_group, _first_payload, _first_outcome = saved[0]
        _request_payload, first_members = _saved_group_request(engine, scope, first_group)
        target = first_members[0]
        provider_id = "synthetic-group-1-0"
        order = manager.repo.get_order(cast(str, target["local_order_id"]))
        assert order is not None
        execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)
        assert execution.process_exchange_order_event(_filled_event(
            order,
            provider_id=provider_id,
            trade_id="two-product-source-late-fill",
        ))["action"] == "applied"

        after_fill = enabled.readback(scope, operation_id)
        assert after_fill is not None
        assert after_fill["initial_plan_status"] == "COMPLETE"
        assert after_fill["initial_plan_missing_members"] == []
        assert after_fill["phase"] == "INCOMPLETE"
        assert after_fill["trading_ready"] is False
        with Session(engine) as session:
            saved_order = session.get(Order, order.id)
            trade = session.execute(
                select(Trade).where(
                    Trade.exchange_trade_id == "two-product-source-late-fill"
                )
            ).scalar_one()
            assert saved_order is not None
            assert saved_order.exchange_order_id == provider_id
            assert saved_order.status == "FILLED"
            assert saved_order.filled_quantity == order.quantity
            assert saved_order.filled_price == order.price
            assert trade.order_id == order.id
            assert trade.quantity == order.quantity
            assert trade.price == order.price
            assert trade.fee == Decimal("0.01")
            receipt_before_stop = _financial_receipt_snapshot(session, scope)
            assert len(receipt_before_stop) == 6
            kind_rows = session.execute(
                select(SpiderRuntimeDelivery.sequence, SpiderRuntimeDelivery.kind).where(
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id
                )
            ).all()
            receipt_kind_by_sequence: dict[int, str] = {
                cast(int, sequence): cast(str, kind) for sequence, kind in kind_rows
            }
            assert sum(
                kind == "TRANSPORT_RESULT" for kind in receipt_kind_by_sequence.values()
            ) == 4
            assert sum(
                kind in {"FILL", "ORDER_STATE"}
                for kind in receipt_kind_by_sequence.values()
            ) == 2
            fill_receipt = next(row for row in receipt_before_stop if row[4] == trade.id)
            assert fill_receipt[2] == target["action_id"]
            assert fill_receipt[3] == order.id
            assert fill_receipt[7] == "PENDING"
            assert fill_receipt[8] is None
            fill_payload_bytes = cast(bytes, fill_receipt[6])
            assert fill_receipt[5] == sha256(fill_payload_bytes).hexdigest()
            fill_payload = cast(dict[str, object], decode_canonical(fill_payload_bytes))
            assert fill_payload["kind"] == "FILL"
            assert fill_payload["trade_id"] == trade.id
            assert fill_payload["binding_resolution"] == "BOUND"
            assert fill_payload["reason_code"] is None
            fill_binding = cast(dict[str, object], fill_payload["binding"])
            assert fill_binding["action_id"] == target["action_id"]
            assert fill_binding["local_order_id"] == order.id
            received_sequence, consumed_sequence = session.execute(
                select(
                    SpiderRuntimeScope.last_received_sequence,
                    SpiderRuntimeScope.last_consumed_sequence,
                ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
            ).one()
            assert received_sequence > consumed_sequence
            financial_before_stop = (
                saved_order.id,
                saved_order.exchange_order_id,
                saved_order.status,
                saved_order.filled_quantity,
                saved_order.filled_price,
                trade.id,
                trade.exchange_trade_id,
                trade.quantity,
                trade.price,
                trade.fee,
                trade.fee_asset,
            )

        class RedisProbe:
            def publish(self, _channel: str, _message: str) -> int:
                return 1

        factory = sessionmaker(engine)
        lifecycle_lock = RLock()
        stop_owner = SpiderRuntimeOwner(
            factory,
            manager,
            owned_scopes=(scope,),
            model_sources_by_confirmation=owner._model_sources,
            strategy_state_manager=StrategyStateManager(factory, RedisProbe()),
            lifecycle_lock=lambda _strategy_id: lifecycle_lock,
            order_event_lock=RLock(),
            transport_response_classifier=classify_okx_batch_response,
        )
        stopped = _dispatch(
            stop_owner,
            _command(
                scope,
                key="two-product-late-fill-stop",
                version=cast(int, initial["state_version"]),
            ),
        )
        assert stopped.success is True and stopped.completed is True
        assert stop_owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
        after_stop = stop_owner.readback(scope, operation_id)
        assert after_stop is not None
        assert after_stop["runtime_control"] == "STOP"
        assert after_stop["initial_plan_status"] == "COMPLETE"
        assert after_stop["initial_plan_missing_members"] == []
        assert after_stop["phase"] == "INCOMPLETE"
        assert after_stop["trading_ready"] is False
        assert after_stop["checkpoint_sha256"] == complete["checkpoint_sha256"]
        assert client.calls == len(group_sizes)
        with Session(engine) as session:
            saved_order = session.get(Order, order.id)
            trade = session.execute(
                select(Trade).where(
                    Trade.exchange_trade_id == "two-product-source-late-fill"
                )
            ).scalar_one()
            pending_count = session.scalar(
                select(func.count()).select_from(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                    SpiderRuntimeDelivery.disposition == "PENDING",
                )
            )
            receipt_after_stop = _financial_receipt_snapshot(session, scope)
            received_sequence, consumed_sequence = session.execute(
                select(
                    SpiderRuntimeScope.last_received_sequence,
                    SpiderRuntimeScope.last_consumed_sequence,
                ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
            ).one()
            assert saved_order is not None
            assert (
                saved_order.id,
                saved_order.exchange_order_id,
                saved_order.status,
                saved_order.filled_quantity,
                saved_order.filled_price,
                trade.id,
                trade.exchange_trade_id,
                trade.quantity,
                trade.price,
                trade.fee,
                trade.fee_asset,
            ) == financial_before_stop
            assert pending_count == 0
            assert len(receipt_after_stop) == len(receipt_before_stop)
            for before, after in zip(
                receipt_before_stop, receipt_after_stop, strict=True
            ):
                assert before[:7] == after[:7]
                receipt_kind = receipt_kind_by_sequence[cast(int, before[0])]
                if receipt_kind == "TRANSPORT_RESULT":
                    assert before == after
                else:
                    assert before[7] == "PENDING" and before[8] is None
                    assert after[7] == "SUPPRESSED"
                    assert after[8] == "STOP_CONTROL"
            assert received_sequence == consumed_sequence

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


def _execution_for_owner(
    owner, engine: Engine, clock, adapter, *, observe_order_events: bool = False
) -> ExecutionEngine:
    return ExecutionEngine(
        db_session=None,
        clock=clock,
        adapter=adapter,
        db_session_factory=sessionmaker(engine),
        repository_transaction_participant=owner.participate_in_order_transaction,
        owned_order_event_observer=(
            owner.observe_order_event if observe_order_events else None
        ),
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


def _normal_n3_run() -> HistoricalRunInput:
    """Reuse the admitted native fixture with the contract's exact N3 source cache."""
    run = _admitted_run()
    cache = cast(dict[str, object], decode_canonical(run.initial_policy_cache))
    capital = cast(dict[str, object], cache["capital"])
    capital.update(total="10000", usdt="10000", avail="10000", earn="0", position="0")
    rows = cast(list[dict[str, object]], cache["rows"])
    row = next(value for value in rows if value.get("name") == "A-USDT-SWAP")
    row.update(leverage="6", 歩差="1", 單數="3", hold上限="0.8", hold下限="-0.8", hold="0")
    markets = cast(list[dict[str, object]], cache["markets"])
    market = next(value for value in markets if value.get("product_id") == "A-USDT-SWAP")
    market.update(price="100", ctVal="1", lotSz="0.001", minSz="0.001",
                  increment="0.01", ratioHL="0.1", state="live", instIdCode=1)
    return replace(run, initial_policy_cache=canonical_bytes(cache))


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


def test_normal_tx_b_after_execution_fill_before_rest_ack_derives_local_intents_and_cold_proof(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
) -> None:
    """Exercise TXA receipts, ordered source callbacks, TXB actions and readback."""
    engine = operation_pg
    owner, scope, operation_id, order_manager, _prepared, _initial, *_ = _install_owner(
        engine, admitted_run=_normal_n3_run()
    )
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    candidate = next(
        member for member in members
        if cast(dict[str, object], member["request_payload"])["side"] == "buy"
        and Decimal(cast(str, cast(dict[str, object], member["request_payload"])["px"])) == Decimal("50")
    )
    order = order_manager.repo.get_order(cast(str, candidate["local_order_id"]))
    assert order is not None and order.price is not None
    assert order.price == Decimal("50") and order.quantity == Decimal("100")
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)
    first_quantity = order.quantity * Decimal("0.6")
    second_quantity = order.quantity - first_quantity

    class FillBeforeAckClient:
        calls = 0

        def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            self.calls += 1
            assert execution.process_exchange_order_event(ExchangeOrderEvent(
                status="partially_filled",
                product_id=order.product_id,
                client_order_id=order.client_order_id,
                exchange_order_id="normal-fill-provider-order",
                cumulative_filled_quantity=first_quantity,
                cumulative_average_price=Decimal("48"),
                last_fill_quantity=first_quantity,
                last_fill_price=Decimal("48"),
                fee=Decimal("0.01"),
                fee_asset="USDT",
                last_fill_exchange_trade_id="normal-fill-60-at-48",
            ))["action"] == "applied"
            assert execution.process_exchange_order_event(ExchangeOrderEvent(
                status="filled",
                product_id=order.product_id,
                client_order_id=order.client_order_id,
                exchange_order_id="normal-fill-provider-order",
                cumulative_filled_quantity=order.quantity,
                cumulative_average_price=Decimal("48.4"),
                last_fill_quantity=second_quantity,
                last_fill_price=Decimal("49"),
                fee=Decimal("0.02"),
                fee_asset="USDT",
                last_fill_exchange_trade_id="normal-fill-40-at-49",
            ))["action"] == "applied"
            # Exact duplicate has no new financial receipt and cannot invoke
            # the source full callback a second time.
            duplicate = execution.process_exchange_order_event(ExchangeOrderEvent(
                status="filled",
                product_id=order.product_id,
                client_order_id=order.client_order_id,
                exchange_order_id="normal-fill-provider-order",
                cumulative_filled_quantity=order.quantity,
                cumulative_average_price=Decimal("48.4"),
                last_fill_quantity=second_quantity,
                last_fill_price=Decimal("49"),
                fee=Decimal("0.02"),
                fee_asset="USDT",
                last_fill_exchange_trade_id="normal-fill-40-at-49",
            ))
            assert duplicate["fill_quantity"] == Decimal("0")
            return {
                "code": "0",
                "data": [
                    {
                        "clOrdId": member["wire_clOrdId"],
                        "ordId": (
                            "normal-fill-provider-order"
                            if member["action_id"] == candidate["action_id"]
                            else f"normal-provider-{ordinal}"
                        ),
                        "sCode": "0",
                    }
                    for ordinal, member in enumerate(members)
                ],
                "msg": "synthetic acknowledgement after fills",
            }

        def privatePostTradeCancelBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            raise AssertionError("TX B persists cancel intents; it does not transmit them")

    offline = FillBeforeAckClient()
    result = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, offline)),
    )
    assert result.disposition == "RECORDED"
    assert offline.calls == 1

    with Session(engine) as session:
        trades = session.execute(
            select(Trade)
            .join(SpiderRuntimeDelivery, SpiderRuntimeDelivery.trade_id == Trade.id)
            .where(
                Trade.order_id == order.id,
                SpiderRuntimeDelivery.kind == "FILL",
            )
            .order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        assert [(trade.quantity, trade.price) for trade in trades] == [
            (first_quantity, Decimal("48")), (second_quantity, Decimal("49"))
        ]
        assert (
            sum((trade.quantity * trade.price for trade in trades), Decimal("0"))
            / order.quantity
        ) == Decimal("48.4")
        runtime = session.execute(
            select(SpiderRuntimeScope).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).scalar_one()
        deliveries = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        normal = [row for row in deliveries if row.kind in {"FILL", "ORDER_STATE", "TRANSPORT_RESULT"}]
        assert normal and all(row.disposition == "CONSUMED" for row in normal)
        decisions = [
            cast(dict[str, object], decode_canonical(bytes(row.decision_payload)))
            for row in normal if row.decision_payload is not None
        ]
        full_decisions = [item for item in decisions if item["full_callback_key"] is not None]
        assert len(full_decisions) == 1
        full = full_decisions[0]
        source_input = cast(dict[str, object], full["source_input"])
        source_order = cast(dict[str, object], source_input["order"])
        assert Decimal(cast(str, source_order["px"])) == order.price
        assert source_order["fillPx"] == "49"
        assert Decimal(cast(str, source_order["accFillSz"])) == order.quantity
        events = cast(list[dict[str, object]], full["events"])
        assert [row["kind"] for row in events if row["kind"] in {"cancel", "send"}] == [
            "cancel", "send"
        ]
        cancel_sources = cast(list[dict[str, object]], events[0]["orders"])
        assert [row["ordId"] for row in cancel_sources] == ["0000065010"]
        child_sources = cast(list[dict[str, object]], events[1]["orders"])
        assert [(row["px"], row["sz"], row["side"], row["clOrdId"]) for row in child_sources] == [
            ("6.25", "355.555", "buy", "0000115003"),
            ("100", "150", "sell", "0000125007"),
        ]
        assert runtime.last_received_sequence == runtime.last_consumed_sequence
        derived = session.execute(
            select(SpiderRuntimeAction).where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.trigger_delivery_id.is_not(None),
            ).order_by(SpiderRuntimeAction.candidate_ordinal)
        ).scalars().all()
        assert {action.operation for action in derived} >= {"PLACE", "CANCEL"}
        cancel = next(action for action in derived if action.operation == "CANCEL")
        assert cancel.local_order_id is not None and cancel.role == "GRID"
        assert cancel.trigger_delivery_id is not None
        assert cancel.parent_action_id is not None
        assert session.execute(
            select(SpiderRuntimeBinding).where(
                SpiderRuntimeBinding.action_id == cancel.action_id
            )
        ).scalar_one_or_none() is None
        full_key_count = session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.full_callback_key == full["full_callback_key"],
            )
        )
        assert full_key_count == 1

    # The ordinary query exposes only explicitly bound PLACE actions. CANCEL
    # remains an immutable intent and cannot fabricate a local Order/binding.
    from test_spider_order_query_pg import _query_service

    query_status, query_body = _query_service(engine, scope).orders(
        f"strategy_id={scope.strategy_id}"
    )
    assert query_status == 200, query_body
    query_orders = cast(list[dict[str, object]], query_body["orders"])
    with Session(engine) as session:
        derived_place_orders = set(session.scalars(
            select(SpiderRuntimeAction.local_order_id).where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.trigger_delivery_id.is_not(None),
                SpiderRuntimeAction.operation == "PLACE",
                SpiderRuntimeAction.local_order_id.is_not(None),
            )
        ))
    assert derived_place_orders
    assert derived_place_orders <= {row["local_order_id"] for row in query_orders}

    cold = _transport_enabled_owner(enabled, scope)
    proved = cold.readback(scope, operation_id)
    assert proved is not None
    assert proved["trading_ready"] is False
    prior_readback = enabled.readback(scope, operation_id)
    assert prior_readback is not None
    assert proved["checkpoint_sha256"] == prior_readback["checkpoint_sha256"]


@pytest.mark.parametrize("terminal_status", ["canceled", "rejected"])
def test_rest_ack_uses_terminal_receipt_before_its_delivery_cutoff(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    terminal_status: str,
) -> None:
    """A proved cancel/failure before REST ACK cannot be moved back to live."""
    engine = operation_pg
    owner, scope, _operation_id, order_manager, _prepared, _initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    target = members[0]
    order = order_manager.repo.get_order(cast(str, target["local_order_id"]))
    assert order is not None
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status=terminal_status,
        product_id=order.product_id,
        client_order_id=order.client_order_id,
    ))["action"] == "applied"
    client = _OfflinePlaceClient(_accepted_envelope(members))
    result = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, client)),
    )
    assert result.disposition == "RECORDED"

    with Session(engine) as session:
        target_alias = session.execute(
            select(SpiderRuntimeBinding.source_client_order_id).where(
                SpiderRuntimeBinding.action_id == target["action_id"]
            )
        ).scalar_one()
        result_decision = session.execute(
            select(SpiderRuntimeDelivery.decision_payload).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.kind == "TRANSPORT_RESULT",
            ).order_by(SpiderRuntimeDelivery.sequence.desc())
        ).scalars().first()
        assert type(result_decision) is bytes
        decision = cast(dict[str, object], decode_canonical(result_decision))
        source_input = cast(dict[str, object], decision["source_input"])
        assert source_input["type"] == "REST_ACK"
        entries = cast(list[dict[str, object]], source_input["entries"])
        assert target_alias not in {entry["clOrdId"] for entry in entries}
        assert entries
        assert enabled.readback(scope, _operation_id) is not None


def test_rest_ack_decision_remains_before_later_terminal_receipt(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
) -> None:
    """A later terminal status cannot rewrite the saved earlier ACK input."""
    engine = operation_pg
    owner, scope, operation_id, order_manager, _prepared, _initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    target = members[0]
    order = order_manager.repo.get_order(cast(str, target["local_order_id"]))
    assert order is not None
    client = _OfflinePlaceClient(_accepted_envelope(members))
    result = enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, client)),
    )
    assert result.disposition == "RECORDED"
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="canceled",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
    ))["action"] == "applied"

    with Session(engine) as session:
        target_alias = session.execute(
            select(SpiderRuntimeBinding.source_client_order_id).where(
                SpiderRuntimeBinding.action_id == target["action_id"]
            )
        ).scalar_one()
        result_decision = session.execute(
            select(SpiderRuntimeDelivery.decision_payload).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.kind == "TRANSPORT_RESULT",
            ).order_by(SpiderRuntimeDelivery.sequence.desc())
        ).scalars().first()
        assert type(result_decision) is bytes
        decision = cast(dict[str, object], decode_canonical(result_decision))
        entries = cast(
            list[dict[str, object]],
            cast(dict[str, object], decision["source_input"])["entries"],
        )
        assert target_alias in {entry["clOrdId"] for entry in entries}
        assert enabled.readback(scope, operation_id) is not None


@pytest.mark.parametrize("commit_fault", ["rollback", "lost_commit"])
def test_normal_tx_b_rollback_or_lost_commit_replays_once_from_cold_owner(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    commit_fault: str,
) -> None:
    """A failed TX B is retried from durable receipts, never source-local memory."""
    engine = operation_pg
    owner, scope, operation_id, order_manager, _prepared, _initial, *_ = _install_owner(engine)
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    enabled = _transport_enabled_owner(owner, scope)
    target = members[0]
    order = order_manager.repo.get_order(cast(str, target["local_order_id"]))
    assert order is not None and order.price is not None
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)
    fired = {"count": 0}

    def fail_before_flush(session: Session, _flush_context: object, _instances: object) -> None:
        staged_decision = any(
            isinstance(row, SpiderRuntimeDelivery)
            and row.kind in {"FILL", "ORDER_STATE", "TRANSPORT_RESULT"}
            and row.decision_payload is not None
            for row in (*session.new, *session.dirty)
        )
        if staged_decision:
            fired["count"] += 1
            session.info["spider_normal_tx_b_commit"] = True
            if commit_fault == "rollback":
                raise RuntimeError("injected normal TX B rollback")

    def fail_after_commit(session: Session) -> None:
        if session.info.pop("spider_normal_tx_b_commit", False) and commit_fault == "lost_commit":
            fired["count"] += 1
            raise RuntimeError("injected lost TX B commit response")

    event.listen(Session, "before_flush", fail_before_flush)
    event.listen(Session, "after_commit", fail_after_commit)
    try:
        class FillBeforeAckClient:
            def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
                quantity = order.quantity
                assert execution.process_exchange_order_event(ExchangeOrderEvent(
                    status="filled",
                    product_id=order.product_id,
                    client_order_id=order.client_order_id,
                    exchange_order_id="normal-replay-provider-order",
                    cumulative_filled_quantity=quantity,
                    cumulative_average_price=cast(Decimal, order.price),
                    last_fill_quantity=quantity,
                    last_fill_price=cast(Decimal, order.price),
                    fee=Decimal("0.01"),
                    fee_asset="USDT",
                    last_fill_exchange_trade_id="normal-replay-trade",
                ))["action"] == "applied"
                return {
                    "code": "0",
                    "data": [
                        {"clOrdId": member["wire_clOrdId"], "ordId": f"replay-{i}", "sCode": "0"}
                        for i, member in enumerate(members)
                    ],
                    "msg": "synthetic ACK after financial fact",
                }

            def privatePostTradeCancelBatchOrders(self, _orders: list[dict[str, object]]) -> object:
                raise AssertionError("normal callback records a cancel intent only")

        enabled.transmit_initial_place_group(
            scope,
            source_group,
            scope.execution_scope_id,
            request_payload,
            OkxSpiderBatchTransport(cast(Any, FillBeforeAckClient())),
        )
    finally:
        event.remove(Session, "before_flush", fail_before_flush)
        event.remove(Session, "after_commit", fail_after_commit)

    assert fired["count"] >= 1
    with Session(engine) as session:
        runtime_before_replay = session.execute(
            select(SpiderRuntimeScope).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).scalar_one()
        received_before_replay = runtime_before_replay.last_received_sequence
        consumed_before_replay = runtime_before_replay.last_consumed_sequence
        trade_before_replay = session.execute(
            select(Trade).where(Trade.exchange_trade_id == "normal-replay-trade")
        ).scalar_one()
        saved_fact = (trade_before_replay.id, trade_before_replay.quantity, trade_before_replay.price)
        receipt_count = session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            )
        )
    if commit_fault == "rollback":
        assert consumed_before_replay < received_before_replay
    else:
        assert consumed_before_replay == received_before_replay

    # Reconstructing the owner exercises checkpoint/decision/action proofs with
    # no process-local callbacks. Any committed decision keeps its full key once.
    cold = _transport_enabled_owner(owner, scope)
    cold.restore_owned_runtimes()
    cold_readback = cold.readback(scope, operation_id)
    assert cold_readback is not None and cold_readback["trading_ready"] is False
    with Session(engine) as session:
        runtime_after_replay = session.execute(
            select(SpiderRuntimeScope).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).scalar_one()
        trade_after_replay = session.execute(
            select(Trade).where(Trade.exchange_trade_id == "normal-replay-trade")
        ).scalar_one()
        assert runtime_after_replay.last_consumed_sequence == runtime_after_replay.last_received_sequence
        assert (trade_after_replay.id, trade_after_replay.quantity, trade_after_replay.price) == saved_fact
        full_claims = session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.full_callback_key.is_not(None),
            )
        )
        assert full_claims == 1
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            )
        ) == receipt_count


def _record_n3_policy_failure(
    engine: Engine,
    mock_clock,
    mock_exchange_adapter,
    monkeypatch: pytest.MonkeyPatch,
    *,
    with_lifecycle: bool = False,
) -> tuple[SpiderRuntimeOwner, RuntimeScopeKey, str, str, str, bytes, int]:
    base_owner, scope, operation_id, order_manager, _prepared, _initial, *_ = _install_owner(
        engine, admitted_run=_normal_n3_run()
    )
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    if with_lifecycle:
        class RedisProbe:
            def publish(self, _channel: str, _message: str) -> int:
                return 1

        lifecycle_lock = RLock()
        order_event_lock = RLock()
        enabled = SpiderRuntimeOwner(
            base_owner._sessions,
            base_owner._order_manager,
            owned_scopes=(scope,),
            model_sources_by_confirmation=base_owner._model_sources,
            strategy_state_manager=StrategyStateManager(
                sessionmaker(engine), RedisProbe()
            ),
            lifecycle_lock=lambda _strategy_id: lifecycle_lock,
            order_event_lock=order_event_lock,
            transport_response_classifier=classify_okx_batch_response,
        )
    else:
        enabled = _transport_enabled_owner(base_owner, scope)
    target = next(
        member for member in members
        if cast(dict[str, object], member["request_payload"]).get("side") == "buy"
        and cast(dict[str, object], member["request_payload"]).get("px") == "50"
    )
    order = order_manager.repo.get_order(cast(str, target["local_order_id"]))
    assert order is not None
    execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        db_session_factory=sessionmaker(engine),
        repository_transaction_participant=enabled.participate_in_order_transaction,
        owned_order_event_observer=(
            enabled.observe_order_event if with_lifecycle else None
        ),
        is_backtest=True,
    )
    before_fill: dict[str, object] = {}

    original_callback = runtime_owner_module.invoke_saved_source_callback

    def fail_after_saved_callback(policy: object, source_input: dict[str, object]) -> None:
        order_input = source_input.get("order")
        if (
            source_input.get("type") == "ORDERS"
            and type(order_input) is dict
            and order_input.get("state") == "filled"
        ):
            with Session(engine) as session:
                runtime_before_callback = session.execute(
                    select(
                        SpiderRuntimeScope.source_checkpoint,
                        SpiderRuntimeScope.last_consumed_sequence,
                    ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
                ).one()
                before_fill["checkpoint"] = bytes(runtime_before_callback.source_checkpoint)
                before_fill["consumed"] = runtime_before_callback.last_consumed_sequence
        original_callback(policy, source_input)
        if (
            source_input.get("type") == "ORDERS"
            and type(order_input) is dict
            and order_input.get("state") == "filled"
        ):
            raise RuntimeError("test failure after saved Policy callback")

    monkeypatch.setattr(
        runtime_owner_module, "invoke_saved_source_callback", fail_after_saved_callback
    )

    class FillBeforeAckClient:
        calls = 0

        def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            self.calls += 1
            assert execution.process_exchange_order_event(_filled_event(
                order,
                provider_id="normal-policy-failure-provider",
                trade_id="normal-policy-failure-trade",
            ))["action"] == "applied"
            response = _accepted_envelope(members)
            rows = cast(list[dict[str, object]], response["data"])
            selected = next(
                row for row in rows
                if row["clOrdId"] == target["wire_clOrdId"]
            )
            selected["ordId"] = "normal-policy-failure-provider"
            return response

        def privatePostTradeCancelBatchOrders(
            self, _orders: list[dict[str, object]]
        ) -> object:
            raise AssertionError("PLACE transport must not call CANCEL")

    client = FillBeforeAckClient()
    outcome = enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, client)),
    )
    assert outcome.disposition == "RECORDED"
    assert client.calls == 1
    with Session(engine) as session:
        failure = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.kind.in_(("FILL", "ORDER_STATE")),
                SpiderRuntimeDelivery.disposition == "PENDING",
                SpiderRuntimeDelivery.decision_payload.is_not(None),
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().first()
        assert failure is not None
        assert failure.disposition == "PENDING"
        assert failure.decision_payload is not None
        assert failure.full_callback_key is None
        failure_decision = cast(
            dict[str, object], decode_canonical(bytes(failure.decision_payload))
        )
        assert failure_decision["outcome"] == "POLICY_FAILED"
        initial_plan_id = session.execute(
            select(SpiderRuntimeScope.initial_plan_id).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeAction).where(
                SpiderRuntimeAction.plan_id == initial_plan_id,
                SpiderRuntimeAction.trigger_delivery_id == failure.delivery_id,
            )
        ) == 0
        assert session.scalar(
            select(func.count()).select_from(Trade).where(
                Trade.exchange_trade_id == "normal-policy-failure-trade"
            )
        ) == 1
    assert type(before_fill.get("checkpoint")) is bytes
    assert type(before_fill.get("consumed")) is int
    return (
        enabled, scope, operation_id, failure.delivery_id,
        "normal-policy-failure-trade",
        cast(bytes, before_fill["checkpoint"]),
        cast(int, before_fill["consumed"]),
    )


def test_pending_policy_failure_is_reproved_as_cold_financial_prefix(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, failure_id, trade_id, checkpoint_before, cursor_before = (
        _record_n3_policy_failure(engine, mock_clock, mock_exchange_adapter, monkeypatch)
    )
    with Session(engine) as session:
        failure = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.delivery_id == failure_id,
            )
        ).scalar_one()
        assert failure.disposition == "PENDING"
        assert failure.decision_payload is not None
        saved_decision = cast(dict[str, object], decode_canonical(bytes(failure.decision_payload)))
        assert saved_decision["outcome"] == "POLICY_FAILED"
        assert saved_decision["reason_code"] == "SOURCE_POLICY_FAILED"
        assert saved_decision["full_callback_key"] is not None
        assert saved_decision["candidates"] == []
        assert session.execute(
            select(SpiderRuntimeScope.source_checkpoint).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one() == checkpoint_before
        failure_cursor = session.execute(
            select(SpiderRuntimeScope.last_consumed_sequence).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        assert failure_cursor == cursor_before
        assert cursor_before < failure.sequence
        trade_before = session.execute(
            select(Trade).where(Trade.exchange_trade_id == trade_id)
        ).scalar_one()
        trade_fact = (trade_before.id, trade_before.order_id, trade_before.quantity, trade_before.price)

    cold = _transport_enabled_owner(owner, scope)
    proved = cold.readback(scope, operation_id)
    assert proved is not None and proved["trading_ready"] is False
    with Session(engine) as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).scalar_one()
        trade_after = session.execute(
            select(Trade).where(Trade.exchange_trade_id == trade_id)
        ).scalar_one()
        assert runtime.last_consumed_sequence == cursor_before
        assert bytes(runtime.source_checkpoint) == checkpoint_before
        assert (trade_after.id, trade_after.order_id, trade_after.quantity, trade_after.price) == trade_fact


def test_existing_stop_suppresses_pending_policy_failure_without_replaying_it(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, failure_id, trade_id, _checkpoint, _cursor = (
        _record_n3_policy_failure(
            engine,
            mock_clock,
            mock_exchange_adapter,
            monkeypatch,
            with_lifecycle=True,
        )
    )
    before = owner.readback(scope, operation_id)
    assert before is not None and before["runtime_control"] == "RUN"
    state_version = cast(int, before["state_version"])
    with Session(engine) as session:
        failure = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.delivery_id == failure_id,
            )
        ).scalar_one()
        assert failure.disposition == "PENDING"
        assert failure.decision_payload is not None
        assert failure.decision_sha256 is not None
        decision_bytes = bytes(failure.decision_payload)
        decision_hash = failure.decision_sha256
        runtime = session.execute(
            select(
                SpiderRuntimeScope.source_checkpoint,
                SpiderRuntimeScope.last_consumed_sequence,
                SpiderRuntimeScope.last_received_sequence,
            ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).one()
        assert runtime.source_checkpoint is not None
        checkpoint_before_stop = bytes(runtime.source_checkpoint)
        consumed_before_stop = runtime.last_consumed_sequence
        received_before_stop = runtime.last_received_sequence
        trade = session.execute(
            select(Trade).where(Trade.exchange_trade_id == trade_id)
        ).scalar_one()
        trade_fact = (trade.id, trade.order_id, trade.quantity, trade.price, trade.fee)
        original_action_ids = tuple(session.execute(
            select(SpiderRuntimeAction.action_id).where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.trigger_delivery_id.is_(None),
            ).order_by(SpiderRuntimeAction.candidate_ordinal)
        ).scalars().all())
    assert consumed_before_stop < received_before_stop

    stopped = _dispatch(
        owner,
        _command(
            scope,
            key="pending-policy-failure-stop",
            version=state_version,
        ),
    )
    assert stopped.success and stopped.completed
    stopped_readback = owner.readback(scope, operation_id)
    assert stopped_readback is not None
    assert stopped_readback["runtime_control"] == "STOP"
    assert stopped_readback["trading_ready"] is False
    assert stopped_readback["checkpoint_sha256"] == before["checkpoint_sha256"]
    assert owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
    cold_readback = owner.readback(scope, operation_id)
    assert cold_readback is not None
    expected_cold_readback = dict(stopped_readback)
    expected_cold_readback["consumed_sequence"] = received_before_stop
    assert cold_readback == expected_cold_readback

    with Session(engine) as session:
        failure = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.delivery_id == failure_id,
            )
        ).scalar_one()
        assert failure.disposition == "SUPPRESSED"
        assert failure.suppression_reason == "STOP_CONTROL"
        assert failure.decision_payload is not None
        assert bytes(failure.decision_payload) == decision_bytes
        assert failure.decision_sha256 == decision_hash
        runtime = session.execute(
            select(SpiderRuntimeScope).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).scalar_one()
        assert runtime.last_consumed_sequence == runtime.last_received_sequence
        assert runtime.last_received_sequence == received_before_stop
        assert bytes(runtime.source_checkpoint) == checkpoint_before_stop
        trade = session.execute(
            select(Trade).where(Trade.exchange_trade_id == trade_id)
        ).scalar_one()
        assert (trade.id, trade.order_id, trade.quantity, trade.price, trade.fee) == trade_fact
        current_action_ids = tuple(session.execute(
            select(SpiderRuntimeAction.action_id).where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.trigger_delivery_id.is_(None),
            ).order_by(SpiderRuntimeAction.candidate_ordinal)
        ).scalars().all())
        assert current_action_ids == original_action_ids
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeAction).where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.trigger_delivery_id.is_not(None),
            )
        ) == 0
        suppressed_after_cursor = session.execute(
            select(SpiderRuntimeDelivery.kind, SpiderRuntimeDelivery.disposition,
                   SpiderRuntimeDelivery.suppression_reason)
            .where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.sequence > consumed_before_stop,
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).all()
        assert suppressed_after_cursor
        assert all(
            disposition == "SUPPRESSED" and reason == "STOP_CONTROL"
            for _kind, disposition, reason in suppressed_after_cursor
        )


@pytest.mark.parametrize("changed_field", ["event_prefix", "full_callback_key", "reason_code"])
def test_cold_proof_rejects_inconsistent_pending_policy_failure_evidence(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    monkeypatch: pytest.MonkeyPatch,
    changed_field: str,
) -> None:
    owner, scope, operation_id, failure_id, _trade_id, _checkpoint, _cursor = _record_n3_policy_failure(
        operation_pg, mock_clock, mock_exchange_adapter, monkeypatch
    )
    with Session(operation_pg) as session:
        failure = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.delivery_id == failure_id,
            )
        ).scalar_one()
        assert failure.decision_payload is not None
        decision = cast(dict[str, object], decode_canonical(bytes(failure.decision_payload)))
        if changed_field == "event_prefix":
            cast(list[object], decision["events"]).append({"kind": "send", "orders": []})
        elif changed_field == "full_callback_key":
            decision["full_callback_key"] = "0" * 64
        else:
            decision["reason_code"] = "OTHER_FAILURE"
        corrupted = canonical_bytes(decision)
        failure.decision_payload = corrupted
        failure.decision_sha256 = sha256(corrupted).hexdigest()
        session.commit()
    assert _transport_enabled_owner(owner, scope).readback(scope, operation_id) is None


def _record_n3_full_decision(
    engine: Engine,
    mock_clock,
    mock_exchange_adapter,
) -> tuple[SpiderRuntimeOwner, RuntimeScopeKey, str, str]:
    owner, scope, operation_id, order_manager, _prepared, _initial, *_ = _install_owner(
        engine, admitted_run=_normal_n3_run()
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
    execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)

    class FillBeforeAckClient:
        def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            assert execution.process_exchange_order_event(_filled_event(
                order,
                provider_id="normal-candidate-proof-provider",
                trade_id="normal-candidate-proof-trade",
            ))["action"] == "applied"
            return _accepted_envelope(members)

        def privatePostTradeCancelBatchOrders(
            self, _orders: list[dict[str, object]]
        ) -> object:
            raise AssertionError("PLACE transport must not call CANCEL")

    assert enabled.transmit_initial_place_group(
        scope,
        source_group,
        scope.execution_scope_id,
        request_payload,
        OkxSpiderBatchTransport(cast(Any, FillBeforeAckClient())),
    ).disposition == "RECORDED"
    with Session(engine) as session:
        rows = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.decision_payload.is_not(None),
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        full = next(
            row for row in rows
            if type(row.decision_payload) is bytes
            and cast(dict[str, object], decode_canonical(row.decision_payload))[
                "full_callback_key"
            ] is not None
        )
        assert full.decision_payload is not None
        decision = cast(dict[str, object], decode_canonical(bytes(full.decision_payload)))
        assert any(
            candidate.get("operation") == "CANCEL"
            for candidate in cast(list[dict[str, object]], decision["candidates"])
        )
        assert any(
            candidate.get("operation") == "PLACE"
            for candidate in cast(list[dict[str, object]], decision["candidates"])
        )
    assert enabled.readback(scope, operation_id) is not None
    return owner, scope, operation_id, full.delivery_id


def _saved_normal_place_group(engine: Engine, scope: RuntimeScopeKey, delivery_id: str):
    with Session(engine) as session:
        delivery = session.execute(select(SpiderRuntimeDelivery).where(
            SpiderRuntimeDelivery.delivery_id == delivery_id,
        )).scalar_one()
        assert delivery.decision_payload is not None
        decision = cast(dict[str, object], decode_canonical(bytes(delivery.decision_payload)))
        runtime = session.execute(select(SpiderRuntimeScope).where(
            SpiderRuntimeScope.strategy_id == scope.strategy_id,
        )).scalar_one()
        plan = session.get(SpiderRuntimePlan, runtime.initial_plan_id)
        assert plan is not None
        candidates = cast(list[dict[str, object]], decision["candidates"])
        candidate = next(
            item for item in candidates
            if item.get("operation") == "PLACE" and item.get("disposition") == "PLANNED"
        )
        trigger_id = delivery.delivery_id
        group_ordinal = cast(int, candidate["candidate_ordinal"]) - cast(
            int, candidate["source_member_index"]
        )
        group_candidates = [
            item for item in candidates
            if item.get("operation") == "PLACE"
            and item.get("disposition") == "PLANNED"
            and cast(int, item["candidate_ordinal"]) - cast(int, item["source_member_index"])
            == group_ordinal
        ]
        action_ids = [cast(str, item["action_id"]) for item in group_candidates]
        actions = session.execute(select(SpiderRuntimeAction).where(
            SpiderRuntimeAction.action_id.in_(action_ids),
        ).order_by(SpiderRuntimeAction.transmission_ordinal)).scalars().all()
        assert len(actions) == len(action_ids) and actions
        actions = [cast(SpiderRuntimeAction, action) for action in actions]
        assert all(action.trigger_delivery_id == trigger_id for action in actions)
        orders = {row.id: row for row in session.execute(select(Order).where(
            Order.id.in_([cast(str, action.local_order_id) for action in actions]),
        )).scalars().all()}
        members: list[dict[str, object]] = []
        for action in actions:
            order = orders[cast(str, action.local_order_id)]
            intent = cast(dict[str, object], order.intent_payload)
            raw = cast(dict[str, object], decode_canonical(bytes(action.original_payload)))
            request_order = dict(raw)
            request_order["clOrdId"] = intent["provider_wire_client_order_id"]
            members.append({
                "action_id": action.action_id,
                "product_id": action.product_id,
                "local_order_id": order.id,
                "binding_sha256": intent["provider_binding_sha256"],
                "wire_clOrdId": intent["provider_wire_client_order_id"],
                "ordId": order.exchange_order_id,
                "request_payload": request_order,
            })
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
            "ordered_action_ids": [member["action_id"] for member in members],
        })).hexdigest()
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
        request = {**body, "payload_sha256": sha256(canonical_bytes(body)).hexdigest()}
        return group_id, canonical_bytes(request), members


@pytest.mark.parametrize(
    ("response_kind", "expected_state"),
    [
        ("accepted", "ACCEPTED"), ("rejected", "REJECTED"),
        ("unknown", "SEND_UNKNOWN"),
        ("reject_before_full", "SEND_UNKNOWN"),
        ("reject_before_partial", "SEND_UNKNOWN"),
    ],
)
def test_normal_place_owner_result_ack_and_cold_proof(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    response_kind: str,
    expected_state: str,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, full_delivery_id = _record_n3_full_decision(
        engine, mock_clock, mock_exchange_adapter
    )
    group_id, request_payload, members = _saved_normal_place_group(
        engine, scope, full_delivery_id
    )
    receipt_id = sha256(canonical_bytes({
        "domain": "SPIDER_TRANSPORT_RESULT_DELIVERY_ID_V1",
        "scope": {"environment": scope.environment,
                  "execution_scope_id": scope.execution_scope_id,
                  "strategy_id": scope.strategy_id},
        "transmission_group_id": group_id,
    })).hexdigest()
    enabled = _transport_enabled_owner(owner, scope)
    with Session(engine) as session:
        runtime = session.execute(select(SpiderRuntimeScope).where(
            SpiderRuntimeScope.strategy_id == scope.strategy_id,
        )).scalar_one()
        plan = session.get(SpiderRuntimePlan, runtime.initial_plan_id)
        assert plan is not None
        original_manifest = (bytes(plan.manifest_payload), plan.manifest_sha256)
        original_place_coverage = tuple(session.execute(select(
            SpiderRuntimeAction.action_id,
            SpiderRuntimeAction.candidate_ordinal,
            SpiderRuntimeAction.local_order_id,
            SpiderRuntimeAction.original_payload,
        ).where(
            SpiderRuntimeAction.plan_id == plan.plan_id,
            SpiderRuntimeAction.operation == "PLACE",
            SpiderRuntimeAction.trigger_delivery_id.is_(None),
        ).order_by(SpiderRuntimeAction.candidate_ordinal)).all())
    if response_kind == "accepted":
        raw_response = _accepted_envelope(members)
    else:
        code = "51008" if response_kind in {"rejected", "reject_before_full", "reject_before_partial"} else "51009"
        raw_response = {
            "code": "0",
            "data": [
                {"clOrdId": member["wire_clOrdId"], "ordId": "", "sCode": code}
                for member in members
            ],
            "msg": "bounded normal PLACE result",
        }
    def financial_snapshot() -> tuple[object, ...]:
        with Session(engine) as session:
            orders = tuple(session.execute(select(
                Order.id, Order.status, Order.exchange_order_id,
                Order.filled_quantity, Order.filled_price,
            ).where(Order.id.in_([cast(str, member["local_order_id"]) for member in members]))
              .order_by(Order.id)).all())
            trades = tuple(session.execute(select(
                Trade.id, Trade.order_id, Trade.quantity, Trade.price,
                Trade.fee, Trade.fee_asset, Trade.exchange_trade_id,
            ).where(Trade.order_id.in_([cast(str, member["local_order_id"]) for member in members]))
              .order_by(Trade.order_id, Trade.id)).all())
            return orders, trades

    class SnapshotPlaceClient(_OfflinePlaceClient):
        financial_at_response: tuple[object, ...] | None = None

        def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            self.calls += 1
            self.financial_at_response = financial_snapshot()
            return self.response

    offline = SnapshotPlaceClient(raw_response)
    if response_kind in {"reject_before_full", "reject_before_partial"}:
        target = enabled._order_manager.repo.get_order(cast(str, members[0]["local_order_id"]))
        assert target is not None
        execution = _execution_for_owner(enabled, engine, mock_clock, mock_exchange_adapter)

        class FinancialBeforeResponse(SnapshotPlaceClient):
            def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
                assert target is not None and target.price is not None
                if response_kind == "reject_before_full":
                    event_value = _filled_event(
                        target, provider_id="normal-provider-before-rejection",
                        trade_id="normal-derived-before-rejection-full",
                    )
                else:
                    quantity = target.quantity / Decimal("2")
                    assert target.price is not None
                    event_value = ExchangeOrderEvent(
                        status="partially_filled", product_id=target.product_id,
                        client_order_id=target.client_order_id,
                        exchange_order_id="normal-provider-before-rejection",
                        cumulative_filled_quantity=quantity,
                        cumulative_average_price=target.price,
                        last_fill_quantity=quantity, last_fill_price=target.price,
                        fee=Decimal("0.01"), fee_asset="USDT",
                        last_fill_exchange_trade_id="normal-derived-before-rejection-partial",
                    )
                assert execution.process_exchange_order_event(event_value)["action"] == "applied"
                self.calls += 1
                self.financial_at_response = financial_snapshot()
                return self.response

        offline = FinancialBeforeResponse(raw_response)
    outcome = enabled._transmit_normal_place_group(
        scope, request_payload, OkxSpiderBatchTransport(cast(Any, offline))
    )
    assert outcome.disposition == "RECORDED"
    assert offline.calls == 1
    assert offline.financial_at_response is not None
    assert financial_snapshot() == offline.financial_at_response
    if response_kind in {"reject_before_full", "reject_before_partial"}:
        target = enabled._order_manager.repo.get_order(cast(str, members[0]["local_order_id"]))
        assert target is not None and target.price is not None
        snapshot = cast(
            tuple[tuple[tuple[object, ...], ...], tuple[tuple[object, ...], ...]],
            offline.financial_at_response,
        )
        target_orders = [row for row in snapshot[0] if row[0] == target.id]
        target_trades = [row for row in snapshot[1] if row[1] == target.id]
        is_full = response_kind == "reject_before_full"
        fill_quantity = target.quantity if is_full else target.quantity / Decimal("2")
        trade_id = (
            "normal-derived-before-rejection-full"
            if is_full else "normal-derived-before-rejection-partial"
        )
        assert target_orders == [(
            target.id, "FILLED" if is_full else "PARTIALLY_FILLED",
            "normal-provider-before-rejection", fill_quantity, target.price,
        )]
        assert len(target_trades) == 1
        assert target_trades[0][2:] == (
            fill_quantity, target.price, Decimal("0.01"), "USDT", trade_id,
        )
    with Session(engine) as session:
        runtime = session.execute(select(SpiderRuntimeScope).where(
            SpiderRuntimeScope.strategy_id == scope.strategy_id,
        )).scalar_one()
        plan = session.get(SpiderRuntimePlan, runtime.initial_plan_id)
        assert plan is not None
        assert (bytes(plan.manifest_payload), plan.manifest_sha256) == original_manifest
        place_coverage = tuple(session.execute(select(
            SpiderRuntimeAction.action_id,
            SpiderRuntimeAction.candidate_ordinal,
            SpiderRuntimeAction.local_order_id,
            SpiderRuntimeAction.original_payload,
        ).where(
            SpiderRuntimeAction.plan_id == plan.plan_id,
            SpiderRuntimeAction.operation == "PLACE",
            SpiderRuntimeAction.trigger_delivery_id.is_(None),
        ).order_by(SpiderRuntimeAction.candidate_ordinal)).all())
        assert place_coverage == original_place_coverage
    readback = enabled.readback(scope, operation_id)
    assert readback is not None
    with Session(engine) as session:
        transmission = session.get(SpiderRuntimeTransmission, group_id)
        assert transmission is not None and transmission.response_payload is not None
        assert transmission.request_payload == request_payload
        for index, member in enumerate(members):
            action = session.get(SpiderRuntimeAction, cast(str, member["action_id"]))
            member_state = (
                "SEND_UNKNOWN"
                if response_kind in {"reject_before_full", "reject_before_partial"} and index == 0
                else "REJECTED" if response_kind in {"reject_before_full", "reject_before_partial"}
                else expected_state
            )
            assert action is not None and action.outcome_state == member_state
        target_order_id = cast(str, members[0]["local_order_id"])
        full_claims = session.execute(select(SpiderRuntimeDelivery.delivery_id).where(
            SpiderRuntimeDelivery.local_order_id == target_order_id,
            SpiderRuntimeDelivery.full_callback_key.is_not(None),
        )).scalars().all()
        assert len(full_claims) == (1 if response_kind == "reject_before_full" else 0)
        receipt = session.execute(select(SpiderRuntimeDelivery).where(
            SpiderRuntimeDelivery.delivery_id == receipt_id,
            SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
        )).scalar_one()
        assert receipt.disposition == "CONSUMED"
        assert receipt.decision_payload is not None
        decision = cast(dict[str, object], decode_canonical(bytes(receipt.decision_payload)))
        source_input = cast(dict[str, object], decision["source_input"])
        assert source_input["type"] == "REST_ACK"
        assert source_input["operation"] == "send"
        entries = cast(list[dict[str, object]], source_input["entries"])
        expected_entries = (
            0 if expected_state == "SEND_UNKNOWN" and response_kind == "unknown"
            else len(members) - 1 if response_kind in {"reject_before_full", "reject_before_partial"}
            else len(members)
        )
        assert len(entries) == expected_entries
        expected_code = "0" if expected_state == "ACCEPTED" else "51008"
        assert all(entry["sCode"] == expected_code for entry in entries)
        assert all(
            entry["ordId"] == entry["clOrdId"]
            and not str(entry["ordId"]).startswith("synthetic-provider-order-")
            for entry in entries
        )
        saved_decision_bytes = bytes(receipt.decision_payload)
    if response_kind in {"accepted", "rejected"}:
        target_member = members[0]
        order = enabled._order_manager.repo.get_order(cast(str, target_member["local_order_id"]))
        assert order is not None
        execution = _execution_for_owner(
            enabled, engine, mock_clock, mock_exchange_adapter,
            observe_order_events=True,
        )
        provider_id = (
            "synthetic-provider-order-0" if response_kind == "accepted"
            else "normal-late-fill-after-rejection"
        )
        assert execution.process_exchange_order_event(_filled_event(
            order,
            provider_id=provider_id,
            trade_id=f"normal-derived-place-late-{response_kind}",
        ))["action"] == "applied"
        with Session(engine) as session:
            saved_result = session.execute(select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.delivery_id == receipt_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )).scalar_one()
            assert saved_result.decision_payload == saved_decision_bytes
            action = session.get(SpiderRuntimeAction, cast(str, target_member["action_id"]))
            assert action is not None
            assert action.outcome_state == (
                "ACCEPTED" if response_kind == "accepted" else "SEND_UNKNOWN"
            )
            trades = session.execute(select(
                Trade.quantity, Trade.price, Trade.fee, Trade.fee_asset,
                Trade.exchange_trade_id,
            ).where(Trade.order_id == order.id)).all()
            assert trades == [(
                order.quantity, order.price, Decimal("0.01"), "USDT",
                f"normal-derived-place-late-{response_kind}",
            )]
            full_claims = session.execute(select(SpiderRuntimeDelivery.delivery_id).where(
                SpiderRuntimeDelivery.local_order_id == order.id,
                SpiderRuntimeDelivery.full_callback_key.is_not(None),
            )).scalars().all()
            assert len(full_claims) == 1
    assert enabled.restore_owned_runtimes() == frozenset({scope.strategy_id})
    cold = enabled.readback(scope, operation_id)
    assert cold is not None
    if response_kind in {"unknown", "reject_before_full", "reject_before_partial"}:
        assert cold == readback
    else:
        assert cold != readback
        assert cast(int, cold["consumed_sequence"]) > cast(
            int, readback["consumed_sequence"]
        )
        assert enabled.restore_owned_runtimes() == frozenset({scope.strategy_id})
        assert enabled.readback(scope, operation_id) == cold
    assert offline.calls == 1


def test_normal_place_existing_marker_readback_under_stop_is_io_free(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, full_delivery_id = _record_n3_full_decision(
        engine, mock_clock, mock_exchange_adapter
    )
    group_id, request_payload, members = _saved_normal_place_group(
        engine, scope, full_delivery_id
    )
    enabled = _transport_enabled_owner(owner, scope)
    offline = _OfflinePlaceClient(_accepted_envelope(members))
    transport = OkxSpiderBatchTransport(cast(Any, offline))
    recorded = enabled._transmit_normal_place_group(scope, request_payload, transport)
    assert recorded.disposition == "RECORDED"
    stopper = SpiderRuntimeOwner(
        enabled._sessions,
        enabled._order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation=enabled._model_sources,
        strategy_state_manager=StrategyStateManager(sessionmaker(engine), _RedisProbe()),
        lifecycle_lock=lambda _strategy_id: RLock(),
        order_event_lock=RLock(),
        transport_response_classifier=classify_okx_batch_response,
    )
    current = stopper.readback(scope, operation_id)
    assert current is not None
    stopped = _dispatch(
        stopper,
        _command(scope, key="normal-place-stop-retry", version=cast(int, current["state_version"])),
    )
    assert stopped.success is True and stopped.completed is True
    duplicate = stopper._transmit_normal_place_group(scope, request_payload, transport)
    assert duplicate.disposition == "RECORDED"
    assert duplicate.transmission_group_id == group_id
    assert offline.calls == 1
    decoded = cast(dict[str, object], decode_canonical(request_payload))
    altered_body = dict(decoded)
    altered_body.pop("payload_sha256")
    altered_members = [dict(member) for member in cast(list[dict[str, object]], altered_body["members"])]
    altered_order = dict(cast(dict[str, object], altered_members[0]["request_payload"]))
    altered_order["px"] = "999999"
    altered_members[0]["request_payload"] = altered_order
    altered_body["members"] = altered_members
    altered = {**altered_body, "payload_sha256": sha256(canonical_bytes(altered_body)).hexdigest()}
    rejected = stopper._transmit_normal_place_group(scope, canonical_bytes(altered), transport)
    assert rejected.disposition == "REJECTED"
    assert offline.calls == 1
    assert stopper.readback(scope, operation_id) is not None


@pytest.mark.parametrize(
    ("failure_point", "expected_disposition", "expected_calls"),
    [
        ("marker_lost_commit", "UNKNOWN", 0),
        ("result_rollback", "SEND_UNKNOWN", 1),
        ("result_lost_commit", "RECORDED", 1),
    ],
)
def test_normal_place_commit_uncertainty_uses_exact_readback_without_resend(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    failure_point: str,
    expected_disposition: str,
    expected_calls: int,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, full_delivery_id = _record_n3_full_decision(
        engine, mock_clock, mock_exchange_adapter
    )
    group_id, request_payload, members = _saved_normal_place_group(
        engine, scope, full_delivery_id
    )
    receipt_id = sha256(canonical_bytes({
        "domain": "SPIDER_TRANSPORT_RESULT_DELIVERY_ID_V1",
        "scope": {"environment": scope.environment,
                  "execution_scope_id": scope.execution_scope_id,
                  "strategy_id": scope.strategy_id},
        "transmission_group_id": group_id,
    })).hexdigest()
    enabled = _transport_enabled_owner(owner, scope)
    offline = _OfflinePlaceClient(_accepted_envelope(members))
    transport = OkxSpiderBatchTransport(cast(Any, offline))
    fired = {"count": 0}

    def mark_marker(session: Session, _context: object, _instances: object) -> None:
        if any(isinstance(row, SpiderRuntimeTransmission) and row.operation == "PLACE" for row in session.new):
            session.info["normal_place_marker_commit"] = True

    def lose_marker(session: Session) -> None:
        if session.info.pop("normal_place_marker_commit", False):
            fired["count"] += 1
            raise RuntimeError("injected normal PLACE marker commit response loss")

    def fail_or_lose_result(
        session: Session, _context: object, _instances: object
    ) -> None:
        if any(
            isinstance(row, SpiderRuntimeTransmission)
            and row.operation == "PLACE"
            and row.response_recorded_at is not None
            for row in session.dirty
        ):
            session.info["normal_place_result_commit"] = True
            if failure_point == "result_rollback":
                fired["count"] += 1
                raise RuntimeError("injected normal PLACE result rollback")

    def lose_result(session: Session) -> None:
        if session.info.pop("normal_place_result_commit", False):
            fired["count"] += 1
            raise RuntimeError("injected normal PLACE result commit response loss")

    if failure_point == "marker_lost_commit":
        event.listen(Session, "before_flush", mark_marker)
        event.listen(Session, "after_commit", lose_marker)
    else:
        event.listen(Session, "before_flush", fail_or_lose_result)
        if failure_point == "result_lost_commit":
            event.listen(Session, "after_commit", lose_result)
    if failure_point == "marker_lost_commit":
        enabled._strategy_state_manager = StrategyStateManager(
            sessionmaker(engine), _RedisProbe()
        )
    try:
        outcome = enabled._transmit_normal_place_group(scope, request_payload, transport)
    finally:
        if failure_point == "marker_lost_commit":
            event.remove(Session, "before_flush", mark_marker)
            event.remove(Session, "after_commit", lose_marker)
        else:
            event.remove(Session, "before_flush", fail_or_lose_result)
            if failure_point == "result_lost_commit":
                event.remove(Session, "after_commit", lose_result)
    assert outcome.disposition == expected_disposition
    assert offline.calls == expected_calls
    assert fired["count"] == 1
    if failure_point == "marker_lost_commit":
        current = enabled.readback(scope, operation_id)
        assert current is not None
        stopped = _dispatch(
            enabled,
            _command(scope, key="normal-place-pending-marker-stop", version=cast(int, current["state_version"])),
        )
        assert stopped.success is True and stopped.completed is True
    retry = enabled._transmit_normal_place_group(scope, request_payload, transport)
    retry_disposition = (
        "SEND_UNKNOWN" if failure_point == "marker_lost_commit" else expected_disposition
    )
    assert retry.disposition == retry_disposition
    assert retry.transmission_group_id == group_id
    assert offline.calls == expected_calls
    with Session(engine) as session:
        transmission = session.get(SpiderRuntimeTransmission, group_id)
        assert transmission is not None
        if failure_point == "marker_lost_commit":
            assert transmission.response_recorded_at is None
        elif failure_point == "result_rollback":
            assert transmission.response_recorded_at is None
            assert session.execute(select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.delivery_id == receipt_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )).scalar_one_or_none() is None
        else:
            assert transmission.response_recorded_at is not None
            receipt = session.execute(select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.delivery_id == receipt_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )).scalar_one()
            assert receipt.disposition == "CONSUMED"
    assert enabled.readback(scope, operation_id) is not None


@pytest.mark.parametrize(
    ("member_code", "expected_state", "scenario"),
    [
        ("0", "ACCEPTED", "normal"),
        ("51400", "REJECTED", "normal"),
        ("51000", "SEND_UNKNOWN", "normal"),
        ("0", "UNSENT", "missing_target"),
        ("0", "UNSENT", "conflicting_target"),
        ("0", "ACCEPTED", "fill_before_result"),
        ("0", "ACCEPTED", "fill_after_result"),
        ("0", "ACCEPTED", "terminal_before_ack"),
        ("0", "ACCEPTED", "terminal_after_ack"),
        ("0", "ACCEPTED", "stop_in_flight"),
        ("0", "UNSENT", "duplicate_target"),
        ("0", "ACCEPTED", "changed_same_group"),
        ("0", "SEND_UNKNOWN", "marker_lost_commit"),
        ("0", "SEND_UNKNOWN", "result_rollback"),
        ("0", "ACCEPTED", "result_lost_commit"),
    ],
)
def test_normal_cancel_owner_result_ack_and_cold_proof(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    member_code: str,
    expected_state: str,
    scenario: str,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, full_delivery_id = _record_n3_full_decision(
        engine, mock_clock, mock_exchange_adapter
    )

    # A persisted PLACE result proves only transport acceptance.  Establish the
    # target provider identity through the ordinary Execution/TXA ACK path.
    with Session(engine) as session:
        full_delivery = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.delivery_id == full_delivery_id,
            )
        ).scalar_one()
        assert full_delivery.decision_payload is not None
        saved_decision = cast(
            dict[str, object], decode_canonical(bytes(full_delivery.decision_payload))
        )
        saved_cancel = next(
            candidate
            for candidate in cast(list[dict[str, object]], saved_decision["candidates"])
            if candidate["operation"] == "CANCEL"
            and candidate["disposition"] == "PLANNED"
        )
        saved_cancel_action = session.get(
            SpiderRuntimeAction, cast(str, saved_cancel["action_id"])
        )
        assert saved_cancel_action is not None
        saved_target_action = session.get(
            SpiderRuntimeAction, cast(str, saved_cancel_action.parent_action_id)
        )
        assert saved_target_action is not None
        saved_target_order = session.get(Order, saved_target_action.local_order_id)
        assert saved_target_order is not None
        saved_transmission = session.get(
            SpiderRuntimeTransmission, saved_target_action.outcome_reference
        )
        assert saved_transmission is not None and saved_transmission.response_payload is not None
        saved_place_outcomes = classify_okx_batch_response(
            bytes(saved_transmission.request_payload), bytes(saved_transmission.response_payload)
        )
        assert saved_place_outcomes is not None
        saved_place_outcome = saved_place_outcomes[saved_target_action.action_id]
        assert saved_place_outcome[0] == "ACCEPTED"
        proved_target_provider_id = saved_place_outcome[2]
        assert proved_target_provider_id is not None
        target_local_order_id = saved_target_order.id

    ack_owner = _transport_enabled_owner(owner, scope)
    ack_order = ack_owner._order_manager.repo.get_order(target_local_order_id)
    assert ack_order is not None
    ack_execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        db_session_factory=sessionmaker(engine),
        repository_transaction_participant=ack_owner.participate_in_order_transaction,
        owned_order_event_observer=ack_owner.observe_order_event,
        is_backtest=True,
    )
    if scenario != "missing_target":
        assert proved_target_provider_id is not None
        assert ack_execution.process_exchange_order_event(ExchangeOrderEvent(
            status="open",
            product_id=ack_order.product_id,
            client_order_id=ack_order.client_order_id,
            exchange_order_id=proved_target_provider_id,
            cumulative_filled_quantity=Decimal("0"),
        ))["action"] == "applied"

    def financial_snapshot(
        target_order_id: str,
    ) -> tuple[tuple[object, ...], tuple[object, ...]]:
        with Session(engine) as session:
            order = session.get(Order, target_order_id)
            assert order is not None
            return (
                (order.id, order.status, order.exchange_order_id,
                 order.filled_quantity, order.filled_price),
                tuple(session.execute(
                    select(Trade.id, Trade.quantity, Trade.price, Trade.fee).where(
                        Trade.order_id == target_order_id
                    ).order_by(Trade.id)
                ).all()),
            )

    with Session(engine) as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        plan = session.get(SpiderRuntimePlan, runtime.initial_plan_id)
        assert plan is not None
        initial_manifest_snapshot = (
            bytes(plan.manifest_payload), plan.manifest_sha256, plan.plan_status,
            bytes(plan.missing_members), plan.tape_cursor,
        )
        initial_place_snapshot = tuple(session.execute(
            select(
                SpiderRuntimeAction.action_id,
                SpiderRuntimeAction.candidate_ordinal,
                SpiderRuntimeAction.local_order_id,
                SpiderRuntimeAction.original_payload,
                SpiderRuntimeAction.payload_sha256,
            ).where(
                SpiderRuntimeAction.plan_id == plan.plan_id,
                SpiderRuntimeAction.operation == "PLACE",
                SpiderRuntimeAction.trigger_delivery_id.is_(None),
            ).order_by(SpiderRuntimeAction.candidate_ordinal)
        ).all())
        initial_plan_id = plan.plan_id

        full_delivery = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.delivery_id == full_delivery_id,
            )
        ).scalar_one()
        assert full_delivery.decision_payload is not None
        decision = cast(
            dict[str, object], decode_canonical(bytes(full_delivery.decision_payload))
        )
        cancel_candidate = next(
            candidate
            for candidate in cast(list[dict[str, object]], decision["candidates"])
            if candidate["operation"] == "CANCEL"
            and candidate["disposition"] == "PLANNED"
        )
        cancel_action = session.get(
            SpiderRuntimeAction, cast(str, cancel_candidate["action_id"])
        )
        assert cancel_action is not None and cancel_action.operation == "CANCEL"
        assert cancel_candidate["target_place_action_id"] == cancel_action.parent_action_id
        target_action = session.get(
            SpiderRuntimeAction, cast(str, cancel_action.parent_action_id)
        )
        assert target_action is not None and target_action.operation == "PLACE"
        assert target_action.trigger_delivery_id is None
        binding = session.get(SpiderRuntimeBinding, target_action.action_id)
        target_order = session.get(Order, target_action.local_order_id)
        assert binding is not None and target_order is not None
        if scenario != "missing_target":
            assert target_order.exchange_order_id is not None
            assert target_action.outcome_state == "ACCEPTED"
            assert target_action.outcome_reference is not None
            original_transmission = session.get(
                SpiderRuntimeTransmission, target_action.outcome_reference
            )
            assert original_transmission is not None
            assert original_transmission.response_payload is not None
            accepted_target = classify_okx_batch_response(
                bytes(original_transmission.request_payload),
                bytes(original_transmission.response_payload),
            )
            assert accepted_target is not None
            assert accepted_target[target_action.action_id] == (
                "ACCEPTED", None, target_order.exchange_order_id
            )
        target_intent = cast(dict[str, object], target_order.intent_payload)
        binding_bytes = bytes.fromhex(cast(str, target_intent["provider_binding_bytes_hex"]))
        source_target = cast(dict[str, object], decode_canonical(target_action.original_payload))
        assert cast(dict[str, object], cancel_candidate["original_payload"])["ordId"] == (
            binding.source_client_order_id
        )
        assert cancel_action.outcome_state == "UNSENT"
        request_payload = {"instId": source_target["instId"], "ordId": target_order.exchange_order_id}
        executing_account_ref = scope.execution_scope_id
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
        cancel_member = {
            "cancel_action_id": cancel_action.action_id,
            "product_id": target_action.product_id,
            "target_place_action_id": target_action.action_id,
            "target_local_order_id": target_order.id,
            "target_binding_sha256": sha256(binding_bytes).hexdigest(),
            "target_wire_clOrdId": target_intent["provider_wire_client_order_id"],
            "target_ordId": target_order.exchange_order_id,
            "request_payload": request_payload,
        }
        if scenario == "missing_target":
            cancel_member["target_ordId"] = None
        elif scenario == "conflicting_target":
            cancel_member["target_ordId"] = "foreign-provider-order"
        cancel_request: dict[str, object] = {
            "schema_version": "SPIDER_TRANSPORT_BATCH_V1",
            "scope": {
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "strategy_id": scope.strategy_id,
            },
            "executing_account_ref": executing_account_ref,
            "transmission_group_id": cancel_group_id,
            "operation": "CANCEL",
            "members": [cancel_member],
        }
        cancel_request["payload_sha256"] = sha256(canonical_bytes(cancel_request)).hexdigest()
        if scenario == "duplicate_target":
            cancel_request["members"] = [cancel_member, dict(cancel_member)]
            cancel_group_id = sha256(canonical_bytes({
                "domain": "SPIDER_TRANSMISSION_GROUP_V1",
                "scope": cancel_request["scope"],
                "operation_id": runtime.current_operation_id,
                "plan_id": plan.plan_id,
                "generation": runtime.generation,
                "configuration_sha256": runtime.configuration_sha256,
                "operation": "CANCEL",
                "ordered_action_ids": [cancel_action.action_id, cancel_action.action_id],
            })).hexdigest()
            cancel_request["transmission_group_id"] = cancel_group_id
            cancel_request["payload_sha256"] = sha256(canonical_bytes({
                key: value for key, value in cancel_request.items() if key != "payload_sha256"
            })).hexdigest()
        if scenario == "conflicting_target":
            cancel_member["request_payload"] = {"instId": "FOREIGN-SWAP", "ordId": "foreign-provider-order"}
            cancel_request["payload_sha256"] = sha256(canonical_bytes({
                key: value for key, value in cancel_request.items() if key != "payload_sha256"
            })).hexdigest()
        cancel_request_bytes = canonical_bytes(cancel_request)
        target_order_id = target_order.id
        cancel_action_id = cancel_action.action_id
        target_provider_id = target_order.exchange_order_id
        source_alias_id = binding.source_client_order_id
        checkpoint_before_cancel_result = bytes(runtime.source_checkpoint)

    def assert_initial_manifest_unchanged() -> None:
        with Session(engine) as session:
            current_plan = session.get(SpiderRuntimePlan, initial_plan_id)
            assert current_plan is not None
            assert (
                bytes(current_plan.manifest_payload), current_plan.manifest_sha256,
                current_plan.plan_status, bytes(current_plan.missing_members), current_plan.tape_cursor,
            ) == initial_manifest_snapshot
            current_initial_places = tuple(session.execute(
                select(
                    SpiderRuntimeAction.action_id,
                    SpiderRuntimeAction.candidate_ordinal,
                    SpiderRuntimeAction.local_order_id,
                    SpiderRuntimeAction.original_payload,
                    SpiderRuntimeAction.payload_sha256,
                ).where(
                    SpiderRuntimeAction.plan_id == initial_plan_id,
                    SpiderRuntimeAction.operation == "PLACE",
                    SpiderRuntimeAction.trigger_delivery_id.is_(None),
                ).order_by(SpiderRuntimeAction.candidate_ordinal)
            ).all())
            assert current_initial_places == initial_place_snapshot
    before = financial_snapshot(target_order_id)
    cancel_result_delivery_id = sha256(canonical_bytes({
        "domain": "SPIDER_TRANSPORT_RESULT_DELIVERY_ID_V1",
        "scope": {
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "strategy_id": scope.strategy_id,
        },
        "transmission_group_id": cancel_group_id,
    })).hexdigest()

    class OfflineCancelClient:
        calls = 0
        submitted: list[dict[str, object]] = []

        def privatePostTradeCancelBatchOrders(
            self, orders: list[dict[str, object]]
        ) -> object:
            self.calls += 1
            self.submitted.extend(orders)
            return {
                "code": "0",
                "data": [{
                    "ordId": target_provider_id,
                    "clOrdId": "",
                    "sCode": member_code,
                }],
                "msg": "offline cancel result",
            }

        def privatePostTradeBatchOrders(
            self, _orders: list[dict[str, object]]
        ) -> object:
            raise AssertionError("CANCEL classification must use the cancel endpoint")

    offline = OfflineCancelClient()
    concrete_transport = OkxSpiderBatchTransport(cast(Any, offline))
    checkpoint_at_cancel_response: list[bytes] = []
    financial_at_cancel_response: list[tuple[tuple[object, ...], tuple[object, ...]]] = []
    fault_fired = {"count": 0}
    def mark_cancel_marker_commit(session: Session, _context: object, _instances: object) -> None:
        if any(
            isinstance(row, SpiderRuntimeTransmission) and row.operation == "CANCEL"
            for row in session.new
        ):
            session.info["spider_cancel_marker_commit"] = True

    def lose_cancel_marker_commit(session: Session) -> None:
        if session.info.pop("spider_cancel_marker_commit", False):
            fault_fired["count"] += 1
            raise RuntimeError("injected lost CANCEL marker commit response")

    if scenario == "marker_lost_commit":
        event.listen(Session, "before_flush", mark_cancel_marker_commit)
        event.listen(Session, "after_commit", lose_cancel_marker_commit)
    def fail_cancel_result_before_flush(
        session: Session, _context: object, _instances: object
    ) -> None:
        if any(
            isinstance(row, SpiderRuntimeTransmission)
            and row.operation == "CANCEL"
            and row.response_recorded_at is not None
            for row in session.dirty
        ):
            fault_fired["count"] += 1
            session.info["spider_cancel_result_commit"] = True
            if scenario == "result_rollback":
                raise RuntimeError("injected cancel result rollback")

    def lose_cancel_result_commit(session: Session) -> None:
        if session.info.pop("spider_cancel_result_commit", False) and scenario == "result_lost_commit":
            fault_fired["count"] += 1
            raise RuntimeError("injected lost CANCEL result commit response")

    if scenario in {"result_rollback", "result_lost_commit"}:
        event.listen(Session, "before_flush", fail_cancel_result_before_flush)
        event.listen(Session, "after_commit", lose_cancel_result_commit)
    if scenario == "stop_in_flight":
        class RedisProbe:
            def publish(self, _channel: str, _message: str) -> int:
                return 1

        ack_owner._strategy_state_manager = StrategyStateManager(
            sessionmaker(engine), RedisProbe()
        )
    original_submit = offline.privatePostTradeCancelBatchOrders

    def submit_with_causal_boundary(orders: list[dict[str, object]]) -> object:
        if scenario == "fill_before_result":
            assert proved_target_provider_id is not None
            assert ack_execution.process_exchange_order_event(_filled_event(
                ack_order, provider_id=proved_target_provider_id,
                trade_id="normal-cancel-fill-before-result",
            ))["action"] == "applied"
            with Session(engine) as session:
                checkpoint_at_cancel_response.append(bytes(session.execute(
                    select(SpiderRuntimeScope.source_checkpoint).where(
                        SpiderRuntimeScope.strategy_id == scope.strategy_id
                    )
                ).scalar_one()))
            financial_at_cancel_response.append(financial_snapshot(target_order_id))
        elif scenario == "terminal_before_ack":
            assert proved_target_provider_id is not None
            assert ack_execution.process_exchange_order_event(ExchangeOrderEvent(
                status="canceled",
                product_id=ack_order.product_id,
                client_order_id=ack_order.client_order_id,
                exchange_order_id=proved_target_provider_id,
                cumulative_filled_quantity=Decimal("0"),
            ))["action"] == "applied"
            with Session(engine) as session:
                checkpoint_at_cancel_response.append(bytes(session.execute(
                    select(SpiderRuntimeScope.source_checkpoint).where(
                        SpiderRuntimeScope.strategy_id == scope.strategy_id
                    )
                ).scalar_one()))
            financial_at_cancel_response.append(financial_snapshot(target_order_id))
        elif scenario == "stop_in_flight":
            current = ack_owner.readback(scope, operation_id)
            assert current is not None
            stopped = _dispatch(
                ack_owner,
                _command(
                    scope,
                    key="normal-cancel-stop-in-flight",
                    version=cast(int, current["state_version"]),
                ),
            )
            assert stopped.success is True and stopped.completed is True
        return original_submit(orders)

    offline.privatePostTradeCancelBatchOrders = submit_with_causal_boundary
    try:
        result = ack_owner._transmit_normal_cancel_group(scope, cancel_request_bytes, concrete_transport)
    finally:
        if scenario in {"result_rollback", "result_lost_commit"}:
            event.remove(Session, "before_flush", fail_cancel_result_before_flush)
            event.remove(Session, "after_commit", lose_cancel_result_commit)
        if scenario == "marker_lost_commit":
            event.remove(Session, "before_flush", mark_cancel_marker_commit)
            event.remove(Session, "after_commit", lose_cancel_marker_commit)
    if scenario in {"missing_target", "conflicting_target", "duplicate_target"}:
        assert offline.calls == 0
        assert result.disposition in {"UNAVAILABLE", "REJECTED"}
        with Session(engine) as session:
            saved_cancel = session.get(SpiderRuntimeAction, cancel_action_id)
            assert saved_cancel is not None and saved_cancel.outcome_state == "UNSENT"
            assert session.get(SpiderRuntimeTransmission, cancel_group_id) is None
        assert ack_owner.readback(scope, operation_id) is not None
        assert ack_owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
        assert_initial_manifest_unchanged()
        return
    if scenario == "marker_lost_commit":
        assert result.disposition == "UNKNOWN"
        assert fault_fired["count"] == 1
        assert offline.calls == 0
        with Session(engine) as session:
            saved_marker = session.get(SpiderRuntimeTransmission, cancel_group_id)
            saved_cancel = session.get(SpiderRuntimeAction, cancel_action_id)
            runtime_after_marker = session.execute(
                select(SpiderRuntimeScope).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                )
            ).scalar_one()
            assert saved_marker is not None and saved_marker.operation == "CANCEL"
            assert saved_marker.response_payload is None
            assert saved_cancel is not None
            assert saved_cancel.outcome_state == "SEND_UNKNOWN"
            assert bytes(runtime_after_marker.source_checkpoint) == checkpoint_before_cancel_result
        assert ack_owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
        assert ack_owner.readback(scope, operation_id) is not None
        assert ack_owner._transmit_normal_cancel_group(
            scope, cancel_request_bytes, concrete_transport
        ).disposition == "SEND_UNKNOWN"
        assert offline.calls == 0
        with Session(engine) as session:
            assert session.execute(select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.delivery_id == cancel_result_delivery_id
            )).scalar_one_or_none() is None
        assert_initial_manifest_unchanged()
        return
    if scenario == "result_rollback":
        assert result.disposition == "SEND_UNKNOWN"
    else:
        assert result.disposition == "RECORDED"
    assert offline.calls == 1
    assert offline.submitted == [request_payload]
    if scenario in {"fill_before_result", "terminal_before_ack"}:
        assert financial_at_cancel_response
        assert financial_snapshot(target_order_id) == financial_at_cancel_response[0]
        if scenario == "terminal_before_ack":
            assert financial_at_cancel_response[0][1] == before[1]
    else:
        assert financial_snapshot(target_order_id) == before
    if scenario == "terminal_after_ack":
        assert proved_target_provider_id is not None
        assert ack_execution.process_exchange_order_event(ExchangeOrderEvent(
            status="canceled",
            product_id=ack_order.product_id,
            client_order_id=ack_order.client_order_id,
            exchange_order_id=proved_target_provider_id,
            cumulative_filled_quantity=Decimal("0"),
        ))["action"] == "applied"
        after_terminal = financial_snapshot(target_order_id)
        assert after_terminal[0][2:] == before[0][2:]
        assert after_terminal[1] == before[1]
        assert after_terminal[0][1] != before[0][1]
    if scenario == "fill_after_result":
        assert proved_target_provider_id is not None
        assert ack_execution.process_exchange_order_event(_filled_event(
            ack_order, provider_id=proved_target_provider_id,
            trade_id="normal-cancel-fill-after-result",
        ))["action"] == "applied"
    if scenario in {"result_rollback", "result_lost_commit"}:
        assert fault_fired["count"] >= 1
        with Session(engine) as session:
            saved_transmission = session.get(SpiderRuntimeTransmission, cancel_group_id)
            assert saved_transmission is not None
            if scenario == "result_rollback":
                assert saved_transmission.response_recorded_at is None
                assert saved_transmission.response_payload is None
                assert session.execute(select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.delivery_id == cancel_result_delivery_id
                )).scalar_one_or_none() is None
            else:
                assert saved_transmission.response_recorded_at is not None
                assert saved_transmission.response_payload is not None
        if scenario == "result_rollback":
            assert ack_owner._transmit_normal_cancel_group(
                scope, cancel_request_bytes, concrete_transport
            ).disposition == "SEND_UNKNOWN"
            assert offline.calls == 1
            assert ack_owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
            assert ack_owner.readback(scope, operation_id) is not None
            with Session(engine) as session:
                assert session.execute(select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.delivery_id == cancel_result_delivery_id
                )).scalar_one_or_none() is None
                runtime_after_rollback = session.execute(select(SpiderRuntimeScope).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                )).scalar_one()
                assert bytes(runtime_after_rollback.source_checkpoint) == checkpoint_before_cancel_result
            assert_initial_manifest_unchanged()
            return
        elif scenario == "result_lost_commit":
            assert ack_owner._transmit_normal_cancel_group(
                scope, cancel_request_bytes, concrete_transport
            ).disposition == "RECORDED"
            assert offline.calls == 1
    with Session(engine) as session:
        saved_cancel = session.get(SpiderRuntimeAction, cancel_action_id)
        result_receipt = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.delivery_id == cancel_result_delivery_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.kind == "TRANSPORT_RESULT",
            )
        ).scalar_one_or_none()
        assert saved_cancel is not None and saved_cancel.outcome_state == expected_state
        assert result_receipt is not None
        if scenario == "stop_in_flight":
            assert result_receipt.decision_payload is None
            entries: list[dict[str, object]] = []
        else:
            assert result_receipt.decision_payload is not None
            saved_decision = cast(dict[str, object], decode_canonical(bytes(result_receipt.decision_payload)))
            source_input = cast(dict[str, object], saved_decision["source_input"])
            assert source_input["type"] == "REST_ACK"
            assert source_input["operation"] == "cancel"
            entries = cast(list[dict[str, object]], source_input["entries"])
        runtime_after = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        if scenario == "stop_in_flight":
            assert result_receipt.disposition == "SUPPRESSED"
            assert result_receipt.suppression_reason == "STOP_CONTROL"
            assert result_receipt.decision_payload is None
            assert bytes(runtime_after.source_checkpoint) == checkpoint_before_cancel_result
        elif expected_state == "ACCEPTED" and scenario not in {"fill_before_result", "terminal_before_ack"}:
            assert len(entries) == 1 and entries[0]["ordId"] == source_alias_id
            assert entries[0]["sCode"] == "0"
            if scenario != "fill_after_result":
                assert bytes(runtime_after.source_checkpoint) != checkpoint_before_cancel_result
        elif expected_state == "ACCEPTED" and scenario in {"fill_before_result", "terminal_before_ack"}:
            assert entries == []
            assert checkpoint_at_cancel_response
            assert bytes(runtime_after.source_checkpoint) == checkpoint_at_cancel_response[0]
        elif expected_state == "REJECTED":
            assert len(entries) == 1 and entries[0]["sCode"] == "51400"
            assert bytes(runtime_after.source_checkpoint) != checkpoint_before_cancel_result
        else:
            assert entries == []
            assert bytes(runtime_after.source_checkpoint) == checkpoint_before_cancel_result
        if expected_state in {"ACCEPTED", "REJECTED"} and scenario not in {
            "fill_before_result", "fill_after_result", "terminal_before_ack",
            "terminal_after_ack", "stop_in_flight"
        }:
            checkpoint_value = cast(dict[str, object], decode_canonical(bytes(runtime_after.source_checkpoint)))
            replies = cast(list[dict[str, object]], checkpoint_value["replies"])
            reply = next(row for row in replies if row["alias"] == source_alias_id)
            reply_fields = cast(list[dict[str, object]], reply["value"])
            reply_values = {row["key"]: row["value"] for row in reply_fields}
            expected_source_state = "canceling" if expected_state == "ACCEPTED" else "failed"
            assert reply_values["state"] == {"tag": "STRING", "value": expected_source_state}
        saved_transmission = session.get(SpiderRuntimeTransmission, cancel_group_id)
        assert saved_transmission is not None
        assert saved_transmission.response_payload is not None
        assert saved_transmission.response_sha256 == sha256(saved_transmission.response_payload).hexdigest()
    if scenario not in {"stop_in_flight"}:
        assert ack_owner._transmit_normal_cancel_group(
            scope, cancel_request_bytes, concrete_transport
        ).disposition == "RECORDED"
        assert offline.calls == 1
    if scenario == "changed_same_group":
        changed_request = cast(dict[str, object], decode_canonical(cancel_request_bytes))
        changed_members = cast(list[dict[str, object]], changed_request["members"])
        changed_members[0]["request_payload"] = {
            "instId": "FOREIGN-SWAP", "ordId": target_provider_id
        }
        changed_request["payload_sha256"] = sha256(canonical_bytes({
            key: value for key, value in changed_request.items() if key != "payload_sha256"
        })).hexdigest()
        changed_result = ack_owner._transmit_normal_cancel_group(
            scope, canonical_bytes(changed_request), concrete_transport
        )
        assert changed_result.disposition == "REJECTED"
        assert offline.calls == 1
    assert ack_owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert ack_owner.readback(scope, operation_id) is not None
    assert_initial_manifest_unchanged()


@pytest.mark.parametrize("operation", ["PLACE", "CANCEL"])
def test_cold_proof_recomputes_filtered_and_unresolved_candidate_disposition(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
    operation: str,
) -> None:
    owner, scope, operation_id, delivery_id = _record_n3_full_decision(
        operation_pg, mock_clock, mock_exchange_adapter
    )
    with Session(operation_pg) as session:
        delivery = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.delivery_id == delivery_id,
            )
        ).scalar_one()
        assert delivery.decision_payload is not None
        decision = cast(dict[str, object], decode_canonical(bytes(delivery.decision_payload)))
        candidates = cast(list[dict[str, object]], decision["candidates"])
        candidate = next(value for value in candidates if value["operation"] == operation)
        persisted_action_id = cast(str, candidate["action_id"])
        if operation == "PLACE":
            local_order_id = cast(str, candidate["local_order_id"])
            binding = session.get(SpiderRuntimeBinding, persisted_action_id)
            action = session.get(SpiderRuntimeAction, persisted_action_id)
            local_order = session.get(Order, local_order_id)
            assert binding is not None and action is not None and local_order is not None
            session.delete(binding)
            session.flush()
            session.delete(action)
            session.flush()
            session.delete(local_order)
            candidate.update(
                disposition="FILTERED_SOURCE",
                reason_code="SOURCE_PRODUCT_UNAVAILABLE",
                action_id=None,
                local_order_id=None,
                target_place_action_id=None,
            )
        else:
            assert candidate["target_place_action_id"] is not None
            cancel_action = session.get(SpiderRuntimeAction, persisted_action_id)
            assert cancel_action is not None and cancel_action.operation == "CANCEL"
            session.delete(cancel_action)
            candidate.update(
                disposition="UNRESOLVED_TARGET",
                reason_code="SOURCE_CANCEL_TARGET_UNRESOLVED",
                action_id=None,
                local_order_id=None,
                target_place_action_id=None,
            )
        corrupted = canonical_bytes(decision)
        delivery.decision_payload = corrupted
        delivery.decision_sha256 = sha256(corrupted).hexdigest()
        session.commit()
    assert _transport_enabled_owner(owner, scope).readback(scope, operation_id) is None


def test_stop_drains_actual_execution_receipts_without_normal_grid_decisions(
    operation_pg: Engine,
    mock_clock,
    mock_exchange_adapter,
) -> None:
    engine = operation_pg
    owner, scope, operation_id, _factory, initial = _owner_with_lifecycle(engine)
    order_manager = owner._order_manager
    source_group, request_payload, members = _saved_initial_place_request(engine, scope)
    owner._transport_response_classifier = classify_okx_batch_response
    enabled = owner
    offline = _OfflinePlaceClient({
        "code": "0",
        "data": [
            {"clOrdId": member["wire_clOrdId"], "ordId": f"stop-provider-{i}", "sCode": "0"}
            for i, member in enumerate(members)
        ],
        "msg": "synthetic acknowledged initial source orders",
    })
    assert enabled.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, offline)),
    ).disposition == "RECORDED"
    with Session(engine) as session:
        before_barrier = session.scalar(
            select(SpiderRuntimeScope.last_tape_sequence).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        )
        before_checkpoint = bytes(session.execute(
            select(SpiderRuntimeScope.source_checkpoint).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one())
        before_receipts = session.execute(
            select(
                SpiderRuntimeTapeIndex.ordinal,
                SpiderRuntimeDelivery.sequence,
                SpiderRuntimeDelivery.delivery_id,
                SpiderRuntimeDelivery.kind,
                SpiderRuntimeDelivery.disposition,
                SpiderRuntimeDelivery.suppression_reason,
                SpiderRuntimeDelivery.content_sha256,
                SpiderRuntimeDelivery.source_payload,
                SpiderRuntimeDelivery.decision_payload,
                SpiderRuntimeDelivery.decision_sha256,
            )
            .join(
                SpiderRuntimeTapeIndex,
                SpiderRuntimeTapeIndex.delivery_sequence == SpiderRuntimeDelivery.sequence,
            ).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.kind.in_(("FILL", "ORDER_STATE")),
                SpiderRuntimeTapeIndex.kind == "DELIVERY",
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).all()
    stopped = _dispatch(owner, _command(
        scope, key="normal-receipt-stop", version=cast(int, initial["state_version"])
    ))
    assert stopped.success and stopped.completed

    order = order_manager.repo.get_order(cast(str, members[0]["local_order_id"]))
    assert order is not None and order.price is not None
    execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        db_session_factory=sessionmaker(engine),
        repository_transaction_participant=owner.participate_in_order_transaction,
        owned_order_event_observer=owner.observe_order_event,
        is_backtest=True,
    )
    assert execution.process_exchange_order_event(_filled_event(
        order, provider_id="stop-preserved-provider-order", trade_id="stop-preserved-trade"
    ))["action"] == "applied"
    stopped_readback = enabled.readback(scope, operation_id)
    assert stopped_readback is not None
    assert stopped_readback["runtime_control"] == "STOP"
    with Session(engine) as session:
        trade = session.execute(
            select(Trade).where(Trade.exchange_trade_id == "stop-preserved-trade")
        ).scalar_one()
        assert (trade.order_id, trade.quantity, trade.price) == (
            order.id, order.quantity, order.price
        )
        barrier = cast(int, stopped_readback["barrier_sequence"])
        assert barrier == before_barrier
        after_receipts = session.execute(
            select(
                SpiderRuntimeTapeIndex.ordinal,
                SpiderRuntimeDelivery.sequence,
                SpiderRuntimeDelivery.delivery_id,
                SpiderRuntimeDelivery.kind,
                SpiderRuntimeDelivery.disposition,
                SpiderRuntimeDelivery.suppression_reason,
                SpiderRuntimeDelivery.content_sha256,
                SpiderRuntimeDelivery.source_payload,
                SpiderRuntimeDelivery.decision_payload,
                SpiderRuntimeDelivery.decision_sha256,
            )
            .join(
                SpiderRuntimeTapeIndex,
                SpiderRuntimeTapeIndex.delivery_sequence == SpiderRuntimeDelivery.sequence,
            ).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.kind.in_(("FILL", "ORDER_STATE")),
                SpiderRuntimeTapeIndex.kind == "DELIVERY",
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).all()
        prior_receipts = [row for row in after_receipts if row.ordinal <= barrier]
        assert prior_receipts == before_receipts
        post_barrier = [row for row in after_receipts if row.ordinal > barrier]
        assert post_barrier and all(
            row.disposition == "SUPPRESSED"
            and row.suppression_reason == "STOP_CONTROL"
            and row.decision_payload is None
            and row.decision_sha256 is None
            for row in post_barrier
        )
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeAction).where(
                SpiderRuntimeAction.strategy_id == scope.strategy_id,
                SpiderRuntimeAction.trigger_delivery_id.is_not(None),
            )
        ) == 0
        after_checkpoint = bytes(session.execute(
            select(SpiderRuntimeScope.source_checkpoint).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one())
        assert after_checkpoint == before_checkpoint


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
