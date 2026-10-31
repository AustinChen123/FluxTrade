"""Actual-owner cutoff projection proof for non-null MODEL order snapshots."""

from __future__ import annotations

from decimal import Decimal
import os
from typing import Any, cast

import pytest
from sqlalchemy import select
from sqlalchemy.engine import Engine

from src.core.backtest.spider_run_artifacts import decode_canonical
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.orm_models import Order
from src.core.spider_management.member_resubmission import MemberResubmissionRequest
from src.core.spider_management.orm import SpiderRuntimeAction, SpiderRuntimeBinding
from src.core.adapters.okx_spider_transport import OkxSpiderBatchTransport
from test_spider_financial_delivery_pg import _operation_pg
from test_spider_financial_delivery_pg import _saved_initial_place_request, _transport_enabled_owner
from test_spider_source_time_pg import (
    _UTC, _model_time, _observation_id, _owner, _runtime, _time_rows,
)
from test_spider_control_command_pg import _command, _dispatch

pytest_plugins = ["test_migrations"]
operation_pg = _operation_pg
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]


def _time_snapshot(
    prepared: Any, observed: int, open_orders: list[dict[str, object]]
) -> dict[str, object]:
    checkpoint = cast(dict[str, Any], decode_canonical(prepared.build_checkpoint_bytes))
    products = [cast(dict[str, Any], item)["product_id"] for item in checkpoint["markets"]]
    return {
        "earn": "0",
        "trading": {"eq": "100", "availEq": "90"},
        "positions": {cast(str, product): "0" for product in products},
        "open_orders": open_orders,
        "as_of_ms": observed,
    }


def _partial_fixture(engine: Engine, mock_clock: Any, mock_exchange_adapter: Any):
    owner, scope, operation_id, factory, prepared, _initial, order_manager, _confirmation, _source = _owner(engine)
    with factory() as session:
        found = session.execute(
            select(SpiderRuntimeAction, SpiderRuntimeBinding, Order)
            .join(SpiderRuntimeBinding, SpiderRuntimeBinding.action_id == SpiderRuntimeAction.action_id)
            .join(Order, Order.id == SpiderRuntimeAction.local_order_id)
            .where(
                SpiderRuntimeAction.plan_id == _runtime(factory, scope).initial_plan_id,
                SpiderRuntimeAction.operation == "PLACE",
                SpiderRuntimeAction.parent_action_id.is_(None),
            ).order_by(SpiderRuntimeAction.candidate_ordinal)
        ).first()
        assert found is not None
        action, binding, order = found
        saved_action_id, saved_order_id = action.action_id, order.id
        price, quantity, timestamp = order.price, order.quantity, order.timestamp
        product_id, client_order_id = order.product_id, order.client_order_id
        source_price = cast(str, cast(dict[str, object], decode_canonical(bytes(action.original_payload)))["px"])
    assert price is not None and quantity > 0 and type(timestamp) is int
    execution = ExecutionEngine(
        db_session=None, clock=mock_clock, adapter=mock_exchange_adapter,
        db_session_factory=factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    partial = quantity / Decimal("2")
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="partially_filled", product_id=product_id,
        client_order_id=client_order_id,
        cumulative_filled_quantity=partial, cumulative_average_price=price,
        last_fill_quantity=partial, last_fill_price=price, fee=Decimal("0"),
        fee_asset="USDT", last_fill_exchange_trade_id="time-cutoff-partial",
    ))["action"] == "applied"
    owner.observe_order_event(saved_order_id)
    with factory() as session:
        runtime = _runtime(factory, scope)
        action, binding, order = session.execute(
            select(SpiderRuntimeAction, SpiderRuntimeBinding, Order)
            .join(SpiderRuntimeBinding, SpiderRuntimeBinding.action_id == SpiderRuntimeAction.action_id)
            .join(Order, Order.id == SpiderRuntimeAction.local_order_id)
            .where(SpiderRuntimeAction.action_id == saved_action_id)
        ).one()
        assert binding.action_id == saved_action_id
        anchor = _model_time(
            scope, prepared, runtime, observation_id=_observation_id("orders-anchor"),
            event="ANCHOR", epoch_id="orders-epoch", observed_utc_ms=_UTC,
            elapsed_ms=1_000,
        )
        owner.receive_source_time(anchor)
        runtime = _runtime(factory, scope)
        snapshot_row: dict[str, object] = {
            "local_order_id": saved_order_id,
            "state": "partially_filled",
            "price": source_price,
            "quantity_contracts": format(quantity.normalize(), "f"),
            "cumulative_contracts": format(partial.normalize(), "f"),
            "created_at_ms": timestamp,
        }
        # Use the canonical source decimal text already persisted by the order.
        snapshot_row["price"] = source_price
    return owner, scope, factory, prepared, runtime, snapshot_row, execution, product_id, client_order_id, price, quantity


def test_complete_partial_working_set_is_proved_at_cutoff_and_not_rewritten_by_later_fill(
    operation_pg: Engine, mock_clock: Any, mock_exchange_adapter: Any,
) -> None:
    owner, scope, factory, prepared, runtime, row, execution, product_id, client_id, price, quantity = _partial_fixture(
        operation_pg, mock_clock, mock_exchange_adapter
    )
    timer = _model_time(
        scope, prepared, runtime, observation_id=_observation_id("orders-time"),
        event="TIMER", epoch_id="orders-epoch", observed_utc_ms=_UTC + 1_000,
        elapsed_ms=1_000, predecessor_id=_observation_id("orders-anchor"),
        snapshots=_time_snapshot(prepared, _UTC + 1_000, [row]),
    )
    invalid_snapshots: tuple[tuple[str, list[dict[str, object]]], ...] = (
        ("missing", []),
        ("extra", [{**row, "local_order_id": "unproved-order"}]),
        ("duplicate", [row, dict(row)]),
        ("wrong-terms", [{**row, "price": "999"}]),
    )
    for label, bad_orders in invalid_snapshots:
        rejected = owner.receive_source_time(_model_time(
            scope, prepared, runtime, observation_id=_observation_id(f"orders-{label}"),
            event="TIMER", epoch_id="orders-epoch", observed_utc_ms=_UTC + 1_000,
            elapsed_ms=1_000, predecessor_id=_observation_id("orders-anchor"),
            snapshots=_time_snapshot(prepared, _UTC + 1_000, bad_orders),
        ))
        assert not rejected.success and rejected.data is not None
        assert rejected.data["disposition"] == "REJECTED"
        assert len(_time_rows(factory, scope)) == 1
    accepted = owner.receive_source_time(timer)
    assert accepted.success and accepted.data is not None
    assert accepted.data["delivery_disposition"] == "CONSUMED"
    assert owner.readback(scope, cast(str, accepted.data["operation_id"])) is not None
    first_time = _time_rows(factory, scope)[-1]
    assert first_time.disposition == "CONSUMED"

    # Later state must not change the earlier time decision's working set.
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open", product_id=product_id, client_order_id=client_id,
        exchange_order_id="time-cutoff-provider-id",
    ))["action"] == "applied"
    owner.observe_order_event(cast(str, row["local_order_id"]))
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="filled", product_id=product_id, client_order_id=client_id,
        exchange_order_id="time-cutoff-provider-id", cumulative_filled_quantity=quantity,
        cumulative_average_price=price, last_fill_quantity=quantity - Decimal(cast(str, row["cumulative_contracts"])),
        last_fill_price=price, fee=Decimal("0"), fee_asset="USDT",
        last_fill_exchange_trade_id="time-cutoff-final",
    ))["action"] == "applied"
    owner.observe_order_event(cast(str, row["local_order_id"]))
    assert owner.readback(scope, cast(str, accepted.data["operation_id"])) is not None


def test_send_unknown_blocks_nonnull_snapshot_without_fabricating_empty_working_set(
    operation_pg: Engine,
) -> None:
    owner, scope, operation_id, factory, prepared, _initial, _order_manager, _confirmation, _source = _owner(operation_pg)
    runtime = _runtime(factory, scope)
    anchor_id = _observation_id("unknown-send-anchor")
    anchor = _model_time(
        scope, prepared, runtime, observation_id=anchor_id, event="ANCHOR",
        epoch_id="unknown-send-epoch", observed_utc_ms=_UTC, elapsed_ms=1_000,
    )
    assert owner.receive_source_time(anchor).success
    source_group, request_payload, _members = _saved_initial_place_request(operation_pg, scope)

    class FailingClient:
        def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
            raise OSError("synthetic transport ambiguity")

    transport_owner = _transport_enabled_owner(owner, scope)
    outcome = transport_owner.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, FailingClient())),
    )
    assert outcome.disposition == "RECORDED"
    assert transport_owner.readback(scope, operation_id) is not None
    current = _runtime(factory, scope)
    timer = _model_time(
        scope, prepared, current, observation_id=_observation_id("unknown-send-time"),
        event="TIMER", epoch_id="unknown-send-epoch", observed_utc_ms=_UTC + 1_000,
        elapsed_ms=1_000, predecessor_id=anchor_id,
        snapshots=_time_snapshot(prepared, _UTC + 1_000, []),
    )
    rejected = transport_owner.receive_source_time(timer)
    assert not rejected.success and rejected.data is not None
    assert rejected.data["disposition"] == "REJECTED"
    assert len(_time_rows(factory, scope)) == 1


def test_later_transport_result_does_not_rewrite_pre_send_empty_snapshot(
    operation_pg: Engine,
) -> None:
    owner, scope, operation_id, factory, prepared, _initial, _order_manager, _confirmation, _source = _owner(operation_pg)
    anchor_id = _observation_id("pre-send-anchor")
    anchor = _model_time(
        scope, prepared, _runtime(factory, scope), observation_id=anchor_id,
        event="ANCHOR", epoch_id="pre-send-epoch", observed_utc_ms=_UTC,
        elapsed_ms=1_000,
    )
    assert owner.receive_source_time(anchor).success
    timer = _model_time(
        scope, prepared, _runtime(factory, scope), observation_id=_observation_id("pre-send-time"),
        event="TIMER", epoch_id="pre-send-epoch", observed_utc_ms=_UTC + 1_000,
        elapsed_ms=1_000, predecessor_id=anchor_id,
        snapshots=_time_snapshot(prepared, _UTC + 1_000, []),
    )
    accepted_time = owner.receive_source_time(timer)
    assert accepted_time.success and accepted_time.data is not None
    assert accepted_time.data["delivery_disposition"] == "CONSUMED"

    source_group, request_payload, members = _saved_initial_place_request(operation_pg, scope)
    transport_owner = _transport_enabled_owner(owner, scope)
    outcome = transport_owner.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, _AcceptedClient(members))),
    )
    assert outcome.disposition == "RECORDED"
    assert transport_owner.readback(scope, operation_id) is not None
    assert transport_owner.readback(scope, cast(str, accepted_time.data["operation_id"])) is not None


def test_active_resubmitted_child_replaces_terminal_ancestor_in_working_set(
    operation_pg: Engine,
    mock_clock: Any,
    mock_exchange_adapter: Any,
) -> None:
    owner, scope, operation_id, factory, prepared, _initial, order_manager, _confirmation, _source = _owner(operation_pg)
    source_group, request_payload, members = _saved_initial_place_request(operation_pg, scope)
    initial_response = {
        "code": "0",
        "data": [
            {
                "clOrdId": member["wire_clOrdId"],
                "ordId": "",
                "sCode": "51008",
            }
            for index, member in enumerate(members)
        ],
        "msg": "one rejected terminal ancestor for resubmission test",
    }
    transport_owner = _transport_enabled_owner(owner, scope)
    assert transport_owner.transmit_initial_place_group(
        scope, source_group, scope.execution_scope_id, request_payload,
        OkxSpiderBatchTransport(cast(Any, _ResponseClient(initial_response))),
    ).disposition == "RECORDED"
    runtime = _runtime(factory, scope)
    target = cast(str, members[0]["action_id"])
    saved = MemberResubmissionRequest.from_object({
        "schema_version": "spider_member_resubmission_request_v1",
        "environment": scope.environment,
        "execution_scope_id": scope.execution_scope_id,
        "strategy_id": scope.strategy_id,
        "actor": "operator",
        "idempotency_key": "source-time-resubmit-frontier",
        "expected_version": runtime.state_version,
        "generation": runtime.generation,
        "configuration_sha256": runtime.configuration_sha256,
        "initial_plan_id": runtime.initial_plan_id,
        "target_action_id": target,
    })
    assert transport_owner.record_member_resubmission_request(saved.canonical_request_bytes) is not None
    command = _command(scope, key=saved.idempotency_key, version=saved.expected_version)
    command["command"] = "RESUBMIT"
    command["params"]["actor"] = "operator"
    assert _dispatch(transport_owner, command).success
    child_proof = transport_owner._member_resubmission_readback(scope, saved)
    assert child_proof is not None and child_proof["disposition"] == "PLANNED"
    child_id = cast(str, child_proof["resubmission_action_id"])
    with factory() as session:
        child_action, child_order = session.execute(
            select(SpiderRuntimeAction, Order)
            .join(Order, Order.id == SpiderRuntimeAction.local_order_id)
            .where(SpiderRuntimeAction.action_id == child_id)
        ).one()
        child_order_id = child_order.id
        child_product, child_client = child_order.product_id, child_order.client_order_id
        child_price, child_quantity, child_created = child_order.price, child_order.quantity, child_order.timestamp
        source = cast(dict[str, object], decode_canonical(bytes(child_action.original_payload)))
        child_price_text = cast(str, source["px"])
        ancestor_action = session.get(SpiderRuntimeAction, target)
        assert ancestor_action is not None and ancestor_action.local_order_id is not None
        ancestor_order = session.get(Order, ancestor_action.local_order_id)
        assert ancestor_order is not None
        ancestor_order_id = ancestor_order.id
        ancestor_product, ancestor_client = ancestor_order.product_id, ancestor_order.client_order_id
    assert child_price is not None and type(child_created) is int
    execution = ExecutionEngine(
        db_session=None, clock=mock_clock, adapter=mock_exchange_adapter,
        db_session_factory=factory,
        repository_transaction_participant=transport_owner.participate_in_order_transaction,
        is_backtest=True,
    )
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open", product_id=child_product, client_order_id=child_client,
        exchange_order_id="active-resubmitted-child",
    ))["action"] == "applied"
    transport_owner.observe_order_event(child_order_id)

    anchor_id = _observation_id("resubmit-working-set-anchor")
    anchor = _model_time(
        scope, prepared, _runtime(factory, scope), observation_id=anchor_id,
        event="ANCHOR", epoch_id="resubmit-working-set-epoch", observed_utc_ms=_UTC,
        elapsed_ms=1_000,
    )
    assert transport_owner.receive_source_time(anchor).success
    row: dict[str, object] = {
        "local_order_id": child_order_id, "state": "live", "price": child_price_text,
        "quantity_contracts": format(child_quantity.normalize(), "f"),
        "cumulative_contracts": "0", "created_at_ms": child_created,
    }
    first_timer_id = _observation_id("resubmit-working-set-time")
    timer = _model_time(
        scope, prepared, _runtime(factory, scope), observation_id=first_timer_id,
        event="TIMER", epoch_id="resubmit-working-set-epoch", observed_utc_ms=_UTC + 1_000,
        elapsed_ms=1_000, predecessor_id=anchor_id,
        snapshots=_time_snapshot(prepared, _UTC + 1_000, [row]),
    )
    accepted = transport_owner.receive_source_time(timer)
    assert accepted.success and accepted.data is not None
    assert accepted.data["delivery_disposition"] == "CONSUMED"
    assert transport_owner.readback(scope, cast(str, accepted.data["operation_id"])) is not None

    # A contradictory late active ancestor must not disappear behind the child frontier.
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open", product_id=ancestor_product, client_order_id=ancestor_client,
        exchange_order_id="late-active-ancestor",
    ))["action"] == "applied"
    transport_owner.observe_order_event(ancestor_order_id)
    contradictory = _model_time(
        scope, prepared, _runtime(factory, scope), observation_id=_observation_id("late-ancestor-time"),
        event="TIMER", epoch_id="resubmit-working-set-epoch", observed_utc_ms=_UTC + 2_000,
        elapsed_ms=1_000, predecessor_id=first_timer_id,
        snapshots=_time_snapshot(prepared, _UTC + 2_000, [row]),
    )
    rejected = transport_owner.receive_source_time(contradictory)
    assert not rejected.success and rejected.data is not None
    assert rejected.data["disposition"] == "REJECTED"
    assert rejected.data["reason_code"] == "SPIDER_SOURCE_TIME_INVALID"


class _ResponseClient:
    def __init__(self, response: dict[str, object]) -> None:
        self.response = response

    def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
        return self.response


class _AcceptedClient:
    def __init__(self, members: list[dict[str, object]]) -> None:
        self.members = members

    def privatePostTradeBatchOrders(self, _orders: list[dict[str, object]]) -> object:
        return {
            "code": "0",
            "data": [
                {"clOrdId": member["wire_clOrdId"], "ordId": f"cutoff-{index}", "sCode": "0"}
                for index, member in enumerate(self.members)
            ],
            "msg": "synthetic accepted after source-time cutoff",
        }
