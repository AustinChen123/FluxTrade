"""Actual PostgreSQL SOURCE_MARKET receipt and checkpoint consumption."""

from __future__ import annotations

import os
from hashlib import sha256
from threading import RLock
from typing import Any, cast

import pytest
from sqlalchemy import event, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

pytest_plugins = ["test_migrations"]
pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.getenv("FLUXTRADE_RUN_POSTGRES_MIGRATION_TESTS") != "1",
        reason="explicit isolated PostgreSQL opt-in required",
    ),
]

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical  # noqa: E402
from src.core.spider_management.orm import (  # noqa: E402
    SpiderRuntimeDelivery,
    SpiderRuntimePlan,
    SpiderRuntimeScope,
    SpiderRuntimeTapeIndex,
)
from src.core.orm_models import Order, Trade  # noqa: E402
from src.core.spider_management.runtime_owner import RuntimeScopeKey, SpiderRuntimeOwner  # noqa: E402
from src.core.strategy_state_manager import StrategyStateManager  # noqa: E402
from test_migrations import fresh_pg_db as _migration_fresh_pg_db  # noqa: E402
from test_spider_control_command_pg import _dispatch  # noqa: E402
from test_spider_financial_delivery_pg import _install_owner  # noqa: E402
from test_spider_operation_request_pg import operation_pg as _operation_pg  # noqa: E402

fresh_pg_db = _migration_fresh_pg_db
operation_pg = _operation_pg


class _RedisProbe:
    def publish(self, _channel: str, _message: str) -> int:
        return 1


def _owner(engine: Engine):
    install_owner, scope, operation_id, order_manager, prepared, initial, confirmation, source = (
        _install_owner(engine)
    )
    factory = sessionmaker(engine)
    lifecycle_lock = RLock()
    owner = SpiderRuntimeOwner(
        factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={confirmation.request_id: source},
        strategy_state_manager=StrategyStateManager(factory, _RedisProbe()),
        lifecycle_lock=lambda _strategy_id: lifecycle_lock,
        order_event_lock=RLock(),
    )
    assert install_owner.readback(scope, operation_id) == initial
    return owner, scope, operation_id, factory, prepared, initial, order_manager, confirmation, source


def _market_payload(
    scope: RuntimeScopeKey,
    prepared: Any,
    snapshot_id: str,
    *,
    price: str,
    ratio: str = "0.75",
    observed_at_ms: int = 1_800_000_000_000,
    snapshot_as_of_ms: int = 1_800_000_000_000,
) -> bytes:
    checkpoint = cast(dict[str, Any], decode_canonical(prepared.build_checkpoint_bytes))
    original_markets = cast(list[dict[str, Any]], checkpoint["markets"])
    markets = []
    for row in original_markets:
        value = dict(cast(dict[str, Any], row["value"]))
        value["price"] = {"tag": "DECIMAL", "value": price}
        value["ratioHL"] = {"tag": "DECIMAL", "value": ratio}
        markets.append({"product_id": row["product_id"], "value": value})
    return canonical_bytes({
        "schema_version": "SPIDER_SOURCE_MARKET_V1",
        "scope": {
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "strategy_id": scope.strategy_id,
        },
        "effective_identity": {
            "generation": 1,
            "confirmation_request_id": prepared.runtime_selection["confirmation_request_id"],
            "configuration_sha256": prepared.runtime_selection["configuration_sha256"],
        },
        "snapshot_id": snapshot_id,
        "observed_at_ms": observed_at_ms,
        "snapshot_as_of_ms": snapshot_as_of_ms,
        "provenance": {
            "kind": "MODEL",
            "evidence_sha256": prepared.historical_input_artifact_sha256,
        },
        "markets": markets,
    })


def _market_state(checkpoint: bytes) -> tuple[Any, Any]:
    decoded = cast(dict[str, Any], decode_canonical(checkpoint))
    return decoded["markets"], {
        key: value for key, value in decoded.items() if key != "markets"
    }


def _source_rows(factory, scope: RuntimeScopeKey):
    with factory() as session:
        return session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()


def _financial_facts(factory, scope: RuntimeScopeKey):
    with factory() as session:
        orders = session.execute(
            select(
                Order.id, Order.product_id, Order.status, Order.side, Order.type,
                Order.price, Order.quantity, Order.filled_quantity,
                Order.exchange_order_id, Order.client_order_id, Order.intent_payload,
            ).where(Order.strategy_id == scope.strategy_id).order_by(Order.id)
        ).all()
        trades = session.execute(
            select(
                Trade.id, Trade.order_id, Trade.quantity, Trade.price, Trade.fee,
                Trade.fee_asset, Trade.exchange_trade_id,
            ).join(Order, Order.id == Trade.order_id)
            .where(Order.strategy_id == scope.strategy_id).order_by(Trade.id)
        ).all()
        return tuple(orders), tuple(trades)


def test_market_source_receipt_exact_duplicate_conflict_and_checkpoint_chain(operation_pg: Engine):
    owner, scope, operation_id, factory, prepared, initial, order_manager, confirmation, source = _owner(operation_pg)
    original_checkpoint = bytes(prepared.build_checkpoint_bytes)
    baseline_markets, baseline_nonmarket = _market_state(original_checkpoint)
    original_financial_facts = _financial_facts(factory, scope)
    first = _market_payload(
        scope, prepared, "a" * 64, price="12.5",
        observed_at_ms=1_800_000_000_003, snapshot_as_of_ms=1_800_000_000_002,
    )
    result = owner.receive_source_market(first)
    assert result.success is True and result.data is not None
    assert result.data["disposition"] == "ACCEPTED"
    assert result.data["delivery_sequence"] == 1
    assert result.data["delivery_disposition"] == "CONSUMED"
    readback = owner.readback(scope, operation_id)
    assert readback is not None
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        current_checkpoint = bytes(runtime.source_checkpoint)
        current_checkpoint_sha256 = runtime.source_checkpoint_sha256
        plan = session.execute(
            select(SpiderRuntimePlan).where(
                SpiderRuntimePlan.plan_id == runtime.initial_plan_id
            )
        ).scalar_one()
        delivery = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.sequence == 1,
            )
        ).scalar_one()
        tape = session.execute(
            select(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id,
                SpiderRuntimeTapeIndex.ordinal == 1,
            )
        ).scalar_one()
        assert delivery.kind == "SOURCE_MARKET"
        assert delivery.disposition == "CONSUMED"
        assert delivery.source_payload == first
        assert delivery.content_sha256 == sha256(first).hexdigest()
        assert delivery.local_order_id is None and delivery.action_id is None
        assert delivery.trade_id is None and delivery.role is None
        assert tape.kind == "DELIVERY" and tape.delivery_sequence == 1
        assert runtime.last_received_sequence == runtime.last_consumed_sequence == 1
        assert runtime.control_mode == "RUN"
        assert plan.plan_status == "INCOMPLETE"
    actual_markets, actual_nonmarket = _market_state(current_checkpoint)
    assert actual_markets != baseline_markets
    assert actual_nonmarket == baseline_nonmarket
    assert current_checkpoint_sha256 == sha256(current_checkpoint).hexdigest()
    assert readback["checkpoint_sha256"] == current_checkpoint_sha256
    assert readback["trading_ready"] is False
    assert readback["phase"] == "INCOMPLETE"
    assert readback["initial_plan_missing_members"] == initial[
        "initial_plan_missing_members"
    ]

    duplicate = owner.receive_source_market(first)
    assert duplicate.success is True and duplicate.data is not None
    assert duplicate.data["disposition"] == "DUPLICATE"
    assert duplicate.data["delivery_sequence"] == 1
    conflicting = _market_payload(scope, prepared, "a" * 64, price="13.5")
    conflict = owner.receive_source_market(conflicting)
    assert conflict.success is False and conflict.data is not None
    assert conflict.data["disposition"] == "CONFLICT"
    assert owner.readback(scope, operation_id) == readback
    assert len(_source_rows(factory, scope)) == 1

    second = _market_payload(
        scope, prepared, "b" * 64, price="14.5", ratio="0.5",
        observed_at_ms=1_800_000_000_001, snapshot_as_of_ms=1_800_000_000_000,
    )
    second_result = owner.receive_source_market(second)
    assert second_result.success is True and second_result.data is not None
    assert second_result.data["delivery_sequence"] == 2
    after_second = owner.readback(scope, operation_id)
    assert after_second is not None
    with factory() as session:
        checkpoint_after_second = bytes(session.execute(
            select(SpiderRuntimeScope.source_checkpoint).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one())
    final_markets, final_nonmarket = _market_state(checkpoint_after_second)
    assert final_markets != actual_markets
    assert final_nonmarket == baseline_nonmarket
    assert after_second["initial_plan_missing_members"] == initial[
        "initial_plan_missing_members"
    ]
    assert after_second["trading_ready"] is False
    assert [row.sequence for row in _source_rows(factory, scope)] == [1, 2]
    assert _financial_facts(factory, scope) == original_financial_facts

    stopped = _dispatch(owner, {
        "command": "STOP",
        "strategy_id": scope.strategy_id,
        "params": {
            "strategy_id": scope.strategy_id,
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "actor": "api_key",
            "idempotency_key": "source-market-stop",
            "expected_version": cast(int, initial["state_version"]),
        },
    })
    assert stopped.success is True
    held = _market_payload(scope, prepared, "c" * 64, price="15.5")
    held_result = owner.receive_source_market(held)
    assert held_result.success is True and held_result.data is not None
    assert held_result.data["delivery_disposition"] == "SUPPRESSED"
    final = owner.readback(scope, operation_id)
    assert final is not None and final["runtime_control"] == "STOP"
    assert final["checkpoint_sha256"] == after_second["checkpoint_sha256"]
    assert final["trading_ready"] is False
    rows = _source_rows(factory, scope)
    assert [row.disposition for row in rows] == ["CONSUMED", "CONSUMED", "SUPPRESSED"]
    assert _financial_facts(factory, scope) == original_financial_facts
    assert owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert owner.readback(scope, operation_id) == final


def test_market_source_boolean_generation_rejects_without_owner_writes(operation_pg: Engine):
    owner, scope, operation_id, factory, prepared, _initial, *_rest = _owner(operation_pg)
    before_readback = owner.readback(scope, operation_id)
    assert before_readback is not None
    before_financial = _financial_facts(factory, scope)
    before_rows = tuple(
        (row.delivery_id, row.sequence, row.content_sha256, bytes(row.source_payload))
        for row in _source_rows(factory, scope)
    )
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        before_frontiers = (
            runtime.last_received_sequence,
            runtime.last_consumed_sequence,
            runtime.last_tape_sequence,
        )

    value = cast(dict[str, Any], decode_canonical(
        _market_payload(scope, prepared, "9" * 64, price="26.5")
    ))
    cast(dict[str, Any], value["effective_identity"])["generation"] = True
    result = owner.receive_source_market(canonical_bytes(value))

    assert result.success is False and result.data is not None
    assert result.data["disposition"] == "REJECTED"
    assert result.data["reason_code"] == "SPIDER_SOURCE_MARKET_INVALID"
    assert owner.readback(scope, operation_id) == before_readback
    assert _financial_facts(factory, scope) == before_financial
    assert tuple(
        (row.delivery_id, row.sequence, row.content_sha256, bytes(row.source_payload))
        for row in _source_rows(factory, scope)
    ) == before_rows
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        assert (
            runtime.last_received_sequence,
            runtime.last_consumed_sequence,
            runtime.last_tape_sequence,
        ) == before_frontiers


def test_market_source_does_not_skip_earlier_financial_delivery(operation_pg: Engine):
    owner, scope, operation_id, factory, prepared, _initial, _order_manager, _confirmation, _source = _owner(operation_pg)
    from src.core.clock import BacktestClock
    from src.core.execution import ExecutionEngine
    from src.core.interfaces.exchange import ExchangeOrderEvent
    from src.core.interfaces.exchange import IExchangeAdapter
    from src.core.orm_models import Order

    class AdapterProbe:
        def supports_runtime_reconciliation(self) -> bool:
            return False

        def close(self) -> None:
            pass

    execution = ExecutionEngine(
        db_session=None,
        clock=BacktestClock(1_800_000_000),
        adapter=cast(IExchangeAdapter, AdapterProbe()),
        db_session_factory=factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    with factory() as session:
        order = session.execute(
            select(Order).where(Order.strategy_id == scope.strategy_id)
        ).scalars().first()
    assert order is not None
    result = execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id="source-market-prior-financial-order",
    ))
    assert result["action"] == "applied"
    with factory() as session:
        runtime_before_market = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        financial_prefix = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        prefix_facts = tuple(
            (
                row.delivery_id,
                row.sequence,
                row.kind,
                row.local_order_id,
                row.action_id,
                row.trade_id,
                row.content_sha256,
                bytes(row.source_payload),
                row.disposition,
            )
            for row in financial_prefix
        )
        assert [row.kind for row in financial_prefix] == ["ORDER_STATE", "ORDER_STATE"]
        assert [row.sequence for row in financial_prefix] == [1, 2]
        assert runtime_before_market.last_received_sequence == 2
        assert runtime_before_market.last_tape_sequence == 2
        assert bytes(runtime_before_market.source_checkpoint) == prepared.build_checkpoint_bytes
    market = _market_payload(scope, prepared, "d" * 64, price="17.5")
    received = owner.receive_source_market(market)
    assert received.success is True and received.data is not None
    assert received.data["delivery_sequence"] == 3
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        deliveries = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        assert runtime.last_received_sequence == 3
        assert runtime.last_tape_sequence == 3
        assert runtime.last_consumed_sequence == 0
        assert [row.kind for row in deliveries] == [
            "ORDER_STATE", "ORDER_STATE", "SOURCE_MARKET"
        ]
        assert [row.disposition for row in deliveries] == [
            "PENDING", "PENDING", "PENDING"
        ]
        assert tuple(
            (
                row.delivery_id,
                row.sequence,
                row.kind,
                row.local_order_id,
                row.action_id,
                row.trade_id,
                row.content_sha256,
                bytes(row.source_payload),
                row.disposition,
            )
            for row in deliveries[:2]
        ) == prefix_facts
        assert bytes(runtime.source_checkpoint) == prepared.build_checkpoint_bytes
    assert owner.readback(scope, operation_id) is not None


def test_market_source_recovers_pending_row_on_fresh_owned_restore(
    operation_pg: Engine, monkeypatch
):
    owner, scope, operation_id, factory, prepared, _initial, order_manager, confirmation, source = _owner(operation_pg)
    monkeypatch.setattr(owner, "_drain_source_market_scope", lambda scope: None)
    raw = _market_payload(scope, prepared, "f" * 64, price="21.5")
    result = owner.receive_source_market(raw)
    assert result.success is True and result.data is not None
    assert result.data["delivery_disposition"] == "PENDING"
    with factory() as session:
        row = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        assert row.disposition == "PENDING"
    lifecycle_lock = RLock()
    restored = SpiderRuntimeOwner(
        factory,
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={confirmation.request_id: source},
        strategy_state_manager=StrategyStateManager(factory, _RedisProbe()),
        lifecycle_lock=lambda _strategy_id: lifecycle_lock,
        order_event_lock=RLock(),
    )
    assert restored.restore_owned_runtimes() == frozenset({scope.strategy_id})
    readback = restored.readback(scope, operation_id)
    assert readback is not None
    assert readback["consumed_sequence"] == 1
    assert readback["trading_ready"] is False
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        row = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        assert runtime.last_received_sequence == runtime.last_consumed_sequence == 1
        assert row.source_payload == raw and row.disposition == "CONSUMED"


@pytest.mark.parametrize("fault", ["before_flush", "after_commit"])
def test_market_source_transaction_proves_commit_or_leaves_no_partial_rows(
    operation_pg: Engine, fault: str, monkeypatch
):
    owner, scope, operation_id, factory, prepared, _initial, *_rest = _owner(operation_pg)
    raw = _market_payload(scope, prepared, "9" * 64, price="23.5")
    fired = {"value": False}
    apply_calls: list[object] = []
    from src.core.backtest.spider_policy import Policy

    original_apply_market = Policy.apply_market

    def counted_apply_market(self, markets: dict[str, dict[str, object]]) -> None:
        apply_calls.append(markets)
        original_apply_market(self, markets)

    monkeypatch.setattr(Policy, "apply_market", counted_apply_market)

    def before_flush(session: Session, _flush_context: object, _instances: object) -> None:
        if any(
            isinstance(item, SpiderRuntimeDelivery) and item.kind == "SOURCE_MARKET"
            for item in session.new
        ):
            fired["value"] = True
            session.info["source_market_written"] = True
            if fault == "before_flush":
                raise RuntimeError("injected source-market pre-commit failure")

    def after_commit(session: Session) -> None:
        if fault == "after_commit" and session.info.pop("source_market_written", False):
            fired["value"] = True
            raise RuntimeError("injected source-market lost commit response")

    event.listen(Session, "before_flush", before_flush)
    event.listen(Session, "after_commit", after_commit)
    try:
        result = owner.receive_source_market(raw)
    finally:
        event.remove(Session, "before_flush", before_flush)
        event.remove(Session, "after_commit", after_commit)
    assert fired["value"] is True
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        rows = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            )
        ).scalars().all()
        tapes = session.execute(
            select(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            )
        ).scalars().all()
        if fault == "before_flush":
            assert runtime.last_received_sequence == 0
            assert runtime.last_tape_sequence == 0
            assert runtime.last_consumed_sequence == 0
            assert rows == [] and tapes == []
            assert result.success is False
            assert result.data is not None
            assert result.data["disposition"] == "UNKNOWN"
        else:
            assert runtime.last_received_sequence == 1
            assert runtime.last_tape_sequence == 1
            assert runtime.last_consumed_sequence == 0
            assert len(rows) == len(tapes) == 1
            assert rows[0].source_payload == raw
            assert rows[0].disposition == "PENDING"
            assert result.success is True and result.data is not None
            assert result.data["disposition"] == "ACCEPTED"
            assert result.data["delivery_disposition"] == "PENDING"
            assert runtime.source_checkpoint == prepared.build_checkpoint_bytes
            assert apply_calls == []
    if fault == "after_commit":
        proved = owner.readback(scope, operation_id)
        assert proved is not None and proved["consumed_sequence"] == 0
        assert owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
        assert len(apply_calls) == 1
        owner._drain_source_market_scope(scope)
        assert len(apply_calls) == 1
    assert owner.readback(scope, operation_id) is not None


@pytest.mark.parametrize("fault", ["before_flush", "after_commit"])
def test_market_source_consume_transaction_proves_rollback_or_commit(
    operation_pg: Engine, fault: str, monkeypatch
):
    owner, scope, operation_id, factory, prepared, *_rest = _owner(operation_pg)
    original_checkpoint = bytes(prepared.build_checkpoint_bytes)
    raw = _market_payload(scope, prepared, "a" * 64, price="24.5")
    drain = owner._drain_source_market_scope
    monkeypatch.setattr(owner, "_drain_source_market_scope", lambda _scope: None)
    received = owner.receive_source_market(raw)
    assert received.success is True and received.data is not None
    assert received.data["delivery_disposition"] == "PENDING"
    monkeypatch.setattr(owner, "_drain_source_market_scope", drain)

    from src.core.backtest.spider_policy import Policy

    apply_calls: list[object] = []
    original_apply_market = Policy.apply_market

    def counted_apply_market(self, markets: dict[str, dict[str, object]]) -> None:
        apply_calls.append(markets)
        original_apply_market(self, markets)

    monkeypatch.setattr(Policy, "apply_market", counted_apply_market)
    flags = {"staged": False, "rollback": False, "committed": False}

    def before_flush(session: Session, _flush_context: object, _instances: object) -> None:
        runtime = next(
            (item for item in session.dirty if isinstance(item, SpiderRuntimeScope)), None
        )
        delivery = next(
            (
                item for item in session.dirty
                if isinstance(item, SpiderRuntimeDelivery)
                and item.kind == "SOURCE_MARKET"
            ),
            None,
        )
        if (
            runtime is not None
            and delivery is not None
            and runtime.last_consumed_sequence == 1
            and delivery.disposition == "CONSUMED"
            and bytes(runtime.source_checkpoint) != original_checkpoint
        ):
            flags["staged"] = True
            session.info["source_market_consume_staged"] = True
            if fault == "before_flush":
                raise RuntimeError("injected source-market TXB pre-commit failure")

    def after_commit(session: Session) -> None:
        if fault == "after_commit" and session.info.pop(
            "source_market_consume_staged", False
        ):
            flags["committed"] = True
            raise RuntimeError("injected source-market TXB lost commit response")

    def after_rollback(session: Session) -> None:
        if session.info.pop("source_market_consume_staged", False):
            flags["rollback"] = True

    event.listen(Session, "before_flush", before_flush)
    event.listen(Session, "after_commit", after_commit)
    event.listen(Session, "after_rollback", after_rollback)
    try:
        assert owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
    finally:
        event.remove(Session, "before_flush", before_flush)
        event.remove(Session, "after_commit", after_commit)
        event.remove(Session, "after_rollback", after_rollback)

    assert flags["staged"] is True
    assert len(apply_calls) == 1
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        row = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        tape = session.execute(
            select(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        if fault == "before_flush":
            assert flags["rollback"] is True and flags["committed"] is False
            assert runtime.last_received_sequence == runtime.last_tape_sequence == 1
            assert runtime.last_consumed_sequence == 0
            assert bytes(runtime.source_checkpoint) == original_checkpoint
            assert row.disposition == "PENDING"
            assert (tape.ordinal, tape.kind, tape.delivery_sequence) == (1, "DELIVERY", 1)
            expected_consumed = 0
        else:
            assert flags["committed"] is True and flags["rollback"] is False
            assert runtime.last_received_sequence == runtime.last_tape_sequence == 1
            assert runtime.last_consumed_sequence == 1
            assert runtime.source_checkpoint != original_checkpoint
            assert runtime.source_checkpoint_sha256 == sha256(
                runtime.source_checkpoint
            ).hexdigest()
            assert row.disposition == "CONSUMED"
            assert (tape.ordinal, tape.kind, tape.delivery_sequence) == (1, "DELIVERY", 1)
            expected_consumed = 1
    readback = owner.readback(scope, operation_id)
    assert readback is not None and readback["consumed_sequence"] == expected_consumed
    owner._drain_source_market_scope(scope)
    assert len(apply_calls) == (2 if fault == "before_flush" else 1)
    assert owner.readback(scope, operation_id) is not None


def test_market_source_readback_rejects_corrupt_saved_bytes_then_recovers(
    operation_pg: Engine,
):
    owner, scope, operation_id, factory, prepared, _initial, *_rest = _owner(operation_pg)
    raw = _market_payload(scope, prepared, "8" * 64, price="25.5")
    result = owner.receive_source_market(raw)
    assert result.success is True
    baseline = owner.readback(scope, operation_id)
    assert baseline is not None
    original_row = _source_rows(factory, scope)[0]
    original_payload = bytes(original_row.source_payload)
    original_hash = original_row.content_sha256
    with factory.begin() as session:
        row = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.sequence == 1,
            ).with_for_update()
        ).scalar_one()
        row.content_sha256 = "f" * 64
    assert owner.readback(scope, operation_id) is None
    with factory.begin() as session:
        row = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.sequence == 1,
            ).with_for_update()
        ).scalar_one()
        row.source_payload = original_payload
        row.content_sha256 = original_hash
    assert owner.readback(scope, operation_id) == baseline


@pytest.mark.parametrize("tamper", ["checkpoint_nonmarket", "input_and_hash", "tape_swap"])
def test_market_source_cold_readback_rejects_coherent_chain_tampering(
    operation_pg: Engine, tamper: str
):
    owner, scope, operation_id, factory, prepared, *_rest = _owner(operation_pg)
    first = _market_payload(scope, prepared, "1" * 64, price="25.5")
    assert owner.receive_source_market(first).success is True
    second = None
    if tamper == "tape_swap":
        second = _market_payload(scope, prepared, "2" * 64, price="26.5")
        assert owner.receive_source_market(second).success is True
    baseline = owner.readback(scope, operation_id)
    assert baseline is not None
    before_financial = _financial_facts(factory, scope)
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        original_checkpoint = bytes(runtime.source_checkpoint)
        original_checkpoint_hash = runtime.source_checkpoint_sha256
        deliveries = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeDelivery.sequence)
        ).scalars().all()
        original_delivery_facts = tuple(
            (row.delivery_id, row.content_sha256, bytes(row.source_payload), row.disposition)
            for row in deliveries
        )
        tapes = session.execute(
            select(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeTapeIndex.ordinal)
        ).scalars().all()
        original_tape_rows = tuple(
            (row.ordinal, row.kind, row.delivery_sequence, row.transmission_group_id)
            for row in tapes
        )

    with factory.begin() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            ).with_for_update()
        ).scalar_one()
        rows = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeDelivery.sequence).with_for_update()
        ).scalars().all()
        tape_rows = session.execute(
            select(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeTapeIndex.ordinal).with_for_update()
        ).scalars().all()
        if tamper == "checkpoint_nonmarket":
            altered = cast(dict[str, Any], decode_canonical(runtime.source_checkpoint))
            altered["now_ms"] = cast(int, altered["now_ms"]) + 1
            runtime.source_checkpoint = canonical_bytes(altered)
            runtime.source_checkpoint_sha256 = sha256(runtime.source_checkpoint).hexdigest()
        elif tamper == "input_and_hash":
            altered = cast(dict[str, Any], decode_canonical(rows[0].source_payload))
            market = cast(dict[str, Any], cast(list[Any], altered["markets"])[0])
            cast(dict[str, Any], market["value"])["price"] = {
                "tag": "DECIMAL", "value": "31.5"
            }
            rows[0].source_payload = canonical_bytes(altered)
            rows[0].content_sha256 = sha256(rows[0].source_payload).hexdigest()
        else:
            assert len(tape_rows) == 2
            # Rotate the referenced receipt order via a temporary ordinal so
            # both immediate uniqueness and delivery foreign keys remain valid.
            tape_rows[0].ordinal = 3
            session.flush()
            tape_rows[1].ordinal = 1
            session.flush()
            tape_rows[0].ordinal = 2

    assert owner.readback(scope, operation_id) is None
    assert _financial_facts(factory, scope) == before_financial
    with factory.begin() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            ).with_for_update()
        ).scalar_one()
        rows = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeDelivery.sequence).with_for_update()
        ).scalars().all()
        tape_rows = session.execute(
            select(SpiderRuntimeTapeIndex).where(
                SpiderRuntimeTapeIndex.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeTapeIndex.ordinal).with_for_update()
        ).scalars().all()
        runtime.source_checkpoint = original_checkpoint
        runtime.source_checkpoint_sha256 = original_checkpoint_hash
        for row, (delivery_id, content_hash, payload, disposition) in zip(
            rows, original_delivery_facts, strict=True
        ):
            row.delivery_id = delivery_id
            row.content_sha256 = content_hash
            row.source_payload = payload
            row.disposition = disposition
        if tamper == "tape_swap":
            assert len(tape_rows) == 2
            tape_rows[1].ordinal = 3
            session.flush()
            tape_rows[0].ordinal = 2
            session.flush()
            tape_rows[1].ordinal = 1
        else:
            for row, (ordinal, kind, sequence, group_id) in zip(
                tape_rows, original_tape_rows, strict=True
            ):
                row.ordinal = ordinal
                row.kind = kind
                row.delivery_sequence = sequence
                row.transmission_group_id = group_id
    assert owner.readback(scope, operation_id) == baseline


def test_market_source_drain_predicate_leaves_source_pending_in_drain_mode(
    operation_pg: Engine, monkeypatch
):
    owner, scope, _operation_id, factory, prepared, *_rest = _owner(operation_pg)
    monkeypatch.setattr(owner, "_drain_source_market_scope", lambda _scope: None)
    raw = _market_payload(scope, prepared, "3" * 64, price="29.5")
    result = owner.receive_source_market(raw)
    assert result.success is True and result.data is not None
    assert result.data["delivery_disposition"] == "PENDING"
    with factory.begin() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            ).with_for_update()
        ).scalar_one()
        runtime.control_mode = "DRAIN"
    monkeypatch.undo()
    owner._drain_source_market_scope(scope)
    with factory() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        row = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            )
        ).scalar_one()
        assert runtime.control_mode == "DRAIN"
        assert runtime.last_received_sequence == 1
        assert runtime.last_consumed_sequence == 0
        assert runtime.source_checkpoint == prepared.build_checkpoint_bytes
        assert row.source_payload == raw and row.disposition == "PENDING"


def test_market_input_after_stop_is_suppressed_without_checkpoint_change(operation_pg: Engine):
    owner, scope, operation_id, factory, prepared, initial, *_rest = _owner(operation_pg)
    before = owner.readback(scope, operation_id)
    assert before is not None
    stopped = _dispatch(owner, {
        "command": "STOP",
        "strategy_id": scope.strategy_id,
        "params": {
            "strategy_id": scope.strategy_id,
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "actor": "api_key",
            "idempotency_key": "source-market-stop-before",
            "expected_version": cast(int, initial["state_version"]),
        },
    })
    assert stopped.success is True
    raw = _market_payload(scope, prepared, "7" * 64, price="26.5")
    result = owner.receive_source_market(raw)
    assert result.success is True and result.data is not None
    assert result.data["delivery_disposition"] == "SUPPRESSED"
    after = owner.readback(scope, operation_id)
    assert after is not None
    assert after["checkpoint_sha256"] == before["checkpoint_sha256"]
    assert after["consumed_sequence"] == 1
    assert after["runtime_control"] == "STOP"
    assert after["trading_ready"] is False
    rows = _source_rows(factory, scope)
    assert len(rows) == 1 and rows[0].disposition == "SUPPRESSED"


@pytest.mark.parametrize(
    "case", ["source_suppressed_in_run", "financial_suppressed_in_run", "stop_then_run"]
)
def test_suppressed_delivery_prefix_requires_current_stop_proof(
    operation_pg: Engine, case: str, monkeypatch
):
    owner, scope, operation_id, factory, prepared, initial, *_rest = _owner(operation_pg)
    before_financial = _financial_facts(factory, scope)
    if case == "source_suppressed_in_run":
        monkeypatch.setattr(owner, "_drain_source_market_scope", lambda _scope: None)
        raw = _market_payload(scope, prepared, "4" * 64, price="30.5")
        received = owner.receive_source_market(raw)
        assert received.success is True and received.data is not None
        assert received.data["delivery_disposition"] == "PENDING"
        monkeypatch.undo()
        baseline = owner.readback(scope, operation_id)
        assert baseline is not None
        with factory.begin() as session:
            runtime = session.execute(
                select(SpiderRuntimeScope).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                ).with_for_update()
            ).scalar_one()
            row = session.execute(
                select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id
                ).with_for_update()
            ).scalar_one()
            row.disposition = "SUPPRESSED"
            row.suppression_reason = "STOP_CONTROL"
            runtime.last_consumed_sequence = row.sequence
        restore_rows = ((1, "PENDING", None),)
        restore_consumed = 0
    elif case == "financial_suppressed_in_run":
        from src.core.clock import BacktestClock
        from src.core.execution import ExecutionEngine
        from src.core.interfaces.exchange import ExchangeOrderEvent, IExchangeAdapter
        from src.core.orm_models import Order

        class AdapterProbe:
            def supports_runtime_reconciliation(self) -> bool:
                return False

            def close(self) -> None:
                pass

        with factory() as session:
            order = session.execute(
                select(Order).where(Order.strategy_id == scope.strategy_id)
                .order_by(Order.id)
            ).scalars().first()
        assert order is not None
        execution = ExecutionEngine(
            db_session=None,
            clock=BacktestClock(1_800_000_000),
            adapter=cast(IExchangeAdapter, AdapterProbe()),
            db_session_factory=factory,
            repository_transaction_participant=owner.participate_in_order_transaction,
            is_backtest=True,
        )
        assert execution.process_exchange_order_event(ExchangeOrderEvent(
            status="open",
            product_id=order.product_id,
            client_order_id=order.client_order_id,
            exchange_order_id="suppressed-proof-provider-order",
        ))["action"] == "applied"
        before_financial = _financial_facts(factory, scope)
        baseline = owner.readback(scope, operation_id)
        assert baseline is not None
        with factory() as session:
            runtime = session.execute(
                select(SpiderRuntimeScope).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                )
            ).scalar_one()
            rows = session.execute(
                select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id
                ).order_by(SpiderRuntimeDelivery.sequence)
            ).scalars().all()
            assert [row.kind for row in rows] == ["ORDER_STATE", "ORDER_STATE"]
            assert runtime.last_received_sequence == 2
            original_rows = tuple(
                (row.sequence, row.disposition, row.suppression_reason) for row in rows
            )
        with factory.begin() as session:
            runtime = session.execute(
                select(SpiderRuntimeScope).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                ).with_for_update()
            ).scalar_one()
            rows = session.execute(
                select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id
                ).order_by(SpiderRuntimeDelivery.sequence).with_for_update()
            ).scalars().all()
            for row in rows:
                row.disposition = "SUPPRESSED"
                row.suppression_reason = "STOP_CONTROL"
            runtime.last_consumed_sequence = runtime.last_received_sequence
        restore_rows = tuple((sequence, disposition, reason) for sequence, disposition, reason in original_rows)
        restore_consumed = 0
    else:
        stopped = _dispatch(owner, {
            "command": "STOP",
            "strategy_id": scope.strategy_id,
            "params": {
                "strategy_id": scope.strategy_id,
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "actor": "api_key",
                "idempotency_key": "suppressed-proof-stop",
                "expected_version": cast(int, initial["state_version"]),
            },
        })
        assert stopped.success is True
        raw = _market_payload(scope, prepared, "5" * 64, price="31.5")
        received = owner.receive_source_market(raw)
        assert received.success is True and received.data is not None
        assert received.data["delivery_disposition"] == "SUPPRESSED"
        baseline = owner.readback(scope, operation_id)
        assert baseline is not None and baseline["runtime_control"] == "STOP"
        with factory.begin() as session:
            runtime = session.execute(
                select(SpiderRuntimeScope).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                ).with_for_update()
            ).scalar_one()
            runtime.control_mode = "RUN"
        assert owner.readback(scope, operation_id) is None
        with factory.begin() as session:
            runtime = session.execute(
                select(SpiderRuntimeScope).where(
                    SpiderRuntimeScope.strategy_id == scope.strategy_id
                ).with_for_update()
            ).scalar_one()
            runtime.control_mode = "STOP"
        assert owner.readback(scope, operation_id) == baseline
        assert _financial_facts(factory, scope) == before_financial
        return

    assert owner.readback(scope, operation_id) is None
    assert _financial_facts(factory, scope) == before_financial
    with factory.begin() as session:
        runtime = session.execute(
            select(SpiderRuntimeScope).where(
                SpiderRuntimeScope.strategy_id == scope.strategy_id
            ).with_for_update()
        ).scalar_one()
        rows = session.execute(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            ).order_by(SpiderRuntimeDelivery.sequence).with_for_update()
        ).scalars().all()
        assert tuple(
            (row.sequence, row.disposition, row.suppression_reason) for row in rows
        ) == tuple((sequence, "SUPPRESSED", "STOP_CONTROL") for sequence, _, _ in restore_rows)
        for row, (sequence, disposition, reason) in zip(rows, restore_rows, strict=True):
            assert row.sequence == sequence
            row.disposition = disposition
            row.suppression_reason = reason
        runtime.last_consumed_sequence = restore_consumed
    assert owner.readback(scope, operation_id) == baseline
    assert _financial_facts(factory, scope) == before_financial
