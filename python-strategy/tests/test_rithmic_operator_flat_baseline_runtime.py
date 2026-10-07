"""Causal Redis/SQLite coverage for operator-approved Rithmic flat baselines."""

from __future__ import annotations

import os
from contextlib import nullcontext
from dataclasses import replace
from decimal import Decimal
from threading import Event
from types import SimpleNamespace
from typing import Any, Callable, cast
from unittest.mock import MagicMock
from urllib.parse import urlparse
from uuid import uuid4

import pytest
from redis import Redis
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.adapters.rithmic_operator_flat_baseline import (
    read_latest_rithmic_operator_flat_baseline,
)
from src.core.adapters.rithmic_runtime_composition import (
    RithmicRuntimeCallbacks,
    build_rithmic_runtime_owners,
)
from src.core.execution import ExecutionEngine
from src.core.engine_boot_state_service import EngineBootStateService
from src.core.interfaces.exchange import OwnedOrderReconciliationContext
from src.core.ops_safety import OpsSafetyService
from src.core.ops_command_service import OpsCommandService
from src.core.orm_models import (
    Base,
    Exchange,
    Order,
    Product,
    Strategy,
    SystemEvent,
    Trade,
)
from src.core.product_registry import to_rithmic_symbol
from src.core.risk_manager import AccountService
from src.core.runtime_environment import RuntimeEnvironment


def _redis_endpoint() -> str:
    endpoint = os.environ.get("RITHMIC_TEST_REDIS_URL")
    if not endpoint:
        pytest.skip("set RITHMIC_TEST_REDIS_URL to the isolated loopback Redis")
    parsed = urlparse(endpoint)
    if parsed.scheme != "redis" or parsed.hostname not in {"127.0.0.1", "localhost"}:
        pytest.fail("RITHMIC_TEST_REDIS_URL must name an explicit loopback Redis")
    if parsed.path not in {"/0", ""}:
        pytest.fail("RITHMIC_TEST_REDIS_URL must use the isolated database 0")
    return endpoint


def _runtime_callbacks() -> RithmicRuntimeCallbacks:
    return RithmicRuntimeCallbacks(
        is_running=MagicMock(return_value=True),
        publish_worker=MagicMock(),
        on_runtime_started=MagicMock(return_value=True),
        reconcile_if_needed=MagicMock(return_value=True),
        process_event=MagicMock(return_value={"action": "applied"}),
        lockdown=MagicMock(),
        assert_runtime_leadership=MagicMock(),
        halt_submissions=MagicMock(),
        clear_local_halt=MagicMock(),
        persist_lockdown_state=MagicMock(),
        persist_redis_lockdown=MagicMock(),
        stop_order_event_stream=MagicMock(return_value=True),
        start_order_event_stream=MagicMock(),
        current_order_event_thread=MagicMock(return_value=None),
        publish_authoritative_summary=MagicMock(),
    )


def _snapshot(account_id: str) -> SimpleNamespace:
    return SimpleNamespace(
        account_id=account_id,
        account_currency="USD",
        account_summary=SimpleNamespace(
            account_balance=Decimal("50000.25"),
            day_pnl=Decimal("0"),
            timestamp_ms=1_700_000_000_000,
        ),
        positions=(),
        orders=(),
        order_history=(),
        fills=(),
    )


def _prepare_event_table(session_factory) -> None:
    Base.metadata.create_all(
        session_factory.kw["bind"],
        tables=[SystemEvent.__table__],
    )


def _prepare_runtime_sql_scope(session_factory, product_ids: tuple[str, ...]) -> None:
    Base.metadata.create_all(
        session_factory.kw["bind"],
        tables=[
            Exchange.__table__,
            Product.__table__,
            Strategy.__table__,
            SystemEvent.__table__,
        ],
    )
    with session_factory() as session:
        for exchange_id in {product_id.split(":", 1)[0] for product_id in product_ids}:
            if session.get(Exchange, exchange_id) is None:
                session.add(Exchange(id=exchange_id, name=exchange_id.title()))
        for product_id in product_ids:
            if session.get(Product, product_id) is None:
                exchange_id, symbol = product_id.split(":", 1)
                session.add(
                    Product(
                        id=product_id,
                        exchange_id=exchange_id,
                        base_asset=symbol.split("-", 1)[0],
                        quote_asset="USD",
                    )
                )
        for strategy_id in ("history-strategy", "matrix-history"):
            if session.get(Strategy, strategy_id) is None:
                session.add(Strategy(id=strategy_id, name=strategy_id))
        session.commit()


def _baseline_count(session_factory) -> int:
    with session_factory() as session:
        return int(
            session.scalar(
                select(func.count())
                .select_from(SystemEvent)
                .where(SystemEvent.event_subtype == "rithmic_operator_flat_baseline")
            )
            or 0
        )


def _positions_for_product(account_service, product_id: str) -> list[Any]:
    return [
        position
        for position in account_service.get_all_positions()
        if position.product_id == product_id
    ]


def _unrelated_position_state(case) -> dict[str, str]:
    return case.redis_client.hgetall(case.unrelated_redis_key)


def _seed_historical_order_and_trade(
    session_factory, product_id: str
) -> tuple[str, str]:
    order_id = f"history-order-{uuid4().hex}"
    trade_id = f"history-trade-{uuid4().hex}"
    with session_factory() as session:
        session.add(
            Order(
                id=order_id,
                exchange_order_id=f"history-basket-{uuid4().hex}",
                strategy_id="history-strategy",
                product_id=product_id,
                exchange_id="RITHMIC",
                account_profile="test",
                account_id="TEST-ACCOUNT",
                type="market",
                side="buy",
                price=Decimal("20000.25"),
                quantity=Decimal("1.000"),
                status="FILLED",
                timestamp=1_700_000_000_000,
                filled_quantity=Decimal("1.000"),
                filled_price=Decimal("20000.25"),
                client_order_id=f"history-client-{uuid4().hex}",
                intent_payload={},
            )
        )
        session.add(
            Trade(
                id=trade_id,
                order_id=order_id,
                exchange_trade_id=f"history-execution-{uuid4().hex}",
                product_id=product_id,
                side="buy",
                price=Decimal("20000.2500"),
                quantity=Decimal("1.000"),
                fee=Decimal("0.2500"),
                fee_asset="USD",
                timestamp=1_700_000_001_000,
            )
        )
        session.commit()
    return order_id, trade_id


def _order_trade_rows(
    session_factory, order_id: str, trade_id: str
) -> tuple[tuple, tuple]:
    with session_factory() as session:
        order = session.get(Order, order_id)
        trade = session.get(Trade, trade_id)
        assert order is not None and trade is not None
        return (
            (
                order.id,
                order.exchange_order_id,
                order.strategy_id,
                order.product_id,
                order.side,
                order.quantity,
                order.filled_quantity,
                order.filled_price,
                order.status,
            ),
            (
                trade.id,
                trade.order_id,
                trade.exchange_trade_id,
                trade.product_id,
                trade.side,
                trade.price,
                trade.quantity,
                trade.fee,
                trade.fee_asset,
            ),
        )


def _insert_scoped_order(
    session_factory,
    *,
    product_id: str,
    account_id: str,
    profile: str,
    exchange_id: str = "RITHMIC",
) -> str:
    _prepare_runtime_sql_scope(session_factory, (product_id,))
    order_id = f"matrix-order-{uuid4().hex}"
    with session_factory() as session:
        session.add(
            Order(
                id=order_id,
                exchange_order_id=f"matrix-basket-{uuid4().hex}",
                strategy_id="matrix-history",
                product_id=product_id,
                exchange_id=exchange_id,
                account_profile=profile,
                account_id=account_id,
                type="market",
                side="buy",
                price=Decimal("20000.25"),
                quantity=Decimal("1.000"),
                status="CANCELLED",
                timestamp=1_700_000_010_000,
                filled_quantity=Decimal("0"),
                filled_price=None,
                client_order_id=f"matrix-client-{uuid4().hex}",
                intent_payload={},
            )
        )
        session.commit()
    return order_id


def _build_runtime_case(
    monkeypatch,
    request,
    session_factory,
    *,
    account_id: str = "TEST-ACCOUNT",
    balance_account_id: str = "TEST-ACCOUNT",
    local_position: bool = True,
    product_id: str | None = None,
    strategy_id: str | None = None,
):
    endpoint = _redis_endpoint()
    product_id = product_id or f"RITHMIC:QT{uuid4().hex[:6].upper()}-202609"
    _prepare_runtime_sql_scope(session_factory, (product_id,))
    strategy_id = strategy_id or f"matrix-{uuid4().hex}"
    profile = "test"
    unrelated_product_id = f"RITHMIC:UNRELATED{uuid4().hex[:6].upper()}-202612"
    unrelated_strategy_id = f"unrelated-{uuid4().hex}"
    snapshot_holder: dict[str, Any] = {"snapshot": _snapshot(account_id)}
    redis_client = Redis.from_url(endpoint, decode_responses=True)
    redis_client.ping()
    monkeypatch.setattr(
        "src.core.risk_manager.create_redis_client",
        lambda: Redis.from_url(endpoint, decode_responses=True),
    )
    account_service = AccountService()
    account_service.configure_authoritative_balance(
        venue="rithmic",
        account_id=balance_account_id,
        max_age_seconds=600,
        runtime_environment=RuntimeEnvironment("test"),
    )
    redis_key = f"state:position:{strategy_id}:{product_id}"
    unrelated_redis_key = (
        f"state:position:{unrelated_strategy_id}:{unrelated_product_id}"
    )
    if local_position:
        redis_client.hset(
            redis_key,
            mapping={"quantity": "2", "entry_price": "20000.25"},
        )
    redis_client.hset(
        unrelated_redis_key,
        mapping={"quantity": "0.5", "entry_price": "19000.75"},
    )
    request.addfinalizer(lambda: redis_client.delete(redis_key, unrelated_redis_key))
    account_service.get_all_positions()
    adapter = RithmicExchangeAdapter(
        profile=profile,
        account_id=account_id,
        instruments={
            product_id: {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        client_factory=MagicMock(),
    )
    execution_engine = MagicMock(spec=ExecutionEngine)
    execution_engine.clock = MagicMock()
    execution_engine.clock.now.return_value = 1_700_000_000.0
    execution_engine.audit_external_orders = True
    execution_engine._db_session_factory = session_factory
    execution_engine.list_recoverable_client_orders.return_value = []
    reconciliation = adapter.create_owned_order_reconciler(
        OwnedOrderReconciliationContext(
            list_recoverable_client_orders=lambda: [],
            process_exchange_order_event=MagicMock(return_value={"action": "applied"}),
            now_seconds=lambda: 1_700_000_000,
            db_session_factory=session_factory,
            local_positions_loader=cast(
                Callable[[], list[object]], account_service.get_all_positions
            ),
            logger=MagicMock(),
        )
    )

    def load_case_snapshot(*_args, **_kwargs):
        if "error" in snapshot_holder:
            raise snapshot_holder["error"]
        return snapshot_holder["snapshot"]

    def reconcile_owned_orders(*, snapshot_loader=None, startup_position_restorer=None):
        return reconciliation.reconcile(
            snapshot_loader=load_case_snapshot,
            startup_position_restorer=startup_position_restorer,
        )

    execution_engine.reconcile_owned_orders.side_effect = reconcile_owned_orders
    ops_safety = MagicMock(spec=OpsSafetyService)
    ops_safety.kill_switch_with_authoritative_positions.return_value = {
        "cancelled_orders": 0,
        "cancel_failures": [],
        "flattened_positions": 0,
        "flatten_pending": [],
        "flatten_failures": [],
        "recovery_failures": [],
        "already_flat": True,
        "drain_timeout": False,
    }
    owners = build_rithmic_runtime_owners(
        adapter=adapter,
        profile=profile,
        account_id=account_id,
        execution_engine=execution_engine,
        account_service=account_service,
        ops_safety=ops_safety,
        stop_event=Event(),
        callbacks=_runtime_callbacks(),
        logger=MagicMock(),
    )
    emergency_flatten = owners.emergency_flatten
    assert emergency_flatten is not None
    emergency_flatten.stop_current_worker = lambda **_kwargs: True
    emergency_flatten.restart_generic_worker = lambda: None

    monkeypatch.setattr(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        load_case_snapshot,
    )
    request.addfinalizer(account_service.close)
    request.addfinalizer(redis_client.close)
    return SimpleNamespace(
        account_service=account_service,
        adapter=adapter,
        emergency_flatten=emergency_flatten,
        execution_engine=execution_engine,
        ops_safety=ops_safety,
        owners=owners,
        product_id=product_id,
        profile=profile,
        redis_client=redis_client,
        redis_key=redis_key,
        snapshot_holder=snapshot_holder,
        strategy_id=strategy_id,
        unrelated_product_id=unrelated_product_id,
        unrelated_redis_key=unrelated_redis_key,
        reconciliation=reconciliation,
    )


def _dispatch_operator_flatten(case, *, operation_id: str | None):
    results: list[dict[str, Any]] = []
    command = OpsCommandService(
        operation_lock=nullcontext,
        kill_switch_operation_completed=lambda **_kwargs: False,
        halt_for_kill_switch=lambda: None,
        persist_lockdown_database=lambda **_kwargs: None,
        persist_lockdown_redis=lambda: None,
        run_kill_switch=lambda **kwargs: results.append(
            case.emergency_flatten.execute(**kwargs)
        )
        or results[-1],
        mark_kill_switch_halted=lambda: None,
        requires_authoritative_verification=lambda: True,
        kill_switch_result_is_complete=lambda result, **_kwargs: (
            result.get("authoritative_flatten_verified") is True
        ),
        mark_kill_switch_operation_completed=lambda **_kwargs: None,
        prepare_kill_switch_clear=MagicMock(),
        assert_leadership=lambda: None,
        clear_kill_switch=lambda **_kwargs: {},
        persist_clear_database=lambda **_kwargs: None,
        persist_clear_redis=lambda: None,
        clear_local_halt=lambda: None,
        finalize_external_drift_clear=lambda **_kwargs: None,
        event_logger=lambda: MagicMock(),
    )
    params: dict[str, str] = {"actor": "operator", "reason": "approved flat baseline"}
    if operation_id is not None:
        params["idempotency_key"] = operation_id
    command.handle_kill_switch(params)
    return results[0]


def test_rithmic_reconciler_scopes_only_local_positions_and_keeps_loader_failures(
    monkeypatch,
    request,
    sqlite_order_session_factory,
):
    case = _build_runtime_case(monkeypatch, request, sqlite_order_session_factory)

    def snapshot_loader(*_args, **_kwargs):
        return case.snapshot_holder["snapshot"]

    current = case.reconciliation.reconcile(snapshot_loader=snapshot_loader)
    verification = current["ledger_verification"]
    assert verification["errors"] == []
    assert verification["position_drifts"] == [
        {
            "product_id": case.product_id,
            "local_quantity": "2",
            "remote_quantity": "0",
        }
    ]
    assert _unrelated_position_state(case) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }

    context = case.reconciliation.context
    missing_loader = case.adapter.create_owned_order_reconciler(
        replace(context, local_positions_loader=None)
    )
    missing = missing_loader.reconcile(snapshot_loader=snapshot_loader)
    assert missing["ledger_verification"]["errors"] == [
        "local_positions_loader_missing"
    ]

    def failed_loader() -> list[object]:
        raise RuntimeError("local_positions_loader_failed")

    throwing_loader = case.adapter.create_owned_order_reconciler(
        replace(context, local_positions_loader=failed_loader)
    )
    failed = throwing_loader.reconcile(snapshot_loader=snapshot_loader)
    assert failed["ledger_verification"]["errors"] == ["position_verification_failed"]


@pytest.mark.parametrize("with_operation_id", [True, False])
def test_operator_flat_correction_commits_before_redis_and_restores_after_restart(
    monkeypatch,
    request,
    sqlite_order_session_factory,
    with_operation_id,
):
    endpoint = _redis_endpoint()
    product_id = f"RITHMIC:QT{uuid4().hex[:6].upper()}-202609"
    _prepare_runtime_sql_scope(sqlite_order_session_factory, (product_id,))
    strategy_id = f"baseline-{uuid4().hex}"
    history_order_id, history_trade_id = _seed_historical_order_and_trade(
        sqlite_order_session_factory,
        product_id,
    )
    original_history = _order_trade_rows(
        sqlite_order_session_factory,
        history_order_id,
        history_trade_id,
    )
    account_id = "TEST-ACCOUNT"
    profile = "test"
    snapshot = _snapshot(account_id)
    redis_client = Redis.from_url(endpoint, decode_responses=True)
    redis_client.ping()
    monkeypatch.setattr(
        "src.core.risk_manager.create_redis_client",
        lambda: Redis.from_url(endpoint, decode_responses=True),
    )
    account_service = AccountService()
    account_service.configure_authoritative_balance(
        venue="rithmic",
        account_id=account_id,
        max_age_seconds=600,
        runtime_environment=RuntimeEnvironment("test"),
    )
    redis_key = f"state:position:{strategy_id}:{product_id}"
    redis_client.hset(
        redis_key,
        mapping={"quantity": "2", "entry_price": "20000.25"},
    )
    unrelated_product_id = f"RITHMIC:UNRELATED{uuid4().hex[:6].upper()}-202612"
    unrelated_strategy_id = f"unrelated-{uuid4().hex}"
    unrelated_redis_key = (
        f"state:position:{unrelated_strategy_id}:{unrelated_product_id}"
    )
    redis_client.hset(
        unrelated_redis_key,
        mapping={"quantity": "0.5", "entry_price": "19000.75"},
    )
    request.addfinalizer(lambda: redis_client.delete(redis_key, unrelated_redis_key))
    account_service.get_all_positions()
    adapter = RithmicExchangeAdapter(
        profile=profile,
        account_id=account_id,
        instruments={
            product_id: {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        client_factory=MagicMock(),
    )
    execution_engine = MagicMock(spec=ExecutionEngine)
    execution_engine.clock = MagicMock()
    execution_engine.clock.now.return_value = 1_700_000_000.0
    execution_engine.audit_external_orders = True
    execution_engine._db_session_factory = sqlite_order_session_factory
    execution_engine.list_recoverable_client_orders.return_value = []
    reconciliation = adapter.create_owned_order_reconciler(
        OwnedOrderReconciliationContext(
            list_recoverable_client_orders=lambda: [],
            process_exchange_order_event=MagicMock(return_value={"action": "applied"}),
            now_seconds=lambda: 1_700_000_000,
            db_session_factory=sqlite_order_session_factory,
            local_positions_loader=cast(
                Callable[[], list[object]], account_service.get_all_positions
            ),
            logger=MagicMock(),
        )
    )

    reconciliation_results: list[dict[str, object]] = []

    def reconcile_owned_orders(*, snapshot_loader=None, startup_position_restorer=None):
        summary = reconciliation.reconcile(
            snapshot_loader=lambda *_args, **_kwargs: snapshot,
            startup_position_restorer=startup_position_restorer,
        )
        reconciliation_results.append(summary)
        return summary

    execution_engine.reconcile_owned_orders.side_effect = reconcile_owned_orders
    owners = build_rithmic_runtime_owners(
        adapter=adapter,
        profile=profile,
        account_id=account_id,
        execution_engine=execution_engine,
        account_service=account_service,
        ops_safety=MagicMock(spec=OpsSafetyService),
        stop_event=Event(),
        callbacks=_runtime_callbacks(),
        logger=MagicMock(),
    )
    emergency_flatten = owners.emergency_flatten
    assert emergency_flatten is not None
    emergency_flatten.stop_current_worker = lambda **_kwargs: True
    emergency_flatten.restart_generic_worker = lambda: None
    emergency_flatten.ops_safety.kill_switch_with_authoritative_positions = (
        lambda **_kwargs: {
            "cancelled_orders": 0,
            "cancel_failures": [],
            "flattened_positions": 0,
            "flatten_pending": [],
            "flatten_failures": [],
            "recovery_failures": [],
            "already_flat": True,
            "drain_timeout": False,
        }
    )

    original_replace = account_service.replace_positions_for_products
    ordering: list[str] = []
    unrelated_key = f"state:position:unrelated-{uuid4().hex}:BINANCE:BTCUSDT-PERP"
    request.addfinalizer(lambda: redis_client.delete(unrelated_key))

    def observe_durable_commit(positions, product_ids, *, timestamp_ms):
        with sqlite_order_session_factory() as session:
            count = session.scalar(
                select(func.count())
                .select_from(SystemEvent)
                .where(SystemEvent.event_subtype == "rithmic_operator_flat_baseline")
            )
        assert count == 1
        ordering.append("sql-committed")
        redis_client.hset(
            unrelated_key,
            mapping={"quantity": "0.25", "entry_price": "60000.50"},
        )
        result = original_replace(positions, product_ids, timestamp_ms=timestamp_ms)
        assert redis_client.hgetall(unrelated_key) == {
            "quantity": "0.25",
            "entry_price": "60000.50",
        }
        ordering.append("redis-projected")
        return result

    monkeypatch.setattr(
        account_service,
        "replace_positions_for_products",
        observe_durable_commit,
    )
    monkeypatch.setattr(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        lambda *_args, **_kwargs: snapshot,
    )
    operation_id = f"operation-{uuid4().hex}" if with_operation_id else None
    operation_results: list[dict[str, Any]] = []
    command = OpsCommandService(
        operation_lock=nullcontext,
        kill_switch_operation_completed=lambda **_kwargs: False,
        halt_for_kill_switch=lambda: None,
        persist_lockdown_database=lambda **_kwargs: None,
        persist_lockdown_redis=lambda: None,
        run_kill_switch=lambda **kwargs: operation_results.append(
            emergency_flatten.execute(**kwargs)
        )
        or operation_results[-1],
        mark_kill_switch_halted=lambda: None,
        requires_authoritative_verification=lambda: True,
        kill_switch_result_is_complete=lambda result, **_kwargs: (
            result.get("authoritative_flatten_verified") is True
        ),
        mark_kill_switch_operation_completed=lambda **_kwargs: None,
        prepare_kill_switch_clear=MagicMock(),
        assert_leadership=lambda: None,
        clear_kill_switch=lambda **_kwargs: {},
        persist_clear_database=lambda **_kwargs: None,
        persist_clear_redis=lambda: None,
        clear_local_halt=lambda: None,
        finalize_external_drift_clear=lambda **_kwargs: None,
        event_logger=lambda: MagicMock(),
    )
    command_params = {
        "actor": "operator",
        "reason": "approved flat correction",
    }
    if operation_id is not None:
        command_params["idempotency_key"] = operation_id
    command.handle_kill_switch(command_params)
    result = operation_results[0]

    if not with_operation_id:
        assert result["authoritative_flatten_verified"] is False
        assert result["position_correction"] == {
            "status": "inapplicable",
            "baseline_event_id": None,
            "reason": "no_baseline",
            "economics_unresolved": None,
        }
        assert ordering == []
        assert len(_positions_for_product(account_service, product_id)) == 1
        assert redis_client.hgetall(unrelated_redis_key) == {
            "quantity": "0.5",
            "entry_price": "19000.75",
        }
        with sqlite_order_session_factory() as session:
            assert (
                session.scalar(
                    select(func.count())
                    .select_from(SystemEvent)
                    .where(
                        SystemEvent.event_subtype == "rithmic_operator_flat_baseline"
                    )
                )
                == 0
            )
        account_service.close()
        redis_client.close()
        return

    assert result["authoritative_flatten_verified"] is True, (
        result.get("flatten_failures"),
        result.get("recovery_failures"),
        result.get("position_correction"),
        ordering,
        execution_engine.reconcile_owned_orders.call_count,
        [
            (
                summary.get("external_count"),
                summary.get("results"),
                summary.get("ledger_verification"),
            )
            for summary in reconciliation_results
        ],
    )
    assert result["position_correction"]["status"] == "applied"
    assert result["position_correction"]["economics_unresolved"] is True
    assert ordering == ["sql-committed", "redis-projected"]
    assert redis_client.hgetall(unrelated_key) == {
        "quantity": "0.25",
        "entry_price": "60000.50",
    }
    redis_client.delete(unrelated_key)
    assert _positions_for_product(account_service, product_id) == []
    assert redis_client.hgetall(unrelated_redis_key) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }
    with sqlite_order_session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 1
        assert session.scalar(select(func.count()).select_from(Trade)) == 1
    assert (
        _order_trade_rows(
            sqlite_order_session_factory,
            history_order_id,
            history_trade_id,
        )
        == original_history
    )

    first_event_id = result["position_correction"]["baseline_event_id"]
    redis_client.hset(
        redis_key,
        mapping={"quantity": "2", "entry_price": "20000.25"},
    )
    account_service.get_all_positions()
    command.handle_kill_switch(command_params)
    replayed = operation_results[-1]
    assert replayed["position_correction"]["baseline_event_id"] == first_event_id
    assert replayed["position_correction"]["status"] == "applied"
    assert _positions_for_product(account_service, product_id) == []
    assert redis_client.hgetall(unrelated_redis_key) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }
    assert ordering == [
        "sql-committed",
        "redis-projected",
        "sql-committed",
        "redis-projected",
    ]
    with sqlite_order_session_factory() as session:
        assert (
            session.scalar(
                select(func.count())
                .select_from(SystemEvent)
                .where(SystemEvent.event_subtype == "rithmic_operator_flat_baseline")
            )
            == 1
        )
    assert (
        _order_trade_rows(
            sqlite_order_session_factory,
            history_order_id,
            history_trade_id,
        )
        == original_history
    )
    redis_client.delete(unrelated_key)

    baseline = read_latest_rithmic_operator_flat_baseline(
        sqlite_order_session_factory,
        profile=profile,
        account_id=account_id,
        product_ids=adapter.configured_product_ids,
    )
    assert baseline.record is not None
    assert baseline.record.payload["economics_unresolved"] is True

    redis_client.hset(
        redis_key,
        mapping={"quantity": "2", "entry_price": "20000.25"},
    )
    account_service.close()
    restarted_account_service = AccountService()
    restarted_account_service.configure_authoritative_balance(
        venue="rithmic",
        account_id=account_id,
        max_age_seconds=600,
        runtime_environment=RuntimeEnvironment("test"),
    )
    restarted_engine = MagicMock(spec=ExecutionEngine)
    restarted_engine.clock = MagicMock()
    restarted_engine.clock.now.return_value = 1_700_000_100.0
    restarted_engine.audit_external_orders = True
    restarted_engine._db_session_factory = sqlite_order_session_factory
    restarted_reconciler = adapter.create_owned_order_reconciler(
        OwnedOrderReconciliationContext(
            list_recoverable_client_orders=lambda: [],
            process_exchange_order_event=MagicMock(return_value={"action": "applied"}),
            now_seconds=lambda: 1_700_000_100,
            db_session_factory=sqlite_order_session_factory,
            local_positions_loader=cast(
                Callable[[], list[object]],
                restarted_account_service.get_all_positions,
            ),
            logger=MagicMock(),
        )
    )
    restarted_engine.reconcile_owned_orders.side_effect = (
        lambda **kwargs: restarted_reconciler.reconcile(
            snapshot_loader=lambda *_args, **_kwargs: snapshot,
            startup_position_restorer=kwargs.get("startup_position_restorer"),
        )
    )
    restarted_ops_safety = OpsSafetyService(
        execution_engine=restarted_engine,
        account_service=restarted_account_service,
        db_session_factory=sqlite_order_session_factory,
    )
    restarted_ops_safety.persist_kill_switch_state(
        "LOCKDOWN",
        actor="operator",
        reason="preserve operator stop",
        operation_id=f"stop-{uuid4().hex}",
    )
    system_state_key = f"test:system-state:{uuid4().hex}"
    system_boot_key = f"test:engine-boot-state:{uuid4().hex}"
    redis_client.set(system_state_key, "LOCKDOWN")
    request.addfinalizer(lambda: redis_client.delete(system_state_key, system_boot_key))
    restarted_owners = build_rithmic_runtime_owners(
        adapter=adapter,
        profile=profile,
        account_id=account_id,
        execution_engine=restarted_engine,
        account_service=restarted_account_service,
        ops_safety=restarted_ops_safety,
        stop_event=Event(),
        callbacks=_runtime_callbacks(),
        logger=MagicMock(),
    )
    restarted_ledger_recovery = restarted_owners.ledger_recovery
    assert restarted_ledger_recovery is not None
    restarted_ledger_recovery._maintenance_active = lambda: False
    restored = restarted_ledger_recovery.reconcile_startup()

    assert restored["position_correction"]["status"] == "applied"
    assert (
        restored["position_correction"]["reason"] == "restored_operator_flat_baseline"
    )
    assert restored["auto_resume_safe"] is True
    assert _positions_for_product(restarted_account_service, product_id) == []
    assert redis_client.hgetall(unrelated_redis_key) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }
    boot = EngineBootStateService(
        ops_safety=restarted_ops_safety,
        redis_client=redis_client,
        system_state_key=system_state_key,
        system_boot_state_key=system_boot_key,
        boot_id=f"boot-{uuid4().hex}",
        logger=MagicMock(),
    ).assess_startup()
    assert boot.locked is True
    assert boot.lock_cause == "explicit_lockdown"
    assert boot.auto_recovery_allowed is False
    assert restarted_ops_safety.latest_kill_switch_state() == "LOCKDOWN"
    assert redis_client.get(system_state_key) == "LOCKDOWN"
    with sqlite_order_session_factory() as session:
        assert session.scalar(select(func.count()).select_from(Order)) == 1
        assert session.scalar(select(func.count()).select_from(Trade)) == 1
    assert (
        _order_trade_rows(
            sqlite_order_session_factory,
            history_order_id,
            history_trade_id,
        )
        == original_history
    )
    restarted_account_service.close()
    redis_client.delete(redis_key, unrelated_redis_key)
    redis_client.close()


def test_startup_restore_requires_matching_authoritative_account_scope(
    monkeypatch,
    request,
    sqlite_order_session_factory,
):
    endpoint = _redis_endpoint()
    _prepare_event_table(sqlite_order_session_factory)
    product_id = f"RITHMIC:QT{uuid4().hex[:6].upper()}-202609"
    account_id = "TEST-ACCOUNT"
    profile = "test"
    redis_client = Redis.from_url(endpoint, decode_responses=True)
    redis_client.ping()
    monkeypatch.setattr(
        "src.core.risk_manager.create_redis_client",
        lambda: Redis.from_url(endpoint, decode_responses=True),
    )
    account_service = AccountService()
    redis_key = f"state:position:LIVE:{product_id}"
    redis_client.hset(
        redis_key,
        mapping={"quantity": "1", "entry_price": "20000.25"},
    )
    request.addfinalizer(lambda: redis_client.delete(redis_key))
    adapter = RithmicExchangeAdapter(
        profile=profile,
        account_id=account_id,
        instruments={
            product_id: {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        client_factory=MagicMock(),
    )
    execution_engine = MagicMock(spec=ExecutionEngine)
    execution_engine.clock = MagicMock()
    execution_engine.clock.now.return_value = 1_700_000_000.0
    execution_engine.audit_external_orders = True
    execution_engine._db_session_factory = sqlite_order_session_factory
    snapshot = _snapshot(account_id)
    owners = build_rithmic_runtime_owners(
        adapter=adapter,
        profile=profile,
        account_id=account_id,
        execution_engine=execution_engine,
        account_service=account_service,
        ops_safety=MagicMock(spec=OpsSafetyService),
        stop_event=Event(),
        callbacks=_runtime_callbacks(),
        logger=MagicMock(),
    )
    # The baseline writer is intentionally absent; wrong account scope cannot clear Redis.
    reconciler = adapter.create_owned_order_reconciler(
        OwnedOrderReconciliationContext(
            list_recoverable_client_orders=lambda: [],
            process_exchange_order_event=MagicMock(return_value={"action": "applied"}),
            now_seconds=lambda: 1_700_000_000,
            db_session_factory=sqlite_order_session_factory,
            local_positions_loader=cast(
                Callable[[], list[object]], account_service.get_all_positions
            ),
            logger=MagicMock(),
        )
    )
    execution_engine.reconcile_owned_orders.side_effect = lambda **kwargs: (
        reconciler.reconcile(
            snapshot_loader=lambda *_args, **_kw: snapshot,
            startup_position_restorer=kwargs.get("startup_position_restorer"),
        )
    )
    ledger_recovery = owners.ledger_recovery
    assert ledger_recovery is not None
    ledger_recovery._maintenance_active = lambda: False

    result = ledger_recovery.reconcile_startup()

    assert result["auto_resume_safe"] is False
    assert result["position_correction"]["status"] == "inapplicable"
    assert result["position_correction"]["reason"] == "remote_state_inapplicable"
    assert _positions_for_product(account_service, product_id)[0].quantity == Decimal(
        "1"
    )
    account_service.close()
    redis_client.delete(redis_key)
    redis_client.close()


def test_sql_baseline_failure_prevents_real_redis_projection(
    monkeypatch,
    request,
    sqlite_order_session_factory,
):
    case = _build_runtime_case(monkeypatch, request, sqlite_order_session_factory)
    original_commit = Session.commit

    def fail_baseline_commit(session):
        pending_baseline = any(
            isinstance(event, SystemEvent)
            and event.event_subtype == "rithmic_operator_flat_baseline"
            for event in [*session.new, *session.identity_map.values()]
        )
        if pending_baseline:
            raise RuntimeError("injected_baseline_sql_commit_failure")
        return original_commit(session)

    monkeypatch.setattr(Session, "commit", fail_baseline_commit)
    with pytest.raises(RuntimeError, match="injected_baseline_sql_commit_failure"):
        _dispatch_operator_flatten(case, operation_id=f"sql-{uuid4().hex}")

    assert _baseline_count(sqlite_order_session_factory) == 0
    assert case.redis_client.hgetall(case.redis_key) == {
        "quantity": "2",
        "entry_price": "20000.25",
    }
    assert _unrelated_position_state(case) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }


def test_committed_baseline_survives_redis_projection_failure_and_restores(
    monkeypatch,
    request,
    sqlite_order_session_factory,
):
    case = _build_runtime_case(monkeypatch, request, sqlite_order_session_factory)
    redis_instance = case.account_service.redis
    assert redis_instance is not None
    original_pipeline = redis_instance.pipeline

    def fail_projection_pipeline(*args, **kwargs):
        pipeline = original_pipeline(*args, **kwargs)

        def fail_before_execute(*_args, **_kwargs):
            raise RuntimeError("injected_redis_projection_failure")

        pipeline.execute = fail_before_execute
        return pipeline

    with monkeypatch.context() as patcher:
        patcher.setattr(redis_instance, "pipeline", fail_projection_pipeline)
        with pytest.raises(RuntimeError, match="injected_redis_projection_failure"):
            _dispatch_operator_flatten(case, operation_id=f"redis-{uuid4().hex}")

    assert _baseline_count(sqlite_order_session_factory) == 1
    assert case.redis_client.hgetall(case.redis_key) == {
        "quantity": "2",
        "entry_price": "20000.25",
    }
    assert _unrelated_position_state(case) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }
    restart = _build_runtime_case(
        monkeypatch,
        request,
        sqlite_order_session_factory,
        product_id=case.product_id,
        strategy_id=case.strategy_id,
    )
    ledger_recovery = restart.owners.ledger_recovery
    assert ledger_recovery is not None
    ledger_recovery._maintenance_active = lambda: False
    restored = ledger_recovery.reconcile_startup()

    assert restored["position_correction"]["status"] == "applied"
    assert restored["position_correction"]["reason"] == (
        "restored_operator_flat_baseline"
    )
    assert _positions_for_product(restart.account_service, case.product_id) == []
    assert _unrelated_position_state(restart) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }
    assert _baseline_count(sqlite_order_session_factory) == 1


@pytest.mark.parametrize("background", [False, True])
def test_background_emergency_projection_does_not_create_operator_baseline(
    monkeypatch,
    request,
    sqlite_order_session_factory,
    background,
):
    case = _build_runtime_case(monkeypatch, request, sqlite_order_session_factory)
    if background:
        result = case.emergency_flatten.execute(
            actor="engine",
            reason="ordinary background mitigation",
            operation_id=f"background-{uuid4().hex}",
        )
    else:
        result = _dispatch_operator_flatten(
            case,
            operation_id=f"operator-{uuid4().hex}",
        )

    assert _baseline_count(sqlite_order_session_factory) == (0 if background else 1)
    if background:
        assert "position_correction" not in result
    else:
        assert result["position_correction"]["status"] == "applied"
    assert _positions_for_product(case.account_service, case.product_id) == []
    assert _unrelated_position_state(case) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }


@pytest.mark.parametrize(
    ("remote_case", "expected_projection"),
    [
        ("working_order", False),
        ("unknown_order", False),
        ("nonzero_remote", True),
        ("wrong_account", False),
        ("wrong_product", False),
        ("provider_unavailable", False),
    ],
)
def test_ineligible_remote_state_never_records_or_projects_an_invalid_baseline(
    monkeypatch,
    request,
    sqlite_order_session_factory,
    remote_case,
    expected_projection,
):
    from src.core.interfaces.exchange import ExchangeError

    case = _build_runtime_case(monkeypatch, request, sqlite_order_session_factory)
    snapshot = case.snapshot_holder["snapshot"]
    symbol = to_rithmic_symbol(case.product_id)
    if remote_case in {"working_order", "unknown_order"}:
        status = "WORKING" if remote_case == "working_order" else "UNRECOGNIZED"
        snapshot.orders = (
            SimpleNamespace(
                exchange="CME",
                symbol=symbol,
                status=status,
                basket_id=f"remote-{uuid4().hex}",
                client_order_id=f"remote-client-{uuid4().hex}",
                original_basket_id=None,
                notification_type="",
                filled_quantity=None,
                quantity=Decimal("1"),
                unfilled_quantity=Decimal("1"),
                price_type="LIMIT",
            ),
        )
    elif remote_case == "nonzero_remote":
        snapshot.positions = (
            SimpleNamespace(
                exchange="CME",
                symbol=symbol,
                net_quantity=Decimal("1.000"),
                average_open_fill_price=Decimal("20000.25"),
                open_pnl=Decimal("0"),
            ),
        )
    elif remote_case == "wrong_account":
        snapshot.account_id = "OTHER-ACCOUNT"
    elif remote_case == "wrong_product":
        snapshot.positions = (
            SimpleNamespace(
                exchange="CME",
                symbol="UNKNOWN-CONTRACT",
                net_quantity=Decimal("1"),
                average_open_fill_price=Decimal("20000.25"),
                open_pnl=Decimal("0"),
            ),
        )
    else:
        case.snapshot_holder["error"] = RuntimeError("provider_snapshot_unavailable")

    original_projection = case.account_service.replace_positions_for_products
    projections: list[tuple[list[object], tuple[str, ...]]] = []

    def observe_projection(positions, product_ids, *, timestamp_ms):
        projections.append((list(positions), tuple(product_ids)))
        return original_projection(positions, product_ids, timestamp_ms=timestamp_ms)

    monkeypatch.setattr(
        case.account_service,
        "replace_positions_for_products",
        observe_projection,
    )
    if remote_case in {"wrong_account", "wrong_product"}:
        with pytest.raises(ExchangeError):
            _dispatch_operator_flatten(case, operation_id=f"negative-{uuid4().hex}")
    elif remote_case == "provider_unavailable":
        with pytest.raises(RuntimeError, match="provider_snapshot_unavailable"):
            _dispatch_operator_flatten(case, operation_id=f"negative-{uuid4().hex}")
    else:
        result = _dispatch_operator_flatten(
            case,
            operation_id=f"negative-{uuid4().hex}",
        )
        assert result["authoritative_flatten_verified"] is False

    assert _baseline_count(sqlite_order_session_factory) == 0
    assert bool(projections) is expected_projection
    if remote_case == "nonzero_remote":
        projected = _positions_for_product(case.account_service, case.product_id)
        assert len(projected) == 1
        assert projected[0].quantity == Decimal("1.000")
    elif not expected_projection:
        assert case.redis_client.hgetall(case.redis_key) == {
            "quantity": "2",
            "entry_price": "20000.25",
        }
    assert _unrelated_position_state(case) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }


@pytest.mark.parametrize("new_activity_scope", ["scoped", "unrelated"])
def test_startup_restore_rechecks_cut_and_ignores_unrelated_activity(
    monkeypatch,
    request,
    sqlite_order_session_factory,
    new_activity_scope,
):
    case = _build_runtime_case(monkeypatch, request, sqlite_order_session_factory)
    operation_id = f"cut-{uuid4().hex}"
    created = _dispatch_operator_flatten(case, operation_id=operation_id)
    assert created["position_correction"]["status"] == "applied"
    case.redis_client.hset(
        case.redis_key,
        mapping={"quantity": "2", "entry_price": "20000.25"},
    )

    activity_product = (
        case.product_id if new_activity_scope == "scoped" else "RITHMIC:ESZ6-202612"
    )
    _insert_scoped_order(
        sqlite_order_session_factory,
        product_id=activity_product,
        account_id="TEST-ACCOUNT",
        profile="test",
    )
    restart = _build_runtime_case(
        monkeypatch,
        request,
        sqlite_order_session_factory,
        product_id=case.product_id,
        strategy_id=case.strategy_id,
    )
    ledger_recovery = restart.owners.ledger_recovery
    assert ledger_recovery is not None
    ledger_recovery._maintenance_active = lambda: False
    restored = ledger_recovery.reconcile_startup()

    if new_activity_scope == "scoped":
        assert restored["position_correction"]["status"] == "inapplicable"
        assert restored["position_correction"]["reason"] == "durable_cut_changed"
        assert _positions_for_product(restart.account_service, case.product_id)[
            0
        ].quantity == Decimal("2")
    else:
        assert restored["position_correction"]["status"] == "applied"
        assert _positions_for_product(restart.account_service, case.product_id) == []
    assert _unrelated_position_state(restart) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }
    assert _baseline_count(sqlite_order_session_factory) == 1


def test_existing_baseline_is_not_applied_under_wrong_authoritative_account_scope(
    monkeypatch,
    request,
    sqlite_order_session_factory,
):
    case = _build_runtime_case(monkeypatch, request, sqlite_order_session_factory)
    operation_id = f"account-scope-{uuid4().hex}"
    created = _dispatch_operator_flatten(case, operation_id=operation_id)
    assert created["position_correction"]["status"] == "applied"
    case.redis_client.hset(
        case.redis_key,
        mapping={"quantity": "2", "entry_price": "20000.25"},
    )

    restart = _build_runtime_case(
        monkeypatch,
        request,
        sqlite_order_session_factory,
        balance_account_id="OTHER-ACCOUNT",
        product_id=case.product_id,
        strategy_id=case.strategy_id,
    )
    ledger_recovery = restart.owners.ledger_recovery
    assert ledger_recovery is not None
    ledger_recovery._maintenance_active = lambda: False
    restored = ledger_recovery.reconcile_startup()

    assert restored["position_correction"]["status"] == "inapplicable"
    assert restored["position_correction"]["reason"] == "remote_state_inapplicable"
    assert _positions_for_product(restart.account_service, case.product_id)[
        0
    ].quantity == Decimal("2")
    assert _unrelated_position_state(restart) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }
    assert _baseline_count(sqlite_order_session_factory) == 1


def _set_unsafe_startup_snapshot(case, scenario: str) -> None:
    snapshot = case.snapshot_holder["snapshot"]
    symbol = to_rithmic_symbol(case.product_id)
    if scenario == "nonzero_remote":
        snapshot.positions = (
            SimpleNamespace(
                exchange="CME",
                symbol=symbol,
                net_quantity=Decimal("1"),
                average_open_fill_price=Decimal("20000.25"),
                open_pnl=Decimal("0"),
            ),
        )
    elif scenario == "unknown_remote_product":
        snapshot.positions = (
            SimpleNamespace(
                exchange="CME",
                symbol="UNKNOWN-CONTRACT",
                net_quantity=Decimal("1"),
                average_open_fill_price=Decimal("20000.25"),
                open_pnl=Decimal("0"),
            ),
        )
    elif scenario in {"working_order", "unresolved_order"}:
        snapshot.orders = (
            SimpleNamespace(
                exchange="CME",
                symbol=symbol,
                status=("WORKING" if scenario == "working_order" else "UNRECOGNIZED"),
                basket_id=f"startup-{uuid4().hex}",
                client_order_id=f"startup-client-{uuid4().hex}",
                original_basket_id=None,
                notification_type="",
                filled_quantity=None,
                quantity=Decimal("1"),
                unfilled_quantity=Decimal("1"),
                price_type="LIMIT",
            ),
        )
    elif scenario == "provider_unavailable":
        case.snapshot_holder["error"] = RuntimeError("provider_snapshot_unavailable")
    else:
        raise AssertionError(f"unsupported startup scenario: {scenario}")


@pytest.mark.parametrize(
    "scenario",
    [
        "nonzero_remote",
        "unknown_remote_product",
        "working_order",
        "unresolved_order",
        "provider_unavailable",
    ],
)
def test_startup_does_not_apply_valid_baseline_when_fresh_remote_state_is_unsafe(
    monkeypatch,
    request,
    sqlite_order_session_factory,
    scenario,
):
    case = _build_runtime_case(monkeypatch, request, sqlite_order_session_factory)
    created = _dispatch_operator_flatten(case, operation_id=f"startup-{uuid4().hex}")
    assert created["position_correction"]["status"] == "applied"
    case.redis_client.hset(
        case.redis_key,
        mapping={"quantity": "2", "entry_price": "20000.25"},
    )
    _set_unsafe_startup_snapshot(case, scenario)

    operator_safety = OpsSafetyService(
        execution_engine=case.execution_engine,
        account_service=case.account_service,
        db_session_factory=sqlite_order_session_factory,
    )
    operator_safety.persist_kill_switch_state(
        "LOCKDOWN",
        actor="operator",
        reason="preserve stop through unsafe restart",
        operation_id=f"stop-{uuid4().hex}",
    )
    ledger_recovery = case.owners.ledger_recovery
    assert ledger_recovery is not None
    ledger_recovery._maintenance_active = lambda: False
    result = ledger_recovery.reconcile_startup()

    assert result["auto_resume_safe"] is False
    assert _baseline_count(sqlite_order_session_factory) == 1
    assert case.redis_client.hgetall(case.redis_key) == {
        "quantity": "2",
        "entry_price": "20000.25",
    }
    assert _unrelated_position_state(case) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }
    assert operator_safety.latest_kill_switch_state() == "LOCKDOWN"
    if "position_correction" in result:
        assert result["position_correction"]["status"] == "inapplicable"
    if scenario == "provider_unavailable":
        assert result.get("snapshot_error_code") == (
            "unclassified_ledger_snapshot_failure"
        )


@pytest.mark.parametrize("failure", ["sql_reader", "redis_projection"])
def test_startup_baseline_failure_preserves_existing_stop_and_redis_exposure(
    monkeypatch,
    request,
    sqlite_order_session_factory,
    failure,
):
    case = _build_runtime_case(monkeypatch, request, sqlite_order_session_factory)
    created = _dispatch_operator_flatten(case, operation_id=f"failure-{uuid4().hex}")
    assert created["position_correction"]["status"] == "applied"
    case.redis_client.hset(
        case.redis_key,
        mapping={"quantity": "2", "entry_price": "20000.25"},
    )
    operator_safety = OpsSafetyService(
        execution_engine=case.execution_engine,
        account_service=case.account_service,
        db_session_factory=sqlite_order_session_factory,
    )
    operator_safety.persist_kill_switch_state(
        "LOCKDOWN",
        actor="operator",
        reason="preserve stop through restore failure",
        operation_id=f"stop-{uuid4().hex}",
    )

    if failure == "sql_reader":
        monkeypatch.setattr(
            "src.core.adapters.rithmic_runtime_composition.read_latest_rithmic_operator_flat_baseline",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("baseline_sql_reader_unavailable")
            ),
        )
    else:
        redis_instance = case.account_service.redis
        assert redis_instance is not None
        original_pipeline = redis_instance.pipeline

        def fail_projection_pipeline(*args, **kwargs):
            pipeline = original_pipeline(*args, **kwargs)

            def fail_before_execute(*_args, **_kwargs):
                raise RuntimeError("startup_redis_projection_unavailable")

            pipeline.execute = fail_before_execute
            return pipeline

        monkeypatch.setattr(redis_instance, "pipeline", fail_projection_pipeline)

    ledger_recovery = case.owners.ledger_recovery
    assert ledger_recovery is not None
    ledger_recovery._maintenance_active = lambda: False
    result = ledger_recovery.reconcile_startup()

    assert result["auto_resume_safe"] is False
    assert "position_correction" not in result
    assert _baseline_count(sqlite_order_session_factory) == 1
    assert case.redis_client.hgetall(case.redis_key) == {
        "quantity": "2",
        "entry_price": "20000.25",
    }
    assert _unrelated_position_state(case) == {
        "quantity": "0.5",
        "entry_price": "19000.75",
    }
    assert operator_safety.latest_kill_switch_state() == "LOCKDOWN"
