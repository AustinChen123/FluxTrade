"""Actual Execution-to-repository TX A receipts on isolated PostgreSQL."""

from decimal import Decimal
import os
from typing import Any, cast

import pytest
from sqlalchemy import Engine, delete, func, select, update
from sqlalchemy.orm import Session, sessionmaker

from src.control_plane.spider_operation_http import SPIDER_OPERATION_REQUEST_PATH
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from src.core.clock import BacktestClock
from src.core.command_router import CommandResult
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.orm_models import Order, StrategyState, StrategyStateTransition, Trade
from src.core.order_manager import OrderManager
from src.core.repositories import LiveOrderRepository
from src.core.runtime_capabilities import OrderAccountIdentity
from src.core.spider_management.operation_request import SpiderOperationRequest
from src.core.spider_management.runtime_owner import (
    ModelInstallSource,
    RuntimeScopeKey,
    SpiderRuntimeComposition,
    SpiderRuntimeOwner,
)
from src.core.spider_management.orm import (
    SpiderControlReceipt,
    SpiderRuntimeDelivery,
    SpiderRuntimeOperation,
    SpiderRuntimeScope,
)
from test_migrations import fresh_pg_db as _migration_fresh_pg_db
from test_spider_operation_request_pg import (
    _operation_http_app,
    operation_pg as _operation_pg,
)
from test_spider_runtime_owner_pg import _install_evidence, _owned_dispatch

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


def _install_owner(engine: Engine):
    run, specs, confirmation, prepared, operation_body = _install_evidence(engine)
    app, _service, headers = _operation_http_app(sessionmaker(engine))
    proposal = SpiderOperationRequest.from_canonical_bytes(canonical_bytes(operation_body))
    proposal_response = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH, proposal.canonical_request_bytes,
        headers=headers,
    )
    assert proposal_response.status_code == 200
    confirmation_body = cast(
        dict[str, Any], decode_canonical(proposal.canonical_request_bytes)
    )
    confirmation_body["request_kind"] = "CONFIRMATION"
    confirmation_body["idempotency_key"] = "start-confirmation"
    confirmation_body["proposal_request_id"] = proposal.request_id
    confirmed = SpiderOperationRequest.from_canonical_bytes(
        canonical_bytes(confirmation_body)
    )
    response = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH, confirmed.canonical_request_bytes,
        headers=headers,
    )
    assert response.status_code == 200
    from src.core.spider_management.operation_request_store import (
        SpiderOperationRequestStore,
    )

    assert SpiderOperationRequestStore(sessionmaker(engine)).readback(confirmed).record

    repository = LiveOrderRepository(
        db_session_factory=sessionmaker(engine),
        account_profile="okx",
        account_id="account-main",
    )
    order_manager = OrderManager(
        repository,
        BacktestClock(1_800_000_000),
        is_backtest=True,
        order_account_identity_resolver=lambda product_id, *, is_backtest: OrderAccountIdentity(
            "okx", "account-main"
        ),
    )
    scope = RuntimeScopeKey("live", "account-main", "spider-main")
    owner = SpiderRuntimeOwner(
        sessionmaker(engine),
        order_manager,
        owned_scopes=(scope,),
        model_sources_by_confirmation={
            confirmation.request_id: ModelInstallSource(run, specs, "okx")
        },
    )
    result: list[CommandResult | None] = []
    _owned_dispatch(owner, result).dispatch({
        "command": "START",
        "strategy_id": scope.strategy_id,
        "params": {
            "strategy_id": scope.strategy_id,
            "environment": scope.environment,
            "execution_scope_id": scope.execution_scope_id,
            "actor": "api_key",
            "idempotency_key": "start-confirmation",
            "expected_version": 7,
        },
    })
    assert result and result[0] is not None and result[0].success
    readback = owner.readback(scope, confirmed.operation_id)
    assert readback is not None
    return (
        owner,
        scope,
        confirmed.operation_id,
        order_manager,
        prepared,
        readback,
        confirmed,
        ModelInstallSource(run, specs, "okx"),
    )


def test_execution_ack_fills_duplicate_and_cancel_share_actual_receipt_chain(
    operation_pg, mock_clock, mock_exchange_adapter
):
    engine = operation_pg
    (
        owner,
        scope,
        operation_id,
        order_manager,
        prepared,
        initial,
        _confirmation_record,
        _model_source,
    ) = _install_owner(engine)
    session_factory = sessionmaker(engine)
    execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        db_session_factory=session_factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    with session_factory() as session:
        order_ids = list(session.scalars(
            select(Order.id).where(Order.strategy_id == scope.strategy_id).order_by(Order.timestamp, Order.id)
        ))
    assert len(order_ids) >= 2
    first_order = order_manager.repo.get_order(order_ids[0])
    cancel_order = order_manager.repo.get_order(order_ids[1])
    assert first_order is not None and cancel_order is not None
    first_quantity = first_order.quantity
    first_price = cast(Decimal, first_order.price)
    partial_quantity = first_quantity / Decimal(2)
    before_ack = ExchangeOrderEvent(
        status="partially_filled",
        product_id=first_order.product_id,
        client_order_id=first_order.client_order_id,
        cumulative_filled_quantity=partial_quantity,
        cumulative_average_price=first_price,
        last_fill_quantity=partial_quantity,
        last_fill_price=first_price,
        fee=Decimal("0.01"),
        fee_asset="USDT",
        last_fill_exchange_trade_id="fixture-trade-1",
    )
    assert execution.process_exchange_order_event(before_ack)["action"] == "applied"
    partial_readback = owner.readback(scope, operation_id)
    assert partial_readback is not None
    assert partial_readback["trading_ready"] is False

    # This input is already normalized at the ExchangeOrderEvent boundary; it
    # proves the financial consumer path, not OKX provider-wire translation.
    execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=first_order.product_id,
        client_order_id=first_order.client_order_id,
        exchange_order_id="fixture-provider-order-1",
    ))
    rest_quantity = first_quantity - partial_quantity
    full = ExchangeOrderEvent(
        status="filled",
        product_id=first_order.product_id,
        client_order_id=first_order.client_order_id,
        exchange_order_id="fixture-provider-order-1",
        cumulative_filled_quantity=first_quantity,
        cumulative_average_price=first_price,
        last_fill_quantity=rest_quantity,
        last_fill_price=first_price,
        fee=Decimal("0.02"),
        fee_asset="USDT",
        last_fill_exchange_trade_id="fixture-trade-2",
    )
    assert execution.process_exchange_order_event(full)["action"] == "applied"
    with session_factory() as session:
        trade_count_before_duplicate = session.scalar(
            select(func.count()).select_from(Trade).where(Trade.order_id == first_order.id)
        )
        receipt_count_before_duplicate = session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        )
    execution.process_exchange_order_event(full)
    with session_factory() as session:
        trade_count_after_duplicate = session.scalar(
            select(func.count()).select_from(Trade).where(Trade.order_id == first_order.id)
        )
        receipt_count_after_duplicate = session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        )
        receipt_rows = list(session.scalars(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            ).order_by(SpiderRuntimeDelivery.sequence)
        ))
        saved_trades = list(session.scalars(
            select(Trade).where(Trade.order_id == first_order.id)
        ))
    assert trade_count_after_duplicate == trade_count_before_duplicate
    assert receipt_count_after_duplicate == receipt_count_before_duplicate
    assert {row.kind for row in receipt_rows} >= {"FILL", "ORDER_STATE"}
    assert all(row.disposition == "PENDING" for row in receipt_rows)
    saved_trades_by_exchange_id = {
        trade.exchange_trade_id: trade for trade in saved_trades
    }
    first_trade = saved_trades_by_exchange_id["fixture-trade-1"]
    second_trade = saved_trades_by_exchange_id["fixture-trade-2"]
    assert (first_trade.quantity, first_trade.price, first_trade.fee) == (
        partial_quantity,
        first_price,
        Decimal("0.01"),
    )
    assert (second_trade.quantity, second_trade.price, second_trade.fee) == (
        rest_quantity,
        first_price,
        Decimal("0.02"),
    )
    assert {row.trade_id for row in receipt_rows if row.kind == "FILL"} == {
        first_trade.id,
        second_trade.id,
    }
    state_ordinals: dict[str, list[int]] = {}
    for row in receipt_rows:
        if row.kind == "ORDER_STATE":
            payload = cast(dict[str, Any], decode_canonical(bytes(row.source_payload)))
            state_ordinals.setdefault(row.local_order_id, []).append(
                cast(int, payload["transition_ordinal"])
            )
    assert state_ordinals[first_order.id] == [1]
    assert state_ordinals.get(cancel_order.id, []) == []
    first_order_state_sequence = next(
        row.sequence for row in receipt_rows if row.local_order_id == first_order.id
        and row.kind == "ORDER_STATE"
    )

    participant_repository = LiveOrderRepository(
        db_session_factory=session_factory,
        account_profile="okx",
        account_id="account-main",
        transaction_participant=owner.participate_in_order_transaction,
    )
    replay_order = participant_repository.get_order(first_order.id)
    assert replay_order is not None
    with session_factory() as session:
        original_trade = session.scalars(
            select(Trade).where(Trade.exchange_trade_id == "fixture-trade-2")
        ).one()
        original_trade_id = original_trade.id
        original_trade_values = {
            name: getattr(original_trade, name)
            for name in (
                "order_id", "exchange_trade_id", "product_id", "side", "price",
                "quantity", "fee", "fee_asset", "timestamp",
            )
        }
    participant_repository.persist_fill(replay_order, original_trade)
    with session_factory() as session:
        assert session.get(Trade, original_trade_id) is not None
        assert session.scalar(
            select(func.count()).select_from(Trade).where(Trade.order_id == first_order.id)
        ) == trade_count_after_duplicate
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        ) == receipt_count_after_duplicate

    conflict_trade = Trade(
        id=original_trade_id,
        **{
            **original_trade_values,
            "fee": cast(Decimal, original_trade_values["fee"]) + Decimal("0.0001"),
        },
    )
    conflict_order = participant_repository.get_order(first_order.id)
    assert conflict_order is not None
    with pytest.raises(RuntimeError, match="spider_financial_trade_identity_conflict"):
        participant_repository.persist_fill(conflict_order, conflict_trade)
    with session_factory() as session:
        saved_trade = session.get(Trade, original_trade_id)
        assert saved_trade is not None
        assert saved_trade.fee == original_trade_values["fee"]
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        ) == receipt_count_after_duplicate

    scale_order = participant_repository.get_order(cancel_order.id)
    assert scale_order is not None
    scale_order.filled_quantity = Decimal("0.000")
    scale_order.filled_price = Decimal("0.000")
    with session_factory() as session:
        sequence_before_scale = session.scalar(
            select(SpiderRuntimeScope.last_received_sequence).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        )
        count_before_scale = session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        )
    participant_repository.update_order(scale_order)
    with session_factory() as session:
        assert session.scalar(
            select(SpiderRuntimeScope.last_received_sequence).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ) == sequence_before_scale
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        ) == count_before_scale
    assert state_ordinals[first_order.id] == [1]

    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="canceled",
        product_id=cancel_order.product_id,
        client_order_id=cancel_order.client_order_id,
    ))["action"] == "applied"
    with session_factory() as session:
        cancel_receipt = session.scalars(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.local_order_id == cancel_order.id,
                SpiderRuntimeDelivery.kind == "ORDER_STATE",
            )
        ).one()
    cancel_payload = cast(
        dict[str, Any], decode_canonical(bytes(cancel_receipt.source_payload))
    )
    assert cancel_payload["transition_ordinal"] == 1
    assert cancel_receipt.sequence > first_order_state_sequence
    assert cancel_receipt.sequence > cancel_payload["transition_ordinal"]
    final_readback = owner.readback(scope, operation_id)
    assert final_readback is not None
    assert final_readback["checkpoint_sha256"] == initial["checkpoint_sha256"]
    assert final_readback["initial_plan_missing_members"] == initial[
        "initial_plan_missing_members"
    ]
    assert final_readback["trading_ready"] is False
    assert owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert prepared.build_checkpoint_sha256 == initial["checkpoint_sha256"]


@pytest.mark.parametrize(
    "mutation",
    [
        "status",
        "filled_quantity",
        "filled_price",
        "provider_order_id",
        "side",
        "price",
        "quantity",
        "client_order_id",
        "intent_payload",
    ],
)
def test_saved_trade_replay_rejects_divergent_order_atomically(
    operation_pg, mock_clock, mock_exchange_adapter, mutation
):
    engine = operation_pg
    (
        owner,
        scope,
        operation_id,
        order_manager,
        _prepared,
        _initial,
        _confirmation_record,
        _model_source,
    ) = _install_owner(engine)
    factory = sessionmaker(engine)
    execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        db_session_factory=factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    with factory() as session:
        order_id = session.scalar(
            select(Order.id)
            .where(Order.strategy_id == scope.strategy_id)
            .order_by(Order.timestamp, Order.id)
            .limit(1)
        )
    assert order_id is not None
    order = order_manager.repo.get_order(order_id)
    assert order is not None
    provider_id = "saved-trade-replay-provider"
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id=provider_id,
    ))["action"] == "applied"
    quantity = order.quantity
    price = cast(Decimal, order.price)
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="filled",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id=provider_id,
        cumulative_filled_quantity=quantity,
        cumulative_average_price=price,
        last_fill_quantity=quantity,
        last_fill_price=price,
        fee=Decimal("0.01"),
        fee_asset="USDT",
        last_fill_exchange_trade_id="saved-trade-replay-original",
    ))["action"] == "applied"
    with factory() as session:
        trade_id = session.scalar(
            select(Trade.id).where(
                Trade.exchange_trade_id == "saved-trade-replay-original"
            )
        )
    assert trade_id is not None

    def snapshot():
        with factory() as session:
            saved_order = session.get(Order, order_id)
            saved_trade = session.get(Trade, trade_id)
            assert saved_order is not None and saved_trade is not None
            receipts = list(session.scalars(
                select(SpiderRuntimeDelivery).where(
                    SpiderRuntimeDelivery.environment == scope.environment,
                    SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                    SpiderRuntimeDelivery.local_order_id == order_id,
                ).order_by(SpiderRuntimeDelivery.sequence)
            ))
            last_sequence = session.scalar(
                select(SpiderRuntimeScope.last_received_sequence).where(
                    SpiderRuntimeScope.environment == scope.environment,
                    SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                    SpiderRuntimeScope.strategy_id == scope.strategy_id,
                )
            )
            order_values = tuple(
                getattr(saved_order, name)
                for name in (
                    "strategy_id", "product_id", "exchange_id", "account_profile",
                    "account_id", "type", "side", "price", "trigger_price",
                    "quantity", "status", "filled_quantity", "filled_price",
                    "client_order_id", "intent_payload", "exchange_order_id",
                )
            )
            trade_values = tuple(
                getattr(saved_trade, name)
                for name in (
                    "order_id", "exchange_trade_id", "product_id", "side", "price",
                    "quantity", "fee", "fee_asset", "timestamp",
                )
            )
            receipt_values = tuple(
                (
                    row.sequence,
                    row.delivery_id,
                    row.content_sha256,
                    bytes(row.source_payload),
                )
                for row in receipts
            )
            return order_values, trade_values, receipt_values, last_sequence

    baseline = snapshot()
    valid_readback = owner.readback(scope, operation_id)
    assert valid_readback is not None
    repository = LiveOrderRepository(
        db_session_factory=factory,
        account_profile="okx",
        account_id="account-main",
        transaction_participant=owner.participate_in_order_transaction,
    )
    replay_order = repository.get_order(order_id)
    assert replay_order is not None
    with factory() as session:
        saved_trade = session.get(Trade, trade_id)
        assert saved_trade is not None

    if mutation == "status":
        replay_order.status = "CANCELLED"
    elif mutation == "filled_quantity":
        replay_order.filled_quantity = cast(
            Decimal, replay_order.filled_quantity
        ) + Decimal("0.1")
    elif mutation == "filled_price":
        replay_order.filled_price = cast(
            Decimal, replay_order.filled_price
        ) + Decimal("1")
    elif mutation == "provider_order_id":
        replay_order.exchange_order_id = "conflicting-provider-order"
    elif mutation == "side":
        replay_order.side = "sell" if replay_order.side == "buy" else "buy"
    elif mutation == "price":
        replay_order.price = cast(Decimal, replay_order.price) + Decimal("1")
    elif mutation == "quantity":
        replay_order.quantity += Decimal("1")
    elif mutation == "client_order_id":
        replay_order.client_order_id = "mutated-local-client-id"
    else:
        assert mutation == "intent_payload"
        replay_order.intent_payload = {
            **cast(dict[str, object], replay_order.intent_payload),
            "unexpected": "mutated",
        }

    with pytest.raises(
        RuntimeError,
        match="^spider_financial_trade_replay_order_conflict$",
    ):
        repository.persist_fill(replay_order, saved_trade)

    assert snapshot() == baseline
    assert owner.readback(scope, operation_id) == valid_readback


def test_fill_participant_flushes_only_trade_after_locks_and_rolls_back_atomically(
    operation_pg, mock_clock, mock_exchange_adapter
):
    engine = operation_pg
    (
        owner,
        scope,
        _operation_id,
        _order_manager,
        _prepared,
        _initial,
        _confirmation_record,
        _model_source,
    ) = _install_owner(engine)
    factory = sessionmaker(engine)
    with factory() as session:
        order_id = session.scalar(
            select(Order.id)
            .where(Order.strategy_id == scope.strategy_id)
            .order_by(Order.timestamp, Order.id)
            .limit(1)
        )
        sequence_before = session.scalar(
            select(SpiderRuntimeScope.last_received_sequence)
            .where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        )
        receipt_count_before = session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        )
    assert order_id is not None
    order = LiveOrderRepository(db_session_factory=factory).get_order(order_id)
    assert order is not None
    original_projection = (
        order.status,
        order.filled_quantity,
        order.filled_price,
        order.exchange_order_id,
    )
    order.status = "PARTIALLY_FILLED"
    order.filled_quantity = order.quantity / Decimal(2)
    order.filled_price = cast(Decimal, order.price)
    trade = Trade(
        id="spider-rollback-trade",
        order_id=order.id,
        exchange_trade_id="spider-rollback-trade",
        product_id=order.product_id,
        side=order.side,
        price=cast(Decimal, order.price),
        quantity=order.filled_quantity,
        fee=Decimal("0"),
        fee_asset="USDT",
        timestamp=1_800_000_001_000,
    )

    def fail_after_receipt(session, intended_order, intended_trade):
        assert intended_trade is trade
        owner.participate_in_order_transaction(
            session, intended_order, intended_trade
        )
        assert session.scalar(select(Trade.id).where(Trade.id == trade.id)) == trade.id
        raise RuntimeError("rollback after staged Spider receipt")

    repository = LiveOrderRepository(
        db_session_factory=factory,
        account_profile="okx",
        account_id="account-main",
        transaction_participant=fail_after_receipt,
    )
    with pytest.raises(RuntimeError, match="rollback after staged Spider receipt"):
        repository.persist_fill(order, trade)

    with factory() as session:
        persisted = session.get(Order, order_id)
        assert persisted is not None
        assert (
            persisted.status,
            persisted.filled_quantity,
            persisted.filled_price,
            persisted.exchange_order_id,
        ) == original_projection
        assert session.get(Trade, trade.id) is None
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
            )
        ) == receipt_count_before
        assert session.scalar(
            select(SpiderRuntimeScope.last_received_sequence).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ) == sequence_before


def test_provider_id_conflict_keeps_unresolved_receipt_and_scoped_readback_unavailable(
    operation_pg, mock_clock, mock_exchange_adapter
):
    engine = operation_pg
    (
        owner,
        scope,
        operation_id,
        order_manager,
        _prepared,
        _initial,
        _confirmation_record,
        _model_source,
    ) = _install_owner(engine)
    factory = sessionmaker(engine)
    execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        db_session_factory=factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    with factory() as session:
        order_id = session.scalar(
            select(Order.id)
            .where(Order.strategy_id == scope.strategy_id)
            .order_by(Order.timestamp, Order.id)
            .limit(1)
        )
    assert order_id is not None
    order = order_manager.repo.get_order(order_id)
    assert order is not None
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id="provider-order-original",
    ))["action"] == "applied"
    assert owner.readback(scope, operation_id) is not None

    conflicted = LiveOrderRepository(
        db_session_factory=factory,
        account_profile="okx",
        account_id="account-main",
        transaction_participant=owner.participate_in_order_transaction,
    ).get_order(order_id)
    assert conflicted is not None
    conflicted.exchange_order_id = "provider-order-conflict"
    LiveOrderRepository(
        db_session_factory=factory,
        account_profile="okx",
        account_id="account-main",
        transaction_participant=owner.participate_in_order_transaction,
    ).update_order(conflicted)

    assert owner.readback(scope, operation_id) is None
    with factory() as session:
        delivery = session.scalars(
            select(SpiderRuntimeDelivery)
            .where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.local_order_id == order_id,
            )
            .order_by(SpiderRuntimeDelivery.sequence.desc())
            .limit(1)
        ).one()
        payload = cast(
            dict[str, Any], decode_canonical(bytes(delivery.source_payload))
        )
        assert payload["binding_resolution"] == "UNRESOLVED"
        assert payload["reason_code"] == "BINDING_PROVIDER_ID_CONFLICT"
        runtime = session.execute(
            select(SpiderRuntimeScope.last_received_sequence).where(
                SpiderRuntimeScope.environment == scope.environment,
                SpiderRuntimeScope.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeScope.strategy_id == scope.strategy_id,
            )
        ).scalar_one()
        phase = session.execute(
            select(SpiderRuntimeOperation.phase).where(
                SpiderRuntimeOperation.operation_id == operation_id
            )
        ).scalar_one()
    assert runtime > 0
    assert phase == "INCOMPLETE"


def test_broken_and_missing_receipt_proofs_never_restore_as_valid(
    operation_pg, mock_clock, mock_exchange_adapter
):
    engine = operation_pg
    (
        owner,
        scope,
        operation_id,
        order_manager,
        _prepared,
        _initial,
        _confirmation_record,
        _model_source,
    ) = _install_owner(engine)
    factory = sessionmaker(engine)
    execution = ExecutionEngine(
        db_session=None,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        db_session_factory=factory,
        repository_transaction_participant=owner.participate_in_order_transaction,
        is_backtest=True,
    )
    with factory() as session:
        order_id = session.scalar(
            select(Order.id)
            .where(Order.strategy_id == scope.strategy_id)
            .order_by(Order.timestamp, Order.id)
            .limit(1)
        )
    assert order_id is not None
    order = order_manager.repo.get_order(order_id)
    assert order is not None
    assert execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id="provider-order-for-proof-test",
    ))["action"] == "applied"
    accepted = owner.readback(scope, operation_id)
    assert accepted is not None

    with factory() as session:
        # The ACK path persists the provider ID and then the SUBMITTED status
        # as two ORDER_STATE receipts. Corrupt the tail edge so the valid ACK
        # prefix remains present but cannot chain to the saved Order state.
        delivery = session.scalars(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.local_order_id == order_id,
            ).order_by(SpiderRuntimeDelivery.sequence.desc()).limit(1)
        ).one()
        assert delivery.kind == "ORDER_STATE"
        tail_payload = cast(
            dict[str, Any], decode_canonical(bytes(delivery.source_payload))
        )
        assert tail_payload["transition_ordinal"] == 2
        delivery_id = delivery.delivery_id
        payload = bytes(delivery.source_payload)
    with factory.begin() as session:
        session.execute(
            update(SpiderRuntimeDelivery)
            .where(SpiderRuntimeDelivery.delivery_id == delivery_id)
            .values(source_payload=b"{}")
        )
    assert owner.readback(scope, operation_id) is None
    # Invalid evidence cannot become an accepted restore; the identity remains
    # owned so generic ACTIVE restoration cannot take over the strategy.
    assert owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert owner.readback(scope, operation_id) is None
    with factory.begin() as session:
        session.execute(
            update(SpiderRuntimeDelivery)
            .where(SpiderRuntimeDelivery.delivery_id == delivery_id)
            .values(source_payload=payload)
        )
    assert owner.readback(scope, operation_id) == accepted
    assert owner.restore_owned_runtimes() == frozenset({scope.strategy_id})

    with factory.begin() as session:
        session.execute(
            delete(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.delivery_id == delivery_id
            )
        )
    assert owner.readback(scope, operation_id) is None
    assert owner.restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert owner.readback(scope, operation_id) is None


def test_postcommit_close_loss_is_resolved_by_original_trade_receipt_readback(
    operation_pg
):
    engine = operation_pg
    (
        owner,
        scope,
        operation_id,
        _order_manager,
        _prepared,
        _initial,
        _confirmation_record,
        _model_source,
    ) = _install_owner(engine)
    close_fault = {"pending": True}

    class LoseCommittedCloseSession(Session):
        def commit(self) -> None:
            super().commit()
            self.info["committed"] = True

        def close(self) -> None:
            committed = self.info.get("committed") is True
            super().close()
            if committed and close_fault["pending"]:
                close_fault["pending"] = False
                raise RuntimeError("writer committed but close response was lost")

    factory = sessionmaker(engine, class_=LoseCommittedCloseSession)
    order_repository = LiveOrderRepository(db_session_factory=sessionmaker(engine))
    order_id = None
    with sessionmaker(engine)() as session:
        order_id = session.scalar(
            select(Order.id)
            .where(Order.strategy_id == scope.strategy_id)
            .order_by(Order.timestamp, Order.id)
            .limit(1)
        )
    assert order_id is not None
    order = order_repository.get_order(order_id)
    assert order is not None
    order.status = "PARTIALLY_FILLED"
    order.filled_quantity = order.quantity / Decimal(2)
    order.filled_price = cast(Decimal, order.price)
    trade_id = "original-trade-after-lost-commit-response"
    trade = Trade(
        id=trade_id,
        order_id=order.id,
        exchange_trade_id=trade_id,
        product_id=order.product_id,
        side=order.side,
        price=cast(Decimal, order.price),
        quantity=order.filled_quantity,
        fee=Decimal("0.01"),
        fee_asset="USDT",
        timestamp=1_800_000_002_000,
    )
    repository = LiveOrderRepository(
        db_session_factory=factory,
        account_profile="okx",
        account_id="account-main",
        transaction_participant=owner.participate_in_order_transaction,
    )
    with pytest.raises(RuntimeError, match="writer committed but close response was lost"):
        repository.persist_fill(order, trade)
    assert close_fault["pending"] is False

    with sessionmaker(engine)() as session:
        saved_trade = session.get(Trade, trade_id)
        assert saved_trade is not None
        assert saved_trade.order_id == order_id
        delivery = session.scalars(
            select(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.environment == scope.environment,
                SpiderRuntimeDelivery.execution_scope_id == scope.execution_scope_id,
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.trade_id == trade_id,
            )
        ).one()
        payload = cast(
            dict[str, Any], decode_canonical(bytes(delivery.source_payload))
        )
        assert payload["trade_id"] == trade_id
    readback = owner.readback(scope, operation_id)
    assert readback is not None
    assert readback["trading_ready"] is False


def test_main_real_engine_restores_then_receives_fill_through_injected_participant(
    operation_pg, monkeypatch
):
    from src import main as main_module
    from src.core.engine import StrategyEngine
    from src.core.interfaces.exchange import ExchangeOrderEvent

    engine_db = operation_pg
    (
        _initial_owner,
        scope,
        operation_id,
        _initial_order_manager,
        prepared,
        initial,
        confirmation,
        model_source,
    ) = _install_owner(engine_db)
    factory = sessionmaker(engine_db)
    composition = SpiderRuntimeComposition(
        (scope,), {confirmation.request_id: model_source}
    )
    monkeypatch.setenv("FLUXTRADE_ENVIRONMENT", "test")
    monkeypatch.setattr(main_module, "SessionLocal", lambda: factory())
    monkeypatch.setattr(main_module, "_adapter_config_from_env", lambda: {"mode": "simulated"})
    monkeypatch.setattr(main_module, "_validate_runtime_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(main_module, "_strategy_artifact_loader_from_env", lambda: lambda: {})
    monkeypatch.setattr(main_module, "runtime_factories_for_config", lambda _config: (None, None))
    monkeypatch.setattr(main_module, "configure_metrics", lambda **_kwargs: None)

    class RedisProbe:
        def hgetall(self, _key: str) -> dict[object, object]:
            return {}

        def register_script(self, _script: str):
            return lambda *, args: None

        def publish(self, _channel: str, _message: str) -> int:
            return 1

        def close(self) -> None:
            pass

    monkeypatch.setattr("src.core.engine.create_redis_client", RedisProbe)
    monkeypatch.setattr("src.core.order_manager.create_redis_client", RedisProbe)

    class AdapterProbe:
        def supports_runtime_reconciliation(self) -> bool:
            return False

        def close(self) -> None:
            pass

    monkeypatch.setattr(main_module, "create_adapter", lambda *_args, **_kwargs: AdapterProbe())
    monkeypatch.setattr(main_module.signal, "signal", lambda *_args: None)

    events: list[tuple[str, frozenset[str]]] = []
    engine_instances: list[StrategyEngine] = []
    owners: list[SpiderRuntimeOwner] = []
    consumer_event_results: list[dict[str, object]] = []
    consumer_order_ids: list[str] = []
    stop_barriers: list[int] = []
    delivery_count_before_late_fill: list[int] = []

    class ConsumerProbe:
        def __init__(self, **_kwargs):
            pass

        def acquire_service_ownership(self):
            pass

        def assert_service_ownership(self):
            pass

        def configure_callbacks(self, **_kwargs):
            pass

        def start(self):
            assert len(engine_instances) == 1 and len(owners) == 1
            execution = engine_instances[0].execution_engine
            with factory() as session:
                order_id = session.scalar(
                    select(Order.id)
                    .where(Order.strategy_id == scope.strategy_id)
                    .order_by(Order.timestamp, Order.id)
                    .limit(1)
                )
            assert order_id is not None
            order = execution.order_manager.repo.get_order(order_id)
            assert order is not None
            quantity = order.quantity / Decimal(2)
            result = execution.process_exchange_order_event(ExchangeOrderEvent(
                status="partially_filled",
                product_id=order.product_id,
                client_order_id=order.client_order_id,
                cumulative_filled_quantity=quantity,
                cumulative_average_price=cast(Decimal, order.price),
                last_fill_quantity=quantity,
                last_fill_price=cast(Decimal, order.price),
                fee=Decimal("0"),
                fee_asset="USDT",
                last_fill_exchange_trade_id="main-composition-trade",
            ))
            consumer_event_results.append(result)
            consumer_order_ids.append(order_id)
            # Malformed STOP envelopes carrying an owned identity must be
            # rejected by the owner and never reach generic StrategyState stop.
            owner = owners[0]
            malformed_owned_stops = [
                {"command": "STOP", "id": scope.strategy_id},
                {"command": "STOP", "id": scope.strategy_id, "params": None},
                {"command": "STOP", "id": scope.strategy_id, "params": False},
                {"command": "STOP", "id": scope.strategy_id, "params": []},
                {"command": "STOP", "id": scope.strategy_id, "params": ""},
                {"command": "STOP", "id": scope.strategy_id, "params": {}},
                {
                    "command": "STOP",
                    "id": scope.strategy_id,
                    "params": {"strategy_id": "ordinary-nonowned"},
                },
                {
                    "command": "STOP",
                    "strategy_id": scope.strategy_id,
                    "params": {"id": "ordinary-nonowned"},
                },
            ]
            assert owner.handle_owned_command(
                {"command": "STOP", "id": "ordinary-nonowned"}
            ) is None
            with factory() as session:
                before_malformed = (
                    session.execute(
                        select(
                            SpiderRuntimeScope.control_mode,
                            SpiderRuntimeScope.state_version,
                            SpiderRuntimeScope.control_barrier_sequence,
                            SpiderRuntimeScope.last_received_sequence,
                        ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
                    ).one(),
                    session.execute(
                        select(StrategyState.status, StrategyState.version).where(
                            StrategyState.strategy_id == scope.strategy_id
                        )
                    ).one(),
                    session.scalar(
                        select(func.count()).select_from(SpiderControlReceipt).where(
                            SpiderControlReceipt.strategy_id == scope.strategy_id
                        )
                    ),
                )
            router_calls: list[dict[str, object]] = []
            engine_instances[0]._command_router.handle = lambda message: (
                router_calls.append(message) or CommandResult(False, "unexpected generic fallback")
            )
            for malformed in malformed_owned_stops:
                result = owner.handle_owned_command(malformed)
                assert result is not None and result.success is False
                engine_instances[0]._strategy_command_dispatch.dispatch(malformed)
            assert router_calls == []
            with factory() as session:
                after_malformed = (
                    session.execute(
                        select(
                            SpiderRuntimeScope.control_mode,
                            SpiderRuntimeScope.state_version,
                            SpiderRuntimeScope.control_barrier_sequence,
                            SpiderRuntimeScope.last_received_sequence,
                        ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
                    ).one(),
                    session.execute(
                        select(StrategyState.status, StrategyState.version).where(
                            StrategyState.strategy_id == scope.strategy_id
                        )
                    ).one(),
                    session.scalar(
                        select(func.count()).select_from(SpiderControlReceipt).where(
                            SpiderControlReceipt.strategy_id == scope.strategy_id
                        )
                    ),
                )
            assert after_malformed == before_malformed
            with factory() as session:
                stop_barriers.append(cast(int, session.scalar(
                    select(SpiderRuntimeScope.last_received_sequence).where(
                        SpiderRuntimeScope.strategy_id == scope.strategy_id
                    )
                )))
            engine_instances[0]._strategy_command_dispatch.dispatch({
                "command": "STOP",
                "strategy_id": scope.strategy_id,
                "params": {
                    "strategy_id": scope.strategy_id,
                    "environment": scope.environment,
                    "execution_scope_id": scope.execution_scope_id,
                    "actor": "api_key",
                    "idempotency_key": "main-stop-key",
                    "expected_version": initial["state_version"],
                },
            })

        def request_stop(self):
            pass

        def stop(self):
            pass

    monkeypatch.setattr(main_module, "DataConsumer", ConsumerProbe)

    # Keep the real Engine startup ordering and repository construction while
    # replacing only external listeners, Redis work, and periodic workers.
    original_init = StrategyEngine.__init__
    original_owned_restore = StrategyEngine._restore_owned_runtimes_on_startup
    original_generic_restore = StrategyEngine._restore_active_strategies_on_startup
    original_owner_type = main_module.SpiderRuntimeOwner

    def capture_init(self, *args, **kwargs):
        original_init(self, *args, **kwargs)
        engine_instances.append(self)

    def capture_owned_restore(self):
        original_owned_restore(self)
        events.append(("owned", self._startup_owned_strategy_ids))

    def capture_generic_restore(self):
        events.append(("generic", self._startup_owned_strategy_ids))
        original_generic_restore(self)

    def capture_owner(*args, **kwargs):
        owner = original_owner_type(*args, **kwargs)
        owners.append(owner)
        return owner

    monkeypatch.setattr(StrategyEngine, "__init__", capture_init)
    monkeypatch.setattr(
        StrategyEngine,
        "_restore_owned_runtimes_on_startup",
        capture_owned_restore,
    )
    monkeypatch.setattr(
        StrategyEngine,
        "_restore_active_strategies_on_startup",
        capture_generic_restore,
    )
    monkeypatch.setattr(main_module, "SpiderRuntimeOwner", capture_owner)
    monkeypatch.setattr(StrategyEngine, "_halt_for_kill_switch", lambda _self: None)
    monkeypatch.setattr(StrategyEngine, "_start_command_listener", lambda _self: None)
    monkeypatch.setattr(StrategyEngine, "_check_system_state", lambda _self: False)
    monkeypatch.setattr(StrategyEngine, "_reconcile_startup_balance", lambda _self: None)
    monkeypatch.setattr(StrategyEngine, "_start_strategy_state_subscriber_on_startup", lambda _self: None)
    monkeypatch.setattr(StrategyEngine, "_start_exchange_order_event_stream", lambda _self: None)
    monkeypatch.setattr(StrategyEngine, "_reconcile_recoverable_orders_on_startup", lambda _self: None)
    monkeypatch.setattr(StrategyEngine, "_start_heartbeat", lambda _self: None)
    monkeypatch.setattr(StrategyEngine, "scan_strategies", lambda _self: None)
    main_module.main(composition)

    assert events == [
        ("owned", frozenset({scope.strategy_id})),
        ("generic", frozenset({scope.strategy_id})),
    ]
    assert len(engine_instances) == 1
    assert len(owners) == 1
    actual_engine = engine_instances[0]
    actual_repository = cast(
        LiveOrderRepository, actual_engine.execution_engine.order_manager.repo
    )
    assert actual_repository._transaction_participant is not None
    assert owners[0]._strategy_state_manager is actual_engine._strategy_state_manager
    assert owners[0]._lifecycle_lock == actual_engine._strategy_lifecycle_lock
    assert owners[0]._order_event_lock is actual_engine.execution_engine._order_event_apply_lock
    assert len(consumer_event_results) == 1
    assert consumer_event_results[0]["action"] == "applied"
    assert len(consumer_order_ids) == 1
    order_id = consumer_order_ids[0]
    with factory() as session:
        stopped = session.execute(
            select(SpiderRuntimeScope.control_mode, SpiderRuntimeScope.control_barrier_sequence,
                   SpiderRuntimeScope.last_received_sequence, SpiderRuntimeScope.state_version)
            .where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).one()
        assert stopped.control_mode == "STOP"
        assert stopped.control_barrier_sequence == stop_barriers[0]
        assert stopped.last_received_sequence == stop_barriers[0]
        assert stopped.state_version == cast(int, initial["state_version"]) + 1
        receipt = session.execute(
            select(SpiderControlReceipt).where(
                SpiderControlReceipt.strategy_id == scope.strategy_id,
                SpiderControlReceipt.idempotency_key == "main-stop-key",
            )
        ).scalar_one()
        assert receipt.operation_id == operation_id
        assert receipt.generation == cast(dict[str, object], initial["effective"])["generation"]
        assert receipt.before_state_version == initial["state_version"]
        assert receipt.after_state_version == cast(int, initial["state_version"]) + 1
        assert receipt.barrier_sequence == 1
    assert receipt.outcome == "COMPLETED"
    execution = actual_engine.execution_engine
    order = execution.order_manager.repo.get_order(order_id)
    assert order is not None
    quantity = order.quantity
    price = cast(Decimal, order.price)
    with factory() as session:
        delivery_count_before_late_fill.append(cast(int, session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id
            )
        )))
    provider_order_id = "main-composition-provider-order"
    execution.process_exchange_order_event(ExchangeOrderEvent(
        status="open",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id=provider_order_id,
    ))
    late_full = ExchangeOrderEvent(
        status="filled",
        product_id=order.product_id,
        client_order_id=order.client_order_id,
        exchange_order_id=provider_order_id,
        cumulative_filled_quantity=quantity,
        cumulative_average_price=price,
        last_fill_quantity=quantity / Decimal(2),
        last_fill_price=price,
        fee=Decimal("0.01"),
        fee_asset="USDT",
        last_fill_exchange_trade_id="main-composition-late-trade",
    )
    assert execution.process_exchange_order_event(late_full)["action"] == "applied"
    execution.process_exchange_order_event(late_full)
    with factory() as session:
        assert session.scalar(
            select(func.count()).select_from(Trade).where(Trade.order_id == order_id)
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(SpiderRuntimeDelivery).where(
                SpiderRuntimeDelivery.strategy_id == scope.strategy_id,
                SpiderRuntimeDelivery.local_order_id == order_id,
            )
        ) == delivery_count_before_late_fill[0] + 2

    # A second authorized STOP advances the lifecycle version after the real
    # late fill advanced the receiver barrier. Retrying the original key must
    # prove A without regressing B's current projection.
    def dispatch_stop(key: str, expected_version: int) -> CommandResult:
        results: list[CommandResult | None] = []
        _owned_dispatch(owners[0], results).dispatch({
            "command": "STOP",
            "strategy_id": scope.strategy_id,
            "params": {
                "strategy_id": scope.strategy_id,
                "environment": scope.environment,
                "execution_scope_id": scope.execution_scope_id,
                "actor": "api_key",
                "idempotency_key": key,
                "expected_version": expected_version,
            },
        })
        assert len(results) == 1 and results[0] is not None
        return cast(CommandResult, results[0])

    second_stop = dispatch_stop(
        "main-stop-key-b", cast(int, initial["state_version"]) + 1
    )
    assert second_stop.success is True and second_stop.data is not None
    assert second_stop.data["disposition"] == "COMPLETED"
    with factory() as session:
        after_b = session.execute(
            select(
                SpiderRuntimeScope.control_mode,
                SpiderRuntimeScope.state_version,
                SpiderRuntimeScope.control_barrier_sequence,
                SpiderRuntimeScope.last_received_sequence,
                SpiderRuntimeScope.current_operation_id,
                SpiderRuntimeScope.generation,
            ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).one()
        assert session.scalar(
            select(func.count()).select_from(SpiderControlReceipt).where(
                SpiderControlReceipt.strategy_id == scope.strategy_id
            )
        ) == 2
    assert after_b.control_mode == "STOP"
    assert after_b.state_version == cast(int, initial["state_version"]) + 2
    assert after_b.control_barrier_sequence == after_b.last_received_sequence
    assert after_b.control_barrier_sequence > stop_barriers[0]
    assert after_b.current_operation_id == operation_id
    assert after_b.generation == cast(dict[str, object], initial["effective"])["generation"]

    retry_a = dispatch_stop("main-stop-key", cast(int, initial["state_version"]))
    assert retry_a.success is True and retry_a.data is not None
    assert retry_a.data["disposition"] == "COMPLETED"
    assert retry_a.data["operation_id"] == operation_id
    assert cast(dict[str, object], retry_a.data["readback"])["runtime_control"] == "STOP"
    with factory() as session:
        after_a_retry = session.execute(
            select(
                SpiderRuntimeScope.control_mode,
                SpiderRuntimeScope.state_version,
                SpiderRuntimeScope.control_barrier_sequence,
                SpiderRuntimeScope.last_received_sequence,
                SpiderRuntimeScope.current_operation_id,
                SpiderRuntimeScope.generation,
            ).where(SpiderRuntimeScope.strategy_id == scope.strategy_id)
        ).one()
        assert session.scalar(
            select(func.count()).select_from(SpiderControlReceipt).where(
                SpiderControlReceipt.strategy_id == scope.strategy_id
            )
        ) == 2
        assert session.scalar(
            select(func.count()).select_from(StrategyStateTransition).where(
                StrategyStateTransition.strategy_id == scope.strategy_id,
                StrategyStateTransition.to_status == "STOPPED",
            )
        ) == 2
    assert after_a_retry == after_b

    readback = owners[0].readback(scope, operation_id)
    assert readback is not None
    assert readback["checkpoint_sha256"] == initial["checkpoint_sha256"]
    assert readback["initial_plan_missing_members"] == initial[
        "initial_plan_missing_members"
    ]
    assert readback["runtime_control"] == "STOP"
    assert readback["trading_ready"] is False
    assert owners[0].restore_owned_runtimes() == frozenset({scope.strategy_id})
    assert prepared.build_checkpoint_sha256 == initial["checkpoint_sha256"]
