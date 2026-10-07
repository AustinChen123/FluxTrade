from __future__ import annotations

from decimal import Decimal
import logging
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock, call, patch

import pytest

from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)
from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.adapters.rithmic_strategy_exit import RithmicStrategyExitService
from src.core.execution import ExecutionEngine
from src.core.execution import ExitDecision
from src.core.interfaces.exchange import ExchangeError
from src.core.models import OrderStatus, Position, PositionSide, Signal, SignalType
from src.core.repositories import LiveOrderRepository
from src.core.orm_models import (
    Exchange,
    Product,
    Strategy as StoredStrategy,
    SystemEvent,
    Trade as StoredTrade,
)


PRODUCT = "RITHMIC:NQ-202609"


def _signal(signal_type: SignalType = SignalType.EXIT_LONG) -> Signal:
    return Signal(
        strategy_id="strategy",
        product_id=PRODUCT,
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=signal_type,
        quantity=Decimal("1"),
    )


def _decision(
    *,
    quantity: Decimal = Decimal("1"),
    position_quantity: Decimal | None = Decimal("1"),
) -> ExitDecision:
    return ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=quantity,
        position_quantity=position_quantity,
    )


def _position(
    quantity: str = "1",
    side: PositionSide = PositionSide.LONG,
) -> Position:
    return Position(
        strategy_id="LIVE",
        product_id=PRODUCT,
        side=side,
        quantity=Decimal(quantity),
        entry_price=Decimal("20000"),
        unrealized_pnl=Decimal("0"),
    )


def _working_order() -> SimpleNamespace:
    return SimpleNamespace(
        basket_id="basket-1",
        exchange_order_id="exchange-order-1",
        client_order_id="client-1",
        notification_type="NEW",
        status="open",
        quantity="1",
        filled_quantity="0",
        unfilled_quantity="1",
        average_fill_price=None,
    )


def _remote_order(
    basket_id: str,
    *,
    status: str = "open",
    notification_type: str = "OPEN",
    quantity: str = "1",
    filled_quantity: str = "0",
    unfilled_quantity: str | None = "1",
) -> SimpleNamespace:
    return SimpleNamespace(
        basket_id=basket_id,
        exchange_order_id=f"exchange-{basket_id}",
        client_order_id="unrelated-child-tag",
        status=status,
        notification_type=notification_type,
        quantity=quantity,
        filled_quantity=filled_quantity,
        unfilled_quantity=unfilled_quantity,
        average_fill_price=None,
    )


def _snapshot(
    *,
    positions: list[Position] | None = None,
    orders: list[object] | None = None,
    order_history: list[object] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        positions=[] if positions is None else positions,
        orders=[] if orders is None else orders,
        order_history=[] if order_history is None else order_history,
    )


def _service() -> tuple[RithmicStrategyExitService, SimpleNamespace]:
    adapter = MagicMock()
    adapter.account_id = "ACCOUNT"
    adapter.configured_product_ids = (PRODUCT,)
    adapter.positions_from_ledger_snapshot.side_effect = lambda snapshot: list(
        snapshot.positions
    )
    execution_engine = MagicMock()
    execution_engine.clock.now.return_value = 1_704_067_200
    execution_engine.list_recoverable_client_orders.return_value = []
    execution_engine.order_manager.repo.list_orders_by_statuses.return_value = []
    execution_engine.reconcile_owned_orders.return_value = {"auto_resume_safe": True}
    account_service = MagicMock()
    dependencies = SimpleNamespace(
        adapter=adapter,
        execution_engine=execution_engine,
        account_service=account_service,
        operation_gate=RithmicOrderEventLifecycleGate(),
        stop_order_event_stream=MagicMock(return_value=True),
        assert_leadership=MagicMock(),
        restart_order_stream=MagicMock(),
        lockdown=MagicMock(),
    )
    service = RithmicStrategyExitService(
        adapter=adapter,
        execution_engine=execution_engine,
        account_service=account_service,
        operation_gate=dependencies.operation_gate,
        profile="test",
        account_id="ACCOUNT",
        stop_order_event_stream=dependencies.stop_order_event_stream,
        assert_leadership=dependencies.assert_leadership,
        restart_order_stream=dependencies.restart_order_stream,
        lockdown=dependencies.lockdown,
        logger=logging.getLogger("test.rithmic_strategy_exit"),
    )
    return service, dependencies


def _actual_sql_exit_runtime(
    factory,
    clock,
    *,
    strategy_id: str = "strategy",
    order: Any = None,
) -> tuple[RithmicStrategyExitService, LiveOrderRepository, Any, Any, Any]:
    SystemEvent.__table__.create(factory.kw["bind"], checkfirst=True)
    with factory() as session:
        if session.get(Exchange, "RITHMIC") is None:
            session.add(Exchange(id="RITHMIC", name="Rithmic"))
        if session.get(Product, PRODUCT) is None:
            session.add(
                Product(
                    id=PRODUCT,
                    exchange_id="RITHMIC",
                    base_asset="NQ",
                    quote_asset="USD",
                )
            )
        for owner_id in {strategy_id, getattr(order, "strategy_id", strategy_id)}:
            if session.get(StoredStrategy, owner_id) is None:
                session.add(StoredStrategy(id=owner_id, name="Exit Strategy"))
        session.commit()
    repository = LiveOrderRepository(
        db_session_factory=factory,
        account_profile="test",
        account_id="ACCOUNT",
    )
    if order is not None:
        repository.add_order(order)
    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments={
            PRODUCT: {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
                "multiplier": "20",
            }
        },
    )
    adapter.close = MagicMock()
    adapter.start_order_event_stream = MagicMock()
    adapter.cancel_order = MagicMock(return_value=True)
    account_service = SimpleNamespace(
        get_all_positions=lambda: [
            position
            for side in ("BUY", "SELL")
            if (position := repository.get_position(strategy_id, PRODUCT, side))
            is not None
        ]
    )
    engine = ExecutionEngine(
        db_session=None,
        db_session_factory=factory,
        clock=clock,
        adapter=adapter,
        order_repository=repository,
        is_backtest=True,
        account_service=account_service,
    )
    engine.exit_authoritative_position = MagicMock()
    publisher = MagicMock()
    service = RithmicStrategyExitService(
        adapter=adapter,
        execution_engine=engine,
        account_service=publisher,
        operation_gate=RithmicOrderEventLifecycleGate(),
        profile="test",
        account_id="ACCOUNT",
        stop_order_event_stream=MagicMock(return_value=True),
        assert_leadership=MagicMock(),
        restart_order_stream=MagicMock(),
        lockdown=MagicMock(),
        logger=logging.getLogger("test.rithmic_strategy_exit.sql"),
    )
    return service, repository, adapter, engine, publisher


def test_valid_request_enters_operation_gate_before_money_path() -> None:
    service, dependencies = _service()
    sentinel = {"status": "serialized"}
    dependencies.operation_gate.run = MagicMock(return_value=sentinel)

    assert service.execute(_signal(), _decision()) is sentinel

    dependencies.operation_gate.run.assert_called_once()
    operation, signal, position_quantity = (
        dependencies.operation_gate.run.call_args.args
    )
    assert operation == service._execute_validated
    assert signal == _signal()
    assert position_quantity == Decimal("1")
    dependencies.stop_order_event_stream.assert_not_called()


@pytest.mark.parametrize(
    ("signal", "decision", "expected"),
    (
        (_signal(SignalType.LONG), _decision(), "requires_exit_signal"),
        (
            _signal(),
            _decision(quantity=Decimal("0.5")),
            "partial_strategy_exit_unsupported",
        ),
        (
            _signal(),
            _decision(position_quantity=None),
            "partial_strategy_exit_unsupported",
        ),
    ),
)
def test_invalid_request_fails_before_runtime_or_money_mutation(
    signal: Signal,
    decision: ExitDecision,
    expected: str,
) -> None:
    service, dependencies = _service()
    dependencies.operation_gate.run = MagicMock()

    with pytest.raises((ValueError, RuntimeError), match=expected):
        service.execute(signal, decision)

    dependencies.operation_gate.run.assert_not_called()
    dependencies.stop_order_event_stream.assert_not_called()
    dependencies.adapter.start_order_event_stream.assert_not_called()
    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.restart_order_stream.assert_not_called()
    dependencies.lockdown.assert_not_called()


def test_stop_timeout_prohibits_money_path_and_replacement_worker() -> None:
    service, dependencies = _service()
    dependencies.stop_order_event_stream.return_value = False

    with pytest.raises(
        RuntimeError,
        match="rithmic_strategy_exit_event_stream_stop_timeout",
    ):
        service.execute(_signal(), _decision())

    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.assert_not_called()
    dependencies.execution_engine.reconcile_owned_orders.assert_not_called()
    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.account_service.replace_positions_for_products.assert_not_called()
    dependencies.assert_leadership.assert_not_called()
    dependencies.restart_order_stream.assert_not_called()
    dependencies.lockdown.assert_called_once_with(
        "rithmic_strategy_exit_requires_reconciliation:RuntimeError"
    )


@pytest.mark.parametrize("preflight_quantity", (None, "1"))
def test_success_preserves_already_flat_and_native_exit_paths(
    preflight_quantity: str | None,
) -> None:
    service, dependencies = _service()
    preflight = _snapshot(
        positions=(
            [] if preflight_quantity is None else [_position(preflight_quantity)]
        )
    )
    flat = _snapshot()

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=[preflight, flat],
    ):
        result = service.execute(_signal(), _decision())

    assert result == {
        "status": "verified_flat",
        "cancelled_orders": 0,
        "product_id": PRODUCT,
    }
    if preflight_quantity is None:
        dependencies.execution_engine.exit_authoritative_position.assert_not_called()
        dependencies.adapter.start_order_event_stream.assert_not_called()
    else:
        dependencies.execution_engine.exit_authoritative_position.assert_called_once_with(
            PRODUCT,
            account_id="ACCOUNT",
        )
        assert dependencies.adapter.start_order_event_stream.call_count == 1
    dependencies.account_service.replace_positions_for_products.assert_called_once()
    dependencies.restart_order_stream.assert_called_once_with()
    dependencies.lockdown.assert_not_called()


@pytest.mark.parametrize("with_position_and_order", (False, True))
def test_success_preserves_exact_money_path_order(
    with_position_and_order: bool,
) -> None:
    service, dependencies = _service()
    trace: list[str] = []
    order = SimpleNamespace(
        id="order-1",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id="client-1",
        type="stop_loss",
    )
    dependencies.stop_order_event_stream.side_effect = lambda **_kwargs: (
        trace.append("stop") or True
    )
    dependencies.assert_leadership.side_effect = lambda: trace.append("fence")
    dependencies.adapter.start_order_event_stream.side_effect = lambda: trace.append(
        "start"
    )
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.side_effect = (
        lambda _statuses: trace.append("list_orders")
        or ([order] if with_position_and_order else [])
    )
    dependencies.adapter.cancel_order.side_effect = (
        lambda *_args, **_kwargs: trace.append("cancel") or True
    )
    dependencies.adapter.close.side_effect = lambda: trace.append("close")
    dependencies.execution_engine.reconcile_owned_orders.side_effect = (
        lambda *_args, **_kwargs: trace.append("reconcile")
        or {"auto_resume_safe": True}
    )
    dependencies.account_service.replace_positions_for_products.side_effect = (
        lambda *_args, **_kwargs: trace.append("publish")
    )
    dependencies.execution_engine.exit_authoritative_position.side_effect = (
        lambda *_args, **_kwargs: trace.append("native_exit")
    )
    dependencies.restart_order_stream.side_effect = lambda: trace.append("restart")
    order.exchange_id = "RITHMIC"
    order.account_profile = "test"
    order.account_id = "ACCOUNT"
    order.exchange_order_id = "basket-1"
    dependencies.execution_engine.order_manager.repo.get_order.return_value = (
        SimpleNamespace(status=OrderStatus.CANCELLED.value)
    )
    snapshots = [
        _snapshot(
            positions=[_position()] if with_position_and_order else [],
            orders=[_remote_order("basket-1")] if with_position_and_order else [],
            order_history=(
                [
                    _remote_order("basket-1", status="open"),
                    _remote_order("basket-1", status="open"),
                ]
                if with_position_and_order
                else []
            ),
        ),
        _snapshot(
            order_history=[
                _remote_order(
                    "basket-1",
                    status="cancelled",
                    notification_type="CANCEL",
                )
            ]
        )
        if with_position_and_order
        else _snapshot(),
    ]

    def load_snapshot(*_args, **_kwargs):
        trace.append("snapshot")
        return snapshots.pop(0)

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=load_snapshot,
    ):
        assert service.execute(_signal(), _decision())["status"] == "verified_flat"

    expected = [
        "stop",
        "fence",
        "close",
        "snapshot",
        "reconcile",
        "list_orders",
    ]
    if with_position_and_order:
        expected.extend(
            [
                "start",
                "fence",
                "cancel",
                "close",
                "snapshot",
                "reconcile",
            ]
        )
    expected.extend(["fence", "publish", "fence", "restart"])
    assert trace == expected


@pytest.mark.parametrize("failure_phase", ("cancel", "native_exit", "flat_publish"))
def test_leadership_fence_failure_prevents_following_money_mutation(
    failure_phase: str,
) -> None:
    service, dependencies = _service()
    primary = RuntimeError(f"{failure_phase}-leadership-lost")
    if failure_phase == "cancel":
        order = SimpleNamespace(
            id="order-1",
            strategy_id="strategy",
            product_id=PRODUCT,
            status=OrderStatus.SUBMITTED.value,
            client_order_id="client-1",
            type="stop_loss",
        )
        dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
            order
        ]
        dependencies.adapter.get_order_by_client_id.return_value = SimpleNamespace(
            status="open",
            exchange_order_id="basket-1",
        )
        dependencies.assert_leadership.side_effect = [None, primary, None]
        order.exchange_id = "RITHMIC"
        order.account_profile = "test"
        order.account_id = "ACCOUNT"
        order.exchange_order_id = "basket-1"
        snapshot = _snapshot(orders=[_remote_order("basket-1")])
    elif failure_phase == "native_exit":
        dependencies.assert_leadership.side_effect = [None, primary, None]
        snapshot = _snapshot(positions=[_position()])
    else:
        dependencies.assert_leadership.side_effect = [None, primary, None]
        snapshot = _snapshot()

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            return_value=snapshot,
        ) as loader,
        pytest.raises(RuntimeError) as caught,
    ):
        service.execute(_signal(), _decision())

    assert caught.value is primary
    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.account_service.replace_positions_for_products.assert_not_called()
    if failure_phase in {"cancel", "flat_publish"}:
        dependencies.adapter.cancel_order.assert_not_called()
        loader.assert_called_once()
    else:
        dependencies.account_service.replace_positions_for_products.assert_not_called()
    dependencies.restart_order_stream.assert_called_once_with()


def test_cancels_scoped_active_order_by_basket_despite_history_rows() -> None:
    service, dependencies = _service()
    matching = SimpleNamespace(
        id="matching",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id="matching-client",
        type="stop_loss",
        exchange_id="RITHMIC",
        account_profile="test",
        account_id="ACCOUNT",
        exchange_order_id="matching-basket",
    )
    local_new = SimpleNamespace(
        id="new",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.NEW.value,
        client_order_id=None,
        type="limit",
    )
    wrong_strategy = SimpleNamespace(**{**vars(matching), "strategy_id": "other"})
    wrong_product = SimpleNamespace(**{**vars(matching), "product_id": "RITHMIC:ES"})
    wrong_account = SimpleNamespace(**{**vars(matching), "account_id": "OTHER"})
    wrong_profile = SimpleNamespace(**{**vars(matching), "account_profile": "other"})
    wrong_venue = SimpleNamespace(**{**vars(matching), "exchange_id": "BINANCE"})
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
        matching,
        local_new,
        wrong_strategy,
        wrong_product,
        wrong_account,
        wrong_profile,
        wrong_venue,
    ]
    dependencies.execution_engine.order_manager.repo.get_order.return_value = (
        SimpleNamespace(status=OrderStatus.CANCELLED.value)
    )
    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=[
            _snapshot(
                orders=[_remote_order("matching-basket")],
                order_history=[
                    _remote_order(
                        "matching-basket",
                        status="open",
                    ),
                    _remote_order(
                        "matching-basket",
                        status="partial",
                    ),
                ],
            ),
            _snapshot(
                order_history=[
                    _remote_order(
                        "matching-basket",
                        status="cancelled",
                        notification_type="CANCEL",
                    ),
                ]
            ),
        ],
    ):
        result = service.execute(_signal(), _decision())

    assert result["cancelled_orders"] == 2
    dependencies.execution_engine.order_manager.fail_order.assert_called_once_with(
        local_new,
        "strategy_exit",
    )
    dependencies.adapter.cancel_order.assert_called_once_with(
        "matching-basket",
        PRODUCT,
        order_type="stop_loss",
    )
    dependencies.adapter.get_order_by_client_id.assert_not_called()
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.assert_called_once_with(
        {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
    )


def test_unsafe_initial_reconciliation_does_not_fail_local_new_order() -> None:
    service, dependencies = _service()
    local_new = SimpleNamespace(
        id="new",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.NEW.value,
        client_order_id=None,
        type="limit",
    )
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
        local_new
    ]
    dependencies.execution_engine.reconcile_owned_orders.return_value = {
        "auto_resume_safe": False
    }

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            return_value=_snapshot(),
        ),
        pytest.raises(RuntimeError, match="preflight_reconciliation_blocked"),
    ):
        service.execute(_signal(), _decision())

    dependencies.execution_engine.order_manager.fail_order.assert_not_called()
    dependencies.adapter.cancel_order.assert_not_called()
    dependencies.adapter.start_order_event_stream.assert_not_called()
    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.account_service.replace_positions_for_products.assert_not_called()


def test_invalid_cancellation_batch_has_no_earlier_local_or_remote_mutation() -> None:
    service, dependencies = _service()
    valid = SimpleNamespace(
        id="valid",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id="client-valid",
        type="stop_loss",
        exchange_id="RITHMIC",
        account_profile="test",
        account_id="ACCOUNT",
        exchange_order_id="basket-valid",
    )
    invalid = SimpleNamespace(
        id="invalid",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id="client-invalid",
        type="stop_loss",
        exchange_id="RITHMIC",
        account_profile="test",
        account_id="ACCOUNT",
        exchange_order_id="basket-missing",
    )
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
        valid,
        invalid,
    ]

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            return_value=_snapshot(orders=[_remote_order("basket-valid")]),
        ),
        pytest.raises(RuntimeError, match="cancel_identity_ambiguous"),
    ):
        service.execute(_signal(), _decision())

    dependencies.adapter.cancel_order.assert_not_called()
    dependencies.execution_engine.order_manager.fail_order.assert_not_called()
    dependencies.adapter.start_order_event_stream.assert_not_called()


@pytest.mark.parametrize(
    ("postcheck", "terminal"),
    (
        ("terminal", True),
        ("working", False),
        ("wrong_side", True),
        ("excess_quantity", True),
        ("projection", True),
    ),
)
def test_cancel_exchange_error_requires_safe_durable_terminal_recheck(
    postcheck: str,
    terminal: bool,
) -> None:
    service, dependencies = _service()
    target = SimpleNamespace(
        id="order-1",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id="client-1",
        type="stop_loss",
        exchange_id="RITHMIC",
        account_profile="test",
        account_id="ACCOUNT",
        exchange_order_id="basket-1",
    )
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
        target
    ]
    dependencies.execution_engine.order_manager.repo.get_order.return_value = (
        SimpleNamespace(
            status=(
                OrderStatus.CANCELLED.value if terminal else OrderStatus.SUBMITTED.value
            )
        )
    )
    dependencies.adapter.cancel_order.side_effect = ExchangeError("uncertain cancel")
    if postcheck == "projection":
        dependencies.adapter.positions_from_ledger_snapshot.side_effect = RuntimeError(
            "position projection failed"
        )
    post_positions = []
    if postcheck == "wrong_side":
        post_positions = [_position(side=PositionSide.SHORT)]
    elif postcheck == "excess_quantity":
        post_positions = [_position(quantity="2")]
    snapshots = [
        _snapshot(orders=[_remote_order("basket-1")]),
        _snapshot(
            positions=post_positions,
            orders=(
                []
                if terminal
                else [
                    _remote_order("basket-1", status="OPEN", notification_type="OPEN")
                ]
            ),
            order_history=[
                _remote_order(
                    "basket-1",
                    status="cancelled" if terminal else "open",
                    notification_type="CANCEL" if terminal else "OPEN",
                )
            ],
        ),
    ]

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            side_effect=snapshots,
        ) as loader,
    ):
        if terminal:
            if postcheck == "terminal":
                assert (
                    service.execute(_signal(), _decision())["status"] == "verified_flat"
                )
            else:
                with pytest.raises(ExchangeError, match="uncertain cancel") as caught:
                    service.execute(_signal(), _decision())
                assert caught.value.__cause__ is not None
        else:
            with pytest.raises(ExchangeError, match="uncertain cancel") as caught:
                service.execute(_signal(), _decision())
            assert caught.value.__cause__ is not None

    assert loader.call_count == 2
    dependencies.adapter.cancel_order.assert_called_once_with(
        "basket-1", PRODUCT, order_type="stop_loss"
    )
    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    if postcheck == "terminal":
        dependencies.account_service.replace_positions_for_products.assert_called_once()
    else:
        dependencies.account_service.replace_positions_for_products.assert_not_called()


@pytest.mark.parametrize(
    "scenario",
    (
        "partial_fill",
        "native_child_parent_tag",
        "native_child_absent_tag",
        "tagged_new",
        "other_strategy_working_residual",
        "other_strategy_working_flat",
    ),
)
def test_owned_partial_fill_persists_before_cancel_and_flat_drift_stays_blocked(
    sqlite_order_session_factory,
    order_factory,
    mock_clock,
    scenario: str,
) -> None:
    factory = sqlite_order_session_factory
    order_options = {
        "order_id": "owned-parent",
        "strategy_id": "strategy",
        "product_id": PRODUCT,
        "exchange_id": "RITHMIC",
        "order_type": "market",
        "side": "buy",
        "quantity": Decimal("2"),
        "status": OrderStatus.SUBMITTED.value,
        "timestamp": 1_700_000_000_000,
        "client_order_id": "flux-parent",
        "exchange_order_id": "basket-parent",
        "account_profile": "test",
        "account_id": "ACCOUNT",
    }
    if scenario.startswith("native_child"):
        order_options.update(
            order_id="owned-child",
            order_type="stop_loss",
            side="sell",
            quantity=Decimal("1"),
            client_order_id="local-child-tag",
            exchange_order_id=None,
        )
    elif scenario == "tagged_new":
        order_options.update(
            status=OrderStatus.NEW.value,
            exchange_order_id=None,
            client_order_id="never-sent-tagged-client",
        )
    elif scenario.startswith("other_strategy_working"):
        order_options.update(
            order_id="other-owned-order",
            strategy_id="other-strategy",
            quantity=Decimal("2"),
            client_order_id="other-strategy-client",
            exchange_order_id="unowned-basket",
        )
    order = order_factory(**order_options) if order_options else None
    if scenario.startswith("native_child"):
        assert order is not None
        order.intent_payload = {
            "placement_mode": "attach-at-entry",
            "native_leg_type": "stop_loss",
            "native_parent_basket_id": "basket-parent",
            "native_parent_client_order_id": "entry-parent-tag",
        }
    service, repository, service_adapter, engine, publisher = _actual_sql_exit_runtime(
        factory,
        mock_clock,
        order=order,
    )
    if scenario == "other_strategy_working_residual":
        repository.update_position(
            strategy_id="strategy",
            product_id=PRODUCT,
            side="buy",
            fill_quantity=Decimal("1"),
            fill_price=Decimal("20000.25"),
            position_side="BUY",
        )

    def remote_order(
        status: str,
        notification: str,
        timestamp_ms: int = 1_700_000_001_000,
    ) -> SimpleNamespace:
        if scenario.startswith("native_child"):
            values = {
                "basket_id": "basket-child",
                "exchange_order_id": "exchange-child",
                "original_basket_id": "basket-parent",
                "client_order_id": (
                    None
                    if scenario == "native_child_absent_tag"
                    else "entry-parent-tag"
                ),
                "transaction_type": "SELL",
                "quantity": "1",
                "filled_quantity": "0",
                "unfilled_quantity": "1",
                "average_fill_price": None,
                "price_type": "stop_market",
                "trigger_price": "19998.25",
            }
        elif scenario.startswith("other_strategy_working"):
            values = {
                "basket_id": "unowned-basket",
                "exchange_order_id": "unowned-exchange",
                "original_basket_id": None,
                "client_order_id": "other-strategy-client",
                "transaction_type": "BUY",
                "quantity": "2",
                "filled_quantity": "0",
                "unfilled_quantity": "2",
                "average_fill_price": None,
                "price_type": None,
                "trigger_price": None,
            }
        else:
            values = {
                "basket_id": "basket-parent",
                "exchange_order_id": "exchange-parent",
                "original_basket_id": None,
                "client_order_id": "flux-parent",
                "transaction_type": "BUY",
                "quantity": "2",
                "filled_quantity": "1",
                "unfilled_quantity": "1",
                "average_fill_price": "20000.25",
                "price_type": None,
                "trigger_price": None,
            }
        return SimpleNamespace(
            **values,
            exchange="CME",
            symbol="NQU6",
            status=status,
            notification_type=notification,
            price=None,
            bracket_type=None,
            timestamp_ms=timestamp_ms,
            completion_reason=None,
        )

    fill = SimpleNamespace(
        basket_id="basket-parent",
        exchange_order_id="exchange-parent",
        fill_id="fill-1",
        exchange="CME",
        symbol="NQU6",
        transaction_type="BUY",
        fill_quantity="1",
        fill_price="20000.25",
        timestamp_ms=1_700_000_001_000,
    )

    def snapshot(
        *,
        status: str,
        notification: str,
        working: bool,
        position_quantity: str = "1",
    ):
        remote = remote_order(status, notification)
        if scenario == "tagged_new":
            history = []
        elif scenario.startswith("other_strategy_working"):
            history = []
        elif working:
            history = [
                remote_order("OPEN", "OPEN", timestamp_ms=1_700_000_000_000),
                remote,
            ]
        elif scenario.startswith("native_child"):
            history = [
                remote_order("OPEN", "OPEN", timestamp_ms=1_700_000_000_000),
                remote_order("CANCELLED", "CANCEL", timestamp_ms=1_700_000_002_000),
            ]
        else:
            history = [
                remote_order(
                    "PARTIALLY_FILLED",
                    "FILL",
                    timestamp_ms=1_700_000_000_000,
                ),
                remote_order(
                    "CANCELLED",
                    "CANCEL",
                    timestamp_ms=1_700_000_002_000,
                ),
            ]
        positions = []
        if scenario == "partial_fill" and position_quantity != "0":
            positions.append(
                SimpleNamespace(
                    exchange="CME",
                    symbol="NQU6",
                    net_quantity=position_quantity,
                    average_open_fill_price="20000.25",
                    open_pnl="0",
                )
            )
        elif scenario == "other_strategy_working_residual":
            positions.append(
                SimpleNamespace(
                    exchange="CME",
                    symbol="NQU6",
                    net_quantity="1",
                    average_open_fill_price="20000.25",
                    open_pnl="0",
                )
            )
        return SimpleNamespace(
            account_id="ACCOUNT",
            account_currency="USD",
            orders=(
                [remote]
                if (
                    working
                    and scenario
                    in {
                        "partial_fill",
                        "native_child_parent_tag",
                        "native_child_absent_tag",
                    }
                )
                or scenario.startswith("other_strategy_working")
                else []
            ),
            order_history=history,
            fills=[fill] if scenario == "partial_fill" else [],
            positions=positions,
            account_summary=SimpleNamespace(account_balance="100000"),
        )

    if scenario == "partial_fill":
        snapshots = [
            snapshot(status="PARTIALLY_FILLED", notification="FILL", working=True),
            snapshot(status="CANCELLED", notification="CANCEL", working=False),
            snapshot(
                status="CANCELLED",
                notification="CANCEL",
                working=False,
                position_quantity="0",
            ),
        ]
        expected_failure = "post_cancel_reconciliation_blocked"
    elif scenario.startswith("native_child"):
        snapshots = [
            snapshot(status="OPEN", notification="OPEN", working=True),
            snapshot(status="CANCELLED", notification="CANCEL", working=False),
        ]
        expected_failure = None
    elif scenario == "tagged_new":
        snapshots = [snapshot(status="OPEN", notification="OPEN", working=False)]
        expected_failure = "preflight_reconciliation_blocked"
    elif scenario.startswith("other_strategy_working"):
        snapshots = [snapshot(status="OPEN", notification="OPEN", working=True)]
        expected_failure = "working_orders_remain"
    else:
        snapshots = [snapshot(status="OPEN", notification="OPEN", working=True)]
        expected_failure = "working_orders_remain"
    fail_order = MagicMock(wraps=engine.order_manager.fail_order)
    engine.order_manager.fail_order = fail_order
    reconciliation_results = []
    reconcile_owned_orders = engine.reconcile_owned_orders

    def record_reconciliation(**kwargs):
        result = reconcile_owned_orders(**kwargs)
        reconciliation_results.append(result)
        return result

    engine.reconcile_owned_orders = record_reconciliation

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=snapshots,
    ) as loader:
        if expected_failure is None:
            result = service.execute(_signal(), _decision())
            assert result["status"] == "verified_flat"
        else:
            with pytest.raises(RuntimeError, match=expected_failure):
                service.execute(_signal(), _decision())

    if scenario == "partial_fill":
        assert [result["auto_resume_safe"] for result in reconciliation_results] == [
            True,
            True,
            False,
        ]
        persisted = repository.get_order("owned-parent")
        assert persisted is not None
        assert persisted.status == OrderStatus.CANCELLED.value
        assert persisted.exchange_order_id == "basket-parent"
        assert persisted.filled_quantity == Decimal("1")
        with factory() as session:
            trades = session.query(StoredTrade).filter_by(order_id="owned-parent").all()
        assert len(trades) == 1
        assert trades[0].quantity == Decimal("1")
        assert trades[0].price == Decimal("20000.25")
        service_adapter.cancel_order.assert_called_once_with(
            "basket-parent", PRODUCT, order_type="market"
        )
        engine.exit_authoritative_position.assert_called_once_with(
            PRODUCT, account_id="ACCOUNT"
        )
        assert loader.call_count == 3
        publisher.replace_positions_for_products.assert_not_called()
    elif scenario.startswith("native_child"):
        assert [result["auto_resume_safe"] for result in reconciliation_results] == [
            True,
            True,
        ]
        persisted = repository.get_order("owned-child")
        assert persisted is not None
        assert persisted.status == OrderStatus.CANCELLED.value
        assert persisted.exchange_order_id == "basket-child"
        service_adapter.cancel_order.assert_called_once_with(
            "basket-child", PRODUCT, order_type="stop_loss"
        )
        engine.exit_authoritative_position.assert_not_called()
        assert loader.call_count == 2
        publisher.replace_positions_for_products.assert_called_once()
    elif scenario == "tagged_new":
        assert reconciliation_results[0]["auto_resume_safe"] is False
        persisted = repository.get_order("owned-parent")
        assert persisted is not None
        assert persisted.status == OrderStatus.NEW.value
        assert persisted.exchange_order_id is None
        assert fail_order.call_count == 0
        service_adapter.cancel_order.assert_not_called()
        engine.exit_authoritative_position.assert_not_called()
        assert loader.call_count == 1
        publisher.replace_positions_for_products.assert_not_called()
    elif scenario.startswith("other_strategy_working"):
        assert reconciliation_results[0]["auto_resume_safe"] is True
        persisted = repository.get_order("other-owned-order")
        assert persisted is not None
        assert persisted.status == OrderStatus.SUBMITTED.value
        assert fail_order.call_count == 0
        service_adapter.cancel_order.assert_not_called()
        engine.exit_authoritative_position.assert_not_called()
        assert loader.call_count == 1
        publisher.replace_positions_for_products.assert_not_called()


@pytest.mark.parametrize(
    ("mode", "expected"),
    (
        ("identity", "cancel_identity_missing"),
        ("lookup", "cancel_identity_ambiguous"),
        ("cancel", "cancel_failed"),
    ),
)
def test_cancel_failures_preserve_exact_primary_reason(
    mode: str,
    expected: str,
) -> None:
    service, dependencies = _service()
    order = SimpleNamespace(
        id="order-1",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        client_order_id=None if mode == "identity" else "client-1",
        type="stop_loss",
        exchange_id="RITHMIC",
        account_profile="test",
        account_id="ACCOUNT",
        exchange_order_id=None if mode == "identity" else "basket-1",
    )
    dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
        order
    ]
    if mode == "cancel":
        dependencies.adapter.cancel_order.return_value = False
    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        return_value=(
            _snapshot(orders=[_remote_order("basket-1")])
            if mode == "cancel"
            else _snapshot()
        ),
    ):
        with pytest.raises(RuntimeError, match=expected):
            service.execute(_signal(), _decision())

    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.restart_order_stream.assert_called_once_with()
    dependencies.lockdown.assert_called_once_with(
        "rithmic_strategy_exit_requires_reconciliation:RuntimeError"
    )


@pytest.mark.parametrize("failure", ("working_order", "unsafe_reconciliation"))
def test_preflight_failures_prevent_native_exit(failure: str) -> None:
    service, dependencies = _service()
    snapshot = _snapshot(
        positions=[_position()],
        orders=[_working_order()] if failure == "working_order" else [],
    )
    if failure == "unsafe_reconciliation":
        dependencies.execution_engine.reconcile_owned_orders.return_value = {
            "auto_resume_safe": False
        }
    else:
        dependencies.execution_engine.reconcile_owned_orders.return_value = {
            "auto_resume_safe": False
        }

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            return_value=snapshot,
        ),
        pytest.raises(RuntimeError),
    ):
        service.execute(_signal(), _decision())

    dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    dependencies.account_service.replace_positions_for_products.assert_not_called()


@pytest.mark.parametrize(
    "position",
    (
        _position(side=PositionSide.SHORT),
        _position(quantity="2"),
    ),
)
def test_position_drift_prevents_native_exit(position: Position) -> None:
    service, dependencies = _service()

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            return_value=_snapshot(positions=[position]),
        ),
        pytest.raises(RuntimeError, match="rithmic_strategy_exit_position_drift"),
    ):
        service.execute(_signal(), _decision())

    dependencies.execution_engine.exit_authoritative_position.assert_not_called()


@pytest.mark.parametrize("remaining", ("working_order", "position"))
def test_verification_uses_one_snapshot_before_failing(remaining: str) -> None:
    service, dependencies = _service()
    target = SimpleNamespace(
        id="order-1",
        strategy_id="strategy",
        product_id=PRODUCT,
        status=OrderStatus.SUBMITTED.value,
        type="stop_loss",
        exchange_id="RITHMIC",
        account_profile="test",
        account_id="ACCOUNT",
        exchange_order_id="basket-1",
    )
    if remaining == "working_order":
        dependencies.execution_engine.order_manager.repo.list_orders_by_statuses.return_value = [
            target
        ]
    preflight = _snapshot(
        positions=[_position()] if remaining == "position" else [],
        orders=[_remote_order("basket-1")] if remaining == "working_order" else [],
    )
    unresolved = _snapshot(
        positions=[_position()] if remaining == "position" else [],
        orders=[_remote_order("basket-1")] if remaining == "working_order" else [],
    )
    if remaining == "working_order":
        dependencies.adapter.cancel_order.return_value = True

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            side_effect=[preflight, unresolved],
        ) as loader,
        pytest.raises(RuntimeError),
    ):
        service.execute(_signal(), _decision())

    assert loader.call_count == 2
    assert dependencies.execution_engine.reconcile_owned_orders.call_count == 2
    if remaining == "working_order":
        dependencies.adapter.cancel_order.assert_called_once_with(
            "basket-1",
            PRODUCT,
            order_type="stop_loss",
        )
        dependencies.account_service.replace_positions_for_products.assert_not_called()
        dependencies.execution_engine.exit_authoritative_position.assert_not_called()
    else:
        dependencies.account_service.replace_positions_for_products.assert_not_called()
        dependencies.execution_engine.exit_authoritative_position.assert_called_once_with(
            PRODUCT,
            account_id="ACCOUNT",
        )


def test_snapshot_filters_rithmic_orders_and_uses_integer_clock() -> None:
    service, dependencies = _service()
    rithmic_order = SimpleNamespace(exchange_id="RITHMIC")
    other_order = SimpleNamespace(exchange_id="BINANCE")
    dependencies.execution_engine.list_recoverable_client_orders.return_value = [
        other_order,
        rithmic_order,
    ]

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        return_value=_snapshot(),
    ) as loader:
        service.execute(_signal(), _decision())

    loader.assert_called_once_with("test", "ACCOUNT", [rithmic_order], 1_704_067_200)


@pytest.mark.parametrize("body_primary", (False, True))
@pytest.mark.parametrize("finalizer_failure", (None, "leadership", "start"))
def test_body_primary_and_finalizer_failure_precedence(
    body_primary: bool,
    finalizer_failure: str | None,
) -> None:
    service, dependencies = _service()
    primary = RuntimeError("body-primary")
    finalizer = RuntimeError("finalizer-failure")
    if body_primary:
        dependencies.execution_engine.reconcile_owned_orders.side_effect = primary
        leadership_calls_before_finalizer = 1
    else:
        leadership_calls_before_finalizer = 2
    if finalizer_failure == "leadership":
        dependencies.assert_leadership.side_effect = [
            *[None for _ in range(leadership_calls_before_finalizer)],
            finalizer,
        ]
    elif finalizer_failure == "start":
        dependencies.restart_order_stream.side_effect = finalizer

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        return_value=_snapshot(),
    ):
        if body_primary:
            with pytest.raises(RuntimeError) as caught:
                service.execute(_signal(), _decision())
            assert caught.value is primary
        elif finalizer_failure is not None:
            with pytest.raises(
                RuntimeError,
                match="rithmic_strategy_exit_order_stream_restart_failed",
            ) as caught:
                service.execute(_signal(), _decision())
            assert caught.value is not finalizer
        else:
            assert service.execute(_signal(), _decision())["status"] == "verified_flat"

    if finalizer_failure is None:
        dependencies.restart_order_stream.assert_called_once_with()
        dependencies.adapter.close.assert_called()
    else:
        dependencies.adapter.close.assert_called()
        assert dependencies.lockdown.call_args_list[-1] == call(
            "rithmic_strategy_exit_order_stream_restart_failed"
        )
        if finalizer_failure == "leadership":
            dependencies.restart_order_stream.assert_not_called()
        else:
            dependencies.restart_order_stream.assert_called_once_with()
