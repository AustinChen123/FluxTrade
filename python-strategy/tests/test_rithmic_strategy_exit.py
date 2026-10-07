from __future__ import annotations

from decimal import Decimal
import logging
from types import SimpleNamespace
from typing import Any, Callable
from unittest.mock import MagicMock, call, patch

import pytest

from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)
from src.core.adapters.rithmic_portfolio_exit import RithmicPortfolioExitService
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
    ledger_positions: Callable[[], list[Position]] | None = None,
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
        get_all_positions=(
            ledger_positions
            if ledger_positions is not None
            else lambda: [
                position
                for side in ("BUY", "SELL")
                if (position := repository.get_position(strategy_id, PRODUCT, side))
                is not None
            ]
        )
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


class _SyntheticPositionCache:
    """Store the Decimal projection supplied by OrderManager's Lua boundary."""

    def __init__(self) -> None:
        self.positions: dict[tuple[str, str], tuple[Decimal, Decimal]] = {}

    def set_position(
        self,
        strategy_id: str,
        product_id: str,
        quantity: Decimal,
        entry_price: Decimal,
    ) -> None:
        self.positions[(strategy_id, product_id)] = (quantity, entry_price)

    def hgetall(self, key: str) -> dict[str, str]:
        prefix = "state:position:"
        strategy_id, product_id = key[len(prefix) :].split(":", 1)
        value = self.positions.get((strategy_id, product_id))
        if value is None:
            return {}
        quantity, entry_price = value
        return {"quantity": str(quantity), "entry_price": str(entry_price)}

    def store_script_projection(self, *, args: list[str]) -> str:
        strategy_id, product_id = args[:2]
        self.set_position(
            strategy_id,
            product_id,
            Decimal(args[8]),
            Decimal(args[9]),
        )
        return "OK"

    def get_position_for_exit(
        self,
        strategy_id: str,
        product_id: str,
    ) -> Position | None:
        value = self.positions.get((strategy_id, product_id))
        if value is None:
            return None
        quantity, entry_price = value
        if quantity == 0:
            return None
        return Position(
            strategy_id=strategy_id,
            product_id=product_id,
            side=PositionSide.LONG if quantity > 0 else PositionSide.SHORT,
            quantity=abs(quantity),
            entry_price=entry_price,
            unrealized_pnl=Decimal("0"),
        )

    def get_all_positions(self) -> list[Position]:
        positions = []
        for strategy_id, product_id in self.positions:
            position = self.get_position_for_exit(strategy_id, product_id)
            if position is not None:
                positions.append(position)
        return positions


def _actual_sql_portfolio_exit_service(adapter, engine):
    return RithmicPortfolioExitService(
        adapter=adapter,
        execution_engine=engine,
        account_service=SimpleNamespace(
            get_position_for_exit=lambda *_args: _position()
        ),
        profile="test",
        account_id="ACCOUNT",
        operation_gate=RithmicOrderEventLifecycleGate(),
        stop_order_event_stream=MagicMock(return_value=True),
        assert_leadership=MagicMock(),
        restart_order_stream=MagicMock(),
        lockdown=MagicMock(),
        schedule_emergency_flatten=MagicMock(),
        portfolio_id_for_sleeve=lambda _strategy_id: "portfolio",
    )


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


def test_portfolio_exit_uses_sql_order_after_owned_partial_fill_reconciliation(
    sqlite_order_session_factory,
    order_factory,
    mock_clock,
) -> None:
    order = order_factory(
        order_id="owned-parent",
        strategy_id="strategy",
        product_id=PRODUCT,
        exchange_id="RITHMIC",
        order_type="market",
        side="buy",
        quantity=Decimal("2"),
        status=OrderStatus.SUBMITTED.value,
        timestamp=1_700_000_000_000,
        client_order_id="flux-parent",
        exchange_order_id="basket-parent",
        account_profile="test",
        account_id="ACCOUNT",
    )
    _, repository, adapter, engine, _ = _actual_sql_exit_runtime(
        sqlite_order_session_factory,
        mock_clock,
        order=order,
    )
    remote = SimpleNamespace(
        basket_id="basket-parent",
        exchange_order_id="exchange-parent",
        original_basket_id=None,
        client_order_id=None,
        transaction_type="BUY",
        exchange="CME",
        symbol="NQU6",
        quantity="2",
        filled_quantity="1",
        unfilled_quantity="1",
        average_fill_price="20000.25",
        status="PARTIALLY_FILLED",
        notification_type="FILL",
        price=None,
        price_type=None,
        trigger_price=None,
        bracket_type=None,
        timestamp_ms=1_700_000_001_000,
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
    snapshot = SimpleNamespace(
        account_id="ACCOUNT",
        account_currency="USD",
        orders=[remote],
        order_history=[remote],
        fills=[fill],
        positions=[
            SimpleNamespace(
                exchange="CME",
                symbol="NQU6",
                net_quantity="1",
                average_open_fill_price="20000.25",
                open_pnl="0",
            )
        ],
        account_summary=SimpleNamespace(account_balance="100000"),
    )

    reconciliation = engine.reconcile_owned_orders(
        snapshot_loader=lambda *_args, **_kwargs: snapshot
    )

    assert reconciliation["auto_resume_safe"] is True
    persisted = repository.get_order("owned-parent")
    assert persisted is not None
    assert persisted.status == OrderStatus.PARTIALLY_FILLED.value
    assert persisted.exchange_order_id == "basket-parent"
    assert persisted.filled_quantity == Decimal("1")
    with sqlite_order_session_factory() as session:
        trades = session.query(StoredTrade).filter_by(order_id="owned-parent").all()
    assert len(trades) == 1
    assert trades[0].quantity == Decimal("1")
    assert trades[0].price == Decimal("20000.25")

    adapter.get_order_by_client_id = MagicMock()
    portfolio_exit = _actual_sql_portfolio_exit_service(adapter, engine)
    cancelled_count, cancelled_baskets = portfolio_exit._cancel_strategy_orders(
        _signal(),
        snapshot,
        mark_compensation_required=MagicMock(),
    )

    assert cancelled_count == 1
    assert cancelled_baskets == {"basket-parent"}
    adapter.cancel_order.assert_called_once_with(
        "basket-parent",
        PRODUCT,
        order_type="market",
    )
    adapter.get_order_by_client_id.assert_not_called()


def test_portfolio_exit_actual_reconciler_blocks_unsafe_sql_order_snapshot(
    sqlite_order_session_factory,
    order_factory,
    mock_clock,
) -> None:
    order = order_factory(
        order_id="owned-parent",
        strategy_id="strategy",
        product_id=PRODUCT,
        exchange_id="RITHMIC",
        order_type="market",
        side="buy",
        quantity=Decimal("2"),
        status=OrderStatus.SUBMITTED.value,
        timestamp=1_700_000_000_000,
        client_order_id="flux-parent",
        exchange_order_id="basket-parent",
        account_profile="test",
        account_id="ACCOUNT",
    )
    _, repository, adapter, engine, _ = _actual_sql_exit_runtime(
        sqlite_order_session_factory,
        mock_clock,
        order=order,
    )
    remote = SimpleNamespace(
        basket_id="basket-parent",
        exchange_order_id="exchange-parent",
        original_basket_id=None,
        client_order_id="flux-parent",
        transaction_type="BUY",
        exchange="ICE",
        symbol="NQU6",
        quantity="2",
        filled_quantity="0",
        unfilled_quantity="2",
        average_fill_price=None,
        status="OPEN",
        notification_type="OPEN",
        price=None,
        price_type=None,
        trigger_price=None,
        bracket_type=None,
        timestamp_ms=1_700_000_001_000,
        completion_reason=None,
    )
    snapshot = SimpleNamespace(
        account_id="ACCOUNT",
        account_currency="USD",
        orders=[remote],
        order_history=[remote],
        fills=[],
        positions=[
            SimpleNamespace(
                exchange="CME",
                symbol="NQU6",
                net_quantity="1",
                average_open_fill_price="20000.25",
                open_pnl="0",
            )
        ],
        account_summary=SimpleNamespace(account_balance="100000"),
    )
    portfolio_exit = _actual_sql_portfolio_exit_service(adapter, engine)
    portfolio_exit._load_snapshot = MagicMock(return_value=snapshot)
    adapter.cancel_order = MagicMock(return_value=True)
    reconciliation_results = []
    reconcile_owned_orders = engine.reconcile_owned_orders

    def record_reconciliation(**kwargs):
        result = reconcile_owned_orders(**kwargs)
        reconciliation_results.append(result)
        return result

    engine.reconcile_owned_orders = record_reconciliation

    with pytest.raises(
        RuntimeError,
        match="rithmic_portfolio_exit_preflight_reconciliation_blocked",
    ):
        portfolio_exit._verified_preflight_position(
            _signal(),
            expected_side="LONG",
            exit_quantity=Decimal("1"),
        )

    persisted = repository.get_order("owned-parent")
    assert persisted is not None
    assert persisted.status == OrderStatus.SUBMITTED.value
    assert persisted.exchange_order_id == "basket-parent"
    assert len(reconciliation_results) == 6
    assert all(result["auto_resume_safe"] is False for result in reconciliation_results)
    adapter.cancel_order.assert_not_called()


@pytest.mark.parametrize("remote_child_tag", [None, "entry-parent-tag"])
def test_portfolio_exit_cancels_reconciled_native_child_without_child_tag(
    sqlite_order_session_factory,
    order_factory,
    mock_clock,
    remote_child_tag,
) -> None:
    order = order_factory(
        order_id="owned-child",
        strategy_id="strategy",
        product_id=PRODUCT,
        exchange_id="RITHMIC",
        order_type="stop_loss",
        side="sell",
        quantity=Decimal("1"),
        status=OrderStatus.SUBMITTED.value,
        timestamp=1_700_000_000_000,
        client_order_id="local-child-tag",
        exchange_order_id=None,
        account_profile="test",
        account_id="ACCOUNT",
    )
    order.intent_payload = {
        "placement_mode": "attach-at-entry",
        "native_leg_type": "stop_loss",
        "native_parent_basket_id": "basket-parent",
        "native_parent_client_order_id": "entry-parent-tag",
    }
    _, repository, adapter, engine, _ = _actual_sql_exit_runtime(
        sqlite_order_session_factory,
        mock_clock,
        order=order,
    )
    remote = SimpleNamespace(
        basket_id="basket-child",
        exchange_order_id="exchange-child",
        original_basket_id="basket-parent",
        client_order_id=remote_child_tag,
        transaction_type="SELL",
        exchange="CME",
        symbol="NQU6",
        quantity="1",
        filled_quantity="0",
        unfilled_quantity="1",
        average_fill_price=None,
        status="OPEN",
        notification_type="OPEN",
        price=None,
        price_type="stop_market",
        trigger_price="19998.25",
        bracket_type=None,
        timestamp_ms=1_700_000_001_000,
        completion_reason=None,
    )
    snapshot = SimpleNamespace(
        account_id="ACCOUNT",
        account_currency="USD",
        orders=[remote],
        order_history=[remote],
        fills=[],
        positions=[],
        account_summary=SimpleNamespace(account_balance="100000"),
    )
    reconciliation = engine.reconcile_owned_orders(
        snapshot_loader=lambda *_args, **_kwargs: snapshot
    )
    assert reconciliation["auto_resume_safe"] is True
    persisted = repository.get_order("owned-child")
    assert persisted is not None
    assert persisted.exchange_order_id == "basket-child"

    adapter.get_order_by_client_id = MagicMock()
    portfolio_exit = _actual_sql_portfolio_exit_service(adapter, engine)
    cancelled_count, cancelled_baskets = portfolio_exit._cancel_strategy_orders(
        _signal(),
        snapshot,
        mark_compensation_required=MagicMock(),
    )

    assert cancelled_count == 1
    assert cancelled_baskets == {"basket-child"}
    adapter.cancel_order.assert_called_once_with(
        "basket-child",
        PRODUCT,
        order_type="stop_loss",
    )
    adapter.get_order_by_client_id.assert_not_called()


def test_portfolio_execute_reconciles_protective_sell_and_repairs_child_basket(
    sqlite_order_session_factory,
    order_factory,
    mock_clock,
) -> None:
    position_cache = _SyntheticPositionCache()
    position_cache.set_position("strategy", PRODUCT, Decimal("3"), Decimal("20000.25"))
    position_cache.set_position("other-sleeve", PRODUCT, Decimal("2"), Decimal("20001"))
    protective = order_factory(
        order_id="protective-sell",
        strategy_id="strategy",
        product_id=PRODUCT,
        exchange_id="RITHMIC",
        order_type="stop_loss",
        side="sell",
        quantity=Decimal("2"),
        status=OrderStatus.SUBMITTED.value,
        timestamp=1_700_000_000_000,
        client_order_id="protective-parent",
        exchange_order_id="protective-basket",
        account_profile="test",
        account_id="ACCOUNT",
    )
    child = order_factory(
        order_id="native-child",
        strategy_id="strategy",
        product_id=PRODUCT,
        exchange_id="RITHMIC",
        order_type="stop_loss",
        side="sell",
        quantity=Decimal("1"),
        status=OrderStatus.SUBMITTED.value,
        timestamp=1_700_000_000_001,
        client_order_id="local-child-tag",
        exchange_order_id=None,
        account_profile="test",
        account_id="ACCOUNT",
    )
    child.intent_payload = {
        "placement_mode": "attach-at-entry",
        "native_leg_type": "stop_loss",
        "native_parent_basket_id": "parent-basket",
        "native_parent_client_order_id": "parent-tag",
    }
    _, repository, adapter, engine, _ = _actual_sql_exit_runtime(
        sqlite_order_session_factory,
        mock_clock,
        order=protective,
        ledger_positions=position_cache.get_all_positions,
    )
    repository.add_order(child)
    engine.order_manager.is_backtest = False
    engine.order_manager.redis_client = position_cache
    engine.order_manager.update_position_script = position_cache.store_script_projection

    def remote_order(
        basket_id: str,
        *,
        status: str,
        notification: str,
        quantity: str,
        filled: str,
        unfilled: str,
        transaction_type: str = "SELL",
        original_basket_id: str | None = None,
        client_order_id: str | None = None,
        timestamp_ms: int = 1_700_000_001_000,
    ) -> SimpleNamespace:
        return SimpleNamespace(
            basket_id=basket_id,
            exchange_order_id=f"exchange-{basket_id}",
            original_basket_id=original_basket_id,
            client_order_id=client_order_id,
            transaction_type=transaction_type,
            exchange="CME",
            symbol="NQU6",
            quantity=quantity,
            filled_quantity=filled,
            unfilled_quantity=unfilled,
            average_fill_price="20000.25" if filled != "0" else None,
            status=status,
            notification_type=notification,
            price=None,
            price_type="stop_market",
            trigger_price="19998.25",
            bracket_type=None,
            timestamp_ms=timestamp_ms,
            completion_reason=None,
        )

    protective_filled = remote_order(
        "protective-basket",
        status="FILLED",
        notification="FILL",
        quantity="2",
        filled="2",
        unfilled="0",
        timestamp_ms=1_700_000_001_000,
    )
    child_open = remote_order(
        "repaired-child-basket",
        status="OPEN",
        notification="OPEN",
        quantity="1",
        filled="0",
        unfilled="1",
        original_basket_id="parent-basket",
        timestamp_ms=1_700_000_001_000,
    )
    child_cancelled = remote_order(
        "repaired-child-basket",
        status="CANCELLED",
        notification="CANCEL",
        quantity="1",
        filled="0",
        unfilled="1",
        original_basket_id="parent-basket",
        timestamp_ms=1_700_000_002_000,
    )
    fill = SimpleNamespace(
        basket_id="protective-basket",
        exchange_order_id="exchange-protective-basket",
        fill_id="protective-fill-1",
        exchange="CME",
        symbol="NQU6",
        transaction_type="SELL",
        fill_quantity="2",
        fill_price="20000.25",
        timestamp_ms=1_700_000_001_000,
    )

    def snapshot(orders, order_history, net_quantity: str):
        return SimpleNamespace(
            account_id="ACCOUNT",
            account_currency="USD",
            orders=orders,
            order_history=order_history,
            fills=[fill],
            positions=[
                SimpleNamespace(
                    exchange="CME",
                    symbol="NQU6",
                    net_quantity=net_quantity,
                    average_open_fill_price="20000.25",
                    open_pnl="0",
                )
            ],
            account_summary=SimpleNamespace(account_balance="100000"),
        )

    unsafe_snapshot = snapshot([], [], "3")
    unsafe_snapshot.account_id = "OTHER-ACCOUNT"
    snapshots = [
        unsafe_snapshot,
        snapshot([child_open], [protective_filled, child_open], "3"),
        snapshot(
            [],
            [protective_filled, child_open, child_cancelled],
            "3",
        ),
        snapshot(
            [],
            [protective_filled, child_open, child_cancelled],
            "2",
        ),
    ]
    service = _actual_sql_portfolio_exit_service(adapter, engine)
    service.account_service.get_position_for_exit = position_cache.get_position_for_exit
    exit_order_ids: list[str] = []

    def submit_reduction(signal, decision, *, candle, preflight_remote_quantity):
        exit_order = order_factory(
            order_id="portfolio-exit",
            strategy_id=signal.strategy_id,
            product_id=signal.product_id,
            exchange_id="RITHMIC",
            order_type="market",
            side="sell",
            quantity=decision.quantity,
            status=OrderStatus.SUBMITTED.value,
            timestamp=1_700_000_003_000,
            client_order_id="portfolio-exit-tag",
            exchange_order_id="portfolio-exit-basket",
            account_profile="test",
            account_id="ACCOUNT",
        )
        repository.add_order(exit_order)
        engine.order_manager.record_fill_delta(
            exit_order,
            Decimal("20000.25"),
            decision.quantity,
            decision.quantity,
            Decimal("20000.25"),
            terminal_status=OrderStatus.FILLED,
        )
        exit_order_ids.append(str(exit_order.id))
        return str(exit_order.id)

    engine.submit_verified_net_reduction = MagicMock(side_effect=submit_reduction)
    engine.record_verified_net_reduction = MagicMock()
    reconcile_results = []
    positions_after_reconciliation = []
    order_state_after_reconciliation = []
    reconcile = engine.reconcile_owned_orders

    def record_reconciliation(**kwargs):
        result = reconcile(**kwargs)
        reconcile_results.append(result)
        positions_after_reconciliation.append(
            position_cache.get_position_for_exit("strategy", PRODUCT)
        )
        order_state_after_reconciliation.append(
            (
                repository.get_order("protective-sell"),
                repository.get_order("native-child"),
            )
        )
        return result

    engine.reconcile_owned_orders = record_reconciliation
    active_row_queries = []
    list_orders = repository.list_orders_by_statuses

    def record_active_rows(statuses):
        rows = list_orders(statuses)
        active_row_queries.append(
            (
                len(reconcile_results),
                [(row.id, row.status, row.exchange_order_id) for row in rows],
            )
        )
        return rows

    snapshot_index = 0

    def load_snapshot(*_args, **_kwargs):
        nonlocal snapshot_index
        snapshot = snapshots[min(snapshot_index, len(snapshots) - 1)]
        snapshot_index += 1
        return snapshot

    with (
        patch(
            "src.core.adapters.rithmic_portfolio_exit.load_rithmic_recovery_snapshot",
            side_effect=load_snapshot,
        ) as loader,
        patch.object(
            repository,
            "list_orders_by_statuses",
            side_effect=record_active_rows,
        ),
    ):
        result = service.execute(_signal(), _decision(), candle=None)

    persisted_protective = repository.get_order("protective-sell")
    persisted_child = repository.get_order("native-child")
    assert persisted_protective is not None
    assert persisted_protective.status == OrderStatus.FILLED.value
    assert persisted_protective.filled_quantity == Decimal("2")
    assert persisted_child is not None
    assert persisted_child.status == OrderStatus.CANCELLED.value
    assert persisted_child.exchange_order_id == "repaired-child-basket"
    with sqlite_order_session_factory() as session:
        trades = session.query(StoredTrade).filter_by(order_id="protective-sell").all()
    assert len(trades) == 1
    assert trades[0].quantity == Decimal("2")
    assert trades[0].price == Decimal("20000.25")
    assert [
        None if position is None else (position.side, position.quantity)
        for position in positions_after_reconciliation
    ] == [
        (PositionSide.LONG, Decimal("3")),
        (PositionSide.LONG, Decimal("1")),
        (PositionSide.LONG, Decimal("1")),
        None,
    ]
    assert [result["auto_resume_safe"] for result in reconcile_results] == [
        False,
        True,
        True,
        True,
    ]
    unsafe_protective, unsafe_child = order_state_after_reconciliation[0]
    assert unsafe_protective is not None
    assert unsafe_protective.status == OrderStatus.SUBMITTED.value
    assert unsafe_protective.filled_quantity == Decimal("0")
    assert unsafe_child is not None
    assert unsafe_child.status == OrderStatus.SUBMITTED.value
    assert unsafe_child.exchange_order_id is None
    repaired_protective, repaired_child = order_state_after_reconciliation[1]
    assert repaired_protective is not None
    assert repaired_protective.status == OrderStatus.FILLED.value
    assert repaired_child is not None
    assert repaired_child.exchange_order_id == "repaired-child-basket"
    other_position = position_cache.get_position_for_exit("other-sleeve", PRODUCT)
    assert other_position is not None
    assert other_position.quantity == Decimal("2")
    assert len(active_row_queries) == 1
    assert active_row_queries[0][0] == 2
    assert {row[0] for row in active_row_queries[0][1]} == {"native-child"}
    assert active_row_queries[0][1][0][2] == "repaired-child-basket"
    assert result["status"] == "verified_portfolio_reduction"
    assert loader.call_count == 4
    assert adapter.cancel_order.call_args_list == [
        call("repaired-child-basket", PRODUCT, order_type="stop_loss"),
    ]
    engine.submit_verified_net_reduction.assert_called_once_with(
        _signal(),
        _decision(),
        candle=None,
        preflight_remote_quantity=Decimal("3"),
    )
    assert exit_order_ids == ["portfolio-exit"]
    with sqlite_order_session_factory() as session:
        exit_trades = (
            session.query(StoredTrade).filter_by(order_id="portfolio-exit").all()
        )
    assert len(exit_trades) == 1
    assert exit_trades[0].quantity == Decimal("1")


def test_portfolio_execute_blocks_reduction_while_cancelled_basket_works(
    sqlite_order_session_factory,
    order_factory,
    mock_clock,
) -> None:
    order = order_factory(
        order_id="owned-stop",
        strategy_id="strategy",
        product_id=PRODUCT,
        exchange_id="RITHMIC",
        order_type="stop_loss",
        side="sell",
        quantity=Decimal("1"),
        status=OrderStatus.SUBMITTED.value,
        timestamp=1_700_000_000_000,
        client_order_id="owned-parent-tag",
        exchange_order_id="owned-basket",
        account_profile="test",
        account_id="ACCOUNT",
    )
    _, repository, adapter, engine, _ = _actual_sql_exit_runtime(
        sqlite_order_session_factory,
        mock_clock,
        order=order,
        ledger_positions=lambda: [
            Position(
                strategy_id="strategy",
                product_id=PRODUCT,
                side=PositionSide.LONG,
                quantity=Decimal("3"),
                entry_price=Decimal("20000.25"),
                unrealized_pnl=Decimal("0"),
            )
        ],
    )
    remote = SimpleNamespace(
        basket_id="owned-basket",
        exchange_order_id="exchange-owned-basket",
        original_basket_id=None,
        client_order_id="owned-parent-tag",
        transaction_type="SELL",
        exchange="CME",
        symbol="NQU6",
        quantity="1",
        filled_quantity="0",
        unfilled_quantity="1",
        average_fill_price=None,
        status="OPEN",
        notification_type="OPEN",
        price=None,
        price_type="stop_market",
        trigger_price="19998.25",
        bracket_type=None,
        timestamp_ms=1_700_000_001_000,
        completion_reason=None,
    )
    snapshot = SimpleNamespace(
        account_id="ACCOUNT",
        account_currency="USD",
        orders=[remote],
        order_history=[remote],
        fills=[],
        positions=[
            SimpleNamespace(
                exchange="CME",
                symbol="NQU6",
                net_quantity="3",
                average_open_fill_price="20000.25",
                open_pnl="0",
            )
        ],
        account_summary=SimpleNamespace(account_balance="100000"),
    )
    service = _actual_sql_portfolio_exit_service(adapter, engine)
    service.account_service.get_position_for_exit = MagicMock(return_value=_position())
    engine.submit_verified_net_reduction = MagicMock(
        side_effect=AssertionError("working target must block reduction")
    )
    reconciliation_results = []
    reconcile = engine.reconcile_owned_orders

    def record_reconciliation(**kwargs):
        result = reconcile(**kwargs)
        reconciliation_results.append(result)
        return result

    engine.reconcile_owned_orders = record_reconciliation
    with (
        patch(
            "src.core.adapters.rithmic_portfolio_exit.load_rithmic_recovery_snapshot",
            return_value=snapshot,
        ) as loader,
        pytest.raises(
            RuntimeError,
            match="rithmic_portfolio_exit_preflight_reconciliation_blocked",
        ),
    ):
        service.execute(_signal(), _decision(), candle=None)

    persisted = repository.get_order("owned-stop")
    assert persisted is not None
    assert persisted.status == OrderStatus.SUBMITTED.value
    assert persisted.exchange_order_id == "owned-basket"
    assert len(reconciliation_results) == 7
    assert all(
        result["auto_resume_safe"] is True for result in reconciliation_results
    ), reconciliation_results
    assert loader.call_count == 7
    adapter.cancel_order.assert_called_once_with(
        "owned-basket", PRODUCT, order_type="stop_loss"
    )
    engine.submit_verified_net_reduction.assert_not_called()


def test_portfolio_execute_ignores_other_sleeve_basket_after_target_terminal(
    sqlite_order_session_factory,
    order_factory,
    mock_clock,
) -> None:
    target = order_factory(
        order_id="owned-stop",
        strategy_id="strategy",
        product_id=PRODUCT,
        exchange_id="RITHMIC",
        order_type="stop_loss",
        side="sell",
        quantity=Decimal("1"),
        status=OrderStatus.SUBMITTED.value,
        timestamp=1_700_000_000_000,
        client_order_id="owned-child-tag",
        exchange_order_id="owned-basket",
        account_profile="test",
        account_id="ACCOUNT",
    )
    _, repository, adapter, engine, _ = _actual_sql_exit_runtime(
        sqlite_order_session_factory,
        mock_clock,
        order=target,
    )
    with sqlite_order_session_factory() as session:
        if session.get(StoredStrategy, "other-sleeve") is None:
            session.add(StoredStrategy(id="other-sleeve", name="Other Sleeve"))
            session.commit()
    other = order_factory(
        order_id="other-stop",
        strategy_id="other-sleeve",
        product_id=PRODUCT,
        exchange_id="RITHMIC",
        order_type="stop_loss",
        side="sell",
        quantity=Decimal("1"),
        status=OrderStatus.SUBMITTED.value,
        timestamp=1_700_000_000_001,
        client_order_id="other-child-tag",
        exchange_order_id="other-basket",
        account_profile="test",
        account_id="ACCOUNT",
    )
    repository.add_order(other)

    def remote_order(
        basket_id: str,
        status: str,
        notification: str,
        *,
        client_order_id: str,
        timestamp_ms: int,
    ):
        return SimpleNamespace(
            basket_id=basket_id,
            exchange_order_id=f"exchange-{basket_id}",
            original_basket_id="shared-parent-basket",
            client_order_id=client_order_id,
            transaction_type="SELL",
            exchange="CME",
            symbol="NQU6",
            quantity="1",
            filled_quantity="0",
            unfilled_quantity="1",
            average_fill_price=None,
            status=status,
            notification_type=notification,
            price=None,
            price_type="stop_market",
            trigger_price="19998.25",
            bracket_type=None,
            timestamp_ms=timestamp_ms,
            completion_reason=None,
        )

    target_open = remote_order(
        "owned-basket",
        "OPEN",
        "OPEN",
        client_order_id="owned-child-tag",
        timestamp_ms=1_700_000_001_000,
    )
    other_open = remote_order(
        "other-basket",
        "OPEN",
        "OPEN",
        client_order_id="owned-child-tag",
        timestamp_ms=1_700_000_001_000,
    )
    assert other_open.client_order_id == target_open.client_order_id
    assert other_open.basket_id != target_open.basket_id
    target_terminal = remote_order(
        "owned-basket",
        "CANCELLED",
        "CANCEL",
        client_order_id="owned-child-tag",
        timestamp_ms=1_700_000_002_000,
    )
    snapshots = [
        SimpleNamespace(
            account_id="ACCOUNT",
            account_currency="USD",
            orders=[target_open, other_open],
            order_history=[target_open, other_open],
            fills=[],
            positions=[
                SimpleNamespace(
                    exchange="CME",
                    symbol="NQU6",
                    net_quantity="3",
                    average_open_fill_price="20000.25",
                    open_pnl="0",
                )
            ],
            account_summary=SimpleNamespace(account_balance="100000"),
        ),
        SimpleNamespace(
            account_id="ACCOUNT",
            account_currency="USD",
            orders=[other_open],
            order_history=[target_open, target_terminal, other_open],
            fills=[],
            positions=[
                SimpleNamespace(
                    exchange="CME",
                    symbol="NQU6",
                    net_quantity="3",
                    average_open_fill_price="20000.25",
                    open_pnl="0",
                )
            ],
            account_summary=SimpleNamespace(account_balance="100000"),
        ),
        SimpleNamespace(
            account_id="ACCOUNT",
            account_currency="USD",
            orders=[other_open],
            order_history=[target_open, target_terminal, other_open],
            fills=[],
            positions=[
                SimpleNamespace(
                    exchange="CME",
                    symbol="NQU6",
                    net_quantity="2",
                    average_open_fill_price="20000.25",
                    open_pnl="0",
                )
            ],
            account_summary=SimpleNamespace(account_balance="100000"),
        ),
    ]
    service = _actual_sql_portfolio_exit_service(adapter, engine)
    local_position = _position()
    service.account_service.get_position_for_exit = MagicMock(
        side_effect=[local_position, local_position, None]
    )
    engine.submit_verified_net_reduction = MagicMock(return_value="exit-order")
    engine.record_verified_net_reduction = MagicMock()
    # Isolate the post-cancel basket identity gate: owned reconciliation can
    # independently block on unrelated account-wide observations.
    engine.reconcile_owned_orders = MagicMock(return_value={"auto_resume_safe": True})
    with patch(
        "src.core.adapters.rithmic_portfolio_exit.load_rithmic_recovery_snapshot",
        side_effect=snapshots,
    ) as loader:
        result = service.execute(_signal(), _decision(), candle=None)

    assert result["status"] == "verified_portfolio_reduction"
    assert loader.call_count == 3
    adapter.cancel_order.assert_called_once_with(
        "owned-basket", PRODUCT, order_type="stop_loss"
    )
    persisted_other = repository.get_order("other-stop")
    assert persisted_other is not None
    assert persisted_other.status == OrderStatus.SUBMITTED.value
    assert persisted_other.exchange_order_id == "other-basket"
    engine.submit_verified_net_reduction.assert_called_once_with(
        _signal(),
        _decision(),
        candle=None,
        preflight_remote_quantity=Decimal("3"),
    )


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
