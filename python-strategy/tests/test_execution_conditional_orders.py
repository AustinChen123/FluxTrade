from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, call

from src.core.execution import ExecutionEngine
from src.core.execution_conditional_orders import ConditionalOrderLifecycleOwner
from src.core.execution_submission_gate import ExecutionSubmissionGate
from src.core.interfaces.exchange import ExchangeError
from src.core.models import OrderStatus
from src.core.repositories import LiveOrderRepository


def _owner(*, manager=None, adapter=None, gate=None, **overrides):
    dependencies = {
        "order_manager": manager or MagicMock(),
        "adapter": adapter or MagicMock(),
        "submission_gate": gate or ExecutionSubmissionGate(MagicMock()),
        "pending_protection_fill_processor": MagicMock(return_value=None),
        "process_exchange_order_event": MagicMock(),
        "assert_external_operation_allowed": MagicMock(),
        "record_order_ack": MagicMock(),
        "write_warning": MagicMock(),
        "logger": MagicMock(),
    }
    dependencies.update(overrides)
    return ConditionalOrderLifecycleOwner(**dependencies)


def test_engine_facades_delegate_to_one_conditional_lifecycle_owner(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
    mock_order_repo,
    signal_factory,
):
    execution_engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
        is_backtest=True,
    )
    lifecycle = MagicMock()
    execution_engine._conditional_order_lifecycle = lifecycle
    signal = signal_factory(stop_loss=Decimal("99"))
    entry = SimpleNamespace(id="entry-1")
    conditional = SimpleNamespace(id="conditional-1")
    failures = [{"reason": "sentinel"}]
    lifecycle.create_orders.return_value = [conditional]
    lifecycle.place_pending_for_entry.return_value = failures
    lifecycle.place_orders.return_value = failures
    lifecycle.recover_pending_protection.return_value = {
        "pending_count": 1,
        "entries_attempted": 1,
        "failures": failures,
    }

    assert execution_engine._create_conditional_orders(
        signal,
        entry,
        Decimal("2.00"),
        None,
    ) == [conditional]
    assert (
        execution_engine._place_pending_conditional_orders_for_entry(entry) is failures
    )
    assert execution_engine._place_conditional_orders([conditional]) is failures
    assert execution_engine.place_pending_protection_for_filled_entries() == {
        "pending_count": 1,
        "entries_attempted": 1,
        "failures": failures,
    }
    lifecycle.create_orders.assert_called_once_with(
        signal=signal,
        entry_order=entry,
        quantity=Decimal("2.00"),
        candle=None,
        attach_min_notional_reference_price=(
            execution_engine._attach_min_notional_reference_price
        ),
    )
    lifecycle.place_pending_for_entry.assert_called_once_with(entry)
    lifecycle.place_orders.assert_called_once_with([conditional])
    lifecycle.recover_pending_protection.assert_called_once_with()


def test_submission_gate_rejects_before_repository_or_adapter_mutation():
    manager = MagicMock()
    adapter = MagicMock()
    gate = ExecutionSubmissionGate(MagicMock())
    gate.claim_reconcile_halt()
    owner = _owner(manager=manager, adapter=adapter, gate=gate)
    entry = SimpleNamespace(
        id="entry-1",
        type="market",
        filled_quantity=Decimal("2"),
    )

    assert owner.place_pending_for_entry(entry) == [
        {
            "order_id": "entry-1",
            "order_type": "market",
            "reason": "reconcile_halted",
        }
    ]
    assert gate.in_flight == 0
    manager.repo.list_orders_by_statuses.assert_not_called()
    adapter.place_order.assert_not_called()


def test_lifecycle_warning_delegates_exact_event_to_dynamic_audit_port():
    write_warning = MagicMock()
    owner = _owner(write_warning=write_warning)
    order = SimpleNamespace(id="entry-1")
    failures: list[dict[str, object]] = [{"reason": "placement-failed"}]

    owner.write_warning(
        event_subtype="conditional-order-sentinel",
        order=order,
        failures=failures,
    )

    write_warning.assert_called_once_with(
        event_subtype="conditional-order-sentinel",
        order=order,
        failures=failures,
    )


def test_partial_fill_resizes_persists_and_places_pending_protection_exactly_once():
    pending = SimpleNamespace(
        id="stop-1",
        type="stop_loss",
        product_id="CME:MNQZ26",
        status=OrderStatus.NEW.value,
        quantity=Decimal("1"),
        client_order_id=None,
        intent_payload={"pending_entry_order_id": "entry-1"},
    )
    entry = SimpleNamespace(
        id="entry-1",
        type="limit",
        filled_quantity=Decimal("2.125"),
    )
    manager = MagicMock()
    manager.repo.list_orders_by_statuses.return_value = [pending]
    adapter = MagicMock()
    adapter.place_order.return_value = "exchange-stop-1"
    operation_guard = MagicMock()
    record_order_ack = MagicMock()
    owner = _owner(
        manager=manager,
        adapter=adapter,
        assert_external_operation_allowed=operation_guard,
        record_order_ack=record_order_ack,
    )

    assert owner.place_pending_for_entry(entry) == []
    assert pending.quantity == Decimal("2.125")
    assert manager.repo.update_order.call_args_list == [call(pending)]
    operation_guard.assert_called_once_with()
    adapter.place_order.assert_called_once_with(pending)
    record_order_ack.assert_called_once_with(
        pending,
        "exchange-stop-1",
        order_id="stop-1",
    )


def test_live_ccxt_conditional_queries_pass_exact_adapter_venue_scope():
    manager = MagicMock()
    manager.repo.list_orders_by_statuses.return_value = []
    adapter = MagicMock()
    adapter.exchange_id = "binance"
    owner = _owner(manager=manager, adapter=adapter)
    entry = SimpleNamespace(
        id="entry-1",
        type="market",
        filled_quantity=Decimal("1"),
    )

    assert owner.place_pending_for_entry(entry) == []
    assert owner.recover_pending_protection() == {
        "pending_count": 0,
        "entries_attempted": 0,
        "failures": [],
    }

    assert manager.repo.list_orders_by_statuses.call_args_list == [
        call(
            {
                OrderStatus.NEW.value,
                OrderStatus.SUBMITTED_UNCONFIRMED.value,
                OrderStatus.SUBMITTED.value,
                OrderStatus.PARTIALLY_FILLED.value,
            },
            exchange_id="binance",
        ),
        call({OrderStatus.NEW.value}, exchange_id="binance"),
    ]


def test_live_ccxt_recovery_submits_only_current_venue_protection(
    mock_order_repo,
    order_factory,
):
    current_entry = order_factory(
        order_id="current-entry",
        exchange_id="BINANCE",
        status=OrderStatus.FILLED.value,
        filled_quantity=Decimal("2"),
    )
    current_stop = order_factory(
        order_id="current-stop",
        exchange_id="BINANCE",
        status=OrderStatus.NEW.value,
        order_type="stop_loss",
        quantity=Decimal("1"),
        client_order_id=None,
    )
    current_stop.intent_payload = {"pending_entry_order_id": "current-entry"}
    foreign_entry = order_factory(
        order_id="foreign-entry",
        exchange_id="BYBIT",
        product_id="BYBIT:BTCUSDT-PERP",
        status=OrderStatus.FILLED.value,
        filled_quantity=Decimal("3"),
    )
    foreign_stop = order_factory(
        order_id="foreign-stop",
        exchange_id="BYBIT",
        product_id="BYBIT:BTCUSDT-PERP",
        status=OrderStatus.NEW.value,
        order_type="stop_loss",
        quantity=Decimal("1"),
        client_order_id=None,
    )
    foreign_stop.intent_payload = {"pending_entry_order_id": "foreign-entry"}
    for order in (current_entry, current_stop, foreign_entry, foreign_stop):
        mock_order_repo.add_order(order)
    manager = MagicMock()
    manager.repo = mock_order_repo
    adapter = MagicMock()
    adapter.exchange_id = "binance"
    adapter.place_order.return_value = "EX-CURRENT-STOP"
    record_order_ack = MagicMock()
    owner = _owner(
        manager=manager,
        adapter=adapter,
        record_order_ack=record_order_ack,
    )

    result = owner.recover_pending_protection()

    assert result == {
        "pending_count": 1,
        "entries_attempted": 1,
        "failures": [],
    }
    adapter.place_order.assert_called_once_with(current_stop)
    record_order_ack.assert_called_once_with(
        current_stop,
        "EX-CURRENT-STOP",
        order_id="current-stop",
    )
    assert current_stop.quantity == Decimal("2")
    assert foreign_stop.quantity == Decimal("1")
    assert foreign_stop.status == OrderStatus.NEW.value


def test_actual_conditional_recovery_scopes_same_venue_collision_by_account(
    sqlite_order_session_factory,
    order_factory,
) -> None:
    repositories = {
        account_id: LiveOrderRepository(
            db_session_factory=sqlite_order_session_factory,
            account_profile="ccxt:binance:live",
            account_id=account_id,
        )
        for account_id in ("ACCOUNT-A", "ACCOUNT-B")
    }
    for account_id, repository in repositories.items():
        entry = order_factory(
            order_id=f"entry-{account_id}",
            exchange_id="BINANCE",
            status=OrderStatus.FILLED.value,
            filled_quantity=Decimal("2"),
            client_order_id="shared-entry-client",
            exchange_order_id="shared-entry-exchange",
        )
        stop = order_factory(
            order_id=f"stop-{account_id}",
            exchange_id="BINANCE",
            status=OrderStatus.NEW.value,
            order_type="stop_loss",
            quantity=Decimal("1"),
            client_order_id=None,
            exchange_order_id=None,
        )
        stop.intent_payload = {"pending_entry_order_id": entry.id}
        repository.add_order(entry)
        repository.add_order(stop)
    manager = MagicMock()
    manager.repo = repositories["ACCOUNT-A"]
    adapter = MagicMock()
    adapter.exchange_id = "binance"
    adapter.place_order.return_value = "EX-CURRENT-STOP"
    record_order_ack = MagicMock()
    owner = _owner(
        manager=manager,
        adapter=adapter,
        record_order_ack=record_order_ack,
    )

    result = owner.recover_pending_protection()

    assert result == {
        "pending_count": 1,
        "entries_attempted": 1,
        "failures": [],
    }
    submitted = adapter.place_order.call_args.args[0]
    assert submitted.id == "stop-ACCOUNT-A"
    assert submitted.quantity == Decimal("2")
    record_order_ack.assert_called_once()
    foreign_stop = repositories["ACCOUNT-B"].get_order("stop-ACCOUNT-B")
    assert foreign_stop.status == OrderStatus.NEW.value
    assert foreign_stop.quantity == Decimal("1")


def test_conditional_submit_failure_always_releases_submission_gate():
    pending = SimpleNamespace(
        id="stop-1",
        type="stop_loss",
        product_id="CME:MNQZ26",
        status=OrderStatus.NEW.value,
        quantity=Decimal("1"),
        client_order_id=None,
        intent_payload={"pending_entry_order_id": "entry-1"},
    )
    entry = SimpleNamespace(
        id="entry-1",
        type="market",
        filled_quantity=Decimal("1"),
    )
    manager = MagicMock()
    manager.repo.list_orders_by_statuses.return_value = [pending]
    adapter = MagicMock()
    adapter.place_order.side_effect = ExchangeError("ordinary placement failure")
    gate = ExecutionSubmissionGate(MagicMock())
    owner = _owner(manager=manager, adapter=adapter, gate=gate)

    failures = owner.place_pending_for_entry(entry)

    assert gate.in_flight == 0
    assert failures[0]["order_id"] == "stop-1"
    assert failures[0]["reason"] == "ordinary placement failure"
    manager.fail_order.assert_called_once_with(
        pending,
        "ordinary placement failure",
    )
