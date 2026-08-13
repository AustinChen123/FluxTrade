from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, call

from src.core.execution import ExecutionEngine
from src.core.execution_conditional_orders import ConditionalOrderLifecycleOwner
from src.core.execution_submission_gate import ExecutionSubmissionGate
from src.core.interfaces.exchange import ExchangeError
from src.core.models import OrderStatus


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
    failures = [{"reason": "placement-failed"}]

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
