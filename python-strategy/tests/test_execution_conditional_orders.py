from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, call

import pytest

from src.core.execution import ExecutionEngine
from src.core.execution_conditional_orders import ConditionalOrderLifecycleOwner
from src.core.execution_submission_gate import ExecutionSubmissionGate
from src.core.interfaces.exchange import ExchangeError
from src.core.models import OrderStatus
from src.core.repositories import LiveOrderRepository


def _owner(*, manager=None, adapter=None, gate=None, **overrides):
    manager = manager or MagicMock()
    adapter = adapter or MagicMock()
    dependencies = {
        "repository": manager.repo,
        "create_order": manager.create_order,
        "mark_submitted_unconfirmed": manager.mark_submitted_unconfirmed,
        "mark_submitted": manager.mark_submitted,
        "fail_order": manager.fail_order,
        "adapter": adapter,
        "place_order": adapter.place_order,
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


def test_engine_requires_conditional_repository_capability_before_lifecycle(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
):
    legacy_repository = SimpleNamespace()

    with pytest.raises(
        RuntimeError,
        match="^conditional_order_repository_capability_required$",
    ):
        ExecutionEngine(
            db_session=mock_db_session,
            clock=mock_clock,
            adapter=mock_exchange_adapter,
            order_repository=legacy_repository,
            is_backtest=True,
        )

    assert mock_exchange_adapter.open_orders == []


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
    manager.repo.list_conditional_orders_by_statuses.assert_not_called()
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


def test_native_group_ack_persists_metadata_before_marking_only_uncertain_children():
    uncertain = SimpleNamespace(
        id="stop-1",
        status=OrderStatus.SUBMITTED_UNCONFIRMED.value,
        intent_payload={"native_leg_type": "stop_loss"},
    )
    advanced = SimpleNamespace(
        id="target-1",
        status=OrderStatus.FILLED.value,
        intent_payload={"native_leg_type": "take_profit"},
    )
    manager = MagicMock()
    manager.repo.get_conditional_order.side_effect = [uncertain, advanced]
    trace = MagicMock()
    trace.attach_mock(manager.repo.persist_conditional_order, "persist")
    trace.attach_mock(manager.mark_submitted, "mark_submitted")
    owner = _owner(manager=manager)

    owner.persist_native_group_ack(
        entry_order=SimpleNamespace(client_order_id="entry-client"),
        conditional_orders=[uncertain, advanced],
        exchange_id="parent-1",
    )

    assert manager.repo.get_conditional_order.call_args_list == [
        call("stop-1"),
        call("target-1"),
    ]
    assert trace.mock_calls == [
        call.persist(uncertain),
        call.mark_submitted(uncertain),
        call.persist(advanced),
    ]
    for order in (uncertain, advanced):
        assert order.intent_payload["native_parent_basket_id"] == "parent-1"
        assert order.intent_payload["native_parent_client_order_id"] == "entry-client"


def test_native_group_ack_persistence_failure_stops_before_later_child():
    first = SimpleNamespace(
        id="stop-1",
        status=OrderStatus.SUBMITTED_UNCONFIRMED.value,
        intent_payload={},
    )
    later = SimpleNamespace(
        id="target-1",
        status=OrderStatus.SUBMITTED_UNCONFIRMED.value,
        intent_payload={},
    )
    error = RuntimeError("persist sentinel")
    manager = MagicMock()
    manager.repo.get_conditional_order.side_effect = [first, later]
    manager.repo.persist_conditional_order.side_effect = error
    owner = _owner(manager=manager)

    with pytest.raises(RuntimeError) as raised:
        owner.persist_native_group_ack(
            entry_order=SimpleNamespace(client_order_id="entry-client"),
            conditional_orders=[first, later],
            exchange_id="parent-1",
        )

    assert raised.value is error
    manager.repo.get_conditional_order.assert_called_once_with("stop-1")
    manager.repo.persist_conditional_order.assert_called_once_with(first)
    manager.mark_submitted.assert_not_called()
    assert later.intent_payload == {}


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
    manager.repo.list_conditional_orders_by_statuses.return_value = [pending]
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
    assert manager.repo.persist_conditional_order.call_args_list == [call(pending)]
    operation_guard.assert_called_once_with()
    adapter.place_order.assert_called_once_with(pending)
    record_order_ack.assert_called_once_with(
        pending,
        "exchange-stop-1",
        order_id="stop-1",
    )


def test_live_ccxt_conditional_queries_pass_exact_adapter_venue_scope():
    manager = MagicMock()
    manager.repo.list_conditional_orders_by_statuses.return_value = []
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

    assert manager.repo.list_conditional_orders_by_statuses.call_args_list == [
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


def test_recovery_orders_unique_entries_skips_missing_and_zero_and_reports_failure():
    pending_entry_ids = [
        "entry-zeta",
        "entry-alpha",
        "entry-missing",
        "entry-zero",
        "entry-zeta",
        "entry-beta",
    ]
    pending = [
        SimpleNamespace(
            id=f"stop-{index}",
            intent_payload={"pending_entry_order_id": entry_id},
        )
        for index, entry_id in enumerate(pending_entry_ids)
    ]
    entries = {
        "entry-alpha": SimpleNamespace(
            id="entry-alpha",
            filled_quantity=Decimal("1"),
        ),
        "entry-beta": SimpleNamespace(
            id="entry-beta",
            filled_quantity=Decimal("2"),
        ),
        "entry-zeta": SimpleNamespace(
            id="entry-zeta",
            filled_quantity=Decimal("3"),
        ),
        "entry-zero": SimpleNamespace(
            id="entry-zero",
            filled_quantity=Decimal("0"),
        ),
    }
    repository = SimpleNamespace(
        get_conditional_order=MagicMock(side_effect=entries.get),
        list_conditional_orders_by_statuses=MagicMock(return_value=pending),
        persist_conditional_order=MagicMock(),
    )
    manager = MagicMock()
    manager.repo = repository
    warning = MagicMock()
    logger = MagicMock()
    owner = _owner(manager=manager, write_warning=warning, logger=logger)
    alpha_failure = {
        "order_id": "entry-alpha",
        "order_type": "market",
        "reason": "stop placement failed",
    }
    beta_failure = {
        "order_id": "entry-beta",
        "order_type": "market",
        "reason": "target placement failed",
    }
    owner.place_pending_for_entry = MagicMock(
        side_effect=lambda entry: {
            "entry-alpha": [alpha_failure],
            "entry-beta": [beta_failure],
        }.get(entry.id, [])
    )

    result = owner.recover_pending_protection()

    assert repository.list_conditional_orders_by_statuses.call_args_list == [
        call({OrderStatus.NEW.value}, exchange_id=None)
    ]
    assert repository.get_conditional_order.call_args_list == [
        call("entry-alpha"),
        call("entry-beta"),
        call("entry-missing"),
        call("entry-zero"),
        call("entry-zeta"),
    ]
    assert owner.place_pending_for_entry.call_args_list == [
        call(entries["entry-alpha"]),
        call(entries["entry-beta"]),
        call(entries["entry-zeta"]),
    ]
    assert warning.call_args_list == [
        call(
            event_subtype="conditional_order_placement_failed_after_entry_fill",
            order=entries["entry-alpha"],
            failures=[alpha_failure],
        ),
        call(
            event_subtype="conditional_order_placement_failed_after_entry_fill",
            order=entries["entry-beta"],
            failures=[beta_failure],
        ),
    ]
    logger.error.assert_called_once_with(
        "Pending protection recovery has %s placement failure(s)",
        2,
    )
    assert result == {
        "pending_count": 6,
        "entries_attempted": 3,
        "failures": [alpha_failure, beta_failure],
    }


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
    manager.repo.list_conditional_orders_by_statuses.return_value = [pending]
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
