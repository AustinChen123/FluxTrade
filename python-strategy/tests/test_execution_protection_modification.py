from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.execution_protection_modification import modify_attached_protection
from src.core.interfaces.exchange import ExchangeError, NetworkError
from src.core.models import OrderStatus


def _subject(*, leg_type: str = "stop_loss"):
    entry = SimpleNamespace(id="entry-1", type="market")
    original_payload = {
        "pending_entry_order_id": "entry-1",
        "placement_mode": "attach-at-entry",
        "effective_price": "99.00",
        "modifications": [{"status": "confirmed", "requested_price": "99.00"}],
    }
    protection = SimpleNamespace(
        id="protection-1",
        type=leg_type,
        trigger_price=Decimal("99.00"),
        intent_payload=original_payload,
    )
    repository = MagicMock()
    repository.get_order.return_value = entry
    repository.list_orders_by_statuses.return_value = [protection]
    adapter = MagicMock()
    adapter.modify_protection.return_value = True
    clock = MagicMock()
    clock.now.side_effect = [1.25, 2.5]
    halt_for_reconcile = MagicMock(return_value=True)
    trace: list[str] = []

    @contextmanager
    def order_event_lock():
        trace.append("lock_enter")
        try:
            yield
        finally:
            trace.append("lock_exit")

    repository.update_order.side_effect = lambda order: trace.append(
        f"persist_{order.intent_payload['modifications'][-1]['status']}"
    )
    adapter.modify_protection.side_effect = lambda order, *, trigger_price: (
        trace.append(f"remote_{trigger_price}") or True
    )
    operation_guard = MagicMock(side_effect=lambda: trace.append("guard"))
    return SimpleNamespace(
        entry=entry,
        protection=protection,
        original_payload=original_payload,
        repository=repository,
        adapter=adapter,
        clock=clock,
        halt_for_reconcile=halt_for_reconcile,
        trace=trace,
        order_event_lock=order_event_lock(),
        operation_guard=operation_guard,
    )


def _modify(subject, *, leg_type: str = "stop_loss", price=Decimal("99.25")):
    return modify_attached_protection(
        repository=subject.repository,
        adapter=subject.adapter,
        clock=subject.clock,
        order_event_lock=subject.order_event_lock,
        assert_operation_allowed=subject.operation_guard,
        halt_for_reconcile=subject.halt_for_reconcile,
        entry_order_id="entry-1",
        leg_type=leg_type,
        price=price,
    )


def test_confirmed_modification_preserves_transaction_order_and_exact_payload():
    subject = _subject()
    original_payload = deepcopy(subject.original_payload)

    result = _modify(subject)

    assert result == {
        "entry_order_id": "entry-1",
        "order_id": "protection-1",
        "leg_type": "stop_loss",
        "effective_price": "99.25",
    }
    assert subject.trace == [
        "lock_enter",
        "persist_pending",
        "guard",
        "remote_99.25",
        "persist_confirmed",
        "lock_exit",
    ]
    assert subject.original_payload == original_payload
    assert subject.protection.trigger_price == Decimal("99.25")
    assert subject.protection.intent_payload == {
        **original_payload,
        "requested_price": "99.25",
        "expected_effective_price": "99.25",
        "effective_price": "99.25",
        "price_drift": "0",
        "modification_mode": "absolute",
        "protection_confirmation": "confirmed",
        "modifications": [
            original_payload["modifications"][0],
            {
                "previous_effective_price": "99.00",
                "requested_price": "99.25",
                "started_at_ms": 1250,
                "status": "confirmed",
                "finished_at_ms": 2500,
            },
        ],
    }
    subject.repository.get_order.assert_called_once_with("entry-1")
    subject.repository.list_orders_by_statuses.assert_called_once_with(
        {
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
    )
    subject.adapter.modify_protection.assert_called_once_with(
        subject.protection,
        trigger_price=Decimal("99.25"),
    )
    subject.halt_for_reconcile.assert_not_called()


@pytest.mark.parametrize(
    ("entry", "protections", "leg_type", "error"),
    [
        (None, [], "stop_loss", "modify_protection_entry_not_found"),
        (
            SimpleNamespace(id="entry-1", type="stop_loss"),
            [],
            "stop_loss",
            "modify_protection_entry_not_found",
        ),
        (
            SimpleNamespace(id="entry-1", type="market"),
            [],
            "stop_loss",
            "modify_protection_leg_identity_ambiguous",
        ),
        (
            SimpleNamespace(id="entry-1", type="market"),
            [
                SimpleNamespace(
                    id="one",
                    type="stop_loss",
                    intent_payload={
                        "pending_entry_order_id": "entry-1",
                        "placement_mode": "attach-at-entry",
                    },
                ),
                SimpleNamespace(
                    id="two",
                    type="stop_loss",
                    intent_payload={
                        "pending_entry_order_id": "entry-1",
                        "placement_mode": "attach-at-entry",
                    },
                ),
            ],
            "stop_loss",
            "modify_protection_leg_identity_ambiguous",
        ),
    ],
)
def test_lookup_rejects_missing_or_ambiguous_identity(
    entry,
    protections,
    leg_type,
    error,
):
    subject = _subject(leg_type=leg_type)
    subject.repository.get_order.return_value = entry
    subject.repository.list_orders_by_statuses.return_value = protections

    with pytest.raises(ExchangeError, match=f"^{error}$"):
        _modify(subject, leg_type=leg_type)

    subject.repository.update_order.assert_not_called()
    subject.adapter.modify_protection.assert_not_called()
    subject.operation_guard.assert_not_called()
    subject.halt_for_reconcile.assert_not_called()


@pytest.mark.parametrize(
    "protection",
    [
        SimpleNamespace(
            type="take_profit",
            intent_payload={
                "pending_entry_order_id": "entry-1",
                "placement_mode": "attach-at-entry",
            },
        ),
        SimpleNamespace(type="stop_loss", intent_payload=None),
        SimpleNamespace(
            type="stop_loss",
            intent_payload={
                "pending_entry_order_id": "other-entry",
                "placement_mode": "attach-at-entry",
            },
        ),
        SimpleNamespace(
            type="stop_loss",
            intent_payload={
                "pending_entry_order_id": "entry-1",
                "placement_mode": "deferred",
            },
        ),
    ],
)
def test_lookup_ignores_each_unrelated_protection(protection):
    subject = _subject()
    subject.repository.list_orders_by_statuses.return_value = [protection]

    with pytest.raises(
        ExchangeError,
        match="^modify_protection_leg_identity_ambiguous$",
    ):
        _modify(subject)

    subject.repository.update_order.assert_not_called()
    subject.adapter.modify_protection.assert_not_called()


@pytest.mark.parametrize(
    ("remote_result", "error_type", "status", "halted"),
    [
        (False, ExchangeError, "rejected", False),
        (ExchangeError("provider rejected"), ExchangeError, "rejected", False),
        (NetworkError("connection lost"), NetworkError, "ambiguous", True),
    ],
)
def test_remote_failure_preserves_previous_price_and_exact_disposition(
    remote_result,
    error_type,
    status,
    halted,
):
    subject = _subject()
    if isinstance(remote_result, Exception):
        subject.adapter.modify_protection.side_effect = remote_result
    else:
        subject.adapter.modify_protection.side_effect = lambda order, **kwargs: (
            subject.trace.append("remote_false") or remote_result
        )

    with pytest.raises(error_type) as raised:
        _modify(subject)

    if isinstance(remote_result, Exception):
        assert raised.value is remote_result
    else:
        assert str(raised.value) == "modify_protection_not_confirmed"
    assert subject.protection.trigger_price == Decimal("99.00")
    assert subject.protection.intent_payload["modifications"][-1]["status"] == status
    assert (
        subject.protection.intent_payload["modifications"][-1]["finished_at_ms"] == 2500
    )
    assert subject.trace[-1] == "lock_exit"
    assert subject.halt_for_reconcile.call_count == int(halted)


def test_operation_guard_failure_keeps_pending_attempt_without_remote_call():
    subject = _subject()
    guard_error = RuntimeError("operations blocked")
    subject.operation_guard.side_effect = guard_error

    with pytest.raises(RuntimeError) as raised:
        _modify(subject)

    assert raised.value is guard_error
    assert subject.protection.trigger_price == Decimal("99.00")
    assert subject.protection.intent_payload["modifications"][-1]["status"] == "pending"
    subject.adapter.modify_protection.assert_not_called()
    subject.halt_for_reconcile.assert_not_called()
    assert subject.trace == ["lock_enter", "persist_pending", "lock_exit"]


def test_confirmed_persistence_failure_restores_pending_state_and_halts():
    subject = _subject()
    persistence_error = RuntimeError("database unavailable")
    snapshots = []

    def persist(order):
        snapshots.append((order.trigger_price, deepcopy(order.intent_payload)))
        if order.intent_payload["modifications"][-1]["status"] == "confirmed":
            raise persistence_error

    subject.repository.update_order.side_effect = persist

    with pytest.raises(RuntimeError) as raised:
        _modify(subject)

    assert raised.value is persistence_error
    assert [snapshot[1]["modifications"][-1]["status"] for snapshot in snapshots] == [
        "pending",
        "confirmed",
    ]
    assert subject.protection.trigger_price == Decimal("99.00")
    assert subject.protection.intent_payload["modifications"][-1]["status"] == "pending"
    subject.halt_for_reconcile.assert_called_once_with()
