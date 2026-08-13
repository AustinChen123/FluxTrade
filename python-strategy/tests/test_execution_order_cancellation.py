from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.execution_order_cancellation import cancel_known_order
from src.core.models import OrderStatus


def _subject(*, status=OrderStatus.SUBMITTED.value, client_order_id="client-1"):
    order = SimpleNamespace(
        id="local-1",
        product_id="TEST:PRODUCT",
        type="limit",
        status=status,
        client_order_id=client_order_id,
        exchange_order_id="exchange-1",
    )
    repository = MagicMock()
    repository.get_order.return_value = order
    adapter = MagicMock()
    adapter.cancel_terminal_state_delivered_by_order_events.return_value = False
    adapter.cancel_order_by_client_id.return_value = True
    adapter.cancel_order.return_value = True
    trace: list[str] = []
    adapter.cancel_terminal_state_delivered_by_order_events.side_effect = lambda: (
        trace.append("terminal_policy") or False
    )
    adapter.cancel_order_by_client_id.side_effect = lambda *args, **kwargs: (
        trace.append("cancel_client") or True
    )
    adapter.cancel_order.side_effect = lambda *args, **kwargs: (
        trace.append("cancel_exchange") or True
    )
    operation_guard = MagicMock(side_effect=lambda: trace.append("guard"))
    mark_cancelled = MagicMock(side_effect=lambda order: trace.append("mark_cancelled"))
    fail_pending = MagicMock(side_effect=lambda order: trace.append("fail_pending"))
    return SimpleNamespace(
        order=order,
        repository=repository,
        adapter=adapter,
        operation_guard=operation_guard,
        mark_cancelled=mark_cancelled,
        fail_pending=fail_pending,
        trace=trace,
    )


def _cancel(subject):
    return cancel_known_order(
        repository=subject.repository,
        adapter=subject.adapter,
        order_id="local-1",
        assert_external_operation_allowed=subject.operation_guard,
        mark_cancelled=subject.mark_cancelled,
        fail_pending_conditional_orders_for_terminal_entry=subject.fail_pending,
    )


def test_missing_order_returns_false_without_remote_or_local_mutation():
    subject = _subject()
    subject.repository.get_order.return_value = None

    assert _cancel(subject) is False

    subject.repository.get_order.assert_called_once_with("local-1")
    subject.adapter.cancel_terminal_state_delivered_by_order_events.assert_not_called()
    subject.adapter.cancel_order_by_client_id.assert_not_called()
    subject.adapter.cancel_order.assert_not_called()
    subject.operation_guard.assert_not_called()
    subject.mark_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_already_cancelled_repairs_pending_protection_without_remote_call():
    subject = _subject(status=OrderStatus.CANCELLED.value)

    assert _cancel(subject) is True

    subject.fail_pending.assert_called_once_with(subject.order)
    subject.adapter.cancel_terminal_state_delivered_by_order_events.assert_not_called()
    subject.adapter.cancel_order_by_client_id.assert_not_called()
    subject.adapter.cancel_order.assert_not_called()
    subject.operation_guard.assert_not_called()
    subject.mark_cancelled.assert_not_called()


def test_client_id_success_is_preferred_and_completes_local_terminal_state():
    subject = _subject()

    assert _cancel(subject) is True

    assert subject.trace == [
        "terminal_policy",
        "guard",
        "cancel_client",
        "mark_cancelled",
        "fail_pending",
    ]
    subject.adapter.cancel_order_by_client_id.assert_called_once_with(
        "client-1",
        "TEST:PRODUCT",
        order_type="limit",
    )
    subject.adapter.cancel_order.assert_not_called()
    subject.mark_cancelled.assert_called_once_with(subject.order)
    subject.fail_pending.assert_called_once_with(subject.order)


@pytest.mark.parametrize("client_order_id", [None, "client-1"])
def test_exchange_id_fallback_preserves_guard_order_and_local_transition(
    client_order_id,
):
    subject = _subject(client_order_id=client_order_id)
    subject.adapter.cancel_order_by_client_id.side_effect = lambda *args, **kwargs: (
        subject.trace.append("cancel_client") or False
    )

    assert _cancel(subject) is True

    expected_trace = ["terminal_policy", "guard"]
    if client_order_id:
        expected_trace.append("cancel_client")
    expected_trace.extend(
        ["guard", "cancel_exchange", "mark_cancelled", "fail_pending"]
    )
    assert subject.trace == expected_trace
    if client_order_id:
        subject.adapter.cancel_order_by_client_id.assert_called_once()
    else:
        subject.adapter.cancel_order_by_client_id.assert_not_called()
    subject.adapter.cancel_order.assert_called_once_with(
        "exchange-1",
        "TEST:PRODUCT",
        order_type="limit",
    )
    assert subject.operation_guard.call_count == 2
    subject.mark_cancelled.assert_called_once_with(subject.order)
    subject.fail_pending.assert_called_once_with(subject.order)


def test_exchange_fallback_uses_local_id_and_false_has_no_local_transition():
    subject = _subject(client_order_id=None)
    subject.order.exchange_order_id = None
    subject.adapter.cancel_order.side_effect = lambda *args, **kwargs: (
        subject.trace.append("cancel_exchange") or False
    )

    assert _cancel(subject) is False

    subject.adapter.cancel_order.assert_called_once_with(
        "local-1",
        "TEST:PRODUCT",
        order_type="limit",
    )
    assert subject.operation_guard.call_count == 2
    subject.mark_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_event_delivered_terminal_state_defers_all_local_transition():
    subject = _subject()
    subject.adapter.cancel_terminal_state_delivered_by_order_events.side_effect = (
        lambda: subject.trace.append("terminal_policy") or True
    )

    assert _cancel(subject) is True

    assert subject.trace == ["terminal_policy", "guard", "cancel_client"]
    subject.mark_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()
