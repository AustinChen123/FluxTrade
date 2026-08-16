from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.execution_order_cancellation import cancel_known_order
from src.core.interfaces.order_cancellation import OrderCancellationSnapshot
from src.core.models import OrderStatus


def _subject(
    *,
    status=OrderStatus.SUBMITTED.value,
    client_order_id: str | None = "client-1",
    exchange_order_id: str | None = "exchange-1",
):
    order = OrderCancellationSnapshot(
        id="local-1",
        product_id="TEST:PRODUCT",
        type="limit",
        status=status,
        filled_quantity=Decimal("0"),
        client_order_id=client_order_id,
        exchange_order_id=exchange_order_id,
    )
    repository = MagicMock()
    repository.get_order_for_cancellation.return_value = order
    adapter = MagicMock()
    adapter.cancel_terminal_state_delivered_by_order_events.return_value = False
    adapter.cancel_order_by_client_id.return_value = True
    adapter.cancel_order.return_value = True
    trace: list[str] = []
    adapter.cancel_terminal_state_delivered_by_order_events.side_effect = (
        lambda _order_type=None: trace.append("terminal_policy") or False
    )
    adapter.cancel_order_by_client_id.side_effect = lambda *args, **kwargs: (
        trace.append("cancel_client") or True
    )
    adapter.cancel_order.side_effect = lambda *args, **kwargs: (
        trace.append("cancel_exchange") or True
    )
    operation_guard = MagicMock(side_effect=lambda: trace.append("guard"))
    repository.mark_order_cancelled.side_effect = lambda order_id: trace.append(
        "mark_cancelled"
    )
    fail_pending = MagicMock(side_effect=lambda order: trace.append("fail_pending"))
    return SimpleNamespace(
        order=order,
        repository=repository,
        adapter=adapter,
        operation_guard=operation_guard,
        fail_pending=fail_pending,
        trace=trace,
    )


def _cancel(subject):
    return cancel_known_order(
        repository=subject.repository,
        adapter=subject.adapter,
        order_id="local-1",
        assert_external_operation_allowed=subject.operation_guard,
        fail_pending_conditional_orders_for_terminal_entry=subject.fail_pending,
    )


def test_missing_order_returns_false_without_remote_or_local_mutation():
    subject = _subject()
    subject.repository.get_order_for_cancellation.return_value = None

    assert _cancel(subject) is False

    subject.repository.get_order_for_cancellation.assert_called_once_with("local-1")
    subject.adapter.cancel_terminal_state_delivered_by_order_events.assert_not_called()
    subject.adapter.cancel_order_by_client_id.assert_not_called()
    subject.adapter.cancel_order.assert_not_called()
    subject.operation_guard.assert_not_called()
    subject.repository.mark_order_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_already_cancelled_repairs_pending_protection_without_remote_call():
    subject = _subject(status=OrderStatus.CANCELLED.value)

    assert _cancel(subject) is True

    subject.fail_pending.assert_called_once_with(subject.order)
    subject.adapter.cancel_terminal_state_delivered_by_order_events.assert_not_called()
    subject.adapter.cancel_order_by_client_id.assert_not_called()
    subject.adapter.cancel_order.assert_not_called()
    subject.operation_guard.assert_not_called()
    subject.repository.mark_order_cancelled.assert_not_called()


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
    subject.repository.mark_order_cancelled.assert_called_once_with("local-1")
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
    subject.repository.mark_order_cancelled.assert_called_once_with("local-1")
    subject.fail_pending.assert_called_once_with(subject.order)


def test_exchange_fallback_uses_local_id_and_false_has_no_local_transition():
    subject = _subject(client_order_id=None, exchange_order_id=None)
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
    subject.repository.mark_order_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_event_delivered_terminal_state_defers_all_local_transition():
    subject = _subject()
    subject.adapter.cancel_terminal_state_delivered_by_order_events.side_effect = (
        lambda _order_type=None: subject.trace.append("terminal_policy") or True
    )

    assert _cancel(subject) is True

    assert subject.trace == ["terminal_policy", "guard", "cancel_client"]
    subject.repository.mark_order_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_load_failure_preserves_identity_without_side_effects():
    subject = _subject()
    failure = RuntimeError("load failed")
    subject.repository.get_order_for_cancellation.side_effect = failure

    with pytest.raises(RuntimeError) as raised:
        _cancel(subject)

    assert raised.value is failure
    subject.adapter.cancel_terminal_state_delivered_by_order_events.assert_not_called()
    subject.operation_guard.assert_not_called()
    subject.adapter.cancel_order_by_client_id.assert_not_called()
    subject.adapter.cancel_order.assert_not_called()
    subject.repository.mark_order_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_first_fence_failure_preserves_identity_without_remote_or_local_effects():
    subject = _subject()
    failure = RuntimeError("fence failed")
    subject.operation_guard.side_effect = failure

    with pytest.raises(RuntimeError) as raised:
        _cancel(subject)

    assert raised.value is failure
    subject.adapter.cancel_order_by_client_id.assert_not_called()
    subject.adapter.cancel_order.assert_not_called()
    subject.repository.mark_order_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_client_exception_does_not_fall_back_or_mutate_local_state():
    subject = _subject()
    failure = RuntimeError("client cancel failed")
    subject.adapter.cancel_order_by_client_id.side_effect = failure

    with pytest.raises(RuntimeError) as raised:
        _cancel(subject)

    assert raised.value is failure
    subject.adapter.cancel_order.assert_not_called()
    subject.repository.mark_order_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_second_fence_failure_after_client_false_prevents_exchange_cancel():
    subject = _subject()
    failure = RuntimeError("second fence failed")
    subject.adapter.cancel_order_by_client_id.side_effect = None
    subject.adapter.cancel_order_by_client_id.return_value = False
    subject.operation_guard.side_effect = [None, failure]

    with pytest.raises(RuntimeError) as raised:
        _cancel(subject)

    assert raised.value is failure
    subject.adapter.cancel_order.assert_not_called()
    subject.repository.mark_order_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_exchange_exception_preserves_identity_without_local_effects():
    subject = _subject(client_order_id=None)
    failure = RuntimeError("exchange cancel failed")
    subject.adapter.cancel_order.side_effect = failure

    with pytest.raises(RuntimeError) as raised:
        _cancel(subject)

    assert raised.value is failure
    subject.repository.mark_order_cancelled.assert_not_called()
    subject.fail_pending.assert_not_called()


def test_persistence_failure_prevents_cleanup_and_preserves_identity():
    subject = _subject()
    failure = RuntimeError("persist failed")
    subject.repository.mark_order_cancelled.side_effect = failure

    with pytest.raises(RuntimeError) as raised:
        _cancel(subject)

    assert raised.value is failure
    subject.fail_pending.assert_not_called()


def test_cleanup_failure_is_repaired_on_retry_without_second_remote_cancel():
    subject = _subject()
    failure = RuntimeError("cleanup failed")
    subject.fail_pending.side_effect = failure

    with pytest.raises(RuntimeError) as raised:
        _cancel(subject)

    assert raised.value is failure
    subject.repository.mark_order_cancelled.assert_called_once_with("local-1")
    subject.repository.get_order_for_cancellation.return_value = (
        OrderCancellationSnapshot(
            id="local-1",
            product_id="TEST:PRODUCT",
            type="limit",
            status=OrderStatus.CANCELLED.value,
            filled_quantity=Decimal("0"),
            client_order_id="client-1",
            exchange_order_id="exchange-1",
        )
    )
    subject.fail_pending.reset_mock(side_effect=True)

    assert _cancel(subject) is True

    subject.adapter.cancel_order_by_client_id.assert_called_once()
    subject.adapter.cancel_order.assert_not_called()
    subject.repository.mark_order_cancelled.assert_called_once()
    subject.fail_pending.assert_called_once_with(
        subject.repository.get_order_for_cancellation.return_value
    )


def test_absent_client_id_keeps_two_fences_for_one_exchange_call():
    subject = _subject(client_order_id=None)

    assert _cancel(subject) is True

    assert subject.trace == [
        "terminal_policy",
        "guard",
        "guard",
        "cancel_exchange",
        "mark_cancelled",
        "fail_pending",
    ]
    subject.adapter.cancel_order_by_client_id.assert_not_called()
    subject.adapter.cancel_order.assert_called_once()
    assert subject.operation_guard.call_count == 2
