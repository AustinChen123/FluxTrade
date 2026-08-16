"""Generic known-order cancellation orchestration."""

from collections.abc import Callable
from src.core.interfaces.exchange import IExchangeAdapter
from src.core.interfaces.order_cancellation import (
    OrderCancellationRepository,
    OrderCancellationSnapshot,
)
from src.core.models import OrderStatus


def cancel_known_order(
    *,
    repository: OrderCancellationRepository,
    adapter: IExchangeAdapter,
    order_id: str,
    assert_external_operation_allowed: Callable[[], None],
    fail_pending_conditional_orders_for_terminal_entry: Callable[
        [OrderCancellationSnapshot], None
    ],
) -> bool:
    order = repository.get_order_for_cancellation(order_id)
    if order is None:
        return False
    if order.status == OrderStatus.CANCELLED.value:
        fail_pending_conditional_orders_for_terminal_entry(order)
        return True

    terminal_event_pending = (
        adapter.cancel_terminal_state_delivered_by_order_events(order.type) is True
    )
    assert_external_operation_allowed()
    if order.client_order_id and adapter.cancel_order_by_client_id(
        order.client_order_id,
        order.product_id,
        order_type=order.type,
    ):
        if not terminal_event_pending:
            repository.mark_order_cancelled(order.id)
            fail_pending_conditional_orders_for_terminal_entry(order)
        return True

    exchange_order_id = order.exchange_order_id or order.id
    assert_external_operation_allowed()
    if not adapter.cancel_order(
        exchange_order_id,
        order.product_id,
        order_type=order.type,
    ):
        return False

    if not terminal_event_pending:
        repository.mark_order_cancelled(order.id)
        fail_pending_conditional_orders_for_terminal_entry(order)
    return True
