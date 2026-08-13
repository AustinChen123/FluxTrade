"""Generic known-order cancellation orchestration."""

from collections.abc import Callable
from typing import Protocol, cast

from src.core.interfaces import IOrderRepository
from src.core.interfaces.exchange import IExchangeAdapter
from src.core.models import OrderStatus
from src.core.orm_models import Order


class CancellableOrder(Protocol):
    id: str
    product_id: str
    type: str
    status: str
    client_order_id: str | None
    exchange_order_id: str | None


def cancel_known_order(
    *,
    repository: IOrderRepository,
    adapter: IExchangeAdapter,
    order_id: str,
    assert_external_operation_allowed: Callable[[], None],
    mark_cancelled: Callable[[Order], None],
    fail_pending_conditional_orders_for_terminal_entry: Callable[[Order], None],
) -> bool:
    order = cast(CancellableOrder | None, repository.get_order(order_id))
    if order is None:
        return False
    persisted_order = cast(Order, order)
    if order.status == OrderStatus.CANCELLED.value:
        fail_pending_conditional_orders_for_terminal_entry(persisted_order)
        return True

    terminal_event_pending = (
        adapter.cancel_terminal_state_delivered_by_order_events() is True
    )
    assert_external_operation_allowed()
    if order.client_order_id and adapter.cancel_order_by_client_id(
        order.client_order_id,
        order.product_id,
        order_type=order.type,
    ):
        if not terminal_event_pending:
            mark_cancelled(persisted_order)
            fail_pending_conditional_orders_for_terminal_entry(persisted_order)
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
        mark_cancelled(persisted_order)
        fail_pending_conditional_orders_for_terminal_entry(persisted_order)
    return True
