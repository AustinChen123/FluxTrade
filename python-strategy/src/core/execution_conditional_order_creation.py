"""Materialize conditional order intents as durable local orders."""

from collections.abc import Callable
from decimal import Decimal
from typing import Protocol

from src.core.client_order_id import linked_client_order_id
from src.core.conditional_order_intents import (
    conditional_oco_pairs,
    conditional_order_intents,
)
from src.core.models import Candlestick, OrderSide, OrderStatus, Signal


class ConditionalOrder(Protocol):
    id: object
    status: str
    exchange_order_id: str | None
    intent_payload: dict[str, object] | None


class ConditionalOrderRepository(Protocol):
    def update_order(self, order: ConditionalOrder) -> None: ...


class ConditionalOrderManager(Protocol):
    repo: ConditionalOrderRepository

    def create_order(self, **kwargs: object) -> ConditionalOrder: ...


class EntryOrder(Protocol):
    id: object
    side: str
    client_order_id: str | None


def _conditional_client_order_id(
    entry_client_order_id: str | None,
    suffix: str,
) -> str | None:
    if not entry_client_order_id:
        return None
    return linked_client_order_id(entry_client_order_id, suffix)


def create_conditional_orders(
    *,
    order_manager: ConditionalOrderManager,
    signal: Signal,
    entry_order: EntryOrder,
    quantity: Decimal,
    candle: Candlestick | None,
    attach_min_notional_reference_price: Callable[
        [ConditionalOrder, Candlestick | None], None
    ],
) -> list[ConditionalOrder]:
    close_side = OrderSide.SELL if entry_order.side.lower() == "buy" else OrderSide.BUY
    orders: list[ConditionalOrder] = []
    intents = conditional_order_intents(signal)

    for intent in intents:
        order = order_manager.create_order(
            signal=signal,
            side=close_side,
            order_type=intent.order_type,
            quantity=quantity,
            trigger_price=intent.trigger_price,
            client_order_id=_conditional_client_order_id(
                entry_order.client_order_id,
                intent.client_order_suffix,
            ),
        )
        if intent.trailing_distance is not None:
            setattr(order, "_trailing_distance", intent.trailing_distance)
        attach_min_notional_reference_price(order, candle)
        orders.append(order)

    for first_index, second_index in conditional_oco_pairs(intents):
        first = orders[first_index]
        second = orders[second_index]
        setattr(first, "_linked_order_id", second.id)
        setattr(second, "_linked_order_id", first.id)

    for order in orders:
        linked_order_id = getattr(order, "_linked_order_id", None)
        order.status = OrderStatus.NEW.value
        order.exchange_order_id = None
        order.intent_payload = {
            "pending_entry_order_id": str(entry_order.id),
            "linked_order_id": str(linked_order_id) if linked_order_id else None,
            "placement_mode": "place-after-fill",
        }
        order_manager.repo.update_order(order)

    return orders
