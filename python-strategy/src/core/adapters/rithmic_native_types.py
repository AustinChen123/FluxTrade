from __future__ import annotations

from typing import Callable, Protocol


class RithmicOrderAck(Protocol):
    client_order_id: str
    basket_id: str


class RithmicLedgerOrder(Protocol):
    client_order_id: str | None
    exchange_order_id: str | None
    basket_id: str
    status: str
    notification_type: str | None
    quantity: str
    filled_quantity: str | None
    average_fill_price: str | None


class RithmicOrderEvent(Protocol):
    account_id: str
    client_order_id: str | None
    basket_id: str
    original_basket_id: str | None
    linked_basket_ids: str | None
    exchange_order_id: str | None
    exchange: str
    symbol: str
    status: str
    notification_type: str
    price: str | None
    trigger_price: str | None
    price_type: str | None
    bracket_type: str | None
    last_fill_quantity: str | None
    last_fill_price: str | None
    cumulative_filled_quantity: str | None
    cumulative_average_price: str | None
    timestamp_ms: int | None


class RithmicOrderClient(Protocol):
    def __init__(self, profile: str, account_id: str | None = None) -> None: ...

    @property
    def is_connected(self) -> bool: ...

    def connection_generation(self) -> int: ...

    def submit(
        self,
        client_order_id: str,
        exchange: str,
        symbol: str,
        quantity: str,
        side: str,
        order_type: str,
        price: str | None = None,
    ) -> RithmicOrderAck: ...

    def submit_bracket(
        self,
        client_order_id: str,
        exchange: str,
        symbol: str,
        quantity: str,
        side: str,
        order_type: str,
        price: str | None = None,
        stop_ticks: int | None = None,
        target_ticks: int | None = None,
    ) -> RithmicOrderAck: ...

    def modify_protection(
        self,
        basket_id: str,
        exchange: str,
        symbol: str,
        quantity: str,
        leg_type: str,
        price: str,
    ) -> bool: ...

    def cancel(self, basket_id: str) -> bool: ...

    def exit_position(
        self,
        exchange: str,
        symbol: str,
        window_name: str | None = None,
    ) -> bool: ...

    def lookup(
        self,
        client_order_id: str,
        exchange: str,
        symbol: str,
    ) -> RithmicLedgerOrder | None: ...

    def poll_event(self) -> RithmicOrderEvent | None: ...


type RithmicOrderClientFactory = Callable[[str, str | None], RithmicOrderClient]
