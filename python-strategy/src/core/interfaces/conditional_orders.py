"""ORM-free persistence boundary for conditional-order lifecycle state."""

from decimal import Decimal
from typing import Protocol, runtime_checkable


class ConditionalOrderRecord(Protocol):
    id: object
    product_id: str
    side: str
    type: str
    status: str
    quantity: Decimal | None
    filled_quantity: Decimal | None
    filled_price: Decimal | None
    trigger_price: Decimal | None
    client_order_id: str | None
    exchange_order_id: str | None
    intent_payload: dict[str, object] | None


@runtime_checkable
class ConditionalOrderRepository(Protocol):
    def get_conditional_order(
        self,
        order_id: str,
    ) -> ConditionalOrderRecord | None: ...

    def list_conditional_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[ConditionalOrderRecord]: ...

    def persist_conditional_order(self, order: ConditionalOrderRecord) -> None: ...
