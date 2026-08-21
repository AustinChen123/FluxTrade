"""ORM-free persistence boundary for known-order cancellation."""

from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol, runtime_checkable


@dataclass(frozen=True, slots=True)
class OrderCancellationSnapshot:
    id: str
    product_id: str
    type: str
    status: str
    filled_quantity: Decimal | None
    client_order_id: str | None
    exchange_order_id: str | None


@runtime_checkable
class OrderCancellationRepository(Protocol):
    def get_order_for_cancellation(
        self,
        order_id: str,
    ) -> OrderCancellationSnapshot | None: ...

    def mark_order_cancelled(self, order_id: str) -> None: ...
