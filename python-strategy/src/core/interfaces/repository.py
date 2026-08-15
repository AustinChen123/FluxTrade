from abc import ABC, abstractmethod
from decimal import Decimal
from typing import Callable, ContextManager, Optional
from sqlalchemy.orm import Session
from src.core.orm_models import Order, Trade, Position


class IOrderRepository(ABC):
    def __init__(
        self,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
    ) -> None:
        """Repository implementations should use short-lived DB sessions.

        Production implementations should perform DB work inside
        ``with self._db_session_factory() as session:``. Lightweight test
        doubles may ignore this constructor contract when they do not touch DB.
        """

    @abstractmethod
    def add_order(self, order: Order) -> None:
        pass

    @abstractmethod
    def update_order(self, order: Order) -> None:
        pass

    @abstractmethod
    def get_order(self, order_id: str) -> Optional[Order]:
        pass

    @abstractmethod
    def get_order_by_client_order_id(self, client_order_id: str) -> Optional[Order]:
        pass

    def get_order_by_exchange_order_id(
        self,
        exchange_order_id: str,
        exchange_id: str | None = None,
        product_id: str | None = None,
    ) -> Optional[Order]:
        """Return an order by scoped exchange order ID when supported."""
        return None

    def list_client_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[Order]:
        """Return client-order-id-backed orders whose status is in ``statuses``.

        This is a recovery/reconciliation hook. Implementations that do not
        persist orders may return an empty list.
        """
        return []

    def list_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[Order]:
        """Return orders whose status is in ``statuses`` when supported."""
        return []

    def list_legacy_orders_by_statuses(
        self,
        statuses: set[str],
        exchange_id: str | None = None,
    ) -> list[Order]:
        """Return account-unidentified orders for explicit local recovery only."""
        return []

    @abstractmethod
    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        pass

    @abstractmethod
    def add_trade(self, trade: Trade) -> None:
        pass

    def persist_fill(self, order: Order, trade: Trade) -> None:
        """Persist one filled order and its trade using repository semantics."""
        self.update_order(order)
        self.add_trade(trade)

    @abstractmethod
    def update_position(
        self,
        strategy_id: str,
        product_id: str,
        side: str,
        fill_quantity: Decimal,
        fill_price: Decimal,
        position_side: str,
    ) -> None:
        pass

    @abstractmethod
    def get_position(
        self, strategy_id: str, product_id: str, side: str
    ) -> Optional[Position]:
        pass
