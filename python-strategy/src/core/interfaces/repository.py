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
    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        pass

    @abstractmethod
    def add_trade(self, trade: Trade) -> None:
        pass

    @abstractmethod
    def update_position(self, strategy_id: str, product_id: str, side: str, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None:
        pass
        
    @abstractmethod
    def get_position(self, strategy_id: str, product_id: str, side: str) -> Optional[Position]:
        pass
