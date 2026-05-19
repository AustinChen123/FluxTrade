import time
from contextlib import nullcontext
from decimal import Decimal
from typing import Callable, ContextManager, Optional
from sqlalchemy.orm import Session
from src.core.interfaces import IOrderRepository
from src.core.orm_models import Order, Trade, Position, BacktestTradeLog
from src.core.models import OrderSide

class LiveOrderRepository(IOrderRepository):
    def __init__(
        self,
        db_session: Session | None = None,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
    ):
        self._db_session_factory = db_session_factory or (lambda: nullcontext(db_session))

    def add_order(self, order: Order) -> None:
        with self._db_session_factory() as db:
            db.add(order)
            db.commit()
            db.refresh(order)

    def update_order(self, order: Order) -> None:
        with self._db_session_factory() as db:
            db.add(order)
            db.commit()

    def get_order(self, order_id: str) -> Optional[Order]:
        with self._db_session_factory() as db:
            return db.query(Order).filter_by(id=order_id).first()

    def get_order_by_client_order_id(self, client_order_id: str) -> Optional[Order]:
        with self._db_session_factory() as db:
            return db.query(Order).filter_by(client_order_id=client_order_id).first()

    def list_client_orders_by_statuses(self, statuses: set[str]) -> list[Order]:
        if not statuses:
            return []
        with self._db_session_factory() as db:
            return (
                db.query(Order)
                .filter(
                    Order.status.in_(statuses),
                    Order.client_order_id.isnot(None),
                )
                .all()
            )

    def add_trade(self, trade: Trade) -> None:
        with self._db_session_factory() as db:
            db.add(trade)
            db.commit()

    def get_position(self, strategy_id: str, product_id: str, side: str) -> Optional[Position]:
        with self._db_session_factory() as db:
            return db.query(Position).filter_by(
                strategy_id=strategy_id,
                product_id=product_id,
                side=side
            ).first()

    def update_position(self, strategy_id: str, product_id: str, side: OrderSide, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None:
        # Use with_for_update for locking
        with self._db_session_factory() as db:
            position = db.query(Position).with_for_update().filter_by(
                strategy_id=strategy_id,
                product_id=product_id,
                side=position_side
            ).first()

            current_time = int(time.time() * 1000)

            if not position:
                if side == OrderSide.BUY:
                    position = Position(
                        strategy_id=strategy_id,
                        product_id=product_id,
                        side=position_side,
                        quantity=Decimal("0"),
                        entry_price=Decimal("0"),
                        unrealized_pnl=Decimal("0"),
                        last_update_timestamp=current_time
                    )
                    db.add(position)
                else:
                    db.commit()
                    return

            if side == OrderSide.BUY:
                total_cost = (position.quantity * position.entry_price) + (fill_quantity * fill_price)
                total_qty = position.quantity + fill_quantity
                if total_qty > 0:
                    position.entry_price = total_cost / total_qty
                position.quantity = total_qty
            elif side == OrderSide.SELL:
                position.quantity = max(Decimal("0"), position.quantity - fill_quantity)

            position.last_update_timestamp = current_time
            db.commit()

    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        order.exchange_order_id = exchange_order_id
        with self._db_session_factory() as db:
            db.add(order)
            db.commit()


class BacktestOrderRepository(IOrderRepository):
    """Order repository for backtest mode.

    Balance and position tracking are delegated to the Rust
    PyMatchingEngine via BacktestAccountService + SimulatedAdapter.
    This repository only records trade logs to the database.
    """

    def __init__(
        self,
        db_session: Session | None,
        session_id: int,
        initial_balance: Decimal = Decimal("10000"),
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
    ):
        self._db_session_factory = db_session_factory or (lambda: nullcontext(db_session))
        self.session_id = session_id
        self.balance = initial_balance  # kept for backward compatibility
        self._order_strategy_map: dict[str, str] = {}

    def add_order(self, order: Order) -> None:
        # Track order → strategy_id for BacktestTradeLog
        if order.strategy_id:
            self._order_strategy_map[order.id] = order.strategy_id

    def update_order(self, order: Order) -> None:
        pass

    def get_order(self, order_id: str) -> Optional[Order]:
        return None

    def get_order_by_client_order_id(self, client_order_id: str) -> Optional[Order]:
        return None

    def list_client_orders_by_statuses(self, statuses: set[str]) -> list[Order]:
        return []

    def add_trade(self, trade: Trade) -> None:
        strategy_id = self._order_strategy_map.get(trade.order_id)
        bt_log = BacktestTradeLog(
            id=trade.id,
            session_id=self.session_id,
            strategy_id=strategy_id,
            order_id=trade.order_id,
            exchange_trade_id=trade.exchange_trade_id,
            product_id=trade.product_id,
            side=trade.side,
            price=trade.price,
            quantity=trade.quantity,
            fee=trade.fee,
            fee_asset=trade.fee_asset,
            timestamp=trade.timestamp
        )
        with self._db_session_factory() as db:
            db.add(bt_log)
            db.commit()

    def update_position(self, strategy_id: str, product_id: str, side: str, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None:
        # No-op: position and balance are tracked by Rust PyMatchingEngine
        pass

    def get_position(self, strategy_id: str, product_id: str, side: str = None) -> Optional[Position]:
        # Position state lives in Rust engine; accessed via BacktestAccountService
        return None

    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        order.exchange_order_id = exchange_order_id
