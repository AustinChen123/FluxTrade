import time
from decimal import Decimal
from typing import Optional
from sqlalchemy.orm import Session
from src.core.interfaces import IOrderRepository
from src.core.orm_models import Order, Trade, Position, BacktestTradeLog
from src.core.models import OrderSide

class LiveOrderRepository(IOrderRepository):
    def __init__(self, db_session: Session):
        self.db = db_session

    def add_order(self, order: Order) -> None:
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)

    def update_order(self, order: Order) -> None:
        self.db.add(order)
        self.db.commit()

    def add_trade(self, trade: Trade) -> None:
        self.db.add(trade)
        self.db.commit()

    def get_position(self, strategy_id: str, product_id: str, side: str) -> Optional[Position]:
        return self.db.query(Position).filter_by(
            strategy_id=strategy_id, 
            product_id=product_id, 
            side=side
        ).first()

    def update_position(self, strategy_id: str, product_id: str, side: OrderSide, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None:
        # Use with_for_update for locking
        position = self.db.query(Position).with_for_update().filter_by(
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
                self.db.add(position)
            else:
                self.db.commit()
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
        self.db.commit()

    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        order.exchange_order_id = exchange_order_id
        self.db.commit()


class BacktestOrderRepository(IOrderRepository):
    """Order repository for backtest mode.

    Balance and position tracking are delegated to the Rust
    PyMatchingEngine via BacktestAccountService + SimulatedAdapter.
    This repository only records trade logs to the database.
    """

    def __init__(self, db_session: Session, session_id: int, initial_balance: Decimal = Decimal("10000")):
        self.db = db_session
        self.session_id = session_id
        self.balance = initial_balance  # kept for backward compatibility
        self._order_strategy_map: dict[str, str] = {}

    def add_order(self, order: Order) -> None:
        # Track order → strategy_id for BacktestTradeLog
        if order.strategy_id:
            self._order_strategy_map[order.id] = order.strategy_id

    def update_order(self, order: Order) -> None:
        pass

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
        self.db.add(bt_log)
        self.db.commit()

    def update_position(self, strategy_id: str, product_id: str, side: str, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None:
        # No-op: position and balance are tracked by Rust PyMatchingEngine
        pass

    def get_position(self, strategy_id: str, product_id: str, side: str = None) -> Optional[Position]:
        # Position state lives in Rust engine; accessed via BacktestAccountService
        return None

    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        order.exchange_order_id = exchange_order_id
