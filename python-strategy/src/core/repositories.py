import time
from decimal import Decimal
from typing import Optional, Dict, Tuple
from sqlalchemy.orm import Session
from src.core.interfaces import IOrderRepository
from src.core.orm_models import Order, Trade, Position, BacktestTradeLog

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

    def update_position(self, strategy_id: str, product_id: str, side: str, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None:
        # Use with_for_update for locking
        position = self.db.query(Position).with_for_update().filter_by(
            strategy_id=strategy_id, 
            product_id=product_id, 
            side=position_side
        ).first()

        current_time = int(time.time() * 1000)

        if not position:
            if side == 'buy':
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

        if side == 'buy':
            total_cost = (position.quantity * position.entry_price) + (fill_quantity * fill_price)
            total_qty = position.quantity + fill_quantity
            if total_qty > 0:
                position.entry_price = total_cost / total_qty
            position.quantity = total_qty
        elif side == 'sell':
            position.quantity = max(Decimal("0"), position.quantity - fill_quantity)
            
        position.last_update_timestamp = current_time
        self.db.commit()

    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        order.exchange_order_id = exchange_order_id
        self.db.commit()


class BacktestOrderRepository(IOrderRepository):
    def __init__(self, db_session: Session, session_id: int):
        self.db = db_session
        self.session_id = session_id
        # In-memory positions: Map[(strategy, product, side), Position]
        self._positions: Dict[Tuple[str, str, str], Position] = {}

    def add_order(self, order: Order) -> None:
        # Backtest orders are not persisted
        pass

    def update_order(self, order: Order) -> None:
        pass

    def add_trade(self, trade: Trade) -> None:
        # Convert Trade to BacktestTradeLog
        bt_log = BacktestTradeLog(
            id=trade.id,
            session_id=self.session_id,
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

    def get_position(self, strategy_id: str, product_id: str, side: str) -> Optional[Position]:
        return self._positions.get((strategy_id, product_id, side))

    def update_position(self, strategy_id: str, product_id: str, side: str, fill_quantity: Decimal, fill_price: Decimal, position_side: str) -> None:
        key = (strategy_id, product_id, position_side)
        current_time = int(time.time() * 1000)
        
        position = self._positions.get(key)
        
        if not position:
            if side == 'buy':
                position = Position(
                    strategy_id=strategy_id,
                    product_id=product_id,
                    side=position_side,
                    quantity=Decimal("0"),
                    entry_price=Decimal("0"),
                    unrealized_pnl=Decimal("0"),
                    last_update_timestamp=current_time
                )
                self._positions[key] = position
            else:
                return

        if side == 'buy':
            total_cost = (position.quantity * position.entry_price) + (fill_quantity * fill_price)
            total_qty = position.quantity + fill_quantity
            if total_qty > 0:
                position.entry_price = total_cost / total_qty
            position.quantity = total_qty
        elif side == 'sell':
            position.quantity = max(Decimal("0"), position.quantity - fill_quantity)
            
        position.last_update_timestamp = current_time
    
    def update_order_exchange_id(self, order: Order, exchange_order_id: str) -> None:
        order.exchange_order_id = exchange_order_id
