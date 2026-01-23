import uuid
from decimal import Decimal
from typing import Optional
from sqlalchemy.orm import Session
from src.core.orm_models import Order, Trade, Position
from src.core.models import Signal
from src.core.clock import Clock

class OrderManager:
    def __init__(self, db_session: Session, clock: Clock):
        self.db = db_session
        self.clock = clock

    def create_order(self, signal: Signal, side: str, order_type: str, quantity: Decimal, price: Optional[Decimal] = None) -> Order:
        exchange_id = signal.product_id.split(':')[0]
        order_id = str(uuid.uuid4())
        
        new_order = Order(
            id=order_id,
            exchange_order_id=f"sim_{order_id[:8]}", # Default mock ID, will be updated if real
            strategy_id=signal.strategy_id,
            product_id=signal.product_id,
            exchange_id=exchange_id,
            type=order_type,
            side=side,
            price=price,
            quantity=quantity,
            status="open",
            timestamp=int(self.clock.now() * 1000),
            filled_quantity=Decimal("0"),
            filled_price=Decimal("0")
        )
        
        self.db.add(new_order)
        self.db.commit()
        self.db.refresh(new_order)
        print(f"📝 DB: Order created {new_order.id} ({side} {quantity} {signal.product_id})")
        return new_order

    def update_exchange_order_id(self, order: Order, exchange_order_id: str):
        order.exchange_order_id = exchange_order_id
        self.db.commit()

    def fill_order(self, order: Order, fill_price: Decimal, fill_quantity: Decimal):
        current_time = int(self.clock.now() * 1000)
        
        # 1. Update Order
        order.status = "closed"
        order.filled_quantity = fill_quantity
        order.filled_price = fill_price
        
        # 2. Create Trade
        trade_id = str(uuid.uuid4())
        new_trade = Trade(
            id=trade_id,
            order_id=order.id,
            exchange_trade_id=f"trd_{trade_id[:8]}",
            product_id=order.product_id,
            side=order.side,
            price=fill_price,
            quantity=fill_quantity,
            fee=Decimal("0"),
            fee_asset="USDT",
            timestamp=current_time
        )
        self.db.add(new_trade)
        
        # 3. Update Position (Simplified)
        target_pos_side = "LONG"
        position = self.db.query(Position).filter_by(
            strategy_id=order.strategy_id, 
            product_id=order.product_id,
            side=target_pos_side
        ).first()
        
        if not position:
            if order.side == 'buy':
                position = Position(
                    strategy_id=order.strategy_id,
                    product_id=order.product_id,
                    side=target_pos_side,
                    quantity=Decimal("0"),
                    entry_price=Decimal("0"),
                    unrealized_pnl=Decimal("0"),
                    last_update_timestamp=current_time
                )
                self.db.add(position)
            else:
                self.db.commit()
                return

        if order.side == 'buy':
            total_cost = (position.quantity * position.entry_price) + (fill_quantity * fill_price)
            total_qty = position.quantity + fill_quantity
            position.entry_price = total_cost / total_qty
            position.quantity = total_qty
        elif order.side == 'sell':
            position.quantity = max(Decimal("0"), position.quantity - fill_quantity)
            
        position.last_update_timestamp = current_time
        self.db.commit()
        print(f"💰 DB: Trade recorded & Position updated. Pos Qty: {position.quantity}")
