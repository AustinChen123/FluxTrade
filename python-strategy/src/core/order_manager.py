import uuid
import os
import redis
from decimal import Decimal
from typing import Optional
from src.core.orm_models import Order, Trade, Position
from src.core.models import Signal
from src.core.clock import Clock
from src.core.interfaces import IOrderRepository

# Redis Config
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

class OrderManager:
    def __init__(self, repo: IOrderRepository, clock: Clock):
        self.repo = repo
        self.clock = clock
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        
        # Load Lua Script
        lua_path = os.path.join(os.path.dirname(__file__), '../lua/update_position.lua')
        try:
            with open(lua_path, 'r') as f:
                self.update_position_script = self.redis_client.register_script(f.read())
        except Exception as e:
            print(f"FATAL: Failed to load Lua script: {e}")
            raise e

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
        
        self.repo.add_order(new_order)
        print(f"📝 DB: Order created {new_order.id} ({side} {quantity} {signal.product_id})")
        return new_order

    def update_exchange_order_id(self, order: Order, exchange_order_id: str):
        self.repo.update_order_exchange_id(order, exchange_order_id)

    def fill_order(self, order: Order, fill_price: Decimal, fill_quantity: Decimal):
        current_time = int(self.clock.now() * 1000)
        
        # 1. Update Order in DB (Keep for record)
        order.status = "closed"
        order.filled_quantity = fill_quantity
        order.filled_price = fill_price
        self.repo.update_order(order)
        
        # 2. Atomic Execution via Redis Lua
        # "Atomic Execution (Mandatory)... If Lua script returns error... throw Fatal Exception"
        
        trade_id = str(uuid.uuid4())
        
        try:
            # Lua Args: account_id, strategy_id, product_id, side, quantity, price, timestamp, trade_id, order_id
            # Assuming account_id is 'default' or passed in context. Using 'main' for now.
            account_id = "main" 
            
            self.update_position_script(
                args=[
                    account_id,
                    order.strategy_id,
                    order.product_id,
                    order.side.upper(),
                    str(fill_quantity),
                    str(fill_price),
                    str(current_time),
                    trade_id,
                    order.id
                ]
            )
            print(f"⚡ Redis: Atomic Position Update Successful (Trade {trade_id})")
            
        except redis.exceptions.ResponseError as e:
            print(f"🔥 FATAL: Redis Lua Script Error: {e}")
            # "throw a Fatal Exception and crash the process. Do NOT retry."
            raise RuntimeError(f"Critical State Corruption: {e}")
        except Exception as e:
             print(f"🔥 FATAL: System Error during execution: {e}")
             raise e

        # 3. Create Trade (SQL Reflection - Optional but good for backup)
        # Note: In strict HFT, we might skip this or do it async. 
        # But keeping it for hybrid robustness as per "Postgres is just for backup".
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
        self.repo.add_trade(new_trade)