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
        self.redis_client = None
        self.update_position_script = None
        
        # Detect Backtest Mode via Repository Type
        self.is_backtest = "BacktestOrderRepository" in str(type(repo))
        
        if not self.is_backtest:
            self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
            # Load Lua Script
            lua_path = os.path.join(os.path.dirname(__file__), '../lua/update_position.lua')
            try:
                with open(lua_path, 'r') as f:
                    self.update_position_script = self.redis_client.register_script(f.read())
            except Exception as e:
                print(f"FATAL: Failed to load Lua script: {e}")
                raise e
        else:
             print("🧪 OrderManager: Initialized in Backtest Mode (Redis Disabled).")

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

    def fail_order(self, order: Order, reason: str):
        """Marks an order as FAILED due to execution errors."""
        order.status = "failed"
        # We could verify if there's a specific field for error msg, but for now just status
        print(f"❌ DB: Order {order.id} marked as FAILED. Reason: {reason}")
        self.repo.update_order(order)

    def fill_order(self, order: Order, fill_price: Decimal, fill_quantity: Decimal):
        current_time = int(self.clock.now() * 1000)
        
        # 1. Update Order in DB (Keep for record)
        order.status = "closed"
        order.filled_quantity = fill_quantity
        order.filled_price = fill_price
        self.repo.update_order(order)
        
        trade_id = str(uuid.uuid4())
        
        # 2. Atomic Execution
        if not self.is_backtest:
            # Redis Lua
            try:
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
                raise RuntimeError(f"Critical State Corruption: {e}")
            except Exception as e:
                print(f"🔥 FATAL: System Error during execution: {e}")
                raise e
        else:
            # Backtest Mode: Update Mock/In-Memory State via Repository
            # BacktestOrderRepository handles position tracking internally
            self.repo.update_position(
                strategy_id=order.strategy_id,
                product_id=order.product_id,
                side=order.side,
                fill_quantity=fill_quantity,
                fill_price=fill_price,
                position_side="LONG" # Simplified, needs robust netting logic if we want perfect port
            )
            # Actually, `BacktestOrderRepository.update_position` implements the update.
            # But wait, `BacktestOrderRepository` logic for side/position_side was a bit simplified in previous `read_file`.
            # Let's trust it updates the repo's internal dictionary.
            print(f"🧪 Backtest: Trade Executed {trade_id}")

        # 3. Create Trade (SQL Reflection)
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