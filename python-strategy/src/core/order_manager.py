import logging
import uuid
import os
import redis
from decimal import Decimal
from typing import Optional
from src.core.orm_models import Order, Trade
from src.core.models import Signal
from src.core.clock import Clock
from src.core.interfaces import IOrderRepository
from src.core.redis_factory import create_redis_client

logger = logging.getLogger(__name__)

_VALID_SIDES = {"buy", "sell"}
_VALID_ORDER_TYPES = {"market", "limit", "stop_loss", "take_profit", "trailing_stop"}

class OrderManager:
    def __init__(self, repo: IOrderRepository, clock: Clock, is_backtest: Optional[bool] = None):
        self.repo = repo
        self.clock = clock
        self.redis_client = None
        self.update_position_script = None

        # Detect Backtest Mode: explicit flag > repository type heuristic
        if is_backtest is not None:
            self.is_backtest = is_backtest
        else:
            self.is_backtest = "BacktestOrderRepository" in str(type(repo))

        if not self.is_backtest:
            self.redis_client = create_redis_client()
            # Load Lua Script
            lua_path = os.path.join(os.path.dirname(__file__), '../lua/update_position.lua')
            try:
                with open(lua_path, 'r') as f:
                    self.update_position_script = self.redis_client.register_script(f.read())
            except Exception as e:
                logger.error("FATAL: Failed to load Lua script: %s", e)
                raise e
        else:
             logger.info("OrderManager: Initialized in Backtest Mode (Redis Disabled).")

    def create_order(
        self,
        signal: Signal,
        side: str,
        order_type: str,
        quantity: Decimal,
        price: Optional[Decimal] = None,
        trigger_price: Optional[Decimal] = None,
    ) -> Order:
        if side.lower() not in _VALID_SIDES:
            raise ValueError(f"Invalid order side: {side!r}. Must be one of {_VALID_SIDES}")
        if order_type.lower() not in _VALID_ORDER_TYPES:
            raise ValueError(f"Invalid order type: {order_type!r}. Must be one of {_VALID_ORDER_TYPES}")
        exchange_id = signal.product_id.split(':')[0]
        order_id = str(uuid.uuid4())

        new_order = Order(
            id=order_id,
            exchange_order_id=f"sim_{order_id[:8]}",
            strategy_id=signal.strategy_id,
            product_id=signal.product_id,
            exchange_id=exchange_id,
            type=order_type,
            side=side,
            price=price,
            trigger_price=trigger_price,
            quantity=quantity,
            status="open",
            timestamp=int(self.clock.now() * 1000),
            filled_quantity=Decimal("0"),
            filled_price=Decimal("0")
        )

        self.repo.add_order(new_order)
        logger.info("Order created %s (%s %s %s %s)", new_order.id, side, order_type, quantity, signal.product_id)
        return new_order

    def update_exchange_order_id(self, order: Order, exchange_order_id: str):
        self.repo.update_order_exchange_id(order, exchange_order_id)

    def fail_order(self, order: Order, reason: str):
        """Marks an order as FAILED due to execution errors."""
        order.status = "failed"
        # We could verify if there's a specific field for error msg, but for now just status
        logger.error("ORDER_FAILED: Order %s marked as FAILED. Reason: %s", order.id, reason)
        self.repo.update_order(order)

    def fill_order(self, order: Order, fill_price: Decimal, fill_quantity: Decimal, fee: Optional[Decimal] = None):
        current_time = int(self.clock.now() * 1000)

        # 1. Update Order in DB
        order.status = "closed"
        order.filled_quantity = fill_quantity
        order.filled_price = fill_price
        self.repo.update_order(order)

        trade_id = str(uuid.uuid4())

        # 2. Atomic Execution
        if not self.is_backtest:
            # Redis Lua (live mode)
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
                logger.info("Redis: Atomic Position Update Successful (Trade %s)", trade_id)

            except redis.exceptions.ResponseError as e:
                logger.error("FATAL: Redis Lua Script Error: %s", e)
                raise RuntimeError(f"Critical State Corruption: {e}")
            except Exception as e:
                logger.error("FATAL: System Error during execution: %s", e)
                raise e
        else:
            # Backtest Mode: position/balance managed by Rust matching engine
            self.repo.update_position(
                strategy_id=order.strategy_id,
                product_id=order.product_id,
                side=order.side,
                fill_quantity=fill_quantity,
                fill_price=fill_price,
                position_side=order.side.upper(),
            )

        # 3. Create Trade record
        new_trade = Trade(
            id=trade_id,
            order_id=order.id,
            exchange_trade_id=f"trd_{trade_id[:8]}",
            product_id=order.product_id,
            side=order.side,
            price=fill_price,
            quantity=fill_quantity,
            fee=fee if fee is not None else Decimal("0"),
            fee_asset="USDT",
            timestamp=current_time
        )
        self.repo.add_trade(new_trade)