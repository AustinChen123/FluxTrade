import logging
from decimal import Decimal
from typing import Optional
from sqlalchemy.orm import Session
from src.core.models import Signal, SignalType, Candlestick
from src.core.order_manager import OrderManager
from src.core.interfaces.exchange import IExchangeAdapter
from src.core.clock import Clock
from src.core.interfaces import IOrderRepository

class ExecutionEngine:
    def __init__(self, db_session: Session, clock: Clock, adapter: IExchangeAdapter, order_repository: Optional[IOrderRepository] = None):
        self.logger = logging.getLogger("ExecutionEngine")
        if order_repository:
            self.order_manager = OrderManager(order_repository, clock)
        else:
            from src.core.repositories import LiveOrderRepository
            self.order_manager = OrderManager(LiveOrderRepository(db_session), clock)
            
        self.default_quantity = Decimal("0.01")
        self.adapter = adapter
        self.logger.info(f"ExecutionEngine initialized with adapter: {type(adapter).__name__}")

    def process_market_data(self, candle: Candlestick):
        """
        Passes market data to the adapter (if applicable) to check for simulated fills.
        """
        # Only SimulatedAdapter needs this loop. 
        # Using hasattr to allow any adapter that implements the simulation hook.
        if hasattr(self.adapter, "on_market_data"):
            fills = self.adapter.on_market_data(candle)
            
            for fill in fills:
                order = fill['order']
                price = fill['price']
                qty = fill['quantity']
                
                self.logger.info(f"⚡ Execution: Adapter reported fill for {order.id} at {price}")
                self.order_manager.fill_order(
                    order=order,
                    fill_price=price,
                    fill_quantity=qty
                )

    def execute_signal(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """
        Converts Signal to Order and delegates execution to the Adapter.
        Returns the Order ID (Internal) if successful.
        """
        side = self._determine_side(signal.type)
        if not side:
            return None

        # Determine Quantity
        qty = signal.quantity if signal.quantity and signal.quantity > 0 else self.default_quantity

        # Determine Order Type and Price
        if signal.price and signal.price > 0:
            order_type = "limit"
            limit_price = signal.price
        elif signal.value: # Legacy support
            order_type = "limit"
            limit_price = signal.value
        else:
            order_type = "market"
            limit_price = None

        # 1. Create Order in DB (Open)
        order = self.order_manager.create_order(
            signal=signal,
            side=side,
            order_type=order_type,
            quantity=qty,
            price=limit_price
        )

        # 2. Execute via Adapter
        try:
            self.logger.info(f"🚀 Sending Order {order.id} via Adapter...")
            exchange_id = self.adapter.place_order(order)
            
            # Update with the ID returned by adapter
            # Note: LiveBinanceAdapter might return "WS-{id}" for async orders
            self.order_manager.update_exchange_order_id(order, exchange_id)
            
            self.logger.info(f"✅ Order Placed. Internal: {order.id}, Exchange: {exchange_id}")
            return order.id

        except Exception as e:
            self.logger.error(f"❌ Execution Failed: {e}")
            self.order_manager.fail_order(order, str(e))
            return None

    def _determine_side(self, signal_type: SignalType) -> Optional[str]:
        if signal_type == SignalType.LONG:
            return "buy"
        elif signal_type == SignalType.SHORT:
            return "sell"
        elif signal_type == SignalType.EXIT_LONG:
            return "sell"
        elif signal_type == SignalType.EXIT_SHORT:
            return "buy"
        return None
