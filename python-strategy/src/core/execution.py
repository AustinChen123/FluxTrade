import os
from decimal import Decimal
from typing import Optional
from sqlalchemy.orm import Session
from src.core.models import Signal, SignalType, Candlestick
from src.core.order_manager import OrderManager
from src.core.exchange_adapter import ExchangeAdapter
from src.core.ws_connector import WebSocketOrderConnector
from src.core.clock import Clock
from src.core.simulation import SlippageModel
from src.core.interfaces import IOrderRepository

class ExecutionEngine:
    def __init__(self, db_session: Session, clock: Clock, order_repository: Optional[IOrderRepository] = None):
        if order_repository:
            self.order_manager = OrderManager(order_repository, clock)
        else:
            from src.core.repositories import LiveOrderRepository
            self.order_manager = OrderManager(LiveOrderRepository(db_session), clock)
            
        self.default_quantity = Decimal("0.01")
        self.slippage_model = SlippageModel()
        self.adapter: Optional[ExchangeAdapter] = None
        self.ws_connector: Optional[WebSocketOrderConnector] = None
        
        # Try to initialize Real Exchange Adapter & WS Connector
        api_key = os.getenv('EXCHANGE_API_KEY')
        secret = os.getenv('EXCHANGE_SECRET')
        exchange_id = os.getenv('EXCHANGE_ID', 'binance')
        testnet = os.getenv('EXCHANGE_TESTNET', 'true').lower() == 'true'
        
        if api_key and secret:
            try:
                # 1. REST Adapter
                self.adapter = ExchangeAdapter(exchange_id, api_key, secret, testnet)
                
                # 2. WebSocket Connector
                self.ws_connector = WebSocketOrderConnector(api_key, secret, exchange_id, testnet)
                self.ws_connector.start()
                
            except Exception as e:
                print(f"⚠️  Execution: Failed to init Exchange Connectivity ({e}). Fallback to Mock.")
        else:
            print("⚠️  Execution: No API Key found. Running in Mock Mode.")

    def execute_signal(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """
        Converts Signal to Order and executes it (WS -> REST -> Mock).
        Returns the order_id if successful.
        """
        side = self._determine_side(signal.type)
        if not side:
            return None

        # Determine Order Type and Price
        if signal.value:
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
            quantity=self.default_quantity,
            price=limit_price
        )

        # 2. Execute (Real or Mock)
        if self.adapter:
            # REAL EXECUTION
            try:
                # Attempt WebSocket First
                ws_success = False
                if self.ws_connector and self.ws_connector.is_connected(exchange_id):
                    print(f"🚀 Sending order via WebSocket ({exchange_id})...")
                    if order_type.lower() == 'market':
                        # WS currently only implemented for Market in this scope logic
                        ws_success = self.ws_connector.place_order(
                            symbol=signal.product_id,
                            side=side,
                            quantity=float(self.default_quantity),
                            price=float(limit_price) if limit_price else 0.0,
                            order_type=order_type
                        )
                
                if ws_success:
                    print(f"✅ WS Order success: {order.id}")
                    return order.id
                else:
                    # FALLBACK TO REST
                    print(f"🔌 WebSocket unavailable/failed. Falling back to REST for {order.id}")
                    response = self.adapter.create_order(
                        symbol=signal.product_id,
                        type='market', # Default to market for now
                        side=side,
                        amount=float(self.default_quantity)
                    )
                    print(f"✅ REST Order Placed: {response['id']}")
                    self.order_manager.update_exchange_order_id(order, str(response['id']))
                    return order.id
            except Exception as e:
                print(f"❌ Real Execution Failed: {e}")
                return None
        else:
            # MOCK EXECUTION
            fill_price = None
            
            if order_type == "limit":
                if not candle:
                    print("⚠️ Cannot execute Limit Order without Candle data.")
                    return None
                
                # Check High/Low
                if candle.low <= limit_price <= candle.high:
                    fill_price = limit_price
                else:
                    print(f"⏳ Limit Order not filled. Price {limit_price} out of range [{candle.low}, {candle.high}]")
                    return None
            else:
                # Market Order
                base_price = candle.close if candle else (signal.value if signal.value else Decimal("50000"))
                fill_price = self.slippage_model.calculate_slippage(base_price)

            print(f"⚡ Execution: Simulating fill for {order.id} at {fill_price}")
            self.order_manager.fill_order(
                order=order,
                fill_price=fill_price,
                fill_quantity=self.default_quantity
            )
            return order.id

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
