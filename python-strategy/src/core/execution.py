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
    def __init__(self, db_session: Session, clock: Clock, order_repository: Optional[IOrderRepository] = None, mock_only: bool = False):
        if order_repository:
            self.order_manager = OrderManager(order_repository, clock)
        else:
            from src.core.repositories import LiveOrderRepository
            self.order_manager = OrderManager(LiveOrderRepository(db_session), clock)
            
        self.default_quantity = Decimal("0.01")
        self.slippage_model = SlippageModel()
        self.adapter: Optional[ExchangeAdapter] = None
        self.ws_connector: Optional[WebSocketOrderConnector] = None
        self.mock_only = mock_only
        
        # Simulation State
        self.open_orders: list = []
        
        if not self.mock_only:
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
        else:
             print("🧪 Execution: Running in MOCK ONLY mode (Backtest/Simulation).")

    def check_open_orders(self, candle: Candlestick):
        """
        Checks pending mock orders against the new candle.
        """
        if not self.open_orders:
            return

        filled_orders = []
        for order in self.open_orders:
            if order.product_id != candle.product_id:
                continue
            
            # Check Limit Match
            if candle.low <= order.price <= candle.high:
                print(f"⚡ Execution: Resting Limit Order {order.id} filled at {order.price}")
                self.order_manager.fill_order(
                    order=order,
                    fill_price=order.price,
                    fill_quantity=order.quantity
                )
                filled_orders.append(order)
        
        # Remove filled
        for order in filled_orders:
            self.open_orders.remove(order)

    def execute_signal(self, signal: Signal, candle: Optional[Candlestick] = None) -> Optional[str]:
        """
        Converts Signal to Order and executes it (WS -> REST -> Mock).
        Returns the order_id if successful.
        """
        side = self._determine_side(signal.type)
        if not side:
            return None

        # Determine Quantity
        qty = signal.quantity if signal.quantity and signal.quantity > 0 else self.default_quantity

        # Determine Order Type and Price
        # Prioritize signal.price (Entry Price) for Limit Orders
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

        # 2. Execute (Real or Mock)
        if self.adapter:
            # REAL EXECUTION
            try:
                # Attempt WebSocket First
                ws_success = False
                if self.ws_connector and self.ws_connector.is_connected(os.getenv('EXCHANGE_ID', 'binance')):
                    print(f"🚀 Sending order via WebSocket...")
                    if order_type.lower() == 'market':
                        # WS currently only implemented for Market in this scope logic
                        ws_success = self.ws_connector.place_order(
                            symbol=signal.product_id,
                            side=side,
                            quantity=float(qty),
                            price=float(limit_price) if limit_price else 0.0,
                            order_type=order_type
                        )
                
                if ws_success:
                    print(f"✅ WS Order success: {order.id}")
                    return order.id
                else:
                    # FALLBACK TO REST
                    print(f"🔌 WebSocket unavailable/failed. Falling back to REST for {order.id}")
                    
                    order_params = {
                        "symbol": signal.product_id,
                        "side": side,
                        "amount": float(qty),
                        "type": order_type
                    }
                    if order_type == 'limit':
                        order_params["price"] = float(limit_price)
                        order_params["params"] = {"timeInForce": "GTC"}

                    response = self.adapter.create_order(**order_params)
                    
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
                    print(f"⏳ Limit Order {order.id} placed in book. Price {limit_price} (Candle [{candle.low}, {candle.high}])")
                    self.open_orders.append(order)
                    return order.id
            else:
                # Market Order
                base_price = candle.close if candle else (limit_price if limit_price else Decimal("50000"))
                fill_price = self.slippage_model.calculate_slippage(base_price)

            print(f"⚡ Execution: Simulating fill for {order.id} at {fill_price}")
            self.order_manager.fill_order(
                order=order,
                fill_price=fill_price,
                fill_quantity=qty
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
