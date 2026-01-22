import os
from decimal import Decimal
from typing import Optional
from sqlalchemy.orm import Session
from src.core.models import Signal, SignalType
from src.core.order_manager import OrderManager
from src.core.exchange_adapter import ExchangeAdapter

class ExecutionEngine:
    def __init__(self, db_session: Session):
        self.order_manager = OrderManager(db_session)
        self.default_quantity = Decimal("0.01")
        self.adapter: Optional[ExchangeAdapter] = None
        
        # Try to initialize Real Exchange Adapter
        api_key = os.getenv('EXCHANGE_API_KEY')
        secret = os.getenv('EXCHANGE_SECRET')
        exchange_id = os.getenv('EXCHANGE_ID', 'binance')
        testnet = os.getenv('EXCHANGE_TESTNET', 'true').lower() == 'true'
        
        if api_key and secret:
            try:
                self.adapter = ExchangeAdapter(exchange_id, api_key, secret, testnet)
            except Exception as e:
                print(f"⚠️  Execution: Failed to init ExchangeAdapter ({e}). Fallback to Mock.")
        else:
            print("⚠️  Execution: No API Key found. Running in Mock Mode.")

    def execute_signal(self, signal: Signal) -> Optional[str]:
        """
        Converts Signal to Order and executes it (Mock).
        Returns the order_id if successful.
        """
        side = self._determine_side(signal.type)
        if not side:
            return None

        price = signal.value if signal.value else Decimal("50000")
        
        # 1. Create Order in DB (Open)
        order = self.order_manager.create_order(
            signal=signal,
            side=side,
            order_type="market",
            quantity=self.default_quantity,
            price=None
        )

        # 2. Execute (Real or Mock)
        if self.adapter:
            # REAL EXECUTION
            try:
                # CCXT requires float
                response = self.adapter.create_order(
                    symbol=signal.product_id,
                    type='market',
                    side=side,
                    amount=float(self.default_quantity)
                )
                print(f"✅ Real Order Placed: {response['id']}")
                self.order_manager.update_exchange_order_id(order, str(response['id']))
                return order.id
            except Exception as e:
                print(f"❌ Real Execution Failed: {e}")
                return None
        else:
            # MOCK EXECUTION (Immediate Fill)
            fill_price = price
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
