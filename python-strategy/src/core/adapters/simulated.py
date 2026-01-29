import uuid
from decimal import Decimal
from typing import Optional, List, Dict
from src.core.interfaces.exchange import IExchangeAdapter, ExchangeError, InsufficientFundsError
from src.core.orm_models import Order
from src.core.models import Position, Candlestick
from src.core.simulation import SlippageModel

class SimulatedAdapter(IExchangeAdapter):
    def __init__(self, initial_balance: Decimal = Decimal("100000")):
        self.balance = {"USDT": initial_balance}
        self.positions: Dict[str, Position] = {} # product_id -> Position
        self.open_orders: List[Order] = []
        self.slippage_model = SlippageModel()
        self.filled_orders_buffer = [] # Store fills to return to engine

    def place_order(self, order: Order) -> str:
        # Generate a simulated exchange ID
        exchange_id = f"SIM-{uuid.uuid4().hex[:8]}"
        
        # Validate Funds (Simple check)
        cost = order.quantity * (order.price if order.price else Decimal("0")) # Approximation for market
        # In a real sim, we'd check margin, leverage, etc. 
        # For now, we assume infinite margin or simple spot-like check if needed.
        
        if order.type.lower() == "market":
            # Market Order: Fills immediately (conceptually)
            # In simulation loop, we might need the CURRENT price.
            # Assuming the engine calls place_order with knowledge of current price 
            # OR we wait for next tick.
            # However, execute_signal usually has a 'candle' context or we use the latest known.
            # To strictly follow "Adapter" pattern, we can't fetch external data.
            # We'll queue it as a "Market" order to be filled at next 'on_market_data' tick 
            # OR if we want immediate fill, we need the price passed in.
            # Refactoring decision: Treat Market orders as immediate fills if price provided in order (as strict limit) 
            # or queue them. 
            # Existing engine logic: execute_signal calculates fill_price immediately for Market.
            # Let's Queue it for 'on_market_data' to be realistic? 
            # No, standard backtest engines often fill on Next Open or Close.
            # Let's store it and fill in `on_market_data`.
            order.exchange_order_id = exchange_id
            self.open_orders.append(order)
            return exchange_id

        elif order.type.lower() == "limit":
            order.exchange_order_id = exchange_id
            self.open_orders.append(order)
            return exchange_id
        
        else:
            raise ExchangeError(f"Unsupported order type: {order.type}")

    def cancel_order(self, order_id: str, product_id: str) -> bool:
        initial_len = len(self.open_orders)
        self.open_orders = [o for o in self.open_orders if o.exchange_order_id != order_id]
        return len(self.open_orders) < initial_len

    def get_balance(self, asset: str) -> Decimal:
        return self.balance.get(asset, Decimal("0"))

    def get_position(self, product_id: str) -> Optional[Position]:
        return self.positions.get(product_id)

    def on_market_data(self, candle: Candlestick) -> List[Dict]:
        """
        Process market data to trigger fills.
        Returns a list of fill dictionaries {order_id, price, quantity, fee}.
        """
        fills = []
        remaining_orders = []

        for order in self.open_orders:
            if order.product_id != candle.product_id:
                remaining_orders.append(order)
                continue

            filled = False
            fill_price = None

            if order.type.lower() == 'market':
                # Fill at Open or Close? Usually Close of the triggering candle or Open of next.
                # Simplification: Fill at Close with slippage
                fill_price = self.slippage_model.calculate_slippage(candle.close)
                filled = True

            elif order.type.lower() == 'limit':
                # Check High/Low
                if candle.low <= order.price <= candle.high:
                    fill_price = order.price
                    filled = True

            if filled and fill_price:
                # Update Internal State (Position)
                self._update_position(order, fill_price)
                
                # Record Fill
                fills.append({
                    "order": order,
                    "price": fill_price,
                    "quantity": order.quantity
                })
            else:
                remaining_orders.append(order)

        self.open_orders = remaining_orders
        return fills

    def _update_position(self, order: Order, price: Decimal):
        """
        Updates the simulated position state.
        Simple Netting Mode.
        """
        pos = self.positions.get(order.product_id)
        
        qty = order.quantity
        if order.side == "sell":
            qty = -qty
            
        if not pos:
            # New Position
            new_pos = Position(
                strategy_id=order.strategy_id,
                product_id=order.product_id,
                side="LONG" if order.side.lower() == "buy" else "SHORT",
                quantity=order.quantity,
                entry_price=price,
                unrealized_pnl=Decimal("0")
            )
            self.positions[order.product_id] = new_pos
        else:
            # Update Existing
            current_qty = pos.quantity if pos.side == "LONG" else -pos.quantity
            new_qty = current_qty + qty
            
            if new_qty == 0:
                del self.positions[order.product_id]
            else:
                # Update Avg Entry Price logic (simplified)
                # If increasing position size, average price.
                # If decreasing, price stays same (Realized PnL logic not fully implemented in state, mostly in Engine)
                is_increase = (current_qty > 0 and qty > 0) or (current_qty < 0 and qty < 0)
                
                if is_increase:
                    total_cost = (abs(current_qty) * pos.entry_price) + (abs(qty) * price)
                    new_avg = total_cost / abs(new_qty)
                    pos.entry_price = new_avg
                
                pos.quantity = abs(new_qty)
                pos.side = "LONG" if new_qty > 0 else "SHORT"
