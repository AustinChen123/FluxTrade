import logging
import os
from decimal import Decimal
from typing import Optional
import ccxt
from src.core.interfaces.exchange import IExchangeAdapter, ExchangeError, InsufficientFundsError, NetworkError
from src.core.exchange_adapter import ExchangeAdapter as CCXTAdapter
from src.core.product_registry import to_ccxt_symbol
from src.core.ws_connector import WebSocketOrderConnector
from src.core.orm_models import Order
from src.core.models import Position

class LiveBinanceAdapter(IExchangeAdapter):
    def __init__(self, exchange_id: str = "binance", api_key: str = None, secret: str = None, testnet: bool = True):
        self.logger = logging.getLogger("LiveBinanceAdapter")
        
        # Load credentials from env if not provided
        self.api_key = api_key or os.getenv('EXCHANGE_API_KEY')
        self.secret = secret or os.getenv('EXCHANGE_SECRET')
        self.exchange_id = exchange_id
        self.testnet = testnet

        if not self.api_key or not self.secret:
            self.logger.warning("⚠️ No API Key found for LiveBinanceAdapter.")

        # Initialize CCXT (REST)
        try:
            self.ccxt_adapter = CCXTAdapter(self.exchange_id, self.api_key, self.secret, self.testnet)
        except Exception as e:
            raise ExchangeError(f"Failed to initialize CCXT: {e}")

        # Initialize WebSocket
        self.ws_connector = WebSocketOrderConnector(self.api_key, self.secret, self.exchange_id, self.testnet)
        self.ws_connector.start()

    def place_order(self, order: Order) -> str:
        """
        Places an order using WS (preferred for Market) or REST.
        """
        symbol = order.product_id
        side = order.side # "buy" or "sell"
        order_type = order.type # "limit" or "market"
        quantity = float(order.quantity)
        price = float(order.price) if order.price else None

        # 1. Try WebSocket (Only for Market orders usually, or if supported)
        # Note: Existing WebSocketOrderConnector implementation logic suggests preference for Market
        if self.ws_connector.is_connected(self.exchange_id):
            if order_type.lower() == 'market':
                try:
                    self.logger.info(f"🚀 Sending WS Order: {side} {quantity} {symbol}")
                    success = self.ws_connector.place_order(
                        symbol=symbol,
                        side=side,
                        quantity=quantity,
                        price=price,
                        order_type=order_type
                    )
                    if success:
                        # WS is async fire-and-forget, we don't have exchange ID yet.
                        # Return a placeholder that indicates WS source.
                        return f"WS-{order.id}"
                except Exception as e:
                    self.logger.error(f"⚠️ WS Order failed: {e}. Falling back to REST.")
        
        # 2. Fallback to REST
        try:
            order_params = {
                "symbol": symbol,
                "type": order_type,
                "side": side,
                "amount": quantity,
                "price": price
            }
            if order_type == 'limit':
                order_params["params"] = {"timeInForce": "GTC"}

            # Use the underlying CCXT client directly or via wrapper
            # wrapper 'create_order' maps internal symbol to CCXT symbol
            response = self.ccxt_adapter.create_order(**order_params)
            return str(response['id'])

        except ccxt.InsufficientFunds as e:
            raise InsufficientFundsError(f"Insufficient funds: {e}")
        except ccxt.NetworkError as e:
            raise NetworkError(f"Network error: {e}")
        except Exception as e:
            raise ExchangeError(f"Order placement failed: {e}")

    def cancel_order(self, order_id: str, product_id: str) -> bool:
        if order_id.startswith("WS-"):
            self.logger.warning("Cannot cancel WS-initiated order via REST until Exchange ID is confirmed.")
            return False

        try:
            # ExchangeAdapter wrapper doesn't have cancel_order, use client directly
            # But we need symbol mapping from the wrapper
            ccxt_symbol = to_ccxt_symbol(product_id)
            self.ccxt_adapter.client.cancel_order(order_id, ccxt_symbol)
            return True
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            return False

    def get_balance(self, asset: str) -> Decimal:
        try:
            balance = self.ccxt_adapter.client.fetch_balance()
            # Handle different exchange structures (using 'free' usually)
            avail = balance.get(asset, {}).get('free', 0.0)
            if not avail and 'free' in balance:
                # Some exchanges return flat dict or different structure
                avail = balance['free'].get(asset, 0.0)
            return Decimal(str(avail))
        except Exception as e:
            raise ExchangeError(f"Failed to fetch balance: {e}")

    def get_position(self, product_id: str) -> Optional[Position]:
        try:
            ccxt_symbol = to_ccxt_symbol(product_id)
            # Fetch positions (Binance specific mostly)
            positions = self.ccxt_adapter.client.fetch_positions([ccxt_symbol])
            
            target_pos = None
            for p in positions:
                if p['symbol'] == ccxt_symbol:
                    target_pos = p
                    break
            
            if not target_pos:
                return None

            # Map to Position Model
            amt = float(target_pos['contracts']) if 'contracts' in target_pos else float(target_pos['info']['positionAmt'])
            
            if amt == 0:
                return None

            side = "LONG" if amt > 0 else "SHORT"
            
            return Position(
                strategy_id="LIVE", # Placeholder as adapter doesn't know strategy
                product_id=product_id,
                side=side,
                quantity=Decimal(str(abs(amt))),
                entry_price=Decimal(str(target_pos['entryPrice'])),
                unrealized_pnl=Decimal(str(target_pos['unrealizedPnl']))
            )
            
        except Exception as e:
            self.logger.error(f"Failed to fetch position: {e}")
            raise ExchangeError(f"Failed to fetch position: {e}")
