import ccxt
from typing import Optional, Dict, Any

class ExchangeAdapter:
    def __init__(self, exchange_id: str, api_key: str, secret: str, testnet: bool = True):
        self.exchange_id = exchange_id.lower()
        
        # Dynamic instantiation of ccxt class
        if not hasattr(ccxt, self.exchange_id):
            raise ValueError(f"Exchange {exchange_id} not supported by CCXT")
            
        exchange_class = getattr(ccxt, self.exchange_id)
        self.client = exchange_class({
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'} # Default to Perpetual Swaps
        })
        
        if testnet:
            self.client.set_sandbox_mode(True)
            print(f"🔌 ExchangeAdapter: Connected to {self.exchange_id} (Testnet)")
        else:
            print(f"🔌 ExchangeAdapter: Connected to {self.exchange_id} (Live)")

    def create_order(self, symbol: str, type: str, side: str, amount: float, price: Optional[float] = None) -> Dict[str, Any]:
        """
        Places an order via CCXT.
        Returns the raw exchange response.
        """
        # Convert custom symbol format (BINANCE:BTCUSDT-PERP) to CCXT format (BTC/USDT)
        ccxt_symbol = self._map_symbol(symbol)
        
        try:
            print(f"🚀 CCXT Sending: {side} {amount} {ccxt_symbol} @ {type}")
            return self.client.create_order(
                symbol=ccxt_symbol,
                type=type,
                side=side,
                amount=amount,
                price=price
            )
        except Exception as e:
            print(f"❌ CCXT Error: {e}")
            raise e

    def _map_symbol(self, internal_symbol: str) -> str:
        # Expected: BINANCE:BTCUSDT-PERP -> BTC/USDT (simplification)
        parts = internal_symbol.split(':')
        if len(parts) > 1:
            s = parts[1] # BTCUSDT-PERP
            s = s.replace('-PERP', '')
            if s.endswith('USDT'):
                base = s[:-4]
                return f"{base}/USDT"
        return internal_symbol
