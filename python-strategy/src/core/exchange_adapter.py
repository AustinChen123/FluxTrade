import ccxt
import logging
from typing import Optional, Dict, Any

from src.core.product_registry import to_ccxt_symbol

logger = logging.getLogger(__name__)


class ExchangeAdapter:
    def __init__(self, exchange_id: str, api_key: str, secret: str, testnet: bool = True):
        self.exchange_id = exchange_id.lower()

        if not hasattr(ccxt, self.exchange_id):
            raise ValueError(f"Exchange {exchange_id} not supported by CCXT")

        exchange_class = getattr(ccxt, self.exchange_id)
        self.client = exchange_class({
            'apiKey': api_key,
            'secret': secret,
            'enableRateLimit': True,
            'options': {'defaultType': 'swap'},
        })

        if testnet:
            self.client.set_sandbox_mode(True)

        logger.info("ExchangeAdapter connected to %s (%s)", self.exchange_id, "Testnet" if testnet else "Live")

    def create_order(self, symbol: str, type: str, side: str, amount: float, price: Optional[float] = None) -> Dict[str, Any]:
        """Place an order via CCXT. Returns the raw exchange response."""
        ccxt_symbol = self._map_symbol(symbol)

        try:
            logger.info("CCXT order: %s %s %s @ %s", side, amount, ccxt_symbol, type)
            return self.client.create_order(
                symbol=ccxt_symbol,
                type=type,
                side=side,
                amount=amount,
                price=price,
            )
        except Exception as e:
            logger.error("CCXT order failed: %s", e)
            raise

    def _map_symbol(self, internal_symbol: str) -> str:
        """Convert product_id to CCXT symbol via product_registry."""
        return to_ccxt_symbol(internal_symbol)
