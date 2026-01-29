"""Universal CCXT exchange adapter.

Implements IExchangeAdapter for any exchange supported by CCXT.
Replaces the old ExchangeAdapter (exchange_adapter.py) which did NOT
implement IExchangeAdapter and only wrapped create_order.
"""

import logging
import os
from decimal import Decimal
from typing import Optional

import ccxt

from src.core.interfaces.exchange import (
    ExchangeError,
    IExchangeAdapter,
    InsufficientFundsError,
    NetworkError,
)
from src.core.models import Position
from src.core.orm_models import Order
from src.core.product_registry import to_ccxt_symbol

logger = logging.getLogger(__name__)


class CcxtExchangeAdapter(IExchangeAdapter):
    """Universal exchange adapter via CCXT.

    Supports any CCXT-compatible exchange (Binance, Bybit, Backpack, etc.)
    through a single implementation.
    """

    def __init__(
        self,
        exchange_id: str,
        api_key: str | None = None,
        secret: str | None = None,
        testnet: bool = False,
        extra_config: dict | None = None,
    ):
        self.exchange_id = exchange_id.lower()
        self.logger = logging.getLogger(f"CcxtAdapter.{self.exchange_id}")

        api_key = api_key or os.getenv("EXCHANGE_API_KEY")
        secret = secret or os.getenv("EXCHANGE_SECRET")

        if not hasattr(ccxt, self.exchange_id):
            raise ValueError(f"Exchange '{exchange_id}' not supported by CCXT")

        exchange_cls = getattr(ccxt, self.exchange_id)
        config = {
            "apiKey": api_key,
            "secret": secret,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        }
        if extra_config:
            config.update(extra_config)

        self.client: ccxt.Exchange = exchange_cls(config)

        if testnet:
            self.client.set_sandbox_mode(True)

        self.logger.info(
            "Connected to %s (%s)", self.exchange_id, "testnet" if testnet else "live"
        )

    # -- IExchangeAdapter ------------------------------------------------

    def place_order(self, order: Order) -> str:
        ccxt_symbol = to_ccxt_symbol(order.product_id)
        params: dict = {}
        if order.type and order.type.lower() == "limit":
            params["timeInForce"] = "GTC"

        try:
            self.logger.info(
                "Placing %s %s %s %s @ %s",
                order.type,
                order.side,
                order.quantity,
                ccxt_symbol,
                order.price or "market",
            )
            response = self.client.create_order(
                symbol=ccxt_symbol,
                type=order.type,
                side=order.side,
                amount=float(order.quantity),
                price=float(order.price) if order.price else None,
                params=params,
            )
            return str(response["id"])

        except ccxt.InsufficientFunds as e:
            raise InsufficientFundsError(f"Insufficient funds: {e}") from e
        except ccxt.NetworkError as e:
            raise NetworkError(f"Network error: {e}") from e
        except ccxt.BaseError as e:
            raise ExchangeError(f"Order placement failed: {e}") from e

    def cancel_order(self, order_id: str, product_id: str) -> bool:
        ccxt_symbol = to_ccxt_symbol(product_id)
        try:
            self.client.cancel_order(order_id, ccxt_symbol)
            return True
        except ccxt.OrderNotFound:
            self.logger.warning("Order %s not found on exchange", order_id)
            return False
        except ccxt.BaseError as e:
            self.logger.error("Failed to cancel order %s: %s", order_id, e)
            return False

    def get_balance(self, asset: str) -> Decimal:
        try:
            balance = self.client.fetch_balance()
            free = balance.get("free", {})
            return Decimal(str(free.get(asset, 0)))
        except ccxt.BaseError as e:
            raise ExchangeError(f"Failed to fetch balance: {e}") from e

    def get_position(self, product_id: str) -> Optional[Position]:
        ccxt_symbol = to_ccxt_symbol(product_id)
        try:
            positions = self.client.fetch_positions([ccxt_symbol])
        except ccxt.BaseError as e:
            raise ExchangeError(f"Failed to fetch position: {e}") from e

        for pos in positions:
            if pos.get("symbol") != ccxt_symbol:
                continue

            contracts = float(pos.get("contracts", 0))
            if contracts == 0:
                return None

            side = "LONG" if contracts > 0 else "SHORT"
            return Position(
                strategy_id="LIVE",
                product_id=product_id,
                side=side,
                quantity=Decimal(str(abs(contracts))),
                entry_price=Decimal(str(pos.get("entryPrice", 0))),
                unrealized_pnl=Decimal(str(pos.get("unrealizedPnl", 0))),
            )

        return None
