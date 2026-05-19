"""Binance-specific adapter with optional WebSocket order entry.

Extends CcxtExchangeAdapter with WS market-order fast path.
Falls back to REST (parent class) when WS is unavailable.
"""

import logging
import asyncio

from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.client_order_id import to_exchange_format
from src.core.orm_models import Order
from src.core.ws_connector import WebSocketOrderConnector


class LiveBinanceAdapter(CcxtExchangeAdapter):
    """CcxtExchangeAdapter + optional WebSocket for market orders."""

    def __init__(
        self,
        api_key: str | None = None,
        secret: str | None = None,
        testnet: bool = True,
        enable_ws: bool = True,
    ):
        super().__init__(
            exchange_id="binance",
            api_key=api_key,
            secret=secret,
            testnet=testnet,
        )
        self.logger = logging.getLogger("LiveBinanceAdapter")

        # Optional WebSocket fast path
        self.ws_connector: WebSocketOrderConnector | None = None
        if enable_ws:
            try:
                self.ws_connector = WebSocketOrderConnector(
                    self.client.apiKey or "",
                    self.client.secret or "",
                    "binance",
                    testnet,
                )
                self.ws_connector.start()
            except Exception as e:
                self.logger.warning("WebSocket init failed, REST only: %s", e)
                self.ws_connector = None

    def place_order(self, order: Order) -> str:
        # Try WS fast path for market orders
        if (
            self.ws_connector
            and self.ws_connector.is_connected("binance")
            and order.type
            and order.type.lower() == "market"
        ):
            try:
                client_order_id = getattr(order, "client_order_id", None)
                exchange_client_order_id = (
                    to_exchange_format(client_order_id, "binance")
                    if client_order_id
                    else None
                )
                success = self.ws_connector.place_order(
                    symbol=order.product_id,
                    side=order.side,
                    quantity=str(order.quantity),
                    price=str(order.price) if order.price else None,
                    order_type=order.type,
                    client_order_id=exchange_client_order_id,
                )
                if success:
                    if exchange_client_order_id:
                        ack = asyncio.run(
                            self.ws_connector._wait_for_ack(exchange_client_order_id)
                        )
                        return ack.exchange_order_id
                    return f"WS-{order.id}"
            except Exception as e:
                self.logger.warning("WS order failed, falling back to REST: %s", e)

        # REST fallback (parent class)
        return super().place_order(order)
