import hashlib
import threading
from typing import Any

import ccxt

from src.core.adapters.backpack_user_stream import BackpackOrderEventStream
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.client_order_id import parse_client_order_id
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
)
from src.core.orm_models import Order
from src.core.product_registry import to_ccxt_symbol


def _backpack_client_id(client_order_id: str) -> int:
    digest = hashlib.sha256(client_order_id.encode()).digest()
    return int.from_bytes(digest[:4], "big")


class LiveBackpackAdapter(CcxtExchangeAdapter):
    """Backpack-specific client identity and private order-event lifecycle."""

    def __init__(
        self,
        api_key: str | None = None,
        secret: str | None = None,
        testnet: bool = False,
        extra_config: dict | None = None,
    ) -> None:
        super().__init__(
            exchange_id="backpack",
            api_key=api_key,
            secret=secret,
            testnet=testnet,
            extra_config=extra_config,
        )
        self._client_order_aliases: dict[str, str] = {}
        self._client_order_alias_lock = threading.Lock()
        self._user_order_stream = BackpackOrderEventStream(
            client=self.client,
            resolve_client_order_id=self._canonical_client_order_id,
        )

    def close(self) -> None:
        if not self._user_order_stream.close():
            self.logger.warning("Backpack order event stream cleanup failed")

    def _exchange_client_order_id(self, client_order_id: str) -> str:
        parse_client_order_id(client_order_id)
        exchange_client_order_id = str(_backpack_client_id(client_order_id))
        with self._client_order_alias_lock:
            existing = self._client_order_aliases.get(exchange_client_order_id)
            if existing is not None and existing != client_order_id:
                raise ExchangeError("backpack_client_order_id_collision")
            self._client_order_aliases[exchange_client_order_id] = client_order_id
        return exchange_client_order_id

    def _canonical_client_order_id(self, exchange_client_order_id: str) -> str:
        with self._client_order_alias_lock:
            return self._client_order_aliases.get(
                exchange_client_order_id,
                exchange_client_order_id,
            )

    def _submission_client_order_id_params(
        self,
        exchange_client_order_id: str,
        order_type: str | None,
    ) -> dict:
        del order_type
        return {"clientOrderId": int(exchange_client_order_id)}

    def place_order(self, order: Order) -> str:
        client_order_id = getattr(order, "client_order_id", None)
        if client_order_id:
            self._exchange_client_order_id(client_order_id)
        return super().place_order(order)

    def start_order_event_stream(self) -> None:
        self._user_order_stream.start()

    def poll_order_event(self) -> ExchangeOrderEvent | None:
        return self._user_order_stream.poll()

    def cancel_terminal_state_delivered_by_order_events(self) -> bool:
        return True

    def get_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: str | None = None,
    ) -> ExchangeOrderSnapshot | None:
        del order_type
        request = self._client_id_request(client_order_id, product_id)
        client: Any = self.client
        try:
            response = client.privateGetApiV1Order(request)
            parsed = client.parse_order(response)
        except ccxt.OrderNotFound:
            self.logger.warning(
                "Order with client_order_id %s not found on exchange",
                client_order_id,
            )
            return None
        except ccxt.BaseError as error:
            raise ExchangeError("backpack_client_order_lookup_failed") from error
        if type(parsed) is not dict:
            raise ExchangeError("backpack_client_order_lookup_failed")
        return self._order_snapshot_from_response(client_order_id, parsed)

    def cancel_order_by_client_id(
        self,
        client_order_id: str,
        product_id: str,
        *,
        order_type: str | None = None,
    ) -> bool:
        del order_type
        request = self._client_id_request(client_order_id, product_id)
        client: Any = self.client
        try:
            client.privateDeleteApiV1Order(request)
            return True
        except ccxt.OrderNotFound:
            self.logger.warning(
                "Order with client_order_id %s not found on exchange",
                client_order_id,
            )
            return False
        except ccxt.BaseError:
            self.logger.error(
                "Failed to cancel order with client_order_id %s",
                client_order_id,
            )
            return False

    def _client_id_request(
        self,
        client_order_id: str,
        product_id: str,
    ) -> dict[str, object]:
        exchange_client_order_id = self._exchange_client_order_id(client_order_id)
        symbol = to_ccxt_symbol(product_id)
        self.client.load_markets()
        market = self.client.market(symbol)
        market_id = market.get("id") if type(market) is dict else None
        if type(market_id) is not str or not market_id:
            raise ExchangeError("backpack_market_identity_missing")
        return {
            "symbol": market_id,
            "clientId": int(exchange_client_order_id),
        }


def create_backpack_live_adapter(
    *,
    api_key: str | None = None,
    secret: str | None = None,
    testnet: bool = False,
    extra_config: dict | None = None,
) -> LiveBackpackAdapter:
    """Construct the configured Backpack adapter without owning its lifecycle."""
    return LiveBackpackAdapter(
        api_key=api_key,
        secret=secret,
        testnet=testnet,
        extra_config=extra_config,
    )
