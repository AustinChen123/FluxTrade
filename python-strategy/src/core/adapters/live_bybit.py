import hashlib
import threading
from typing import Any

import ccxt

from src.core.adapters.bybit_user_stream import BybitOrderEventStream
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.client_order_id import parse_client_order_id
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
)
from src.core.orm_models import Order
from src.core.product_registry import to_ccxt_symbol


def _bybit_client_order_id(client_order_id: str) -> str:
    return f"ft-{hashlib.sha256(client_order_id.encode()).hexdigest()[:33]}"


class LiveBybitAdapter(CcxtExchangeAdapter):
    """Bybit client identity and private order-event lifecycle owner."""

    def __init__(
        self,
        api_key: str | None = None,
        secret: str | None = None,
        testnet: bool = False,
        extra_config: dict | None = None,
    ) -> None:
        super().__init__(
            exchange_id="bybit",
            api_key=api_key,
            secret=secret,
            testnet=testnet,
            extra_config=extra_config,
        )
        self._client_order_aliases: dict[str, str] = {}
        self._client_order_alias_lock = threading.Lock()
        self._user_order_stream = BybitOrderEventStream(
            api_key=self.client.apiKey,
            secret=self.client.secret,
            testnet=testnet,
            resolve_client_order_id=self._canonical_client_order_id,
        )

    def close(self) -> None:
        if not self._user_order_stream.close():
            self.logger.warning("Bybit order event stream cleanup failed")

    def _exchange_client_order_id(self, client_order_id: str) -> str:
        parse_client_order_id(client_order_id)
        provider_id = _bybit_client_order_id(client_order_id)
        with self._client_order_alias_lock:
            existing = self._client_order_aliases.get(provider_id)
            if existing is not None and existing != client_order_id:
                raise ExchangeError("bybit_client_order_id_collision")
            self._client_order_aliases[provider_id] = client_order_id
        return provider_id

    def _canonical_client_order_id(self, provider_id: str) -> str:
        with self._client_order_alias_lock:
            return self._client_order_aliases.get(provider_id, provider_id)

    def _submission_client_order_id_params(
        self,
        exchange_client_order_id: str,
        order_type: str | None,
    ) -> dict:
        del order_type
        return {"clientOrderId": exchange_client_order_id}

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
        rows = self._request_order_rows(client.privateGetV5OrderRealtime, request)
        if not rows:
            rows = self._request_order_rows(client.privateGetV5OrderHistory, request)
        if not rows:
            return None
        try:
            parsed = client.parse_order(rows[0])
        except ccxt.BaseError as error:
            raise ExchangeError("bybit_client_order_lookup_failed") from error
        except Exception:
            raise ExchangeError("bybit_client_order_lookup_failed") from None
        if type(parsed) is not dict:
            raise ExchangeError("bybit_client_order_lookup_failed")
        return self._order_snapshot_from_response(client_order_id, parsed)

    @staticmethod
    def _request_order_rows(requester, request: dict[str, str]) -> list[dict]:
        try:
            response = requester(request)
        except ccxt.OrderNotFound:
            return []
        except ccxt.BaseError as error:
            raise ExchangeError("bybit_client_order_lookup_failed") from error
        except Exception:
            raise ExchangeError("bybit_client_order_lookup_failed") from None
        if type(response) is not dict or type(response.get("result")) is not dict:
            raise ExchangeError("bybit_client_order_lookup_failed")
        rows = response["result"].get("list")
        if type(rows) is not list or len(rows) > 1:
            raise ExchangeError("bybit_client_order_lookup_failed")
        if rows and type(rows[0]) is not dict:
            raise ExchangeError("bybit_client_order_lookup_failed")
        return rows

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
            client.privatePostV5OrderCancel(request)
            return True
        except ccxt.OrderNotFound:
            return False
        except ccxt.BaseError:
            self.logger.error("Failed to cancel Bybit order by client identity")
            return False

    def _client_id_request(
        self,
        client_order_id: str,
        product_id: str,
    ) -> dict[str, str]:
        provider_id = self._exchange_client_order_id(client_order_id)
        symbol = to_ccxt_symbol(product_id)
        self.client.load_markets()
        market = self.client.market(symbol)
        market_id = market.get("id") if type(market) is dict else None
        if type(market_id) is not str or not market_id:
            raise ExchangeError("bybit_market_identity_missing")
        return {"category": "linear", "symbol": market_id, "orderLinkId": provider_id}


def create_bybit_live_adapter(
    *,
    api_key: str | None = None,
    secret: str | None = None,
    testnet: bool = False,
    extra_config: dict | None = None,
) -> LiveBybitAdapter:
    """Construct the configured Bybit adapter without owning its lifecycle."""
    return LiveBybitAdapter(
        api_key=api_key,
        secret=secret,
        testnet=testnet,
        extra_config=extra_config,
    )
