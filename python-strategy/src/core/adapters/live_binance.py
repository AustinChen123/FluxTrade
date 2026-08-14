"""Binance-specific adapter with optional WebSocket order entry.

Extends CcxtExchangeAdapter with WS market-order fast path.
Falls back to REST (parent class) when WS is unavailable.
"""

import asyncio
import logging
import threading
from collections.abc import Callable
from typing import cast

from src.core.adapters.binance_order_routing import (
    binance_conditional_order_mapping,
    binance_lookup_client_order_id_params,
    binance_submission_client_order_id_params,
    uses_binance_algo_order_endpoints,
)
from src.core.adapters.binance_client_order_id import to_binance_client_order_id
from src.core.adapters.binance_user_stream import (
    BinanceOrderEventStream,
    create_binance_user_stream_listen_key,
    keepalive_binance_user_stream,
)
from src.core.adapters.binance_ws_order import (
    BinanceWebSocketOrderConnector,
    OrderRejected,
)
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.interfaces.exchange import ExchangeOrderEvent, NetworkError
from src.core.orm_models import Order
from src.core.product_registry import to_base_quote


class LiveBinanceAdapter(CcxtExchangeAdapter):
    """CcxtExchangeAdapter + optional WebSocket for market orders."""

    def __init__(
        self,
        api_key: str | None = None,
        secret: str | None = None,
        testnet: bool = True,
        enable_ws: bool = True,
        extra_config: dict | None = None,
        operation_guard: Callable[[], None] | None = None,
    ):
        super().__init__(
            exchange_id="binance",
            api_key=api_key,
            secret=secret,
            testnet=testnet,
            extra_config=extra_config,
        )
        self.logger = logging.getLogger("LiveBinanceAdapter")
        self._client_order_aliases: dict[str, str] = {}
        self._client_order_alias_lock = threading.Lock()
        self._user_order_stream = BinanceOrderEventStream(
            client=self.client,
            testnet=testnet,
            resolve_client_order_id=self._canonical_client_order_id,
        )

        # Optional WebSocket fast path
        self.ws_connector: BinanceWebSocketOrderConnector | None = None
        if enable_ws:
            guard = operation_guard or (lambda: None)
            guard()
            try:
                connector = BinanceWebSocketOrderConnector(
                    self.client.apiKey or "",
                    self.client.secret or "",
                    testnet,
                )
            except Exception as e:
                self.logger.warning("WebSocket init failed, REST only: %s", e)
            else:
                guard()
                try:
                    connector.start()
                except Exception as e:
                    self.logger.warning("WebSocket init failed, REST only: %s", e)
                else:
                    self.ws_connector = connector
            try:
                guard()
            except Exception:
                if self.ws_connector is not None:
                    self.ws_connector.running = False
                raise

    def close(self) -> None:
        stream = getattr(self, "_user_order_stream", None)
        if stream is not None and not stream.close():
            self.logger.warning("Binance order event stream cleanup failed")
        ws_connector = getattr(self, "ws_connector", None)
        if ws_connector is not None:
            ws_connector.running = False

    def _submission_client_order_id_params(
        self,
        exchange_client_order_id: str,
        order_type: str | None,
    ) -> dict:
        return binance_submission_client_order_id_params(
            "binance",
            order_type,
            exchange_client_order_id,
        ) or super()._submission_client_order_id_params(
            exchange_client_order_id,
            order_type,
        )

    def _exchange_client_order_id(self, client_order_id: str) -> str:
        exchange_client_order_id = to_binance_client_order_id(client_order_id)
        lock = getattr(self, "_client_order_alias_lock", None)
        if lock is not None:
            with lock:
                self._client_order_aliases[exchange_client_order_id] = client_order_id
        return exchange_client_order_id

    def _canonical_client_order_id(self, exchange_client_order_id: str) -> str:
        with self._client_order_alias_lock:
            return self._client_order_aliases.get(
                exchange_client_order_id,
                exchange_client_order_id,
            )

    def start_order_event_stream(self) -> None:
        self._user_order_stream.start()

    def poll_order_event(self) -> ExchangeOrderEvent | None:
        return self._user_order_stream.poll()

    def _ccxt_order_type_and_params(self, order: Order) -> tuple[str, dict]:
        conditional = binance_conditional_order_mapping(
            "binance",
            (getattr(order, "type", None) or "").lower(),
            getattr(order, "trigger_price", None),
        )
        if conditional is not None:
            return conditional
        return super()._ccxt_order_type_and_params(order)

    def _cancel_order_params(self, order_type: str | None) -> dict | None:
        if uses_binance_algo_order_endpoints("binance", order_type):
            return {"trigger": True}
        return super()._cancel_order_params(order_type)

    def _client_order_id_params(
        self,
        exchange_client_order_id: str,
        order_type: str | None,
    ) -> dict:
        return binance_lookup_client_order_id_params(
            "binance",
            order_type,
            exchange_client_order_id,
        ) or super()._client_order_id_params(exchange_client_order_id, order_type)

    def create_user_stream_listen_key(self) -> str:
        return create_binance_user_stream_listen_key("binance", self.client)

    def keepalive_user_stream(self, listen_key: str) -> None:
        keepalive_binance_user_stream("binance", self.client, listen_key)

    def place_order(self, order: Order) -> str:
        side = self._ccxt_order_side(order.side)
        intent_payload = getattr(order, "intent_payload", None)
        order_type = cast(str | None, order.type)
        reduce_only = (
            isinstance(intent_payload, dict)
            and intent_payload.get("reduce_only") is True
        )
        # Try WS fast path for market orders
        if (
            not reduce_only
            and self.ws_connector
            and self.ws_connector.is_connected()
            and order_type
            and order_type.lower() == "market"
        ):
            client_order_id = cast(
                str | None,
                getattr(order, "client_order_id", None),
            )
            if client_order_id:
                self._quantize_order(order)
                exchange_client_order_id = self._exchange_client_order_id(
                    client_order_id
                )
                base, quote = to_base_quote(cast(str, order.product_id))
                try:
                    success = self.ws_connector.place_order(
                        symbol=f"{base}{quote}",
                        side=side,
                        quantity=str(order.quantity),
                        price=str(order.price) if order.price is not None else None,
                        order_type=order_type,
                        client_order_id=exchange_client_order_id,
                    )
                except NetworkError:
                    raise
                except Exception:
                    self.logger.warning(
                        "Binance WebSocket order failed before scheduling; using REST"
                    )
                else:
                    if success:
                        try:
                            ack = asyncio.run(
                                self.ws_connector._wait_for_ack(
                                    exchange_client_order_id
                                )
                            )
                        except OrderRejected:
                            self.logger.warning(
                                "Binance WebSocket order was rejected; using REST"
                            )
                        except NetworkError:
                            raise
                        except Exception as error:
                            raise NetworkError(
                                "Binance WebSocket order acknowledgement is ambiguous"
                            ) from error
                        else:
                            return ack.exchange_order_id

        # REST fallback (parent class)
        return super().place_order(order)


def create_binance_live_adapter(
    *,
    api_key: str | None = None,
    secret: str | None = None,
    testnet: bool = True,
    enable_ws: object = False,
    extra_config: dict | None = None,
    operation_guard: Callable[[], None] | None = None,
) -> LiveBinanceAdapter:
    """Construct the configured Binance adapter without owning its lifecycle."""
    return LiveBinanceAdapter(
        api_key=api_key,
        secret=secret,
        testnet=testnet,
        enable_ws=bool(enable_ws),
        extra_config=extra_config,
        operation_guard=operation_guard,
    )
