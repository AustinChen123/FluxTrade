"""Binance WebSocket order-entry transport owned by the Binance adapter."""

import asyncio
from dataclasses import dataclass
import hashlib
import hmac
import json
import logging
import threading
import time
from typing import Any, cast

from src.core.interfaces.exchange import ExchangeError, NetworkError

websockets: Any = None
try:
    import websockets as _websockets

    websockets = _websockets
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False

INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 300.0
MAX_RETRIES = 10


class OrderAckTimeout(NetworkError):
    """Raised when a Binance order ACK does not arrive before timeout."""


class OrderRejected(ExchangeError):
    """Sanitized deterministic rejection from Binance WebSocket order entry."""

    def __init__(self, *, status: int, code: int | None) -> None:
        self.status = status
        self.code = code
        super().__init__(
            "Binance WebSocket order rejected: "
            f"status={status} code={code if code is not None else 'unknown'}"
        )


@dataclass(frozen=True)
class ExchangeAck:
    exchange_order_id: str
    ack_type: str


def _sign_payload_binance(payload: str | dict[str, Any], secret: str) -> str:
    """Return Binance-compatible HMAC-SHA256 signature for a payload."""
    if isinstance(payload, dict):
        payload = "&".join(f"{key}={payload[key]}" for key in sorted(payload))
    return hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


class BinanceWebSocketOrderConnector:
    """Persistent Binance WebSocket connection for optional order entry."""

    def __init__(
        self,
        api_key: str,
        secret: str,
        testnet: bool = True,
    ) -> None:
        self.api_key = api_key
        self.secret = secret
        self.testnet = testnet
        self.ws_url = self._get_ws_url()
        self.ws = None
        self.loop = None
        self.running = False
        self.thread = None
        self.logger = logging.getLogger("WS_Connector")
        self._ack_registry: dict[str, ExchangeAck | ExchangeError] = {}
        self._ack_lock = threading.Lock()

    def _get_ws_url(self) -> str:
        if self.testnet:
            return "wss://testnet.binancefuture.com/ws-fapi/v1"
        return "wss://ws-fapi.binance.com/ws-fapi/v1"

    def start(self) -> None:
        if not HAS_WEBSOCKETS:
            self.logger.warning(
                "⚠️ 'websockets' library not installed. WS Order Entry disabled."
            )
            return

        self.running = True
        self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.thread.start()
        self.logger.info(
            "WS Order Connector: Starting connection to %s...", self.ws_url
        )

    def _run_event_loop(self) -> None:
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._connect_and_listen())

    async def _connect_and_listen(self) -> None:
        backoff = INITIAL_BACKOFF
        attempts = 0

        while self.running:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.ws = ws
                    self.logger.info("WS Order Connector: Connected.")
                    backoff = INITIAL_BACKOFF
                    attempts = 0
                    await self._authenticate(ws)

                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                            self._handle_message(msg)
                        except asyncio.TimeoutError:
                            continue
            except Exception as error:
                self.ws = None
                attempts += 1
                if attempts > MAX_RETRIES:
                    self.logger.error(
                        "Max reconnection attempts (%d) exceeded. Stopping.",
                        MAX_RETRIES,
                    )
                    self.running = False
                    return
                self.logger.warning(
                    "WS Connection Error: %s. Reconnecting in %.1fs (attempt %d/%d)",
                    error,
                    backoff,
                    attempts,
                    MAX_RETRIES,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)

    async def _authenticate(self, ws: Any) -> None:
        # The existing connection performs no separate authentication handshake.
        return None

    def _handle_message(self, msg: str | bytes) -> None:
        try:
            data = json.loads(msg)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self.logger.warning("Ignoring non-JSON Binance WS order response")
            return

        if not isinstance(data, dict):
            return
        request_id = data.get("id")
        status = data.get("status")
        if not isinstance(request_id, str) or type(status) is not int:
            return
        if status != 200:
            error = data.get("error")
            code = error.get("code") if isinstance(error, dict) else None
            safe_code = code if type(code) is int else None
            failure: ExchangeError
            if status == 408 or status >= 500 or safe_code in {-1006, -1007}:
                failure = NetworkError(
                    "Binance WebSocket order response is ambiguous: "
                    f"status={status} "
                    f"code={safe_code if safe_code is not None else 'unknown'}"
                )
            else:
                failure = OrderRejected(status=status, code=safe_code)
            self._record_ack(
                request_id,
                failure,
            )
            return

        result = data.get("result")
        if not isinstance(result, dict):
            return
        client_order_id = result.get("clientOrderId")
        exchange_order_id = result.get("orderId")
        if client_order_id != request_id or type(exchange_order_id) not in (int, str):
            return
        ack_type = result.get("status")
        self._record_ack(
            request_id,
            ExchangeAck(
                str(exchange_order_id),
                ack_type if isinstance(ack_type, str) else "ACK",
            ),
        )

    def _record_ack(
        self,
        client_order_id: str,
        ack: ExchangeAck | ExchangeError,
    ) -> None:
        with self._ack_lock:
            self._ack_registry[client_order_id] = ack

    async def _wait_for_ack(
        self,
        client_order_id: str,
        timeout: float = 3.0,
    ) -> ExchangeAck:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        delay = 0.01
        while True:
            with self._ack_lock:
                ack = self._ack_registry.pop(client_order_id, None)
            if ack is not None:
                if isinstance(ack, ExchangeError):
                    raise ack
                return ack
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise OrderAckTimeout(
                    f"timed out waiting for order ack: {client_order_id}"
                )
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 2, 0.25)

    def place_order(
        self,
        symbol: str,
        side: str,
        quantity: str | float,
        price: str | float | None = None,
        order_type: str = "MARKET",
        client_order_id: str | None = None,
    ) -> bool:
        """Send an order asynchronously, returning False for REST fallback."""
        if not self.running or not self.ws or not client_order_id:
            return False

        params: dict[str, Any] = {
            "apiKey": self.api_key,
            "newClientOrderId": client_order_id,
            "quantity": str(quantity),
            "recvWindow": 5000,
            "side": side.upper(),
            "symbol": symbol,
            "timestamp": int(time.time() * 1000),
            "type": order_type.upper(),
        }
        if price is not None:
            params["price"] = str(price)
        if order_type.upper() == "LIMIT":
            params["timeInForce"] = "GTC"
        self._sign_payload(params)
        payload: dict[str, Any] = {
            "id": client_order_id,
            "method": "order.place",
            "params": params,
        }

        send_coro = None
        try:
            send_coro = self.ws.send(json.dumps(payload))
            future = asyncio.run_coroutine_threadsafe(
                send_coro,
                cast(asyncio.AbstractEventLoop, self.loop),
            )
        except Exception:
            if send_coro is not None:
                send_coro.close()
            self.logger.warning("Binance WebSocket order was not scheduled")
            return False
        try:
            future.result(timeout=3.0)
        except Exception as error:
            raise NetworkError(
                "Binance WebSocket order submission result is ambiguous"
            ) from error
        return True

    def _sign_payload(self, payload: dict[str, Any]) -> None:
        payload["signature"] = _sign_payload_binance(payload, self.secret)

    def is_connected(self) -> bool:
        """Return whether the Binance WebSocket order connection is active."""
        return self.running and self.ws is not None
