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
from urllib.parse import urlencode

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


class OrderAckTimeout(TimeoutError):
    """Raised when a Binance order ACK does not arrive before timeout."""


@dataclass(frozen=True)
class ExchangeAck:
    exchange_order_id: str
    ack_type: str


def _sign_payload_binance(payload: str | dict[str, Any], secret: str) -> str:
    """Return Binance-compatible HMAC-SHA256 signature for a payload."""
    if isinstance(payload, dict):
        payload = urlencode(payload, doseq=True)
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
        self._ack_registry: dict[str, ExchangeAck] = {}
        self._ack_lock = threading.Lock()

    def _get_ws_url(self) -> str:
        if self.testnet:
            return "wss://testnet.binancefuture.com/ws-fapi/v1"
        return "wss://fstream.binance.com/ws-fapi/v1"

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
        except json.JSONDecodeError:
            self.logger.warning("Ignoring non-JSON WS message: %s", msg)
            return

        coid = (
            data.get("clientOrderId")
            or data.get("client_order_id")
            or data.get("c")
            or data.get("params", {}).get("clientOrderId")
        )
        exchange_order_id = (
            data.get("orderId")
            or data.get("exchange_order_id")
            or data.get("i")
            or data.get("params", {}).get("orderId")
        )
        ack_type = (
            data.get("ack_type")
            or data.get("status")
            or data.get("X")
            or data.get("params", {}).get("status")
            or "ACK"
        )
        if coid and exchange_order_id:
            self._record_ack(
                str(coid),
                ExchangeAck(str(exchange_order_id), str(ack_type)),
            )

    def _record_ack(self, client_order_id: str, ack: ExchangeAck) -> None:
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
        if not self.running or not self.ws:
            return False

        payload = {
            "method": "order.place",
            "params": {
                "symbol": symbol,
                "side": side.upper(),
                "quantity": str(quantity),
                "price": str(price) if price else "0",
                "type": order_type.upper(),
            },
            "id": int(time.time() * 1000),
        }
        if client_order_id:
            payload["params"]["newClientOrderId"] = client_order_id

        self._sign_payload(payload)

        try:
            asyncio.run_coroutine_threadsafe(
                self.ws.send(json.dumps(payload)),
                cast(asyncio.AbstractEventLoop, self.loop),
            )
            return True
        except Exception as error:
            self.logger.warning("Failed to send WS order: %s", error)
            return False

    def _sign_payload(self, payload: dict[str, Any]) -> None:
        # Preserved behavior: protocol signing is a separate correctness slice.
        return None

    def is_connected(self) -> bool:
        """Return whether the Binance WebSocket order connection is active."""
        return self.running and self.ws is not None
