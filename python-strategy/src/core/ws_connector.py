import asyncio
from dataclasses import dataclass
import json
import time
import threading
import logging
import hashlib
import hmac
from typing import Optional, Dict, Any
from urllib.parse import urlencode

# Try to import websockets, handle if missing
try:
    import websockets
    HAS_WEBSOCKETS = True
except ImportError:
    HAS_WEBSOCKETS = False

INITIAL_BACKOFF = 1.0
MAX_BACKOFF = 300.0
MAX_RETRIES = 10


class OrderAckTimeout(TimeoutError):
    """Raised when an exchange order ACK does not arrive before timeout."""


@dataclass(frozen=True)
class ExchangeAck:
    exchange_order_id: str
    ack_type: str


def _sign_payload_binance(payload: str | Dict[str, Any], secret: str) -> str:
    """Return Binance-compatible HMAC-SHA256 signature for a payload."""
    if isinstance(payload, dict):
        payload = urlencode(payload, doseq=True)
    return hmac.new(
        secret.encode("utf-8"),
        payload.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


class WebSocketOrderConnector:
    """
    Persistent WebSocket connection for Order Entry (Binance/Backpack).
    Falls back to REST if WS is unavailable or fails.
    """
    def __init__(self, api_key: str, secret: str, exchange_id: str = "binance", testnet: bool = True):
        self.api_key = api_key
        self.secret = secret
        self.exchange_id = exchange_id.lower()
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
        if self.exchange_id == "binance":
            if self.testnet:
                return "wss://testnet.binancefuture.com/ws-fapi/v1"
            return "wss://fstream.binance.com/ws-fapi/v1"
        # Add Backpack/Other mappings here
        return ""

    def start(self):
        if not HAS_WEBSOCKETS:
            self.logger.warning("⚠️ 'websockets' library not installed. WS Order Entry disabled.")
            return

        if not self.ws_url:
            self.logger.warning("⚠️ No WS URL for %s. WS Order Entry disabled.", self.exchange_id)
            return

        self.running = True
        self.thread = threading.Thread(target=self._run_event_loop, daemon=True)
        self.thread.start()
        self.logger.info("WS Order Connector: Starting connection to %s...", self.ws_url)

    def _run_event_loop(self):
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._connect_and_listen())

    async def _connect_and_listen(self):
        backoff = INITIAL_BACKOFF
        attempts = 0

        while self.running:
            try:
                async with websockets.connect(self.ws_url) as ws:
                    self.ws = ws
                    self.logger.info("WS Order Connector: Connected.")
                    # Reset backoff on successful connection
                    backoff = INITIAL_BACKOFF
                    attempts = 0
                    await self._authenticate(ws)

                    # Heartbeat & Listen Loop
                    while self.running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                            self._handle_message(msg)
                        except asyncio.TimeoutError:
                            continue
            except Exception as e:
                self.ws = None
                attempts += 1
                if attempts > MAX_RETRIES:
                    self.logger.error("Max reconnection attempts (%d) exceeded. Stopping.",
                                     MAX_RETRIES)
                    self.running = False
                    return
                self.logger.warning("WS Connection Error: %s. Reconnecting in %.1fs (attempt %d/%d)",
                                    e, backoff, attempts, MAX_RETRIES)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)

    async def _authenticate(self, ws):
        # Implementation depends on Exchange API
        # Binance Futures WS doesn't always need auth for connection, 
        # but 'listenKey' is used for User Data Stream.
        # For actual *Order Entry* via WS, Binance uses a specific payload signature.
        pass

    def _handle_message(self, msg: str):
        # Process order updates
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
            self._record_ack(str(coid), ExchangeAck(str(exchange_order_id), str(ack_type)))

    def _record_ack(self, client_order_id: str, ack: ExchangeAck) -> None:
        with self._ack_lock:
            self._ack_registry[client_order_id] = ack

    async def _wait_for_ack(self, client_order_id: str, timeout: float = 3.0) -> ExchangeAck:
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
                raise OrderAckTimeout(f"timed out waiting for order ack: {client_order_id}")
            await asyncio.sleep(min(delay, remaining))
            delay = min(delay * 2, 0.25)

    def place_order(self, symbol: str, side: str, quantity: float, price: Optional[float] = None, order_type: str = "MARKET") -> bool:
        """
        Sends order via WebSocket. Returns True if sent (async), False if failed/fallback needed.
        """
        if not self.running or not self.ws:
            return False

        # Binance WS Order Create Payload (Example)
        # Note: Binance Futures often uses REST for orders and WS for updates.
        # But some APIs allow WS Orders. We assume the 'Backpack' style or specific API support.
        
        # If the exchange supports WS Orders (e.g. Backpack does):
        payload = {
            "method": "order.place",
            "params": {
                "symbol": symbol,
                "side": side.upper(),
                "quantity": str(quantity),
                "price": str(price) if price else "0",
                "type": order_type.upper()
            },
            "id": int(time.time() * 1000)
        }
        
        # Sign payload
        self._sign_payload(payload)

        # Send (Thread-safe interaction with Async Loop is tricky, simplistic approach here)
        try:
             asyncio.run_coroutine_threadsafe(self.ws.send(json.dumps(payload)), self.loop)
             return True
        except Exception as e:
            self.logger.warning("Failed to send WS order: %s", e)
            return False

    def _sign_payload(self, payload: Dict[str, Any]):
        # Add timestamp and signature
        if self.exchange_id == "backpack":
            # Mock signature logic
            payload["signature"] = "signed_hash"
        pass

    def is_connected(self, exchange_id: str) -> bool:
        """
        Checks if the WS connection is active for the given exchange.
        """
        # In this simple implementation, we just check if self.ws is not None
        # and match the exchange_id.
        return self.running and self.ws is not None and self.exchange_id == exchange_id.lower()
