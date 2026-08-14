"""Backpack private order-update WebSocket protocol owner."""

from collections.abc import Callable
from decimal import Decimal, InvalidOperation
import json
from typing import Any, Protocol

from websockets.sync.client import connect

from src.core.interfaces.exchange import ExchangeError, ExchangeOrderEvent, NetworkError
from src.core.product_registry import to_ccxt_symbol

_STREAM_URL = "wss://ws.backpack.exchange"
_STREAM_NAME = "account.orderUpdate"
_WINDOW = "5000"
_START_PROBE_SECONDS = 0.1

_STATUS_MATRIX = {
    ("orderAccepted", "New"): "NEW",
    ("orderCancelled", "Cancelled"): "CANCELLED",
    ("orderExpired", "Expired"): "EXPIRED",
    ("orderFill", "PartiallyFilled"): "PARTIALLY_FILLED",
    ("orderFill", "Filled"): "FILLED",
    ("orderModified", "New"): "NEW",
    ("orderModified", "PartiallyFilled"): "PARTIALLY_FILLED",
    ("triggerPlaced", "TriggerPending"): "NEW",
    ("triggerFailed", "TriggerFailed"): "REJECTED",
}


class _Connection(Protocol):
    def send(self, message: str) -> None: ...

    def recv(self, timeout: float | None = None) -> str | bytes: ...

    def close(self) -> None: ...


class BackpackOrderEventStream:
    """Own one signed Backpack order-update subscription."""

    def __init__(
        self,
        *,
        client: Any,
        resolve_client_order_id: Callable[[str], str],
        connect: Callable[..., _Connection] = connect,
    ) -> None:
        self._client = client
        self._resolve_client_order_id = resolve_client_order_id
        self._connect = connect
        self._connection: _Connection | None = None
        self._buffered_event: ExchangeOrderEvent | None = None

    def start(self) -> None:
        if self._connection is not None:
            self.close()
        try:
            self._client.fetch_open_orders()
        except Exception:
            raise NetworkError("backpack_order_event_stream_preflight_failed") from None

        connection: _Connection | None = None
        try:
            request = self._subscription_request()
            connection = self._connect(
                _STREAM_URL,
                open_timeout=10,
                close_timeout=10,
            )
            connection.send(json.dumps(request, separators=(",", ":")))
            try:
                first_message = connection.recv(timeout=_START_PROBE_SECONDS)
            except TimeoutError:
                first_message = None
            if first_message is not None:
                self._buffered_event = self._project_message(first_message)
        except NetworkError:
            if connection is not None:
                _close_connection(connection)
            raise NetworkError("backpack_order_event_stream_start_failed") from None
        except ExchangeError:
            if connection is not None:
                _close_connection(connection)
            raise
        except Exception:
            if connection is not None:
                _close_connection(connection)
            raise NetworkError("backpack_order_event_stream_start_failed") from None
        self._connection = connection

    def poll(self) -> ExchangeOrderEvent | None:
        if self._buffered_event is not None:
            event, self._buffered_event = self._buffered_event, None
            return event
        connection = self._connection
        if connection is None:
            raise NetworkError("backpack_order_event_stream_not_started")
        try:
            message = connection.recv(timeout=0.1)
        except TimeoutError:
            return None
        except Exception:
            self.close()
            raise NetworkError("backpack_order_event_stream_receive_failed") from None
        try:
            return self._project_message(message)
        except NetworkError:
            self.close()
            raise NetworkError("backpack_order_event_stream_receive_failed") from None
        except ExchangeError:
            self.close()
            raise

    def close(self) -> bool:
        connection, self._connection = self._connection, None
        self._buffered_event = None
        if connection is None:
            return True
        return _close_connection(connection)

    def _subscription_request(self) -> dict[str, object]:
        timestamp = str(self._client.milliseconds())
        payload = f"instruction=subscribe&timestamp={timestamp}&window={_WINDOW}"
        secret_bytes = self._client.base64_to_binary(self._client.secret)
        seed = self._client.array_slice(secret_bytes, 0, 32)
        signature = self._client.eddsa(
            self._client.encode(payload),
            seed,
            "ed25519",
        )
        api_key = self._client.apiKey
        if type(api_key) is not str or not api_key or type(signature) is not str:
            raise NetworkError("backpack_order_event_stream_start_failed")
        return {
            "method": "SUBSCRIBE",
            "params": [_STREAM_NAME],
            "signature": [api_key, signature, timestamp, _WINDOW],
        }

    def _project_message(
        self,
        message: str | bytes,
    ) -> ExchangeOrderEvent | None:
        try:
            payload = json.loads(message)
        except (TypeError, ValueError):
            raise NetworkError("backpack_order_event_control_invalid") from None
        if type(payload) is not dict:
            raise NetworkError("backpack_order_event_control_invalid")
        if payload == {"result": None}:
            return None
        if "error" in payload or set(payload) != {"stream", "data"}:
            raise NetworkError("backpack_order_event_control_invalid")
        stream = payload.get("stream")
        if type(stream) is not str:
            raise NetworkError("backpack_order_event_control_invalid")
        if stream != _STREAM_NAME:
            return None
        if type(payload.get("data")) is not dict:
            raise NetworkError("backpack_order_event_control_invalid")
        return self._project_order_event(payload["data"])

    def _project_order_event(self, values: dict[str, object]) -> ExchangeOrderEvent:
        try:
            provider_event = _required_string(values, "e")
            provider_status = _required_string(values, "X")
            status = _STATUS_MATRIX[(provider_event, provider_status)]
            symbol = _required_string(values, "s")
            client_id = _optional_uint32(values, "c")
            exchange_order_id = _required_string(values, "i")
            _required_int(values, "E")
            engine_timestamp = _required_int(values, "T")
            cumulative_quantity = _optional_decimal(values, "z")
            if cumulative_quantity is not None and cumulative_quantity < 0:
                raise ValueError("z")
            reason = _optional_string(values, "R")
            last_quantity: Decimal | None = None
            last_price: Decimal | None = None
            fee: Decimal | None = None
            fee_asset: str | None = None
            if provider_event == "orderFill":
                _required_int(values, "t")
                cumulative_quantity = _required_decimal(values, "z")
                last_quantity = _required_decimal(values, "l")
                last_price = _required_decimal(values, "L")
                if (
                    cumulative_quantity < last_quantity
                    or last_quantity <= 0
                    or last_price <= 0
                ):
                    raise ValueError("fill")
                fee = _optional_decimal(values, "n")
                fee_asset = _optional_string(values, "N")
                if (fee is None) != (fee_asset is None):
                    raise ValueError("fee")
            product_id = _product_id(symbol)
        except (InvalidOperation, KeyError, ValueError):
            raise ExchangeError("backpack_order_event_payload_invalid") from None
        return ExchangeOrderEvent(
            status=status,
            product_id=product_id,
            client_order_id=(
                self._resolve_client_order_id(str(client_id))
                if client_id is not None
                else None
            ),
            exchange_order_id=exchange_order_id,
            cumulative_filled_quantity=cumulative_quantity,
            cumulative_average_price=None,
            last_fill_quantity=last_quantity,
            last_fill_price=last_price,
            fee=fee,
            fee_asset=fee_asset,
            event_timestamp=engine_timestamp // 1_000,
            reason=reason,
        )


def _close_connection(connection: _Connection) -> bool:
    try:
        connection.close()
    except Exception:
        return False
    return True


def _product_id(symbol: str) -> str:
    contract = symbol.removesuffix("_PERP")
    parts = contract.split("_")
    if (
        not symbol.endswith("_PERP")
        or len(parts) != 2
        or any(not part or not part.isalnum() for part in parts)
    ):
        raise ValueError("symbol")
    product_id = f"BACKPACK:{contract}-PERP"
    to_ccxt_symbol(product_id)
    return product_id


def _required_string(values: dict[str, object], key: str) -> str:
    value = values.get(key)
    if type(value) is not str or not value:
        raise ValueError(key)
    return value


def _optional_string(values: dict[str, object], key: str) -> str | None:
    value = values.get(key)
    if value is None:
        return None
    if type(value) is not str or not value:
        raise ValueError(key)
    return value


def _required_int(values: dict[str, object], key: str) -> int:
    value = values.get(key)
    if type(value) is not int or value < 0:
        raise ValueError(key)
    return value


def _optional_uint32(values: dict[str, object], key: str) -> int | None:
    value = values.get(key)
    if value is None:
        return None
    if type(value) is not int or not 0 <= value <= 0xFFFFFFFF:
        raise ValueError(key)
    return value


def _required_decimal(values: dict[str, object], key: str) -> Decimal:
    value = values.get(key)
    if type(value) is not str or not value:
        raise ValueError(key)
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise ValueError(key)
    return parsed


def _optional_decimal(values: dict[str, object], key: str) -> Decimal | None:
    value = values.get(key)
    if value is None:
        return None
    if type(value) is not str or not value:
        raise ValueError(key)
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise ValueError(key)
    return parsed
