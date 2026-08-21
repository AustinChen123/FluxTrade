"""Binance USD-M Futures user-stream protocol operations."""

from collections.abc import Callable
from decimal import Decimal, InvalidOperation
import json
import time
from typing import Any, Protocol

import ccxt
from websockets.sync.client import connect

from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeUserStreamUnsupported,
    NetworkError,
)

_KEEPALIVE_INTERVAL_SECONDS = 30 * 60
_PRODUCTION_STREAM_URL = "wss://fstream.binance.com/ws"
_TESTNET_STREAM_URL = "wss://fstream.binancefuture.com/ws"


class _Connection(Protocol):
    def recv(self, timeout: float | None = None) -> str | bytes: ...

    def close(self) -> None: ...


def create_binance_user_stream_listen_key(exchange_id: str, client: Any) -> str:
    if exchange_id != "binance" or not hasattr(
        client,
        "fapiPrivatePostListenKey",
    ):
        raise ExchangeUserStreamUnsupported(
            f"user_stream_listen_key_unsupported: exchange={exchange_id}"
        )
    try:
        response = client.fapiPrivatePostListenKey()
    except ccxt.BaseError as e:
        raise ExchangeError(f"user_stream_listen_key_create_failed: {e}") from e
    listen_key = response.get("listenKey") if isinstance(response, dict) else None
    if not listen_key:
        raise ExchangeError("user_stream_listen_key_missing")
    return str(listen_key)


def keepalive_binance_user_stream(
    exchange_id: str,
    client: Any,
    listen_key: str,
) -> None:
    if exchange_id != "binance" or not hasattr(
        client,
        "fapiPrivatePutListenKey",
    ):
        raise ExchangeUserStreamUnsupported(
            f"user_stream_keepalive_unsupported: exchange={exchange_id}"
        )
    if not listen_key:
        raise ExchangeError("user_stream_keepalive_requires_listen_key")
    try:
        client.fapiPrivatePutListenKey()
    except ccxt.BaseError as e:
        raise ExchangeError(f"user_stream_keepalive_failed: {e}") from e


def close_binance_user_stream(
    exchange_id: str,
    client: Any,
    listen_key: str,
) -> None:
    if exchange_id != "binance" or not hasattr(
        client,
        "fapiPrivateDeleteListenKey",
    ):
        raise ExchangeUserStreamUnsupported(
            f"user_stream_close_unsupported: exchange={exchange_id}"
        )
    try:
        client.fapiPrivateDeleteListenKey()
    except ccxt.BaseError as error:
        raise ExchangeError("user_stream_close_failed") from error


class BinanceOrderEventStream:
    """Own one Binance USD-M order-event listen-key and WebSocket session."""

    def __init__(
        self,
        *,
        client: Any,
        testnet: bool,
        resolve_client_order_id: Callable[[str], str],
        connect: Callable[..., _Connection] = connect,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._client = client
        self._testnet = testnet
        self._resolve_client_order_id = resolve_client_order_id
        self._connect = connect
        self._monotonic = monotonic
        self._connection: _Connection | None = None
        self._listen_key: str | None = None
        self._next_keepalive_at = 0.0

    def start(self) -> None:
        if self._connection is not None or self._listen_key is not None:
            self.close()
        try:
            listen_key = create_binance_user_stream_listen_key(
                "binance",
                self._client,
            )
        except Exception:
            raise NetworkError("binance_order_event_stream_start_failed") from None
        self._listen_key = listen_key
        base_url = _TESTNET_STREAM_URL if self._testnet else _PRODUCTION_STREAM_URL
        try:
            self._connection = self._connect(
                f"{base_url}/{listen_key}",
                open_timeout=10,
                close_timeout=10,
            )
        except Exception:
            self.close()
            raise NetworkError("binance_order_event_stream_start_failed") from None
        self._next_keepalive_at = self._monotonic() + _KEEPALIVE_INTERVAL_SECONDS

    def poll(self) -> ExchangeOrderEvent | None:
        connection = self._connection
        if connection is None or self._listen_key is None:
            raise NetworkError("binance_order_event_stream_not_started")
        if self._monotonic() >= self._next_keepalive_at:
            try:
                keepalive_binance_user_stream(
                    "binance",
                    self._client,
                    self._listen_key,
                )
            except Exception:
                raise NetworkError(
                    "binance_order_event_stream_keepalive_failed"
                ) from None
            self._next_keepalive_at = self._monotonic() + _KEEPALIVE_INTERVAL_SECONDS
        try:
            message = connection.recv(timeout=0.1)
        except TimeoutError:
            return None
        except Exception:
            raise NetworkError("binance_order_event_stream_receive_failed") from None
        return self._project_message(message)

    def close(self) -> bool:
        connection, self._connection = self._connection, None
        listen_key, self._listen_key = self._listen_key, None
        clean = True
        if connection is not None:
            try:
                connection.close()
            except Exception:
                clean = False
        if listen_key is not None:
            try:
                close_binance_user_stream("binance", self._client, listen_key)
            except Exception:
                clean = False
        return clean

    def _project_message(self, message: str | bytes) -> ExchangeOrderEvent | None:
        try:
            payload = json.loads(message)
        except (TypeError, ValueError):
            raise ExchangeError("binance_order_event_payload_invalid") from None
        if type(payload) is not dict:
            raise ExchangeError("binance_order_event_payload_invalid")
        event_type = payload.get("e")
        if event_type == "listenKeyExpired":
            raise NetworkError("binance_order_event_stream_expired")
        if event_type != "ORDER_TRADE_UPDATE":
            return None
        order = payload.get("o")
        if type(order) is not dict:
            raise ExchangeError("binance_order_event_payload_invalid")
        try:
            symbol = _required_string(order, "s")
            provider_client_order_id = _required_string(order, "c")
            status = _required_string(order, "X")
            exchange_order_id = str(_required_int(order, "i"))
            event_timestamp = _required_int(payload, "E")
            cumulative_quantity = _optional_decimal(order, "z")
            cumulative_average = _optional_decimal(order, "ap")
            last_quantity = _optional_decimal(order, "l")
            last_price = _optional_decimal(order, "L")
            fee = _optional_decimal(order, "n")
            fee_asset = _optional_string(order, "N")
            reason = _optional_string(order, "er")
        except (InvalidOperation, ValueError):
            raise ExchangeError("binance_order_event_payload_invalid") from None
        return ExchangeOrderEvent(
            status=status,
            product_id=f"BINANCE:{symbol}-PERP",
            client_order_id=self._resolve_client_order_id(provider_client_order_id),
            exchange_order_id=exchange_order_id,
            cumulative_filled_quantity=cumulative_quantity,
            cumulative_average_price=cumulative_average,
            last_fill_quantity=last_quantity,
            last_fill_price=last_price,
            fee=fee,
            fee_asset=fee_asset,
            event_timestamp=event_timestamp,
            reason=reason,
        )


def _required_string(values: dict[str, object], key: str) -> str:
    value = values.get(key)
    if type(value) is not str or not value:
        raise ValueError(key)
    return value


def _optional_string(values: dict[str, object], key: str) -> str | None:
    value = values.get(key)
    if value is None:
        return None
    if type(value) is not str:
        raise ValueError(key)
    return value


def _required_int(values: dict[str, object], key: str) -> int:
    value = values.get(key)
    if type(value) is not int:
        raise ValueError(key)
    return value


def _optional_decimal(values: dict[str, object], key: str) -> Decimal | None:
    value = values.get(key)
    if value is None:
        return None
    if type(value) is not str or not value:
        raise ValueError(key)
    return Decimal(value)
