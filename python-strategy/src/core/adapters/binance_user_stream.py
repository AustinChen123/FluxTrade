"""Binance USD-M Futures user-stream protocol operations."""

from typing import Any

import ccxt

from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeUserStreamUnsupported,
)


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
        client.fapiPrivatePutListenKey({"listenKey": listen_key})
    except ccxt.BaseError as e:
        raise ExchangeError(f"user_stream_keepalive_failed: {e}") from e
