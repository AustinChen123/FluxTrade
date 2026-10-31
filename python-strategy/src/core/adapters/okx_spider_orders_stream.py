"""Receive-only OKX Demo private orders transport for Spider."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
import time
from collections.abc import Mapping
from typing import Any

from websockets.asyncio.client import ClientConnection, connect

_OKX_DEMO_PRIVATE_URL = "wss://wspap.okx.com/ws/v5/private"
_CONNECT_TIMEOUT_SECONDS = 5
_CLOSE_TIMEOUT_SECONDS = 5
_HANDSHAKE_TIMEOUT_SECONDS = 10
_IDLE_PING_SECONDS = 15
_PONG_TIMEOUT_SECONDS = 10
_MAX_MESSAGE_BYTES = 1_048_576


class OkxSpiderOrdersError(RuntimeError):
    """Sanitized failure from the receive-only OKX orders session."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class _NoRedirectConnect(connect):
    def process_redirect(self, exc: Exception) -> Exception:
        return exc


def _timestamp_seconds() -> str:
    return str(int(time.time()))


def _login_signature(secret: str, timestamp: str) -> str:
    preimage = f"{timestamp}GET/users/self/verify".encode()
    digest = hmac.new(secret.encode(), preimage, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("ascii")


def _decode_envelope(raw: str | bytes) -> dict[str, Any]:
    if type(raw) is not str:
        raise OkxSpiderOrdersError("FRAME_NOT_TEXT")
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeError):
        raise OkxSpiderOrdersError("FRAME_INVALID_JSON") from None
    if type(value) is not dict:
        raise OkxSpiderOrdersError("ENVELOPE_INVALID")
    return value


def _is_success_code(value: object) -> bool:
    return type(value) is str and value == "0"


def _validate_count_notice(envelope: Mapping[str, Any]) -> None:
    count = envelope.get("connCount")
    if (
        envelope.get("channel") != "orders"
        or type(count) is not str
        or not count.isascii()
        or not count.isdigit()
    ):
        raise OkxSpiderOrdersError("CONNECTION_COUNT_INVALID")


class OkxSpiderOrdersSession:
    """One authenticated Demo orders subscription; no reconnect or order writes."""

    def __init__(self, api_key: str, secret: str, passphrase: str) -> None:
        if any(
            type(value) is not str or not value.strip()
            for value in (api_key, secret, passphrase)
        ):
            raise OkxSpiderOrdersError("CREDENTIALS_INVALID")
        self._api_key = api_key
        self._secret = secret
        self._passphrase = passphrase
        self._connector: _NoRedirectConnect | None = None
        self._connection: ClientConnection | None = None
        self._subscribed = False
        self._closed = False
        self._last_frame_at = 0.0
        self._pong_deadline: float | None = None
        self._text_pong_count = 0

    @property
    def subscribed(self) -> bool:
        return self._subscribed and not self._closed

    @property
    def text_pong_count(self) -> int:
        return self._text_pong_count

    async def __aenter__(self) -> OkxSpiderOrdersSession:
        if self._closed or self._connection is not None:
            raise OkxSpiderOrdersError("SESSION_ALREADY_USED")
        logger = logging.Logger("okx_spider_orders_wire", level=logging.WARNING)
        logger.disabled = True
        logger.propagate = False
        self._connector = _NoRedirectConnect(
            _OKX_DEMO_PRIVATE_URL,
            proxy=None,
            open_timeout=_CONNECT_TIMEOUT_SECONDS,
            close_timeout=_CLOSE_TIMEOUT_SECONDS,
            ping_interval=None,
            max_size=_MAX_MESSAGE_BYTES,
            max_queue=16,
            logger=logger,
        )
        try:
            self._connection = await self._connector.__aenter__()
            self._last_frame_at = asyncio.get_running_loop().time()
            async with asyncio.timeout(_HANDSHAKE_TIMEOUT_SECONDS):
                await self._login()
                await self._subscribe()
            self._subscribed = True
            return self
        except asyncio.CancelledError:
            try:
                await self.close()
            except Exception:
                pass
            raise
        except OkxSpiderOrdersError:
            await self.close()
            raise
        except TimeoutError:
            await self.close()
            raise OkxSpiderOrdersError("HANDSHAKE_TIMEOUT") from None
        except Exception:
            await self.close()
            raise OkxSpiderOrdersError("CONNECT_OR_HANDSHAKE_FAILED") from None

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._subscribed = False
        connector, self._connector = self._connector, None
        connection, self._connection = self._connection, None
        if connector is None:
            return
        try:
            await connector.__aexit__(None, None, None)
        except asyncio.CancelledError:
            if connection is not None:
                connection.transport.abort()
            raise
        except Exception:
            if connection is not None:
                connection.transport.abort()
            raise OkxSpiderOrdersError("CLOSE_FAILED") from None

    async def receive(self) -> dict[str, Any]:
        if not self.subscribed or self._connection is None:
            raise OkxSpiderOrdersError("SESSION_NOT_SUBSCRIBED")
        try:
            while True:
                envelope = await self._receive_envelope()
                arg = envelope.get("arg")
                if (
                    type(arg) is not dict
                    or arg.get("channel") != "orders"
                    or type(envelope.get("data")) is not list
                    or any(type(row) is not dict for row in envelope["data"])
                ):
                    raise OkxSpiderOrdersError("ORDERS_ENVELOPE_INVALID")
                return envelope
        except asyncio.CancelledError:
            try:
                await self.close()
            except Exception:
                pass
            raise
        except OkxSpiderOrdersError:
            await self.close()
            raise
        except TimeoutError:
            await self.close()
            raise OkxSpiderOrdersError("PONG_TIMEOUT") from None
        except Exception:
            await self.close()
            raise OkxSpiderOrdersError("RECEIVE_FAILED") from None

    async def _login(self) -> None:
        timestamp = _timestamp_seconds()
        await self._send(
            {
                "op": "login",
                "args": [
                    {
                        "apiKey": self._api_key,
                        "passphrase": self._passphrase,
                        "timestamp": timestamp,
                        "sign": _login_signature(self._secret, timestamp),
                    }
                ],
            }
        )
        while True:
            message = await self._read_message(timeout=None)
            if message == "ping":
                await self._send_text("pong")
                continue
            if message in {"pong"}:
                continue
            envelope = _decode_envelope(message)
            event = envelope.get("event")
            if event == "channel-conn-count":
                _validate_count_notice(envelope)
                continue
            if event != "login" or not _is_success_code(envelope.get("code")):
                raise OkxSpiderOrdersError("LOGIN_REJECTED")
            return

    async def _subscribe(self) -> None:
        await self._send(
            {
                "op": "subscribe",
                "args": [{"channel": "orders", "instType": "SWAP"}],
            }
        )
        while True:
            message = await self._read_message(timeout=None)
            if message == "ping":
                await self._send_text("pong")
                continue
            if message == "pong":
                continue
            envelope = _decode_envelope(message)
            event = envelope.get("event")
            if event == "channel-conn-count":
                _validate_count_notice(envelope)
                continue
            arg = envelope.get("arg")
            if (
                event != "subscribe"
                or ("code" in envelope and not _is_success_code(envelope.get("code")))
                or type(arg) is not dict
                or arg.get("channel") != "orders"
                or arg.get("instType") != "SWAP"
            ):
                raise OkxSpiderOrdersError("SUBSCRIPTION_REJECTED")
            return

    async def _send(self, envelope: dict[str, Any]) -> None:
        assert self._connection is not None
        await self._connection.send(json.dumps(envelope, separators=(",", ":")))

    async def _send_text(self, message: str) -> None:
        assert self._connection is not None
        await self._connection.send(message)

    async def _read_message(self, *, timeout: float | None) -> str:
        assert self._connection is not None
        try:
            if timeout is None:
                raw = await self._connection.recv()
            else:
                async with asyncio.timeout(timeout):
                    raw = await self._connection.recv()
        except TimeoutError:
            raise
        self._last_frame_at = asyncio.get_running_loop().time()
        if type(raw) is not str:
            raise OkxSpiderOrdersError("FRAME_NOT_TEXT")
        return raw

    async def _receive_envelope(self) -> dict[str, Any]:
        loop = asyncio.get_running_loop()
        while True:
            now = loop.time()
            deadline = self._pong_deadline
            if deadline is not None and now >= deadline:
                raise TimeoutError
            if deadline is None:
                idle_for = now - self._last_frame_at
                if idle_for >= _IDLE_PING_SECONDS:
                    await self._send_text("ping")
                    self._pong_deadline = now + _PONG_TIMEOUT_SECONDS
                    deadline = self._pong_deadline
                else:
                    deadline = self._last_frame_at + _IDLE_PING_SECONDS
            remaining = max(0.0, deadline - loop.time())
            try:
                message = await self._read_message(timeout=remaining)
            except TimeoutError:
                if self._pong_deadline is not None:
                    raise
                continue
            if message == "ping":
                await self._send_text("pong")
                continue
            if message == "pong":
                if self._pong_deadline is None:
                    raise OkxSpiderOrdersError("UNEXPECTED_PONG")
                self._pong_deadline = None
                self._text_pong_count += 1
                self._last_frame_at = loop.time()
                continue
            envelope = _decode_envelope(message)
            if envelope.get("event") == "channel-conn-count-error":
                raise OkxSpiderOrdersError("CONNECTION_COUNT_ERROR")
            if envelope.get("event") == "channel-conn-count":
                _validate_count_notice(envelope)
                continue
            if envelope.get("event") == "notice":
                raise OkxSpiderOrdersError("SERVER_NOTICE")
            return envelope
