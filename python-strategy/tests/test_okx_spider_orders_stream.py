from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import logging
from collections.abc import Awaitable, Callable, Mapping, Sequence
from typing import Any

import pytest
from websockets.asyncio.server import ServerConnection, serve

from src.core.adapters import okx_spider_orders_stream as orders_stream
from src.core.adapters.okx_spider_orders_stream import (
    OkxSpiderOrdersError,
    OkxSpiderOrdersSession,
)


def _wire(value: object) -> str:
    return json.dumps(value, separators=(",", ":"))


def _count_notice(count: str = "1") -> dict[str, object]:
    return {
        "event": "channel-conn-count",
        "channel": "orders",
        "connCount": count,
        "connId": "private-conn-id",
    }


def _order_envelope(
    rows: Mapping[str, object] | Sequence[Mapping[str, object]],
) -> dict[str, object]:
    data = [dict(rows)] if isinstance(rows, Mapping) else [dict(row) for row in rows]
    return {"arg": {"channel": "orders", "instType": "SWAP"}, "data": data}


async def _local_server(
    monkeypatch: pytest.MonkeyPatch,
    handler: Callable[[ServerConnection], Awaitable[None]],
) -> tuple[Any, str]:
    server_logger = logging.Logger("test_okx_spider_server", level=logging.WARNING)
    server_logger.disabled = True
    server = await serve(
        handler,
        "127.0.0.1",
        0,
        ping_interval=None,
        logger=server_logger,
    )
    port = server.sockets[0].getsockname()[1]
    monkeypatch.setattr(
        orders_stream,
        "_OKX_DEMO_PRIVATE_URL",
        f"ws://127.0.0.1:{port}/private",
    )
    return server, f"ws://127.0.0.1:{port}/private"


async def _expect_handshake(
    ws: ServerConnection,
    *,
    api_key: str = "dummy-key",
    secret: str = "dummy-secret",
    passphrase: str = "dummy-pass",
) -> None:
    login = json.loads(await ws.recv())
    assert login["op"] == "login"
    assert login["args"][0]["apiKey"] == api_key
    assert login["args"][0]["passphrase"] == passphrase
    timestamp = login["args"][0]["timestamp"]
    expected = base64.b64encode(
        hmac.new(
            secret.encode(),
            f"{timestamp}GET/users/self/verify".encode(),
            hashlib.sha256,
        ).digest()
    ).decode()
    assert login["args"][0]["sign"] == expected
    await ws.send(_wire({"event": "login", "code": "0"}))
    subscription = json.loads(await ws.recv())
    assert subscription == {
        "op": "subscribe",
        "args": [{"channel": "orders", "instType": "SWAP"}],
    }


def _session() -> OkxSpiderOrdersSession:
    return OkxSpiderOrdersSession("dummy-key", "dummy-secret", "dummy-pass")


def test_login_signature_fixture(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(orders_stream, "_timestamp_seconds", lambda: "1700000000")
    expected = base64.b64encode(
        hmac.new(
            b"secret",
            b"1700000000GET/users/self/verify",
            hashlib.sha256,
        ).digest()
    ).decode()
    assert orders_stream._login_signature("secret", "1700000000") == expected


def test_successful_local_login_subscription_and_order_preservation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = [
        {
            "instId": "BTC-USDT-SWAP",
            "ordId": "",
            "accFillSz": "0.2",
            "fee": "-0.01",
            "ccy": "USDT",
        },
        {"instId": "ETH-USDT-SWAP", "accFillSz": "0", "fee": "0", "ccy": "USDC"},
    ]
    envelopes = [
        _order_envelope(rows),
        *(_order_envelope(rows[0]) for _ in range(19)),
    ]
    received_ops: list[str | bytes] = []

    async def handler(ws: ServerConnection) -> None:
        await _expect_handshake(ws)
        await ws.send(_wire(_count_notice("2")))
        await ws.send(
            _wire(
                {
                    "event": "subscribe",
                    "arg": {"channel": "orders", "instType": "SWAP"},
                }
            )
        )
        await ws.send(_wire(_count_notice("3")))
        for envelope in envelopes:
            await ws.send(_wire(envelope))
        try:
            async for frame in ws:
                received_ops.append(frame)
        except Exception:
            pass

    async def scenario() -> None:
        server, _ = await _local_server(monkeypatch, handler)
        try:
            async with _session() as session:
                assert session.subscribed
                for envelope in envelopes:
                    assert await session.receive() == envelope
                assert session.subscribed
        finally:
            server.close()
            await server.wait_closed()
        assert received_ops == []

    asyncio.run(scenario())


@pytest.mark.parametrize(
    ("mode", "reason"),
    [
        ("login-reject", "LOGIN_REJECTED"),
        ("login-no-ack", "HANDSHAKE_TIMEOUT"),
        ("subscribe-mismatch", "SUBSCRIPTION_REJECTED"),
        ("subscribe-error", "SUBSCRIPTION_REJECTED"),
        ("subscribe-no-ack", "HANDSHAKE_TIMEOUT"),
        ("order-before-ack", "SUBSCRIPTION_REJECTED"),
        ("conn-count-error", "CONNECTION_COUNT_ERROR"),
        ("bad-count-negative", "CONNECTION_COUNT_INVALID"),
        ("bad-count-type", "CONNECTION_COUNT_INVALID"),
        ("bad-count-channel", "CONNECTION_COUNT_INVALID"),
        ("bad-json", "FRAME_INVALID_JSON"),
        ("binary-frame", "FRAME_NOT_TEXT"),
        ("oversize-frame", "RECEIVE_FAILED"),
        ("bad-envelope", "ORDERS_ENVELOPE_INVALID"),
        ("notice", "SERVER_NOTICE"),
        ("disconnect", "RECEIVE_FAILED"),
    ],
)
def test_handshake_and_receive_failures_are_sanitized_and_close(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    reason: str,
) -> None:
    monkeypatch.setattr(orders_stream, "_HANDSHAKE_TIMEOUT_SECONDS", 0.05)

    async def handler(ws: ServerConnection) -> None:
        login = json.loads(await ws.recv())
        assert login["op"] == "login"
        if mode == "login-reject":
            await ws.send(
                _wire({"event": "login", "code": "1", "msg": "secret provider text"})
            )
            return
        if mode == "login-no-ack":
            await asyncio.sleep(0.2)
            return
        await ws.send(_wire({"event": "login", "code": "0"}))
        subscription = json.loads(await ws.recv())
        assert subscription == {
            "op": "subscribe",
            "args": [{"channel": "orders", "instType": "SWAP"}],
        }
        if mode == "subscribe-no-ack":
            await asyncio.sleep(0.2)
        elif mode == "order-before-ack":
            await ws.send(_wire(_order_envelope({"ordId": "premature"})))
        elif mode == "subscribe-mismatch":
            await ws.send(
                _wire(
                    {
                        "event": "subscribe",
                        "code": "0",
                        "arg": {"channel": "orders", "instType": "SPOT"},
                    }
                )
            )
        elif mode == "subscribe-error":
            await ws.send(
                _wire(
                    {
                        "event": "subscribe",
                        "code": "1",
                        "msg": "private provider detail",
                    }
                )
            )
        else:
            await ws.send(
                _wire(
                    {
                        "event": "subscribe",
                        "code": "0",
                        "arg": {"channel": "orders", "instType": "SWAP"},
                    }
                )
            )
            if mode == "conn-count-error":
                await ws.send(
                    _wire(
                        {
                            "event": "channel-conn-count-error",
                            "connId": "private-conn-id",
                        }
                    )
                )
            elif mode.startswith("bad-count"):
                notice = _count_notice("1" if mode == "bad-count-channel" else "-1")
                if mode == "bad-count-type":
                    notice["connCount"] = 1
                elif mode == "bad-count-channel":
                    notice["channel"] = "positions"
                await ws.send(_wire(notice))
            elif mode == "bad-json":
                await ws.send("{provider-private-frame")
            elif mode == "binary-frame":
                await ws.send(b"not-json")
            elif mode == "oversize-frame":
                try:
                    await ws.send("x" * (orders_stream._MAX_MESSAGE_BYTES + 1))
                except Exception:
                    return
            elif mode == "bad-envelope":
                await ws.send(
                    _wire({"arg": {"channel": "orders"}, "data": ["not-an-object"]})
                )
            elif mode == "notice":
                await ws.send(
                    _wire(
                        {
                            "event": "notice",
                            "code": "64008",
                            "msg": "private shutdown details",
                        }
                    )
                )
            elif mode == "disconnect":
                await ws.close()
            elif mode == "login-reject":
                return

    async def scenario() -> None:
        server, _ = await _local_server(monkeypatch, handler)
        session = _session()
        try:
            try:
                async with session:
                    await session.receive()
                raise AssertionError("expected sanitized failure")
            except OkxSpiderOrdersError as exc:
                assert exc.reason_code == reason
                assert str(exc) == reason
                assert exc.__cause__ is None
            assert not session.subscribed
            await session.close()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_idle_ping_pong_and_orders_do_not_extend_pong_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orders_stream, "_IDLE_PING_SECONDS", 0.01)
    monkeypatch.setattr(orders_stream, "_PONG_TIMEOUT_SECONDS", 0.05)

    async def handler(ws: ServerConnection) -> None:
        await _expect_handshake(ws)
        await ws.send(
            _wire(
                {
                    "event": "subscribe",
                    "code": "0",
                    "arg": {"channel": "orders", "instType": "SWAP"},
                }
            )
        )
        assert await ws.recv() == "ping"
        await ws.send(_wire(_order_envelope({"ordId": "order-during-pong"})))
        await asyncio.sleep(0.08)

    async def scenario() -> None:
        server, _ = await _local_server(monkeypatch, handler)
        session = _session()
        try:
            await session.__aenter__()
            assert await session.receive() == _order_envelope(
                {"ordId": "order-during-pong"}
            )
            with pytest.raises(OkxSpiderOrdersError, match="^PONG_TIMEOUT$"):
                await session.receive()
            assert not session.subscribed
        finally:
            await session.close()
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_text_pong_observed_and_receive_cancellation_closes_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(orders_stream, "_IDLE_PING_SECONDS", 0.01)

    async def handler(ws: ServerConnection) -> None:
        await _expect_handshake(ws)
        await ws.send(
            _wire(
                {
                    "event": "subscribe",
                    "code": "0",
                    "arg": {"channel": "orders", "instType": "SWAP"},
                }
            )
        )
        assert await ws.recv() == "ping"
        await ws.send("pong")
        await ws.send(_wire(_order_envelope({"ordId": "after-pong"})))
        try:
            await ws.recv()
        except Exception:
            return

    async def scenario() -> None:
        server, _ = await _local_server(monkeypatch, handler)
        session = _session()
        try:
            await session.__aenter__()
            assert await session.receive() == _order_envelope({"ordId": "after-pong"})
            assert session.text_pong_count == 1
            task = asyncio.create_task(session.receive())
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            assert not session.subscribed
            await session.close()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())


def test_root_debug_logging_never_receives_private_frames_or_credentials(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG")

    async def handler(ws: ServerConnection) -> None:
        await ws.recv()
        await ws.send(
            _wire({"event": "login", "code": "1", "msg": "PRIVATE_PROVIDER_SENTINEL"})
        )

    async def scenario() -> None:
        server, _ = await _local_server(monkeypatch, handler)
        try:
            with pytest.raises(OkxSpiderOrdersError) as caught:
                async with OkxSpiderOrdersSession(
                    "PRIVATE_KEY_SENTINEL",
                    "PRIVATE_SECRET_SENTINEL",
                    "PRIVATE_PASS_SENTINEL",
                ):
                    pass
            assert caught.value.reason_code == "LOGIN_REJECTED"
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())
    captured = caplog.text
    for sentinel in (
        "PRIVATE_KEY_SENTINEL",
        "PRIVATE_SECRET_SENTINEL",
        "PRIVATE_PASS_SENTINEL",
        "PRIVATE_PROVIDER_SENTINEL",
    ):
        assert sentinel not in captured


def test_root_debug_logging_keeps_successful_order_frame_private(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level("DEBUG")
    row = {"instId": "PRIVATE_ORDER_SENTINEL", "clOrdId": "", "ordId": ""}

    async def handler(ws: ServerConnection) -> None:
        await _expect_handshake(
            ws,
            api_key="PRIVATE_KEY_SENTINEL",
            secret="PRIVATE_SECRET_SENTINEL",
            passphrase="PRIVATE_PASS_SENTINEL",
        )
        await ws.send(
            _wire(
                {
                    "event": "subscribe",
                    "arg": {"channel": "orders", "instType": "SWAP"},
                }
            )
        )
        await ws.send(_wire(_order_envelope(row)))
        try:
            await ws.recv()
        except Exception:
            return

    async def scenario() -> None:
        server, _ = await _local_server(monkeypatch, handler)
        try:
            async with OkxSpiderOrdersSession(
                "PRIVATE_KEY_SENTINEL",
                "PRIVATE_SECRET_SENTINEL",
                "PRIVATE_PASS_SENTINEL",
            ) as session:
                assert await session.receive() == _order_envelope(row)
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(scenario())
    for sentinel in (
        "PRIVATE_KEY_SENTINEL",
        "PRIVATE_SECRET_SENTINEL",
        "PRIVATE_PASS_SENTINEL",
        "PRIVATE_ORDER_SENTINEL",
    ):
        assert sentinel not in caplog.text


def test_session_uses_direct_fixed_demo_connector_and_rejects_redirect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from websockets.exceptions import InvalidStatus
    from websockets.datastructures import Headers
    from websockets.http11 import Response
    from websockets.asyncio.client import connect

    fixed_demo_url = "wss://wspap.okx.com/ws/v5/private"
    assert orders_stream._OKX_DEMO_PRIVATE_URL == fixed_demo_url
    redirect = InvalidStatus(
        Response(
            302,
            "Found",
            Headers({"Location": "wss://redirect.example/private"}),
            b"",
        )
    )
    assert (
        connect(fixed_demo_url).process_redirect(redirect)
        == "wss://redirect.example/private"
    )
    assert (
        orders_stream._NoRedirectConnect(fixed_demo_url).process_redirect(redirect)
        is redirect
    )

    connector_calls: list[tuple[str, dict[str, Any]]] = []
    real_connector = orders_stream._NoRedirectConnect

    def observe_connector(uri: str, **kwargs: Any) -> Any:
        connector_calls.append((uri, dict(kwargs)))
        return real_connector(uri, **kwargs)

    monkeypatch.setattr(orders_stream, "_NoRedirectConnect", observe_connector)

    async def handler(ws: ServerConnection) -> None:
        await _expect_handshake(ws)
        await ws.send(
            _wire(
                {
                    "event": "subscribe",
                    "arg": {"channel": "orders", "instType": "SWAP"},
                }
            )
        )
        try:
            await ws.recv()
        except Exception:
            return

    async def scenario() -> None:
        server, local_url = await _local_server(monkeypatch, handler)
        try:
            async with _session() as session:
                assert session.subscribed
        finally:
            server.close()
            await server.wait_closed()
        assert len(connector_calls) == 1
        uri, kwargs = connector_calls[0]
        assert uri == local_url
        assert kwargs["proxy"] is None
        assert kwargs["ping_interval"] is None
        assert kwargs["open_timeout"] <= 5
        assert kwargs["close_timeout"] <= 5
        assert kwargs["max_queue"] == 16
        assert kwargs["max_size"] == 1_048_576

    asyncio.run(scenario())
