import asyncio
import json
from unittest.mock import MagicMock, patch

import pytest

from src.core.ws_connector import (
    ExchangeAck,
    OrderAckTimeout,
    WebSocketOrderConnector,
    _sign_payload_binance,
)


def test_sign_payload_binance_matches_known_hmac_sha256_vector() -> None:
    payload = "The quick brown fox jumps over the lazy dog"
    secret = "key"

    assert (
        _sign_payload_binance(payload, secret)
        == "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
    )


def test_sign_payload_binance_accepts_dict_payloads_in_insertion_order() -> None:
    payload = {
        "symbol": "LTCBTC",
        "side": "BUY",
        "type": "LIMIT",
        "timeInForce": "GTC",
        "quantity": "1",
        "price": "0.1",
        "recvWindow": "5000",
        "timestamp": "1499827319559",
    }
    secret = "NhqPtmdSJYYUnHB3O3rQ5JYkqV8nG8fI"

    assert (
        _sign_payload_binance(payload, secret)
        == "d9d6f1eae0e75783037ddf14087af6307f4f0da8a9dc6da99a4e7778eccd536a"
    )


def test_sign_payload_binance_changes_when_payload_changes() -> None:
    secret = "NhqPtmdSJYYUnHB3O3rQ5JYkqV8nG8fI"

    assert _sign_payload_binance("timestamp=1", secret) != _sign_payload_binance(
        "timestamp=2",
        secret,
    )


def test_wait_for_ack_returns_and_cleans_registry() -> None:
    connector = WebSocketOrderConnector("key", "secret")
    connector._record_ack("coid-1", ExchangeAck("ex-1", "ACK"))

    ack = asyncio.run(connector._wait_for_ack("coid-1", timeout=0.1))

    assert ack == ExchangeAck("ex-1", "ACK")
    assert "coid-1" not in connector._ack_registry


def test_wait_for_ack_times_out() -> None:
    connector = WebSocketOrderConnector("key", "secret")

    with pytest.raises(OrderAckTimeout, match="coid-missing"):
        asyncio.run(connector._wait_for_ack("coid-missing", timeout=0.01))


def test_handle_message_records_ack() -> None:
    connector = WebSocketOrderConnector("key", "secret")

    connector._handle_message(
        '{"clientOrderId":"coid-1","orderId":"ex-1","status":"SUBMITTED"}'
    )

    assert connector._ack_registry["coid-1"] == ExchangeAck("ex-1", "SUBMITTED")


def test_place_order_includes_client_order_id() -> None:
    connector = WebSocketOrderConnector("key", "secret")
    connector.running = True
    connector.ws = MagicMock()
    connector.loop = MagicMock()
    captured = {}

    async def fake_send(data):
        captured["payload"] = data

    with patch("src.core.ws_connector.asyncio.run_coroutine_threadsafe") as send:
        connector.ws.send.side_effect = fake_send
        result = connector.place_order(
            symbol="BTCUSDT",
            side="buy",
            quantity=0.1,
            order_type="market",
            client_order_id="client-123",
        )

    assert result is True
    coro = send.call_args.args[0]
    try:
        coro.send(None)
    except StopIteration:
        pass
    payload = json.loads(captured["payload"])
    assert payload["params"]["newClientOrderId"] == "client-123"
