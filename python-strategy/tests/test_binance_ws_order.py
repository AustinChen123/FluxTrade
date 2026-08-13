import asyncio
from concurrent.futures import Future
import inspect
import json
from unittest.mock import MagicMock, patch

import pytest

from src.core.adapters.binance_ws_order import (
    BinanceWebSocketOrderConnector,
    ExchangeAck,
    OrderAckTimeout,
    OrderRejected,
    _sign_payload_binance,
)
from src.core.interfaces.exchange import NetworkError


def test_binance_owner_has_no_exchange_selector_or_backpack_placeholder() -> None:
    signature = inspect.signature(BinanceWebSocketOrderConnector)
    module = inspect.getmodule(BinanceWebSocketOrderConnector)
    assert module is not None
    source = inspect.getsource(module)

    assert "exchange_id" not in signature.parameters
    assert tuple(
        inspect.signature(BinanceWebSocketOrderConnector.is_connected).parameters
    ) == ("self",)
    assert "backpack" not in source.lower()


@pytest.mark.parametrize(
    ("testnet", "expected"),
    [
        (True, "wss://testnet.binancefuture.com/ws-fapi/v1"),
        (False, "wss://ws-fapi.binance.com/ws-fapi/v1"),
    ],
)
def test_binance_websocket_url_is_preserved(testnet: bool, expected: str) -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret", testnet=testnet)

    assert connector.ws_url == expected


def test_sign_payload_binance_matches_known_hmac_sha256_vector() -> None:
    payload = "The quick brown fox jumps over the lazy dog"
    secret = "key"

    assert (
        _sign_payload_binance(payload, secret)
        == "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
    )


def test_sign_payload_binance_sorts_dict_params_by_name() -> None:
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
    secret = "test-secret"

    assert (
        _sign_payload_binance(payload, secret)
        == "493c124c259acf778bf59dcf6fd9da8b59297b0c3ca665f0255f6192b6d811c3"
    )


def test_sign_payload_binance_changes_when_payload_changes() -> None:
    secret = "test-secret"

    assert _sign_payload_binance("timestamp=1", secret) != _sign_payload_binance(
        "timestamp=2",
        secret,
    )


def test_wait_for_ack_returns_and_cleans_registry() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")
    connector._record_ack("coid-1", ExchangeAck("ex-1", "ACK"))

    ack = asyncio.run(connector._wait_for_ack("coid-1", timeout=0.1))

    assert ack == ExchangeAck("ex-1", "ACK")
    assert "coid-1" not in connector._ack_registry


def test_wait_for_ack_times_out() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")

    with pytest.raises(OrderAckTimeout, match="coid-missing"):
        asyncio.run(connector._wait_for_ack("coid-missing", timeout=0.01))


def test_handle_message_records_ack() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")

    connector._handle_message(
        '{"id":"coid-1","status":200,"result":'
        '{"clientOrderId":"coid-1","orderId":123,"status":"NEW"}}'
    )

    assert connector._ack_registry["coid-1"] == ExchangeAck("123", "NEW")


def test_handle_message_rejects_without_rendering_provider_message() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")

    connector._handle_message(
        '{"id":"coid-1","status":400,"error":'
        '{"code":-1102,"msg":"PROVIDER_SECRET_SENTINEL"}}'
    )

    with pytest.raises(OrderRejected) as exc_info:
        asyncio.run(connector._wait_for_ack("coid-1", timeout=0.1))
    assert exc_info.value.status == 400
    assert exc_info.value.code == -1102
    assert "PROVIDER_SECRET_SENTINEL" not in str(exc_info.value)


@pytest.mark.parametrize(
    "response",
    [
        '{"id":"coid-1","status":500,"error":'
        '{"code":-1000,"msg":"PROVIDER_SECRET_SENTINEL"}}',
        '{"id":"coid-1","status":400,"error":'
        '{"code":-1007,"msg":"PROVIDER_SECRET_SENTINEL"}}',
        '{"id":"coid-1","status":400,"error":'
        '{"code":-1006,"msg":"PROVIDER_SECRET_SENTINEL"}}',
        '{"id":"coid-1","status":408,"error":'
        '{"code":-1000,"msg":"PROVIDER_SECRET_SENTINEL"}}',
    ],
)
def test_handle_message_classifies_unknown_execution_as_ambiguous(
    response: str,
) -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")

    connector._handle_message(response)

    with pytest.raises(NetworkError) as exc_info:
        asyncio.run(connector._wait_for_ack("coid-1", timeout=0.1))
    assert "ambiguous" in str(exc_info.value)
    assert "PROVIDER_SECRET_SENTINEL" not in str(exc_info.value)


def test_handle_message_does_not_ack_mismatched_client_order_id() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")

    connector._handle_message(
        '{"id":"coid-1","status":200,"result":'
        '{"clientOrderId":"different","orderId":123,"status":"NEW"}}'
    )

    with pytest.raises(OrderAckTimeout):
        asyncio.run(connector._wait_for_ack("coid-1", timeout=0.01))


def test_place_market_order_sends_signed_official_request() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")
    connector.running = True
    captured: dict[str, str] = {}

    class WebSocket:
        async def send(self, data: str) -> None:
            captured["payload"] = data

    connector.ws = WebSocket()
    connector.loop = MagicMock()

    def schedule(coro, _loop):
        asyncio.run(coro)
        future: Future[None] = Future()
        future.set_result(None)
        return future

    with (
        patch(
            "src.core.adapters.binance_ws_order.asyncio.run_coroutine_threadsafe",
            side_effect=schedule,
        ),
        patch(
            "src.core.adapters.binance_ws_order.time.time", return_value=1700000000.123
        ),
    ):
        result = connector.place_order(
            symbol="BTCUSDT",
            side="buy",
            quantity="0.1",
            order_type="market",
            client_order_id="client-123",
        )

    assert result is True
    payload = json.loads(captured["payload"])
    assert payload["id"] == "client-123"
    assert payload["method"] == "order.place"
    assert payload["params"] == {
        "apiKey": "key",
        "newClientOrderId": "client-123",
        "quantity": "0.1",
        "recvWindow": 5000,
        "side": "BUY",
        "signature": "bb2d5fcf2c956ecad7e674e09389aeddc2b4c8c116b919e6e944796bb2b50ad0",
        "symbol": "BTCUSDT",
        "timestamp": 1700000000123,
        "type": "MARKET",
    }
    assert payload["params"]["newClientOrderId"] == "client-123"
    assert "price" not in payload["params"]
    assert "timeInForce" not in payload["params"]


def test_place_order_requires_client_id_before_scheduling() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")
    connector.running = True
    connector.ws = MagicMock()
    connector.loop = MagicMock()

    with patch(
        "src.core.adapters.binance_ws_order.asyncio.run_coroutine_threadsafe"
    ) as schedule:
        assert connector.place_order("BTCUSDT", "buy", "0.1") is False

    schedule.assert_not_called()
    connector.ws.send.assert_not_called()


def test_synchronous_scheduling_failure_is_safe_fallback() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")
    connector.running = True
    connector.ws = MagicMock()
    connector.loop = MagicMock()

    async def send(_data: str) -> None:
        return None

    connector.ws.send.side_effect = send

    def fail_before_schedule(coro, _loop):
        coro.close()
        raise RuntimeError("local scheduler unavailable")

    with patch(
        "src.core.adapters.binance_ws_order.asyncio.run_coroutine_threadsafe",
        side_effect=fail_before_schedule,
    ):
        assert (
            connector.place_order(
                "BTCUSDT",
                "buy",
                "0.1",
                client_order_id="client-123",
            )
            is False
        )


def test_scheduled_send_failure_is_ambiguous_network_error() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")
    connector.running = True
    connector.ws = MagicMock()
    connector.loop = MagicMock()

    async def send(_data: str) -> None:
        return None

    connector.ws.send.side_effect = send
    failed: Future[None] = Future()
    failed.set_exception(RuntimeError("socket failed after scheduling"))

    def schedule(coro, _loop):
        coro.close()
        return failed

    with (
        patch(
            "src.core.adapters.binance_ws_order.asyncio.run_coroutine_threadsafe",
            side_effect=schedule,
        ),
        pytest.raises(NetworkError, match="ambiguous"),
    ):
        connector.place_order(
            "BTCUSDT",
            "buy",
            "0.1",
            client_order_id="client-123",
        )


def test_connected_state_is_intrinsically_binance_owned() -> None:
    connector = BinanceWebSocketOrderConnector("key", "secret")
    connector.running = True
    connector.ws = MagicMock()

    assert connector.is_connected() is True
