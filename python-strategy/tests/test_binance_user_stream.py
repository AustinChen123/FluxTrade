"""Tests for the Binance-owned user-stream protocol boundary."""

import ast
import inspect
import json
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import ccxt
import pytest

import src.core.adapters.live_binance as live_binance_module
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.adapters.live_binance import LiveBinanceAdapter
from src.core.generic_order_event_stream import GenericOrderEventStream
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeUserStreamUnsupported,
    NetworkError,
)
from src.core.order_event_sync import OrderEventApplier


def _owner_functions():
    from src.core.adapters.binance_user_stream import (
        create_binance_user_stream_listen_key,
        keepalive_binance_user_stream,
    )

    return create_binance_user_stream_listen_key, keepalive_binance_user_stream


def test_order_event_owner_projects_exact_fill_without_raw_payload() -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    connection = MagicMock()
    connection.recv.return_value = json.dumps(
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1_700_000_000_123,
            "o": {
                "s": "MNQUSDT",
                "c": "provider-client-id",
                "i": 987654,
                "X": "FILLED",
                "z": "1.2500",
                "ap": "20001.1250",
                "l": "0.2500",
                "L": "20002.5000",
                "n": "0.0100",
                "N": "USDT",
                "er": "0",
            },
        }
    )
    owner = BinanceOrderEventStream(
        client=SimpleNamespace(),
        testnet=True,
        resolve_client_order_id=lambda value: {
            "provider-client-id": "canonical-client-id"
        }.get(value, value),
        connect=lambda *_args, **_kwargs: connection,
    )
    owner._connection = connection
    owner._listen_key = "listen-key"
    owner._next_keepalive_at = 1e12

    event = owner.poll()

    assert event is not None
    assert event.product_id == "BINANCE:MNQUSDT-PERP"
    assert event.client_order_id == "canonical-client-id"
    assert event.exchange_order_id == "987654"
    assert event.status == "FILLED"
    assert event.cumulative_filled_quantity == Decimal("1.2500")
    assert event.cumulative_average_price == Decimal("20001.1250")
    assert event.last_fill_quantity == Decimal("0.2500")
    assert event.last_fill_price == Decimal("20002.5000")
    assert event.fee == Decimal("0.0100")
    assert event.fee_asset == "USDT"
    assert event.event_timestamp == 1_700_000_000_123
    assert event.reason == "0"
    assert event.raw is None


@pytest.mark.parametrize(
    "status",
    [
        "NEW",
        "PARTIALLY_FILLED",
        "FILLED",
        "CANCELED",
        "REJECTED",
        "EXPIRED",
        "EXPIRED_IN_MATCH",
    ],
)
def test_order_event_owner_preserves_supported_provider_statuses(status: str) -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    connection = MagicMock()
    connection.recv.return_value = json.dumps(
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": 7,
            "o": {"s": "BTCUSDT", "c": "client", "i": 8, "X": status},
        }
    )
    owner = BinanceOrderEventStream(
        client=SimpleNamespace(),
        testnet=False,
        resolve_client_order_id=lambda value: value,
        connect=lambda *_args, **_kwargs: connection,
        monotonic=lambda: 0,
    )
    owner._connection = connection
    owner._listen_key = "key"
    owner._next_keepalive_at = 1

    event = owner.poll()

    assert event is not None
    assert event.status == status


class _ImmediateWorker:
    def __init__(self, *, target, **_kwargs) -> None:
        self._target = target

    def start(self) -> None:
        self._target()

    def is_alive(self) -> bool:
        return False

    def join(self, *, timeout: float) -> None:
        del timeout


@pytest.mark.parametrize("known_order", [True, False])
def test_expired_in_match_routes_through_existing_terminal_disposition(
    known_order: bool,
) -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    connection = MagicMock()
    connection.recv.return_value = json.dumps(
        {
            "e": "ORDER_TRADE_UPDATE",
            "E": 1_700_000_000_123,
            "o": {
                "s": "BTCUSDT",
                "c": "client",
                "i": 8,
                "X": "EXPIRED_IN_MATCH",
                "z": "0",
            },
        }
    )
    binance_stream = BinanceOrderEventStream(
        client=SimpleNamespace(),
        testnet=False,
        resolve_client_order_id=lambda value: value,
        monotonic=lambda: 0,
    )
    binance_stream._connection = connection
    binance_stream._listen_key = "key"
    binance_stream._next_keepalive_at = 1
    adapter = SimpleNamespace(
        start_order_event_stream=MagicMock(),
        poll_order_event=binance_stream.poll,
    )
    order = SimpleNamespace(
        id="local-order",
        product_id="BINANCE:BTCUSDT-PERP",
        exchange_order_id="8",
        filled_quantity=Decimal("0"),
        filled_price=None,
        quantity=Decimal("1"),
        status="NEW",
    )
    manager = MagicMock()
    manager.repo.get_order_by_client_order_id.return_value = (
        order if known_order else None
    )
    manager.repo.get_order_by_exchange_order_id.return_value = None
    cleanup = MagicMock()
    applier = OrderEventApplier(
        order_manager=manager,
        journal_fill=MagicMock(),
        fail_pending_conditionals_for_terminal_entry=cleanup,
        protective_terminal_without_fill_failure=MagicMock(return_value=None),
        write_conditional_warning=MagicMock(),
        place_pending_conditionals_for_entry=MagicMock(return_value=[]),
        protective_partial_fill_requires_resize=MagicMock(return_value=None),
        cancel_linked_conditional_for_protection_fill=MagicMock(return_value=None),
    )
    stop = MagicMock()
    stop.is_set.return_value = False
    latch = MagicMock()
    halt = MagicMock()

    def process_event(event: object) -> dict[str, object]:
        assert isinstance(event, ExchangeOrderEvent)
        result = applier.process_exchange_order_event(event)
        if known_order:
            stop.is_set.return_value = True
        return result

    worker = GenericOrderEventStream(
        adapter_loader=lambda: adapter,
        is_running=lambda: True,
        stop_event=lambda: stop,
        assert_leadership=lambda: None,
        process_event=process_event,
        latch_stream_failure=latch,
        halt_submissions=halt,
        publish_worker=lambda _worker: None,
        current_worker=lambda: None,
        event_logger=MagicMock(),
        thread_factory=_ImmediateWorker,
    )

    worker.start()

    if known_order:
        cleanup.assert_called_once_with(order)
        manager.fail_order.assert_called_once_with(order, "exchange_event_expired")
        latch.assert_not_called()
        halt.assert_not_called()
    else:
        cleanup.assert_not_called()
        manager.fail_order.assert_not_called()
        latch.assert_called_once_with()
        halt.assert_called_once_with()


@pytest.mark.parametrize(
    ("testnet", "expected_url"),
    [
        (False, "wss://fstream.binance.com/ws/listen-key"),
        (True, "wss://fstream.binancefuture.com/ws/listen-key"),
    ],
)
def test_order_event_start_uses_exact_endpoint_and_keepalive_boundary(
    testnet: bool,
    expected_url: str,
) -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    now = [10.0]
    connection = MagicMock()
    connection.recv.side_effect = TimeoutError
    client = SimpleNamespace(
        fapiPrivatePostListenKey=MagicMock(return_value={"listenKey": "listen-key"}),
        fapiPrivatePutListenKey=MagicMock(),
        fapiPrivateDeleteListenKey=MagicMock(),
    )
    connector = MagicMock(return_value=connection)
    owner = BinanceOrderEventStream(
        client=client,
        testnet=testnet,
        resolve_client_order_id=lambda value: value,
        connect=connector,
        monotonic=lambda: now[0],
    )

    owner.start()
    now[0] += 1_799.999
    assert owner.poll() is None
    client.fapiPrivatePutListenKey.assert_not_called()
    now[0] += 0.001
    assert owner.poll() is None

    connector.assert_called_once_with(
        expected_url,
        open_timeout=10,
        close_timeout=10,
    )
    client.fapiPrivatePutListenKey.assert_called_once_with()


def test_duplicate_start_closes_old_connection_and_key_before_replacement() -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    events: list[str] = []
    keys = iter(("old-key", "new-key"))

    class Connection:
        def __init__(self, key: str) -> None:
            self.key = key

        def recv(self, timeout: float | None = None) -> str:
            raise AssertionError(timeout)

        def close(self) -> None:
            events.append(f"local-close:{self.key}")

    client = SimpleNamespace(
        fapiPrivatePostListenKey=lambda: (
            events.append("create-key"),
            {"listenKey": next(keys)},
        )[1],
        fapiPrivateDeleteListenKey=lambda: events.append("remote-close"),
    )

    def connector(url: str, **_kwargs) -> Connection:
        key = url.rsplit("/", 1)[1]
        events.append(f"connect:{key}")
        return Connection(key)

    owner = BinanceOrderEventStream(
        client=client,
        testnet=True,
        resolve_client_order_id=lambda value: value,
        connect=connector,
        monotonic=lambda: 0,
    )

    owner.start()
    owner.start()

    assert events == [
        "create-key",
        "connect:old-key",
        "local-close:old-key",
        "remote-close",
        "create-key",
        "connect:new-key",
    ]


def test_connect_failure_is_sanitized_and_deletes_created_key() -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    remote_close = MagicMock()
    client = SimpleNamespace(
        fapiPrivatePostListenKey=MagicMock(return_value={"listenKey": "secret-key"}),
        fapiPrivateDeleteListenKey=remote_close,
    )
    owner = BinanceOrderEventStream(
        client=client,
        testnet=True,
        resolve_client_order_id=lambda value: value,
        connect=MagicMock(side_effect=RuntimeError("provider sentinel")),
    )

    with pytest.raises(
        NetworkError,
        match="^binance_order_event_stream_start_failed$",
    ) as caught:
        owner.start()

    assert "sentinel" not in str(caught.value)
    assert caught.value.__cause__ is None
    remote_close.assert_called_once_with()
    assert owner._connection is None
    assert owner._listen_key is None


@pytest.mark.parametrize(
    ("keepalive_due", "expected"),
    [
        (False, "binance_order_event_stream_receive_failed"),
        (True, "binance_order_event_stream_keepalive_failed"),
    ],
)
def test_poll_failures_are_fixed_and_sanitized(
    keepalive_due: bool,
    expected: str,
) -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    connection = MagicMock()
    connection.recv.side_effect = RuntimeError("receive sentinel")
    keepalive = MagicMock(side_effect=ccxt.ExchangeError("keepalive sentinel"))
    owner = BinanceOrderEventStream(
        client=SimpleNamespace(fapiPrivatePutListenKey=keepalive),
        testnet=True,
        resolve_client_order_id=lambda value: value,
        monotonic=lambda: 1,
    )
    owner._connection = connection
    owner._listen_key = "secret-key"
    owner._next_keepalive_at = 0 if keepalive_due else 2

    with pytest.raises(NetworkError, match=f"^{expected}$") as caught:
        owner.poll()

    assert "sentinel" not in str(caught.value)
    assert caught.value.__cause__ is None
    if keepalive_due:
        connection.recv.assert_not_called()
    else:
        keepalive.assert_not_called()


def test_close_is_nonthrowing_and_attempts_remote_cleanup_after_local_failure() -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    connection = MagicMock()
    connection.close.side_effect = RuntimeError("local sentinel")
    remote_close = MagicMock(side_effect=ccxt.ExchangeError("remote sentinel"))
    owner = BinanceOrderEventStream(
        client=SimpleNamespace(fapiPrivateDeleteListenKey=remote_close),
        testnet=True,
        resolve_client_order_id=lambda value: value,
    )
    owner._connection = connection
    owner._listen_key = "secret-listen-key"

    assert owner.close() is False

    connection.close.assert_called_once_with()
    remote_close.assert_called_once_with()
    assert owner._connection is None
    assert owner._listen_key is None


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"e": "ACCOUNT_UPDATE"}, None),
        ({"e": "listenKeyExpired"}, "binance_order_event_stream_expired"),
        ({"e": "ORDER_TRADE_UPDATE", "E": 1, "o": {}}, "payload_invalid"),
    ],
)
def test_poll_classifies_unrelated_expired_and_malformed_events(
    payload: dict[str, object],
    expected: str | None,
) -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    connection = MagicMock()
    connection.recv.return_value = json.dumps(payload)
    owner = BinanceOrderEventStream(
        client=SimpleNamespace(),
        testnet=True,
        resolve_client_order_id=lambda value: value,
        monotonic=lambda: 0,
    )
    owner._connection = connection
    owner._listen_key = "key"
    owner._next_keepalive_at = 1

    if expected is None:
        assert owner.poll() is None
    elif expected == "payload_invalid":
        with pytest.raises(
            ExchangeError,
            match="^binance_order_event_payload_invalid$",
        ):
            owner.poll()
    else:
        with pytest.raises(NetworkError, match=f"^{expected}$"):
            owner.poll()


def test_poll_rejects_malformed_json_without_raw_payload_text() -> None:
    from src.core.adapters.binance_user_stream import BinanceOrderEventStream

    connection = MagicMock()
    connection.recv.return_value = "provider-payload-sentinel"
    owner = BinanceOrderEventStream(
        client=SimpleNamespace(),
        testnet=True,
        resolve_client_order_id=lambda value: value,
        monotonic=lambda: 0,
    )
    owner._connection = connection
    owner._listen_key = "key"
    owner._next_keepalive_at = 1

    with pytest.raises(
        ExchangeError,
        match="^binance_order_event_payload_invalid$",
    ) as caught:
        owner.poll()

    assert "sentinel" not in str(caught.value)


def _adapter(client: object) -> LiveBinanceAdapter:
    adapter = object.__new__(LiveBinanceAdapter)
    adapter.exchange_id = "binance"
    vars(adapter)["client"] = client
    return adapter


def test_live_adapter_delegates_create_with_exact_identities(monkeypatch) -> None:
    client = object()
    owner = MagicMock(return_value="listen-key")
    monkeypatch.setattr(
        live_binance_module,
        "create_binance_user_stream_listen_key",
        owner,
        raising=False,
    )

    assert _adapter(client).create_user_stream_listen_key() == "listen-key"
    owner.assert_called_once_with("binance", client)


def test_live_adapter_delegates_keepalive_with_exact_identities(monkeypatch) -> None:
    client = object()
    listen_key = "listen-key"
    owner = MagicMock()
    monkeypatch.setattr(
        live_binance_module,
        "keepalive_binance_user_stream",
        owner,
        raising=False,
    )

    assert _adapter(client).keepalive_user_stream(listen_key) is None
    owner.assert_called_once_with("binance", client, listen_key)


@pytest.mark.parametrize("cleanup_ok", [True, False])
def test_live_adapter_delegates_order_event_lifecycle_to_one_owner(
    cleanup_ok: bool,
) -> None:
    adapter = object.__new__(LiveBinanceAdapter)
    owner = MagicMock()
    event = object()
    owner.poll.return_value = event
    owner.close.return_value = cleanup_ok
    adapter._user_order_stream = owner
    adapter.ws_connector = MagicMock()
    adapter.logger = MagicMock()

    adapter.start_order_event_stream()
    assert adapter.poll_order_event() is event
    adapter.close()

    owner.start.assert_called_once_with()
    owner.poll.assert_called_once_with()
    owner.close.assert_called_once_with()
    assert adapter.ws_connector.running is False
    if cleanup_ok:
        adapter.logger.warning.assert_not_called()
    else:
        adapter.logger.warning.assert_called_once_with(
            "Binance order event stream cleanup failed"
        )


@pytest.mark.parametrize(
    ("owner_name", "adapter_method", "adapter_args", "owner_args"),
    [
        (
            "create_binance_user_stream_listen_key",
            "create_user_stream_listen_key",
            (),
            (),
        ),
        (
            "keepalive_binance_user_stream",
            "keepalive_user_stream",
            ("listen-key",),
            ("listen-key",),
        ),
    ],
)
def test_live_adapter_preserves_owner_exception_identity(
    monkeypatch,
    owner_name: str,
    adapter_method: str,
    adapter_args: tuple[str, ...],
    owner_args: tuple[str, ...],
) -> None:
    client = object()
    sentinel = RuntimeError("owner sentinel")
    owner = MagicMock(side_effect=sentinel)
    monkeypatch.setattr(live_binance_module, owner_name, owner)

    with pytest.raises(RuntimeError) as exc_info:
        getattr(_adapter(client), adapter_method)(*adapter_args)

    assert exc_info.value is sentinel
    owner.assert_called_once_with("binance", client, *owner_args)


@pytest.mark.parametrize(
    ("exchange_id", "method_present"),
    [("binance", False), ("bybit", False), ("bybit", True)],
)
def test_create_rejects_unsupported_boundary_before_provider_call(
    exchange_id: str,
    method_present: bool,
) -> None:
    create, _ = _owner_functions()
    provider = MagicMock(return_value={"listenKey": "listen-key"})
    client = SimpleNamespace()
    if method_present:
        client.fapiPrivatePostListenKey = provider

    with pytest.raises(
        ExchangeUserStreamUnsupported,
        match=rf"^user_stream_listen_key_unsupported: exchange={exchange_id}$",
    ):
        create(exchange_id, client)

    provider.assert_not_called()


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"listenKey": "listen-key"}, "listen-key"),
        ({"listenKey": 123}, "123"),
    ],
)
def test_create_returns_existing_string_projection(response, expected: str) -> None:
    create, _ = _owner_functions()
    provider = MagicMock(return_value=response)

    assert (
        create("binance", SimpleNamespace(fapiPrivatePostListenKey=provider))
        == expected
    )
    provider.assert_called_once_with()


@pytest.mark.parametrize(
    "response",
    [{}, {"listenKey": ""}, {"listenKey": None}, "listen-key"],
)
def test_create_rejects_missing_or_malformed_listen_key(response) -> None:
    create, _ = _owner_functions()
    provider = MagicMock(return_value=response)

    with pytest.raises(ExchangeError, match="^user_stream_listen_key_missing$"):
        create("binance", SimpleNamespace(fapiPrivatePostListenKey=provider))

    provider.assert_called_once_with()


@pytest.mark.parametrize(
    ("provider_error", "expected_message"),
    [
        (
            ccxt.ExchangeError("provider unavailable"),
            "user_stream_listen_key_create_failed: provider unavailable",
        ),
        (RuntimeError("generic sentinel"), None),
    ],
)
def test_create_preserves_provider_error_contract(
    provider_error: Exception,
    expected_message: str | None,
) -> None:
    create, _ = _owner_functions()
    provider = MagicMock(side_effect=provider_error)

    if expected_message is None:
        with pytest.raises(RuntimeError) as exc_info:
            create("binance", SimpleNamespace(fapiPrivatePostListenKey=provider))
        assert exc_info.value is provider_error
    else:
        with pytest.raises(ExchangeError, match=f"^{expected_message}$") as exc_info:
            create("binance", SimpleNamespace(fapiPrivatePostListenKey=provider))
        assert exc_info.value.__cause__ is provider_error
    provider.assert_called_once_with()


@pytest.mark.parametrize("exchange_id", ["binance", "bybit"])
@pytest.mark.parametrize("method_present", [False, True])
@pytest.mark.parametrize("listen_key", ["", "listen-key"])
def test_keepalive_validation_precedence_matrix(
    exchange_id: str,
    method_present: bool,
    listen_key: str,
) -> None:
    _, keepalive = _owner_functions()
    provider = MagicMock()
    client = SimpleNamespace()
    if method_present:
        client.fapiPrivatePutListenKey = provider

    if exchange_id != "binance" or not method_present:
        with pytest.raises(
            ExchangeUserStreamUnsupported,
            match=rf"^user_stream_keepalive_unsupported: exchange={exchange_id}$",
        ):
            keepalive(exchange_id, client, listen_key)
        provider.assert_not_called()
    elif not listen_key:
        with pytest.raises(
            ExchangeError,
            match="^user_stream_keepalive_requires_listen_key$",
        ):
            keepalive(exchange_id, client, listen_key)
        provider.assert_not_called()
    else:
        assert keepalive(exchange_id, client, listen_key) is None
        provider.assert_called_once_with()


@pytest.mark.parametrize(
    ("provider_error", "expected_message"),
    [
        (
            ccxt.ExchangeError("provider unavailable"),
            "user_stream_keepalive_failed: provider unavailable",
        ),
        (RuntimeError("generic sentinel"), None),
    ],
)
def test_keepalive_preserves_provider_error_contract(
    provider_error: Exception,
    expected_message: str | None,
) -> None:
    _, keepalive = _owner_functions()
    provider = MagicMock(side_effect=provider_error)
    client = SimpleNamespace(fapiPrivatePutListenKey=provider)

    if expected_message is None:
        with pytest.raises(RuntimeError) as exc_info:
            keepalive("binance", client, "listen-key")
        assert exc_info.value is provider_error
    else:
        with pytest.raises(ExchangeError, match=f"^{expected_message}$") as exc_info:
            keepalive("binance", client, "listen-key")
        assert exc_info.value.__cause__ is provider_error
    provider.assert_called_once_with()


def test_owner_and_live_adapter_keep_the_dependency_boundary() -> None:
    from src.core.adapters import binance_user_stream

    owner_source = inspect.getsource(binance_user_stream)
    owner_imports = {
        node.module if isinstance(node, ast.ImportFrom) else alias.name
        for node in ast.walk(ast.parse(owner_source))
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    shared_create = inspect.getsource(CcxtExchangeAdapter.create_user_stream_listen_key)
    shared_keepalive = inspect.getsource(CcxtExchangeAdapter.keepalive_user_stream)

    assert owner_imports == {
        "collections.abc",
        "decimal",
        "json",
        "time",
        "typing",
        "ccxt",
        "websockets.sync.client",
        "src.core.interfaces.exchange",
    }
    assert "fapiPrivatePostListenKey" not in shared_create
    assert "fapiPrivatePutListenKey" not in shared_keepalive
    assert "listenKey" not in shared_create + shared_keepalive
