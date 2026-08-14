"""Backpack private order-stream protocol and adapter lifecycle tests."""

from collections import deque
from decimal import Decimal
import json
from unittest.mock import MagicMock

import pytest

from src.core.adapters.backpack_user_stream import BackpackOrderEventStream
from src.core.execution import ExecutionEngine
from src.core.generic_order_event_stream import GenericOrderEventStream
from src.core.interfaces.exchange import ExchangeError, NetworkError
from src.core.models import OrderStatus


class _Connection:
    def __init__(
        self,
        messages: list[object],
        *,
        close_fails: bool = False,
        send_fails: bool = False,
    ) -> None:
        self.messages = deque(messages)
        self.sent: list[str] = []
        self.recv_timeouts: list[float | None] = []
        self.closed = False
        self.close_fails = close_fails
        self.send_fails = send_fails
        self.on_send = lambda: None
        self.on_recv = lambda: None

    def send(self, message: str) -> None:
        self.on_send()
        if self.send_fails:
            raise RuntimeError("send-sentinel")
        self.sent.append(message)

    def recv(self, timeout: float | None = None) -> str:
        self.on_recv()
        self.recv_timeouts.append(timeout)
        if not self.messages:
            raise TimeoutError
        message = self.messages.popleft()
        if isinstance(message, BaseException):
            raise message
        assert type(message) is str
        return message

    def close(self) -> None:
        self.closed = True
        if self.close_fails:
            raise RuntimeError("close-sentinel")


def _client() -> MagicMock:
    client = MagicMock()
    client.apiKey = "public-key"
    client.secret = "private-seed"
    client.milliseconds.return_value = 1_700_000_000_000
    client.base64_to_binary.return_value = b"seed"
    client.array_slice.return_value = b"seed"
    client.encode.side_effect = lambda value: value.encode()
    client.eddsa.return_value = "signature"
    client.fetch_open_orders.return_value = []
    return client


def _wrapped(data: dict[str, object]) -> str:
    return json.dumps({"stream": "account.orderUpdate", "data": data})


def _event(**overrides: object) -> dict[str, object]:
    event: dict[str, object] = {
        "e": "orderFill",
        "E": 1_700_000_000_001_000,
        "T": 1_700_000_000_000_999,
        "s": "BTC_USDC_PERP",
        "c": 17,
        "X": "PartiallyFilled",
        "i": "provider-order-1",
        "t": 81,
        "z": "0.25",
        "l": "0.25",
        "L": "101.25",
        "n": "0.0010",
        "N": "USDC",
    }
    event.update(overrides)
    return event


def _stream(
    messages: list[object],
) -> tuple[BackpackOrderEventStream, _Connection, MagicMock]:
    client = _client()
    connection = _Connection(messages)
    stream = BackpackOrderEventStream(
        client=client,
        resolve_client_order_id=lambda value: f"canonical:{value}",
        connect=MagicMock(return_value=connection),
    )
    return stream, connection, client


def test_start_preflights_signs_and_accepts_quiet_account() -> None:
    stream, connection, client = _stream([])
    unpublished_during_io: list[object | None] = []
    connection.on_send = lambda: unpublished_during_io.append(stream._connection)
    connection.on_recv = lambda: unpublished_during_io.append(stream._connection)

    stream.start()

    client.fetch_open_orders.assert_called_once_with()
    connector = stream._connect
    assert isinstance(connector, MagicMock)
    connector.assert_called_once_with(
        "wss://ws.backpack.exchange",
        open_timeout=10,
        close_timeout=10,
    )
    assert unpublished_during_io == [None, None]
    assert connection.recv_timeouts == [0.1]
    assert len(connection.sent) == 1
    assert json.loads(connection.sent[0]) == {
        "method": "SUBSCRIBE",
        "params": ["account.orderUpdate"],
        "signature": [
            "public-key",
            "signature",
            "1700000000000",
            "5000",
        ],
    }
    client.eddsa.assert_called_once_with(
        b"instruction=subscribe&timestamp=1700000000000&window=5000",
        b"seed",
        "ed25519",
    )
    assert stream.poll() is None
    assert not connection.closed


@pytest.mark.parametrize("failure_stage", ["connect", "send", "receive"])
def test_start_transport_failures_are_sanitized_and_leave_not_started(
    failure_stage: str,
) -> None:
    client = _client()
    connection = _Connection(
        [RuntimeError("receive-sentinel")] if failure_stage == "receive" else [],
        send_fails=failure_stage == "send",
    )
    connector = MagicMock(return_value=connection)
    if failure_stage == "connect":
        connector.side_effect = RuntimeError("connect-sentinel")
    stream = BackpackOrderEventStream(
        client=client,
        resolve_client_order_id=lambda value: value,
        connect=connector,
    )

    with pytest.raises(NetworkError) as raised:
        stream.start()

    assert str(raised.value) == "backpack_order_event_stream_start_failed"
    if failure_stage != "connect":
        assert connection.closed
    with pytest.raises(
        NetworkError,
        match="^backpack_order_event_stream_not_started$",
    ):
        stream.poll()


def test_preflight_failure_stops_before_connect_and_hides_source() -> None:
    client = _client()
    client.fetch_open_orders.side_effect = RuntimeError("credential-sentinel")
    connector = MagicMock()
    stream = BackpackOrderEventStream(
        client=client,
        resolve_client_order_id=lambda value: value,
        connect=connector,
    )

    with pytest.raises(
        NetworkError,
        match="^backpack_order_event_stream_preflight_failed$",
    ):
        stream.start()

    connector.assert_not_called()


def test_start_buffers_first_order_event() -> None:
    stream, connection, _client = _stream([_wrapped(_event())])

    stream.start()
    event = stream.poll()

    assert event is not None
    assert event.status == "PARTIALLY_FILLED"
    assert event.product_id == "BACKPACK:BTC_USDC-PERP"
    assert event.client_order_id == "canonical:17"
    assert event.exchange_order_id == "provider-order-1"
    assert event.cumulative_filled_quantity == Decimal("0.25")
    assert event.cumulative_average_price is None
    assert event.last_fill_quantity == Decimal("0.25")
    assert event.last_fill_price == Decimal("101.25")
    assert event.fee == Decimal("0.0010")
    assert event.fee_asset == "USDC"
    assert event.event_timestamp == 1_700_000_000_000
    assert event.raw is None
    assert not connection.closed


def test_duplicate_start_closes_previous_connection_before_replacement() -> None:
    client = _client()
    first = _Connection([])
    second = _Connection([])
    connector = MagicMock(side_effect=[first, second])
    stream = BackpackOrderEventStream(
        client=client,
        resolve_client_order_id=lambda value: value,
        connect=connector,
    )

    stream.start()
    stream.start()

    assert first.closed
    assert not second.closed
    assert connector.call_count == 2
    assert client.fetch_open_orders.call_count == 2


@pytest.mark.parametrize(
    ("provider_event", "provider_status", "expected"),
    [
        ("orderAccepted", "New", "NEW"),
        ("orderCancelled", "Cancelled", "CANCELLED"),
        ("orderExpired", "Expired", "EXPIRED"),
        ("orderFill", "PartiallyFilled", "PARTIALLY_FILLED"),
        ("orderFill", "Filled", "FILLED"),
        ("orderModified", "New", "NEW"),
        ("orderModified", "PartiallyFilled", "PARTIALLY_FILLED"),
        ("triggerPlaced", "TriggerPending", "NEW"),
        ("triggerFailed", "TriggerFailed", "REJECTED"),
    ],
)
def test_exact_event_status_matrix(
    provider_event: str,
    provider_status: str,
    expected: str,
) -> None:
    values = _event(e=provider_event, X=provider_status)
    if provider_event != "orderFill":
        values.update(t=None)
    stream, _connection, _client = _stream([_wrapped(values)])

    stream.start()
    event = stream.poll()

    assert event is not None
    assert event.status == expected
    if provider_event == "orderFill":
        assert event.last_fill_quantity == Decimal("0.25")
        assert event.fee == Decimal("0.0010")
    else:
        assert event.last_fill_quantity is None
        assert event.last_fill_price is None
        assert event.fee is None
        assert event.fee_asset is None


@pytest.mark.parametrize(
    "message",
    [
        json.dumps({"error": {"code": "UNAUTHORIZED", "message": "secret"}}),
        json.dumps({"result": "unexpected"}),
        "not-json",
    ],
)
def test_start_rejection_is_sanitized_and_closes(message: str) -> None:
    stream, connection, _client = _stream([message])

    with pytest.raises(
        NetworkError,
        match="^backpack_order_event_stream_start_failed$",
    ):
        stream.start()

    assert connection.closed


def test_delayed_rejection_after_quiet_start_is_not_ignored() -> None:
    stream, connection, _client = _stream([])
    stream.start()
    connection.messages.append(
        json.dumps({"error": {"code": "UNAUTHORIZED", "message": "secret"}})
    )

    with pytest.raises(
        NetworkError,
        match="^backpack_order_event_stream_receive_failed$",
    ):
        stream.poll()

    assert connection.closed


def test_delayed_invalid_order_event_closes_and_preserves_projection_error() -> None:
    stream, connection, _client = _stream([])
    stream.start()
    connection.messages.append(_wrapped(_event(X="unknown")))

    with pytest.raises(
        ExchangeError,
        match="^backpack_order_event_payload_invalid$",
    ):
        stream.poll()

    assert connection.closed


def test_delayed_rejection_reaches_generic_latch_and_halt() -> None:
    stream, _connection, _client = _stream(
        [
            TimeoutError(),
            json.dumps({"error": {"code": "UNAUTHORIZED", "message": "secret"}}),
        ]
    )
    events: list[str] = []
    stop_event = MagicMock()
    stop_event.is_set.return_value = False

    class ImmediateWorker:
        def __init__(self, *, target, name: str, daemon: bool) -> None:
            self.target = target
            self.name = name
            self.daemon = daemon

        def start(self) -> None:
            self.target()

        def is_alive(self) -> bool:
            return False

        def join(self, *, timeout: float) -> None:
            raise AssertionError(timeout)

    current_worker: ImmediateWorker | None = None

    def publish_worker(value: object) -> None:
        nonlocal current_worker
        assert isinstance(value, ImmediateWorker)
        current_worker = value

    def load_worker() -> ImmediateWorker | None:
        return current_worker

    worker = GenericOrderEventStream(
        adapter_loader=lambda: MagicMock(
            start_order_event_stream=stream.start,
            poll_order_event=stream.poll,
        ),
        is_running=lambda: True,
        stop_event=lambda: stop_event,
        assert_leadership=lambda: None,
        process_event=lambda _event: {"action": "applied"},
        latch_stream_failure=lambda: events.append("latch"),
        halt_submissions=lambda: events.append("halt"),
        publish_worker=publish_worker,
        current_worker=load_worker,
        event_logger=MagicMock(),
        thread_factory=ImmediateWorker,
    )

    worker.start()

    assert events == ["latch", "halt"]


def test_exact_success_control_may_be_ignored_after_start() -> None:
    stream, connection, _client = _stream([])
    stream.start()
    connection.messages.extend([json.dumps({"result": None}), _wrapped(_event())])

    assert stream.poll() is None
    assert stream.poll() is not None


def test_unrelated_stream_is_ignored() -> None:
    stream, _connection, _client = _stream([])
    stream.start()
    _connection.messages.append(
        json.dumps({"stream": "account.positionUpdate", "data": {}})
    )

    assert stream.poll() is None


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("E", True),
        ("T", True),
        ("L", "not-decimal"),
        ("s", "UNKNOWN_USDC_PERP"),
        ("z", "NaN"),
        ("l", "sNaN"),
        ("L", "Infinity"),
        ("n", "-Infinity"),
        ("z", "-0.01"),
        ("l", "-0.01"),
        ("L", "-101"),
    ],
)
def test_ordinary_invalid_event_fields_fail_closed(
    field: str,
    value: object,
) -> None:
    stream, _connection, _client = _stream([_wrapped(_event(**{field: value}))])

    with pytest.raises(ExchangeError, match="^backpack_order_event_payload_invalid$"):
        stream.start()


def test_close_is_idempotent_and_reports_cleanup_failure() -> None:
    client = _client()
    connection = _Connection([], close_fails=True)
    stream = BackpackOrderEventStream(
        client=client,
        resolve_client_order_id=lambda value: value,
        connect=MagicMock(return_value=connection),
    )
    stream.start()

    assert stream.close() is False
    assert stream.close() is True


@pytest.mark.parametrize(
    "missing",
    ["z", "l", "L", "t"],
)
def test_fill_requires_exact_cumulative_and_delta_fields(missing: str) -> None:
    values = _event()
    values.pop(missing)
    stream, _connection, _client = _stream([_wrapped(values)])

    with pytest.raises(ExchangeError, match="^backpack_order_event_payload_invalid$"):
        stream.start()


def test_fill_fee_and_asset_are_atomic() -> None:
    values = _event()
    values.pop("N")
    stream, _connection, _client = _stream([_wrapped(values)])

    with pytest.raises(ExchangeError, match="^backpack_order_event_payload_invalid$"):
        stream.start()


def test_real_order_applier_aggregates_two_fills_and_replay_is_idempotent(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
    mock_order_repo,
    order_factory,
) -> None:
    journal = MagicMock()
    engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
        journal=journal,
        is_backtest=True,
    )
    canonical_id = "strategy_1-worker_a-entry-1704067200000000000"
    order = order_factory(
        product_id="BACKPACK:BTC_USDC-PERP",
        client_order_id=canonical_id,
        exchange_order_id=None,
        status=OrderStatus.SUBMITTED.value,
        price=Decimal("100"),
        quantity=Decimal("0.10"),
    )
    mock_order_repo.add_order(order)
    partial = _event(
        c=17,
        z="0.04",
        l="0.04",
        L="101",
        n="0.0010",
        N="USDC",
    )
    final = _event(
        c=17,
        X="Filled",
        t=82,
        z="0.10",
        l="0.06",
        L="103",
        n="0.0020",
        N="USDC",
    )
    stream, connection, _client = _stream([_wrapped(partial)])
    stream._resolve_client_order_id = lambda _value: canonical_id
    stream.start()
    partial_event = stream.poll()
    connection.messages.append(_wrapped(final))
    final_event = stream.poll()
    assert partial_event is not None
    assert final_event is not None

    assert engine.process_exchange_order_event(partial_event)["action"] == "applied"
    assert engine.process_exchange_order_event(final_event)["action"] == "applied"
    assert order.status == OrderStatus.FILLED.value
    assert order.filled_quantity == Decimal("0.10")
    assert order.filled_price == Decimal("102.2")
    assert [trade.quantity for trade in mock_order_repo.trades] == [
        Decimal("0.04"),
        Decimal("0.06"),
    ]
    assert [trade.price for trade in mock_order_repo.trades] == [
        Decimal("101"),
        Decimal("103"),
    ]
    assert [trade.fee for trade in mock_order_repo.trades] == [
        Decimal("0.0010"),
        Decimal("0.0020"),
    ]
    persisted_notional = sum(
        (trade.quantity * trade.price for trade in mock_order_repo.trades),
        Decimal("0"),
    )
    assert persisted_notional / order.filled_quantity == Decimal("102.2")
    assert sum(
        (trade.fee for trade in mock_order_repo.trades),
        Decimal("0"),
    ) == Decimal("0.0030")
    assert [call.args[1]["quantity"] for call in journal.log.call_args_list] == [
        "0.04",
        "0.06",
    ]
    assert [call.args[1]["fee"] for call in journal.log.call_args_list] == [
        "0.0010",
        "0.0020",
    ]

    engine.process_exchange_order_event(partial_event)
    engine.process_exchange_order_event(final_event)
    assert len(mock_order_repo.trades) == 2


def test_alias_miss_resolves_by_persisted_exchange_order_id(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
    mock_order_repo,
    order_factory,
) -> None:
    engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
        is_backtest=True,
    )
    order = order_factory(
        product_id="BACKPACK:BTC_USDC-PERP",
        exchange_id="BACKPACK",
        client_order_id="strategy_1-worker_a-entry-1704067200000000002",
        exchange_order_id="provider-order-1",
        status=OrderStatus.SUBMITTED.value,
        price=Decimal("100"),
        quantity=Decimal("0.25"),
    )
    mock_order_repo.add_order(order)
    stream, _connection, _client = _stream([_wrapped(_event())])
    stream.start()
    event = stream.poll()
    assert event is not None

    result = engine.process_exchange_order_event(event)

    assert result["action"] == "applied"
    assert order.status == OrderStatus.PARTIALLY_FILLED.value
    assert order.filled_quantity == Decimal("0.25")


def test_missing_partial_does_not_invent_cumulative_average(
    mock_db_session,
    mock_clock,
    mock_exchange_adapter,
    mock_order_repo,
    order_factory,
) -> None:
    engine = ExecutionEngine(
        db_session=mock_db_session,
        clock=mock_clock,
        adapter=mock_exchange_adapter,
        order_repository=mock_order_repo,
        is_backtest=True,
    )
    canonical_id = "strategy_1-worker_a-entry-1704067200000000001"
    order = order_factory(
        product_id="BACKPACK:BTC_USDC-PERP",
        client_order_id=canonical_id,
        exchange_order_id=None,
        status=OrderStatus.SUBMITTED.value,
        price=Decimal("100"),
        quantity=Decimal("0.10"),
    )
    mock_order_repo.add_order(order)
    stream, _connection, _client = _stream(
        [
            _wrapped(
                _event(
                    X="Filled",
                    z="0.10",
                    l="0.06",
                    L="103",
                )
            )
        ]
    )
    stream._resolve_client_order_id = lambda _value: canonical_id
    stream.start()
    event = stream.poll()
    assert event is not None

    result = engine.process_exchange_order_event(event)

    assert result["action"] == "unresolved_missing_fill_price"
    assert order.status == OrderStatus.SUBMITTED.value
    assert mock_order_repo.trades == []
