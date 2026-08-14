"""Causal tests for the Bybit private order-event protocol owner."""

from collections import deque
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, localcontext
import hashlib
import hmac
import json
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core.adapters.bybit_user_stream import BybitOrderEventStream
from src.core.generic_order_event_stream import GenericOrderEventStream
from src.core.interfaces.exchange import ExchangeError, NetworkError
from src.core.order_event_sync import OrderEventApplier


class _Connection:
    def __init__(self, messages: list[object]) -> None:
        self.messages = deque(messages)
        self.sent: list[str] = []
        self.closed = False

    def send(self, message: str) -> None:
        self.sent.append(message)

    def recv(self, timeout: float | None = None) -> str:
        del timeout
        if not self.messages:
            raise TimeoutError
        value = self.messages.popleft()
        if isinstance(value, BaseException):
            raise value
        assert isinstance(value, str)
        return value

    def close(self) -> None:
        self.closed = True


def _control(op: str, success: bool = True) -> str:
    return json.dumps({"op": op, "success": success})


def _trade(
    *,
    order_id: str = "provider-order-1",
    order_link_id: str = "strategy-market-LONG-123",
    exec_id: str = "execution-1",
    exec_qty: str = "1",
    exec_price: str = "101",
    exec_fee: str = "0.01",
    order_qty: str = "3",
    leaves_qty: str = "2",
    exec_time: str = "1800000000001",
    extra_fees: object = "",
    exec_type: str = "Trade",
) -> dict[str, object]:
    return {
        "category": "linear",
        "symbol": "BTCUSDT",
        "orderId": order_id,
        "orderLinkId": order_link_id,
        "execId": exec_id,
        "execType": exec_type,
        "execQty": exec_qty,
        "execPrice": exec_price,
        "execFee": exec_fee,
        "feeCurrency": "USDT",
        "orderQty": order_qty,
        "leavesQty": leaves_qty,
        "execTime": exec_time,
        "extraFees": extra_fees,
    }


def _execution_message(*rows: dict[str, object]) -> str:
    return json.dumps({"topic": "execution.linear", "data": list(rows)})


def _order_message(
    *,
    order_status: str,
    cumulative: str,
    order_id: str = "provider-order-1",
    order_link_id: str = "strategy-market-LONG-123",
    updated_time: str = "1800000000005",
) -> str:
    return json.dumps(
        {
            "topic": "order.linear",
            "data": [
                {
                    "category": "linear",
                    "symbol": "BTCUSDT",
                    "orderId": order_id,
                    "orderLinkId": order_link_id,
                    "orderStatus": order_status,
                    "cumExecQty": cumulative,
                    "updatedTime": updated_time,
                }
            ],
        }
    )


def _stream(
    connection: _Connection,
    *,
    monotonic=lambda: 0.0,
) -> BybitOrderEventStream:
    return BybitOrderEventStream(
        api_key="api-key-sentinel",
        secret="secret-sentinel",
        testnet=False,
        resolve_client_order_id=lambda value: f"canonical:{value}",
        connect=lambda *_args, **_kwargs: connection,
        monotonic=monotonic,
        clock_ms=lambda: 1_800_000_000_000,
    )


def test_start_authenticates_then_subscribes_with_exact_frames() -> None:
    connection = _Connection([_control("auth"), _control("subscribe")])
    stream = _stream(connection)

    stream.start()

    expiry = 1_800_000_010_000
    signature = hmac.new(
        b"secret-sentinel",
        f"GET/realtime{expiry}".encode(),
        hashlib.sha256,
    ).hexdigest()
    assert [json.loads(frame) for frame in connection.sent] == [
        {"op": "auth", "args": ["api-key-sentinel", expiry, signature]},
        {"op": "subscribe", "args": ["order.linear", "execution.linear"]},
    ]


def test_same_message_rows_aggregate_by_order_with_context_independent_values() -> None:
    results = []
    for precision, rounding in ((6, ROUND_DOWN), (60, ROUND_HALF_UP)):
        connection = _Connection(
            [
                _control("auth"),
                _control("subscribe"),
                _execution_message(
                    _trade(
                        exec_id="execution-2",
                        exec_qty="2",
                        exec_price="103",
                        exec_fee="0.02",
                        leaves_qty="0",
                        exec_time="1800000000002",
                        extra_fees=[],
                    ),
                    _trade(),
                ),
            ]
        )
        stream = _stream(connection)
        stream.start()
        with localcontext() as context:
            context.prec = precision
            context.rounding = rounding
            event = stream.poll()
        assert event is not None
        results.append(event)

    first = results[0]
    assert first.status == "FILLED"
    assert first.product_id == "BYBIT:BTCUSDT-PERP"
    assert first.client_order_id == "canonical:strategy-market-LONG-123"
    assert first.exchange_order_id == "provider-order-1"
    assert first.cumulative_filled_quantity == Decimal("3")
    assert first.last_fill_quantity == Decimal("3")
    assert first.last_fill_price == Decimal("102.3333333333333333333333333")
    assert first.fee == Decimal("0.03")
    assert first.fee_asset == "USDT"
    assert first.event_timestamp == 1_800_000_000_002
    assert first.raw is None
    assert stream.poll() is None
    assert results[0] == results[1]


def test_funding_is_ignored_and_position_mutation_fails_closed() -> None:
    funding = _trade(exec_type="Funding")
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _execution_message(funding),
            _execution_message(_trade(exec_type="AdlTrade")),
        ]
    )
    stream = _stream(connection)
    stream.start()

    assert stream.poll() is None
    with pytest.raises(
        ExchangeError, match="^bybit_order_event_external_position_mutation$"
    ):
        stream.poll()


def test_expired_status_gap_emits_one_cumulative_only_probe() -> None:
    now = [0.0]
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _order_message(order_status="Cancelled", cumulative="1"),
        ]
    )
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()

    assert stream.poll() is None
    now[0] = 4.999
    assert stream.poll() is None
    now[0] = 5.0
    probe = stream.poll()
    assert probe is not None
    assert probe.status == "CANCELLED"
    assert probe.cumulative_filled_quantity == Decimal("1")
    assert probe.cumulative_average_price is None
    assert probe.last_fill_quantity is None
    assert probe.last_fill_price is None
    assert probe.fee is None
    assert probe.fee_asset is None
    assert probe.raw is None
    assert stream.poll() is None


def test_start_failure_is_fixed_and_closes_partial_connection() -> None:
    connection = _Connection([_control("auth", success=False)])
    stream = _stream(connection)

    with pytest.raises(NetworkError, match="^bybit_order_event_stream_start_failed$"):
        stream.start()

    assert connection.closed is True
    assert "secret-sentinel" not in repr(connection.sent)


@pytest.mark.parametrize(
    ("testnet", "expected_url"),
    [
        (False, "wss://stream.bybit.com/v5/private"),
        (True, "wss://stream-testnet.bybit.com/v5/private"),
    ],
)
def test_start_uses_exact_private_endpoint(testnet: bool, expected_url: str) -> None:
    connection = _Connection([_control("auth"), _control("subscribe")])
    connector = MagicMock(return_value=connection)
    stream = BybitOrderEventStream(
        api_key="api-key-sentinel",
        secret="secret-sentinel",
        testnet=testnet,
        resolve_client_order_id=lambda value: value,
        connect=connector,
        monotonic=lambda: 0.0,
        clock_ms=lambda: 1_800_000_000_000,
    )

    stream.start()

    connector.assert_called_once_with(expected_url, open_timeout=10, close_timeout=10)


@pytest.mark.parametrize("extra_fees", ["", []])
def test_both_official_empty_extra_fee_forms_are_accepted(extra_fees: object) -> None:
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _execution_message(
                _trade(order_qty="1", leaves_qty="0", extra_fees=extra_fees)
            ),
        ]
    )
    stream = _stream(connection)
    stream.start()

    event = stream.poll()
    assert event is not None
    assert event.fee == Decimal("0.01")


@pytest.mark.parametrize("extra_fees", [None, ["fee"], "fee", {}, 0])
def test_nonempty_or_malformed_extra_fees_fail_closed(extra_fees: object) -> None:
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _execution_message(_trade(extra_fees=extra_fees)),
        ]
    )
    stream = _stream(connection)
    stream.start()

    with pytest.raises(
        ExchangeError,
        match="^bybit_order_event_additional_fee_unsupported$",
    ):
        stream.poll()


def test_terminating_weighted_price_preserves_all_exact_digits() -> None:
    results = []
    for precision, rounding in ((6, ROUND_DOWN), (60, ROUND_HALF_UP)):
        connection = _Connection(
            [
                _control("auth"),
                _control("subscribe"),
                _execution_message(
                    _trade(
                        exec_id="one",
                        exec_qty="1",
                        exec_price="1",
                        exec_fee="0",
                        order_qty="2",
                        leaves_qty="1",
                    ),
                    _trade(
                        exec_id="two",
                        exec_qty="1",
                        exec_price="1.000000000000000000000000001",
                        exec_fee="0",
                        order_qty="2",
                        leaves_qty="0",
                    ),
                ),
            ]
        )
        stream = _stream(connection)
        stream.start()
        with localcontext() as context:
            context.prec = precision
            context.rounding = rounding
            event = stream.poll()
        assert event is not None
        results.append(event.last_fill_price)

    assert results == [
        Decimal("1.0000000000000000000000000005"),
        Decimal("1.0000000000000000000000000005"),
    ]


def test_execution_before_deadline_emits_fill_then_terminal_without_late_probe() -> (
    None
):
    now = [0.0]
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _order_message(order_status="Cancelled", cumulative="1"),
            _execution_message(_trade(order_qty="1", leaves_qty="0")),
        ]
    )
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()

    assert stream.poll() is None
    fill = stream.poll()
    terminal = stream.poll()
    assert fill is not None and fill.status == "FILLED"
    assert terminal is not None and terminal.status == "CANCELLED"
    now[0] = 10.0
    assert stream.poll() is None


def test_later_control_cannot_extend_first_gap_deadline() -> None:
    now = [0.0]
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _order_message(order_status="PartiallyFilled", cumulative="1"),
            _order_message(order_status="Cancelled", cumulative="1"),
        ]
    )
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()

    assert stream.poll() is None
    now[0] = 4.0
    assert stream.poll() is None
    now[0] = 5.0
    probe = stream.poll()
    assert probe is not None and probe.status == "CANCELLED"


@pytest.mark.parametrize(
    ("provider_status", "expected"),
    [
        ("New", "NEW"),
        ("Untriggered", "NEW"),
        ("Triggered", "NEW"),
        ("Cancelled", "CANCELLED"),
        ("Rejected", "REJECTED"),
        ("Deactivated", "REJECTED"),
    ],
)
def test_order_status_projection_is_status_only(
    provider_status: str,
    expected: str,
) -> None:
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _order_message(order_status=provider_status, cumulative="0"),
        ]
    )
    stream = _stream(connection)
    stream.start()

    event = stream.poll()
    assert event is not None
    assert event.status == expected
    assert event.last_fill_quantity is None
    assert event.last_fill_price is None
    assert event.fee is None
    assert event.raw is None


def test_duplicate_terminal_control_is_suppressed() -> None:
    message = _order_message(order_status="Cancelled", cumulative="0")
    connection = _Connection(
        [_control("auth"), _control("subscribe"), message, message]
    )
    stream = _stream(connection)
    stream.start()

    assert stream.poll() is not None
    assert stream.poll() is None


def test_heartbeat_runs_at_threshold_and_requires_exact_pong() -> None:
    now = [0.0]
    pong = json.dumps(
        {"op": "ping", "success": True, "ret_msg": "pong", "conn_id": "one"}
    )
    connection = _Connection([_control("auth"), _control("subscribe")])
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()

    now[0] = 19.999
    assert stream.poll() is None
    connection.messages.append(pong)
    now[0] = 20.0
    assert stream.poll() is None
    assert json.loads(connection.sent[-1]) == {"op": "ping"}


def test_duplicate_start_closes_and_clears_previous_session() -> None:
    first = _Connection([_control("auth"), _control("subscribe")])
    second = _Connection([_control("auth"), _control("subscribe")])
    connections = deque([first, second])
    stream = BybitOrderEventStream(
        api_key="api-key-sentinel",
        secret="secret-sentinel",
        testnet=False,
        resolve_client_order_id=lambda value: value,
        connect=lambda *_args, **_kwargs: connections.popleft(),
        monotonic=lambda: 0.0,
        clock_ms=lambda: 1_800_000_000_000,
    )

    stream.start()
    stream.start()

    assert first.closed is True
    assert second.closed is False


def test_status_probe_uses_real_applier_for_restart_equal_and_behind_states() -> None:
    now = [0.0]
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _order_message(order_status="Cancelled", cumulative="1"),
        ]
    )
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()
    assert stream.poll() is None
    now[0] = 5.0
    probe = stream.poll()
    assert probe is not None

    def apply_with_local_quantity(quantity: Decimal) -> dict[str, object]:
        order = SimpleNamespace(
            id="local-order",
            product_id="BYBIT:BTCUSDT-PERP",
            exchange_order_id="provider-order-1",
            filled_quantity=quantity,
            filled_price=Decimal("101") if quantity else None,
            quantity=Decimal("3"),
            status="PARTIALLY_FILLED",
        )
        manager = MagicMock()
        manager.repo.get_order_by_client_order_id.return_value = order
        applier = OrderEventApplier(
            order_manager=manager,
            journal_fill=MagicMock(),
            fail_pending_conditionals_for_terminal_entry=MagicMock(),
            protective_terminal_without_fill_failure=MagicMock(return_value=None),
            write_conditional_warning=MagicMock(),
            place_pending_conditionals_for_entry=MagicMock(return_value=[]),
            protective_partial_fill_requires_resize=MagicMock(return_value=None),
            cancel_linked_conditional_for_protection_fill=MagicMock(return_value=None),
        )
        return applier.process_exchange_order_event(probe)

    assert apply_with_local_quantity(Decimal("1"))["action"] == "applied"
    assert apply_with_local_quantity(Decimal("0"))["action"] == (
        "unresolved_missing_fill_price"
    )


@pytest.mark.parametrize(
    "failure_stage",
    ["connect", "auth_send", "auth_receive", "subscribe_send", "subscribe_receive"],
)
def test_start_transport_failure_matrix_is_fixed_and_sanitized(
    failure_stage: str,
) -> None:
    connection = MagicMock()
    connection.recv.side_effect = [_control("auth"), _control("subscribe")]
    if failure_stage == "auth_send":
        connection.send.side_effect = RuntimeError("provider-secret-sentinel")
    elif failure_stage == "auth_receive":
        connection.recv.side_effect = RuntimeError("provider-secret-sentinel")
    elif failure_stage == "subscribe_send":
        connection.send.side_effect = [None, RuntimeError("provider-secret-sentinel")]
    elif failure_stage == "subscribe_receive":
        connection.recv.side_effect = [
            _control("auth"),
            RuntimeError("provider-secret-sentinel"),
        ]
    connector = MagicMock(return_value=connection)
    if failure_stage == "connect":
        connector.side_effect = RuntimeError("provider-secret-sentinel")
    stream = BybitOrderEventStream(
        api_key="api-key-sentinel",
        secret="secret-sentinel",
        testnet=False,
        resolve_client_order_id=lambda value: value,
        connect=connector,
        monotonic=lambda: 0.0,
        clock_ms=lambda: 1_800_000_000_000,
    )

    with pytest.raises(NetworkError) as raised:
        stream.start()

    assert str(raised.value) == "bybit_order_event_stream_start_failed"
    assert "provider-secret-sentinel" not in repr(raised.value)
    if failure_stage != "connect":
        connection.close.assert_called_once_with()


def test_receive_and_close_failures_are_bounded_and_sanitized() -> None:
    connection = _Connection(
        [_control("auth"), _control("subscribe"), RuntimeError("provider-sentinel")]
    )
    stream = _stream(connection)
    stream.start()

    with pytest.raises(
        NetworkError,
        match="^bybit_order_event_stream_receive_failed$",
    ):
        stream.poll()
    with pytest.raises(
        NetworkError,
        match="^bybit_order_event_stream_not_started$",
    ):
        stream.poll()
    assert stream.close() is True

    broken = MagicMock()
    broken.close.side_effect = RuntimeError("provider-sentinel")
    stream._connection = broken
    assert stream.close() is False
    assert stream.close() is True


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("category", "spot"),
        ("symbol", "btcusdt"),
        ("symbol", "ＢＴＣ"),
        ("orderId", ""),
        ("orderLinkId", ""),
        ("execId", ""),
        ("execQty", "0"),
        ("execQty", 1),
        ("execPrice", "NaN"),
        ("orderQty", "0"),
        ("leavesQty", "4"),
        ("execTime", True),
        ("feeCurrency", ""),
    ],
)
def test_ordinary_invalid_trade_fields_fail_closed(field: str, value: object) -> None:
    trade = _trade()
    trade[field] = value
    connection = _Connection(
        [_control("auth"), _control("subscribe"), _execution_message(trade)]
    )
    stream = _stream(connection)
    stream.start()

    with pytest.raises(
        ExchangeError,
        match="^bybit_order_event_payload_invalid$",
    ):
        stream.poll()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("category", "spot"),
        ("symbol", "btcusdt"),
        ("orderId", ""),
        ("orderLinkId", ""),
        ("orderStatus", "Unknown"),
        ("cumExecQty", "NaN"),
        ("updatedTime", True),
    ],
)
def test_ordinary_invalid_order_fields_fail_closed(field: str, value: object) -> None:
    payload = json.loads(_order_message(order_status="Cancelled", cumulative="0"))
    payload["data"][0][field] = value
    connection = _Connection(
        [_control("auth"), _control("subscribe"), json.dumps(payload)]
    )
    stream = _stream(connection)
    stream.start()

    with pytest.raises(
        ExchangeError,
        match="^bybit_order_event_payload_invalid$",
    ):
        stream.poll()


def test_different_order_groups_preserve_first_seen_order() -> None:
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _execution_message(
                _trade(
                    order_id="provider-b",
                    order_link_id="strategy-market-LONG-2",
                    exec_id="b",
                    order_qty="1",
                    leaves_qty="0",
                ),
                _trade(
                    order_id="provider-a",
                    order_link_id="strategy-market-LONG-1",
                    exec_id="a",
                    order_qty="1",
                    leaves_qty="0",
                ),
            ),
        ]
    )
    stream = _stream(connection)
    stream.start()

    assert stream.poll().exchange_order_id == "provider-b"
    assert stream.poll().exchange_order_id == "provider-a"


@pytest.mark.parametrize(
    ("provider_status", "expected_status"),
    [("PartiallyFilled", "PARTIALLY_FILLED"), ("Filled", "FILLED")],
)
def test_fill_status_gap_expiry_produces_status_only_probe(
    provider_status: str,
    expected_status: str,
) -> None:
    now = [0.0]
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _order_message(order_status=provider_status, cumulative="1"),
        ]
    )
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()

    assert stream.poll() is None
    now[0] = 5.0
    event = stream.poll()
    assert event is not None
    assert event.status == expected_status
    assert event.last_fill_price is None


class _ImmediateWorker:
    def __init__(self, *, target, **_kwargs) -> None:
        self._target = target

    def start(self) -> None:
        self._target()

    def is_alive(self) -> bool:
        return False

    def join(self, *, timeout: float) -> None:
        del timeout


def test_unresolved_probe_reaches_existing_generic_latch_once() -> None:
    event = object()
    adapter = MagicMock()
    adapter.poll_order_event.return_value = event
    stop = MagicMock()
    stop.is_set.return_value = False
    latch = MagicMock()
    halt = MagicMock()
    stream = GenericOrderEventStream(
        adapter_loader=lambda: adapter,
        is_running=lambda: True,
        stop_event=lambda: stop,
        assert_leadership=lambda: None,
        process_event=MagicMock(
            return_value={"action": "unresolved_missing_fill_price"}
        ),
        latch_stream_failure=latch,
        halt_submissions=halt,
        publish_worker=lambda _worker: None,
        current_worker=lambda: None,
        event_logger=MagicMock(),
        thread_factory=_ImmediateWorker,
    )

    stream.start()

    adapter.start_order_event_stream.assert_called_once_with()
    latch.assert_called_once_with()
    halt.assert_called_once_with()


@pytest.mark.parametrize("success", [False, 1, "true", None])
def test_start_rejects_truthy_or_nonboolean_success_controls(success: object) -> None:
    connection = _Connection([json.dumps({"op": "auth", "success": success})])
    stream = _stream(connection)

    with pytest.raises(
        NetworkError,
        match="^bybit_order_event_stream_start_failed$",
    ):
        stream.start()


def test_heartbeat_rejects_nonexact_pong_and_closes() -> None:
    now = [0.0]
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            json.dumps(
                {"op": "ping", "success": 1, "ret_msg": "pong", "conn_id": "one"}
            ),
        ]
    )
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()
    now[0] = 20.0

    with pytest.raises(
        NetworkError,
        match="^bybit_order_event_stream_heartbeat_failed$",
    ):
        stream.poll()
    assert connection.closed is True


@pytest.mark.parametrize(
    ("interleaved", "expected_status"),
    [
        (
            _execution_message(_trade(order_qty="1", leaves_qty="0")),
            "FILLED",
        ),
        (_order_message(order_status="New", cumulative="0"), "NEW"),
    ],
)
def test_heartbeat_dispatches_data_frames_that_arrive_before_pong(
    interleaved: str,
    expected_status: str,
) -> None:
    now = [0.0]
    pong = json.dumps(
        {"op": "ping", "success": True, "ret_msg": "pong", "conn_id": "one"}
    )
    connection = _Connection(
        [_control("auth"), _control("subscribe"), interleaved, pong]
    )
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()
    now[0] = 20.0

    event = stream.poll()

    assert event is not None and event.status == expected_status
    assert connection.closed is False
    assert json.loads(connection.sent[-1]) == {"op": "ping"}


def test_interleaved_heartbeat_frame_cannot_extend_the_original_pong_deadline() -> None:
    now = [0.0]

    class AdvancingConnection(_Connection):
        def __init__(self) -> None:
            super().__init__(
                [
                    _control("auth"),
                    _control("subscribe"),
                    _order_message(order_status="New", cumulative="0"),
                    TimeoutError(),
                ]
            )
            self.timeouts: list[float | None] = []

        def recv(self, timeout: float | None = None) -> str:
            self.timeouts.append(timeout)
            result = super().recv(timeout)
            if len(self.timeouts) == 3:
                now[0] += 6.0
            return result

    connection = AdvancingConnection()
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()
    now[0] = 20.0

    with pytest.raises(
        NetworkError,
        match="^bybit_order_event_stream_heartbeat_failed$",
    ):
        stream.poll()

    assert connection.timeouts[-2:] == [10.0, 4.0]
    assert connection.closed is True


@pytest.mark.parametrize("message", ["{", "[]", "null", "1"])
def test_malformed_envelopes_fail_closed_without_rendering(message: str) -> None:
    connection = _Connection([_control("auth"), _control("subscribe"), message])
    stream = _stream(connection)
    stream.start()

    with pytest.raises(ExchangeError) as raised:
        stream.poll()
    assert str(raised.value) == "bybit_order_event_payload_invalid"
    assert message not in str(raised.value)


def test_unrelated_valid_topic_is_ignored() -> None:
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            json.dumps({"topic": "position.linear", "data": []}),
        ]
    )
    stream = _stream(connection)
    stream.start()

    assert stream.poll() is None


@pytest.mark.parametrize(
    ("provider_status", "local_quantity", "order_quantity", "expected_action"),
    [
        ("PartiallyFilled", Decimal("1"), Decimal("3"), "applied"),
        ("Filled", Decimal("1"), Decimal("1"), "applied"),
        (
            "Cancelled",
            Decimal("2"),
            Decimal("3"),
            "unresolved_local_fill_exceeds_exchange",
        ),
    ],
)
def test_restart_probe_matrix_uses_existing_applier_classification(
    provider_status: str,
    local_quantity: Decimal,
    order_quantity: Decimal,
    expected_action: str,
) -> None:
    now = [0.0]
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _order_message(order_status=provider_status, cumulative="1"),
        ]
    )
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()
    assert stream.poll() is None
    now[0] = 5.0
    probe = stream.poll()
    assert probe is not None

    order = SimpleNamespace(
        id="local-order",
        product_id="BYBIT:BTCUSDT-PERP",
        exchange_order_id="provider-order-1",
        filled_quantity=local_quantity,
        filled_price=Decimal("101"),
        quantity=order_quantity,
        status="PARTIALLY_FILLED",
    )
    manager = MagicMock()
    manager.repo.get_order_by_client_order_id.return_value = order
    applier = OrderEventApplier(
        order_manager=manager,
        journal_fill=MagicMock(),
        fail_pending_conditionals_for_terminal_entry=MagicMock(),
        protective_terminal_without_fill_failure=MagicMock(return_value=None),
        write_conditional_warning=MagicMock(),
        place_pending_conditionals_for_entry=MagicMock(return_value=[]),
        protective_partial_fill_requires_resize=MagicMock(return_value=None),
        cancel_linked_conditional_for_protection_fill=MagicMock(return_value=None),
    )

    assert applier.process_exchange_order_event(probe)["action"] == expected_action


def test_unknown_order_probe_keeps_existing_fail_closed_result() -> None:
    now = [0.0]
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            _order_message(order_status="Cancelled", cumulative="1"),
        ]
    )
    stream = _stream(connection, monotonic=lambda: now[0])
    stream.start()
    assert stream.poll() is None
    now[0] = 5.0
    probe = stream.poll()
    manager = MagicMock()
    manager.repo.get_order_by_client_order_id.return_value = None
    manager.repo.get_order_by_exchange_order_id.return_value = None
    applier = OrderEventApplier(
        order_manager=manager,
        journal_fill=MagicMock(),
        fail_pending_conditionals_for_terminal_entry=MagicMock(),
        protective_terminal_without_fill_failure=MagicMock(return_value=None),
        write_conditional_warning=MagicMock(),
        place_pending_conditionals_for_entry=MagicMock(return_value=[]),
        protective_partial_fill_requires_resize=MagicMock(return_value=None),
        cancel_linked_conditional_for_protection_fill=MagicMock(return_value=None),
    )

    assert probe is not None
    assert applier.process_exchange_order_event(probe)["action"] == "unknown_order"


def test_real_applier_persists_two_fill_deltas_and_ignores_stream_replay() -> None:
    first_message = _execution_message(
        _trade(
            exec_id="first",
            exec_qty="1",
            exec_price="101",
            exec_fee="0.01",
            order_qty="2",
            leaves_qty="1",
        )
    )
    second_message = _execution_message(
        _trade(
            exec_id="second",
            exec_qty="1",
            exec_price="103",
            exec_fee="0.02",
            order_qty="2",
            leaves_qty="0",
        )
    )
    connection = _Connection(
        [
            _control("auth"),
            _control("subscribe"),
            first_message,
            second_message,
            second_message,
        ]
    )
    stream = _stream(connection)
    stream.start()
    order = SimpleNamespace(
        id="local-order",
        product_id="BYBIT:BTCUSDT-PERP",
        exchange_order_id="provider-order-1",
        filled_quantity=Decimal("0"),
        filled_price=None,
        quantity=Decimal("2"),
        status="NEW",
    )
    manager = MagicMock()
    manager.repo.get_order_by_client_order_id.return_value = order

    def persist_fill(
        _order,
        _price,
        _quantity,
        *,
        cumulative_filled_quantity,
        cumulative_average_price,
        **_kwargs,
    ) -> None:
        order.filled_quantity = cumulative_filled_quantity
        order.filled_price = cumulative_average_price

    manager.record_fill_delta.side_effect = persist_fill
    journal = MagicMock()
    applier = OrderEventApplier(
        order_manager=manager,
        journal_fill=journal,
        fail_pending_conditionals_for_terminal_entry=MagicMock(),
        protective_terminal_without_fill_failure=MagicMock(return_value=None),
        write_conditional_warning=MagicMock(),
        place_pending_conditionals_for_entry=MagicMock(return_value=[]),
        protective_partial_fill_requires_resize=MagicMock(return_value=None),
        cancel_linked_conditional_for_protection_fill=MagicMock(return_value=None),
    )

    first = stream.poll()
    second = stream.poll()
    assert first is not None and second is not None
    assert applier.process_exchange_order_event(first)["action"] == "applied"
    assert applier.process_exchange_order_event(second)["action"] == "applied"

    assert order.filled_quantity == Decimal("2")
    assert order.filled_price == Decimal("102")
    assert [call.args[1].fee for call in journal.call_args_list] == [
        Decimal("0.01"),
        Decimal("0.02"),
    ]
    assert stream.poll() is None
