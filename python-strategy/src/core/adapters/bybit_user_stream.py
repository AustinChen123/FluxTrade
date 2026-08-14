"""Bybit V5 private order and execution WebSocket protocol owner."""

from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Context, Decimal, InvalidOperation, ROUND_HALF_EVEN, localcontext
from fractions import Fraction
import hashlib
import hmac
import json
import time
from typing import Protocol

from websockets.sync.client import connect

from src.core.decimal_math import exact_decimal_add
from src.core.interfaces.exchange import ExchangeError, ExchangeOrderEvent, NetworkError
from src.core.product_registry import to_ccxt_symbol

_PRODUCTION_STREAM_URL = "wss://stream.bybit.com/v5/private"
_TESTNET_STREAM_URL = "wss://stream-testnet.bybit.com/v5/private"
_HEARTBEAT_SECONDS = 20.0
_GAP_SECONDS = 5.0

_ORDINARY_ORDER_STATUS = {
    "New": "NEW",
    "Untriggered": "NEW",
    "Triggered": "NEW",
}
_TERMINAL_ORDER_STATUS = {
    "Cancelled": "CANCELLED",
    "Rejected": "REJECTED",
    "Deactivated": "REJECTED",
}
_GAP_ORDER_STATUS = {
    "PartiallyFilled": "PARTIALLY_FILLED",
    "Filled": "FILLED",
    **_TERMINAL_ORDER_STATUS,
}


class _Connection(Protocol):
    def send(self, message: str) -> None: ...

    def recv(self, timeout: float | None = None) -> str | bytes: ...

    def close(self) -> None: ...


@dataclass(frozen=True)
class _TradeRow:
    key: tuple[str, str, str]
    product_id: str
    order_quantity: Decimal
    leaves_quantity: Decimal
    execution_id: str
    quantity: Decimal
    price: Decimal
    fee: Decimal
    fee_asset: str
    timestamp: int


@dataclass(frozen=True)
class _PendingStatus:
    key: tuple[str, str, str]
    product_id: str
    target: Decimal
    status: str
    timestamp: int
    deadline: float


class BybitOrderEventStream:
    """Own one Bybit linear private order/execution session."""

    def __init__(
        self,
        *,
        api_key: str | None,
        secret: str | None,
        testnet: bool,
        resolve_client_order_id: Callable[[str], str],
        connect: Callable[..., _Connection] = connect,
        monotonic: Callable[[], float] = time.monotonic,
        clock_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self._api_key = api_key
        self._secret = secret
        self._testnet = testnet
        self._resolve_client_order_id = resolve_client_order_id
        self._connect = connect
        self._monotonic = monotonic
        self._clock_ms = clock_ms
        self._connection: _Connection | None = None
        self._events: deque[ExchangeOrderEvent] = deque()
        self._emitted_cumulative: dict[tuple[str, str, str], Decimal] = {}
        self._pending_statuses: dict[tuple[str, str, str], _PendingStatus] = {}
        self._terminal_controls: set[tuple[tuple[str, str, str], str, Decimal]] = set()
        self._next_heartbeat_at = 0.0

    def start(self) -> None:
        if self._connection is not None:
            self.close()
        api_key = self._api_key
        secret = self._secret
        if (
            type(api_key) is not str
            or not api_key
            or type(secret) is not str
            or not secret
        ):
            raise NetworkError("bybit_order_event_stream_start_failed")

        connection: _Connection | None = None
        try:
            expiry = self._clock_ms() + 10_000
            signature = hmac.new(
                secret.encode(),
                f"GET/realtime{expiry}".encode(),
                hashlib.sha256,
            ).hexdigest()
            url = _TESTNET_STREAM_URL if self._testnet else _PRODUCTION_STREAM_URL
            connection = self._connect(url, open_timeout=10, close_timeout=10)
            _send_json(
                connection,
                {"op": "auth", "args": [api_key, expiry, signature]},
            )
            _require_success_control(connection.recv(timeout=10), "auth")
            _send_json(
                connection,
                {"op": "subscribe", "args": ["order.linear", "execution.linear"]},
            )
            _require_success_control(connection.recv(timeout=10), "subscribe")
        except Exception:
            if connection is not None:
                _close_connection(connection)
            self._clear_session_state()
            raise NetworkError("bybit_order_event_stream_start_failed") from None

        self._connection = connection
        self._next_heartbeat_at = self._monotonic() + _HEARTBEAT_SECONDS

    def poll(self) -> ExchangeOrderEvent | None:
        connection = self._connection
        if connection is None:
            raise NetworkError("bybit_order_event_stream_not_started")
        if self._events:
            return self._events.popleft()
        self._expire_pending_statuses()
        if self._events:
            return self._events.popleft()
        if self._monotonic() >= self._next_heartbeat_at:
            try:
                _send_json(connection, {"op": "ping"})
                self._wait_for_pong(connection)
            except Exception:
                self.close()
                raise NetworkError(
                    "bybit_order_event_stream_heartbeat_failed"
                ) from None
            self._next_heartbeat_at = self._monotonic() + _HEARTBEAT_SECONDS
            if self._events:
                return self._events.popleft()
        try:
            message = connection.recv(timeout=0.1)
        except TimeoutError:
            self._expire_pending_statuses()
            return self._events.popleft() if self._events else None
        except Exception:
            self.close()
            raise NetworkError("bybit_order_event_stream_receive_failed") from None
        try:
            self._project_message(message)
        except (ExchangeError, NetworkError):
            self.close()
            raise
        return self._events.popleft() if self._events else None

    def _wait_for_pong(self, connection: _Connection) -> None:
        deadline = self._monotonic() + 10.0
        while True:
            remaining = deadline - self._monotonic()
            if remaining <= 0:
                raise NetworkError("bybit_order_event_control_invalid")
            message = connection.recv(timeout=remaining)
            payload = _json_object(message, "bybit_order_event_control_invalid")
            if _is_pong_control(payload):
                return
            self._project_message(message)
            self._expire_pending_statuses()

    def close(self) -> bool:
        connection, self._connection = self._connection, None
        self._clear_session_state()
        return True if connection is None else _close_connection(connection)

    def _clear_session_state(self) -> None:
        self._events.clear()
        self._emitted_cumulative.clear()
        self._pending_statuses.clear()
        self._terminal_controls.clear()
        self._next_heartbeat_at = 0.0

    def _project_message(self, message: str | bytes) -> None:
        payload = _json_object(message, "bybit_order_event_payload_invalid")
        if _is_pong_control(payload):
            return
        topic = payload.get("topic")
        if type(topic) is not str:
            if "op" in payload:
                raise NetworkError("bybit_order_event_control_invalid")
            raise ExchangeError("bybit_order_event_payload_invalid")
        data = payload.get("data")
        if type(data) is not list:
            raise ExchangeError("bybit_order_event_payload_invalid")
        if topic == "execution.linear":
            self._project_execution_rows(data)
        elif topic == "order.linear":
            self._project_order_rows(data)

    def _project_execution_rows(self, values: list[object]) -> None:
        parsed: list[_TradeRow] = []
        seen_execution_ids: set[str] = set()
        for value in values:
            if type(value) is not dict:
                raise ExchangeError("bybit_order_event_payload_invalid")
            try:
                _require_linear(value)
            except ValueError:
                raise ExchangeError("bybit_order_event_payload_invalid") from None
            execution_type = value.get("execType")
            if execution_type == "Funding":
                continue
            if execution_type != "Trade":
                if type(execution_type) is str:
                    raise ExchangeError("bybit_order_event_external_position_mutation")
                raise ExchangeError("bybit_order_event_payload_invalid")
            extra_fees = value.get("extraFees", object())
            if not (
                type(extra_fees) is str
                and extra_fees == ""
                or type(extra_fees) is list
                and not extra_fees
            ):
                raise ExchangeError("bybit_order_event_additional_fee_unsupported")
            try:
                row = _trade_row(value)
            except (InvalidOperation, ValueError):
                raise ExchangeError("bybit_order_event_payload_invalid") from None
            if row.execution_id in seen_execution_ids:
                raise ExchangeError("bybit_order_event_payload_invalid")
            seen_execution_ids.add(row.execution_id)
            parsed.append(row)

        groups: dict[tuple[str, str, str], list[_TradeRow]] = {}
        for row in parsed:
            groups.setdefault(row.key, []).append(row)
        projected: list[tuple[tuple[str, str, str], ExchangeOrderEvent, Decimal]] = []
        for key, rows in groups.items():
            first = rows[0]
            if any(
                row.order_quantity != first.order_quantity
                or row.fee_asset != first.fee_asset
                or row.product_id != first.product_id
                for row in rows[1:]
            ):
                raise ExchangeError("bybit_order_event_payload_invalid")
            batch_quantity = Decimal(0)
            batch_fee = Decimal(0)
            weighted = Fraction(0)
            final_cumulative = Decimal(0)
            timestamp = 0
            for row in rows:
                batch_quantity = exact_decimal_add(batch_quantity, row.quantity)
                batch_fee = exact_decimal_add(batch_fee, row.fee)
                weighted += Fraction(row.quantity) * Fraction(row.price)
                cumulative = _exact_subtract(row.order_quantity, row.leaves_quantity)
                final_cumulative = max(final_cumulative, cumulative)
                timestamp = max(timestamp, row.timestamp)
            if batch_quantity <= 0 or batch_quantity > final_cumulative:
                raise ExchangeError("bybit_order_event_payload_invalid")
            previous = self._emitted_cumulative.get(key, Decimal(0))
            if final_cumulative <= previous:
                continue
            event = ExchangeOrderEvent(
                status=(
                    "FILLED"
                    if final_cumulative == first.order_quantity
                    else "PARTIALLY_FILLED"
                ),
                product_id=first.product_id,
                client_order_id=self._resolve_client_order_id(key[1]),
                exchange_order_id=key[0],
                cumulative_filled_quantity=final_cumulative,
                cumulative_average_price=None,
                last_fill_quantity=batch_quantity,
                last_fill_price=_fraction_to_decimal(
                    weighted / Fraction(batch_quantity)
                ),
                fee=batch_fee,
                fee_asset=first.fee_asset,
                event_timestamp=timestamp,
                raw=None,
            )
            projected.append((key, event, final_cumulative))

        for key, event, final_cumulative in projected:
            self._events.append(event)
            self._emitted_cumulative[key] = final_cumulative
            pending = self._pending_statuses.get(key)
            if pending is not None and pending.target <= final_cumulative:
                del self._pending_statuses[key]
                if pending.status in _TERMINAL_ORDER_STATUS.values():
                    self._queue_terminal_status(pending)

    def _project_order_rows(self, values: list[object]) -> None:
        projected: list[tuple[str, _PendingStatus]] = []
        for value in values:
            if type(value) is not dict:
                raise ExchangeError("bybit_order_event_payload_invalid")
            try:
                _require_linear(value)
                symbol = _required_string(value, "symbol")
                product_id = _product_id(symbol)
                order_id = _required_string(value, "orderId")
                order_link_id = _required_string(value, "orderLinkId")
                provider_status = _required_string(value, "orderStatus")
                cumulative = _required_decimal(value, "cumExecQty")
                timestamp = _required_uint(value, "updatedTime")
                if cumulative < 0:
                    raise ValueError("cumExecQty")
                status = _ORDINARY_ORDER_STATUS.get(
                    provider_status
                ) or _GAP_ORDER_STATUS.get(provider_status)
                if status is None:
                    raise ValueError("orderStatus")
            except (InvalidOperation, ValueError):
                raise ExchangeError("bybit_order_event_payload_invalid") from None
            key = (order_id, order_link_id, symbol)
            pending = _PendingStatus(
                key=key,
                product_id=product_id,
                target=cumulative,
                status=status,
                timestamp=timestamp,
                deadline=self._monotonic() + _GAP_SECONDS,
            )
            projected.append((provider_status, pending))

        for provider_status, pending in projected:
            emitted = self._emitted_cumulative.get(pending.key, Decimal(0))
            if provider_status in _ORDINARY_ORDER_STATUS:
                self._events.append(self._status_event(pending))
            elif pending.target > emitted:
                existing = self._pending_statuses.get(pending.key)
                if existing is not None:
                    pending = _PendingStatus(
                        key=pending.key,
                        product_id=pending.product_id,
                        target=max(existing.target, pending.target),
                        status=pending.status,
                        timestamp=max(existing.timestamp, pending.timestamp),
                        deadline=min(existing.deadline, pending.deadline),
                    )
                self._pending_statuses[pending.key] = pending
            elif provider_status in _TERMINAL_ORDER_STATUS:
                self._queue_terminal_status(pending)

    def _expire_pending_statuses(self) -> None:
        now = self._monotonic()
        expired = [
            pending
            for pending in self._pending_statuses.values()
            if now >= pending.deadline
        ]
        for pending in expired:
            self._pending_statuses.pop(pending.key, None)
            if pending.status in _TERMINAL_ORDER_STATUS.values():
                self._queue_terminal_status(pending)
            else:
                self._events.append(self._status_event(pending))

    def _queue_terminal_status(self, pending: _PendingStatus) -> None:
        identity = (pending.key, pending.status, pending.target)
        if identity in self._terminal_controls:
            return
        self._terminal_controls.add(identity)
        self._events.append(self._status_event(pending))

    def _status_event(self, pending: _PendingStatus) -> ExchangeOrderEvent:
        return ExchangeOrderEvent(
            status=pending.status,
            product_id=pending.product_id,
            client_order_id=self._resolve_client_order_id(pending.key[1]),
            exchange_order_id=pending.key[0],
            cumulative_filled_quantity=pending.target,
            cumulative_average_price=None,
            last_fill_quantity=None,
            last_fill_price=None,
            fee=None,
            fee_asset=None,
            event_timestamp=pending.timestamp,
            raw=None,
        )


def _trade_row(values: dict[str, object]) -> _TradeRow:
    _require_linear(values)
    symbol = _required_string(values, "symbol")
    order_id = _required_string(values, "orderId")
    order_link_id = _required_string(values, "orderLinkId")
    order_quantity = _required_decimal(values, "orderQty")
    leaves_quantity = _required_decimal(values, "leavesQty")
    quantity = _required_decimal(values, "execQty")
    price = _required_decimal(values, "execPrice")
    fee = _required_decimal(values, "execFee")
    if (
        order_quantity <= 0
        or leaves_quantity < 0
        or leaves_quantity > order_quantity
        or quantity <= 0
        or price <= 0
    ):
        raise ValueError("quantity")
    return _TradeRow(
        key=(order_id, order_link_id, symbol),
        product_id=_product_id(symbol),
        order_quantity=order_quantity,
        leaves_quantity=leaves_quantity,
        execution_id=_required_string(values, "execId"),
        quantity=quantity,
        price=price,
        fee=fee,
        fee_asset=_required_string(values, "feeCurrency"),
        timestamp=_required_uint(values, "execTime"),
    )


def _require_linear(values: dict[str, object]) -> None:
    category = values.get("category")
    if type(category) is not str or category != "linear":
        raise ValueError("category")


def _required_string(values: dict[str, object], key: str) -> str:
    value = values.get(key)
    if type(value) is not str or not value:
        raise ValueError(key)
    return value


def _required_uint(values: dict[str, object], key: str) -> int:
    value = values.get(key)
    if type(value) is str and value.isdigit():
        return int(value)
    if type(value) is int and value >= 0:
        return value
    raise ValueError(key)


def _required_decimal(values: dict[str, object], key: str) -> Decimal:
    value = values.get(key)
    if type(value) is not str or not value:
        raise ValueError(key)
    parsed = Decimal(value)
    if not parsed.is_finite():
        raise ValueError(key)
    return parsed


def _product_id(symbol: str) -> str:
    if not symbol.isascii() or not symbol.isalnum() or symbol != symbol.upper():
        raise ValueError("symbol")
    product_id = f"BYBIT:{symbol}-PERP"
    to_ccxt_symbol(product_id)
    return product_id


def _exact_subtract(left: Decimal, right: Decimal) -> Decimal:
    return exact_decimal_add(left, right.copy_negate())


def _fraction_to_decimal(value: Fraction) -> Decimal:
    numerator = value.numerator
    denominator = value.denominator
    twos = 0
    fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator == 1:
        scale = max(twos, fives)
        coefficient = abs(numerator) * 2 ** (scale - twos) * 5 ** (scale - fives)
        if coefficient == 0:
            return Decimal(0)
        return Decimal((int(numerator < 0), tuple(map(int, str(coefficient))), -scale))
    with localcontext(Context(prec=28, rounding=ROUND_HALF_EVEN)):
        return Decimal(value.numerator) / Decimal(value.denominator)


def _json_object(message: str | bytes, error_code: str) -> dict[str, object]:
    try:
        payload = json.loads(message)
    except (TypeError, ValueError):
        raise ExchangeError(error_code) from None
    if type(payload) is not dict:
        raise ExchangeError(error_code)
    return payload


def _send_json(connection: _Connection, payload: dict[str, object]) -> None:
    connection.send(json.dumps(payload, separators=(",", ":")))


def _require_success_control(message: str | bytes, operation: str) -> None:
    payload = _json_object(message, "bybit_order_event_control_invalid")
    if payload.get("op") != operation or payload.get("success") is not True:
        raise NetworkError("bybit_order_event_control_invalid")
    allowed = {"op", "success", "ret_msg", "conn_id", "req_id"}
    if not set(payload).issubset(allowed):
        raise NetworkError("bybit_order_event_control_invalid")
    for optional in ("ret_msg", "conn_id", "req_id"):
        value = payload.get(optional)
        if value is not None and type(value) is not str:
            raise NetworkError("bybit_order_event_control_invalid")


def _is_pong_control(payload: dict[str, object]) -> bool:
    return (
        payload.get("op") == "ping"
        and payload.get("success") is True
        and payload.get("ret_msg") == "pong"
        and type(payload.get("conn_id")) is str
        and bool(payload.get("conn_id"))
        and set(payload).issubset({"op", "success", "ret_msg", "conn_id"})
    )


def _require_pong_control(message: str | bytes) -> None:
    payload = _json_object(message, "bybit_order_event_control_invalid")
    if not _is_pong_control(payload):
        raise NetworkError("bybit_order_event_control_invalid")


def _close_connection(connection: _Connection) -> bool:
    try:
        connection.close()
    except Exception:
        return False
    return True
