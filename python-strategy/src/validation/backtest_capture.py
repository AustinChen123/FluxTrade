"""Pure event-time projections for backtest parity evidence."""

from decimal import Decimal, InvalidOperation

from src.core.backtest.endpoint_state import ReplayEndpointState
from src.core.models import OrderSide, Signal, SignalType
from src.validation.trading_outcome import (
    FillObservation,
    FinancialOutcome,
    JournalObservation,
    OrderObservation,
    SignalObservation,
    TradingOutcome,
)

__all__ = [
    "BacktestOutcomeCaptureError",
    "build_normal_backtest_trading_outcome",
    "capture_signal_batch",
    "exact_decimal_add",
    "exact_decimal_subtract",
]

_FIELD_NAMES = frozenset(Signal.model_fields)
_RENAMED_FIELDS = {
    "timestamp": "timestamp_ms",
    "type": "signal_type",
    "metadata": "metadata_json",
}

_FILL_KEYS = frozenset(
    {
        "id",
        "strategy_id",
        "order_id",
        "exchange_trade_id",
        "product_id",
        "side",
        "price",
        "quantity",
        "fee",
        "fee_asset",
        "timestamp",
        "fill_sequence",
    }
)
_JOURNAL_KEYS = frozenset({"strategy_id", "timestamp", "tag", "data", "trade_id"})
_ENTRY_KEYS = frozenset(
    {
        "order_id",
        "side",
        "order_type",
        "quantity",
        "price",
        "stop_loss",
        "take_profit",
        "trailing_distance",
    }
)
_JOURNAL_FILL_KEYS = frozenset(
    {"order_id", "side", "price", "quantity", "fee", "fill_type"}
)
_SIGNAL_SIDES = {
    "LONG": "buy",
    "SHORT": "sell",
    "EXIT_LONG": "sell",
    "EXIT_SHORT": "buy",
}


class BacktestOutcomeCaptureError(ValueError):
    """Safe outer failure for the bounded normal backtest projection."""

    stage = "normal_backtest_outcome_capture"


def capture_signal_batch(signals: object) -> tuple[SignalObservation, ...]:
    """Freeze one finalized signal batch at the observer call boundary."""
    if type(signals) is not tuple:
        raise ValueError("signal capture requires an exact tuple")

    captured: list[SignalObservation] = []
    for signal in signals:
        if type(signal) is not Signal:
            raise ValueError("signal capture requires exact Signal instances")
        values = dict(signal.__dict__)
        if signal.model_extra is not None or set(values) != _FIELD_NAMES:
            raise ValueError("Signal contains missing or unexpected fields")
        signal_type = values["type"]
        if type(signal_type) is not SignalType:
            raise ValueError("Signal type must be an exact SignalType")
        values["type"] = signal_type.value
        captured.append(
            SignalObservation.model_validate(
                {
                    _RENAMED_FIELDS.get(name, name): value
                    for name, value in values.items()
                }
            )
        )
    return tuple(captured)


def _row(value: object, keys: frozenset[str], label: str) -> dict[str, object]:
    if type(value) is not dict:
        raise ValueError(f"{label} must be an exact dict")
    if set(value) != keys:
        raise ValueError(f"{label} has missing or unexpected fields")
    if any(type(key) is not str for key in value):
        raise ValueError(f"{label} field names must be exact strings")
    return dict(value)


def _text(value: object, field: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if type(value) is not str or not value:
        raise ValueError(f"{field} must be a non-empty exact string")
    value.encode("utf-8")
    return value


def _integer(value: object, field: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(f"{field} must be a nonnegative exact integer")
    return value


def _money(
    value: object,
    field: str,
    *,
    allow_zero: bool,
    allow_negative: bool = False,
) -> Decimal:
    if type(value) is not Decimal or not value.is_finite():
        raise ValueError(f"{field} must be a finite exact Decimal")
    if (not allow_negative and value < 0) or (not allow_zero and value == 0):
        qualifier = "nonnegative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {qualifier}")
    return value


def _journal_money(value: object, field: str, *, allow_zero: bool) -> Decimal:
    text = _text(value, field)
    assert text is not None
    try:
        parsed = Decimal(text)
    except InvalidOperation as error:
        raise ValueError(f"{field} must contain Decimal text") from error
    if not parsed.is_finite() or parsed < 0 or (not allow_zero and parsed == 0):
        qualifier = "nonnegative" if allow_zero else "positive"
        raise ValueError(f"{field} must contain a {qualifier} finite Decimal")
    return parsed


def exact_decimal_add(left: Decimal, right: Decimal) -> Decimal:
    left_sign, left_digits, left_exponent = left.as_tuple()
    right_sign, right_digits, right_exponent = right.as_tuple()
    assert isinstance(left_exponent, int) and isinstance(right_exponent, int)
    exponent = min(left_exponent, right_exponent)
    left_coefficient = int("".join(map(str, left_digits))) * 10 ** (
        left_exponent - exponent
    )
    right_coefficient = int("".join(map(str, right_digits))) * 10 ** (
        right_exponent - exponent
    )
    total = (-left_coefficient if left_sign else left_coefficient) + (
        -right_coefficient if right_sign else right_coefficient
    )
    if total == 0:
        return Decimal(0)
    return Decimal(
        (
            int(total < 0),
            tuple(map(int, str(abs(total)))),
            exponent,
        )
    )


def exact_decimal_subtract(left: Decimal, right: Decimal) -> Decimal:
    """Subtract finite Decimals without consulting the ambient context."""
    return exact_decimal_add(left, right.copy_negate())


def _build_normal_backtest_trading_outcome(
    *,
    signals: object,
    fills: object,
    journal: object,
    endpoint_state: object,
    initial_balance: object,
    total_pnl: object,
) -> TradingOutcome:
    if type(signals) is not tuple or not signals:
        raise ValueError("signals must be one non-empty exact tuple")
    captured: list[SignalObservation] = []
    for signal in signals:
        if type(signal) is not SignalObservation:
            raise ValueError("signals must contain exact SignalObservation values")
        captured.append(SignalObservation.model_validate(signal))
    captured_signals = tuple(captured)
    strategy_id = captured_signals[0].strategy_id
    product_id = captured_signals[0].product_id
    if any(
        signal.strategy_id != strategy_id or signal.product_id != product_id
        for signal in captured_signals
    ):
        raise ValueError("signals must use one strategy and product")
    actionable_signals = tuple(
        signal for signal in captured_signals if signal.signal_type != "NO_SIGNAL"
    )
    if not actionable_signals:
        raise ValueError("normal capture requires at least one actionable signal")

    if type(fills) is not tuple or type(journal) is not tuple:
        raise ValueError("fill and journal sources must be exact tuples")
    if len(fills) != len(actionable_signals) or len(journal) != len(fills) * 2:
        raise ValueError("signal, fill and journal cardinality is inconsistent")

    if type(endpoint_state) is not ReplayEndpointState:
        raise ValueError("endpoint_state must be an exact ReplayEndpointState")
    validated_endpoint = ReplayEndpointState.model_validate(endpoint_state)
    if validated_endpoint.positions:
        raise ValueError("endpoint positions must be empty")
    if validated_endpoint.working_orders:
        raise ValueError("endpoint working_orders must be empty")
    initial = _money(initial_balance, "initial_balance", allow_zero=False)
    pnl = _money(
        total_pnl,
        "total_pnl",
        allow_zero=True,
        allow_negative=True,
    )

    order_observations: list[OrderObservation] = []
    fill_observations: list[FillObservation] = []
    journal_observations: list[JournalObservation] = []
    raw_fill_ids: set[str] = set()
    raw_order_ids: set[str] = set()
    fees = Decimal("0")

    for index, (signal, fill_source) in enumerate(
        zip(actionable_signals, fills, strict=True)
    ):
        fill = _row(fill_source, _FILL_KEYS, "persisted fill")
        fill_id = _text(fill["id"], "fill id")
        raw_order_id = _text(fill["order_id"], "fill order_id")
        fill_strategy = _text(fill["strategy_id"], "fill strategy_id")
        fill_product = _text(fill["product_id"], "fill product_id")
        fill_side = _text(fill["side"], "fill side")
        _text(fill["exchange_trade_id"], "exchange_trade_id", nullable=True)
        _text(fill["fee_asset"], "fee_asset", nullable=True)
        assert fill_id is not None and raw_order_id is not None
        assert fill_strategy is not None and fill_product is not None
        assert fill_side is not None
        if fill_id in raw_fill_ids or raw_order_id in raw_order_ids:
            raise ValueError("duplicate fill or order identity")
        raw_fill_ids.add(fill_id)
        raw_order_ids.add(raw_order_id)
        if fill_strategy != strategy_id or fill_product != product_id:
            raise ValueError("fill strategy or product does not match signals")
        if fill_side not in {"buy", "sell"}:
            raise ValueError("fill side must be buy or sell")
        sequence = _integer(fill["fill_sequence"], "fill_sequence")
        if sequence != index:
            raise ValueError("fill_sequence must be exact and contiguous")
        fill_timestamp = _integer(fill["timestamp"], "fill timestamp")
        fill_price = _money(fill["price"], "fill price", allow_zero=False)
        fill_quantity = _money(fill["quantity"], "fill quantity", allow_zero=False)
        fill_fee = _money(fill["fee"], "fill fee", allow_zero=True)

        entry = _row(journal[index * 2], _JOURNAL_KEYS, "entry journal")
        fill_journal = _row(journal[index * 2 + 1], _JOURNAL_KEYS, "fill journal")
        entry_tag = _text(entry["tag"], "entry tag")
        fill_tag = _text(fill_journal["tag"], "fill journal tag")
        if entry_tag != "entry" or fill_tag != "fill":
            raise ValueError("journal must alternate entry and fill")
        entry_strategy = _text(entry["strategy_id"], "entry strategy_id")
        journal_fill_strategy = _text(
            fill_journal["strategy_id"], "fill journal strategy_id"
        )
        if entry_strategy != strategy_id or journal_fill_strategy != strategy_id:
            raise ValueError("journal strategy does not match signals")
        entry_timestamp = _integer(entry["timestamp"], "entry timestamp")
        journal_fill_timestamp = _integer(
            fill_journal["timestamp"], "fill journal timestamp"
        )
        if entry_timestamp != signal.timestamp_ms:
            raise ValueError("entry timestamp does not match signal")
        if journal_fill_timestamp != fill_timestamp or fill_timestamp < entry_timestamp:
            raise ValueError("fill timestamp does not match journal chronology")
        entry_trade_id = _text(entry["trade_id"], "entry trade_id")
        fill_trade_id = _text(fill_journal["trade_id"], "fill trade_id")
        if entry_trade_id != raw_order_id or fill_trade_id != raw_order_id:
            raise ValueError("journal trade identity does not match fill order_id")

        entry_data = _row(entry["data"], _ENTRY_KEYS, "entry journal data")
        fill_data = _row(fill_journal["data"], _JOURNAL_FILL_KEYS, "fill journal data")
        entry_order_id = _text(entry_data["order_id"], "entry data order_id")
        journal_fill_order_id = _text(
            fill_data["order_id"], "fill journal data order_id"
        )
        if entry_order_id != raw_order_id:
            raise ValueError("entry order_id does not match fill")
        if journal_fill_order_id != raw_order_id:
            raise ValueError("fill journal order_id does not match persisted fill")
        if type(entry_data["side"]) is not OrderSide:
            raise ValueError("entry side must be an exact OrderSide")
        if type(fill_data["side"]) is not OrderSide:
            raise ValueError("fill journal side must be an exact OrderSide")
        entry_side = entry_data["side"].value
        journal_fill_side = fill_data["side"].value
        expected_side = _SIGNAL_SIDES.get(signal.signal_type)
        if expected_side is None or entry_side != expected_side:
            raise ValueError("entry side does not match signal type")
        if journal_fill_side != fill_side or entry_side != fill_side:
            raise ValueError("journal and persisted fill side do not match")
        entry_order_type = _text(entry_data["order_type"], "entry order_type")
        entry_price = _text(entry_data["price"], "entry price")
        if entry_order_type != "market" or entry_price != "market":
            raise ValueError("normal capture requires a market order")
        if any(
            entry_data[field] is not None
            for field in ("stop_loss", "take_profit", "trailing_distance")
        ):
            raise ValueError("normal capture excludes protective orders")
        journal_fill_type = _text(fill_data["fill_type"], "fill type")
        if journal_fill_type != "MARKET":
            raise ValueError("normal capture requires a market fill")
        entry_quantity = _journal_money(
            entry_data["quantity"], "entry quantity", allow_zero=False
        )
        journal_price = _journal_money(
            fill_data["price"], "fill journal price", allow_zero=False
        )
        journal_quantity = _journal_money(
            fill_data["quantity"], "fill journal quantity", allow_zero=False
        )
        journal_fee = _journal_money(
            fill_data["fee"], "fill journal fee", allow_zero=True
        )
        if signal.quantity != entry_quantity or entry_quantity != fill_quantity:
            raise ValueError("signal, entry and fill quantity do not match")
        if journal_quantity != fill_quantity:
            raise ValueError("journal and persisted fill quantity do not match")
        if journal_price != fill_price:
            raise ValueError("journal and persisted fill price do not match")
        if journal_fee != fill_fee:
            raise ValueError("journal and persisted fill fee do not match")

        logical_id = f"order-{index:06d}"
        shared_order = {
            "logical_order_id": logical_id,
            "parent_logical_order_id": None,
            "linked_logical_order_id": None,
            "strategy_id": strategy_id,
            "product_id": product_id,
            "order_type": "market",
            "side": fill_side,
            "quantity": fill_quantity,
            "trigger_price": None,
            "trailing_distance": None,
        }
        order_observations.extend(
            (
                OrderObservation(
                    **shared_order,
                    timestamp_ms=entry_timestamp,
                    phase="submitted",
                    status="PLACED",
                    filled_quantity=Decimal("0"),
                    price=None,
                ),
                OrderObservation(
                    **shared_order,
                    timestamp_ms=fill_timestamp,
                    phase="filled",
                    status="FILLED",
                    filled_quantity=fill_quantity,
                    price=fill_price,
                ),
            )
        )
        fill_observations.append(
            FillObservation(
                logical_order_id=logical_id,
                strategy_id=strategy_id,
                product_id=product_id,
                timestamp_ms=fill_timestamp,
                fill_type="MARKET",
                side=fill_side,
                price=fill_price,
                quantity=fill_quantity,
                fee=fill_fee,
            )
        )
        for timestamp_ms, tag, data in (
            (entry_timestamp, entry_tag, entry_data),
            (journal_fill_timestamp, fill_tag, fill_data),
        ):
            projected_data = dict(data)
            projected_data["order_id"] = logical_id
            projected_side = projected_data["side"]
            assert type(projected_side) is OrderSide
            projected_data["side"] = projected_side.value
            journal_observations.append(
                JournalObservation.model_validate(
                    {
                        "strategy_id": strategy_id,
                        "timestamp_ms": timestamp_ms,
                        "tag": tag,
                        "logical_trade_id": logical_id,
                        "data_json": projected_data,
                    }
                )
            )
        fees = exact_decimal_add(fees, fill_fee)

    return TradingOutcome(
        signals=captured_signals,
        order_observations=tuple(order_observations),
        fills=tuple(fill_observations),
        endpoint_state=validated_endpoint,
        financial=FinancialOutcome(
            fees=fees,
            realized_pnl=pnl,
            unrealized_pnl=Decimal("0"),
            equity=exact_decimal_add(initial, pnl),
        ),
        journal=tuple(journal_observations),
    )


def build_normal_backtest_trading_outcome(
    *,
    signals: object,
    fills: object,
    journal: object,
    endpoint_state: object,
    initial_balance: object,
    total_pnl: object,
) -> TradingOutcome:
    """Build the bounded normal-path canonical outcome or raise a safe error."""
    try:
        return _build_normal_backtest_trading_outcome(
            signals=signals,
            fills=fills,
            journal=journal,
            endpoint_state=endpoint_state,
            initial_balance=initial_balance,
            total_pnl=total_pnl,
        )
    except BacktestOutcomeCaptureError:
        raise
    except (ArithmeticError, KeyError, TypeError, ValueError) as error:
        raise BacktestOutcomeCaptureError(
            "normal backtest outcome capture failed"
        ) from error
