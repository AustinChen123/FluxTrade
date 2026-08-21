from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP, localcontext

import pytest
from pydantic import ValidationError

from src.core.backtest.endpoint_state import (
    EndpointOrder,
    EndpointPosition,
    ReplayEndpointState,
    build_replay_endpoint_state,
)
from src.core.models import OrderSide, PositionSide, Signal, SignalType
from src.core.decimal_math import canonical_decimal_text
from src.validation.backtest_capture import (
    BacktestOutcomeCaptureError,
    build_normal_backtest_trading_outcome,
    capture_signal_batch,
)
from src.validation.trading_outcome import SignalObservation, TradingOutcome


def _signal(
    *,
    signal_type: SignalType = SignalType.LONG,
    timestamp: int = 1_700_000_000_000,
    metadata: dict[str, object] | None = None,
) -> Signal:
    return Signal(
        strategy_id="strategy-a",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=timestamp,
        type=signal_type,
        value=Decimal("101.25"),
        quantity=Decimal("0.5"),
        price=Decimal("101.50"),
        stop_loss=Decimal("99.75"),
        take_profit=Decimal("104.00"),
        trailing_distance=Decimal("1.25"),
        metadata=metadata
        or {
            "client_order_id": "strategy-a:1",
            "nested": {"levels": [Decimal("101.25"), "entry"]},
        },
    )


def test_capture_signal_batch_projects_every_field_and_preserves_order() -> None:
    first = _signal()
    no_signal = _signal(
        signal_type=SignalType.NO_SIGNAL,
        timestamp=first.timestamp + 60_000,
        metadata={"client_order_id": "strategy-a:2"},
    )

    captured = capture_signal_batch((first, no_signal))

    assert type(captured) is tuple
    assert tuple(type(item) for item in captured) == (
        SignalObservation,
        SignalObservation,
    )
    assert tuple(item.timestamp_ms for item in captured) == (
        first.timestamp,
        no_signal.timestamp,
    )
    assert tuple(item.signal_type for item in captured) == ("LONG", "NO_SIGNAL")
    assert captured[0].strategy_id == first.strategy_id
    assert captured[0].product_id == first.product_id
    assert captured[0].timeframe == first.timeframe
    assert captured[0].value == Decimal("101.25")
    assert captured[0].quantity == Decimal("0.5")
    assert captured[0].price == Decimal("101.5")
    assert captured[0].stop_loss == Decimal("99.75")
    assert captured[0].take_profit == Decimal("104")
    assert captured[0].trailing_distance == Decimal("1.25")
    assert captured[0].metadata_json == (
        '["map",[["client_order_id",["string","strategy-a:1"]],'
        '["nested",["map",[["levels",["list",[['
        '"decimal",0,"10125",-2],["string","entry"]]]]]]]]]'
    )


def test_capture_signal_batch_detaches_source_at_observer_time() -> None:
    metadata: dict[str, object] = {
        "client_order_id": "strategy-a:1",
        "nested": {"levels": [Decimal("101.25"), "entry"]},
    }
    signal = _signal(metadata=metadata)

    captured = capture_signal_batch((signal,))
    original_json = captured[0].metadata_json
    original_dump = captured[0].model_dump()

    signal.quantity = Decimal("99")
    metadata["client_order_id"] = "mutated"
    nested = metadata["nested"]
    assert type(nested) is dict
    levels = nested["levels"]
    assert type(levels) is list
    levels.append("late")

    assert captured[0].metadata_json == original_json
    assert captured[0].model_dump() == original_dump
    assert captured[0].quantity == Decimal("0.5")


def test_capture_signal_batch_preserves_nullable_fields() -> None:
    signal = _signal().model_copy(
        update={
            "value": None,
            "quantity": None,
            "price": None,
            "stop_loss": None,
            "take_profit": None,
            "trailing_distance": None,
            "metadata": None,
        }
    )

    captured = capture_signal_batch((signal,))

    assert captured[0].value is None
    assert captured[0].quantity is None
    assert captured[0].price is None
    assert captured[0].stop_loss is None
    assert captured[0].take_profit is None
    assert captured[0].trailing_distance is None
    assert captured[0].metadata_json == '["null"]'


def test_capture_signal_batch_preserves_validation_error_owner() -> None:
    corrupt = _signal().model_copy(update={"quantity": 0.5})

    with pytest.raises(ValidationError) as caught:
        capture_signal_batch((corrupt,))

    assert caught.value.title == "SignalObservation"
    assert [error["loc"] for error in caught.value.errors()] == [("quantity",)]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("quantity", 0.5),
        ("timestamp", "1700000000000"),
        ("type", "LONG"),
        ("strategy_id", 7),
        ("metadata", {"bad": 1.5}),
    ],
)
def test_capture_signal_batch_rejects_corrupt_model_copy(
    field: str, value: object
) -> None:
    corrupt = _signal().model_copy(update={field: value})

    with pytest.raises((ValidationError, ValueError)):
        capture_signal_batch((corrupt,))


@pytest.mark.parametrize("missing", ["product_id", "timestamp", "type", "quantity"])
def test_capture_signal_batch_rejects_missing_constructed_field(missing: str) -> None:
    values = _signal().model_dump()
    values.pop(missing)
    corrupt = Signal.model_construct(**values)
    vars(corrupt).pop(missing, None)

    with pytest.raises((ValidationError, ValueError)):
        capture_signal_batch((corrupt,))


def test_capture_signal_batch_rejects_extra_constructed_state() -> None:
    corrupt = _signal().model_copy()
    vars(corrupt)["unexpected"] = "state"

    with pytest.raises(ValueError, match="unexpected fields"):
        capture_signal_batch((corrupt,))


def test_capture_signal_batch_requires_exact_tuple_and_signal() -> None:
    class DerivedSignal(Signal):
        pass

    with pytest.raises(ValueError, match="exact tuple"):
        capture_signal_batch([_signal()])
    with pytest.raises(ValueError, match="exact Signal"):
        capture_signal_batch((DerivedSignal.model_validate(_signal().model_dump()),))


def test_capture_signal_batch_preserves_empty_batch() -> None:
    assert capture_signal_batch(()) == ()


def _outcome_sources(
    *,
    raw_order_id: str = "raw-order-a",
    raw_fill_id: str = "raw-fill-a",
) -> dict[str, object]:
    signals = capture_signal_batch(
        (_signal(signal_type=SignalType.LONG, timestamp=100),)
    )
    fills = (
        {
            "id": raw_fill_id,
            "strategy_id": "strategy-a",
            "order_id": raw_order_id,
            "exchange_trade_id": None,
            "product_id": "BINANCE:BTCUSDT-PERP",
            "side": "buy",
            "price": Decimal("101.50"),
            "quantity": Decimal("0.5"),
            "fee": Decimal("0.25"),
            "fee_asset": "USDT",
            "timestamp": 101,
            "fill_sequence": 0,
        },
    )
    journal = (
        {
            "strategy_id": "strategy-a",
            "timestamp": 100,
            "tag": "entry",
            "data": {
                "order_id": raw_order_id,
                "side": OrderSide.BUY,
                "order_type": "market",
                "quantity": "0.5",
                "price": "market",
                "stop_loss": None,
                "take_profit": None,
                "trailing_distance": None,
            },
            "trade_id": raw_order_id,
        },
        {
            "strategy_id": "strategy-a",
            "timestamp": 101,
            "tag": "fill",
            "data": {
                "order_id": raw_order_id,
                "side": OrderSide.BUY,
                "price": "101.50",
                "quantity": "0.5",
                "fee": "0.25",
                "fill_type": "MARKET",
            },
            "trade_id": raw_order_id,
        },
    )
    endpoint_state = build_replay_endpoint_state(
        positions=(),
        working_orders=(),
        final_mark=Decimal("101.50"),
        end_timestamp=200,
        halted_early=False,
    )
    return {
        "signals": signals,
        "fills": fills,
        "journal": journal,
        "endpoint_state": endpoint_state,
        "initial_balance": Decimal("10000"),
        "total_pnl": Decimal("5"),
    }


def test_build_normal_backtest_outcome_links_all_canonical_sections() -> None:
    sources = _outcome_sources()

    outcome = build_normal_backtest_trading_outcome(**sources)

    assert type(outcome) is TradingOutcome
    assert outcome.signals is not sources["signals"]
    assert tuple(order.phase for order in outcome.order_observations) == (
        "submitted",
        "filled",
    )
    assert tuple(order.status for order in outcome.order_observations) == (
        "PLACED",
        "FILLED",
    )
    assert {order.logical_order_id for order in outcome.order_observations} == {
        "order-000000"
    }
    assert outcome.fills[0].logical_order_id == "order-000000"
    assert tuple(item.logical_trade_id for item in outcome.journal) == (
        "order-000000",
        "order-000000",
    )
    assert outcome.financial.fees == Decimal("0.25")
    assert outcome.financial.realized_pnl == Decimal("5")
    assert outcome.financial.unrealized_pnl == Decimal("0")
    assert outcome.financial.equity == Decimal("10005")
    canonical = outcome.canonical_bytes()
    assert outcome.sha256()
    assert b"raw-order-a" not in canonical
    assert b"raw-fill-a" not in canonical


def test_build_normal_backtest_outcome_is_independent_of_raw_ids_and_sources() -> None:
    first_sources = _outcome_sources()
    second_sources = _outcome_sources(
        raw_order_id="other-order",
        raw_fill_id="other-fill",
    )

    first = build_normal_backtest_trading_outcome(**first_sources)
    second = build_normal_backtest_trading_outcome(**second_sources)
    before = first.canonical_bytes()

    fills = first_sources["fills"]
    journal = first_sources["journal"]
    assert type(fills) is tuple and type(journal) is tuple
    fills[0]["price"] = Decimal("999")
    journal[0]["data"]["quantity"] = "999"

    assert first.canonical_bytes() == before
    assert second.canonical_bytes() == before
    assert first.sha256() == second.sha256()
    assert first.first_difference(second) is None
    assert b"raw-order-a" not in before
    assert b"other-order" not in before
    assert b"raw-fill-a" not in before
    assert b"other-fill" not in before


@pytest.mark.parametrize("precision", [6, 60])
@pytest.mark.parametrize("rounding", [ROUND_DOWN, ROUND_HALF_UP])
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (Decimal("1E-8"), "0.00000001"),
        (Decimal("-1E-8"), "-0.00000001"),
        (Decimal("1E+3"), "1000"),
        (Decimal("0E-13"), "0"),
        (Decimal("-0"), "0"),
        (Decimal("1.230000"), "1.23"),
        (Decimal("1.234567890123"), "1.234567890123"),
    ],
)
def test_canonical_decimal_text_is_fixed_and_context_independent(
    precision: int,
    rounding: str,
    value: Decimal,
    expected: str,
) -> None:
    with localcontext() as context:
        context.prec = precision
        context.rounding = rounding
        actual = canonical_decimal_text(value)

    assert actual == expected


@pytest.mark.parametrize("precision", [6, 60])
@pytest.mark.parametrize("rounding", [ROUND_DOWN, ROUND_HALF_UP])
def test_build_normal_backtest_outcome_canonicalizes_journal_money_text(
    precision: int,
    rounding: str,
) -> None:
    minimal = _outcome_sources()
    exponent = _outcome_sources()
    exponent_fills = exponent["fills"]
    exponent_journal = exponent["journal"]
    assert type(exponent_fills) is tuple and type(exponent_journal) is tuple
    fill = dict(exponent_fills[0])
    fill.update(
        price=Decimal("1.015E+2"),
        quantity=Decimal("5E-1"),
        fee=Decimal("2.5E-1"),
    )
    exponent["fills"] = (fill,)
    rows = [dict(row) for row in exponent_journal]
    entry_data = dict(rows[0]["data"])
    entry_data["quantity"] = "5E-1"
    rows[0]["data"] = entry_data
    fill_data = dict(rows[1]["data"])
    fill_data.update(price="1.015E+2", quantity="5E-1", fee="2.5E-1")
    rows[1]["data"] = fill_data
    exponent["journal"] = tuple(rows)

    with localcontext() as context:
        context.prec = precision
        context.rounding = rounding
        minimal_outcome = build_normal_backtest_trading_outcome(**minimal)
        exponent_outcome = build_normal_backtest_trading_outcome(**exponent)

    assert exponent_outcome.canonical_bytes() == minimal_outcome.canonical_bytes()
    assert exponent_outcome.sha256() == minimal_outcome.sha256()
    assert exponent_outcome.first_difference(minimal_outcome) is None
    assert '"quantity",["string","0.5"]' in exponent_outcome.journal[0].data_json
    fill_json = exponent_outcome.journal[1].data_json
    assert '"price",["string","101.5"]' in fill_json
    assert '"quantity",["string","0.5"]' in fill_json
    assert '"fee",["string","0.25"]' in fill_json


def test_journal_money_mismatch_fails_before_canonical_projection() -> None:
    sources = _outcome_sources()
    journal = sources["journal"]
    assert type(journal) is tuple
    rows = [dict(row) for row in journal]
    entry_data = dict(rows[0]["data"])
    entry_data["quantity"] = "5.1E-1"
    rows[0]["data"] = entry_data
    sources["journal"] = tuple(rows)

    with pytest.raises(BacktestOutcomeCaptureError) as caught:
        build_normal_backtest_trading_outcome(**sources)

    assert type(caught.value.__cause__) is ValueError
    assert str(caught.value.__cause__) == (
        "signal, entry and fill quantity do not match"
    )


def test_build_normal_backtest_outcome_accepts_negative_total_pnl() -> None:
    sources = _outcome_sources()
    sources["total_pnl"] = Decimal("-5.25")

    outcome = build_normal_backtest_trading_outcome(**sources)

    assert outcome.financial.realized_pnl == Decimal("-5.25")
    assert outcome.financial.equity == Decimal("9994.75")


@pytest.mark.parametrize("precision", [6, 28, 50])
def test_build_normal_backtest_outcome_financial_equation_is_context_independent(
    precision: int,
) -> None:
    sources = _outcome_sources()
    fills = sources["fills"]
    journal = sources["journal"]
    assert type(fills) is tuple and type(journal) is tuple
    fee = Decimal("0.1234567890123456789012345678")
    fill = dict(fills[0])
    fill["fee"] = fee
    sources["fills"] = (fill,)
    rows = [dict(row) for row in journal]
    fill_data = dict(rows[1]["data"])
    fill_data["fee"] = str(fee)
    rows[1]["data"] = fill_data
    sources["journal"] = tuple(rows)
    initial = Decimal("1234567890123456789012345678")
    pnl = Decimal("0.1")
    sources["initial_balance"] = initial
    sources["total_pnl"] = pnl

    with localcontext() as context:
        context.prec = precision
        outcome = build_normal_backtest_trading_outcome(**sources)

    assert outcome.financial.fees == fee
    assert outcome.financial.realized_pnl == pnl
    assert outcome.financial.equity == Decimal("1234567890123456789012345678.1")


@pytest.mark.parametrize("invalid_input", ["no_actionable_signal", "endpoint_dict"])
def test_build_normal_backtest_outcome_requires_supported_normal_path(
    invalid_input: str,
) -> None:
    sources = _outcome_sources()
    if invalid_input == "no_actionable_signal":
        sources["signals"] = capture_signal_batch((_signal(timestamp=100),))
        sources["fills"] = ()
        sources["journal"] = ()
    else:
        endpoint_state = sources["endpoint_state"]
        assert type(endpoint_state) is ReplayEndpointState
        sources["endpoint_state"] = endpoint_state.model_dump()

    with pytest.raises(BacktestOutcomeCaptureError) as caught:
        build_normal_backtest_trading_outcome(**sources)

    assert caught.value.args == ("normal backtest outcome capture failed",)
    assert type(caught.value.__cause__) is ValueError


@pytest.mark.parametrize(
    ("mutation", "expected_cause"),
    [
        ("fill_price", "price"),
        ("fill_sequence", "fill_sequence"),
        ("strategy", "strategy"),
        ("working_order", "working_orders"),
        ("position", "positions"),
    ],
)
def test_build_normal_backtest_outcome_rejects_inconsistent_sources(
    mutation: str, expected_cause: str
) -> None:
    sources = _outcome_sources()
    fills = sources["fills"]
    journal = sources["journal"]
    assert type(fills) is tuple and type(journal) is tuple
    if mutation == "fill_price":
        changed = dict(fills[0])
        changed["price"] = Decimal("102")
        sources["fills"] = (changed,)
    elif mutation == "fill_sequence":
        changed = dict(fills[0])
        changed["fill_sequence"] = 1
        sources["fills"] = (changed,)
    elif mutation == "strategy":
        changed = dict(fills[0])
        changed["strategy_id"] = "other"
        sources["fills"] = (changed,)
    elif mutation == "working_order":
        sources["endpoint_state"] = build_replay_endpoint_state(
            positions=(),
            working_orders=(
                EndpointOrder(
                    strategy_id="strategy-a",
                    product_id="BINANCE:BTCUSDT-PERP",
                    side=OrderSide.BUY,
                    order_type="LIMIT",
                    quantity=Decimal("0.5"),
                    timestamp=199,
                    price=Decimal("100"),
                ),
            ),
            final_mark=Decimal("101.50"),
            end_timestamp=200,
            halted_early=False,
        )
    else:
        sources["endpoint_state"] = build_replay_endpoint_state(
            positions=(
                EndpointPosition(
                    strategy_id="strategy-a",
                    product_id="BINANCE:BTCUSDT-PERP",
                    side=PositionSide.LONG,
                    quantity=Decimal("0.5"),
                    average_entry_price=Decimal("101.50"),
                ),
            ),
            working_orders=(),
            final_mark=Decimal("101.50"),
            end_timestamp=200,
            halted_early=False,
        )

    with pytest.raises(BacktestOutcomeCaptureError) as caught:
        build_normal_backtest_trading_outcome(**sources)

    assert caught.value.stage == "normal_backtest_outcome_capture"
    assert caught.value.__cause__ is not None
    assert expected_cause in str(caught.value.__cause__)


@pytest.mark.parametrize(
    ("signal_type", "side"),
    [
        (SignalType.LONG, OrderSide.BUY),
        (SignalType.SHORT, OrderSide.SELL),
        (SignalType.EXIT_LONG, OrderSide.SELL),
        (SignalType.EXIT_SHORT, OrderSide.BUY),
    ],
)
def test_build_normal_backtest_outcome_maps_all_signal_sides(
    signal_type: SignalType, side: OrderSide
) -> None:
    sources = _outcome_sources()
    sources["signals"] = capture_signal_batch(
        (_signal(signal_type=signal_type, timestamp=100),)
    )
    fills = sources["fills"]
    journal = sources["journal"]
    assert type(fills) is tuple and type(journal) is tuple
    fill = dict(fills[0])
    fill["side"] = side.value
    sources["fills"] = (fill,)
    changed_journal = []
    for row in journal:
        changed = dict(row)
        data = dict(changed["data"])
        data["side"] = side
        changed["data"] = data
        changed_journal.append(changed)
    sources["journal"] = tuple(changed_journal)

    outcome = build_normal_backtest_trading_outcome(**sources)

    assert outcome.order_observations[0].side == side.value
    assert outcome.fills[0].side == side.value


@pytest.mark.parametrize(
    ("target", "replacement"),
    [
        ("fill_price", 101.5),
        ("fill_quantity", "0.5"),
        ("fill_fee", 0),
        ("initial_balance", 10_000),
        ("total_pnl", "5"),
    ],
)
def test_build_normal_backtest_outcome_rejects_non_decimal_money(
    target: str, replacement: object
) -> None:
    sources = _outcome_sources()
    if target.startswith("fill_"):
        fills = sources["fills"]
        assert type(fills) is tuple
        fill = dict(fills[0])
        fill[target.removeprefix("fill_")] = replacement
        sources["fills"] = (fill,)
    else:
        sources[target] = replacement

    with pytest.raises(BacktestOutcomeCaptureError) as caught:
        build_normal_backtest_trading_outcome(**sources)

    assert caught.value.args == ("normal backtest outcome capture failed",)
    assert type(caught.value.__cause__) is ValueError


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_fill_key",
        "extra_fill_key",
        "unknown_order_id",
        "reordered_journal",
        "journal_fee",
        "product",
        "duplicate_order_id",
        "missing_fill",
    ],
)
def test_build_normal_backtest_outcome_rejects_invalid_evidence_shape(
    mutation: str,
) -> None:
    sources = _outcome_sources()
    fills = sources["fills"]
    journal = sources["journal"]
    assert type(fills) is tuple and type(journal) is tuple
    fill = dict(fills[0])
    rows = [dict(row) for row in journal]
    if mutation == "missing_fill_key":
        del fill["fee_asset"]
        sources["fills"] = (fill,)
    elif mutation == "extra_fill_key":
        fill["unexpected"] = None
        sources["fills"] = (fill,)
    elif mutation == "unknown_order_id":
        fill["order_id"] = "unknown"
        sources["fills"] = (fill,)
    elif mutation == "reordered_journal":
        sources["journal"] = tuple(reversed(rows))
    elif mutation == "journal_fee":
        data = dict(rows[1]["data"])
        data["fee"] = "0.26"
        rows[1]["data"] = data
        sources["journal"] = tuple(rows)
    elif mutation == "product":
        fill["product_id"] = "BINANCE:ETHUSDT-PERP"
        sources["fills"] = (fill,)
    elif mutation == "duplicate_order_id":
        second = dict(fill)
        second["id"] = "raw-fill-b"
        second["side"] = "sell"
        second["timestamp"] = 201
        second["fill_sequence"] = 1
        sources["fills"] = (fill, second)
        sources["signals"] = capture_signal_batch(
            (
                _signal(signal_type=SignalType.LONG, timestamp=100),
                _signal(signal_type=SignalType.EXIT_LONG, timestamp=200),
            )
        )
        second_entry = dict(rows[0])
        second_entry["timestamp"] = 200
        second_entry_data = dict(second_entry["data"])
        second_entry_data["side"] = OrderSide.SELL
        second_entry["data"] = second_entry_data
        second_fill = dict(rows[1])
        second_fill["timestamp"] = 201
        second_fill_data = dict(second_fill["data"])
        second_fill_data["side"] = OrderSide.SELL
        second_fill["data"] = second_fill_data
        sources["journal"] = (*rows, second_entry, second_fill)
    else:
        sources["fills"] = ()

    with pytest.raises(BacktestOutcomeCaptureError) as caught:
        build_normal_backtest_trading_outcome(**sources)

    assert caught.value.args == ("normal backtest outcome capture failed",)
    assert type(caught.value.__cause__) is ValueError
    assert "raw-order-a" not in str(caught.value)
