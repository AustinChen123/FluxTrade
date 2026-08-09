from decimal import Decimal

import pytest
from pydantic import ValidationError

from src.core.models import Signal, SignalType
from src.validation.backtest_capture import capture_signal_batch
from src.validation.trading_outcome import SignalObservation


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
