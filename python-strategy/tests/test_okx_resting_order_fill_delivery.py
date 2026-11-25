from decimal import Decimal
from typing import cast

import pytest

from src.core.adapters.okx_resting_order_fill_delivery import (
    OkxFillDelivery,
    deliver_okx_fills,
)
from src.core.adapters.okx_resting_order_fills import OkxFillRead, OkxOwnFill
from src.core.adapters.okx_resting_order_plan import OkxSwapInstrument
from src.core.resting_order_strategy import (
    DesiredOrder,
    MinuteBar,
    RestingFill,
    RestingIntent,
    RestingOrderStrategy,
)


INSTRUMENT = OkxSwapInstrument(
    "TOY:SWAP", Decimal("0.1"), Decimal("1"), Decimal("1"), Decimal("0.1")
)


def own_fill(
    bill_id: str,
    *,
    side: str = "buy",
    px: str = "10",
    sz: str = "1",
    ts_ms: int = 1_700_000_040_000,
) -> OkxOwnFill:
    return OkxOwnFill(bill_id, bill_id, bill_id, "wire", side, Decimal(px), Decimal(sz), ts_ms, "M")


def read(*fills: OkxOwnFill, cursor: str | None = "12") -> OkxFillRead:
    return OkxFillRead(tuple(fills), 0, cursor)


class RecordingStrategy(RestingOrderStrategy):
    def __init__(self, intents: list[RestingIntent] | None = None, fail_at: int | None = None) -> None:
        self.fills: list[RestingFill] = []
        self.intents = intents or [RestingIntent(())]
        self.fail_at = fail_at

    def on_bar(self, bar: MinuteBar) -> RestingIntent:
        raise AssertionError("on_bar must not be called")

    def on_fill(self, fill: RestingFill) -> RestingIntent:
        self.fills.append(fill)
        if len(self.fills) == self.fail_at:
            raise RuntimeError("toy on_fill failure")
        return self.intents[min(len(self.fills) - 1, len(self.intents) - 1)]


def test_delivers_fills_in_order_with_base_quantity_and_returns_last_intent() -> None:
    fills = (
        own_fill("5", side="buy", px="10.25", sz="3", ts_ms=1_700_000_040_000),
        own_fill("9", side="sell", px="11.5", sz="1", ts_ms=1_700_000_100_000),
        own_fill("12", side="buy", px="9.75", sz="2", ts_ms=1_700_000_160_000),
    )
    intents = [
        RestingIntent((DesiredOrder("buy", 9.0, 1.0),)),
        RestingIntent((DesiredOrder("sell", 12.0, 1.0),)),
        RestingIntent((DesiredOrder("buy", 8.0, 2.0),)),
    ]
    strategy = RecordingStrategy(intents)

    result = deliver_okx_fills(strategy, read(*fills), INSTRUMENT)

    assert strategy.fills == [
        RestingFill(1_700_000_040_000, "buy", 10.25, 0.3),
        RestingFill(1_700_000_100_000, "sell", 11.5, 0.1),
        RestingFill(1_700_000_160_000, "buy", 9.75, 0.2),
    ]
    assert result == OkxFillDelivery(intents[2], False, 3, "12")


def test_empty_read_does_not_call_strategy_and_passes_cursor() -> None:
    strategy = RecordingStrategy()

    result = deliver_okx_fills(strategy, read(cursor="8"), INSTRUMENT)

    assert strategy.fills == []
    assert result == OkxFillDelivery(None, False, 0, "8")


def test_flatten_is_sticky_across_intents() -> None:
    intents = [RestingIntent(()), RestingIntent((), flatten=True), RestingIntent(())]
    strategy = RecordingStrategy(intents)

    result = deliver_okx_fills(
        strategy,
        read(own_fill("5"), own_fill("9"), own_fill("12")),
        INSTRUMENT,
    )

    assert result.intent is intents[2]
    assert result.flatten is True
    assert result.delivered == 3


def test_on_fill_exception_propagates_after_prior_fill() -> None:
    strategy = RecordingStrategy(fail_at=2)

    with pytest.raises(RuntimeError, match="toy on_fill failure"):
        deliver_okx_fills(
            strategy,
            read(own_fill("5"), own_fill("9"), own_fill("12")),
            INSTRUMENT,
        )

    assert len(strategy.fills) == 2


@pytest.mark.parametrize("bill_ids", [("9", "5"), ("5", "5")])
def test_invalid_bill_order_fails_before_any_delivery(bill_ids: tuple[str, str]) -> None:
    strategy = RecordingStrategy()

    with pytest.raises(ValueError, match="strictly ascending"):
        deliver_okx_fills(strategy, read(*(own_fill(bill_id) for bill_id in bill_ids)), INSTRUMENT)

    assert strategy.fills == []


def test_numeric_bill_order_accepts_different_width_legal_twin() -> None:
    strategy = RecordingStrategy([RestingIntent(()), RestingIntent(())])

    result = deliver_okx_fills(strategy, read(own_fill("9"), own_fill("10")), INSTRUMENT)

    assert len(strategy.fills) == 2
    assert result.delivered == 2


def test_non_intent_return_is_rejected() -> None:
    class Invalid(RestingOrderStrategy):
        def __init__(self) -> None:
            self.fills: list[RestingFill] = []

        def on_bar(self, bar: MinuteBar) -> RestingIntent:
            raise AssertionError("on_bar must not be called")

        def on_fill(self, fill: RestingFill) -> RestingIntent:
            self.fills.append(fill)
            return cast(RestingIntent, None)

    with pytest.raises(ValueError, match="must return RestingIntent"):
        deliver_okx_fills(Invalid(), read(own_fill("5")), INSTRUMENT)


def test_millisecond_fill_times_are_delivered_unchanged_in_a_mixed_batch() -> None:
    strategy = RecordingStrategy()
    result = deliver_okx_fills(
        strategy,
        read(own_fill("5", ts_ms=1_700_000_040_000), own_fill("6", ts_ms=1_700_000_040_001)),
        INSTRUMENT,
    )
    assert [fill.ts_ms for fill in strategy.fills] == [1_700_000_040_000, 1_700_000_040_001]
    assert result.delivered == 2


@pytest.mark.parametrize(
    ("px", "sz"),
    [("0.99999999999999999", "1"), ("1E-400", "1"), ("10", "1E+400"), ("10", "0.99999999999999999")],
)
def test_values_not_exact_as_float_fail_before_any_delivery(px: str, sz: str) -> None:
    strategy = RecordingStrategy()
    with pytest.raises(ValueError, match="exactly representable"):
        deliver_okx_fills(strategy, read(own_fill("5"), own_fill("6", px=px, sz=sz)), INSTRUMENT)
    assert strategy.fills == []
    twin = RecordingStrategy()
    deliver_okx_fills(twin, read(own_fill("5"), own_fill("6", px="0.999999", sz="7")), INSTRUMENT)
    assert [(fill.price, fill.quantity) for fill in twin.fills] == [(10.0, 0.1), (0.999999, 0.7)]


@pytest.mark.parametrize("prec", [28, 2])
def test_contract_to_base_product_is_exact_regardless_of_ambient_precision(prec: int) -> None:
    from decimal import localcontext

    with localcontext() as ctx:
        ctx.prec = prec
        rejected = RecordingStrategy()
        with pytest.raises(ValueError, match="exactly representable"):
            deliver_okx_fills(
                rejected,
                read(own_fill("5"), own_fill("6", sz="0.99999999999999999999999999999")),
                INSTRUMENT,
            )
        assert rejected.fills == []
        twin = RecordingStrategy()
        deliver_okx_fills(twin, read(own_fill("5"), own_fill("6", sz="3.33")), INSTRUMENT)
        assert [fill.quantity for fill in twin.fills] == [0.1, 0.333]


def test_product_beyond_exact_precision_is_rejected_not_rounded() -> None:
    strategy = RecordingStrategy()
    with pytest.raises(ValueError, match="exactly representable"):
        deliver_okx_fills(strategy, read(own_fill("5", sz="1." + "0" * 63 + "1")), INSTRUMENT)
    assert strategy.fills == []
