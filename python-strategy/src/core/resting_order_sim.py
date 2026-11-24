import math
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from src.core.resting_order_strategy import (
    DesiredOrder,
    MinuteBar,
    RestingFill,
    RestingIntent,
    RestingOrderStrategy,
)


@dataclass(frozen=True)
class SimRecord:
    kind: str
    ts_ms: int
    touch_buy: bool | None = None
    touch_sell: bool | None = None
    side: str | None = None
    price: float | None = None
    quantity: float | None = None
    fee: float | None = None
    gross: float | None = None


def simulate(
    strategy: RestingOrderStrategy,
    bars: Iterable[MinuteBar],
    maker_fee: float,
    taker_fee: float,
) -> list[SimRecord]:
    return list(iter_simulate(strategy, bars, maker_fee, taker_fee))


def iter_simulate(
    strategy: RestingOrderStrategy,
    bars: Iterable[MinuteBar],
    maker_fee: float,
    taker_fee: float,
) -> Iterator[SimRecord]:
    for name, fee in (("maker_fee", maker_fee), ("taker_fee", taker_fee)):
        if type(fee) not in (int, float) or not math.isfinite(fee) or fee < 0:
            raise ValueError(f"{name} must be finite and non-negative")

    resting: tuple[DesiredOrder, ...] = ()
    position = 0.0
    cash = 0.0
    fees = 0.0
    settle_cash = 0.0
    settle_fees = 0.0
    previous_ts: int | None = None

    def apply(intent: RestingIntent, price: float, ts_ms: int) -> None:
        nonlocal resting, position, cash, fees, settle_cash, settle_fees
        if type(intent) is not RestingIntent:
            raise ValueError("strategy must return RestingIntent")
        if intent.flatten:
            if position != 0:
                quantity = abs(position)
                cash += position * price
                position = 0.0
                fee = taker_fee * quantity * price
                fees += fee
                pending.append(
                    SimRecord("flatten", ts_ms, price=price, quantity=quantity, fee=fee)
                )
            pending.append(
                SimRecord(
                    "settle", ts_ms, gross=cash - settle_cash, fee=fees - settle_fees
                )
            )
            settle_cash = cash
            settle_fees = fees
        resting = intent.orders

    pending: list[SimRecord] = []
    for bar in bars:
        if type(bar) is not MinuteBar:
            raise ValueError("bars must contain MinuteBar")
        if previous_ts is not None and bar.ts_ms <= previous_ts:
            raise ValueError("bar timestamps must strictly increase")
        previous_ts = bar.ts_ms
        yield SimRecord(
            "bar",
            bar.ts_ms,
            touch_buy=any(
                order.side == "buy" and bar.low <= order.price for order in resting
            ),
            touch_sell=any(
                order.side == "sell" and bar.high >= order.price for order in resting
            ),
        )
        fill_count = 0
        while True:
            crossed_buys = [
                order
                for order in resting
                if order.side == "buy" and bar.close <= order.price
            ]
            crossed_sells = [
                order
                for order in resting
                if order.side == "sell" and bar.close >= order.price
            ]
            if not crossed_buys and not crossed_sells:
                break
            fill_count += 1
            if fill_count > 1000:
                raise ValueError("more than 1000 fills in one bar")
            order = (
                max(crossed_buys, key=lambda item: item.price)
                if crossed_buys
                else min(crossed_sells, key=lambda item: item.price)
            )
            resting = tuple(item for item in resting if item is not order)
            if order.side == "buy":
                position += order.quantity
                cash -= order.quantity * order.price
            else:
                position -= order.quantity
                cash += order.quantity * order.price
            fee = maker_fee * order.quantity * order.price
            fees += fee
            fill = RestingFill(bar.ts_ms, order.side, order.price, order.quantity)
            yield SimRecord(
                "fill",
                bar.ts_ms,
                side=order.side,
                price=order.price,
                quantity=order.quantity,
                fee=fee,
            )
            pending.clear()
            apply(strategy.on_fill(fill), bar.close, bar.ts_ms)
            yield from pending
            pending.clear()
        apply(strategy.on_bar(bar), bar.close, bar.ts_ms)
        yield from pending
        pending.clear()
