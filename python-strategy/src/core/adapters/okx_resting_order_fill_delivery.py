from __future__ import annotations

from dataclasses import dataclass
from decimal import Context, Decimal, Inexact, InvalidOperation, Overflow

from src.core.adapters.okx_resting_order_fills import OkxFillRead
from src.core.adapters.okx_resting_order_plan import OkxSwapInstrument
from src.core.resting_order_strategy import RestingFill, RestingIntent, RestingOrderStrategy


_EXACT = Context(prec=60, traps=[Inexact, InvalidOperation, Overflow])

@dataclass(frozen=True, slots=True)
class OkxFillDelivery:
    intent: RestingIntent | None
    flatten: bool
    delivered: int
    cursor: str | None


def deliver_okx_fills(
    strategy: RestingOrderStrategy,
    read: OkxFillRead,
    instrument: OkxSwapInstrument,
) -> OkxFillDelivery:
    if type(read) is not OkxFillRead or type(instrument) is not OkxSwapInstrument:
        raise ValueError("invalid OKX fill delivery input")

    previous_bill_id: int | None = None
    for fill in read.fills:
        bill_id = int(fill.bill_id)
        if previous_bill_id is not None and bill_id <= previous_bill_id:
            raise ValueError("OKX fill billIds must be strictly ascending")
        previous_bill_id = bill_id

    resting_fills: list[RestingFill] = []
    for fill in read.fills:
        if fill.side not in ("buy", "sell"):
            raise ValueError("invalid OKX fill side")
        try:
            quantity = _EXACT.multiply(fill.sz, instrument.ct_val)
        except ArithmeticError as error:
            raise ValueError("OKX fill value is not exactly representable as float") from error
        price_float, quantity_float = float(fill.px), float(quantity)
        if Decimal(str(price_float)) != fill.px or Decimal(str(quantity_float)) != quantity:
            raise ValueError("OKX fill value is not exactly representable as float")
        resting_fills.append(RestingFill(ts_ms=fill.ts_ms, side=fill.side, price=price_float, quantity=quantity_float))

    intent: RestingIntent | None = None
    flatten = False
    delivered = 0
    for resting_fill in resting_fills:
        returned = strategy.on_fill(resting_fill)
        if not isinstance(returned, RestingIntent):
            raise ValueError("on_fill must return RestingIntent")
        intent = returned
        flatten = flatten or returned.flatten
        delivered += 1

    return OkxFillDelivery(
        intent=intent,
        flatten=flatten,
        delivered=delivered,
        cursor=read.cursor,
    )
