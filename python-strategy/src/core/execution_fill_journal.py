from decimal import Decimal
from typing import Any

from src.core.interfaces.exchange import ExchangeOrderEvent
from src.core.models import Candlestick


def intent_signal_price(order: Any) -> str | None:
    intent_payload = getattr(order, "intent_payload", None)
    if not isinstance(intent_payload, dict):
        return None
    order_payload = intent_payload.get("order")
    if not isinstance(order_payload, dict):
        return None
    price = order_payload.get("price")
    return str(price) if price is not None else None


def journal_fill(
    journal: Any,
    order: Any,
    price: Decimal,
    quantity: Decimal,
    fee: Decimal | None,
    fill_type: str,
    candle: Candlestick | None = None,
) -> None:
    tag = {
        "STOP_LOSS": "sl_hit",
        "TAKE_PROFIT": "tp_hit",
        "TRAILING_STOP": "trailing_hit",
        "MARKET": "fill",
        "LIMIT": "fill",
    }.get(fill_type, "fill")
    journal.log(
        tag,
        {
            "order_id": str(order.id),
            "side": order.side,
            "price": str(price),
            "quantity": str(quantity),
            "fee": str(fee) if fee else "0",
            "fill_type": fill_type,
        },
        timestamp=candle.timestamp if candle else 0,
        trade_id=str(order.id),
    )


def journal_exchange_order_event_fill(
    journal: Any,
    clock: Any,
    order: Any,
    event: ExchangeOrderEvent,
    fill_price: Decimal,
    fill_quantity: Decimal,
) -> None:
    journal.log(
        "fill",
        {
            "order_id": str(order.id),
            "side": order.side,
            "signal_price": intent_signal_price(order),
            "submitted_price": str(order.price) if order.price else "market",
            "fill_price": str(fill_price),
            "quantity": str(fill_quantity),
            "fee": str(event.fee) if event.fee is not None else "0",
            "fee_asset": event.fee_asset,
            "exchange_order_id": event.exchange_order_id,
            "client_order_id": event.client_order_id,
            "exchange_status": event.status,
        },
        timestamp=event.event_timestamp or int(clock.now() * 1000),
        trade_id=str(order.id),
    )
