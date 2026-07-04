"""Centralized product ID ↔ exchange symbol mapping.

Replaces ad-hoc _map_symbol() in ExchangeAdapter and PRODUCT_TO_CCXT
in fetch_real_data.py with a single registry.

Product ID format: EXCHANGE:BASEQUOTE-PERP
  e.g. BINANCE:BTCUSDT-PERP, BYBIT:ETHUSDT-PERP
"""

import re
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import Any


# Known product mappings with exchange-specific overrides.
# Only entries that cannot be derived from generic parsing need to be here.
_KNOWN_PRODUCTS: dict[str, dict] = {
    "BINANCE:BTCUSDT-PERP": {
        "exchange": "binance",
        "ccxt": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
    },
    "BINANCE:ETHUSDT-PERP": {
        "exchange": "binance",
        "ccxt": "ETH/USDT:USDT",
        "base": "ETH",
        "quote": "USDT",
    },
    "BYBIT:BTCUSDT-PERP": {
        "exchange": "bybit",
        "ccxt": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
    },
    "BYBIT:ETHUSDT-PERP": {
        "exchange": "bybit",
        "ccxt": "ETH/USDT:USDT",
        "base": "ETH",
        "quote": "USDT",
    },
    "BACKPACK:BTCUSDT-PERP": {
        "exchange": "backpack",
        "ccxt": "BTC/USDT:USDT",
        "base": "BTC",
        "quote": "USDT",
    },
}

_PRODUCT_ID_PATTERN = re.compile(r"^([A-Z0-9]+):([A-Z0-9]+)(USDT|USDC|BUSD)-PERP$")


@dataclass(frozen=True)
class InstrumentSpec:
    """Venue-neutral instrument trading rules for outbound order validation."""

    product_id: str
    exchange: str
    symbol: str
    base: str
    quote: str
    quantity_step: Decimal | None = None
    price_tick: Decimal | None = None
    min_notional: Decimal | None = None
    min_quantity: Decimal | None = None
    multiplier: Decimal | None = None
    tick_value: Decimal | None = None
    fee_model: str | None = None
    session_calendar_id: str | None = None


@dataclass(frozen=True)
class QuantizedOrder:
    quantity: Decimal
    price: Decimal | None
    trigger_price: Decimal | None
    changed: bool


def _parse_product_id(product_id: str) -> dict:
    """Parse product_id into components using generic rules.

    Falls back to regex parsing when not in _KNOWN_PRODUCTS.

    Raises:
        ValueError: If product_id format is unrecognizable.
    """
    if product_id in _KNOWN_PRODUCTS:
        return _KNOWN_PRODUCTS[product_id]

    m = _PRODUCT_ID_PATTERN.match(product_id)
    if not m:
        raise ValueError(
            f"Cannot parse product_id: {product_id}. "
            f"Expected EXCHANGE:BASEQUOTE-PERP (e.g. BINANCE:BTCUSDT-PERP)"
        )

    exchange = m.group(1).lower()
    base = m.group(2)
    quote = m.group(3)

    return {
        "exchange": exchange,
        "ccxt": f"{base}/{quote}:{quote}",
        "base": base,
        "quote": quote,
    }


def to_ccxt_symbol(product_id: str) -> str:
    """Convert product_id to CCXT symbol.

    Examples:
        >>> to_ccxt_symbol("BINANCE:BTCUSDT-PERP")
        'BTC/USDT:USDT'
        >>> to_ccxt_symbol("BYBIT:ETHUSDT-PERP")
        'ETH/USDT:USDT'
    """
    return _parse_product_id(product_id)["ccxt"]


def to_exchange_name(product_id: str) -> str:
    """Extract exchange name from product_id.

    Examples:
        >>> to_exchange_name("BINANCE:BTCUSDT-PERP")
        'binance'
    """
    return _parse_product_id(product_id)["exchange"]


def to_base_quote(product_id: str) -> tuple[str, str]:
    """Extract (base, quote) pair from product_id.

    Examples:
        >>> to_base_quote("BINANCE:BTCUSDT-PERP")
        ('BTC', 'USDT')
    """
    info = _parse_product_id(product_id)
    return info["base"], info["quote"]


def to_stream_key(product_id: str, timeframe: str) -> str:
    """Build Redis stream key from product_id and timeframe.

    Examples:
        >>> to_stream_key("BINANCE:BTCUSDT-PERP", "15m")
        'stream:market:binance:btcusdt:15m'
    """
    info = _parse_product_id(product_id)
    symbol_flat = f"{info['base']}{info['quote']}".lower()
    return f"stream:market:{info['exchange']}:{symbol_flat}:{timeframe}"


def resolve_exchange(product_id: str) -> tuple[str, str]:
    """Return (exchange_name, ccxt_symbol) tuple.

    Drop-in replacement for fetch_real_data.resolve_exchange().

    Examples:
        >>> resolve_exchange("BINANCE:BTCUSDT-PERP")
        ('binance', 'BTC/USDT:USDT')
    """
    info = _parse_product_id(product_id)
    return info["exchange"], info["ccxt"]


def list_known_products() -> list[str]:
    """Return all explicitly registered product IDs."""
    return list(_KNOWN_PRODUCTS.keys())


def instrument_spec_from_product(
    product_id: str,
    *,
    quantity_step: Decimal | None = None,
    price_tick: Decimal | None = None,
    min_notional: Decimal | None = None,
    min_quantity: Decimal | None = None,
    multiplier: Decimal | None = None,
    tick_value: Decimal | None = None,
    fee_model: str | None = None,
    session_calendar_id: str | None = None,
) -> InstrumentSpec:
    info = _parse_product_id(product_id)
    return InstrumentSpec(
        product_id=product_id,
        exchange=info["exchange"],
        symbol=info["ccxt"],
        base=info["base"],
        quote=info["quote"],
        quantity_step=quantity_step,
        price_tick=price_tick,
        min_notional=min_notional,
        min_quantity=min_quantity,
        multiplier=multiplier,
        tick_value=tick_value,
        fee_model=fee_model,
        session_calendar_id=session_calendar_id,
    )


def instrument_spec_from_ccxt_market(
    product_id: str,
    market: dict[str, Any] | None,
) -> InstrumentSpec:
    quantity_step = None
    price_tick = None
    min_notional = None
    min_quantity = None

    if market:
        limits = market.get("limits") or {}
        amount_limits = limits.get("amount") or {}
        cost_limits = limits.get("cost") or {}
        min_quantity = _decimal_or_none(amount_limits.get("min"))
        min_notional = _decimal_or_none(cost_limits.get("min"))
        precision = market.get("precision") or {}
        if isinstance(precision, dict):
            quantity_step = _step_from_precision(precision.get("amount"))
            price_tick = _step_from_precision(precision.get("price"))

        info = market.get("info") or {}
        if isinstance(info, dict):
            lot_size_filter = info.get("lotSizeFilter") or {}
            if isinstance(lot_size_filter, dict):
                quantity_step = _decimal_or_none(
                    lot_size_filter.get("qtyStep"),
                    fallback=quantity_step,
                )
                min_quantity = _decimal_or_none(
                    lot_size_filter.get("minOrderQty"),
                    fallback=min_quantity,
                )
                min_notional = _decimal_or_none(
                    lot_size_filter.get("minNotionalValue"),
                    fallback=min_notional,
                )
            price_filter = info.get("priceFilter") or {}
            if isinstance(price_filter, dict):
                price_tick = _decimal_or_none(
                    price_filter.get("tickSize"),
                    fallback=price_tick,
                )

        filters = info.get("filters") if isinstance(info, dict) else []
        filters = filters or []
        if isinstance(filters, list):
            for exchange_filter in filters:
                filter_type = exchange_filter.get("filterType")
                if filter_type == "LOT_SIZE":
                    quantity_step = _decimal_or_none(
                        exchange_filter.get("stepSize"),
                        fallback=quantity_step,
                    )
                    min_quantity = _decimal_or_none(
                        exchange_filter.get("minQty"),
                        fallback=min_quantity,
                    )
                elif filter_type == "PRICE_FILTER":
                    price_tick = _decimal_or_none(
                        exchange_filter.get("tickSize"),
                        fallback=price_tick,
                    )
                elif filter_type in {"MIN_NOTIONAL", "NOTIONAL"}:
                    min_notional = _decimal_or_none(
                        exchange_filter.get("minNotional")
                        or exchange_filter.get("notional"),
                        fallback=min_notional,
                    )

    return instrument_spec_from_product(
        product_id,
        quantity_step=quantity_step,
        price_tick=price_tick,
        min_notional=min_notional,
        min_quantity=min_quantity,
    )


def quantize_order_values(
    *,
    quantity: Decimal,
    price: Decimal | None,
    side: str | None = None,
    trigger_price: Decimal | None = None,
    spec: InstrumentSpec,
) -> QuantizedOrder:
    quantized_quantity = _floor_to_step(quantity, spec.quantity_step)
    quantized_price = (
        _quantize_limit_price(price, spec.price_tick, side)
        if price is not None
        else None
    )
    quantized_trigger_price = (
        _require_on_step("trigger_price", trigger_price, spec.price_tick)
        if trigger_price is not None
        else None
    )
    return QuantizedOrder(
        quantity=quantized_quantity,
        price=quantized_price,
        trigger_price=quantized_trigger_price,
        changed=(
            quantized_quantity != quantity
            or quantized_price != price
            or quantized_trigger_price != trigger_price
        ),
    )


def validate_min_notional(
    *,
    quantity: Decimal,
    price: Decimal | None,
    spec: InstrumentSpec,
) -> None:
    if quantity <= 0:
        raise ValueError(f"quantity_must_be_positive: quantity={quantity}")
    if spec.min_quantity is not None and quantity < spec.min_quantity:
        raise ValueError(
            f"quantity_below_min: quantity={quantity} min_quantity={spec.min_quantity}"
        )
    if price is None or spec.min_notional is None:
        return
    notional = quantity * price
    if notional < spec.min_notional:
        raise ValueError(
            f"min_notional_not_met: notional={notional} min_notional={spec.min_notional}"
        )


def _floor_to_step(value: Decimal, step: Decimal | None) -> Decimal:
    if step is None or step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _ceil_to_step(value: Decimal, step: Decimal | None) -> Decimal:
    if step is None or step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_UP) * step


def _quantize_limit_price(
    price: Decimal,
    tick: Decimal | None,
    side: str | None,
) -> Decimal:
    if tick is None or tick <= 0 or _floor_to_step(price, tick) == price:
        return price

    normalized_side = side.lower() if side is not None else None
    if normalized_side == "buy":
        return _floor_to_step(price, tick)
    if normalized_side == "sell":
        return _ceil_to_step(price, tick)
    raise ValueError(f"price_off_tick_without_side: price={price} tick={tick}")


def _require_on_step(label: str, value: Decimal, step: Decimal | None) -> Decimal:
    if step is None or step <= 0 or _floor_to_step(value, step) == value:
        return value
    raise ValueError(f"{label}_off_tick: {label}={value} tick={step}")


def _step_from_precision(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    if isinstance(value, int):
        if value < 0:
            return None
        return Decimal(1).scaleb(-value)

    text = str(value)
    decimal = Decimal(text)
    if decimal <= 0:
        return None
    if decimal < 1:
        return decimal
    if decimal == decimal.to_integral_value() and "." not in text and "e" not in text.lower():
        return Decimal(1).scaleb(-int(decimal))
    return decimal


def _decimal_or_none(value: Any, fallback: Decimal | None = None) -> Decimal | None:
    if value is None or value == "":
        return fallback
    decimal = Decimal(str(value))
    if decimal <= 0:
        return fallback
    return decimal
