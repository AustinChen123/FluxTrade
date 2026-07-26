"""Centralized product ID ↔ exchange symbol mapping.

Replaces ad-hoc _map_symbol() in ExchangeAdapter and PRODUCT_TO_CCXT
in fetch_real_data.py with a single registry.

Product ID formats:
  - perpetual: EXCHANGE:SYMBOL-PERP (e.g. BINANCE:BTCUSDT-PERP)
  - dated future: EXCHANGE:ROOT-YYYYMM (e.g. RITHMIC:MNQ-202509)
"""

import logging
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
from enum import Enum
from typing import Any

logger = logging.getLogger(__name__)


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

_PERPETUAL_PRODUCT_ID_PATTERN = re.compile(r"^([A-Z0-9]+):([A-Z0-9_]+)-PERP$")
_DATED_FUTURE_PRODUCT_ID_PATTERN = re.compile(
    r"^([A-Z0-9]+):([A-Z][A-Z0-9]*)-([0-9]{4})([0-9]{2})$"
)
_CONTINUOUS_FUTURE_PRODUCT_ID_PATTERN = re.compile(
    r"^([A-Z0-9]+):([A-Z][A-Z0-9]*)-CONTINUOUS$"
)


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
    fee_model: "FeeModel | None" = None
    capital_model: "CapitalModel | None" = None
    capital_per_contract: Decimal | None = None
    session_calendar_id: str | None = None


def resolve_contract_multiplier(spec: InstrumentSpec | None) -> Decimal:
    """Return the authoritative positive contract multiplier for an instrument."""
    multiplier = spec.multiplier if spec is not None else None
    if multiplier is None:
        return Decimal("1")
    if multiplier <= 0:
        raise ValueError("instrument multiplier must be positive")
    return multiplier


@dataclass(frozen=True)
class QuantizedOrder:
    quantity: Decimal
    price: Decimal | None
    trigger_price: Decimal | None
    changed: bool


class PrecisionMode(str, Enum):
    """CCXT precision modes translated at the adapter boundary."""

    DECIMAL_PLACES = "decimal_places"
    SIGNIFICANT_DIGITS = "significant_digits"
    TICK_SIZE = "tick_size"


class FeeModel(str, Enum):
    PERCENTAGE_NOTIONAL = "percentage_notional"
    PER_CONTRACT = "per_contract"


class CapitalModel(str, Enum):
    NOTIONAL = "notional"
    PER_CONTRACT = "per_contract"


def resolve_fee_model(spec: InstrumentSpec | None) -> FeeModel:
    if spec is None or spec.fee_model is None:
        return FeeModel.PERCENTAGE_NOTIONAL
    return FeeModel(spec.fee_model)


def calculate_required_capital(
    quantity: Decimal,
    price: Decimal,
    spec: InstrumentSpec | None,
) -> Decimal:
    """Calculate non-negative capital usage from one explicit instrument model."""
    model = CapitalModel(spec.capital_model) if spec and spec.capital_model else CapitalModel.NOTIONAL
    if model == CapitalModel.PER_CONTRACT:
        capital_per_contract = spec.capital_per_contract if spec else None
        if capital_per_contract is None or capital_per_contract <= 0:
            raise ValueError("capital_per_contract must be positive for per_contract capital")
        return abs(quantity) * capital_per_contract
    return abs(quantity * price * resolve_contract_multiplier(spec))


def calculate_notional_exposure(
    quantity: Decimal,
    price: Decimal,
    spec: InstrumentSpec | None,
) -> Decimal:
    """Return absolute economic exposure independent of fee or capital models."""
    return abs(quantity * price * resolve_contract_multiplier(spec))


class TriggerPricePolicy(str, Enum):
    REQUIRE_ON_TICK = "require_on_tick"
    ROUND_DOWN = "round_down"
    ROUND_UP = "round_up"


def _parse_product_id(product_id: str) -> dict:
    """Parse product_id into components using generic rules.

    Falls back to regex parsing when not in _KNOWN_PRODUCTS.

    Raises:
        ValueError: If product_id format is unrecognizable.
    """
    if product_id in _KNOWN_PRODUCTS:
        return _KNOWN_PRODUCTS[product_id]

    perpetual = _PERPETUAL_PRODUCT_ID_PATTERN.fullmatch(product_id)
    if perpetual:
        exchange = perpetual.group(1).lower()
        symbol = perpetual.group(2)
        quote = next(
            (
                candidate
                for candidate in ("USDT", "USDC", "BUSD")
                if len(symbol) > len(candidate) and symbol.endswith(candidate)
            ),
            "",
        )
        base = symbol[: -len(quote)] if quote else symbol
        if base.endswith("_"):
            base = symbol
            quote = ""
        return {
            "exchange": exchange,
            "ccxt": f"{base}/{quote}:{quote}" if quote else None,
            "stream_symbol": symbol.lower(),
            "base": base,
            "quote": quote,
        }

    dated_future = _DATED_FUTURE_PRODUCT_ID_PATTERN.fullmatch(product_id)
    if dated_future:
        month = int(dated_future.group(4))
        if 1 <= month <= 12:
            exchange = dated_future.group(1).lower()
            root = dated_future.group(2)
            contract = f"{root}-{dated_future.group(3)}{dated_future.group(4)}"
            return {
                "exchange": exchange,
                "ccxt": None,
                "symbol": contract,
                "stream_symbol": contract.lower(),
                "base": root,
                "quote": "USD",
            }

    continuous_future = _CONTINUOUS_FUTURE_PRODUCT_ID_PATTERN.fullmatch(product_id)
    if continuous_future:
        return {
            "exchange": continuous_future.group(1).lower(),
            "ccxt": None,
            "symbol": None,
            "base": continuous_future.group(2),
            "quote": "USD",
            "research_only": True,
        }

    raise ValueError(
        f"Cannot parse product_id: {product_id}. Expected "
        "EXCHANGE:BASEQUOTE-PERP, EXCHANGE:ROOT-YYYYMM, or "
        "EXCHANGE:ROOT-CONTINUOUS"
    )


def validate_product_id(product_id: str) -> str:
    """Validate and return one canonical product ID unchanged."""
    _parse_product_id(product_id)
    return product_id


def is_dated_future_product_id(product_id: str) -> bool:
    """Return whether a canonical product ID identifies an expiring contract."""
    validate_product_id(product_id)
    return _DATED_FUTURE_PRODUCT_ID_PATTERN.fullmatch(product_id) is not None


def is_research_only_product_id(product_id: str) -> bool:
    """Return whether a product identifies a non-executable research series."""
    validate_product_id(product_id)
    return _CONTINUOUS_FUTURE_PRODUCT_ID_PATTERN.fullmatch(product_id) is not None


def to_ccxt_symbol(product_id: str) -> str:
    """Convert product_id to CCXT symbol.

    Examples:
        >>> to_ccxt_symbol("BINANCE:BTCUSDT-PERP")
        'BTC/USDT:USDT'
        >>> to_ccxt_symbol("BYBIT:ETHUSDT-PERP")
        'ETH/USDT:USDT'
    """
    symbol = _parse_product_id(product_id)["ccxt"]
    if symbol is None:
        raise ValueError(f"CCXT symbol mapping is unavailable for {product_id}")
    return symbol


def to_rithmic_symbol(product_id: str) -> str:
    """Convert one canonical Rithmic dated future to its native contract symbol."""
    match = _DATED_FUTURE_PRODUCT_ID_PATTERN.fullmatch(product_id)
    if match is None or match.group(1) != "RITHMIC":
        raise ValueError(f"Rithmic symbol mapping is unavailable for {product_id}")
    month = int(match.group(4))
    month_code = "FGHJKMNQUVXZ"[month - 1]
    return f"{match.group(2)}{month_code}{match.group(3)[-1]}"


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
    if info.get("research_only"):
        raise ValueError(f"live stream mapping is unavailable for {product_id}")
    stream_symbol = info.get("stream_symbol") or f"{info['base']}{info['quote']}".lower()
    return f"stream:market:{info['exchange']}:{stream_symbol}:{timeframe}"


def resolve_exchange(product_id: str) -> tuple[str, str]:
    """Return (exchange_name, ccxt_symbol) tuple.

    Drop-in replacement for fetch_real_data.resolve_exchange().

    Examples:
        >>> resolve_exchange("BINANCE:BTCUSDT-PERP")
        ('binance', 'BTC/USDT:USDT')
    """
    info = _parse_product_id(product_id)
    if info["ccxt"] is None:
        raise ValueError(f"CCXT exchange resolution is unavailable for {product_id}")
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
    symbol = info.get("symbol") or info.get("ccxt")
    if symbol is None:
        raise ValueError(f"Instrument symbol mapping is unavailable for {product_id}")
    return InstrumentSpec(
        product_id=product_id,
        exchange=info["exchange"],
        symbol=symbol,
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
    *,
    precision_mode: PrecisionMode | None,
) -> InstrumentSpec:
    quantity_step = None
    price_tick = None
    min_notional = None
    min_quantity = None
    multiplier = None

    if market is not None:
        multiplier = _contract_multiplier_from_ccxt_market(market)
        limits = market.get("limits") or {}
        amount_limits = limits.get("amount") or {}
        cost_limits = limits.get("cost") or {}
        min_quantity = _decimal_or_none(amount_limits.get("min"))
        min_notional = _decimal_or_none(cost_limits.get("min"))
        precision = market.get("precision") or {}
        if isinstance(precision, dict):
            quantity_step = _step_from_precision(
                precision.get("amount"),
                precision_mode,
            )
            price_tick = _step_from_precision(
                precision.get("price"),
                precision_mode,
            )

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
        multiplier=multiplier,
    )


def _contract_multiplier_from_ccxt_market(market: dict[str, Any]) -> Decimal | None:
    is_contract = market.get("contract")
    if is_contract is False:
        return None
    if is_contract is not True:
        raise ValueError("CCXT market is missing an explicit contract classification")
    if market.get("linear") is not True or market.get("inverse") is True:
        raise ValueError("only linear CCXT contracts are supported")

    try:
        multiplier = Decimal(str(market.get("contractSize")))
    except (InvalidOperation, ValueError):
        multiplier = None
    if multiplier is None or not multiplier.is_finite() or multiplier <= 0:
        raise ValueError("linear CCXT contractSize must be positive")
    return multiplier


def quantize_order_values(
    *,
    quantity: Decimal,
    price: Decimal | None,
    side: str | None = None,
    order_type: str | None = None,
    trigger_price: Decimal | None = None,
    trailing_distance: Decimal | None = None,
    spec: InstrumentSpec,
) -> QuantizedOrder:
    if _DATED_FUTURE_PRODUCT_ID_PATTERN.fullmatch(spec.product_id):
        return _validate_dated_future_order_values(
            quantity=quantity,
            price=price,
            trigger_price=trigger_price,
            trailing_distance=trailing_distance,
            spec=spec,
        )

    quantized_quantity = _floor_to_step(quantity, spec.quantity_step)
    quantized_price = (
        _quantize_limit_price(price, spec.price_tick, side)
        if price is not None
        else None
    )
    quantized_trigger_price = (
        _quantize_trigger_price(
            trigger_price,
            spec.price_tick,
            side=side,
            order_type=order_type,
        )
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


def _validate_dated_future_order_values(
    *,
    quantity: Decimal,
    price: Decimal | None,
    trigger_price: Decimal | None,
    trailing_distance: Decimal | None,
    spec: InstrumentSpec,
) -> QuantizedOrder:
    if (
        spec.quantity_step is None
        or not spec.quantity_step.is_finite()
        or spec.quantity_step <= 0
    ):
        raise ValueError("futures_quantity_step_must_be_positive")
    if (
        not quantity.is_finite()
        or quantity <= 0
    ):
        raise ValueError(f"futures_quantity_must_be_positive: quantity={quantity}")
    if _floor_to_step(quantity, spec.quantity_step) != quantity:
        raise ValueError(
            "futures_quantity_off_step: "
            f"quantity={quantity} step={spec.quantity_step}"
        )

    has_price = any(
        value is not None for value in (price, trigger_price, trailing_distance)
    )
    if has_price and (
        spec.price_tick is None
        or not spec.price_tick.is_finite()
        or spec.price_tick <= 0
    ):
        raise ValueError("futures_price_tick_must_be_positive")

    for label, value in (
        ("price", price),
        ("trigger_price", trigger_price),
        ("trailing_distance", trailing_distance),
    ):
        if value is None:
            continue
        if not value.is_finite() or value <= 0:
            raise ValueError(f"{label}_must_be_positive: {label}={value}")
        _require_on_step(label, value, spec.price_tick)

    return QuantizedOrder(
        quantity=quantity,
        price=price,
        trigger_price=trigger_price,
        changed=False,
    )


def validate_min_notional(
    *,
    quantity: Decimal,
    price: Decimal | None,
    reference_price: Decimal | None = None,
    spec: InstrumentSpec,
) -> None:
    """Validate min notional using order price or an execution reference price.

    Reference price is an estimate for market orders; the exchange remains the
    final rejection point if the execution price moves through the threshold.
    """
    if quantity <= 0:
        raise ValueError(f"quantity_must_be_positive: quantity={quantity}")
    if spec.min_quantity is not None and quantity < spec.min_quantity:
        raise ValueError(
            f"quantity_below_min: quantity={quantity} min_quantity={spec.min_quantity}"
        )
    if spec.min_notional is None:
        return
    effective_price = price if price is not None else reference_price
    if effective_price is None:
        raise ValueError(
            "min_notional_unverifiable: market order without reference price, "
            f"min_notional={spec.min_notional}"
        )
    notional = quantity * effective_price
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


def _quantize_trigger_price(
    trigger_price: Decimal,
    tick: Decimal | None,
    *,
    side: str | None,
    order_type: str | None,
) -> Decimal:
    if tick is None or tick <= 0 or _floor_to_step(trigger_price, tick) == trigger_price:
        return trigger_price

    policy = _trigger_price_policy(order_type=order_type, side=side)
    if policy == TriggerPricePolicy.ROUND_DOWN:
        return _floor_to_step(trigger_price, tick)
    if policy == TriggerPricePolicy.ROUND_UP:
        return _ceil_to_step(trigger_price, tick)
    return _require_on_step("trigger_price", trigger_price, tick)


def _trigger_price_policy(
    *,
    order_type: str | None,
    side: str | None,
) -> TriggerPricePolicy:
    normalized_type = order_type.lower() if order_type is not None else None
    normalized_side = side.lower() if side is not None else None
    if normalized_type in {"stop_loss", "take_profit", "trailing_stop"}:
        if normalized_side == "buy":
            return TriggerPricePolicy.ROUND_DOWN
        if normalized_side == "sell":
            return TriggerPricePolicy.ROUND_UP
        raise ValueError(
            "trigger_price_off_tick_without_side: "
            f"order_type={order_type}"
        )
    return TriggerPricePolicy.REQUIRE_ON_TICK


def _step_from_precision(
    value: Any,
    precision_mode: PrecisionMode | None,
) -> Decimal | None:
    if value is None or value == "":
        return None

    text = str(value)
    decimal = Decimal(text)

    if precision_mode == PrecisionMode.TICK_SIZE:
        if decimal <= 0:
            logger.warning(
                "Ignoring non-positive TICK_SIZE precision value: value=%s",
                value,
            )
            return None
        return decimal

    if precision_mode == PrecisionMode.DECIMAL_PLACES:
        is_integer_text = "." not in text and "e" not in text.lower()
        if decimal == decimal.to_integral_value() and is_integer_text:
            return Decimal(1).scaleb(-int(decimal))
        logger.warning(
            "Ignoring non-integer DECIMAL_PLACES precision value: value=%s",
            value,
        )
        return None

    if precision_mode == PrecisionMode.SIGNIFICANT_DIGITS:
        return None

    logger.warning(
        "Ignoring precision value without supported precisionMode: value=%s precision_mode=%s",
        value,
        precision_mode,
    )
    return None


def _decimal_or_none(value: Any, fallback: Decimal | None = None) -> Decimal | None:
    if value is None or value == "":
        return fallback
    decimal = Decimal(str(value))
    if decimal <= 0:
        return fallback
    return decimal
