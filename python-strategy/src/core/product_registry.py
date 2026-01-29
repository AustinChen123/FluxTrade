"""Centralized product ID ↔ exchange symbol mapping.

Replaces ad-hoc _map_symbol() in ExchangeAdapter and PRODUCT_TO_CCXT
in fetch_real_data.py with a single registry.

Product ID format: EXCHANGE:BASEQUOTE-PERP
  e.g. BINANCE:BTCUSDT-PERP, BYBIT:ETHUSDT-PERP
"""

import re


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
