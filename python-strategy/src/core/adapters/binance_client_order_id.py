"""Binance-owned client-order-id formatting."""

import hashlib
import re

from src.core.client_order_id import parse_client_order_id


MAX_BINANCE_CLIENT_ORDER_ID_LENGTH = 36
_BINANCE_SAFE_CHARS_RE = re.compile(r"[^A-Za-z0-9_-]")


def to_binance_client_order_id(client_order_id: str) -> str:
    """Return the existing deterministic Binance-safe client order ID."""
    parts = parse_client_order_id(client_order_id)
    strategy_prefix = _BINANCE_SAFE_CHARS_RE.sub("", parts.strategy_id)[:8]
    strategy_prefix = strategy_prefix or "strategy"
    timestamp_suffix = _base36(parts.ts_ns)[-10:]
    digest = hashlib.blake2s(client_order_id.encode("utf-8"), digest_size=8).hexdigest()
    exchange_id = f"{strategy_prefix}-{timestamp_suffix}-{digest}"
    return exchange_id[:MAX_BINANCE_CLIENT_ORDER_ID_LENGTH]


def _base36(value: int) -> str:
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if value == 0:
        return "0"
    result = ""
    while value:
        value, remainder = divmod(value, 36)
        result = alphabet[remainder] + result
    return result
