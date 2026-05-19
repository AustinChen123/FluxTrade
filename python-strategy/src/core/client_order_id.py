"""Client order ID generation and exchange-format conversion."""

from __future__ import annotations

import hashlib
import re
import threading
import time
from dataclasses import dataclass


MAX_CANONICAL_LENGTH = 128
MAX_BINANCE_LENGTH = 36

_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_.:]+$")
_EXCHANGE_CHARS_RE = re.compile(r"[^A-Za-z0-9_-]")
_last_ts_ns = 0
_lock = threading.Lock()


@dataclass(frozen=True)
class ClientOrderIdParts:
    strategy_id: str
    instance_id: str
    action: str
    ts_ns: int


def generate_client_order_id(
    strategy_id: str,
    instance_id: str,
    action: str,
    *,
    clock_ns: callable | None = None,
) -> str:
    """Generate a canonical client order ID: strategy-instance-action-ts_ns."""
    _validate_component("strategy_id", strategy_id)
    _validate_component("instance_id", instance_id)
    _validate_component("action", action)

    ts_ns = _next_ts_ns(clock_ns or time.time_ns)
    coid = f"{strategy_id}-{instance_id}-{action}-{ts_ns}"
    if len(coid) > MAX_CANONICAL_LENGTH:
        raise ValueError("client_order_id exceeds 128 characters")
    return coid


def parse_client_order_id(client_order_id: str) -> ClientOrderIdParts:
    """Parse and validate a canonical client order ID."""
    if not isinstance(client_order_id, str) or not client_order_id:
        raise ValueError("client_order_id must be a non-empty string")
    if len(client_order_id) > MAX_CANONICAL_LENGTH:
        raise ValueError("client_order_id exceeds 128 characters")

    parts = client_order_id.split("-")
    if len(parts) < 4:
        raise ValueError("client_order_id must have at least 4 '-' separated parts")
    strategy_id = "-".join(parts[:-3])
    instance_id, action, ts_ns_raw = parts[-3:]
    _validate_component("strategy_id", strategy_id)
    _validate_component("instance_id", instance_id)
    _validate_component("action", action)
    if not ts_ns_raw.isdigit():
        raise ValueError("client_order_id timestamp must be numeric nanoseconds")
    return ClientOrderIdParts(
        strategy_id=strategy_id,
        instance_id=instance_id,
        action=action,
        ts_ns=int(ts_ns_raw),
    )


def is_valid_client_order_id(client_order_id: str) -> bool:
    """Return True when the ID matches FluxTrade's canonical format."""
    try:
        parse_client_order_id(client_order_id)
    except ValueError:
        return False
    return True


def to_exchange_format(client_order_id: str, exchange: str) -> str:
    """Convert a canonical client order ID to a deterministic exchange-safe ID."""
    parts = parse_client_order_id(client_order_id)
    exchange_name = exchange.lower()
    if exchange_name == "binance":
        strategy_prefix = _exchange_safe(parts.strategy_id)[:8] or "strategy"
        ts_suffix = _base36(parts.ts_ns)[-10:]
        digest = hashlib.blake2s(client_order_id.encode("utf-8"), digest_size=8).hexdigest()
        exchange_id = f"{strategy_prefix}-{ts_suffix}-{digest}"
        return exchange_id[:MAX_BINANCE_LENGTH]
    return client_order_id if len(client_order_id) <= MAX_CANONICAL_LENGTH else _fallback_exchange_id(client_order_id)


def _validate_component(name: str, value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    if "-" in value:
        raise ValueError(f"{name} cannot contain '-'")
    if not _COMPONENT_RE.match(value):
        raise ValueError(f"{name} contains unsupported characters")


def _next_ts_ns(clock_ns: callable) -> int:
    global _last_ts_ns
    with _lock:
        ts_ns = int(clock_ns())
        if ts_ns <= _last_ts_ns:
            ts_ns = _last_ts_ns + 1
        _last_ts_ns = ts_ns
        return ts_ns


def _base36(value: int) -> str:
    alphabet = "0123456789abcdefghijklmnopqrstuvwxyz"
    if value == 0:
        return "0"
    result = ""
    while value:
        value, remainder = divmod(value, 36)
        result = alphabet[remainder] + result
    return result


def _exchange_safe(value: str) -> str:
    return _EXCHANGE_CHARS_RE.sub("", value)


def _fallback_exchange_id(client_order_id: str) -> str:
    digest = hashlib.blake2s(client_order_id.encode("utf-8"), digest_size=8).hexdigest()
    return f"ft-{digest}"
