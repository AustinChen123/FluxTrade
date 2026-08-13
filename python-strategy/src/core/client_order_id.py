"""Venue-neutral client order ID generation and validation."""

from __future__ import annotations

import hashlib
import re
import threading
import time
from dataclasses import dataclass


MAX_CANONICAL_LENGTH = 128
_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_.:]+$")
_STRATEGY_COMPONENT_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
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


def market_signal_client_order_id(
    strategy_id: str,
    product_id: str,
    event_scope: str,
    event_timestamp: int,
    action: str,
    ordinal: int,
) -> str:
    """Return a replay-stable ID for one signal emitted by a market event."""
    _validate_component("strategy_id", strategy_id)
    _validate_component("action", action)
    if ordinal < 0:
        raise ValueError("ordinal must be non-negative")
    identity = (
        f"{product_id}\0{event_scope}\0{event_timestamp}\0"
        f"{strategy_id}\0{action}\0{ordinal}"
    )
    digest = hashlib.blake2b(identity.encode(), digest_size=8).digest()
    stable_number = int.from_bytes(digest, "big")
    client_order_id = f"{strategy_id}-market-{action}_{ordinal}-{stable_number}"
    parse_client_order_id(client_order_id)
    return client_order_id


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


def linked_client_order_id(client_order_id: str, action: str) -> str:
    """Return a deterministic sibling ID in the same execution identity."""
    parts = parse_client_order_id(client_order_id)
    _validate_component("action", action)
    linked = f"{parts.strategy_id}-{parts.instance_id}-{action}-{parts.ts_ns}"
    parse_client_order_id(linked)
    return linked


def is_valid_client_order_id(client_order_id: str) -> bool:
    """Return True when the ID matches FluxTrade's canonical format."""
    try:
        parse_client_order_id(client_order_id)
    except ValueError:
        return False
    return True


def _validate_component(name: str, value: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{name} must be a non-empty string")
    if name != "strategy_id" and "-" in value:
        raise ValueError(f"{name} cannot contain '-'")
    pattern = _STRATEGY_COMPONENT_RE if name == "strategy_id" else _COMPONENT_RE
    if not pattern.match(value):
        raise ValueError(f"{name} contains unsupported characters")


def _next_ts_ns(clock_ns: callable) -> int:
    global _last_ts_ns
    with _lock:
        ts_ns = int(clock_ns())
        if ts_ns <= _last_ts_ns:
            ts_ns = _last_ts_ns + 1
        _last_ts_ns = ts_ns
        return ts_ns
