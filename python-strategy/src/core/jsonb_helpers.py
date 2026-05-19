"""
JSONB Decimal serialization helpers.

Python's Decimal cannot be JSON-serialized natively, so financial values
destined for PostgreSQL JSONB columns must be converted to strings (which
preserve full precision) and converted back when read.

These helpers enforce the FluxTrade Decimal-only rule at the JSONB boundary:
no float ever appears in monetary payloads, and round-tripping is lossless.
"""

from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


def decimal_to_jsonb_str(value: Decimal) -> str:
    """Convert a Decimal to a JSONB-safe string preserving full precision."""
    if not isinstance(value, Decimal):
        raise TypeError(f"expected Decimal, got {type(value).__name__}")
    return str(value)


def jsonb_to_decimal(value: str | None) -> Decimal | None:
    """Restore a Decimal from a JSONB string. None passes through; empty/invalid raises."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise TypeError(f"expected str or None, got {type(value).__name__}")
    if value == "":
        raise InvalidOperation("cannot convert empty string to Decimal")
    return Decimal(value)


def serialize_payload_with_decimals(payload: dict) -> dict:
    """Recursively convert all Decimal values inside a dict (or nested list/dict) to strings.

    Returns a new dict; the input is not mutated. Non-Decimal scalars
    (int, float, str, bool, None) are kept as-is.
    """
    if not isinstance(payload, dict):
        raise TypeError(f"expected dict, got {type(payload).__name__}")
    return _convert_for_serialization(payload)  # type: ignore[return-value]


def deserialize_payload_with_decimals(
    payload: dict, decimal_keys: set[str]
) -> dict:
    """Recursively convert string values at the given keys back into Decimal.

    Walks nested dicts and lists; any dict key matching `decimal_keys` whose
    value is a string is converted via `Decimal(value)`. Other fields are left
    untouched. Returns a new dict; the input is not mutated.
    """
    if not isinstance(payload, dict):
        raise TypeError(f"expected dict, got {type(payload).__name__}")
    return _convert_for_deserialization(payload, decimal_keys)  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _convert_for_serialization(node: Any) -> Any:
    if isinstance(node, Decimal):
        return str(node)
    if isinstance(node, dict):
        return {k: _convert_for_serialization(v) for k, v in node.items()}
    if isinstance(node, list):
        return [_convert_for_serialization(item) for item in node]
    return node


def _convert_for_deserialization(node: Any, decimal_keys: set[str], parent_key: str | None = None) -> Any:
    if isinstance(node, dict):
        return {
            k: _convert_for_deserialization(v, decimal_keys, parent_key=k)
            for k, v in node.items()
        }
    if isinstance(node, list):
        # List items inherit the parent key context so that e.g. a list of
        # Decimal strings under a tracked key still gets converted.
        return [
            _convert_for_deserialization(item, decimal_keys, parent_key=parent_key)
            for item in node
        ]
    if parent_key in decimal_keys and isinstance(node, str):
        return Decimal(node)
    return node
