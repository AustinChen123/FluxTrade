"""Pure classification of ordered same-snapshot rows; CLEAR is not ABSENT proof."""

from collections.abc import Mapping
from typing import Literal

from .bootstrap_seed import BootstrapKey
from .decision_application import MarketDataDecisionBatch, MAX_DECISION_BATCH_BYTES
from .read_types import _integer

MAX_HISTORY_BATCH_ROWS = 32
_SCOPE = ("environment", "execution_scope_id", "product_id", "timeframe")
_FIELDS = frozenset(_SCOPE) | {
    "bar_start_ms",
    "contract_version",
    "participant_count",
    "canonical_payload",
    "batch_digest",
}


def classify_batch_history(
    rows: tuple[Mapping[str, object], ...],
    key: BootstrapKey,
) -> Literal["CLEAR", "UNKNOWN"]:
    if type(rows) is not tuple or type(key) is not BootstrapKey:
        return "UNKNOWN"
    if len(rows) > MAX_HISTORY_BATCH_ROWS:
        return "UNKNOWN"
    previous = -1
    try:
        for row in rows:
            if set(row) != _FIELDS:
                return "UNKNOWN"
            if any(
                type(row[name]) is not str or row[name] != getattr(key, name)
                for name in _SCOPE
            ):
                return "UNKNOWN"
            start = row["bar_start_ms"]
            if type(start) is not int or start <= previous:
                return "UNKNOWN"
            _integer(start)
            previous = start
            if (
                type(row["contract_version"]) is not int
                or row["contract_version"] != 1
                or type(row["participant_count"]) is not int
                or type(row["batch_digest"]) is not str
            ):
                return "UNKNOWN"
            raw = row["canonical_payload"]
            if type(raw) is memoryview:
                if not 1 <= raw.nbytes <= MAX_DECISION_BATCH_BYTES:
                    return "UNKNOWN"
                raw = raw.tobytes()
            if type(raw) is not bytes or not 1 <= len(raw) <= MAX_DECISION_BATCH_BYTES:
                return "UNKNOWN"
            batch = MarketDataDecisionBatch.from_canonical_bytes(raw)
            if (
                any(
                    getattr(batch, name) != row[name]
                    for name in (*_SCOPE, "bar_start_ms")
                )
                or batch.canonical_bytes != raw
                or batch.digest != row["batch_digest"]
                or len(batch.participants) != row["participant_count"]
            ):
                return "UNKNOWN"
            if any(
                participant.strategy_id == key.strategy_id
                for participant in batch.participants
            ):
                return "UNKNOWN"
    except (ValueError, TypeError, KeyError, OverflowError):
        return "UNKNOWN"
    return "CLEAR"
