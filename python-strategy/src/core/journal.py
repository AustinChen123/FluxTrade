"""Strategy Trade Journal — structured event recording for backtest analysis.

Strategies log decisions (entry reasons, skip reasons, structure context) via
``self.journal.log(tag, data)``.  The system layer auto-logs fill events
(SL hit, TP hit, trailing move) so strategies don't need to track exits.

Events are stored in memory during a backtest run and can be exported to
JSON-lines for post-run analysis.
"""

import json
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class JournalEntry:
    """Single journal event."""

    timestamp: int  # unix ms
    tag: str  # e.g. "entry", "sl_hit", "skip", "structure"
    data: Dict[str, Any]
    trade_id: Optional[str] = None


class StrategyJournal:
    """Append-only structured event log attached to a strategy.

    Usage from a strategy::

        self.journal.log("entry", {"side": "LONG", "reason": "bos_confirm"})
        self.journal.log("skip", {"reason": "rr_below_threshold", "rr": 0.8})
    """

    def __init__(self, strategy_id: str = "") -> None:
        self.strategy_id = strategy_id
        self._entries: List[JournalEntry] = []

    # ── Recording ────────────────────────────────────────────────

    def log(
        self,
        tag: str,
        data: Dict[str, Any],
        *,
        timestamp: int = 0,
        trade_id: Optional[str] = None,
    ) -> None:
        """Append a structured event."""
        self._entries.append(
            JournalEntry(
                timestamp=timestamp,
                tag=tag,
                data=data,
                trade_id=trade_id,
            )
        )

    # ── Querying ─────────────────────────────────────────────────

    def entries(
        self,
        *,
        tag: Optional[str] = None,
        trade_id: Optional[str] = None,
        start: Optional[int] = None,
        end: Optional[int] = None,
    ) -> List[JournalEntry]:
        """Return entries matching optional filters."""
        result = self._entries
        if tag is not None:
            result = [e for e in result if e.tag == tag]
        if trade_id is not None:
            result = [e for e in result if e.trade_id == trade_id]
        if start is not None:
            result = [e for e in result if e.timestamp >= start]
        if end is not None:
            result = [e for e in result if e.timestamp <= end]
        return result

    @property
    def tags(self) -> List[str]:
        """Return sorted unique tags."""
        return sorted({e.tag for e in self._entries})

    def __len__(self) -> int:
        return len(self._entries)

    # ── Export ────────────────────────────────────────────────────

    def to_jsonl(self) -> str:
        """Export all entries as JSON-lines string."""
        lines: List[str] = []
        for e in self._entries:
            obj = {
                "strategy_id": self.strategy_id,
                "timestamp": e.timestamp,
                "tag": e.tag,
                "data": e.data,
            }
            if e.trade_id is not None:
                obj["trade_id"] = e.trade_id
            lines.append(json.dumps(obj, default=str))
        return "\n".join(lines)

    def to_dicts(self) -> List[Dict[str, Any]]:
        """Export all entries as list of dicts (for programmatic use)."""
        result: List[Dict[str, Any]] = []
        for e in self._entries:
            obj: Dict[str, Any] = {
                "strategy_id": self.strategy_id,
                "timestamp": e.timestamp,
                "tag": e.tag,
                "data": e.data,
            }
            if e.trade_id is not None:
                obj["trade_id"] = e.trade_id
            result.append(obj)
        return result

    def clear(self) -> None:
        """Remove all entries."""
        self._entries.clear()
