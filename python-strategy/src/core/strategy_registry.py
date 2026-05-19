"""Thread-safe strategy instance registry."""

from __future__ import annotations

import threading
from typing import Optional

from src.strategies.base import BaseStrategy


class StrategyRegistry:
    """Manage live strategy instances by strategy ID."""

    def __init__(self) -> None:
        self._strategies: dict[str, BaseStrategy] = {}
        self._lock = threading.Lock()

    def register(self, strategy: BaseStrategy) -> None:
        """Register or replace a strategy instance."""
        with self._lock:
            self._strategies[strategy.strategy_id] = strategy

    def unregister(self, strategy_id: str) -> None:
        """Remove a strategy if present."""
        with self._lock:
            self._strategies.pop(strategy_id, None)

    def get(self, strategy_id: str) -> Optional[BaseStrategy]:
        """Return a strategy by ID, or None when absent."""
        with self._lock:
            return self._strategies.get(strategy_id)

    def list_active(self) -> list[BaseStrategy]:
        """Return a snapshot of all registered strategies."""
        with self._lock:
            return list(self._strategies.values())

    def list_by_timeframe(self, timeframe: str) -> list[BaseStrategy]:
        """Return registered strategies whose requirements match timeframe."""
        with self._lock:
            return [
                strategy
                for strategy in self._strategies.values()
                if strategy.requirements.timeframe == timeframe
            ]

    def reload(self, strategy_id: str) -> None:
        """Reload is wired in a later phase."""
        raise NotImplementedError(
            f"Strategy reload is not implemented yet: {strategy_id}"
        )
