"""Strategy heartbeat tracking and health metrics."""

from __future__ import annotations

import time
from typing import Any

from src.core.strategy_registry import StrategyRegistry


class HealthMonitor:
    """Track per-strategy heartbeats."""

    def __init__(
        self,
        registry: StrategyRegistry,
        timeout_threshold: float = 5.0,
        redis_client: Any | None = None,
    ) -> None:
        self.registry = registry
        self.timeout_threshold = timeout_threshold
        self.redis_client = redis_client
        self._heartbeats: dict[str, float] = {}
        self._started_at: dict[str, float] = {}

    def update_heartbeat(self, strategy_id: str) -> None:
        """Record a heartbeat timestamp for a strategy."""
        now = time.time()
        self._heartbeats[strategy_id] = now
        self._started_at.setdefault(strategy_id, now)

        if self.redis_client is not None:
            ttl = max(1, int(self.timeout_threshold))
            self.redis_client.setex(f"heartbeat:strategy:{strategy_id}", ttl, "1")

    def is_healthy(self, strategy_id: str) -> bool:
        """Return whether a strategy has a recent heartbeat."""
        last_seen = self._heartbeats.get(strategy_id)
        if last_seen is None:
            return False
        return (time.time() - last_seen) < self.timeout_threshold

    def get_uptime(self, strategy_id: str) -> float:
        """Return seconds since first heartbeat, or 0 for unknown strategies."""
        started_at = self._started_at.get(strategy_id)
        if started_at is None:
            return 0.0
        return max(0.0, time.time() - started_at)

    def expose_prometheus_metrics(self) -> str:
        """Render strategy health metrics in Prometheus text format."""
        lines: list[str] = []
        for strategy in self.registry.list_active():
            strategy_id = strategy.strategy_id
            labels = f'strategy_id="{strategy_id}"'
            lines.append(
                "fluxtrade_strategy_uptime_seconds"
                f"{{{labels}}} {self.get_uptime(strategy_id):.6f}"
            )
            lines.append(
                "fluxtrade_strategy_healthy"
                f"{{{labels}}} {1 if self.is_healthy(strategy_id) else 0}"
            )
        return "\n".join(lines)
