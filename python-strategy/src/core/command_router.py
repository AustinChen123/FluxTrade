"""Redis pubsub command routing for strategy control."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional

from src.core.health_monitor import HealthMonitor
from src.core.strategy_registry import StrategyRegistry

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CommandResult:
    success: bool
    message: str
    data: Optional[dict] = None


class CommandRouter:
    """Parse and dispatch strategy control commands."""

    def __init__(
        self,
        registry: StrategyRegistry,
        state_manager: Any,
        health_monitor: HealthMonitor | None = None,
    ) -> None:
        self.registry = registry
        self.state_manager = state_manager
        self.health_monitor = health_monitor

    def handle(self, message: dict) -> CommandResult:
        """Dispatch a command message to a handler."""
        if not isinstance(message, dict):
            return CommandResult(False, "Malformed command message")

        command = message.get("command") or message.get("cmd")
        if not command:
            return CommandResult(False, "Missing command")

        command = str(command).upper()
        params = message.get("params") or {}
        if not isinstance(params, dict):
            return CommandResult(False, "Malformed command params")

        strategy_id = params.get("id") or params.get("strategy_id")
        if strategy_id is None:
            strategy_id = message.get("id") or message.get("strategy_id")

        handlers = {
            "START": self._handle_start,
            "STOP": self._handle_stop,
            "RESUME": self._handle_resume,
            "FORCE_RECOVER": self._handle_force_recover,
            "RELOAD": self._handle_reload,
            "LIST": self._handle_list,
            "HEALTH_CHECK": self._handle_health_check,
        }
        handler = handlers.get(command)
        if handler is None:
            return CommandResult(False, f"Unknown command: {command}")

        if command in {"START", "STOP", "RESUME", "FORCE_RECOVER", "RELOAD"}:
            if not strategy_id:
                return CommandResult(False, f"{command} requires strategy_id")
            return handler(str(strategy_id), params, message)
        return handler()

    def _handle_start(self, strategy_id: str, params: dict, message: dict) -> CommandResult:
        self.state_manager.transition_to_running(strategy_id)
        return CommandResult(True, f"Started strategy {strategy_id}")

    def _handle_stop(self, strategy_id: str, params: dict, message: dict) -> CommandResult:
        reason = params.get("reason") or message.get("reason")
        self.state_manager.transition_to_stopped(
            strategy_id,
            actor="operator",
            reason=reason,
        )
        return CommandResult(True, f"Stopped strategy {strategy_id}")

    def _handle_resume(self, strategy_id: str, params: dict, message: dict) -> CommandResult:
        reason = params.get("reason") or message.get("reason")
        self.state_manager.transition_to_running(
            strategy_id,
            actor="operator",
            force=True,
            reason=reason,
        )
        return CommandResult(True, f"Resumed strategy {strategy_id}")

    def _handle_force_recover(self, strategy_id: str, params: dict, message: dict) -> CommandResult:
        reason = params.get("reason") or message.get("reason")
        self.state_manager.transition_to_running(
            strategy_id,
            actor="operator",
            force=True,
            reason=reason,
        )
        return CommandResult(True, f"Force recovered strategy {strategy_id}")

    def _handle_reload(self, strategy_id: str, params: dict, message: dict) -> CommandResult:
        logger.warning("Strategy reload is not implemented yet: %s", strategy_id)
        return CommandResult(
            True,
            f"Reload queued for later implementation: {strategy_id}",
        )

    def _handle_list(self) -> CommandResult:
        strategies = [
            {
                "strategy_id": strategy.strategy_id,
                "product_id": strategy.product_id,
                "timeframe": strategy.requirements.timeframe,
            }
            for strategy in self.registry.list_active()
        ]
        return CommandResult(True, "Listed active strategies", {"strategies": strategies})

    def _handle_health_check(self) -> CommandResult:
        if self.health_monitor is None:
            return CommandResult(True, "Health monitor unavailable", {"healthy": {}})

        healthy = {
            strategy.strategy_id: self.health_monitor.is_healthy(strategy.strategy_id)
            for strategy in self.registry.list_active()
        }
        return CommandResult(True, "Health check complete", {"healthy": healthy})
