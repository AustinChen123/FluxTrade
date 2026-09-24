"""Redis pubsub command routing for strategy control."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from src.core.health_monitor import HealthMonitor
from src.core.strategy_registry import StrategyRegistry

logger = logging.getLogger(__name__)


class StrategyStartDisposition(Enum):
    ACTIVE = "ACTIVE"
    WAITING_FOR_CUTOVER = "WAITING_FOR_CUTOVER"


@dataclass(frozen=True)
class CommandResult:
    success: bool
    message: str
    data: Optional[dict] = None
    completed: bool = True


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
            expected_version = params.get("expected_version")
            if expected_version is not None and (
                isinstance(expected_version, bool)
                or not isinstance(expected_version, int)
                or expected_version < 0
            ):
                return CommandResult(
                    False, "expected_version must be a non-negative integer"
                )
            return handler(str(strategy_id), params, message)
        return handler()

    @staticmethod
    def _expected_version_kwargs(params: dict) -> dict:
        expected_version = params.get("expected_version")
        return (
            {} if expected_version is None else {"expected_version": expected_version}
        )

    @staticmethod
    def _activation_kwargs(params: dict, command: str) -> dict:
        key = params.get("idempotency_key")
        return (
            {"activation_command": command, "idempotency_key": key}
            if type(key) is str
            else {}
        )

    def _handle_start(
        self, strategy_id: str, params: dict, message: dict
    ) -> CommandResult:
        actor = params.get("actor", "operator")
        reason = params.get("reason") or message.get("reason")
        disposition = self.state_manager.transition_to_running(
            strategy_id,
            actor=actor,
            reason=reason,
            **self._expected_version_kwargs(params),
            **self._activation_kwargs(params, "START"),
        )
        return self._start_result(disposition, strategy_id, "Started")

    def _handle_stop(
        self, strategy_id: str, params: dict, message: dict
    ) -> CommandResult:
        actor = params.get("actor", "operator")
        reason = params.get("reason") or message.get("reason")
        self.state_manager.transition_to_stopped(
            strategy_id,
            actor=actor,
            reason=reason,
            **self._expected_version_kwargs(params),
        )
        return CommandResult(True, f"Stopped strategy {strategy_id}")

    def _handle_resume(
        self, strategy_id: str, params: dict, message: dict
    ) -> CommandResult:
        actor = params.get("actor", "operator")
        reason = params.get("reason") or message.get("reason")
        disposition = self.state_manager.transition_to_running(
            strategy_id,
            actor=actor,
            force=True,
            reason=reason,
            **self._expected_version_kwargs(params),
            **self._activation_kwargs(params, "RESUME"),
        )
        return self._start_result(disposition, strategy_id, "Resumed")

    def _handle_force_recover(
        self, strategy_id: str, params: dict, message: dict
    ) -> CommandResult:
        actor = params.get("actor", "operator")
        reason = params.get("reason") or message.get("reason")
        disposition = self.state_manager.transition_to_running(
            strategy_id,
            actor=actor,
            force=True,
            reason=reason,
            **self._expected_version_kwargs(params),
            **self._activation_kwargs(params, "FORCE_RECOVER"),
        )
        return self._start_result(disposition, strategy_id, "Force recovered")

    @staticmethod
    def _start_result(
        disposition: object, strategy_id: str, verb: str
    ) -> CommandResult:
        if disposition is None or disposition is StrategyStartDisposition.ACTIVE:
            return CommandResult(True, f"{verb} strategy {strategy_id}")
        if disposition is StrategyStartDisposition.WAITING_FOR_CUTOVER:
            return CommandResult(
                True,
                f"Accepted strategy {strategy_id}; waiting for cutover",
                completed=False,
            )
        raise ValueError("invalid strategy start disposition")

    def _handle_reload(
        self, strategy_id: str, params: dict, message: dict
    ) -> CommandResult:
        logger.warning("Strategy reload is not implemented yet: %s", strategy_id)
        return CommandResult(
            False,
            f"Strategy reload is not implemented: {strategy_id}",
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
        return CommandResult(
            True, "Listed active strategies", {"strategies": strategies}
        )

    def _handle_health_check(self) -> CommandResult:
        if self.health_monitor is None:
            return CommandResult(True, "Health monitor unavailable", {"healthy": {}})

        healthy = {
            strategy.strategy_id: self.health_monitor.is_healthy(strategy.strategy_id)
            for strategy in self.registry.list_active()
        }
        return CommandResult(True, "Health check complete", {"healthy": healthy})
