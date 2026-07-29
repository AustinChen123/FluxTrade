from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Protocol

from src.control_plane.models import StrategyCommandRequest
from src.control_plane.strategy_state_query import StrategyStateQueryService


STRATEGY_COMMAND_CHANNEL = "cmd:strategy:control"


@dataclass(frozen=True, slots=True)
class StrategyCommandResult:
    success: bool
    message: str
    data: dict[str, Any] | None = None
    accepted: bool = False


class StrategyControlUnavailable(RuntimeError):
    """Raised when the live strategy engine or its read model is unavailable."""

    def __init__(
        self,
        message: str,
        *,
        code: str = "strategy_control_unavailable",
    ) -> None:
        super().__init__(message)
        self.code = code


class CommandRouterLike(Protocol):
    def handle(self, message: dict) -> Any:
        ...


class RedisStrategyCommandRouter:
    """Cross-process strategy command/query adapter for the control plane."""

    def __init__(
        self,
        redis_client: Any,
        state_query: StrategyStateQueryService,
    ) -> None:
        self._redis_client = redis_client
        self._state_query = state_query

    def handle(self, message: dict) -> StrategyCommandResult:
        try:
            command = str(message["command"]).upper()
            if command == "LIST":
                strategies, total = self._state_query.list_states(
                    status="ACTIVE",
                    limit=None,
                )
                return StrategyCommandResult(
                    True,
                    "Listed active strategies",
                    {"strategies": strategies, "total": total},
                )
            if command == "HEALTH_CHECK":
                strategies, _ = self._state_query.list_states(
                    status="ACTIVE",
                    limit=None,
                )
                healthy = {
                    strategy["strategy_id"]: bool(
                        self._redis_client.exists(
                            f"heartbeat:strategy:{strategy['strategy_id']}"
                        )
                    )
                    for strategy in strategies
                }
                return StrategyCommandResult(
                    True,
                    "Health check complete",
                    {"healthy": healthy},
                )

            subscribers = self._redis_client.publish(
                STRATEGY_COMMAND_CHANNEL,
                json.dumps(message, separators=(",", ":")),
            )
        except Exception as exc:
            raise StrategyControlUnavailable(
                "Strategy control backend unavailable"
            ) from exc
        if subscribers == 0:
            raise StrategyControlUnavailable(
                "Strategy engine listener unavailable",
                code="strategy_engine_listener_unavailable",
            )
        return StrategyCommandResult(
            True,
            f"{command} command accepted",
            accepted=True,
        )


class StrategyControlService:
    """Control-plane facade over the existing strategy CommandRouter."""

    def __init__(self, command_router: CommandRouterLike) -> None:
        self._command_router = command_router

    def list_strategies(self) -> dict[str, Any]:
        return self._result_payload(self._command_router.handle({"command": "LIST"}))

    def health(self) -> dict[str, Any]:
        return self._result_payload(
            self._command_router.handle({"command": "HEALTH_CHECK"})
        )

    def submit_command(
        self,
        strategy_id: str,
        request: StrategyCommandRequest,
        *,
        actor: str,
        idempotency_key: str,
    ) -> dict[str, Any]:
        params = {
            **request.params,
            "strategy_id": strategy_id,
            "actor": actor,
            "expected_version": request.expected_version,
            "idempotency_key": idempotency_key,
        }
        if request.reason is not None:
            params["reason"] = request.reason
        message = {
            "command": request.command,
            "strategy_id": strategy_id,
            "params": params,
        }
        return self._result_payload(self._command_router.handle(message))

    @staticmethod
    def _result_payload(result: Any) -> dict[str, Any]:
        payload = {
            "success": bool(result.success),
            "message": str(result.message),
            "data": result.data or {},
        }
        if getattr(result, "accepted", False):
            payload["accepted"] = True
        return payload
