from __future__ import annotations

from typing import Any, Protocol

from src.control_plane.models import StrategyCommandRequest


class CommandRouterLike(Protocol):
    def handle(self, message: dict) -> Any:
        ...


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
    ) -> dict[str, Any]:
        params = {**request.params, "strategy_id": strategy_id}
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
        return {
            "success": bool(result.success),
            "message": str(result.message),
            "data": result.data or {},
        }
