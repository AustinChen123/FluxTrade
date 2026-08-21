import logging
import traceback
from typing import Callable, Protocol


class _CommandResult(Protocol):
    @property
    def success(self) -> bool: ...

    @property
    def message(self) -> str: ...


class _CommandRouter(Protocol):
    def handle(self, message: dict) -> _CommandResult: ...


class _OpsCommandService(Protocol):
    def handle_kill_switch(self, params: dict) -> None: ...

    def handle_clear_kill_switch(self, params: dict) -> None: ...


_STATE_COMMANDS = frozenset({"START", "STOP", "RESUME", "FORCE_RECOVER"})


class StrategyCommandDispatchService:
    """Coordinate one venue-neutral strategy-control command."""

    def __init__(
        self,
        *,
        assert_leadership: Callable[[], None],
        scan_strategies: Callable[[], object],
        test_run_strategy: Callable[[str, int], object],
        ops_command_service: Callable[[], _OpsCommandService],
        assert_strategy_command_allowed: Callable[..., None],
        claim_strategy_command_operation: Callable[..., bool],
        mark_strategy_command_operation_completed: Callable[..., None],
        command_router: Callable[[], _CommandRouter],
        event_logger: Callable[[], logging.Logger],
    ) -> None:
        self._assert_leadership = assert_leadership
        self._scan_strategies = scan_strategies
        self._test_run_strategy = test_run_strategy
        self._ops_command_service = ops_command_service
        self._assert_strategy_command_allowed = assert_strategy_command_allowed
        self._claim_strategy_command_operation = claim_strategy_command_operation
        self._mark_strategy_command_operation_completed = (
            mark_strategy_command_operation_completed
        )
        self._command_router = command_router
        self._event_logger = event_logger

    def dispatch(self, data: object) -> None:
        self._assert_leadership()
        event_logger = self._event_logger()
        if not isinstance(data, dict):
            event_logger.error("Malformed command payload")
            return
        cmd = str(data.get("command") or data.get("cmd") or "").upper()
        params = data.get("params") or {}
        if not isinstance(params, dict):
            params = {}

        event_logger.info("Received Command: %s with params %s", cmd, params)

        try:
            if cmd == "SCAN":
                self._scan_strategies()
                return
            if cmd == "TEST_RUN":
                strategy_id = params.get("id")
                days = params.get("days", 1)
                if (
                    not isinstance(strategy_id, str)
                    or not strategy_id.strip()
                    or type(days) is not int
                ):
                    event_logger.error("Malformed TEST_RUN command")
                    return
                self._test_run_strategy(strategy_id, days)
                return
            if cmd == "KILL_SWITCH":
                self._ops_command_service().handle_kill_switch(params)
                return
            if cmd == "CLEAR_KILL_SWITCH":
                self._ops_command_service().handle_clear_kill_switch(params)
                return

            idempotency_key: object = None
            actor = "operator"
            expected_version = params.get("expected_version")
            if cmd in _STATE_COMMANDS and (
                expected_version is None
                or (
                    isinstance(expected_version, int)
                    and not isinstance(expected_version, bool)
                )
            ):
                self._assert_strategy_command_allowed(
                    strategy_id=str(
                        params.get("id")
                        or params.get("strategy_id")
                        or data.get("id")
                        or data.get("strategy_id")
                        or ""
                    ),
                    command=cmd,
                    expected_version=expected_version,
                )
            if cmd in _STATE_COMMANDS:
                idempotency_key = params.get("idempotency_key")
                actor = str(params.get("actor", "operator"))
                if isinstance(
                    idempotency_key, str
                ) and not self._claim_strategy_command_operation(
                    actor=actor,
                    idempotency_key=idempotency_key,
                ):
                    event_logger.info(
                        "Skipping duplicate strategy command for actor %s",
                        actor,
                    )
                    return
            result = self._command_router().handle(data)
            if cmd in _STATE_COMMANDS and isinstance(idempotency_key, str):
                self._mark_strategy_command_operation_completed(
                    actor=actor,
                    idempotency_key=idempotency_key,
                )
            if result.success:
                event_logger.info("Command %s succeeded: %s", cmd, result.message)
            else:
                event_logger.warning("Command %s failed: %s", cmd, result.message)
        except Exception as error:
            event_logger.error(
                "Error executing command %s: %s\n%s",
                cmd,
                error,
                traceback.format_exc(),
            )
