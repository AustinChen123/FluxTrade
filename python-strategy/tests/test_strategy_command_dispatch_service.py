from unittest.mock import MagicMock

import pytest

from src.core.command_router import CommandResult
from src.core.strategy_command_dispatch_service import StrategyCommandDispatchService


def _service(events: list[str]):
    logger = MagicMock()
    router = MagicMock()
    router.handle.side_effect = lambda data: events.append("route") or CommandResult(
        True, "ok"
    )
    ops = MagicMock()

    def action(name: str, result=None):
        def run(*_args, **_kwargs):
            events.append(name)
            return result

        return run

    service = StrategyCommandDispatchService(
        assert_leadership=action("leadership"),
        scan_strategies=action("scan"),
        test_run_strategy=lambda strategy_id, days: events.append(
            f"test-run:{strategy_id}:{days}"
        ),
        ops_command_service=lambda: ops,
        assert_strategy_command_allowed=lambda **kwargs: events.append(
            "validate:"
            f"{kwargs['strategy_id']}:{kwargs['command']}:{kwargs['expected_version']}"
        ),
        claim_strategy_command_operation=lambda **_: events.append("claim") or True,
        mark_strategy_command_operation_completed=lambda **_: events.append("complete"),
        command_router=lambda: router,
        event_logger=lambda: logger,
    )
    return service, router, ops, logger


@pytest.mark.parametrize("payload", (None, [], "SCAN", 1))
def test_non_mapping_payload_is_rejected_after_leadership(payload: object) -> None:
    events: list[str] = []
    service, router, ops, logger = _service(events)

    service.dispatch(payload)

    assert events == ["leadership"]
    logger.error.assert_called_once_with("Malformed command payload")
    router.handle.assert_not_called()
    ops.handle_kill_switch.assert_not_called()


def test_scan_and_test_run_preserve_direct_routing_and_validation() -> None:
    events: list[str] = []
    service, router, ops, _logger = _service(events)

    service.dispatch({"command": "scan", "params": "ignored"})
    service.dispatch({"cmd": "test_run", "params": {"id": "strategy-1", "days": 3}})

    assert events == [
        "leadership",
        "scan",
        "leadership",
        "test-run:strategy-1:3",
    ]
    router.handle.assert_not_called()
    ops.handle_kill_switch.assert_not_called()


@pytest.mark.parametrize(
    "params",
    (
        {},
        {"id": None},
        {"id": ""},
        {"id": "   "},
        {"id": 1},
        {"id": "strategy-1", "days": True},
        {"id": "strategy-1", "days": "3"},
    ),
)
def test_test_run_rejects_each_malformed_identity_or_days(params: dict) -> None:
    events: list[str] = []
    service, router, _ops, logger = _service(events)

    service.dispatch({"command": "TEST_RUN", "params": params})

    assert events == ["leadership"]
    logger.error.assert_called_once_with("Malformed TEST_RUN command")
    router.handle.assert_not_called()


@pytest.mark.parametrize(
    ("command", "method"),
    (
        ("KILL_SWITCH", "handle_kill_switch"),
        ("CLEAR_KILL_SWITCH", "handle_clear_kill_switch"),
    ),
)
def test_ops_commands_select_exactly_one_current_owner(
    command: str,
    method: str,
) -> None:
    events: list[str] = []
    service, router, old_ops, _logger = _service(events)
    current_ops = MagicMock()
    service._ops_command_service = lambda: current_ops
    params = {"actor": "operator", "reason": "test"}

    service.dispatch({"command": command, "params": params})

    getattr(current_ops, method).assert_called_once_with(params)
    old_ops.handle_kill_switch.assert_not_called()
    old_ops.handle_clear_kill_switch.assert_not_called()
    router.handle.assert_not_called()


def test_state_command_preserves_validation_claim_route_complete_log_order() -> None:
    events: list[str] = []
    service, router, _ops, logger = _service(events)
    command = {
        "command": "START",
        "params": {
            "strategy_id": "strategy-1",
            "actor": "operator@example.com",
            "expected_version": 3,
            "idempotency_key": "start-1",
        },
    }

    service.dispatch(command)

    assert events == [
        "leadership",
        "validate:strategy-1:START:3",
        "claim",
        "route",
        "complete",
    ]
    router.handle.assert_called_once_with(command)
    logger.info.assert_any_call("Command %s succeeded: %s", "START", "ok")


def test_duplicate_claim_returns_before_router_or_completion() -> None:
    events: list[str] = []
    service, router, _ops, logger = _service(events)
    service._claim_strategy_command_operation = (
        lambda **_: events.append("claim") or False
    )

    service.dispatch(
        {
            "command": "STOP",
            "params": {
                "id": "strategy-1",
                "idempotency_key": "stop-1",
            },
        }
    )

    assert events == [
        "leadership",
        "validate:strategy-1:STOP:None",
        "claim",
    ]
    router.handle.assert_not_called()
    logger.info.assert_any_call(
        "Skipping duplicate strategy command for actor %s",
        "operator",
    )


def test_router_failure_logs_warning_after_completion() -> None:
    events: list[str] = []
    service, router, _ops, logger = _service(events)
    router.handle.side_effect = lambda _data: events.append("route") or CommandResult(
        False, "rejected"
    )

    service.dispatch({"command": "LIST"})

    assert events == ["leadership", "route"]
    logger.warning.assert_called_once_with(
        "Command %s failed: %s",
        "LIST",
        "rejected",
    )


def test_dispatch_uses_the_current_command_router() -> None:
    events: list[str] = []
    service, old_router, _ops, _logger = _service(events)
    current_router = MagicMock()
    current_router.handle.return_value = CommandResult(True, "current")
    service._command_router = lambda: current_router
    command = {"command": "LIST"}

    service.dispatch(command)

    old_router.handle.assert_not_called()
    current_router.handle.assert_called_once_with(command)


def test_handler_exception_is_logged_once_and_not_rethrown() -> None:
    events: list[str] = []
    service, router, _ops, logger = _service(events)
    failure = RuntimeError("scan failed")
    service._scan_strategies = MagicMock(side_effect=failure)

    service.dispatch({"command": "SCAN"})

    router.handle.assert_not_called()
    logger.error.assert_called_once()
    assert logger.error.call_args.args[0] == "Error executing command %s: %s\n%s"
    assert logger.error.call_args.args[1:3] == ("SCAN", failure)
    assert "RuntimeError: scan failed" in logger.error.call_args.args[3]


def test_leadership_exception_preserves_identity_before_terminal_boundary() -> None:
    events: list[str] = []
    service, router, _ops, logger = _service(events)
    failure = RuntimeError("leadership lost")
    service._assert_leadership = MagicMock(side_effect=failure)

    with pytest.raises(RuntimeError) as caught:
        service.dispatch({"command": "SCAN"})

    assert caught.value is failure
    router.handle.assert_not_called()
    logger.error.assert_not_called()
