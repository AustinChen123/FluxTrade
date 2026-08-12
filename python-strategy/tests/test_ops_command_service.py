from contextlib import contextmanager, nullcontext
from typing import Any, Callable
from unittest.mock import MagicMock

import pytest

from src.core.ops_command_service import OpsCommandService
from src.core.runtime_capabilities import KillSwitchClearPreparation


def _service(events: list[str], *, preparation=None):
    logger = MagicMock()

    def action(name: str, result: Any = None) -> Callable[..., Any]:
        def run(*_args: object, **_kwargs: object) -> Any:
            events.append(name)
            return result

        return run

    completed = MagicMock(side_effect=action("completed-read", False))
    mark_completed = MagicMock(side_effect=action("completed-write"))
    run_kill = MagicMock(side_effect=action("flatten", {"complete": True}))
    classify = MagicMock(side_effect=action("classify", True))
    finalize = MagicMock(side_effect=action("finalize"))
    service = OpsCommandService(
        operation_lock=lambda: nullcontext(),
        kill_switch_operation_completed=completed,
        halt_for_kill_switch=action("local-halt"),
        persist_lockdown_database=action("db-lockdown"),
        persist_lockdown_redis=action("redis-lockdown"),
        run_kill_switch=run_kill,
        mark_kill_switch_halted=action("mark-halted"),
        requires_authoritative_verification=action("authoritative", False),
        kill_switch_result_is_complete=classify,
        mark_kill_switch_operation_completed=mark_completed,
        prepare_kill_switch_clear=action(
            "prepare",
            preparation or KillSwitchClearPreparation(True, None, None),
        ),
        assert_leadership=action("leadership"),
        clear_kill_switch=action("clear", {"cleared": True, "reason": None}),
        persist_clear_database=action("db-ok"),
        persist_clear_redis=action("redis-ok"),
        clear_local_halt=action("clear-local-halt"),
        finalize_external_drift_clear=finalize,
        event_logger=lambda: logger,
    )
    return service, logger, completed, run_kill, classify, mark_completed, finalize


def test_kill_switch_preserves_exact_success_order_and_arguments() -> None:
    events: list[str] = []
    service, _logger, completed, run_kill, classify, mark_completed, _finalize = (
        _service(events)
    )

    service.handle_kill_switch(
        {
            "actor": "operator@example.com",
            "reason": "manual halt",
            "idempotency_key": "halt-1",
        }
    )

    assert events == [
        "completed-read",
        "local-halt",
        "db-lockdown",
        "redis-lockdown",
        "flatten",
        "mark-halted",
        "authoritative",
        "classify",
        "completed-write",
    ]
    completed.assert_called_once_with(
        actor="operator@example.com",
        idempotency_key="halt-1",
    )
    run_kill.assert_called_once_with(
        actor="operator@example.com",
        reason="manual halt",
        operation_id="halt-1",
    )
    classify.assert_called_once_with(
        {"complete": True},
        authoritative_required=False,
    )
    mark_completed.assert_called_once_with(
        actor="operator@example.com",
        idempotency_key="halt-1",
    )


def test_completed_kill_switch_returns_before_mutation() -> None:
    events: list[str] = []
    service, logger, completed, run_kill, _classify, mark_completed, _finalize = (
        _service(events)
    )
    completed.side_effect = lambda **_: events.append("completed-read") or True

    service.handle_kill_switch(
        {"actor": "operator", "idempotency_key": "halt-complete"}
    )

    assert events == ["completed-read"]
    run_kill.assert_not_called()
    mark_completed.assert_not_called()
    logger.info.assert_called_once_with(
        "Skipping completed kill switch operation for actor %s",
        "operator",
    )


def test_operation_lock_is_loaded_at_call_time() -> None:
    events: list[str] = []
    service, *_rest = _service(events)

    @contextmanager
    def replacement_lock():
        events.append("lock-enter")
        yield
        events.append("lock-exit")

    service._operation_lock = replacement_lock
    service.handle_kill_switch({})

    assert events[0] == "lock-enter"
    assert events[-1] == "lock-exit"


@pytest.mark.parametrize("failed", ["database", "redis", "both"])
def test_persistence_failure_still_flattens_but_never_marks_complete(failed) -> None:
    events: list[str] = []
    service, logger, _completed, run_kill, classify, mark_completed, _finalize = (
        _service(events)
    )
    failure = RuntimeError("unavailable")
    if failed in {"database", "both"}:
        service._persist_lockdown_database = MagicMock(side_effect=failure)
    if failed in {"redis", "both"}:
        service._persist_lockdown_redis = MagicMock(side_effect=failure)

    service.handle_kill_switch({"actor": "operator", "idempotency_key": "halt-retry"})

    run_kill.assert_called_once()
    classify.assert_not_called()
    mark_completed.assert_not_called()
    assert logger.exception.call_count == (2 if failed == "both" else 1)


def test_incomplete_flatten_is_not_marked_complete() -> None:
    events: list[str] = []
    service, _logger, _completed, _run_kill, classify, mark_completed, _finalize = (
        _service(events)
    )
    classify.side_effect = lambda *_args, **_kwargs: False

    service.handle_kill_switch({"idempotency_key": "halt-incomplete"})

    classify.assert_called_once()
    mark_completed.assert_not_called()


def test_rejected_clear_returns_before_leadership_or_mutation() -> None:
    events: list[str] = []
    service, logger, *_rest = _service(
        events,
        preparation=KillSwitchClearPreparation(False, None, "still unsafe"),
    )

    service.handle_clear_kill_switch({"actor": "operator"})

    assert events == ["prepare"]
    logger.warning.assert_called_once_with(
        "Kill switch clear rejected: %s",
        "still unsafe",
    )


@pytest.mark.parametrize(
    ("generation", "cleared", "expected"),
    [
        (None, False, ["prepare", "leadership", "clear"]),
        (
            None,
            True,
            [
                "prepare",
                "leadership",
                "clear",
                "leadership",
                "clear-local-halt",
            ],
        ),
        (7, False, ["prepare", "leadership", "clear", "finalize"]),
        (7, True, ["prepare", "leadership", "clear", "finalize"]),
    ],
)
def test_clear_result_and_drift_finalizer_precedence(
    generation, cleared, expected
) -> None:
    events: list[str] = []
    service, logger, *_prefix, finalize = _service(
        events,
        preparation=KillSwitchClearPreparation(True, generation, None),
    )

    def clear(*, persist_clear):
        events.append("clear")
        if cleared:
            persist_clear()
        return {"cleared": cleared, "reason": "not flat"}

    service._clear_kill_switch = clear
    service.handle_clear_kill_switch({"actor": "operator", "reason": "verified"})

    persisted = ["leadership", "db-ok", "leadership", "redis-ok", "leadership"]
    assert events == (expected[:3] + persisted + expected[3:] if cleared else expected)
    if generation is None:
        finalize.assert_not_called()
    else:
        finalize.assert_called_once_with(
            prepared_generation=generation,
            clear_succeeded=cleared,
        )
    if not cleared:
        logger.warning.assert_called_once_with(
            "Kill switch clear rejected: %s",
            "not flat",
        )


def test_clear_failure_preserves_identity_and_still_finalizes_drift() -> None:
    events: list[str] = []
    service, _logger, *_prefix, finalize = _service(
        events,
        preparation=KillSwitchClearPreparation(True, 11, None),
    )
    failure = RuntimeError("clear failed")
    service._clear_kill_switch = MagicMock(side_effect=failure)

    with pytest.raises(RuntimeError) as exc_info:
        service.handle_clear_kill_switch({"actor": "operator"})

    assert exc_info.value is failure
    finalize.assert_called_once_with(
        prepared_generation=11,
        clear_succeeded=False,
    )
