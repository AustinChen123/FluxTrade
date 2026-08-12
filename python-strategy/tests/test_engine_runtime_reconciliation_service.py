from __future__ import annotations

import inspect
from unittest.mock import MagicMock, patch

import pytest

from src.core.engine_runtime_reconciliation_service import (
    EngineRuntimeReconciliationService,
)


def _service(
    *,
    stop_event: MagicMock,
    is_running: MagicMock | None = None,
    select_reconciliation: MagicMock | None = None,
    assert_leadership: MagicMock | None = None,
    event_logger: MagicMock | None = None,
) -> EngineRuntimeReconciliationService:
    return EngineRuntimeReconciliationService(
        is_running=is_running or MagicMock(return_value=True),
        select_reconciliation=select_reconciliation
        or MagicMock(return_value=(False, MagicMock())),
        assert_leadership=assert_leadership or MagicMock(),
        event_logger=event_logger or MagicMock(),
    )


def test_generic_reconciliation_runs_immediately_then_waits() -> None:
    stop_event = MagicMock()
    stop_event.is_set.return_value = False
    stop_event.wait.return_value = True
    operation = MagicMock()
    select_reconciliation = MagicMock(return_value=(False, operation))
    leadership = MagicMock()
    service = _service(
        stop_event=stop_event,
        select_reconciliation=select_reconciliation,
        assert_leadership=leadership,
    )

    service._run(300.0, stop_event)

    select_reconciliation.assert_called_once_with()
    operation.assert_called_once_with()
    assert leadership.call_count == 2
    stop_event.wait.assert_called_once_with(300.0)


def test_venue_reconciliation_stops_during_initial_defer() -> None:
    stop_event = MagicMock()
    stop_event.wait.return_value = True
    operation = MagicMock()
    leadership = MagicMock()
    service = _service(
        stop_event=stop_event,
        select_reconciliation=MagicMock(return_value=(True, operation)),
        assert_leadership=leadership,
    )

    service._run(300.0, stop_event)

    stop_event.wait.assert_called_once_with(300.0)
    operation.assert_not_called()
    leadership.assert_not_called()


def test_operation_failure_logs_once_then_waits() -> None:
    stop_event = MagicMock()
    stop_event.is_set.return_value = False
    stop_event.wait.side_effect = [False, True]
    failure = RuntimeError("reconciliation failed")
    operation = MagicMock(side_effect=failure)
    event_logger = MagicMock()
    service = _service(
        stop_event=stop_event,
        select_reconciliation=MagicMock(return_value=(True, operation)),
        event_logger=event_logger,
    )

    service._run(300.0, stop_event)

    operation.assert_called_once_with()
    event_logger.error.assert_called_once_with(
        "Runtime reconciliation loop failed: %s",
        failure,
    )
    assert stop_event.wait.call_args_list[0].args == (300.0,)
    assert stop_event.wait.call_args_list[1].args == (300.0,)


@pytest.mark.parametrize("failure_call", [1, 2])
def test_leadership_loss_exits_without_retry(failure_call: int) -> None:
    stop_event = MagicMock()
    stop_event.is_set.return_value = False
    failure = RuntimeError("leadership lost")
    leadership = MagicMock(
        side_effect=[None, failure] if failure_call == 2 else [failure]
    )
    operation = MagicMock()
    service = _service(
        stop_event=stop_event,
        select_reconciliation=MagicMock(return_value=(False, operation)),
        assert_leadership=leadership,
    )

    service._run(300.0, stop_event)

    assert operation.call_count == failure_call - 1
    stop_event.wait.assert_not_called()


def test_start_returns_the_current_daemon_thread() -> None:
    stop_event = MagicMock()
    thread = MagicMock()
    with patch(
        "src.core.engine_runtime_reconciliation_service.threading.Thread",
        return_value=thread,
    ) as thread_factory:
        service = _service(stop_event=stop_event)

        result = service.start(interval=300.0, stop_event=stop_event)

    stop_event.clear.assert_called_once_with()
    thread_factory.assert_called_once_with(
        target=service._run,
        args=(300.0, stop_event),
        daemon=True,
    )
    thread.start.assert_called_once_with()
    assert result is thread


def test_owner_has_no_concrete_venue_dependency() -> None:
    source = inspect.getsource(EngineRuntimeReconciliationService)

    for forbidden in ("rithmic", "binance", "backpack", "bybit", "okx", "ccxt"):
        assert forbidden not in source.lower()
