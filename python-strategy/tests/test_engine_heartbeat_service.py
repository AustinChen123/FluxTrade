import inspect
from unittest.mock import ANY, MagicMock

import pytest

from src.core.engine_heartbeat_service import EngineHeartbeatService


def _run_one_cycle(
    *,
    admission_allowed: bool,
    balance_error: Exception | None = None,
) -> tuple[list[object], MagicMock]:
    events: list[object] = []
    running = True

    def is_running() -> bool:
        return running

    def assert_leadership() -> None:
        events.append("leadership")

    def write_process_heartbeat() -> None:
        events.append("process")

    def observe_entry_admission() -> bool:
        events.append("admission")
        return admission_allowed

    def record_balance_metric() -> None:
        events.append("balance")
        if balance_error is not None:
            raise balance_error

    def load_active_strategy_ids() -> list[str]:
        events.append("load_strategies")
        return ["portfolio", "standalone"]

    def record_strategy_heartbeats(strategy_ids: list[str]) -> None:
        events.append(("strategy_heartbeats", strategy_ids))

    def sleep(_seconds: float) -> None:
        nonlocal running
        assert _seconds == 1.0
        events.append("sleep")
        running = False

    event_logger = MagicMock()
    service = EngineHeartbeatService(
        is_running=is_running,
        assert_leadership=assert_leadership,
        write_process_heartbeat=write_process_heartbeat,
        observe_entry_admission=observe_entry_admission,
        record_balance_metric=record_balance_metric,
        load_active_strategy_ids=load_active_strategy_ids,
        record_strategy_heartbeats=record_strategy_heartbeats,
        sleep=sleep,
        event_logger=event_logger,
    )

    thread = service.start()
    thread.join(timeout=1.0)
    assert not thread.is_alive()
    return events, event_logger


@pytest.mark.parametrize("admission_allowed", [False, True])
def test_process_heartbeat_precedes_entry_admission_and_strategy_heartbeat(
    admission_allowed,
) -> None:
    events, event_logger = _run_one_cycle(
        admission_allowed=admission_allowed,
    )

    expected: list[object] = [
        "leadership",
        "process",
        "admission",
        "balance",
        "load_strategies",
    ]
    if admission_allowed:
        expected.append(("strategy_heartbeats", ["portfolio", "standalone"]))
    expected.append("sleep")
    assert events == expected
    event_logger.error.assert_not_called()


def test_balance_metric_failure_does_not_suppress_strategy_heartbeat() -> None:
    events, event_logger = _run_one_cycle(
        admission_allowed=True,
        balance_error=RuntimeError("balance unavailable"),
    )

    assert ("strategy_heartbeats", ["portfolio", "standalone"]) in events
    event_logger.error.assert_not_called()


def test_leadership_loss_stops_before_process_heartbeat() -> None:
    process_heartbeat = MagicMock()
    sleep = MagicMock()
    event_logger = MagicMock()
    service = EngineHeartbeatService(
        is_running=lambda: True,
        assert_leadership=MagicMock(side_effect=RuntimeError("lease lost")),
        write_process_heartbeat=process_heartbeat,
        observe_entry_admission=lambda: True,
        record_balance_metric=MagicMock(),
        load_active_strategy_ids=MagicMock(return_value=[]),
        record_strategy_heartbeats=MagicMock(),
        sleep=sleep,
        event_logger=event_logger,
    )

    thread = service.start()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    process_heartbeat.assert_not_called()
    sleep.assert_not_called()
    event_logger.error.assert_not_called()


def test_cycle_failure_is_logged_and_waits_before_retry() -> None:
    running = True

    def is_running() -> bool:
        return running

    def sleep(seconds: float) -> None:
        nonlocal running
        assert seconds == 1.0
        running = False

    event_logger = MagicMock()
    service = EngineHeartbeatService(
        is_running=is_running,
        assert_leadership=MagicMock(),
        write_process_heartbeat=MagicMock(
            side_effect=RuntimeError("redis unavailable")
        ),
        observe_entry_admission=lambda: True,
        record_balance_metric=MagicMock(),
        load_active_strategy_ids=MagicMock(return_value=[]),
        record_strategy_heartbeats=MagicMock(),
        sleep=sleep,
        event_logger=event_logger,
    )

    thread = service.start()
    thread.join(timeout=1.0)

    assert not thread.is_alive()
    event_logger.error.assert_called_once_with(
        "💓 Heartbeat Failed: %s",
        ANY,
    )
    assert isinstance(event_logger.error.call_args.args[1], RuntimeError)


def test_heartbeat_owner_has_no_concrete_venue_dependency() -> None:
    source = inspect.getsource(EngineHeartbeatService)

    for forbidden in ("rithmic", "binance", "backpack", "bybit", "okx", "ccxt"):
        assert forbidden not in source.lower()
