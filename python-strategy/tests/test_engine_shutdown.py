from unittest.mock import MagicMock, call

import pytest

from src.core.engine_shutdown import EngineShutdownService


class _Thread:
    def __init__(self, name: str, events: list[str], *, alive: bool = True) -> None:
        self._name = name
        self._events = events
        self._alive = alive

    def is_alive(self) -> bool:
        self._events.append(f"alive:{self._name}")
        return self._alive

    def join(self, *, timeout: float) -> None:
        self._events.append(f"join:{self._name}:{timeout}")


def _action(events: list[str], name: str):
    return lambda: events.append(name)


def _service(
    events: list[str],
    *,
    threads: tuple[_Thread | None, ...] = (),
    clean_allowed: bool = True,
    logger: MagicMock | None = None,
) -> EngineShutdownService:
    event_logger = logger or MagicMock()
    adapter = MagicMock()
    adapter.close.side_effect = _action(events, "adapter")
    return EngineShutdownService(
        mark_stopping=_action(events, "mark_stopping"),
        shutdown_executor=_action(events, "executor"),
        thread_loaders=tuple((lambda thread=thread: thread) for thread in threads),
        adapter_loader=lambda: adapter,
        shutdown_strategy_state=_action(events, "strategy_state"),
        clean_persistence_allowed=lambda: clean_allowed,
        persist_clean=_action(events, "persist_clean"),
        close_redis=_action(events, "redis"),
        close_entry_gate=_action(events, "entry_gate"),
        event_logger=event_logger,
    )


def test_shutdown_preserves_the_complete_ordered_transaction() -> None:
    events: list[str] = []
    logger = MagicMock()
    logger.info.side_effect = lambda message: events.append(f"info:{message}")
    threads = tuple(
        _Thread(name, events) for name in ("heartbeat", "command", "runtime", "order")
    )

    _service(events, threads=threads, logger=logger).shutdown(
        timeout=7.5,
        clean_exit=True,
    )

    assert events == [
        "info:StrategyEngine shutting down...",
        "mark_stopping",
        "executor",
        "alive:heartbeat",
        "join:heartbeat:7.5",
        "alive:command",
        "join:command:7.5",
        "alive:runtime",
        "join:runtime:7.5",
        "alive:order",
        "join:order:7.5",
        "adapter",
        "strategy_state",
        "persist_clean",
        "redis",
        "entry_gate",
        "info:StrategyEngine shutdown complete.",
    ]


def test_thread_loaders_resolve_current_threads_and_skip_absent_or_dead() -> None:
    events: list[str] = []
    current = {"thread": _Thread("old", events)}
    service = EngineShutdownService(
        mark_stopping=_action(events, "mark_stopping"),
        shutdown_executor=_action(events, "executor"),
        thread_loaders=(lambda: current["thread"], lambda: None),
        adapter_loader=lambda: MagicMock(
            close=MagicMock(side_effect=_action(events, "adapter"))
        ),
        shutdown_strategy_state=_action(events, "strategy_state"),
        clean_persistence_allowed=lambda: False,
        persist_clean=_action(events, "persist_clean"),
        close_redis=_action(events, "redis"),
        close_entry_gate=_action(events, "entry_gate"),
        event_logger=MagicMock(),
    )
    current["thread"] = _Thread("new", events, alive=False)

    service.shutdown(timeout=3.0, clean_exit=False)

    assert "alive:new" in events
    assert all("old" not in event for event in events)
    assert all(not event.startswith("join:") for event in events)


@pytest.mark.parametrize(
    ("clean_exit", "clean_allowed", "expected"),
    [
        (False, False, 0),
        (False, True, 0),
        (True, False, 0),
        (True, True, 1),
    ],
)
def test_clean_persistence_requires_both_shutdown_and_engine_safety(
    clean_exit: bool,
    clean_allowed: bool,
    expected: int,
) -> None:
    events: list[str] = []
    service = _service(events, clean_allowed=clean_allowed)

    service.shutdown(timeout=1.0, clean_exit=clean_exit)

    assert events.count("persist_clean") == expected


@pytest.mark.parametrize(
    "stage",
    [
        "mark_stopping",
        "executor",
        "adapter",
        "strategy_state",
        "persist_clean",
        "entry_gate",
    ],
)
def test_non_redis_failures_preserve_identity_and_stop_later_cleanup(
    stage: str,
) -> None:
    events: list[str] = []
    failure = RuntimeError(stage)
    logger = MagicMock()
    service = _service(events, logger=logger)
    field = {
        "mark_stopping": "_mark_stopping",
        "executor": "_shutdown_executor",
        "adapter": "_close_adapter",
        "strategy_state": "_shutdown_strategy_state",
        "persist_clean": "_persist_clean",
        "entry_gate": "_close_entry_gate",
    }[stage]
    setattr(service, field, MagicMock(side_effect=failure))

    with pytest.raises(RuntimeError) as exc_info:
        service.shutdown(timeout=1.0, clean_exit=True)

    assert exc_info.value is failure
    later_marker = {
        "mark_stopping": "executor",
        "executor": "adapter",
        "adapter": "strategy_state",
        "strategy_state": "persist_clean",
        "persist_clean": "redis",
        "entry_gate": None,
    }[stage]
    if later_marker is not None:
        assert later_marker not in events
    assert call("StrategyEngine shutdown complete.") not in logger.info.call_args_list


def test_redis_close_is_the_only_swallowed_cleanup_error() -> None:
    events: list[str] = []
    logger = MagicMock()
    failure = RuntimeError("redis unavailable")
    service = _service(events, logger=logger)
    service._close_redis = MagicMock(side_effect=failure)

    service.shutdown(timeout=1.0, clean_exit=False)

    logger.warning.assert_called_once_with("Error closing Redis: %s", failure)
    assert events[-1] == "entry_gate"
    logger.info.assert_any_call("StrategyEngine shutdown complete.")
