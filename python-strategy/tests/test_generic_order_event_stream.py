from unittest.mock import MagicMock

import pytest

from src.core.generic_order_event_stream import GenericOrderEventStream

_APPLIED = {"action": "applied"}


class _StopEvent:
    def __init__(self, events: list[str]) -> None:
        self._events = events
        self._set = False

    def clear(self) -> None:
        self._events.append("clear")
        self._set = False

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._events.append("set")
        self._set = True

    def wait(self, timeout: float) -> None:
        self._events.append(f"wait:{timeout}")
        self._set = True


class _ImmediateThread:
    def __init__(
        self,
        *,
        target,
        name: str,
        daemon: bool,
        events: list[str],
    ) -> None:
        self._target = target
        self.name = name
        self.daemon = daemon
        self._events = events

    def start(self) -> None:
        self._events.append("worker-start")
        self._target()

    def is_alive(self) -> bool:
        return False

    def join(self, *, timeout: float) -> None:
        raise AssertionError(f"unexpected join with timeout {timeout}")


def _service(
    events: list[str],
    *,
    adapter: object,
    stop_event: _StopEvent | None = None,
    process_result: object = _APPLIED,
):
    current = {"adapter": adapter, "worker": None}
    event = stop_event or _StopEvent(events)
    logger = MagicMock()

    def publish_worker(worker: object) -> None:
        events.append("publish")
        current["worker"] = worker

    def default_thread_factory(**values):
        return _ImmediateThread(events=events, **values)

    def process_event(value: object) -> object:
        events.append(f"process:{value}")
        return process_result

    service = GenericOrderEventStream(
        adapter_loader=lambda: current["adapter"],
        is_running=lambda: True,
        stop_event=lambda: event,
        assert_leadership=lambda: events.append("fence"),
        process_event=process_event,
        latch_stream_failure=lambda: events.append("latch"),
        halt_submissions=lambda: events.append("halt"),
        publish_worker=publish_worker,
        current_worker=lambda: current["worker"],
        event_logger=logger,
        thread_factory=default_thread_factory,
    )
    return service, current, event, logger


def test_start_preserves_worker_metadata_fences_and_dispatch_order() -> None:
    events: list[str] = []
    adapter = MagicMock()
    adapter.start_order_event_stream.side_effect = lambda: events.append(
        "adapter-start"
    )

    def poll_once() -> str:
        events.append("poll")
        return "event-1"

    adapter.poll_order_event.side_effect = poll_once
    service, current, stop_event, _logger = _service(events, adapter=adapter)

    def process_event(value: object) -> object:
        events.append(f"process:{value}")
        stop_event.set()
        return _APPLIED

    service._process_event = process_event

    service.start()

    assert events == [
        "adapter-start",
        "clear",
        "publish",
        "worker-start",
        "fence",
        "poll",
        "fence",
        "process:event-1",
        "set",
        "fence",
    ]
    assert current["worker"].name == "exchange-order-events"
    assert current["worker"].daemon is True


def test_missing_adapter_capability_is_a_noop() -> None:
    events: list[str] = []
    adapter = object()
    service, current, _stop_event, _logger = _service(events, adapter=adapter)

    service.start()

    assert events == []
    assert current["worker"] is None


def test_adapter_is_loaded_at_each_start() -> None:
    events: list[str] = []
    old_adapter = object()
    new_adapter = MagicMock()
    service, current, stop_event, _logger = _service(events, adapter=old_adapter)
    new_adapter.poll_order_event.side_effect = lambda: stop_event.set()
    current["adapter"] = new_adapter

    service.start()

    new_adapter.start_order_event_stream.assert_called_once_with()
    new_adapter.poll_order_event.assert_called_once_with()


def test_empty_poll_waits_without_dispatch() -> None:
    events: list[str] = []
    adapter = MagicMock()
    adapter.poll_order_event.return_value = None
    service, _current, _stop_event, _logger = _service(events, adapter=adapter)

    service.start()

    assert events == [
        "clear",
        "publish",
        "worker-start",
        "fence",
        "wait:0.05",
    ]
    assert not any(event.startswith("process:") for event in events)


def test_start_failure_is_contained_latched_and_keeps_control_plane_alive() -> None:
    events: list[str] = []
    adapter = MagicMock()
    failure = RuntimeError("offline")
    adapter.start_order_event_stream.side_effect = failure
    service, current, _stop_event, _logger = _service(events, adapter=adapter)

    assert service.start() is None

    assert events == ["latch", "halt"]
    assert current["worker"] is None
    _logger.error.assert_called_once_with(
        "Exchange order event stream could not start; submissions remain halted"
    )


@pytest.mark.parametrize(
    "process_result",
    [None, {}, {"action": "unresolved"}, {"action": "future_action"}],
)
def test_non_exact_applied_result_latches_before_halting(
    process_result: object,
) -> None:
    events: list[str] = []
    adapter = MagicMock()
    adapter.poll_order_event.return_value = "event-1"
    service, _current, _stop_event, logger = _service(
        events,
        adapter=adapter,
        process_result=process_result,
    )

    service.start()

    assert events[-2:] == ["latch", "halt"]
    logger.error.assert_called_once_with(
        "Exchange order event could not be applied; submissions remain halted"
    )


@pytest.mark.parametrize("failure_owner", ("leadership", "poll", "process"))
def test_worker_failure_logs_once_and_halts(failure_owner: str) -> None:
    events: list[str] = []
    adapter = MagicMock()
    failure = RuntimeError(failure_owner)
    service, _current, _stop_event, logger = _service(events, adapter=adapter)
    if failure_owner == "leadership":
        service._assert_leadership = MagicMock(side_effect=failure)
    elif failure_owner == "poll":
        adapter.poll_order_event.side_effect = failure
    else:
        adapter.poll_order_event.return_value = "event-1"
        service._process_event = MagicMock(side_effect=failure)

    service.start()

    logger.error.assert_called_once_with(
        "Exchange order event stream failed; submissions remain halted"
    )
    assert events[-2:] == ["latch", "halt"]


@pytest.mark.parametrize(
    ("alive_results", "expected", "join_expected"),
    [
        ((False,), True, False),
        ((True, False), True, True),
        ((True, True), False, True),
    ],
)
def test_stop_is_bounded_and_resolves_the_current_worker(
    alive_results: tuple[bool, ...],
    expected: bool,
    join_expected: bool,
) -> None:
    events: list[str] = []
    service, current, _stop_event, _logger = _service(events, adapter=object())
    stale_worker = MagicMock()
    current_worker = MagicMock()
    current_worker.is_alive.side_effect = alive_results
    current["worker"] = stale_worker
    current["worker"] = current_worker

    assert service.stop(timeout=4.5) is expected

    assert events == ["set"]
    if join_expected:
        current_worker.join.assert_called_once_with(timeout=4.5)
    else:
        current_worker.join.assert_not_called()
    stale_worker.is_alive.assert_not_called()


def test_stop_without_worker_only_sets_the_stop_event() -> None:
    events: list[str] = []
    service, _current, _stop_event, _logger = _service(events, adapter=object())

    assert service.stop(timeout=1.0) is True
    assert events == ["set"]
