import logging
import threading
from collections.abc import Mapping
from typing import Callable, Protocol, cast


class _Worker(Protocol):
    def start(self) -> None: ...

    def is_alive(self) -> bool: ...

    def join(self, *, timeout: float) -> None: ...


class _StopEvent(Protocol):
    def clear(self) -> None: ...

    def is_set(self) -> bool: ...

    def set(self) -> None: ...

    def wait(self, timeout: float) -> object: ...


class _ThreadFactory(Protocol):
    def __call__(
        self,
        *,
        target: Callable[[], None],
        name: str,
        daemon: bool,
    ) -> _Worker: ...


class GenericOrderEventStream:
    """Own the venue-neutral adapter order-event worker lifecycle."""

    def __init__(
        self,
        *,
        adapter_loader: Callable[[], object],
        is_running: Callable[[], bool],
        stop_event: Callable[[], _StopEvent],
        assert_leadership: Callable[[], None],
        process_event: Callable[[object], object],
        latch_stream_failure: Callable[[], None],
        halt_submissions: Callable[[], None],
        publish_worker: Callable[[_Worker], None],
        current_worker: Callable[[], _Worker | None],
        event_logger: logging.Logger,
        thread_factory: _ThreadFactory = threading.Thread,
    ) -> None:
        self._adapter_loader = adapter_loader
        self._is_running = is_running
        self._stop_event = stop_event
        self._assert_leadership = assert_leadership
        self._process_event = process_event
        self._latch_stream_failure = latch_stream_failure
        self._halt_submissions = halt_submissions
        self._publish_worker = publish_worker
        self._current_worker = current_worker
        self._event_logger = event_logger
        self._thread_factory = thread_factory

    def start(self) -> None:
        adapter = self._adapter_loader()
        start = getattr(adapter, "start_order_event_stream", None)
        poll = getattr(adapter, "poll_order_event", None)
        if not callable(start) or not callable(poll):
            return

        try:
            start()
        except Exception:
            self._event_logger.error(
                "Exchange order event stream could not start; submissions remain halted"
            )
            self._latch_stream_failure()
            self._halt_submissions()
            return
        self._stop_event().clear()
        poll_event = cast(Callable[[], object | None], poll)

        def order_event_loop() -> None:
            while self._is_running() and not self._stop_event().is_set():
                try:
                    self._assert_leadership()
                    event = poll_event()
                    if event is None:
                        self._stop_event().wait(0.05)
                        continue
                    self._assert_leadership()
                    result = self._process_event(event)
                    if not (
                        isinstance(result, Mapping)
                        and result.get("action") == "applied"
                    ):
                        self._event_logger.error(
                            "Exchange order event could not be applied; submissions remain halted"
                        )
                        self._latch_stream_failure()
                        self._halt_submissions()
                        return
                    self._assert_leadership()
                except Exception:
                    self._event_logger.error(
                        "Exchange order event stream failed; submissions remain halted"
                    )
                    self._latch_stream_failure()
                    self._halt_submissions()
                    return

        worker = self._thread_factory(
            target=order_event_loop,
            name="exchange-order-events",
            daemon=True,
        )
        self._publish_worker(worker)
        worker.start()

    def stop(self, *, timeout: float) -> bool:
        self._stop_event().set()
        worker = self._current_worker()
        if worker is not None and worker.is_alive():
            worker.join(timeout=timeout)
            if worker.is_alive():
                return False
        return True
