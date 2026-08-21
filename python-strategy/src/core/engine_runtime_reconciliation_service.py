"""Provider-neutral Engine runtime reconciliation worker lifecycle."""

from __future__ import annotations

import threading
from collections.abc import Callable
from logging import Logger


ReconciliationOperation = Callable[[], object]


class EngineRuntimeReconciliationService:
    """Run the selected reconciliation operation in one daemon worker."""

    def __init__(
        self,
        *,
        is_running: Callable[[], bool],
        select_reconciliation: Callable[[], tuple[bool, ReconciliationOperation]],
        assert_leadership: Callable[[], None],
        event_logger: Logger,
    ) -> None:
        self._is_running = is_running
        self._select_reconciliation = select_reconciliation
        self._assert_leadership = assert_leadership
        self._event_logger = event_logger

    def start(
        self,
        *,
        interval: float,
        stop_event: threading.Event,
    ) -> threading.Thread:
        """Start the worker and return its current thread."""
        stop_event.clear()
        thread = threading.Thread(
            target=self._run,
            args=(interval, stop_event),
            daemon=True,
        )
        thread.start()
        return thread

    def _run(self, interval: float, stop_event: threading.Event) -> None:
        self._event_logger.info("Runtime reconciliation service started.")
        venue_owned, run_reconciliation = self._select_reconciliation()
        # Venue startup already completed reconciliation; avoid an immediate
        # second operation against the freshly started session.
        if venue_owned and stop_event.wait(interval):
            return
        while self._is_running() and not stop_event.is_set():
            try:
                self._assert_leadership()
            except Exception:
                return
            try:
                run_reconciliation()
            except Exception as error:
                self._event_logger.error(
                    "Runtime reconciliation loop failed: %s",
                    error,
                )
            try:
                self._assert_leadership()
            except Exception:
                return
            if stop_event.wait(interval):
                break
