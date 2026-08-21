"""Venue-neutral execution submission concurrency owner."""

from __future__ import annotations

import threading
from collections.abc import Callable


class ExecutionSubmissionGate:
    """Own kill-switch, reconcile, drain, and authoritative-exit state."""

    def __init__(self, on_callback_error: Callable[[], None]) -> None:
        self._on_callback_error = on_callback_error
        self._condition = threading.Condition()
        self._submissions_halted = False
        self._reconcile_halted = False
        self._order_event_stream_failed = False
        self._reconcile_generation = 0
        self._reconcile_claim = threading.local()
        self._in_flight = 0
        self._drain_callbacks: list[Callable[[], None]] = []

    @property
    def submissions_halted(self) -> bool:
        with self._condition:
            return self._submissions_halted

    @property
    def reconcile_halted(self) -> bool:
        with self._condition:
            return self._reconcile_halted

    @property
    def order_event_stream_failed(self) -> bool:
        with self._condition:
            return self._order_event_stream_failed

    @property
    def generation(self) -> int:
        with self._condition:
            return self._reconcile_generation

    @property
    def in_flight(self) -> int:
        with self._condition:
            return self._in_flight

    @property
    def authoritative_exit_active(self) -> bool:
        with self._condition:
            return self._reconcile_halted and self._in_flight == 1

    def try_begin_submission(self) -> str | None:
        with self._condition:
            if self._submissions_halted:
                return "kill_switch_halted"
            if self._order_event_stream_failed:
                return "order_event_stream_failed"
            if self._reconcile_halted:
                return "reconcile_halted"
            self._in_flight += 1
            return None

    def finish_submission(self) -> None:
        with self._condition:
            self._in_flight -= 1
            callbacks = self._detach_callbacks_if_drained_locked()
            self._condition.notify_all()
        self._run_queued_callbacks(callbacks)

    def halt_and_drain(self, timeout: float = 30.0) -> bool:
        with self._condition:
            self._submissions_halted = True
            return self._condition.wait_for(
                lambda: self._in_flight == 0,
                timeout=timeout,
            )

    def run_when_submissions_drained(self, callback: Callable[[], None]) -> None:
        with self._condition:
            if self._in_flight > 0:
                self._drain_callbacks.append(callback)
                return
        callback()

    def resume_submissions(self) -> None:
        with self._condition:
            self._submissions_halted = False
            self._condition.notify_all()

    def latch_order_event_stream_failure(self) -> None:
        """Permanently reject money-path operations after stream ownership fails."""
        with self._condition:
            self._order_event_stream_failed = True
            self._condition.notify_all()

    def halt_for_reconcile(self, timeout: float = 0.0) -> bool:
        with self._condition:
            generation = self._claim_reconcile_halt_locked()
            self._reconcile_claim.generation = generation
            return self._condition.wait_for(
                lambda: self._in_flight == 0,
                timeout=timeout,
            )

    def claim_reconcile_halt(self) -> int:
        with self._condition:
            return self._claim_reconcile_halt_locked()

    def resume_after_reconcile(self) -> None:
        expected_generation = getattr(
            self._reconcile_claim,
            "generation",
            None,
        )
        with self._condition:
            if (
                expected_generation is None
                or self._reconcile_generation == expected_generation
            ):
                self._reconcile_halted = False
            self._condition.notify_all()
        if expected_generation is not None:
            del self._reconcile_claim.generation

    def begin_authoritative_exit(self, *, timeout: float) -> int | None:
        with self._condition:
            if (
                self._submissions_halted
                or self._order_event_stream_failed
                or self._reconcile_halted
            ):
                return None
            reconcile_generation = self._claim_reconcile_halt_locked()
            if not self._condition.wait_for(
                lambda: self._in_flight == 0,
                timeout=timeout,
            ):
                return None
            if self._submissions_halted or self._order_event_stream_failed:
                return None
            self._in_flight += 1
            return reconcile_generation

    def finish_authoritative_exit(
        self,
        *,
        resume_after_reconcile: bool,
        reconcile_generation: int,
    ) -> None:
        with self._condition:
            self._in_flight -= 1
            if (
                resume_after_reconcile
                and self._reconcile_generation == reconcile_generation
            ):
                self._reconcile_halted = False
            callbacks = self._detach_callbacks_if_drained_locked()
            self._condition.notify_all()
        self._run_queued_callbacks(callbacks)

    def _claim_reconcile_halt_locked(self) -> int:
        self._reconcile_generation += 1
        self._reconcile_halted = True
        return self._reconcile_generation

    def _detach_callbacks_if_drained_locked(self) -> list[Callable[[], None]]:
        if self._in_flight != 0:
            return []
        callbacks, self._drain_callbacks = self._drain_callbacks, []
        return callbacks

    def _run_queued_callbacks(self, callbacks: list[Callable[[], None]]) -> None:
        for callback in callbacks:
            try:
                callback()
            except Exception:
                self._on_callback_error()
