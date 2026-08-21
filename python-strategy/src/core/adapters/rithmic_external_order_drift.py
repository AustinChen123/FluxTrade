"""Rithmic external-order drift state and clear finalization owner."""

from __future__ import annotations

import threading
from collections.abc import Callable
from logging import Logger


class RithmicExternalOrderDriftService:
    """Serialize external-order detection against kill-switch clear."""

    def __init__(
        self,
        *,
        halt_submissions: Callable[[], None],
        clear_local_halt: Callable[[], None],
        persist_lockdown_state: Callable[[str], None],
        persist_redis_lockdown: Callable[[], object],
        assert_runtime_leadership: Callable[[], None],
        resume_after_reconcile: Callable[[], None],
        logger: Logger,
    ) -> None:
        self._halt_submissions = halt_submissions
        self._clear_local_halt = clear_local_halt
        self._persist_lockdown_state = persist_lockdown_state
        self._persist_redis_lockdown = persist_redis_lockdown
        self._assert_runtime_leadership = assert_runtime_leadership
        self._resume_after_reconcile = resume_after_reconcile
        self._logger = logger
        self._lock = threading.Lock()
        self._pending = False
        self._generation = 0

    @property
    def pending(self) -> bool:
        with self._lock:
            return self._pending

    def current_generation(self) -> int:
        with self._lock:
            return self._generation

    def detect(self, reason: str) -> None:
        """Publish a new drift generation and halt submissions atomically."""
        with self._lock:
            first_detection = not self._pending
            self._pending = True
            self._generation += 1
            self._halt_submissions()
        self._logger.error(
            "%s; submissions locked pending authoritative reconciliation",
            reason,
        )
        if first_detection:
            self._persist_lockdown(reason)

    def finalize_clear(
        self,
        *,
        prepared_generation: int,
        clear_succeeded: bool,
    ) -> None:
        """Apply the final clear decision before releasing reconciliation."""
        with self._lock:
            drift_advanced = self._generation != prepared_generation
            if clear_succeeded and not drift_advanced:
                self._assert_runtime_leadership()
                self._pending = False
                self._clear_local_halt()
            elif drift_advanced:
                self._pending = True
                self._halt_submissions()
        if drift_advanced:
            self._persist_lockdown("rithmic_external_order_detected_during_clear")
        self._assert_runtime_leadership()
        self._resume_after_reconcile()

    def _persist_lockdown(self, reason: str) -> None:
        try:
            self._persist_lockdown_state(reason)
        except Exception:
            self._logger.exception(
                "Failed to persist external-order lockdown to database"
            )
        try:
            self._persist_redis_lockdown()
        except Exception:
            self._logger.exception(
                "Failed to persist external-order lockdown to Redis; local halt remains active"
            )
