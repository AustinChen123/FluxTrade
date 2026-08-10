"""Rithmic kill-switch clear preparation lifecycle owner."""

from __future__ import annotations

from collections.abc import Callable
from logging import Logger
from typing import Any, Protocol

from .rithmic_order_event_lifecycle import RithmicOrderEventLifecycleGate


class RithmicClearAdapter(Protocol):
    def close(self) -> None: ...


class OrderEventWorker(Protocol):
    def is_alive(self) -> bool: ...

    def join(self, *, timeout: float) -> None: ...


class RithmicKillSwitchClearPreparationService:
    """Own the existing fail-closed Rithmic clear preparation cycle."""

    def __init__(
        self,
        *,
        adapter: RithmicClearAdapter,
        profile: str,
        account_id: str | None,
        operation_gate: RithmicOrderEventLifecycleGate,
        set_order_event_stop: Callable[[], None],
        clear_order_event_stop: Callable[[], None],
        current_order_event_thread: Callable[[], OrderEventWorker | None],
        halt_for_reconcile: Callable[..., bool],
        reconcile_owned_orders: Callable[[str, str | None], dict[str, Any]],
        publish_authoritative_summary: Callable[[dict[str, Any]], None],
        current_drift_generation: Callable[[], int],
        assert_runtime_leadership: Callable[[], None],
        start_order_event_stream: Callable[[], None],
        resume_after_reconcile: Callable[[], None],
        logger: Logger,
    ) -> None:
        self._adapter = adapter
        self._profile = profile
        self._account_id = account_id
        self._operation_gate = operation_gate
        self._set_order_event_stop = set_order_event_stop
        self._clear_order_event_stop = clear_order_event_stop
        self._current_order_event_thread = current_order_event_thread
        self._halt_for_reconcile = halt_for_reconcile
        self._reconcile_owned_orders = reconcile_owned_orders
        self._publish_authoritative_summary = publish_authoritative_summary
        self._current_drift_generation = current_drift_generation
        self._assert_runtime_leadership = assert_runtime_leadership
        self._start_order_event_stream = start_order_event_stream
        self._resume_after_reconcile = resume_after_reconcile
        self._logger = logger

    def prepare(self) -> tuple[bool, int | None]:
        """Return whether generic clear may proceed and the drift generation."""
        return self._operation_gate.run(self._prepare_serialized)

    def _prepare_serialized(self) -> tuple[bool, int | None]:
        self._set_order_event_stop()
        thread = self._current_order_event_thread()
        if thread is not None and thread.is_alive():
            thread.join(timeout=30.0)
            self._assert_runtime_leadership()
            if thread.is_alive():
                self._logger.error(
                    "Rithmic clear reconciliation timed out stopping the order event stream"
                )
                self._clear_order_event_stop()
                return False, None

        if not self._halt_for_reconcile(timeout=30.0):
            self._logger.error(
                "Rithmic clear reconciliation timed out draining in-flight submissions"
            )
            self._assert_runtime_leadership()
            try:
                self._start_order_event_stream()
            except Exception:
                self._logger.exception(
                    "Order stream restart failed after reconciliation drain timeout"
                )
                return False, None
            self._assert_runtime_leadership()
            self._resume_after_reconcile()
            return False, None

        self._adapter.close()
        summary = None
        try:
            summary = self._reconcile_owned_orders(self._profile, self._account_id)
        except Exception:
            self._logger.exception("Rithmic clear reconciliation failed")

        drift_generation = self._current_drift_generation()

        self._assert_runtime_leadership()
        try:
            self._start_order_event_stream()
        except Exception:
            self._logger.exception(
                "Order stream restart failed after external-order reconciliation"
            )
            return False, None
        self._assert_runtime_leadership()

        if not summary or summary.get("auto_resume_safe") is not True:
            self._resume_after_reconcile()
            self._logger.error(
                "Rithmic clear reconciliation is unresolved; lockdown remains active"
            )
            return False, None
        try:
            self._publish_authoritative_summary(summary)
        except Exception:
            self._logger.exception(
                "Rithmic clear account reconciliation failed; lockdown remains active"
            )
            self._assert_runtime_leadership()
            self._resume_after_reconcile()
            return False, None
        self._assert_runtime_leadership()
        return True, drift_generation
