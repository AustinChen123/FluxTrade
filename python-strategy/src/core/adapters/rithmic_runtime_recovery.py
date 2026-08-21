"""Periodic Rithmic ledger reconciliation and runtime recovery owner."""

from __future__ import annotations

from collections.abc import Callable
from logging import Logger
from typing import Any, Protocol, cast


class RithmicRuntimeAdapter(Protocol):
    def close(self) -> None: ...


class RithmicRuntimeRecoveryService:
    """Own one fail-closed periodic Rithmic recovery cycle."""

    def __init__(
        self,
        *,
        adapter: RithmicRuntimeAdapter,
        profile: str,
        account_id: str | None,
        halt_for_reconcile: Callable[..., bool],
        stop_order_event_stream: Callable[..., bool],
        reconcile_owned_orders: Callable[[str, str | None], dict[str, Any]],
        publish_authoritative_summary: Callable[[dict[str, Any]], None],
        assert_runtime_leadership: Callable[[], None],
        start_order_event_stream: Callable[[], None],
        resume_after_reconcile: Callable[[], None],
        lockdown: Callable[[str], None],
        logger: Logger,
    ) -> None:
        self._adapter = adapter
        self._profile = profile
        self._account_id = account_id
        self._halt_for_reconcile = halt_for_reconcile
        self._stop_order_event_stream = stop_order_event_stream
        self._reconcile_owned_orders = reconcile_owned_orders
        self._publish_authoritative_summary = publish_authoritative_summary
        self._assert_runtime_leadership = assert_runtime_leadership
        self._start_order_event_stream = start_order_event_stream
        self._resume_after_reconcile = resume_after_reconcile
        self._lockdown = lockdown
        self._logger = logger

    def _fence_or_close(self) -> None:
        try:
            self._assert_runtime_leadership()
        except Exception:
            self._adapter.close()
            raise

    def run_once(self) -> bool:
        """Run one exact-account reconciliation and runtime restart cycle."""
        if not self._halt_for_reconcile(timeout=30.0):
            self._lockdown("rithmic_runtime_reconciliation_drain_timeout")
            return False

        if not self._stop_order_event_stream(timeout=30.0):
            self._lockdown("rithmic_runtime_reconciliation_stream_stop_timeout")
            return False

        summary: dict[str, Any] | None = None
        failure_reason: str | None = None
        self._adapter.close()
        try:
            summary = self._reconcile_owned_orders(self._profile, self._account_id)
            if summary.get("auto_resume_safe") is not True:
                failure_reason = "rithmic_runtime_reconciliation_unresolved"
            else:
                self._publish_authoritative_summary(summary)
        except Exception:
            self._logger.exception("Periodic Rithmic ledger reconciliation failed")
            failure_reason = "rithmic_runtime_reconciliation_failed"

        self._fence_or_close()
        try:
            self._start_order_event_stream()
        except Exception:
            self._logger.exception(
                "Order stream restart failed after periodic Rithmic reconciliation"
            )
            self._adapter.close()
            failure_reason = "rithmic_runtime_reconciliation_stream_restart_failed"

        self._fence_or_close()
        if failure_reason is not None:
            self._lockdown(failure_reason)
            return False

        self._fence_or_close()
        self._resume_after_reconcile()
        successful_summary = cast(dict[str, Any], summary)
        self._logger.info(
            "Periodic Rithmic reconciliation complete: %s recoverable orders",
            successful_summary["recoverable_count"],
        )
        return True
