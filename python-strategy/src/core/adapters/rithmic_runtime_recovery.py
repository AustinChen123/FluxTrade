"""Periodic Rithmic ledger reconciliation and runtime recovery owner."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime
from logging import Logger
from time import monotonic
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo


_FAILURE_RETRY_DELAYS_SECONDS = (900.0, 1_800.0, 3_600.0, 14_400.0)
_SUCCESS_RECHECK_DELAY_SECONDS = 3_600.0
_POST_MAINTENANCE_VERIFICATION_ATTEMPTS = 3
_RITHMIC_MAINTENANCE_TIMEZONE = ZoneInfo("America/New_York")
_DAILY_MAINTENANCE_START_MINUTE = 17 * 60 + 15
_DAILY_MAINTENANCE_END_MINUTE = 17 * 60 + 50
_SUNDAY_VERIFICATION_START_MINUTE = 12 * 60 + 15


def rithmic_maintenance_active(now: datetime | None = None) -> bool:
    """Return the conservative Rithmic provider maintenance window."""
    eastern_now = now or datetime.now(tz=_RITHMIC_MAINTENANCE_TIMEZONE)
    if eastern_now.tzinfo is None:
        raise ValueError("Rithmic maintenance clock must be timezone-aware")
    eastern_now = eastern_now.astimezone(_RITHMIC_MAINTENANCE_TIMEZONE)
    weekday = eastern_now.weekday()
    minute_of_day = eastern_now.hour * 60 + eastern_now.minute
    daily_maintenance = (
        _DAILY_MAINTENANCE_START_MINUTE
        <= minute_of_day
        < _DAILY_MAINTENANCE_END_MINUTE
    )
    if weekday < 4:
        return daily_maintenance
    if weekday == 4:
        return minute_of_day >= _DAILY_MAINTENANCE_START_MINUTE
    if weekday == 5:
        return True
    if weekday == 6:
        return minute_of_day < _SUNDAY_VERIFICATION_START_MINUTE
    return False


def _is_transient_provider_summary(summary: dict[str, Any]) -> bool:
    return summary.get("snapshot_retryable") is True


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
        start_order_event_stream: Callable[[], bool],
        resume_after_reconcile: Callable[[], None],
        lockdown: Callable[[str], None],
        logger: Logger,
        monotonic_seconds: Callable[[], float] = monotonic,
        failure_retry_delays_seconds: tuple[float, ...] = (
            _FAILURE_RETRY_DELAYS_SECONDS
        ),
        success_recheck_delay_seconds: float = _SUCCESS_RECHECK_DELAY_SECONDS,
        maintenance_active: Callable[[], bool] = (
            rithmic_maintenance_active
        ),
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
        self._monotonic_seconds = monotonic_seconds
        self._failure_retry_delays_seconds = failure_retry_delays_seconds
        self._success_recheck_delay_seconds = success_recheck_delay_seconds
        self._maintenance_active = maintenance_active
        self._next_attempt_at = 0.0
        self._consecutive_failures = 0
        self._last_result_safe = False
        self._maintenance_observed = False
        self._post_maintenance_attempts_remaining: int | None = None
        self._post_maintenance_exhaustion_logged = False

        if not self._failure_retry_delays_seconds or any(
            delay <= 0.0 for delay in self._failure_retry_delays_seconds
        ):
            raise ValueError("Rithmic recovery retry delays must be positive")
        if self._success_recheck_delay_seconds <= 0.0:
            raise ValueError("Rithmic recovery success delay must be positive")

    def _fence_or_close(self) -> None:
        try:
            self._assert_runtime_leadership()
        except Exception:
            self._adapter.close()
            raise

    def run_once(self) -> bool:
        """Run one exact-account reconciliation and runtime restart cycle."""
        if self._maintenance_active():
            self._maintenance_observed = True
            self._last_result_safe = False
            self._logger.debug(
                "Periodic Rithmic reconciliation skipped during maintenance window"
            )
            return False
        if self._maintenance_observed:
            self._maintenance_observed = False
            self._post_maintenance_attempts_remaining = (
                _POST_MAINTENANCE_VERIFICATION_ATTEMPTS
            )
            self._post_maintenance_exhaustion_logged = False
            self._next_attempt_at = 0.0
            self._consecutive_failures = 0
        if self._post_maintenance_attempts_remaining == 0:
            if not self._post_maintenance_exhaustion_logged:
                self._logger.error(
                    "Post-maintenance Rithmic verification exhausted; "
                    "provider I/O remains quiescent until a new maintenance "
                    "recovery epoch or service restart"
                )
                self._post_maintenance_exhaustion_logged = True
            return False
        now = self._monotonic_seconds()
        if now < self._next_attempt_at:
            self._logger.debug(
                "Periodic Rithmic reconciliation skipped during provider cooldown"
            )
            return self._last_result_safe

        if not self._halt_for_reconcile(timeout=30.0):
            self._lockdown("rithmic_runtime_reconciliation_drain_timeout")
            self._record_failure(now)
            return False

        if not self._stop_order_event_stream(timeout=30.0):
            self._lockdown("rithmic_runtime_reconciliation_stream_stop_timeout")
            self._record_failure(now)
            return False

        if self._maintenance_active():
            self._maintenance_observed = True
            self._last_result_safe = False
            self._adapter.close()
            self._logger.info(
                "Rithmic maintenance started after runtime drain; provider I/O deferred"
            )
            return False

        summary: dict[str, Any] | None = None
        failure_reason: str | None = None
        requires_global_lockdown = False
        self._adapter.close()
        try:
            summary = self._reconcile_owned_orders(self._profile, self._account_id)
        except Exception:
            self._logger.exception("Periodic Rithmic ledger reconciliation failed")
            failure_reason = "rithmic_runtime_reconciliation_failed"
        else:
            if summary.get("auto_resume_safe") is True:
                try:
                    self._publish_authoritative_summary(summary)
                except Exception:
                    self._logger.exception("Periodic Rithmic ledger projection failed")
                    failure_reason = "rithmic_runtime_reconciliation_failed"
                    requires_global_lockdown = True
            else:
                failure_reason = "rithmic_runtime_reconciliation_unresolved"
                requires_global_lockdown = not _is_transient_provider_summary(
                    summary
                )

        self._fence_or_close()
        if self._maintenance_active():
            if failure_reason is not None and requires_global_lockdown:
                self._lockdown(failure_reason)
            self._maintenance_observed = True
            self._last_result_safe = False
            self._adapter.close()
            self._logger.info(
                "Rithmic maintenance started after ledger verification; "
                "order stream restart deferred"
            )
            return False
        stream_connected = False
        try:
            stream_connected = self._start_order_event_stream()
        except Exception:
            self._logger.exception(
                "Order stream restart failed after periodic Rithmic reconciliation"
            )
            self._adapter.close()
            if not requires_global_lockdown:
                failure_reason = (
                    "rithmic_runtime_reconciliation_stream_restart_failed"
                )
        else:
            if failure_reason is None and not stream_connected:
                failure_reason = (
                    "rithmic_runtime_reconciliation_stream_disconnected"
                )

        self._fence_or_close()
        if failure_reason is not None:
            if requires_global_lockdown:
                self._lockdown(failure_reason)
            self._record_failure(now)
            self._logger.warning(
                "Periodic Rithmic recovery remains entry-blocked: reason=%s",
                failure_reason,
            )
            return False

        self._fence_or_close()
        self._resume_after_reconcile()
        successful_summary = cast(dict[str, Any], summary)
        self._logger.info(
            "Periodic Rithmic reconciliation complete: %s recoverable orders",
            successful_summary["recoverable_count"],
        )
        self._record_success(now)
        return True

    def _record_failure(self, now: float) -> None:
        if self._post_maintenance_attempts_remaining is not None:
            self._post_maintenance_attempts_remaining = max(
                0,
                self._post_maintenance_attempts_remaining - 1,
            )
        delay_index = min(
            self._consecutive_failures,
            len(self._failure_retry_delays_seconds) - 1,
        )
        self._next_attempt_at = now + self._failure_retry_delays_seconds[delay_index]
        self._consecutive_failures += 1
        self._last_result_safe = False

    def _record_success(self, now: float) -> None:
        self._next_attempt_at = now + self._success_recheck_delay_seconds
        self._consecutive_failures = 0
        self._last_result_safe = True
        self._post_maintenance_attempts_remaining = None
        self._post_maintenance_exhaustion_logged = False
