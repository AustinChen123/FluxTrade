"""Rithmic order-session reconnect reconciliation owner."""

from __future__ import annotations

import threading
from collections.abc import Callable
from logging import Logger
from typing import Any, Protocol


class RithmicOrderRuntime(Protocol):
    def connection_generation(self) -> int: ...

    def close(self) -> None: ...

    def start_order_event_stream(self) -> None: ...


class RithmicOrderReconnectService:
    """Own generation state and fail-closed Rithmic reconnect recovery."""

    def __init__(
        self,
        *,
        adapter: RithmicOrderRuntime,
        profile: str,
        account_id: str | None,
        audit_external_orders: Callable[[], bool],
        reconcile_owned_orders: Callable[[str, str | None], dict[str, Any]],
        publish_authoritative_summary: Callable[[dict[str, Any]], None],
        halt_for_reconcile: Callable[..., bool],
        resume_after_reconcile: Callable[[], None],
        assert_runtime_leadership: Callable[[], None],
        logger: Logger,
    ) -> None:
        self._adapter = adapter
        self._profile = profile
        self._account_id = account_id
        self._audit_external_orders = audit_external_orders
        self._reconcile_owned_orders = reconcile_owned_orders
        self._publish_authoritative_summary = publish_authoritative_summary
        self._halt_for_reconcile = halt_for_reconcile
        self._resume_after_reconcile = resume_after_reconcile
        self._assert_runtime_leadership = assert_runtime_leadership
        self._logger = logger
        self._generation_lock = threading.Lock()
        self._last_generation: int | None = None
        self._pending_generation: int | None = None

    @property
    def last_generation(self) -> int | None:
        with self._generation_lock:
            return self._last_generation

    @property
    def pending_generation(self) -> int | None:
        with self._generation_lock:
            return self._pending_generation

    def on_runtime_started(self) -> None:
        """Atomically baseline every successfully started ORDER runtime."""
        with self._generation_lock:
            self._last_generation = 1
            self._pending_generation = None

    def _generation_to_reconcile(self) -> tuple[int, int] | None:
        with self._generation_lock:
            last_generation = self._last_generation
            if self._pending_generation is None:
                if last_generation is None:
                    last_generation = 1
                    self._last_generation = last_generation
                try:
                    generation = self._adapter.connection_generation()
                except Exception:
                    self._logger.exception(
                        "Order connection generation unavailable; "
                        "reconciling fail closed"
                    )
                    generation = last_generation + 1
                if generation <= last_generation:
                    return None
                self._pending_generation = generation
            pending_generation = self._pending_generation
            if last_generation is None or pending_generation is None:
                raise RuntimeError("rithmic_reconnect_generation_state_invalid")
            return last_generation, pending_generation

    def reconcile_if_needed(self) -> bool:
        """Reconcile a new ORDER generation before submissions resume."""
        generations = self._generation_to_reconcile()
        if generations is None:
            return True
        last_generation, pending_generation = generations

        self._logger.info(
            "Order session reconnected (generation %s -> %s); reconciling owned orders",
            last_generation,
            pending_generation,
        )
        if not self._halt_for_reconcile(timeout=30.0):
            self._logger.error(
                "Reconnect order reconciliation waiting for in-flight submissions"
            )
            return False

        self._adapter.close()
        if not self._audit_external_orders() or not self._profile:
            self._logger.error(
                "Reconnect order reconciliation is unavailable; "
                "submissions remain gated"
            )
            return False
        try:
            summary = self._reconcile_owned_orders(self._profile, self._account_id)
        except Exception:
            self._logger.exception(
                "Reconnect order reconciliation failed; submissions remain gated"
            )
            return False
        if summary.get("auto_resume_safe") is not True:
            self._logger.error(
                "Reconnect order reconciliation is unresolved; submissions remain gated"
            )
            return False
        try:
            self._publish_authoritative_summary(summary)
        except Exception:
            self._logger.exception(
                "Reconnect authoritative account reconciliation failed; "
                "submissions remain gated"
            )
            return False

        try:
            self._assert_runtime_leadership()
        except Exception:
            self._adapter.close()
            raise
        try:
            self._adapter.start_order_event_stream()
        except Exception:
            self._logger.exception(
                "Reconnect order stream restart failed; submissions remain gated"
            )
            self._adapter.close()
            return False
        try:
            self._assert_runtime_leadership()
        except Exception:
            self._adapter.close()
            raise

        self.on_runtime_started()
        self._resume_after_reconcile()
        self._logger.info(
            "Reconnect order reconciliation complete: %s recoverable orders",
            summary["recoverable_count"],
        )
        return True
