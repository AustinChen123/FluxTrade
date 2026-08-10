"""Rithmic order-event stream runtime owner."""

from __future__ import annotations

import threading
from collections.abc import Callable
from logging import Logger
from typing import Any, Protocol

from src.core.interfaces.exchange import ExchangeOrderEvent

from .rithmic_adapter import RithmicUnmappedOrderEvent

_SAFE_ACTIONS = frozenset({"applied"})


class RithmicOrderEventAdapter(Protocol):
    def start_order_event_stream(self) -> None: ...

    def poll_order_event(self) -> ExchangeOrderEvent | None: ...


class StopEvent(Protocol):
    def clear(self) -> None: ...

    def is_set(self) -> bool: ...

    def wait(self, timeout: float) -> bool: ...


class RithmicOrderEventStreamService:
    """Own Rithmic polling, classification and terminal stream failure."""

    def __init__(
        self,
        *,
        adapter: RithmicOrderEventAdapter,
        stop_event: StopEvent,
        is_running: Callable[[], bool],
        publish_worker: Callable[[threading.Thread], None],
        reconcile_if_needed: Callable[[], bool],
        process_event: Callable[[ExchangeOrderEvent], dict[str, Any]],
        lockdown: Callable[[str], None],
        assert_runtime_leadership: Callable[[], None],
        halt_submissions: Callable[[], None],
        on_runtime_started: Callable[[], None],
        logger: Logger,
    ) -> None:
        self._adapter = adapter
        self._stop_event = stop_event
        self._is_running = is_running
        self._publish_worker = publish_worker
        self._reconcile_if_needed = reconcile_if_needed
        self._process_event = process_event
        self._lockdown = lockdown
        self._assert_runtime_leadership = assert_runtime_leadership
        self._halt_submissions = halt_submissions
        self._on_runtime_started = on_runtime_started
        self._logger = logger

    def start(self) -> None:
        try:
            self._adapter.start_order_event_stream()
        except Exception:
            self._halt_submissions()
            raise
        self._on_runtime_started()
        self._stop_event.clear()
        worker = threading.Thread(
            target=self._run,
            name="exchange-order-events",
            daemon=True,
        )
        self._publish_worker(worker)
        worker.start()

    def _run(self) -> None:
        while self._is_running() and not self._stop_event.is_set():
            try:
                self._assert_runtime_leadership()
                if not self._reconcile_if_needed():
                    self._stop_event.wait(1.0)
                    continue
                event = self._adapter.poll_order_event()
                if event is None:
                    self._stop_event.wait(0.05)
                    continue
                self._assert_runtime_leadership()
                result = self._process_event(event)
                self._assert_runtime_leadership()
                self._classify(event, result)
            except RithmicUnmappedOrderEvent as error:
                try:
                    self._assert_runtime_leadership()
                except Exception:
                    return
                self._lockdown(
                    self._external_order_reason(
                        account_id=error.account_id,
                        exchange=error.exchange,
                        symbol=error.symbol,
                    )
                )
            except Exception:
                self._logger.exception(
                    "Exchange order event stream failed; submissions remain halted"
                )
                self._halt_submissions()
                return

    def _classify(
        self,
        event: ExchangeOrderEvent,
        result: dict[str, Any],
    ) -> None:
        action = str(result.get("action") or "")
        if action == "unknown_order":
            raw = event.raw or {}
            self._lockdown(
                self._external_order_reason(
                    account_id=str(raw.get("account_id") or ""),
                    exchange=str(raw.get("exchange") or ""),
                    symbol=str(raw.get("symbol") or event.product_id),
                    client_order_id=event.client_order_id,
                    exchange_order_id=event.exchange_order_id,
                )
            )
        elif self.requires_reconciliation(result):
            self._lockdown(
                "rithmic_order_event_requires_reconciliation: "
                f"action={action or 'missing_action'} "
                f"product_id={event.product_id} "
                f"client_order_id={event.client_order_id or 'unknown'} "
                f"exchange_order_id={event.exchange_order_id or 'unknown'}"
            )

    @staticmethod
    def requires_reconciliation(result: dict[str, Any]) -> bool:
        return (
            result.get("action") not in _SAFE_ACTIONS
            or bool(result.get("verification_blocked"))
            or bool(result.get("unresolved"))
        )

    @staticmethod
    def _external_order_reason(
        *,
        account_id: str,
        exchange: str,
        symbol: str,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> str:
        return (
            "rithmic_external_order_detected: "
            f"account_id={account_id or 'unknown'} "
            f"exchange={exchange or 'unknown'} symbol={symbol or 'unknown'} "
            f"client_order_id={client_order_id or 'unknown'} "
            f"exchange_order_id={exchange_order_id or 'unknown'}"
        )
