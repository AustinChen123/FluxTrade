"""Live operations safety module.

Provides OpsSafetyService with a kill-switch that cancels all open orders
and flattens all positions via ExecutionEngine, then writes a system audit event.

Implementation notes for the implementer:
- "ops" must be added to SYSTEM_EVENT_TYPES in audit_service.py AND the DB CHECK
  constraint on system_events.event_type must be updated via a migration.
- account_service.get_all_positions() does not exist on the real AccountService;
  the implementer must add it (returns list[Position]).
- Cancel scope: orders with status in {NEW, SUBMITTED_UNCONFIRMED, SUBMITTED,
  PARTIALLY_FILLED}. For NEW orders use order_manager.fail_order; for the rest
  use execution_engine.cancel_order.
"""

from __future__ import annotations

from copy import deepcopy
import logging
import threading
from typing import Any, Callable, ContextManager

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.core.audit_service import write_system_event
from src.core.execution import FlattenPending
from src.core.models import OrderStatus
from src.core.orm_models import SystemEvent

OPS_KILL_SWITCH_STRATEGY_ID = "__ops_kill_switch__"

_DRAIN_TIMEOUT_DEFAULT = 30.0


class OpsSafetyService:
    """Kill-switch and ops-safety façade for live trading.

    Dependencies are injected so that this class can be unit-tested with fakes.
    The db_session_factory is a callable returning a context-manager that yields
    a SQLAlchemy Session (same convention as the rest of the codebase).
    """

    def __init__(
        self,
        execution_engine: Any,
        account_service: Any,
        db_session_factory: Callable[[], ContextManager[Session]],
        logger: logging.Logger | None = None,
        drain_timeout: float = _DRAIN_TIMEOUT_DEFAULT,
    ) -> None:
        self._execution_engine = execution_engine
        self._account_service = account_service
        self._db_session_factory = db_session_factory
        self._logger = logger or logging.getLogger(__name__)
        self._drain_timeout = drain_timeout
        self._kill_switch_lock = threading.Lock()
        self._recovery_pending = False
        self._recovery_result: dict | None = None
        self._recovery_actor = "system"
        self._recovery_reason: str | None = None

    def kill_switch(self, *, actor: str, reason: str | None = None) -> dict:
        return self._kill_switch(
            actor=actor,
            reason=reason,
        )

    def kill_switch_with_authoritative_positions(
        self,
        *,
        actor: str,
        reason: str | None,
        position_loader: Callable[[], list[Any]],
        account_id: str,
    ) -> dict:
        return self._kill_switch(
            actor=actor,
            reason=reason,
            position_loader=position_loader,
            authoritative_account_id=account_id,
            allow_async_recovery=False,
            write_audit=False,
        )

    def record_kill_switch_result(
        self,
        *,
        actor: str,
        reason: str | None,
        result: dict,
    ) -> None:
        self._write_event_best_effort(
            actor=actor,
            reason=reason,
            result=result,
        )

    def _kill_switch(
        self,
        *,
        actor: str,
        reason: str | None,
        position_loader: Callable[[], list[Any]] | None = None,
        authoritative_account_id: str | None = None,
        allow_async_recovery: bool = True,
        write_audit: bool = True,
    ) -> dict:
        with self._kill_switch_lock:
            if self._recovery_pending:
                return deepcopy(self._recovery_result)
            result, recovery_pending = self._run_kill_switch(
                actor=actor,
                reason=reason,
                position_loader=position_loader,
                authoritative_account_id=authoritative_account_id,
                write_audit=write_audit,
            )
            if recovery_pending and allow_async_recovery:
                self._recovery_pending = True
                self._recovery_result = deepcopy(result)
                self._recovery_actor = actor
                self._recovery_reason = reason

        if recovery_pending and allow_async_recovery:
            run_when_drained = getattr(
                self._execution_engine,
                "run_when_submissions_drained",
                None,
            )
            if callable(run_when_drained):
                try:
                    run_when_drained(self._recover_after_submission_drain)
                except Exception as exc:
                    self._record_recovery_failure(
                        result,
                        f"submission_drain_callback_registration_failed: {exc}",
                    )
            else:
                self._record_recovery_failure(
                    result,
                    "submission_drain_callback_unavailable",
                )
        return result

    def _record_recovery_failure(self, result: dict, reason: str) -> None:
        self._logger.error("Kill switch recovery failed: %s", reason)
        failure = {"reason": reason}
        with self._kill_switch_lock:
            result["recovery_failures"].append(failure)
            if self._recovery_result is not None:
                self._recovery_result["recovery_failures"].append(dict(failure))

    def clear_kill_switch(self, *, persist_clear: Callable[[], None]) -> dict:
        with self._kill_switch_lock:
            if self._recovery_pending:
                return {"cleared": False, "reason": "recovery_pending"}
            try:
                positions, degraded_reason = self._positions()
                orders = self._open_orders()
            except Exception as exc:
                return {"cleared": False, "reason": f"verification_failed: {exc}"}
            if degraded_reason is not None:
                return {"cleared": False, "reason": "verification_degraded"}
            if positions or orders:
                return {"cleared": False, "reason": "exposure_not_flat"}

            persist_clear()
            self._execution_engine.resume_submissions()
            return {"cleared": True, "reason": None}

    def persist_kill_switch_state(
        self,
        state: str,
        *,
        actor: str,
        reason: str | None,
    ) -> None:
        self._write_event(
            actor=actor,
            reason=reason,
            result={"state": state},
            event_subtype="kill_switch_state",
        )

    def latest_kill_switch_state(self) -> str | None:
        payload = self._latest_state_payload("kill_switch_state")
        if payload is None:
            return None
        state = payload.get("state")
        return state if state in {"LOCKDOWN", "OK"} else None

    def persist_engine_boot_state(self, state: str, *, boot_id: str) -> None:
        self._write_event(
            actor="engine",
            reason=None,
            result={"state": state, "boot_id": boot_id},
            event_subtype="engine_boot_state",
        )

    def latest_engine_boot_state(self) -> dict[str, str] | None:
        payload = self._latest_state_payload("engine_boot_state")
        if payload is None:
            return None
        state = payload.get("state")
        boot_id = payload.get("boot_id")
        if state not in {"CLEAN", "UNCLEAN"} or not isinstance(boot_id, str):
            return None
        return {"state": state, "boot_id": boot_id}

    def _latest_state_payload(self, event_subtype: str) -> dict | None:
        with self._db_session_factory() as session:
            event = session.execute(
                select(SystemEvent)
                .where(
                    SystemEvent.event_type == "ops",
                    SystemEvent.event_subtype == event_subtype,
                )
                .order_by(SystemEvent.id.desc())
                .limit(1)
            ).scalar_one_or_none()
        if not isinstance(event, SystemEvent) or not isinstance(event.payload, dict):
            return None
        return event.payload

    @property
    def recovery_pending(self) -> bool:
        with self._kill_switch_lock:
            return self._recovery_pending

    def _run_kill_switch(
        self,
        *,
        actor: str,
        reason: str | None = None,
        result: dict | None = None,
        position_loader: Callable[[], list[Any]] | None = None,
        authoritative_account_id: str | None = None,
        write_audit: bool = True,
    ) -> tuple[dict, bool]:
        """Cancel all open orders, then flatten all positions.

        Returns:
            {
                "cancelled_orders": int,
                "cancel_failures": [{"order_id": str, "reason": str}],
                "flattened_positions": int,
                "flatten_failures": [{"strategy_id": str, "product_id": str, "reason": str}],
                "already_flat": bool,
            }

        Ordering guarantee: all order cancellations complete before any
        flatten order is placed.

        Cancel scope: every order with status in {NEW, SUBMITTED_UNCONFIRMED,
        SUBMITTED, PARTIALLY_FILLED}.  NEW orders (never placed on exchange)
        are failed locally via order_manager.fail_order(order, "kill_switch");
        all others are cancelled via execution_engine.cancel_order(order_id).

        Failure isolation: a failure on one order/position is recorded and
        processing continues for the rest.

        Audit: the generic path attempts a system event here; authoritative
        callers defer it until their final remote verification. Audit failures
        never block emergency mitigation.

        Idempotency: no open orders and no positions → already_flat=True with
        zero counts; audit event is still written.
        """
        result = result or {
            "cancelled_orders": 0,
            "cancel_failures": [],
            "flattened_positions": 0,
            "flatten_pending": [],
            "flatten_failures": [],
            "recovery_failures": [],
            "already_flat": False,
            "drain_timeout": False,
        }

        # Drain in-flight order submissions before snapshotting state.
        halt_and_drain = getattr(self._execution_engine, "halt_and_drain", None)
        drained = not callable(halt_and_drain) or halt_and_drain(self._drain_timeout)
        if not drained:
            result["drain_timeout"] = True
            if write_audit:
                self._write_event_best_effort(
                    actor=actor,
                    reason=reason,
                    result=result,
                    event_subtype="kill_switch_pending",
                )
            self._log_drain_timeout()
            return result, True

        orders, positions = self._mitigate_visible_state(
            result,
            position_loader=position_loader,
            authoritative_account_id=authoritative_account_id,
        )

        result["already_flat"] = (
            not orders
            and not positions
            and not result["cancel_failures"]
            and not result["flatten_pending"]
            and not result["flatten_failures"]
            and not result["recovery_failures"]
        )
        if drained and write_audit:
            self._write_event_best_effort(actor=actor, reason=reason, result=result)
        return result, not drained

    def _recover_after_submission_drain(self) -> None:
        with self._kill_switch_lock:
            if not self._recovery_pending or self._recovery_result is None:
                return
            result, recovery_pending = self._run_kill_switch(
                actor=self._recovery_actor,
                reason=self._recovery_reason,
                result=deepcopy(self._recovery_result),
            )
            self._recovery_pending = recovery_pending
            self._recovery_result = deepcopy(result) if recovery_pending else None

    def _log_drain_timeout(self) -> None:
        in_flight = getattr(self._execution_engine, "_submissions_in_flight", None)
        self._logger.warning(
            "Kill switch drain timed out after %.1fs; %s submissions still in flight",
            self._drain_timeout,
            in_flight,
        )

    def _mitigate_visible_state(
        self,
        result: dict,
        *,
        flatten_positions: bool = True,
        position_loader: Callable[[], list[Any]] | None = None,
        authoritative_account_id: str | None = None,
    ) -> tuple[list[Any], list[Any]]:
        orders = self._open_orders()
        for order in orders:
            order_id = str(order.id)
            try:
                if order.status == OrderStatus.NEW.value:
                    self._execution_engine.order_manager.fail_order(order, "kill_switch")
                    result["cancelled_orders"] += 1
                    continue
                if self._execution_engine.cancel_order(order_id):
                    result["cancelled_orders"] += 1
                else:
                    result["cancel_failures"].append(
                        {"order_id": order_id, "reason": "cancel_order_returned_false"}
                    )
            except Exception as exc:
                self._logger.exception("Kill switch failed to cancel order %s", order_id)
                result["cancel_failures"].append(
                    {"order_id": order_id, "reason": str(exc)}
                )

        if not flatten_positions:
            return orders, []

        try:
            if position_loader is None:
                positions, position_fetch_error = self._positions()
            else:
                positions = list(position_loader())
                position_fetch_error = None
        except Exception as exc:
            self._logger.exception("Kill switch failed to enumerate live positions")
            result["flatten_failures"].append(
                {
                    "strategy_id": "unknown",
                    "product_id": "unknown",
                    "reason": str(exc),
                }
            )
            positions = []
            position_fetch_error = None
        else:
            if position_fetch_error is not None:
                result["flatten_failures"].append(
                    {
                        "strategy_id": "unknown",
                        "product_id": "unknown",
                        "reason": position_fetch_error,
                    }
                )
        for position in positions:
            strategy_id = position.strategy_id
            product_id = position.product_id
            side = getattr(position.side, "value", position.side)
            try:
                if authoritative_account_id is not None:
                    flattened_id = (
                        product_id
                        if self._execution_engine.exit_authoritative_position(
                            product_id,
                            account_id=authoritative_account_id,
                        )
                        else None
                    )
                else:
                    flattened_id = self._execution_engine.flatten_position(
                        strategy_id,
                        product_id,
                        side,
                        position.quantity,
                    )
                if isinstance(flattened_id, FlattenPending):
                    result["flatten_pending"].append(
                        {
                            "strategy_id": strategy_id,
                            "product_id": product_id,
                            "order_id": flattened_id.order_id,
                            "reason": flattened_id.reason,
                        }
                    )
                elif flattened_id is not None:
                    result["flattened_positions"] += 1
                else:
                    result["flatten_failures"].append(
                        {
                            "strategy_id": strategy_id,
                            "product_id": product_id,
                            "reason": "flatten_position_returned_none",
                        }
                    )
            except Exception as exc:
                self._logger.exception(
                    "Kill switch failed to flatten %s %s",
                    strategy_id,
                    product_id,
                )
                result["flatten_failures"].append(
                    {
                        "strategy_id": strategy_id,
                        "product_id": product_id,
                        "reason": str(exc),
                    }
                )

        return orders, positions

    def _open_orders(self) -> list[Any]:
        statuses = {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
        return list(
            self._execution_engine.order_manager.repo.list_orders_by_statuses(statuses)
        )

    def _positions(self) -> tuple[list[Any], str | None]:
        """Return positions and any degraded-source reason.

        Fetches local positions for owner attribution first, then queries the
        adapter for authoritative exchange positions.  The adapter path runs
        regardless of whether the local fetch succeeded so that live exchange
        exposure is always flattened.

        Raises only when the adapter has no enumeration method AND the local
        fetch failed — there is nothing to flatten and we must not silently
        return an empty list (which would cause kill_switch to report
        already_flat=True).
        """
        local_error: Exception | None = None
        try:
            local_positions: list[Any] = list(self._account_service.get_all_positions())
        except Exception as exc:
            self._logger.warning(
                "Kill switch: local position fetch failed, proceeding with adapter query: %s",
                exc,
            )
            local_error = exc
            local_positions = []

        adapter_errors: list[Exception] = []
        adapter = getattr(self._execution_engine, "adapter", None)
        if adapter is not None:
            for method_name in ("get_all_positions", "list_positions"):
                method = getattr(adapter, method_name, None)
                if not callable(method):
                    continue
                try:
                    exchange_positions = list(method())
                except Exception as exc:
                    adapter_errors.append(exc)
                    continue
                degraded_reason = (
                    f"local_positions_unavailable: {local_error}"
                    if local_error is not None
                    else None
                )
                return (
                    self._assign_local_position_owners(
                        exchange_positions,
                        local_positions,
                    ),
                    degraded_reason,
                )

        if adapter_errors:
            exchange_reason = "; ".join(str(error) for error in adapter_errors)
            get_position = getattr(adapter, "get_position", None)
            if local_positions and callable(get_position):
                exchange_positions = []
                scoped_errors = []
                scoped_succeeded = False
                for product_id in dict.fromkeys(
                    position.product_id for position in local_positions
                ):
                    try:
                        position = get_position(product_id)
                    except Exception as exc:
                        scoped_errors.append(f"{product_id}: {exc}")
                        continue
                    scoped_succeeded = True
                    if position is not None:
                        exchange_positions.append(position)

                if scoped_succeeded:
                    degraded_reason = (
                        f"exchange_positions_unavailable: {exchange_reason}"
                    )
                    if scoped_errors:
                        degraded_reason += (
                            "; scoped_positions_unavailable: "
                            + "; ".join(scoped_errors)
                        )
                    return (
                        self._assign_local_position_owners(
                            exchange_positions,
                            local_positions,
                        ),
                        degraded_reason,
                    )

            local_reason = (
                f"local_positions_unavailable: {local_error}; "
                if local_error is not None
                else ""
            )
            raise RuntimeError(
                f"{local_reason}exchange_positions_unavailable: {exchange_reason}"
            )

        if local_error is not None:
            # No adapter enumeration method and local fetch failed: we cannot
            # determine exposure, so re-raise to let kill_switch record the
            # failure rather than falsely reporting already_flat.
            raise local_error

        return local_positions, None

    @staticmethod
    def _assign_local_position_owners(
        exchange_positions: list[Any],
        local_positions: list[Any],
    ) -> list[Any]:
        local_by_product: dict[str, list[Any]] = {}
        for position in local_positions:
            local_by_product.setdefault(position.product_id, []).append(position)

        resolved = []
        for position in exchange_positions:
            local_matches = local_by_product.get(position.product_id, [])
            if str(getattr(position, "strategy_id", "")) == "LIVE" and len(local_matches) == 1:
                resolved.append(
                    position.model_copy(
                        update={"strategy_id": local_matches[0].strategy_id}
                    )
                )
                continue
            resolved.append(position)
        return resolved

    def _write_event(
        self,
        *,
        actor: str,
        reason: str | None,
        result: dict,
        event_subtype: str = "kill_switch",
    ) -> None:
        payload = dict(result)
        payload["actor"] = actor
        payload["reason"] = reason
        with self._db_session_factory() as session:
            write_system_event(
                session,
                event_type="ops",
                event_subtype=event_subtype,
                payload=payload,
            )
            commit = getattr(session, "commit", None)
            if callable(commit):
                commit()

    def _write_event_best_effort(self, **kwargs: Any) -> bool:
        try:
            self._write_event(**kwargs)
        except Exception:
            self._logger.exception(
                "Kill switch audit write failed; emergency mitigation continues"
            )
            return False
        return True
