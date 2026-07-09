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

import logging
from typing import Any, Callable, ContextManager

from sqlalchemy.orm import Session

from src.core.audit_service import write_system_event
from src.core.models import OrderStatus

OPS_KILL_SWITCH_STRATEGY_ID = "__ops_kill_switch__"


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
    ) -> None:
        self._execution_engine = execution_engine
        self._account_service = account_service
        self._db_session_factory = db_session_factory
        self._logger = logger or logging.getLogger(__name__)

    def kill_switch(self, *, actor: str, reason: str | None = None) -> dict:
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

        Audit: ONE system_event(event_type="ops", event_subtype="kill_switch")
        is written ALWAYS — even when cancellations or flattens fail.

        Idempotency: no open orders and no positions → already_flat=True with
        zero counts; audit event is still written.
        """
        result = {
            "cancelled_orders": 0,
            "cancel_failures": [],
            "flattened_positions": 0,
            "flatten_failures": [],
            "already_flat": False,
        }

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

        try:
            positions = self._positions()
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
        for position in positions:
            strategy_id = position.strategy_id
            product_id = position.product_id
            side = getattr(position.side, "value", position.side)
            try:
                try:
                    flattened_id = self._execution_engine.flatten_position(
                        strategy_id,
                        product_id,
                        side,
                        position.quantity,
                        reference_price=getattr(position, "entry_price", None),
                    )
                except TypeError as exc:
                    if "reference_price" not in str(exc):
                        raise
                    flattened_id = self._execution_engine.flatten_position(
                        strategy_id,
                        product_id,
                        side,
                        position.quantity,
                    )
                if flattened_id is not None:
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

        result["already_flat"] = (
            not orders
            and not positions
            and not result["cancel_failures"]
            and not result["flatten_failures"]
        )
        self._write_event(actor=actor, reason=reason, result=result)
        return result

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

    def _positions(self) -> list[Any]:
        local_positions = list(self._account_service.get_all_positions())
        adapter = getattr(self._execution_engine, "adapter", None)
        if adapter is not None:
            get_all_positions = getattr(adapter, "get_all_positions", None)
            if callable(get_all_positions):
                return self._assign_local_position_owners(
                    list(get_all_positions()),
                    local_positions,
                )
            list_positions = getattr(adapter, "list_positions", None)
            if callable(list_positions):
                return self._assign_local_position_owners(
                    list(list_positions()),
                    local_positions,
                )
        return local_positions

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

    def _write_event(self, *, actor: str, reason: str | None, result: dict) -> None:
        payload = dict(result)
        payload["actor"] = actor
        payload["reason"] = reason
        with self._db_session_factory() as session:
            write_system_event(
                session,
                event_type="ops",
                event_subtype="kill_switch",
                payload=payload,
            )
            commit = getattr(session, "commit", None)
            if callable(commit):
                commit()
