"""Runtime reconciliation job.

Periodically compares local in-memory state (positions, balance) against the
live exchange via the adapter.  On drift beyond configured thresholds it emits
a system_event and logs a warning.  Never raises — adapter errors are captured
in the result's errors list and trigger their own system_event.

Implementation notes for the implementer:
- account_service.get_all_positions() does not exist on the real AccountService;
  the implementer must add it (returns list[Position]).
- adapter.get_position(product_id) returns Optional[Position].  If a position
  exists locally but the exchange reports None, treat exchange_quantity as 0.
- balance comparison uses adapter.get_balance("USDT") vs account_service.get_balance().
- quantity_drift_threshold and balance_drift_threshold are both Decimal;
  comparison is abs(local - exchange) > threshold.
- When both local and exchange have positions, the drift is
  abs(local.quantity - exchange.quantity) > quantity_drift_threshold.
- "reconcile" is already in SYSTEM_EVENT_TYPES; "runtime_drift" and
  "runtime_reconcile_error" are new event_subtypes (no constraint on subtypes).
- All Decimal arithmetic — never float.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from typing import Any, Callable, ContextManager

from sqlalchemy.orm import Session

from src.core.audit_service import write_system_event


class RuntimeReconciliationJob:
    """Compares local state to exchange state and emits drift alerts.

    Intended to be called from a background scheduler (e.g. APScheduler).
    Never raises — all exceptions are captured in the returned result dict.
    """

    def __init__(
        self,
        account_service: Any,
        adapter: Any,
        db_session_factory: Callable[[], ContextManager[Session]],
        *,
        quantity_drift_threshold: Decimal,
        balance_drift_threshold: Decimal,
        logger: logging.Logger | None = None,
    ) -> None:
        self._account_service = account_service
        self._adapter = adapter
        self._db_session_factory = db_session_factory
        self._quantity_drift_threshold = quantity_drift_threshold
        self._balance_drift_threshold = balance_drift_threshold
        self._logger = logger or logging.getLogger(__name__)

    def run_once(self) -> dict:
        """Compare local positions/balance against the exchange.

        Returns:
            {
                "checked_positions": int,
                "position_drifts": [
                    {
                        "strategy_id": str,
                        "product_id": str,
                        "local_quantity": Decimal,
                        "exchange_quantity": Decimal,
                    }
                ],
                "balance_drift": {"local": Decimal, "exchange": Decimal} | None,
                "errors": [{"scope": str, "reason": str}],
            }

        Drift beyond threshold:
            → system_event(event_type="reconcile",
                           event_subtype="runtime_drift", payload=result)
            → logger.warning

        Exchange fetch error:
            → recorded in errors list (scope="positions" or "balance")
            → system_event(event_type="reconcile",
                           event_subtype="runtime_reconcile_error")
            → NO exception propagates

        No drift, no errors → NO system_event written.

        All Decimal values — never float.
        """
        result = {
            "checked_positions": 0,
            "position_drifts": [],
            "balance_drift": None,
            "errors": [],
        }

        local_positions = self._local_positions(result)
        exchange_positions = self._exchange_positions_by_product(result)
        products = set(exchange_positions.keys())
        products.update(position.product_id for position in local_positions)

        for product_id in sorted(products):
            local_position = next(
                (pos for pos in local_positions if pos.product_id == product_id),
                None,
            )
            if local_position is not None:
                result["checked_positions"] += 1
            exchange_position = exchange_positions.get(product_id)
            if product_id not in exchange_positions:
                try:
                    exchange_position = self._adapter.get_position(product_id)
                except Exception as exc:
                    result["errors"].append(
                        {"scope": "positions", "reason": str(exc)}
                    )
                    continue

            local_quantity = self._quantity(local_position)
            exchange_quantity = self._quantity(exchange_position)
            if abs(local_quantity - exchange_quantity) > self._quantity_drift_threshold:
                result["position_drifts"].append(
                    {
                        "strategy_id": (
                            local_position.strategy_id
                            if local_position is not None
                            else exchange_position.strategy_id
                        ),
                        "product_id": product_id,
                        "local_quantity": local_quantity,
                        "exchange_quantity": exchange_quantity,
                    }
                )

        self._check_balance(result)
        self._emit_events(result)
        return result

    def _local_positions(self, result: dict) -> list[Any]:
        try:
            return list(self._account_service.get_all_positions())
        except Exception as exc:
            result["errors"].append({"scope": "positions", "reason": str(exc)})
            return []

    def _exchange_positions_by_product(self, result: dict) -> dict[str, Any]:
        try:
            if hasattr(self._adapter, "get_all_positions"):
                return {
                    position.product_id: position
                    for position in self._adapter.get_all_positions()
                }
            if hasattr(self._adapter, "list_positions"):
                return {
                    position.product_id: position
                    for position in self._adapter.list_positions()
                }
            positions = getattr(self._adapter, "_positions", None)
            if isinstance(positions, dict):
                return dict(positions)
        except Exception as exc:
            result["errors"].append({"scope": "positions", "reason": str(exc)})
        return {}

    @staticmethod
    def _quantity(position: Any | None) -> Decimal:
        if position is None:
            return Decimal("0")
        quantity = position.quantity
        return quantity if isinstance(quantity, Decimal) else Decimal(str(quantity))

    def _check_balance(self, result: dict) -> None:
        try:
            local_balance = self._account_service.get_balance()
            exchange_balance = self._adapter.get_balance("USDT")
            if not isinstance(local_balance, Decimal):
                local_balance = Decimal(str(local_balance))
            if not isinstance(exchange_balance, Decimal):
                exchange_balance = Decimal(str(exchange_balance))
            if abs(local_balance - exchange_balance) > self._balance_drift_threshold:
                result["balance_drift"] = {
                    "local": local_balance,
                    "exchange": exchange_balance,
                }
        except Exception as exc:
            result["errors"].append({"scope": "balance", "reason": str(exc)})

    def _emit_events(self, result: dict) -> None:
        if result["position_drifts"] or result["balance_drift"] is not None:
            self._logger.warning("Runtime reconciliation drift detected: %s", result)
            self._write_event("runtime_drift", result)
        if result["errors"]:
            self._logger.warning("Runtime reconciliation errors: %s", result["errors"])
            self._write_event("runtime_reconcile_error", result)

    def _write_event(self, event_subtype: str, result: dict) -> None:
        try:
            with self._db_session_factory() as session:
                write_system_event(
                    session,
                    event_type="reconcile",
                    event_subtype=event_subtype,
                    payload=result,
                )
                commit = getattr(session, "commit", None)
                if callable(commit):
                    commit()
        except Exception as exc:
            self._logger.error("Failed to write runtime reconciliation event: %s", exc)
