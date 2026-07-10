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
import threading
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
        product_ids: list[str] | tuple[str, ...] | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self._account_service = account_service
        self._adapter = adapter
        self._db_session_factory = db_session_factory
        self._quantity_drift_threshold = quantity_drift_threshold
        self._balance_drift_threshold = balance_drift_threshold
        self._product_ids = tuple(product_ids or ())
        self._logger = logger or logging.getLogger(__name__)
        self._run_lock = threading.Lock()

    def run_once(self) -> dict:
        with self._run_lock:
            return self._run_once()

    def _run_once(self) -> dict:
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
        if local_positions is None:
            self._check_balance(result)
            self._emit_events(result)
            return result

        local_positions_by_product = self._positions_by_product(local_positions)
        exchange_positions = self._exchange_positions_by_product(result)
        result["checked_positions"] = len(local_positions)
        products = set(exchange_positions.keys())
        products.update(local_positions_by_product.keys())
        products.update(self._product_ids)

        for product_id in sorted(products):
            local_product_positions = local_positions_by_product.get(product_id, [])
            exchange_product_positions = exchange_positions.get(product_id)
            if product_id not in exchange_positions:
                try:
                    exchange_position = self._adapter.get_position(product_id)
                    exchange_product_positions = (
                        [exchange_position] if exchange_position is not None else []
                    )
                except Exception as exc:
                    result["errors"].append(
                        {"scope": "positions", "reason": str(exc)}
                    )
                    continue

            local_quantity = self._signed_total(local_product_positions)
            exchange_quantity = self._signed_total(exchange_product_positions or [])
            if abs(local_quantity - exchange_quantity) > self._quantity_drift_threshold:
                result["position_drifts"].append(
                    {
                        "strategy_id": self._drift_strategy_id(
                            local_product_positions,
                            exchange_product_positions or [],
                        ),
                        "product_id": product_id,
                        "local_quantity": local_quantity,
                        "exchange_quantity": exchange_quantity,
                    }
                )

        self._check_balance(result)
        self._emit_events(result)
        return result

    def _local_positions(self, result: dict) -> list[Any] | None:
        try:
            return list(self._account_service.get_all_positions())
        except Exception as exc:
            result["errors"].append({"scope": "positions", "reason": str(exc)})
            return None

    def _exchange_positions_by_product(self, result: dict) -> dict[str, list[Any]]:
        try:
            if hasattr(self._adapter, "get_all_positions"):
                return self._positions_by_product(self._adapter.get_all_positions())
            if hasattr(self._adapter, "list_positions"):
                return self._positions_by_product(self._adapter.list_positions())
            positions = getattr(self._adapter, "_positions", None)
            if isinstance(positions, dict):
                return {
                    product_id: [position] if position is not None else []
                    for product_id, position in positions.items()
                }
        except Exception as exc:
            result["errors"].append({"scope": "positions", "reason": str(exc)})
        return {}

    @staticmethod
    def _positions_by_product(positions: list[Any]) -> dict[str, list[Any]]:
        grouped: dict[str, list[Any]] = {}
        for position in positions:
            grouped.setdefault(position.product_id, []).append(position)
        return grouped

    def _signed_total(self, positions: list[Any]) -> Decimal:
        return sum((self._signed_quantity(position) for position in positions), Decimal("0"))

    @staticmethod
    def _signed_quantity(position: Any) -> Decimal:
        quantity = position.quantity
        decimal_quantity = quantity if isinstance(quantity, Decimal) else Decimal(str(quantity))
        side = getattr(position, "side", None)
        side_value = getattr(side, "value", side)
        return -decimal_quantity if str(side_value).upper() == "SHORT" else decimal_quantity

    @staticmethod
    def _drift_strategy_id(local_positions: list[Any], exchange_positions: list[Any]) -> str:
        if len(local_positions) == 1:
            return str(local_positions[0].strategy_id)
        if len(local_positions) > 1:
            return "multiple"
        if exchange_positions:
            return str(exchange_positions[0].strategy_id)
        return "unknown"

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
