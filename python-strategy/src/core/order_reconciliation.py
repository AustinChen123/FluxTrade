import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import Callable, ContextManager, Optional, Protocol, TypedDict, cast

from sqlalchemy.orm import Session

from src.core.audit_service import write_system_event
from src.core.fill_delta import FillDeltaState, classify_fill_delta, snapshot_fill_delta
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderLookupUnsupported,
)
from src.core.models import OrderStatus
from src.core.order_event_sync import exchange_snapshot_to_order_event
from src.core.adapters.rithmic_recovery import (
    build_rithmic_recovery_plan,
    compare_rithmic_positions,
    load_rithmic_recovery_snapshot,
)


_SAFE_LEDGER_SNAPSHOT_FAILURES = frozenset(
    {
        ("profile_lease", "profile_lease_failed", "profile lease failed"),
        (
            "runtime_initialization",
            "runtime_initialization_failed",
            "runtime initialization failed",
        ),
        (
            "request_validation",
            "invalid_ledger_snapshot_request",
            "ledger snapshot request validation failed",
        ),
        ("order_config", "order_config_failed", "ORDER config failed"),
        ("order_connect", "order_connect_failed", "ORDER connect failed"),
        ("order_heartbeat", "order_heartbeat_failed", "ORDER heartbeat failed"),
        ("order_login_info", "order_login_info_failed", "ORDER login info failed"),
        (
            "order_account_list",
            "order_account_list_failed",
            "ORDER account list failed",
        ),
        ("order_snapshot", "order_snapshot_failed", "ORDER snapshot failed"),
        ("order_history", "order_history_failed", "ORDER history failed"),
        ("fill_history", "fill_history_failed", "fill history failed"),
        ("pnl_config", "pnl_config_failed", "PNL config failed"),
        ("pnl_connect", "pnl_connect_failed", "PNL connect failed"),
        ("pnl_heartbeat", "pnl_heartbeat_failed", "PNL heartbeat failed"),
        ("pnl_request", "pnl_request_failed", "PNL request failed"),
        ("pnl_snapshot", "pnl_snapshot_failed", "PNL snapshot failed"),
        (
            "unclassified_internal",
            "unclassified_ledger_snapshot_failure",
            "ledger snapshot failed before safe classification",
        ),
    }
)
_LEDGER_SNAPSHOT_FAILURE_FALLBACK = (
    "Exception",
    "unclassified_internal",
    "unclassified_ledger_snapshot_failure",
    "ledger snapshot failed before safe classification",
)


class _ExchangeOrderEventProcessor(Protocol):
    def __call__(
        self,
        event: ExchangeOrderEvent,
        *,
        allow_remote_side_effects: bool = True,
    ) -> dict[str, object]: ...


class _ProtectionRecovery(TypedDict):
    failures: list[dict[str, object]]


class _RecoveryOrderIdentity(Protocol):
    id: object
    client_order_id: object


class _PricedFill(TypedDict):
    price: Decimal
    quantity: Decimal
    fee: Decimal | None


def _classify_ledger_snapshot_failure(exc: Exception) -> tuple[str, str, str, str]:
    error_type = "RuntimeError" if type(exc) is RuntimeError else "Exception"
    if type(exc) is RuntimeError:
        stage = getattr(exc, "stage", None)
        code = getattr(exc, "stable_error_code", None)
        cause = getattr(exc, "safe_cause", None)
        if (
            type(stage) is str
            and type(code) is str
            and type(cause) is str
            and (stage, code, cause) in _SAFE_LEDGER_SNAPSHOT_FAILURES
        ):
            return ("RuntimeError", stage, code, cause)
    return (error_type, *_LEDGER_SNAPSHOT_FAILURE_FALLBACK[1:])


class OrderReconciler:
    def __init__(
        self,
        *,
        adapter,
        order_manager,
        clock,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]],
        process_exchange_order_event: _ExchangeOrderEventProcessor,
        place_pending_protection_for_filled_entries: Callable[[], dict[str, object]],
        fail_pending_conditionals_for_terminal_entry: Callable[[object], None],
        protective_terminal_without_fill_failure: Callable[[object], dict | None],
        cancel_protective_order_when_sibling_closed: Callable[[object], dict | None],
        cancel_linked_conditional_for_protection_fill: Callable[[object], dict | None],
        local_positions_loader: Callable[[], list[object]] | None = None,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        self.adapter = adapter
        self.order_manager = order_manager
        self.clock = clock
        self._db_session_factory = db_session_factory
        self.process_exchange_order_event = process_exchange_order_event
        self.place_pending_protection_for_filled_entries = (
            place_pending_protection_for_filled_entries
        )
        self.fail_pending_conditionals_for_terminal_entry = (
            fail_pending_conditionals_for_terminal_entry
        )
        self.protective_terminal_without_fill_failure = (
            protective_terminal_without_fill_failure
        )
        self.cancel_protective_order_when_sibling_closed = (
            cancel_protective_order_when_sibling_closed
        )
        self.cancel_linked_conditional_for_protection_fill = (
            cancel_linked_conditional_for_protection_fill
        )
        self.local_positions_loader = local_positions_loader
        self.logger = logger or logging.getLogger("OrderReconciler")

    def list_recoverable_client_orders(self):
        """Return persisted client orders that need restart reconciliation."""
        statuses = {
            OrderStatus.NEW.value,
            OrderStatus.SUBMITTED_UNCONFIRMED.value,
            OrderStatus.SUBMITTED.value,
            OrderStatus.PARTIALLY_FILLED.value,
        }
        return self.order_manager.repo.list_client_orders_by_statuses(statuses)

    def record_recoverable_order_scan(self) -> dict:
        """Record a startup scan of client orders that still need reconciliation."""
        if self._db_session_factory is None:
            raise RuntimeError(
                "record_recoverable_order_scan requires db_session_factory"
            )

        orders = self.list_recoverable_client_orders()
        status_counts: dict[str, int] = {}
        for order in orders:
            status_counts[order.status] = status_counts.get(order.status, 0) + 1

        payload = {
            "recoverable_count": len(orders),
            "status_counts": status_counts,
            "orders": [
                {
                    "order_id": order.id,
                    "client_order_id": order.client_order_id,
                    "status": order.status,
                    "strategy_id": order.strategy_id,
                    "product_id": order.product_id,
                    "exchange_order_id": order.exchange_order_id,
                }
                for order in orders
            ],
        }

        with self._db_session_factory() as db:
            try:
                write_system_event(
                    db,
                    event_type="reconcile",
                    event_subtype="startup_recovery_scan",
                    payload=payload,
                )
                db.commit()
            except Exception:
                db.rollback()
                raise

        return payload

    def reconcile_recoverable_client_orders(self) -> dict:
        """Compare recoverable local client orders with exchange snapshots.

        This repairs local order state when the exchange lookup gives enough
        information. It never places replacement orders during startup, with
        one deliberate exception: NEW pending protective orders whose entry
        already has fills are first placements (not replacements) and leaving
        them unplaced keeps a live position naked across restarts.
        """
        if self._db_session_factory is None:
            raise RuntimeError(
                "reconcile_recoverable_client_orders requires db_session_factory"
            )

        orders = self.list_recoverable_client_orders()
        results = []
        result_counts: dict[str, int] = {}
        decision_counts: dict[str, int] = {}

        # Decide skips on the scan snapshot up front: repairing an entry can
        # fail its pending conditionals mid-loop, and a status-based check on
        # the mutated order would stop skipping them.
        pending_protective_ids = {
            order.id for order in orders if self._is_pending_protective_order(order)
        }
        for order in orders:
            if order.id in pending_protective_ids:
                continue
            local_status = order.status
            local_exchange_order_id = order.exchange_order_id
            try:
                snapshot = self.adapter.get_order_by_client_id(
                    order.client_order_id,
                    order.product_id,
                    order_type=order.type,
                )
            except ExchangeOrderLookupUnsupported:
                snapshot = None
                result = "exchange_lookup_unsupported"
            else:
                result = (
                    "exchange_found" if snapshot is not None else "exchange_not_found"
                )
            result_counts[result] = result_counts.get(result, 0) + 1
            if result == "exchange_lookup_unsupported":
                decision = "exchange_unknown"
                repair = self._repair_result(
                    "none",
                    reason="exchange_lookup_unsupported",
                    verification_blocked=True,
                )
            else:
                decision = self._reconcile_decision(
                    order.status, snapshot.status if snapshot else None
                )
                repair = self._repair_reconciled_order(order, decision, snapshot)
            decision_counts[decision] = decision_counts.get(decision, 0) + 1
            unresolved = bool(repair["unresolved"])
            verification_blocked = bool(repair["verification_blocked"])
            results.append(
                {
                    "order_id": order.id,
                    "client_order_id": order.client_order_id,
                    "local_status": local_status,
                    "product_id": order.product_id,
                    "strategy_id": order.strategy_id,
                    "local_exchange_order_id": local_exchange_order_id,
                    "result": result,
                    "decision": decision,
                    "exchange_order_id": snapshot.exchange_order_id
                    if snapshot
                    else None,
                    "exchange_status": snapshot.status if snapshot else None,
                    "repair_action": repair["action"],
                    "repair_reason": repair.get("reason"),
                    "unresolved": unresolved,
                    "verification_blocked": verification_blocked,
                }
            )

        protection_recovery = self.place_pending_protection_for_filled_entries()
        protection_failures = cast(
            _ProtectionRecovery,
            protection_recovery,
        )["failures"]
        reconciliation_unresolved_count = sum(
            1 for result in results if result["unresolved"]
        )
        protection_unresolved_count = len(protection_failures)

        payload = {
            "recoverable_count": len(orders),
            "result_counts": result_counts,
            "decision_counts": decision_counts,
            "unresolved_count": (
                reconciliation_unresolved_count + protection_unresolved_count
            ),
            "reconciliation_unresolved_count": reconciliation_unresolved_count,
            "protection_unresolved_count": protection_unresolved_count,
            "verification_blocked_count": sum(
                1 for result in results if result["verification_blocked"]
            ),
            "results": results,
            "protection_recovery": protection_recovery,
            "skipped_pending_protection_count": len(pending_protective_ids),
        }
        if payload["unresolved_count"] > 0:
            self.logger.error(
                "Startup reconciliation has %s unresolved repair(s)",
                payload["unresolved_count"],
            )
        if payload["verification_blocked_count"] > 0:
            self.logger.warning(
                "Startup reconciliation has %s verification-blocked order(s)",
                payload["verification_blocked_count"],
            )

        with self._db_session_factory() as db:
            try:
                write_system_event(
                    db,
                    event_type="reconcile",
                    event_subtype="startup_exchange_reconcile",
                    payload=payload,
                )
                db.commit()
            except Exception:
                db.rollback()
                raise

        return payload

    def reconcile_rithmic_owned_orders(
        self,
        profile: str,
        account_id: str | None = None,
        *,
        snapshot_loader=None,
    ) -> dict[str, object]:
        """Repair recent FluxTrade-owned Rithmic orders from one remote snapshot."""
        if self._db_session_factory is None:
            raise RuntimeError(
                "reconcile_rithmic_owned_orders requires db_session_factory"
            )

        orders = [
            order
            for order in self.list_recoverable_client_orders()
            if str(order.exchange_id).lower() == "rithmic"
        ]
        if (
            not isinstance(profile, str)
            or not profile.strip()
            or not isinstance(account_id, str)
            or not account_id.strip()
        ):
            return self._write_rithmic_identity_failure_audit(
                orders,
                "configured_account_identity_missing",
            )
        profile = profile.strip()
        account_id = account_id.strip()
        identity_failures = {
            str(order.id): reason
            for order in orders
            if (
                reason := self._rithmic_order_identity_failure(
                    order,
                    profile,
                    account_id,
                )
            )
            is not None
        }
        if identity_failures:
            return self._write_rithmic_identity_failure_audit(
                orders,
                "account_identity_batch_blocked",
                identity_failures=identity_failures,
            )
        try:
            self.adapter.restore_order_groups(orders)
        except ExchangeError as exc:
            return self._write_rithmic_identity_failure_audit(
                orders,
                f"native_order_group_restore_failed:{exc}",
            )
        try:
            snapshot = load_rithmic_recovery_snapshot(
                profile,
                account_id,
                orders,
                int(self.clock.now()),
                snapshot_loader,
            )
        except Exception as exc:
            error_type, error_stage, error_code, error_cause = (
                _classify_ledger_snapshot_failure(exc)
            )
            snapshot_diagnostics = {
                "snapshot_error_type": error_type,
                "snapshot_error_stage": error_stage,
                "snapshot_error_code": error_code,
                "snapshot_error_cause": error_cause,
            }
            self.logger.error(
                "Rithmic ledger snapshot acquisition failed",
                extra=snapshot_diagnostics,
            )
            return self._write_rithmic_recovery_audit(
                {
                    "recoverable_count": len(orders),
                    "matched_count": 0,
                    "repaired_count": 0,
                    "external_count": 0,
                    "unresolved_count": max(1, len(orders)),
                    "verification_blocked_count": max(1, len(orders)),
                    "auto_resume_safe": False,
                    "results": [
                        {
                            "order_id": str(order.id),
                            "classification": "unresolved",
                            "reason": "remote_snapshot_failed",
                            "verification_blocked": True,
                            "unresolved": True,
                        }
                        for order in orders
                    ],
                    "external_orders": [],
                    **snapshot_diagnostics,
                }
            )

        if account_id is not None and snapshot.account_id != account_id:
            recovery_plan = []
            plan = [
                {
                    "order_id": str(order.id),
                    "classification": "unresolved",
                    "reason": "remote_account_id_mismatch",
                    "verification_blocked": True,
                    "unresolved": True,
                }
                for order in orders
            ]
            external_orders = []
        else:
            recovery_plan, external_orders = build_rithmic_recovery_plan(
                orders, snapshot
            )
            plan = []
            for item in recovery_plan:
                order = cast(_RecoveryOrderIdentity, item.order)
                plan.append(
                    {
                        "order_id": str(order.id),
                        "client_order_id": order.client_order_id,
                        "classification": item.classification,
                        "reason": item.reason,
                        "repair_action": "pending"
                        if item.event is not None
                        else "none",
                        "verification_blocked": item.verification_blocked,
                        "unresolved": item.unresolved,
                    }
                )

        planned_payload = self._rithmic_recovery_payload(orders, plan, external_orders)
        if any(item.event is not None for item in recovery_plan):
            self._write_rithmic_recovery_audit(planned_payload, phase="planned")
            for item, result in zip(recovery_plan, plan, strict=True):
                if item.event is None:
                    continue
                applied = self.process_exchange_order_event(
                    item.event,
                    allow_remote_side_effects=False,
                )
                action = str(applied["action"])
                result["repair_action"] = action
                if action not in {"applied", "unresolved_remote_actions_suppressed"}:
                    result["classification"] = "unresolved"
                    result["reason"] = f"event_application_{action}"
                    result["verification_blocked"] = True
                    result["unresolved"] = True
                else:
                    result["unresolved"] = (
                        item.unresolved
                        or action == "unresolved_remote_actions_suppressed"
                    )

        ledger_verification = self._verify_rithmic_ledger(
            orders,
            snapshot,
            expected_account_id=account_id,
        )
        payload = self._rithmic_recovery_payload(
            orders,
            plan,
            external_orders,
            ledger_verification,
        )
        return self._write_rithmic_recovery_audit(payload, phase="completed")

    def _write_rithmic_identity_failure_audit(
        self,
        orders,
        reason: str,
        *,
        identity_failures: dict[str, str] | None = None,
    ) -> dict[str, object]:
        return self._write_rithmic_recovery_audit(
            {
                "recoverable_count": len(orders),
                "matched_count": 0,
                "repaired_count": 0,
                "external_count": 0,
                "unresolved_count": max(1, len(orders)),
                "verification_blocked_count": max(1, len(orders)),
                "auto_resume_safe": False,
                "results": [
                    {
                        "order_id": str(order.id),
                        "classification": "unresolved",
                        "reason": (identity_failures or {}).get(str(order.id), reason),
                        "verification_blocked": True,
                        "unresolved": True,
                    }
                    for order in orders
                ],
                "external_orders": [],
                "ledger_verification": None,
            }
        )

    @staticmethod
    def _rithmic_order_identity_failure(
        order,
        profile: str,
        account_id: str,
    ) -> str | None:
        if not getattr(order, "account_profile", None):
            return "local_account_profile_missing"
        if not getattr(order, "account_id", None):
            return "local_account_id_missing"
        if order.account_profile != profile:
            return "local_account_profile_mismatch"
        if order.account_id != account_id:
            return "local_account_id_mismatch"
        return None

    @staticmethod
    def _rithmic_recovery_payload(
        orders,
        plan,
        external_orders,
        ledger_verification=None,
    ) -> dict[str, object]:
        ledger_blocked = bool(
            ledger_verification and ledger_verification["verification_blocked"]
        )
        external_count = len(external_orders)
        unresolved_count = sum(bool(result["unresolved"]) for result in plan)
        verification_blocked_count = sum(
            bool(result["verification_blocked"]) for result in plan
        )
        return {
            "recoverable_count": len(orders),
            "matched_count": sum(
                result["classification"] == "matched" for result in plan
            ),
            "repaired_count": sum(
                result["classification"] in {"repaired", "repaired_partial"}
                for result in plan
            ),
            "external_count": external_count,
            "unresolved_count": (
                unresolved_count + int(ledger_blocked) + external_count
            ),
            "verification_blocked_count": (
                verification_blocked_count + int(ledger_blocked) + external_count
            ),
            "auto_resume_safe": bool(
                ledger_verification is not None
                and not ledger_blocked
                and unresolved_count == 0
                and verification_blocked_count == 0
                and external_count == 0
            ),
            "results": plan,
            "external_orders": external_orders,
            "ledger_verification": ledger_verification,
        }

    def _verify_rithmic_ledger(
        self,
        orders,
        snapshot,
        *,
        expected_account_id: str | None,
    ) -> dict[str, object]:
        account_summary = snapshot.account_summary
        account_state = (
            {
                field: getattr(account_summary, field, None)
                for field in (
                    "account_balance",
                    "cash_on_hand",
                    "available_buying_power",
                    "day_pnl",
                    "net_quantity",
                    "timestamp_ms",
                )
            }
            if account_summary is not None
            else None
        )
        result = {
            "account_id": snapshot.account_id,
            "account_currency": snapshot.account_currency,
            "account_summary": account_state,
            "position_drifts": [],
            "errors": [],
            "verification_blocked": False,
        }
        if not snapshot.account_currency:
            result["errors"].append("remote_account_currency_missing")
        if (
            expected_account_id is not None
            and snapshot.account_id != expected_account_id
        ):
            result["errors"].append("remote_account_id_mismatch")
        if account_state is None:
            result["errors"].append("remote_account_summary_missing")
        elif not any(value is not None for value in account_state.values()):
            result["errors"].append("remote_account_summary_empty")
        if self.local_positions_loader is None:
            result["errors"].append("local_positions_loader_missing")
        else:
            try:
                result["position_drifts"] = compare_rithmic_positions(
                    orders,
                    self.local_positions_loader(),
                    snapshot.positions,
                )
            except Exception as exc:
                self.logger.error("Rithmic position verification failed: %s", exc)
                result["errors"].append("position_verification_failed")
        result["verification_blocked"] = bool(
            result["errors"] or result["position_drifts"]
        )
        return result

    def _write_rithmic_recovery_audit(
        self,
        payload: dict[str, object],
        *,
        phase: str = "completed",
    ) -> dict[str, object]:
        db_session_factory = self._db_session_factory
        if db_session_factory is None:
            raise RuntimeError("Rithmic recovery audit requires db_session_factory")
        with db_session_factory() as db:
            try:
                write_system_event(
                    db,
                    event_type="reconcile",
                    event_subtype="rithmic_owned_order_recovery",
                    payload={**payload, "phase": phase},
                )
                db.commit()
            except Exception:
                db.rollback()
                raise
        return payload

    def resync_recoverable_order_events(self) -> dict[str, object]:
        """Resync recoverable live orders through the order-event state machine.

        This is intended for disconnect recovery. It converts REST order
        snapshots into ``ExchangeOrderEvent`` instances, then delegates all
        fill/status accounting to ``process_exchange_order_event`` so live
        stream replay and REST catch-up cannot drift into separate semantics.
        """
        orders = self.list_recoverable_client_orders()
        results: list[dict[str, object]] = []
        pending_protective_ids = {
            order.id for order in orders if self._is_pending_protective_order(order)
        }
        for order in orders:
            if order.id in pending_protective_ids:
                continue
            try:
                snapshot = self.adapter.get_order_by_client_id(
                    order.client_order_id,
                    order.product_id,
                    order_type=order.type,
                )
            except ExchangeOrderLookupUnsupported:
                results.append(
                    {
                        "order_id": order.id,
                        "client_order_id": order.client_order_id,
                        "action": "verification_blocked_order_lookup_unsupported",
                        "verification_blocked": True,
                        "unresolved": False,
                    }
                )
                continue
            except ExchangeError as e:
                results.append(
                    {
                        "order_id": order.id,
                        "client_order_id": order.client_order_id,
                        "action": "verification_blocked_order_lookup_failed",
                        "reason": str(e),
                        "verification_blocked": True,
                        "unresolved": False,
                    }
                )
                continue

            if snapshot is None:
                results.append(
                    {
                        "order_id": order.id,
                        "client_order_id": order.client_order_id,
                        "action": "verification_blocked_order_snapshot_missing",
                        "verification_blocked": True,
                        "unresolved": False,
                    }
                )
                continue

            event = self._exchange_snapshot_to_order_event(order.product_id, snapshot)
            result = self.process_exchange_order_event(event)
            result["verification_blocked"] = self._resync_action_verification_blocked(
                str(result["action"])
            )
            result["unresolved"] = str(result["action"]).startswith("unresolved_")
            results.append(result)

        return {
            "recoverable_count": len(orders),
            "applied_count": sum(
                1 for result in results if result["action"] == "applied"
            ),
            "unresolved_count": sum(1 for result in results if result["unresolved"]),
            "verification_blocked_count": sum(
                1 for result in results if result["verification_blocked"]
            ),
            "skipped_pending_protection_count": len(pending_protective_ids),
            "results": results,
        }

    @staticmethod
    def _exchange_snapshot_to_order_event(product_id: str, snapshot):
        return exchange_snapshot_to_order_event(product_id, snapshot)

    @staticmethod
    def _resync_action_verification_blocked(action: str) -> bool:
        return action in {"unknown_order", "unknown_status"}

    def _repair_reconciled_order(
        self, order, decision: str, snapshot
    ) -> dict[str, object]:
        if decision == "local_only":
            self.order_manager.fail_order(
                order, "startup reconciliation: local order not found on exchange"
            )
            self.fail_pending_conditionals_for_terminal_entry(order)
            self._mark_reconciled(order)
            return self._repair_result("marked_failed")

        if snapshot is None:
            return self._repair_result(
                "none",
                reason="exchange_snapshot_unavailable",
                verification_blocked=True,
            )

        if decision == "exchange_unknown":
            return self._repair_result(
                "none",
                reason="exchange_status_unrecognized",
                verification_blocked=True,
            )

        if decision == "exchange_open":
            stale_protective = self.cancel_protective_order_when_sibling_closed(order)
            if stale_protective is not None:
                if stale_protective["cancelled"]:
                    self._mark_reconciled(order)
                    return self._repair_result("cancelled_stale_protective_leg")
                return self._repair_result(
                    "unresolved_linked_conditional_cancel_failed",
                    reason="stale_protective_leg_cancel_failed",
                    unresolved=True,
                )
            partial_repair = self._record_open_snapshot_fill_delta(order, snapshot)
            self.order_manager.mark_submitted(order, snapshot.exchange_order_id)
            if partial_repair["unresolved"]:
                return partial_repair
            if partial_repair["action"] != "none":
                self._mark_reconciled(order)
                return partial_repair
            self._mark_reconciled(order)
            return self._repair_result("restored_tracking")

        if decision == "exchange_closed":
            normalized_status = snapshot.status.lower()
            terminal_status = self._terminal_order_status(normalized_status)
            if terminal_status is None:
                return self._repair_result("none", reason="decision_not_repairable")

            fill_state, terminal_fill = self._classify_fill_delta(order, snapshot)
            if fill_state == FillDeltaState.LOCAL_OVERSTATED:
                return self._repair_result(
                    "unresolved_terminal_local_fill_exceeds_exchange",
                    reason="exchange_filled_quantity_less_than_local",
                    unresolved=True,
                )

            if fill_state == FillDeltaState.DELTA_UNPRICED:
                return self._repair_result(
                    "unresolved_missing_fill_price",
                    reason="exchange_snapshot_missing_fill_price",
                    unresolved=True,
                )

            if fill_state == FillDeltaState.DELTA_PRICED:
                priced_fill = cast(_PricedFill, terminal_fill)
                self.order_manager.record_fill_delta(
                    order,
                    priced_fill["price"],
                    priced_fill["quantity"],
                    snapshot.filled_quantity,
                    snapshot.average_price,
                    terminal_status=terminal_status,
                    fee=priced_fill["fee"],
                )
                cancel_failure = self.cancel_linked_conditional_for_protection_fill(
                    order
                )
                if cancel_failure is not None:
                    return self._repair_result(
                        "unresolved_linked_conditional_cancel_failed",
                        reason="sibling_cancel_failed_after_reconciled_protective_fill",
                        unresolved=True,
                    )
                self._mark_reconciled(order)
                return self._repair_result(
                    self._filled_terminal_repair_action(terminal_status),
                )

            order.status = terminal_status.value
            self.order_manager.repo.update_order(order)
            if terminal_status in {OrderStatus.CANCELLED, OrderStatus.FAILED}:
                self.fail_pending_conditionals_for_terminal_entry(order)
                protective_failure = self.protective_terminal_without_fill_failure(
                    order
                )
                if protective_failure is not None:
                    self._mark_reconciled(order)
                    return self._repair_result(
                        "unresolved_protective_terminal_without_fill",
                        reason="protective_terminal_without_fill",
                        unresolved=True,
                    )
            self._mark_reconciled(order)
            return self._repair_result(
                self._terminal_repair_action(terminal_status),
                reason=self._terminal_no_delta_reason(
                    normalized_status,
                    fill_state,
                ),
            )

        return self._repair_result("none", reason="decision_not_repairable")

    @staticmethod
    def _repair_result(
        action: str,
        *,
        reason: Optional[str] = None,
        unresolved: bool = False,
        verification_blocked: bool = False,
    ) -> dict[str, object]:
        """Build a reconciliation repair result.

        unresolved means an exchange snapshot proved an order/fill accounting
        mismatch that could not be repaired automatically. verification_blocked
        means reconciliation could not verify exchange state, so operators must
        resolve or explicitly waive the unknown state before production gates.
        """
        return {
            "action": action,
            "reason": reason,
            "unresolved": unresolved,
            "verification_blocked": verification_blocked,
        }

    @staticmethod
    def _terminal_order_status(
        normalized_exchange_status: str,
    ) -> Optional[OrderStatus]:
        if normalized_exchange_status in {"closed", "filled"}:
            return OrderStatus.FILLED
        if normalized_exchange_status in {"canceled", "cancelled"}:
            return OrderStatus.CANCELLED
        if normalized_exchange_status in {"rejected", "expired", "failed"}:
            return OrderStatus.FAILED
        return None

    @staticmethod
    def _terminal_repair_action(status: OrderStatus) -> str:
        if status == OrderStatus.CANCELLED:
            return "marked_cancelled"
        if status == OrderStatus.FAILED:
            return "marked_failed"
        return "marked_filled_without_fill"

    @staticmethod
    def _filled_terminal_repair_action(status: OrderStatus) -> str:
        if status == OrderStatus.CANCELLED:
            return "filled_delta_and_marked_cancelled"
        if status == OrderStatus.FAILED:
            return "filled_delta_and_marked_failed"
        return "filled_from_exchange_snapshot"

    @staticmethod
    def _terminal_no_delta_reason(
        normalized_exchange_status: str,
        fill_state: FillDeltaState,
    ) -> Optional[str]:
        if fill_state == FillDeltaState.CONVERGED:
            return "exchange_fill_already_recorded"
        if normalized_exchange_status in {"closed", "filled"}:
            return "exchange_snapshot_missing_fill_details"
        return None

    @staticmethod
    def _snapshot_fill_delta(order, snapshot) -> Optional[dict[str, Optional[Decimal]]]:
        return snapshot_fill_delta(
            local_filled=order.filled_quantity or Decimal("0"),
            local_average_price=order.filled_price,
            cumulative_filled=snapshot.filled_quantity,
            cumulative_average_price=snapshot.average_price,
            cumulative_fee=snapshot.fee,
        )

    @classmethod
    def _classify_fill_delta(
        cls,
        order,
        snapshot,
    ) -> tuple[FillDeltaState, Optional[dict[str, Optional[Decimal]]]]:
        return classify_fill_delta(
            local_filled=order.filled_quantity or Decimal("0"),
            local_average_price=order.filled_price,
            cumulative_filled=snapshot.filled_quantity,
            cumulative_average_price=snapshot.average_price,
            cumulative_fee=snapshot.fee,
        )

    def _record_open_snapshot_fill_delta(self, order, snapshot) -> dict[str, object]:
        fill_state, fill_delta = self._classify_fill_delta(order, snapshot)
        if fill_state == FillDeltaState.NO_FILL:
            return self._repair_result("none")
        if fill_state == FillDeltaState.LOCAL_OVERSTATED:
            return self._repair_result(
                "unresolved_open_local_fill_exceeds_exchange",
                reason="exchange_filled_quantity_less_than_local",
                unresolved=True,
            )
        if fill_state == FillDeltaState.CONVERGED:
            return self._repair_result(
                "none",
                reason="exchange_fill_already_recorded",
            )

        if fill_state == FillDeltaState.DELTA_UNPRICED:
            return self._repair_result(
                "unresolved_open_missing_fill_price",
                reason="exchange_snapshot_missing_fill_price",
                unresolved=True,
            )

        priced_fill = cast(_PricedFill, fill_delta)
        self.order_manager.record_partial_fill(
            order,
            priced_fill["price"],
            priced_fill["quantity"],
            snapshot.filled_quantity,
            snapshot.average_price,
            fee=priced_fill["fee"],
        )
        return self._repair_result("recorded_partial_fill_and_restored_tracking")

    def _mark_reconciled(self, order) -> None:
        order.last_reconciled_at = datetime.fromtimestamp(
            self.clock.now(), timezone.utc
        )
        self.order_manager.repo.update_order(order)

    @staticmethod
    def _reconcile_decision(local_status: str, exchange_status: Optional[str]) -> str:
        if exchange_status is None:
            if local_status == OrderStatus.NEW.value:
                return "local_only"
            return "exchange_unknown"

        normalized_exchange_status = exchange_status.lower()
        if normalized_exchange_status in {
            "open",
            "new",
            "submitted",
            "partially_filled",
            "submitted_unconfirmed",
        }:
            return "exchange_open"
        if normalized_exchange_status in {
            "closed",
            "filled",
            "canceled",
            "cancelled",
            "rejected",
            "expired",
            "failed",
        }:
            return "exchange_closed"
        return "exchange_unknown"

    @staticmethod
    def _is_pending_protective_order(order) -> bool:
        return (
            order.status == OrderStatus.NEW.value
            and isinstance(order.intent_payload, dict)
            and bool(order.intent_payload.get("pending_entry_order_id"))
        )
