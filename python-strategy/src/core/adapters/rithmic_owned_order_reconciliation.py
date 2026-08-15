from typing import Any, Callable, Protocol, cast

from src.core.adapters.rithmic_recovery import (
    build_rithmic_recovery_plan,
    compare_rithmic_positions,
    load_rithmic_recovery_snapshot,
)
from src.core.audit_service import write_system_event
from src.core.interfaces.exchange import (
    ExchangeError,
    IExchangeAdapter,
    OwnedOrderReconciliationContext,
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


class _RecoveryOrderIdentity(Protocol):
    id: object
    client_order_id: object


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


class RithmicOwnedOrderReconciler:
    """Own the complete Rithmic order-recovery and ledger-verification flow."""

    def __init__(
        self,
        *,
        adapter: IExchangeAdapter,
        profile: str,
        account_id: str | None,
        context: OwnedOrderReconciliationContext,
    ) -> None:
        self.adapter = adapter
        self.profile = profile
        self.account_id = account_id
        self.context = context

    def reconcile(
        self,
        *,
        snapshot_loader: Callable[..., Any] | None = None,
    ) -> dict[str, object]:
        """Repair recent FluxTrade-owned orders from one Rithmic snapshot."""
        if self.context.db_session_factory is None:
            raise RuntimeError(
                "reconcile_rithmic_owned_orders requires db_session_factory"
            )

        orders = [
            order
            for order in self.context.list_recoverable_client_orders()
            if str(order.exchange_id).lower() == "rithmic"
        ]
        profile = self.profile
        account_id = self.account_id
        if (
            not isinstance(profile, str)
            or not profile.strip()
            or not isinstance(account_id, str)
            or not account_id.strip()
        ):
            return self._write_identity_failure_audit(
                orders,
                "configured_account_identity_missing",
            )
        profile = profile.strip()
        account_id = account_id.strip()
        identity_failures = {
            str(order.id): reason
            for order in orders
            if (
                reason := self._order_identity_failure(
                    order,
                    profile,
                    account_id,
                )
            )
            is not None
        }
        if identity_failures:
            return self._write_identity_failure_audit(
                orders,
                "account_identity_batch_blocked",
                identity_failures=identity_failures,
            )
        try:
            self.adapter.restore_order_groups(orders)
        except ExchangeError as exc:
            return self._write_identity_failure_audit(
                orders,
                f"native_order_group_restore_failed:{exc}",
            )
        try:
            snapshot = load_rithmic_recovery_snapshot(
                profile,
                account_id,
                orders,
                int(self.context.now_seconds()),
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
            self.context.logger.error(
                "Rithmic ledger snapshot acquisition failed",
                extra=snapshot_diagnostics,
            )
            return self._write_recovery_audit(
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

        if snapshot.account_id != account_id:
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

        planned_payload = self._recovery_payload(orders, plan, external_orders)
        if any(item.event is not None for item in recovery_plan):
            self._write_recovery_audit(planned_payload, phase="planned")
            for item, result in zip(recovery_plan, plan, strict=True):
                if item.event is None:
                    continue
                applied = self.context.process_exchange_order_event(
                    item.event,
                    allow_remote_side_effects=False,
                )
                action = str(applied["action"])
                result["repair_action"] = action
                if action not in {
                    "applied",
                    "applied_position_cache_failed",
                    "unresolved_remote_actions_suppressed",
                }:
                    result["classification"] = "unresolved"
                    result["reason"] = f"event_application_{action}"
                    result["verification_blocked"] = True
                    result["unresolved"] = True
                else:
                    result["unresolved"] = (
                        item.unresolved
                        or action == "unresolved_remote_actions_suppressed"
                    )

        ledger_verification = self._verify_ledger(
            orders,
            snapshot,
            expected_account_id=account_id,
        )
        payload = self._recovery_payload(
            orders,
            plan,
            external_orders,
            ledger_verification,
        )
        return self._write_recovery_audit(payload, phase="completed")

    def _write_identity_failure_audit(
        self,
        orders: list[Any],
        reason: str,
        *,
        identity_failures: dict[str, str] | None = None,
    ) -> dict[str, object]:
        return self._write_recovery_audit(
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
    def _order_identity_failure(
        order: Any,
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
    def _recovery_payload(
        orders: list[Any],
        plan: list[dict[str, Any]],
        external_orders: list[Any],
        ledger_verification: dict[str, Any] | None = None,
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

    def _verify_ledger(
        self,
        orders: list[Any],
        snapshot: Any,
        *,
        expected_account_id: str,
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
        result: dict[str, Any] = {
            "account_id": snapshot.account_id,
            "account_currency": snapshot.account_currency,
            "account_summary": account_state,
            "position_drifts": [],
            "errors": [],
            "verification_blocked": False,
        }
        if not snapshot.account_currency:
            result["errors"].append("remote_account_currency_missing")
        if snapshot.account_id != expected_account_id:
            result["errors"].append("remote_account_id_mismatch")
        if account_state is None:
            result["errors"].append("remote_account_summary_missing")
        elif not any(value is not None for value in account_state.values()):
            result["errors"].append("remote_account_summary_empty")
        if self.context.local_positions_loader is None:
            result["errors"].append("local_positions_loader_missing")
        else:
            try:
                result["position_drifts"] = compare_rithmic_positions(
                    orders,
                    self.context.local_positions_loader(),
                    snapshot.positions,
                )
            except Exception as exc:
                self.context.logger.error(
                    "Rithmic position verification failed: %s", exc
                )
                result["errors"].append("position_verification_failed")
        result["verification_blocked"] = bool(
            result["errors"] or result["position_drifts"]
        )
        return result

    def _write_recovery_audit(
        self,
        payload: dict[str, object],
        *,
        phase: str = "completed",
    ) -> dict[str, object]:
        db_session_factory = self.context.db_session_factory
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
