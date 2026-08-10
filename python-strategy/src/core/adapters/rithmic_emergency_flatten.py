"""Authoritative Rithmic emergency flatten and compensation owner."""

from __future__ import annotations

from collections.abc import Callable
from logging import Logger
from typing import Any

from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.adapters.rithmic_recovery import (
    load_rithmic_recovery_snapshot,
    rithmic_order_may_be_working,
)
from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)
from src.core.execution import ExecutionEngine
from src.core.ops_safety import OpsSafetyService


def _empty_result() -> dict[str, Any]:
    return {
        "cancelled_orders": 0,
        "cancel_failures": [],
        "flattened_positions": 0,
        "flatten_pending": [],
        "flatten_failures": [],
        "recovery_failures": [],
        "already_flat": False,
        "drain_timeout": False,
    }


def _merge_results(
    current: dict[str, Any] | None,
    update: dict[str, Any],
) -> dict[str, Any]:
    if current is None:
        return dict(update)
    for key in ("cancelled_orders", "flattened_positions"):
        current[key] = int(current.get(key, 0)) + int(update.get(key, 0))
    for key in (
        "cancel_failures",
        "flatten_pending",
        "flatten_failures",
        "recovery_failures",
    ):
        current.setdefault(key, []).extend(update.get(key, []))
    current["drain_timeout"] = bool(
        current.get("drain_timeout") or update.get("drain_timeout")
    )
    current["already_flat"] = bool(
        current.get("already_flat") and update.get("already_flat")
    )
    return current


class RithmicEmergencyFlattenService:
    """Own the Rithmic kill-switch flatten lifecycle and compensation queue."""

    def __init__(
        self,
        *,
        adapter: RithmicExchangeAdapter,
        execution_engine: ExecutionEngine,
        account_service: Any,
        ops_safety: OpsSafetyService,
        profile: str,
        account_id: str | None,
        operation_gate: RithmicOrderEventLifecycleGate,
        stop_current_worker: Callable[..., bool],
        clear_polling_stop: Callable[[], None],
        restart_generic_worker: Callable[[], None],
        run_when_submissions_drained: Callable[[Callable[[], None]], None],
        logger: Logger,
    ) -> None:
        if not profile or not account_id:
            raise ValueError("rithmic emergency flatten requires account identity")
        self.adapter = adapter
        self.execution_engine = execution_engine
        self.account_service = account_service
        self.ops_safety = ops_safety
        self.profile = profile
        self.account_id = account_id
        self.operation_gate = operation_gate
        self.stop_current_worker = stop_current_worker
        self.clear_polling_stop = clear_polling_stop
        self.restart_generic_worker = restart_generic_worker
        self.run_when_submissions_drained = run_when_submissions_drained
        self.logger = logger

    def execute(
        self,
        *,
        actor: str,
        reason: str | None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        return self.operation_gate.run(
            self._execute_serialized,
            actor=actor,
            reason=reason,
            operation_id=operation_id,
        )

    def _execute_serialized(
        self,
        *,
        actor: str,
        reason: str | None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        aggregate: dict[str, Any] | None = None
        operation_failed = False

        def finalize(
            verified: bool,
            failure_reason: str | None = None,
        ) -> dict[str, Any]:
            nonlocal aggregate
            aggregate = aggregate or _empty_result()
            aggregate["authoritative_flatten_verified"] = verified
            if failure_reason is not None:
                aggregate["flatten_failures"].append(
                    {
                        "strategy_id": "LIVE",
                        "product_id": "unknown",
                        "reason": failure_reason,
                    }
                )
            audit_kwargs: dict[str, Any] = {
                "actor": actor,
                "reason": reason,
                "result": aggregate,
            }
            if operation_id is not None:
                audit_kwargs["operation_id"] = operation_id
            self.ops_safety.record_kill_switch_result(**audit_kwargs)
            return aggregate

        if not self.stop_current_worker(timeout=30.0):
            self.clear_polling_stop()
            finalize(
                False,
                "rithmic_emergency_flatten_event_stream_stop_timeout",
            )
            raise RuntimeError("rithmic_emergency_flatten_event_stream_stop_timeout")

        try:
            exit_attempts = 0
            exit_failed = False
            submit_exit = True
            for _verification_attempt in range(6):
                if submit_exit:
                    authoritative_kwargs: dict[str, Any] = {
                        "actor": actor,
                        "reason": reason,
                        "position_loader": self._load_positions,
                        "account_id": self.adapter.account_id,
                    }
                    if operation_id is not None:
                        authoritative_kwargs["operation_id"] = operation_id
                    result = self.ops_safety.kill_switch_with_authoritative_positions(
                        **authoritative_kwargs
                    )
                    aggregate = _merge_results(aggregate, result)
                    exit_attempts += 1
                    exit_failed = bool(
                        result.get("drain_timeout")
                        or result.get("flatten_pending")
                        or result.get("flatten_failures")
                    )
                    if result.get("drain_timeout"):
                        break

                snapshot = self._load_snapshot()
                reconciliation = self._reconcile(snapshot)
                remaining_positions = self.adapter.positions_from_ledger_snapshot(
                    snapshot
                )
                working_orders_remain = any(
                    rithmic_order_may_be_working(order) for order in snapshot.orders
                )
                if not working_orders_remain:
                    self.account_service.replace_positions_for_products(
                        remaining_positions,
                        self.adapter.configured_product_ids,
                        timestamp_ms=int(self.execution_engine.clock.now() * 1000),
                    )
                    reconciliation = self._reconcile(snapshot)
                    if (
                        not remaining_positions
                        and reconciliation.get("auto_resume_safe") is True
                    ):
                        return finalize(True)

                if working_orders_remain:
                    if exit_failed:
                        break
                    submit_exit = False
                    continue
                if (
                    exit_failed
                    or exit_attempts >= 3
                    or reconciliation.get("auto_resume_safe") is not True
                ):
                    break

                self.adapter.start_order_event_stream()
                submit_exit = True

            return finalize(
                False,
                "rithmic_authoritative_flatten_not_verified",
            )
        except Exception as error:
            operation_failed = True
            finalize(
                False,
                f"rithmic_authoritative_flatten_failed:{type(error).__name__}",
            )
            raise
        finally:
            try:
                self.restart_generic_worker()
            except Exception as restart_error:
                aggregate = aggregate or _empty_result()
                aggregate.setdefault("authoritative_flatten_verified", False)
                aggregate["recovery_failures"].append(
                    {
                        "reason": "rithmic_order_stream_restart_failed:"
                        f"{type(restart_error).__name__}"
                    }
                )
                audit_kwargs = {
                    "actor": actor,
                    "reason": reason,
                    "result": aggregate,
                }
                if operation_id is not None:
                    audit_kwargs["operation_id"] = operation_id
                self.ops_safety.record_kill_switch_result(**audit_kwargs)
                if operation_failed:
                    self.logger.exception(
                        "Order stream restart also failed after emergency flatten failure"
                    )
                else:
                    raise

    def schedule_portfolio_exit_compensation(self, reason: str) -> None:
        """Run or queue the same authoritative flatten after submission drain."""

        def compensate() -> None:
            self.execute(
                actor="engine",
                reason=f"portfolio_exit_compensation:{reason}",
            )

        self.run_when_submissions_drained(compensate)

    def _load_snapshot(self):
        self.adapter.close()
        recoverable_orders = [
            order
            for order in self.execution_engine.list_recoverable_client_orders()
            if str(order.exchange_id).lower() == "rithmic"
        ]
        return load_rithmic_recovery_snapshot(
            self.profile,
            self.account_id,
            recoverable_orders,
            int(self.execution_engine.clock.now()),
        )

    def _load_positions(self):
        snapshot = self._load_snapshot()
        if any(rithmic_order_may_be_working(order) for order in snapshot.orders):
            raise RuntimeError("rithmic_emergency_flatten_working_orders_remain")
        positions = self.adapter.positions_from_ledger_snapshot(snapshot)
        self.adapter.start_order_event_stream()
        return positions

    def _reconcile(self, snapshot) -> dict[str, Any]:
        return self.execution_engine.reconcile_rithmic_owned_orders(
            self.profile,
            self.account_id,
            snapshot_loader=lambda *_args, **_kwargs: snapshot,
        )
