"""Compose the venue-owned Rithmic runtime service graph."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import logging
import math
import os
import threading
from typing import Any

from src.core.adapters.rithmic_adapter import RithmicExchangeAdapter
from src.core.adapters.rithmic_emergency_flatten import (
    RithmicEmergencyFlattenService,
)
from src.core.adapters.rithmic_external_order_drift import (
    RithmicExternalOrderDriftService,
)
from src.core.adapters.rithmic_kill_switch_clear import (
    RithmicKillSwitchClearPreparationService,
)
from src.core.adapters.rithmic_ledger_recovery import (
    RithmicLedgerRecoveryService,
)
from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)
from src.core.adapters.rithmic_order_event_stream import (
    RithmicOrderEventStreamService,
)
from src.core.adapters.rithmic_order_reconnect import (
    RithmicOrderReconnectService,
)
from src.core.adapters.rithmic_portfolio_exit import RithmicPortfolioExitService
from src.core.adapters.rithmic_runtime_recovery import (
    RithmicRuntimeRecoveryService,
)
from src.core.adapters.rithmic_strategy_exit import RithmicStrategyExitService
from src.core.execution import ExecutionEngine, ExitDecision
from src.core.interfaces import IExchangeAdapter
from src.core.models import Candlestick, Signal, SignalType
from src.core.ops_safety import OpsSafetyService
from src.core.risk_manager import AccountService
from src.core.runtime_capabilities import (
    RuntimeCallbacks as RithmicRuntimeCallbacks,
    StartupReconciliationState,
)
from src.core.runtime_environment import RuntimeEnvironment


def _positive_env_float(name: str, default: str) -> float:
    raw_value = os.getenv(name, default)
    try:
        value = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a finite number greater than zero") from exc
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a finite number greater than zero")
    return value


def _rithmic_account_snapshot_max_age_from_env() -> float:
    return _positive_env_float("RITHMIC_ACCOUNT_SNAPSHOT_MAX_AGE_SECONDS", "600")


def _rithmic_runtime_reconciliation_interval_from_env() -> float:
    return _positive_env_float("RITHMIC_RUNTIME_RECONCILE_INTERVAL_SECONDS", "300")


@dataclass(frozen=True)
class RithmicRuntimeBootstrap:
    """Venue-owned identity and deferred schedule selected during startup."""

    profile: str | None
    account_id: str | None
    is_rithmic_runtime: bool
    _interval_resolver: Callable[[], float] | None = None

    def resolve_reconciliation_schedule(
        self,
        *,
        generic_enabled: bool,
        generic_interval_resolver: Callable[[], float],
    ) -> tuple[bool, float | None]:
        """Apply a Rithmic schedule override without owning generic policy."""
        if self.is_rithmic_runtime:
            if self._interval_resolver is None:
                raise RuntimeError("Rithmic reconciliation schedule is unavailable")
            return True, self._interval_resolver()
        if not generic_enabled:
            return False, None
        return True, generic_interval_resolver()


def prepare_rithmic_runtime_bootstrap(
    *,
    adapter: IExchangeAdapter,
    adapter_config: dict[str, Any],
    audit_external_orders: bool,
    account_service: AccountService,
    runtime_environment: RuntimeEnvironment,
) -> RithmicRuntimeBootstrap:
    """Prepare venue identity and account authority without external I/O."""
    if not isinstance(adapter, RithmicExchangeAdapter):
        return RithmicRuntimeBootstrap(
            profile=None,
            account_id=None,
            is_rithmic_runtime=False,
        )
    profile = adapter_config.get("rithmic_recovery_profile") or adapter_config.get(
        "rithmic_profile"
    )
    account_id = adapter_config.get(
        "rithmic_recovery_account_id"
    ) or adapter_config.get("account_id")
    if not audit_external_orders:
        raise ValueError("Rithmic live trading requires audit_external_orders")

    profile = profile or adapter.profile
    raw_account_id = account_id or adapter.account_id
    if not isinstance(raw_account_id, str):
        raise ValueError("Rithmic recovery account must match order adapter account")
    account_id = raw_account_id.strip()
    if not account_id or account_id != adapter.account_id:
        raise ValueError("Rithmic recovery account must match order adapter account")

    account_service.configure_authoritative_balance(
        venue="rithmic",
        account_id=account_id,
        max_age_seconds=_rithmic_account_snapshot_max_age_from_env(),
        runtime_environment=runtime_environment,
    )
    return RithmicRuntimeBootstrap(
        profile=profile,
        account_id=account_id,
        is_rithmic_runtime=True,
        _interval_resolver=_rithmic_runtime_reconciliation_interval_from_env,
    )


@dataclass
class RithmicRuntimeOwners:
    """Current concrete services owned by one Rithmic runtime composition."""

    order_event_lifecycle: RithmicOrderEventLifecycleGate
    ledger_recovery: RithmicLedgerRecoveryService | None = None
    order_reconnect: RithmicOrderReconnectService | None = None
    runtime_recovery: RithmicRuntimeRecoveryService | None = None
    external_order_drift: RithmicExternalOrderDriftService | None = None
    strategy_exit: RithmicStrategyExitService | None = None
    order_event_stream: RithmicOrderEventStreamService | None = None
    kill_switch_clear_preparation: RithmicKillSwitchClearPreparationService | None = (
        None
    )
    emergency_flatten: RithmicEmergencyFlattenService | None = None
    portfolio_exit_factory: (
        Callable[[Callable[[str], str | None]], RithmicPortfolioExitService] | None
    ) = None
    is_rithmic_runtime: bool = False
    profile: str | None = None
    account_id: str | None = None

    def start_order_event_stream(self) -> bool:
        """Start the current venue stream owner, if configured."""
        if self.order_event_stream is None:
            return False
        self.order_event_stream.start()
        return True

    def on_order_runtime_started(self) -> None:
        """Baseline the current reconnect owner after a successful start."""
        if self.order_reconnect is None:
            return
        self.order_reconnect.on_runtime_started()

    def reconcile_order_reconnect(self) -> bool | None:
        """Run the current venue reconnect policy, or report unavailable."""
        if not self.is_rithmic_runtime:
            return True
        if self.order_reconnect is None:
            return None
        return self.order_reconnect.reconcile_if_needed()

    def detect_external_order_drift(self, reason: str) -> None:
        """Route an external-order finding to the current drift owner."""
        if self.external_order_drift is None:
            raise RuntimeError("Rithmic external-order drift owner is unavailable")
        self.external_order_drift.detect(reason)

    def prepare_kill_switch_clear(self) -> tuple[bool, int | None]:
        """Prepare a clear or preserve the non-Rithmic compatibility default."""
        if self.kill_switch_clear_preparation is None:
            return True, None
        return self.kill_switch_clear_preparation.prepare()

    def current_external_order_drift_generation(self) -> int:
        """Return the current drift generation or its compatibility default."""
        if self.external_order_drift is None:
            return 0
        return self.external_order_drift.current_generation()

    def finalize_external_order_drift_clear(
        self,
        *,
        prepared_generation: int,
        clear_succeeded: bool,
    ) -> None:
        """Finalize a prepared clear through the current drift owner."""
        if self.external_order_drift is None:
            raise RuntimeError("Rithmic external-order drift owner is unavailable")
        self.external_order_drift.finalize_clear(
            prepared_generation=prepared_generation,
            clear_succeeded=clear_succeeded,
        )

    def reconcile_startup(
        self,
        fallback: Callable[[], dict[str, Any] | None],
    ) -> dict[str, Any] | None:
        """Dispatch startup recovery through the current venue owner."""
        if not self.is_rithmic_runtime:
            return fallback()
        if self.ledger_recovery is None:
            raise RuntimeError("rithmic_ledger_recovery_unavailable")
        return self.ledger_recovery.reconcile_startup()

    def publish_authoritative_summary(self, summary: dict[str, Any]) -> None:
        """Publish through the current venue ledger owner."""
        if self.ledger_recovery is None:
            raise RuntimeError("rithmic_ledger_recovery_unavailable")
        self.ledger_recovery.publish_authoritative_summary(summary)

    def runtime_recovery_operation(self) -> Callable[[], bool]:
        """Resolve the current periodic venue recovery operation."""
        if self.runtime_recovery is None:
            raise RuntimeError("rithmic_runtime_reconciliation_unavailable")
        return self.runtime_recovery.run_once

    def select_runtime_reconciliation(
        self,
        fallback: Callable[[], object],
        run_exclusive: Callable[[Callable[[], object]], object],
    ) -> tuple[bool, Callable[[], object]]:
        """Select generic reconciliation or a dynamically resolved venue owner."""
        if not self.is_rithmic_runtime:
            return False, fallback

        def run_current_owner() -> object:
            operation = self.runtime_recovery_operation()
            return run_exclusive(operation)

        return True, run_current_owner

    def run_startup_balance_reconciliation(
        self,
        fallback: Callable[[], object],
    ) -> object | None:
        """Run generic balance startup unless the venue ledger owns it."""
        if self.is_rithmic_runtime:
            return None
        return fallback()

    def classify_startup_reconciliation(
        self,
        summary: Any,
    ) -> StartupReconciliationState:
        """Classify startup recovery without moving generic resume policy."""
        if not self.is_rithmic_runtime:
            return StartupReconciliationState(False, False, None)
        entry_admission_safe = bool(summary and summary.get("auto_resume_safe") is True)
        return StartupReconciliationState(
            owner_handled=True,
            entry_admission_safe=entry_admission_safe,
            blocking_reason=(
                None if entry_admission_safe else "rithmic_reconciliation_blocked"
            ),
        )

    def execute_strategy_exit(
        self,
        signal: Signal,
        decision: ExitDecision,
    ) -> dict[str, object]:
        """Dispatch one authoritative strategy exit to the current owner."""
        if self.strategy_exit is None:
            raise RuntimeError("rithmic_strategy_exit_unavailable")
        return self.strategy_exit.execute(signal, decision)

    def execute_emergency_flatten(
        self,
        *,
        actor: str,
        reason: str | None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        """Dispatch emergency flatten to the current venue owner."""
        if self.emergency_flatten is None:
            raise RuntimeError("rithmic_emergency_flatten_unavailable")
        return self.emergency_flatten.execute(
            actor=actor,
            reason=reason,
            operation_id=operation_id,
        )

    def run_emergency_flatten(
        self,
        fallback: Callable[[], dict[str, Any]],
        *,
        actor: str,
        reason: str | None,
        operation_id: str | None = None,
    ) -> dict[str, Any]:
        """Select generic flatten or the current venue owner."""
        if not self.is_rithmic_runtime:
            return fallback()
        return self.execute_emergency_flatten(
            actor=actor,
            reason=reason,
            operation_id=operation_id,
        )

    def requires_authoritative_flatten_verification(self) -> bool:
        """Declare whether completion requires venue-authoritative proof."""
        return self.is_rithmic_runtime is True

    def execute_portfolio_exit(
        self,
        signal: Signal,
        decision: ExitDecision,
        candle: Candlestick | None,
        portfolio_id_for_sleeve: Callable[[str], str | None],
    ) -> dict[str, object]:
        """Build and execute one portfolio exit through the venue facade."""
        if not isinstance(self.profile, str) or not isinstance(self.account_id, str):
            raise RuntimeError("rithmic_portfolio_exit_account_identity_missing")
        if self.portfolio_exit_factory is None:
            raise RuntimeError("rithmic_portfolio_exit_unavailable")
        owner = self.portfolio_exit_factory(portfolio_id_for_sleeve)
        return owner.execute(signal, decision, candle)

    def route_authoritative_exit(
        self,
        signal: Signal,
        candle: Candlestick | None,
        portfolio_id_for_sleeve: Callable[[str], str | None],
        execute_authoritative_exit_signal: Callable[
            [
                Signal,
                Candlestick | None,
                Callable[[Signal, ExitDecision], dict[str, object]],
            ],
            bool,
        ],
    ) -> tuple[bool, bool]:
        """Route one configured venue exit and report whether it was handled."""
        if not self.is_rithmic_runtime or signal.type not in (
            SignalType.EXIT_LONG,
            SignalType.EXIT_SHORT,
        ):
            return False, False

        portfolio_id = portfolio_id_for_sleeve(signal.strategy_id)
        exit_executor = self.execute_strategy_exit
        if portfolio_id is not None:

            def execute_portfolio_exit(
                routed_signal: Signal,
                decision: ExitDecision,
            ) -> dict[str, object]:
                return self.execute_portfolio_exit(
                    routed_signal,
                    decision,
                    candle,
                    portfolio_id_for_sleeve,
                )

            exit_executor = execute_portfolio_exit
        return True, execute_authoritative_exit_signal(signal, candle, exit_executor)


def build_rithmic_portfolio_exit_owner(
    *,
    adapter: RithmicExchangeAdapter,
    execution_engine: ExecutionEngine,
    account_service: AccountService,
    profile: str,
    account_id: str,
    operation_gate: RithmicOrderEventLifecycleGate,
    stop_order_event_stream: Callable[..., bool],
    assert_leadership: Callable[[], None],
    restart_order_stream: Callable[[], None],
    lockdown: Callable[[str], None],
    schedule_emergency_flatten: Callable[[str], None],
    portfolio_id_for_sleeve: Callable[[str], str | None],
) -> RithmicPortfolioExitService:
    """Build one portfolio-exit owner at the venue composition boundary."""
    return RithmicPortfolioExitService(
        adapter=adapter,
        execution_engine=execution_engine,
        account_service=account_service,
        profile=profile,
        account_id=account_id,
        operation_gate=operation_gate,
        stop_order_event_stream=stop_order_event_stream,
        assert_leadership=assert_leadership,
        restart_order_stream=restart_order_stream,
        lockdown=lockdown,
        schedule_emergency_flatten=schedule_emergency_flatten,
        portfolio_id_for_sleeve=portfolio_id_for_sleeve,
    )


def build_rithmic_runtime_owners(
    *,
    adapter: IExchangeAdapter,
    profile: str | None,
    account_id: str | None,
    execution_engine: ExecutionEngine,
    account_service: AccountService,
    ops_safety: OpsSafetyService,
    stop_event: threading.Event,
    callbacks: RithmicRuntimeCallbacks,
    logger: logging.Logger,
) -> RithmicRuntimeOwners:
    """Build Rithmic owners without performing external I/O."""

    operation_gate = RithmicOrderEventLifecycleGate()
    if not isinstance(adapter, RithmicExchangeAdapter):
        return RithmicRuntimeOwners(order_event_lifecycle=operation_gate)
    ledger_recovery = (
        RithmicLedgerRecoveryService(
            profile=profile or "",
            account_id=account_id,
            reconcile_owned_orders=lambda owner_profile, owner_account_id: (
                execution_engine.reconcile_rithmic_owned_orders(
                    owner_profile,
                    owner_account_id,
                )
            ),
            now_seconds=lambda: execution_engine.clock.now(),
            publish_authoritative_balance=lambda **values: (
                account_service.replace_authoritative_balance(**values)
            ),
            logger=logger,
        )
        if profile
        else None
    )
    order_reconnect = (
        RithmicOrderReconnectService(
            adapter=adapter,
            profile=profile or "",
            account_id=account_id,
            audit_external_orders=lambda: execution_engine.audit_external_orders,
            reconcile_owned_orders=lambda owner_profile, owner_account_id: (
                execution_engine.reconcile_rithmic_owned_orders(
                    owner_profile,
                    owner_account_id,
                )
            ),
            publish_authoritative_summary=(
                ledger_recovery.publish_authoritative_summary
            ),
            halt_for_reconcile=lambda **values: (
                execution_engine.halt_for_reconcile(**values)
            ),
            resume_after_reconcile=lambda: (execution_engine.resume_after_reconcile()),
            assert_runtime_leadership=callbacks.assert_runtime_leadership,
            logger=logger,
        )
        if ledger_recovery is not None
        else None
    )
    runtime_recovery = (
        RithmicRuntimeRecoveryService(
            adapter=adapter,
            profile=profile or "",
            account_id=account_id,
            halt_for_reconcile=lambda **values: (
                execution_engine.halt_for_reconcile(**values)
            ),
            stop_order_event_stream=callbacks.stop_order_event_stream,
            reconcile_owned_orders=lambda owner_profile, owner_account_id: (
                execution_engine.reconcile_rithmic_owned_orders(
                    owner_profile,
                    owner_account_id,
                )
            ),
            publish_authoritative_summary=(
                ledger_recovery.publish_authoritative_summary
            ),
            assert_runtime_leadership=callbacks.assert_runtime_leadership,
            start_order_event_stream=callbacks.start_order_event_stream,
            resume_after_reconcile=lambda: (execution_engine.resume_after_reconcile()),
            lockdown=callbacks.lockdown,
            logger=logger,
        )
        if ledger_recovery is not None
        else None
    )
    external_order_drift = RithmicExternalOrderDriftService(
        halt_submissions=callbacks.halt_submissions,
        clear_local_halt=callbacks.clear_local_halt,
        persist_lockdown_state=callbacks.persist_lockdown_state,
        persist_redis_lockdown=callbacks.persist_redis_lockdown,
        assert_runtime_leadership=callbacks.assert_runtime_leadership,
        resume_after_reconcile=lambda: execution_engine.resume_after_reconcile(),
        logger=logger,
    )
    strategy_exit = RithmicStrategyExitService(
        adapter=adapter,
        execution_engine=execution_engine,
        account_service=account_service,
        profile=profile or "",
        account_id=account_id,
        operation_gate=operation_gate,
        stop_order_event_stream=callbacks.stop_order_event_stream,
        assert_leadership=callbacks.assert_runtime_leadership,
        restart_order_stream=callbacks.start_order_event_stream,
        lockdown=callbacks.lockdown,
        logger=logger,
    )
    order_event_stream = RithmicOrderEventStreamService(
        adapter=adapter,
        stop_event=stop_event,
        is_running=callbacks.is_running,
        publish_worker=callbacks.publish_worker,
        reconcile_if_needed=callbacks.reconcile_if_needed,
        process_event=callbacks.process_event,
        lockdown=callbacks.lockdown,
        assert_runtime_leadership=callbacks.assert_runtime_leadership,
        halt_submissions=callbacks.halt_submissions,
        on_runtime_started=callbacks.on_runtime_started,
        logger=logger,
    )
    kill_switch_clear_preparation = RithmicKillSwitchClearPreparationService(
        adapter=adapter,
        profile=profile or "",
        account_id=account_id,
        operation_gate=operation_gate,
        set_order_event_stop=stop_event.set,
        clear_order_event_stop=stop_event.clear,
        current_order_event_thread=callbacks.current_order_event_thread,
        halt_for_reconcile=lambda **values: (
            execution_engine.halt_for_reconcile(**values)
        ),
        reconcile_owned_orders=lambda owner_profile, owner_account_id: (
            execution_engine.reconcile_rithmic_owned_orders(
                owner_profile,
                owner_account_id,
            )
        ),
        publish_authoritative_summary=callbacks.publish_authoritative_summary,
        current_drift_generation=lambda: external_order_drift.current_generation(),
        assert_runtime_leadership=callbacks.assert_runtime_leadership,
        start_order_event_stream=callbacks.start_order_event_stream,
        resume_after_reconcile=lambda: execution_engine.resume_after_reconcile(),
        logger=logger,
    )
    emergency_flatten = RithmicEmergencyFlattenService(
        adapter=adapter,
        execution_engine=execution_engine,
        account_service=account_service,
        ops_safety=ops_safety,
        profile=profile or "",
        account_id=account_id,
        operation_gate=operation_gate,
        stop_current_worker=callbacks.stop_order_event_stream,
        clear_polling_stop=stop_event.clear,
        restart_generic_worker=callbacks.start_order_event_stream,
        run_when_submissions_drained=execution_engine.run_when_submissions_drained,
        logger=logger,
    )

    def build_portfolio_exit(
        portfolio_id_for_sleeve: Callable[[str], str | None],
    ) -> RithmicPortfolioExitService:
        if account_id is None:
            raise ValueError("rithmic portfolio exit requires account identity")
        return build_rithmic_portfolio_exit_owner(
            adapter=adapter,
            execution_engine=execution_engine,
            account_service=account_service,
            profile=profile or "",
            account_id=account_id,
            operation_gate=operation_gate,
            stop_order_event_stream=callbacks.stop_order_event_stream,
            assert_leadership=callbacks.assert_runtime_leadership,
            restart_order_stream=callbacks.start_order_event_stream,
            lockdown=callbacks.lockdown,
            schedule_emergency_flatten=(
                emergency_flatten.schedule_portfolio_exit_compensation
            ),
            portfolio_id_for_sleeve=portfolio_id_for_sleeve,
        )

    return RithmicRuntimeOwners(
        order_event_lifecycle=operation_gate,
        ledger_recovery=ledger_recovery,
        order_reconnect=order_reconnect,
        runtime_recovery=runtime_recovery,
        external_order_drift=external_order_drift,
        strategy_exit=strategy_exit,
        order_event_stream=order_event_stream,
        kill_switch_clear_preparation=kill_switch_clear_preparation,
        emergency_flatten=emergency_flatten,
        portfolio_exit_factory=build_portfolio_exit,
        is_rithmic_runtime=True,
        profile=profile,
        account_id=account_id,
    )
