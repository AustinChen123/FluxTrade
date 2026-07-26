import hashlib
import json
import math
import os
import time
import threading
import logging
import traceback
import uuid
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from decimal import Decimal
from typing import Callable, ContextManager, List, Union, Optional, Dict, Type
from sqlalchemy.orm import Session
from src.core.models import (
    Candlestick,
    OrderStatus,
    Trade,
    Signal,
    SignalType,
    StrategyStatus,
)
from src.core.orm_models import Candlestick as ORMCandlestick
from src.core.orm_models import StrategyState
from src.strategies.base import BaseStrategy
from src.core.risk_manager import RiskManager, AccountService
from src.core.execution import ExecutionEngine, ExitDecision
from src.core.clock import Clock
from src.core.interfaces import IExchangeAdapter, IOrderRepository
from src.core.daily_nav_snapshot import DailyNavSnapshotService
from src.core.strategy_loader import StrategyLoader
from src.core.data_provider import check_data_availability
from src.core.adapters import (
    CcxtExchangeAdapter,
    RithmicExchangeAdapter,
    RithmicUnmappedOrderEvent,
    SimulatedAdapter,
    create_adapter,
)
from src.core.adapters.rithmic_recovery import (
    load_rithmic_recovery_snapshot,
    rithmic_order_may_be_working,
)
from src.core.journal import StrategyJournal
from src.core.redis_factory import create_redis_client
from src.core.metrics import SIGNALS_TOTAL, ACTIVE_STRATEGIES, BALANCE_USDT
from src.core.command_router import CommandRouter
from src.core.health_monitor import HealthMonitor
from src.core.ops_safety import OpsSafetyService
from src.core.runtime_reconcile import RuntimeReconciliationJob
from src.core.signal_processor import SignalProcessor
from src.core.strategy_registry import StrategyRegistry
from src.core.strategy_state_manager import StrategyStateManager
from src.core.audit_service import build_signal_audit, commit_signal_audit
from src.core.runtime_environment import RuntimeEnvironment

HOT_STRATEGIES_PATH = os.getenv('HOT_STRATEGIES_PATH', '/app/strategies_hot')
_DEFAULT_RUNTIME_ENVIRONMENT = RuntimeEnvironment("live")
SYSTEM_STATE_KEY = _DEFAULT_RUNTIME_ENVIRONMENT.key("system:state")
SYSTEM_BOOT_STATE_KEY = _DEFAULT_RUNTIME_ENVIRONMENT.key("system:engine_boot_state")
SYSTEM_STATE_LOCKDOWN = "LOCKDOWN"
SYSTEM_STATE_OK = "OK"
_RITHMIC_SAFE_ORDER_EVENT_ACTIONS = frozenset({"applied"})
_KILL_SWITCH_IDEMPOTENCY_TTL_SECONDS = 86_400

logger = logging.getLogger(__name__)


def _kill_switch_result_is_complete(
    result: dict,
    *,
    authoritative_required: bool,
) -> bool:
    count_keys = ("cancelled_orders", "flattened_positions")
    list_keys = (
        "cancel_failures",
        "flatten_pending",
        "flatten_failures",
        "recovery_failures",
    )
    if any(
        type(result.get(key)) is not int or result[key] < 0
        for key in count_keys
    ):
        return False
    if any(not isinstance(result.get(key), list) for key in list_keys):
        return False
    if type(result.get("already_flat")) is not bool:
        return False
    if result.get("drain_timeout") is not False:
        return False
    if any(result[key] for key in list_keys):
        return False
    if authoritative_required:
        return result.get("authoritative_flatten_verified") is True
    return "authoritative_flatten_verified" not in result or (
        result["authoritative_flatten_verified"] is True
    )


def _merge_kill_switch_results(current: dict | None, update: dict) -> dict:
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


def _is_runtime_reconciliation_enabled(
    adapter: IExchangeAdapter,
    adapter_config: Optional[Dict],
) -> bool:
    if isinstance(adapter, SimulatedAdapter):
        return False
    if isinstance(adapter, CcxtExchangeAdapter):
        return True
    if isinstance(adapter, RithmicExchangeAdapter):
        return False
    return bool(adapter_config and adapter_config.get("mode") == "live")


def _runtime_reconciliation_interval_from_env() -> float:
    name = "RUNTIME_RECONCILE_INTERVAL_SECONDS"
    raw_value = os.getenv(name, "3600")
    try:
        interval = float(raw_value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a finite number greater than zero") from exc
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError(f"{name} must be a finite number greater than zero")
    return interval


class _EngineLifecycleAdapter:
    """Expose engine lifecycle orchestration through CommandRouter's transition API."""

    def __init__(self, engine: "StrategyEngine") -> None:
        self._engine = engine

    def transition_to_running(self, strategy_id: str, **kwargs) -> None:
        self._engine.activate_strategy(strategy_id, **kwargs)

    def transition_to_stopped(self, strategy_id: str, **kwargs) -> None:
        self._engine.deactivate_strategy(strategy_id, **kwargs)

    def is_running(self, strategy_id: str) -> bool:
        return strategy_id in self._engine.strategy_instances


class StrategyEngine:
    def __init__(
        self,
        db_session: Session,
        clock: Clock,
        order_repository: Optional[IOrderRepository] = None,
        account_service: Optional[AccountService] = None,
        adapter_config: Optional[Dict] = None,
        adapter: Optional[IExchangeAdapter] = None,
        journal: Optional[StrategyJournal] = None,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
        audit_external_orders: bool = False,
    ):
        self._db_session_factory = db_session_factory or (lambda: nullcontext(db_session))
        self.clock = clock
        self.strategies: Dict[str, List[BaseStrategy]] = {}
        self.strategy_instances: Dict[str, BaseStrategy] = {}
        self.loaded_classes: Dict[str, Type[BaseStrategy]] = {}
        self._strategy_lock = threading.Lock()
        self._ops_command_lock = threading.Lock()
        self._boot_id = uuid.uuid4().hex
        self._boot_started = False
        self.runtime_environment = RuntimeEnvironment.from_env()
        self._system_state_key = self.runtime_environment.key("system:state")
        self._system_boot_state_key = self.runtime_environment.key(
            "system:engine_boot_state"
        )
        self._heartbeat_key = self.runtime_environment.key("heartbeat:python")
        self._registry = StrategyRegistry()
        self.redis_client = create_redis_client()
        self._strategy_state_manager = StrategyStateManager(
            self._db_session_factory,
            self.redis_client,
        )
        self._daily_nav_snapshot_service = DailyNavSnapshotService(
            self._db_session_factory,
        )

        # Initialize Services
        self.account_service = account_service if account_service else AccountService()
        self.risk_manager = RiskManager(
            self.account_service,
            state_manager=self._strategy_state_manager,
            daily_nav_service=self._daily_nav_snapshot_service,
        )

        # Use pre-created adapter or build from config
        effective_adapter_config = adapter_config or {"mode": "simulated"}
        self._live_product_ids = (
            frozenset(effective_adapter_config.get("instrument_product_ids") or [])
            if effective_adapter_config.get("mode") == "live"
            else None
        )
        if self._live_product_ids is not None and not self._live_product_ids:
            raise ValueError("live adapter requires instrument_product_ids")
        self._rithmic_recovery_profile = effective_adapter_config.get(
            "rithmic_recovery_profile"
        ) or effective_adapter_config.get("rithmic_profile")
        self._rithmic_recovery_account_id = effective_adapter_config.get(
            "rithmic_recovery_account_id"
        ) or effective_adapter_config.get("account_id")
        self._startup_auto_recovery_allowed = False
        self._startup_lock_cause: str | None = None
        if adapter is None:
            try:
                adapter = create_adapter(effective_adapter_config)
                logger.info("StrategyEngine: Using %s", type(adapter).__name__)
            except Exception as e:
                logger.critical("Failed to init adapter: %s. NOT falling back silently.", e)
                raise
        else:
            logger.info("StrategyEngine: Using provided adapter %s", type(adapter).__name__)
        if isinstance(adapter, RithmicExchangeAdapter):
            if not audit_external_orders:
                raise ValueError("Rithmic live trading requires audit_external_orders")
            self._rithmic_recovery_profile = (
                self._rithmic_recovery_profile or adapter.profile
            )
            self._rithmic_recovery_account_id = (
                self._rithmic_recovery_account_id or adapter.account_id
            )
        self.risk_manager.instrument_spec_resolver = getattr(
            adapter,
            "get_instrument_spec",
            None,
        )
        self._runtime_reconciliation_enabled = _is_runtime_reconciliation_enabled(
            adapter,
            adapter_config,
        )
        self._runtime_reconcile_interval = (
            _runtime_reconciliation_interval_from_env()
            if self._runtime_reconciliation_enabled
            else None
        )

        self.execution_engine = ExecutionEngine(
            db_session,
            clock,
            adapter,
            order_repository,
            journal=journal,
            db_session_factory=self._db_session_factory,
            audit_external_orders=audit_external_orders,
            account_service=self.account_service,
            rithmic_account_profile=self._rithmic_recovery_profile,
            rithmic_account_id=self._rithmic_recovery_account_id,
        )
        self._rithmic_recovery_profile = (
            self.execution_engine.order_manager.rithmic_account_profile
        )
        self._rithmic_recovery_account_id = (
            self.execution_engine.order_manager.rithmic_account_id
        )
        self._lifecycle_adapter = _EngineLifecycleAdapter(self)
        self._health_monitor = HealthMonitor(self._registry)
        self._command_router = CommandRouter(
            self._registry,
            self._lifecycle_adapter,
            self._health_monitor,
        )
        self._signal_processor = SignalProcessor(
            self._registry,
            self.execution_engine,
            self._strategy_state_manager,
            lambda signal, candle: self.process_signal(signal, candle),
            position_loader=getattr(
                self.account_service,
                "get_position_for_exit",
                self.account_service.get_position,
            ),
        )
        self.ops_safety = OpsSafetyService(
            self.execution_engine,
            self.account_service,
            self._db_session_factory,
        )
        self.runtime_reconciliation_job = RuntimeReconciliationJob(
            account_service=self.account_service,
            adapter=adapter,
            db_session_factory=self._db_session_factory,
            quantity_drift_threshold=Decimal(
                os.getenv("RECONCILE_QTY_DRIFT_THRESHOLD", "0.00000001")
            ),
            balance_drift_threshold=Decimal(
                os.getenv("RECONCILE_BALANCE_DRIFT_THRESHOLD", "0.01")
            ),
            product_ids=(
                effective_adapter_config.get("instrument_product_ids")
                or effective_adapter_config.get("product_ids")
                or []
            ),
        )
        
        # System State & Heartbeat
        self._health_monitor.redis_client = self.redis_client
        self.running = True
        self._kill_switch_halted = False
        self.heartbeat_thread = None
        self.command_thread = None
        self.runtime_reconcile_thread = None
        self._runtime_reconcile_stop = threading.Event()
        # Last observed order-session connection generation; a bump means the
        # order session reconnected and owned orders must be reconciled.
        self._last_order_generation: int | None = None
        self._pending_order_reconnect_generation: int | None = None
        self._rithmic_external_order_drift_pending = False
        self._rithmic_external_order_drift_generation = 0
        self._rithmic_external_order_drift_lock = threading.Lock()
        self.order_event_thread = None
        self._order_event_stop = threading.Event()
        self.executor = ThreadPoolExecutor(max_workers=5)

    def startup(self):
        """
        Runs startup checks and starts background services.
        """
        # Start fail-closed so the command listener can accept a manual clear
        # while startup waits on a persisted LOCKDOWN state.
        self._halt_for_kill_switch()
        self._start_command_listener()
        with self._ops_command_lock:
            persisted_lockdown = self._check_system_state()
        self._reconcile_balance()
        self._initialize_strategy_state_cache_on_startup()
        self._start_strategy_state_subscriber_on_startup()
        reconciliation = self._reconcile_recoverable_orders_on_startup()
        self._start_exchange_order_event_stream()
        if isinstance(self.execution_engine.adapter, RithmicExchangeAdapter) and not (
            reconciliation and reconciliation.get("auto_resume_safe") is True
        ):
            self._halt_for_kill_switch()
            self._startup_lock_cause = "rithmic_reconciliation_blocked"
            persisted_lockdown = True
        if persisted_lockdown:
            if self._can_auto_resume_after_startup_recovery(reconciliation):
                self._resume_after_kill_switch()
                persisted_lockdown = False
                logger.info("Startup reconciliation passed; submissions resumed automatically")
            elif self._startup_lock_cause == "explicit_lockdown":
                with self._ops_command_lock:
                    if self._kill_switch_halted:
                        self._run_ops_kill_switch(
                            actor="startup_recovery",
                            reason="persisted_lockdown",
                        )
        self._start_heartbeat()
        if self._runtime_reconciliation_enabled:
            self._start_runtime_reconciliation()
        
        # Initial scan to discover strategies
        self.scan_strategies()
        if not self._kill_switch_halted:
            self._restore_active_strategies_on_startup()

    def _initialize_strategy_state_cache_on_startup(self) -> None:
        """Load strategy lifecycle state into the manager cache."""
        self._strategy_state_manager.initialize_cache_from_db()

    def _start_strategy_state_subscriber_on_startup(self) -> None:
        """Listen for cross-process strategy state updates."""
        self._strategy_state_manager.start_subscriber()

    def _start_exchange_order_event_stream(self) -> None:
        adapter = self.execution_engine.adapter
        start = getattr(adapter, "start_order_event_stream", None)
        poll = getattr(adapter, "poll_order_event", None)
        if not callable(start) or not callable(poll):
            return

        try:
            start()
        except Exception:
            self._halt_for_kill_switch()
            raise
        if isinstance(adapter, RithmicExchangeAdapter):
            # A newly created runtime always publishes generation 1 before its
            # constructor returns. Starting from that known value means a fast
            # reconnect cannot disappear into a delayed first observation.
            self._last_order_generation = 1
            self._pending_order_reconnect_generation = None
        self._order_event_stop.clear()

        def order_event_loop() -> None:
            while self.running and not self._order_event_stop.is_set():
                try:
                    if isinstance(adapter, RithmicExchangeAdapter):
                        if not self._reconcile_owned_orders_on_reconnect():
                            self._order_event_stop.wait(1.0)
                            continue
                    event = poll()
                    if event is None:
                        self._order_event_stop.wait(0.05)
                        continue
                    result = self.execution_engine.process_exchange_order_event(event)
                    if isinstance(adapter, RithmicExchangeAdapter):
                        action = str(result.get("action") or "")
                        if action == "unknown_order":
                            self._lockdown_for_external_order(
                                account_id=str((event.raw or {}).get("account_id") or ""),
                                exchange=str((event.raw or {}).get("exchange") or ""),
                                symbol=str(
                                    (event.raw or {}).get("symbol") or event.product_id
                                ),
                                client_order_id=event.client_order_id,
                                exchange_order_id=event.exchange_order_id,
                            )
                        elif self._rithmic_order_event_requires_reconciliation(result):
                            self._lockdown_for_rithmic_order_event(
                                action=action or "missing_action",
                                event=event,
                            )
                except RithmicUnmappedOrderEvent as error:
                    self._lockdown_for_external_order(
                        account_id=error.account_id,
                        exchange=error.exchange,
                        symbol=error.symbol,
                    )
                    continue
                except Exception:
                    logger.exception(
                        "Exchange order event stream failed; submissions remain halted"
                    )
                    self._halt_for_kill_switch()
                    return

        self.order_event_thread = threading.Thread(
            target=order_event_loop,
            name="exchange-order-events",
            daemon=True,
        )
        self.order_event_thread.start()

    def _lockdown_for_external_order(
        self,
        *,
        account_id: str,
        exchange: str,
        symbol: str,
        client_order_id: str | None = None,
        exchange_order_id: str | None = None,
    ) -> None:
        reason = (
            "rithmic_external_order_detected: "
            f"account_id={account_id or 'unknown'} "
            f"exchange={exchange or 'unknown'} symbol={symbol or 'unknown'} "
            f"client_order_id={client_order_id or 'unknown'} "
            f"exchange_order_id={exchange_order_id or 'unknown'}"
        )
        self._lockdown_for_rithmic_order_drift(reason)

    @staticmethod
    def _rithmic_order_event_requires_reconciliation(result: dict) -> bool:
        return (
            result.get("action") not in _RITHMIC_SAFE_ORDER_EVENT_ACTIONS
            or bool(result.get("verification_blocked"))
            or bool(result.get("unresolved"))
        )

    def _lockdown_for_rithmic_order_event(
        self,
        *,
        action: str,
        event,
    ) -> None:
        self._lockdown_for_rithmic_order_drift(
            "rithmic_order_event_requires_reconciliation: "
            f"action={action} product_id={event.product_id} "
            f"client_order_id={event.client_order_id or 'unknown'} "
            f"exchange_order_id={event.exchange_order_id or 'unknown'}"
        )

    def _lockdown_for_rithmic_order_drift(self, reason: str) -> None:
        with self._rithmic_external_order_drift_lock:
            first_detection = not self._rithmic_external_order_drift_pending
            self._rithmic_external_order_drift_pending = True
            self._rithmic_external_order_drift_generation += 1
            # Publish the generation and raise the submission gate atomically
            # with respect to a concurrent clear decision.
            self._halt_for_kill_switch()
        logger.error("%s; submissions locked pending authoritative reconciliation", reason)
        if not first_detection:
            return
        self._persist_rithmic_external_order_lockdown(reason)

    def _persist_rithmic_external_order_lockdown(self, reason: str) -> None:
        try:
            self.ops_safety.persist_kill_switch_state(
                SYSTEM_STATE_LOCKDOWN,
                actor="rithmic_order_stream",
                reason=reason,
            )
        except Exception:
            logger.exception("Failed to persist external-order lockdown to database")
        try:
            self.redis_client.set(self._system_state_key, SYSTEM_STATE_LOCKDOWN)
        except Exception:
            logger.exception(
                "Failed to persist external-order lockdown to Redis; local halt remains active"
            )

    def _prepare_rithmic_kill_switch_clear(self) -> tuple[bool, int | None]:
        adapter = self.execution_engine.adapter
        if not isinstance(adapter, RithmicExchangeAdapter):
            return True, None

        self._order_event_stop.set()
        thread = self.order_event_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=30.0)
            if thread.is_alive():
                logger.error(
                    "Rithmic clear reconciliation timed out stopping the order event stream"
                )
                self._order_event_stop.clear()
                return False, None

        if not self.execution_engine.halt_for_reconcile(timeout=30.0):
            logger.error(
                "Rithmic clear reconciliation timed out draining in-flight submissions"
            )
            try:
                self._start_exchange_order_event_stream()
            except Exception:
                logger.exception(
                    "Order stream restart failed after reconciliation drain timeout"
                )
                return False, None
            self.execution_engine.resume_after_reconcile()
            return False, None

        adapter.close()
        summary = None
        try:
            summary = self.execution_engine.reconcile_rithmic_owned_orders(
                self._rithmic_recovery_profile,
                self._rithmic_recovery_account_id,
            )
        except Exception:
            logger.exception("Rithmic clear reconciliation failed")

        with self._rithmic_external_order_drift_lock:
            drift_generation = self._rithmic_external_order_drift_generation

        try:
            self._start_exchange_order_event_stream()
        except Exception:
            logger.exception(
                "Order stream restart failed after external-order reconciliation"
            )
            return False, None

        if not summary or summary.get("auto_resume_safe") is not True:
            self.execution_engine.resume_after_reconcile()
            logger.error(
                "Rithmic clear reconciliation is unresolved; lockdown remains active"
            )
            return False, None
        return True, drift_generation

    def _run_ops_kill_switch(
        self,
        *,
        actor: str,
        reason: str | None,
        operation_id: str | None = None,
    ) -> dict:
        adapter = self.execution_engine.adapter
        if not isinstance(adapter, RithmicExchangeAdapter):
            if operation_id is None:
                return self.ops_safety.kill_switch(
                    actor=actor,
                    reason=reason,
                )
            return self.ops_safety.kill_switch(
                actor=actor,
                reason=reason,
                operation_id=operation_id,
            )

        aggregate = None
        operation_failed = False

        def finalize(
            verified: bool,
            failure_reason: str | None = None,
        ) -> dict:
            nonlocal aggregate
            aggregate = aggregate or {
                "cancelled_orders": 0,
                "cancel_failures": [],
                "flattened_positions": 0,
                "flatten_pending": [],
                "flatten_failures": [],
                "recovery_failures": [],
                "already_flat": False,
                "drain_timeout": False,
            }
            aggregate["authoritative_flatten_verified"] = verified
            if failure_reason is not None:
                aggregate["flatten_failures"].append(
                    {
                        "strategy_id": "LIVE",
                        "product_id": "unknown",
                        "reason": failure_reason,
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
            return aggregate

        self._order_event_stop.set()
        thread = self.order_event_thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=30.0)
            if thread.is_alive():
                self._order_event_stop.clear()
                finalize(
                    False,
                    "rithmic_emergency_flatten_event_stream_stop_timeout",
                )
                raise RuntimeError(
                    "rithmic_emergency_flatten_event_stream_stop_timeout"
                )

        def load_snapshot():
            adapter.close()
            recoverable_orders = [
                order
                for order in self.execution_engine.list_recoverable_client_orders()
                if str(order.exchange_id).lower() == "rithmic"
            ]
            return load_rithmic_recovery_snapshot(
                self._rithmic_recovery_profile,
                self._rithmic_recovery_account_id,
                recoverable_orders,
                int(self.execution_engine.clock.now()),
            )

        def load_positions() -> list:
            snapshot = load_snapshot()
            if any(rithmic_order_may_be_working(order) for order in snapshot.orders):
                raise RuntimeError(
                    "rithmic_emergency_flatten_working_orders_remain"
                )
            positions = adapter.positions_from_ledger_snapshot(snapshot)
            adapter.start_order_event_stream()
            return positions

        try:
            exit_attempts = 0
            exit_failed = False
            submit_exit = True
            for _verification_attempt in range(6):
                if submit_exit:
                    authoritative_kwargs = {
                        "actor": actor,
                        "reason": reason,
                        "position_loader": load_positions,
                        "account_id": adapter.account_id,
                    }
                    if operation_id is not None:
                        authoritative_kwargs["operation_id"] = operation_id
                    result = self.ops_safety.kill_switch_with_authoritative_positions(
                        **authoritative_kwargs
                    )
                    aggregate = _merge_kill_switch_results(aggregate, result)
                    exit_attempts += 1
                    exit_failed = bool(
                        result.get("drain_timeout")
                        or result.get("flatten_pending")
                        or result.get("flatten_failures")
                    )
                    if result.get("drain_timeout"):
                        break

                snapshot = load_snapshot()
                reconciliation = self.execution_engine.reconcile_rithmic_owned_orders(
                    self._rithmic_recovery_profile,
                    self._rithmic_recovery_account_id,
                    snapshot_loader=lambda *_args, **_kwargs: snapshot,
                )
                remaining_positions = adapter.positions_from_ledger_snapshot(
                    snapshot
                )
                working_orders_remain = any(
                    rithmic_order_may_be_working(order)
                    for order in snapshot.orders
                )
                if not working_orders_remain:
                    self.account_service.replace_positions_for_products(
                        remaining_positions,
                        adapter.configured_product_ids,
                        timestamp_ms=int(self.execution_engine.clock.now() * 1000),
                    )
                    reconciliation = (
                        self.execution_engine.reconcile_rithmic_owned_orders(
                            self._rithmic_recovery_profile,
                            self._rithmic_recovery_account_id,
                            snapshot_loader=lambda *_args, **_kwargs: snapshot,
                        )
                    )
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

                adapter.start_order_event_stream()
                submit_exit = True

            return finalize(
                False,
                "rithmic_authoritative_flatten_not_verified",
            )
        except Exception as exc:
            operation_failed = True
            finalize(
                False,
                "rithmic_authoritative_flatten_failed:"
                f"{type(exc).__name__}",
            )
            raise
        finally:
            try:
                self._start_exchange_order_event_stream()
            except Exception as restart_error:
                aggregate = aggregate or {
                    "cancelled_orders": 0,
                    "cancel_failures": [],
                    "flattened_positions": 0,
                    "flatten_pending": [],
                    "flatten_failures": [],
                    "recovery_failures": [],
                    "already_flat": False,
                    "drain_timeout": False,
                    "authoritative_flatten_verified": False,
                }
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
                    logger.exception(
                        "Order stream restart also failed after emergency flatten failure"
                    )
                else:
                    raise

    def _reconcile_recoverable_orders_on_startup(self) -> dict | None:
        """Record startup order reconciliation for audited external orders."""
        if not self.execution_engine.audit_external_orders:
            return None

        try:
            if self._rithmic_recovery_profile:
                summary = self.execution_engine.reconcile_rithmic_owned_orders(
                    self._rithmic_recovery_profile,
                    self._rithmic_recovery_account_id,
                )
            else:
                summary = self.execution_engine.reconcile_recoverable_client_orders()
        except Exception:
            logger.exception("Startup order reconciliation failed")
            return {
                "recoverable_count": 0,
                "unresolved_count": 1,
                "verification_blocked_count": 1,
                "auto_resume_safe": False,
            }
        logger.info(
            "Startup order reconciliation complete: %s recoverable orders",
            summary["recoverable_count"],
        )
        return summary

    def _reconcile_owned_orders_on_reconnect(self) -> bool:
        """Reconcile owned orders whenever the order session reconnects.

        The order runtime exposes a monotonic connection generation; a bump
        since the last observation means a mid-session reconnect happened, so a
        fresh authoritative snapshot (orders + history + fills) is reconciled —
        the same path as startup — to catch fills/cancels that occurred while
        disconnected. Submissions are gated for the duration via the independent
        reconcile gate (never touching a kill-switch halt). Fail-safe: if the
        reconcile fails, submissions stay gated and the generation is not
        advanced, so it retries on the next tick rather than trading against an
        unreconciled book.
        """
        adapter = self.execution_engine.adapter
        if not isinstance(adapter, RithmicExchangeAdapter):
            return True
        if self._pending_order_reconnect_generation is None:
            if self._last_order_generation is None:
                self._last_order_generation = 1
            try:
                generation = adapter.connection_generation()
            except Exception:
                logger.exception(
                    "Order connection generation unavailable; reconciling fail closed"
                )
                generation = self._last_order_generation + 1
            if generation <= self._last_order_generation:
                return True
            self._pending_order_reconnect_generation = generation

        generation = self._pending_order_reconnect_generation

        logger.info(
            "Order session reconnected (generation %s -> %s); reconciling owned orders",
            self._last_order_generation,
            generation,
        )
        if not self.execution_engine.halt_for_reconcile(timeout=30.0):
            logger.error(
                "Reconnect order reconciliation waiting for in-flight submissions"
            )
            return False

        # The ORDER runtime owns the profile lease for its whole lifetime.
        # Closing it before the ledger snapshot is therefore part of the
        # reconciliation boundary, not optional cleanup.
        adapter.close()
        if (
            not self.execution_engine.audit_external_orders
            or not self._rithmic_recovery_profile
        ):
            logger.error(
                "Reconnect order reconciliation is unavailable; submissions remain gated"
            )
            return False
        try:
            summary = self.execution_engine.reconcile_rithmic_owned_orders(
                self._rithmic_recovery_profile,
                self._rithmic_recovery_account_id,
            )
        except Exception:
            logger.exception(
                "Reconnect order reconciliation failed; submissions remain gated"
            )
            # Stay gated and do not advance the generation: retry next tick.
            return False
        if summary.get("auto_resume_safe") is not True:
            logger.error(
                "Reconnect order reconciliation is unresolved; submissions remain gated"
            )
            return False
        try:
            adapter.start_order_event_stream()
        except Exception:
            logger.exception(
                "Reconnect order stream restart failed; submissions remain gated"
            )
            adapter.close()
            return False

        # This is a new runtime, whose first successful connection is generation
        # 1. Any subsequent bump is another reconnect and will be observed here.
        self._last_order_generation = 1
        self._pending_order_reconnect_generation = None
        self.execution_engine.resume_after_reconcile()
        logger.info(
            "Reconnect order reconciliation complete: %s recoverable orders",
            summary["recoverable_count"],
        )
        return True

    def _can_auto_resume_after_startup_recovery(self, summary: dict | None) -> bool:
        return bool(
            self._startup_auto_recovery_allowed
            and summary is not None
            and summary.get("auto_resume_safe") is True
        )

    def _start_command_listener(self):
        """
        Starts the Redis command listener in a background thread.
        """
        def command_loop():
            pubsub = self.redis_client.pubsub()
            pubsub.subscribe("cmd:strategy:control")
            logger.info("📡 Command Listener Started. Subscribed to 'cmd:strategy:control'")
            for message in pubsub.listen():
                if not self.running:
                    break
                if message['type'] == 'message':
                    try:
                        data = json.loads(message['data'])
                        self.executor.submit(self._handle_command, data)
                    except Exception as e:
                        logger.error("Error parsing command: %s", e)
        
        self.command_thread = threading.Thread(target=command_loop, daemon=True)
        self.command_thread.start()

    def _handle_command(self, data: dict):
        """
        Routes commands to specific handlers.
        """
        cmd = str(data.get("command") or data.get("cmd") or "").upper()
        params = data.get("params") or {}
        if not isinstance(params, dict):
            params = {}
        
        logger.info("Received Command: %s with params %s", cmd, params)
        
        try:
            if cmd == "SCAN":
                self.scan_strategies()
            elif cmd == "TEST_RUN":
                self.test_run_strategy(params.get("id"), params.get("days", 1))
            elif cmd == "KILL_SWITCH":
                with self._ops_command_lock:
                    actor = params.get("actor", "operator")
                    reason = params.get("reason")
                    idempotency_key = params.get("idempotency_key")
                    if (
                        isinstance(idempotency_key, str)
                        and self._kill_switch_operation_completed(
                            actor=str(actor),
                            idempotency_key=idempotency_key,
                        )
                    ):
                        logger.info(
                            "Skipping completed kill switch operation for actor %s",
                            actor,
                        )
                        return
                    self._halt_for_kill_switch()
                    db_state_persisted = False
                    try:
                        persist_kwargs = {
                            "actor": actor,
                            "reason": reason,
                        }
                        if isinstance(idempotency_key, str):
                            persist_kwargs["operation_id"] = idempotency_key
                        self.ops_safety.persist_kill_switch_state(
                            SYSTEM_STATE_LOCKDOWN,
                            **persist_kwargs,
                        )
                        db_state_persisted = True
                    except Exception:
                        logger.exception(
                            "Failed to persist kill switch state to database"
                        )
                    redis_state_persisted = False
                    try:
                        self.redis_client.set(
                            self._system_state_key,
                            SYSTEM_STATE_LOCKDOWN,
                        )
                        redis_state_persisted = True
                    except Exception:
                        logger.exception(
                            "Failed to persist kill switch state; local halt remains active"
                        )
                    kill_switch_kwargs = {
                        "actor": actor,
                        "reason": reason,
                    }
                    if isinstance(idempotency_key, str):
                        kill_switch_kwargs["operation_id"] = idempotency_key
                    kill_switch_result = self._run_ops_kill_switch(
                        **kill_switch_kwargs
                    )
                    self._kill_switch_halted = True
                    if (
                        isinstance(idempotency_key, str)
                        and db_state_persisted
                        and redis_state_persisted
                        and _kill_switch_result_is_complete(
                            kill_switch_result,
                            authoritative_required=isinstance(
                                self.execution_engine.adapter,
                                RithmicExchangeAdapter,
                            ),
                        )
                    ):
                        self._mark_kill_switch_operation_completed(
                            actor=str(actor),
                            idempotency_key=idempotency_key,
                        )
            elif cmd == "CLEAR_KILL_SWITCH":
                with self._ops_command_lock:
                    actor = params.get("actor", "operator")
                    reason = params.get("reason")

                    verified, drift_generation = self._prepare_rithmic_kill_switch_clear()
                    if not verified:
                        logger.warning(
                            "Kill switch clear rejected: rithmic_reconciliation_required"
                        )
                        return

                    def persist_clear() -> None:
                        self.ops_safety.persist_kill_switch_state(
                            SYSTEM_STATE_OK,
                            actor=actor,
                            reason=reason,
                        )
                        self.redis_client.set(self._system_state_key, SYSTEM_STATE_OK)

                    clear_succeeded = False
                    try:
                        result = self.ops_safety.clear_kill_switch(
                            persist_clear=persist_clear,
                        )
                        clear_succeeded = bool(result["cleared"])
                        if drift_generation is None and clear_succeeded:
                            self._kill_switch_halted = False
                        elif not clear_succeeded:
                            logger.warning(
                                "Kill switch clear rejected: %s",
                                result["reason"],
                            )
                    finally:
                        if drift_generation is not None:
                            with self._rithmic_external_order_drift_lock:
                                drift_advanced = (
                                    self._rithmic_external_order_drift_generation
                                    != drift_generation
                                )
                                if clear_succeeded and not drift_advanced:
                                    self._rithmic_external_order_drift_pending = False
                                    self._kill_switch_halted = False
                                elif drift_advanced:
                                    self._rithmic_external_order_drift_pending = True
                                    self._halt_for_kill_switch()
                            if drift_advanced:
                                self._persist_rithmic_external_order_lockdown(
                                    "rithmic_external_order_detected_during_clear"
                                )
                            self.execution_engine.resume_after_reconcile()
            else:
                result = self._command_router.handle(data)
                if result.success:
                    logger.info("Command %s succeeded: %s", cmd, result.message)
                else:
                    logger.warning("Command %s failed: %s", cmd, result.message)
        except Exception as e:
            logger.error("Error executing command %s: %s\n%s", cmd, e, traceback.format_exc())

    def _kill_switch_operation_redis_key(
        self,
        *,
        actor: str,
        idempotency_key: str,
    ) -> str:
        digest = hashlib.sha256(
            f"{actor}\0{idempotency_key}".encode()
        ).hexdigest()
        return self.runtime_environment.key(
            f"ops:kill-switch:idempotency:{digest}"
        )

    def _kill_switch_operation_completed(
        self,
        *,
        actor: str,
        idempotency_key: str,
    ) -> bool:
        key = self._kill_switch_operation_redis_key(
            actor=actor,
            idempotency_key=idempotency_key,
        )
        try:
            marker = self.redis_client.get(key)
            return marker in {"completed", b"completed"}
        except Exception:
            logger.exception(
                "Unable to read kill switch idempotency marker; retrying safely"
            )
            return False

    def _mark_kill_switch_operation_completed(
        self,
        *,
        actor: str,
        idempotency_key: str,
    ) -> None:
        key = self._kill_switch_operation_redis_key(
            actor=actor,
            idempotency_key=idempotency_key,
        )
        try:
            self.redis_client.set(
                key,
                "completed",
                ex=_KILL_SWITCH_IDEMPOTENCY_TTL_SECONDS,
            )
        except Exception:
            logger.exception(
                "Unable to persist kill switch idempotency marker; "
                "future retries will execute safely"
            )

    def scan_strategies(self):
        """
        Scans for strategy files and syncs with DB.
        """
        logger.info("🔍 Scanning for strategies in %s...", HOT_STRATEGIES_PATH)
        found = StrategyLoader.scan_directory(HOT_STRATEGIES_PATH)
        
        # Update class registry
        new_classes = {k: v for k, v in found.items() if not isinstance(v, str)}
        self.loaded_classes.update(new_classes)
        
        # Sync with DB
        with self._db_session_factory() as db:
            for strategy_id, result in found.items():
                state = db.query(StrategyState).filter(StrategyState.strategy_id == strategy_id).first()
                if not state:
                    state = StrategyState(
                        strategy_id=strategy_id,
                        status=StrategyStatus.DISCOVERED,
                        config_json="{}"
                    )
                    db.add(state)

                if isinstance(result, str):
                    # It was a LoadError (traceback string)
                    state.status = StrategyStatus.ERROR
                    state.performance_json = json.dumps({"error": result})
                    state.last_error_message = result
                    state.entered_error_at = datetime.now(UTC)
                
                db.commit()
        logger.info("✅ Scan Complete. Total loaded: %s", len(self.loaded_classes))

    def _restore_active_strategies_on_startup(self) -> None:
        """Re-instantiate strategies that were ACTIVE before process restart."""
        with self._db_session_factory() as db:
            active_states = (
                db.query(StrategyState)
                .filter(StrategyState.status == StrategyStatus.ACTIVE.value)
                .all()
            )

        for state in active_states:
            if state.strategy_id not in self.loaded_classes:
                logger.error(
                    "Startup restore: strategy class not loaded for %s — marking ERROR",
                    state.strategy_id,
                )
                self._strategy_state_manager.transition_to_error(
                    state.strategy_id,
                    "startup_restore_class_missing",
                    actor="system",
                )
                continue
            try:
                self.activate_strategy(
                    state.strategy_id,
                    actor="system",
                    reason="startup_restore",
                    force=True,
                )
            except Exception as e:
                logger.exception(
                    "Startup restore: failed to activate %s — marking ERROR",
                    state.strategy_id,
                )
                self._strategy_state_manager.transition_to_error(
                    state.strategy_id,
                    f"startup_restore_failed: {e}",
                    actor="system",
                )

    def test_run_strategy(self, strategy_id: str, days: int):
        """
        Performs a test run/warm-up for a strategy.
        """
        logger.info("🧪 Test Run for %s (days=%s)", strategy_id, days)
        if strategy_id not in self.loaded_classes:
            logger.error("Strategy %s not loaded.", strategy_id)
            return

        with self._db_session_factory() as db:
            state = db.query(StrategyState).filter(StrategyState.strategy_id == strategy_id).first()
            if not state:
                logger.error("Strategy %s not in DB.", strategy_id)
                return

            try:
                # Instantiate with dummy product to get requirements
                strategy_cls = self.loaded_classes[strategy_id]
                config = json.loads(state.config_json or "{}")
                product_id = self._strategy_product_id(config)
                
                temp_instance = strategy_cls(strategy_id, product_id)
                reqs = temp_instance.requirements
                
                # Check data availability
                is_available, backfill_cmd = check_data_availability(
                    db, reqs.product_id, reqs.timeframe, reqs.lookback_window
                )
                
                if not is_available:
                    logger.warning("⚠️ Insufficient data for %s. Command: %s", strategy_id, backfill_cmd)
                    state.status = StrategyStatus.WARNING
                    state.performance_json = json.dumps({"backfill_command": backfill_cmd})
                    db.commit()
                    return

                # If OK, update status to READY
                state.status = StrategyStatus.READY
                db.commit()
                logger.info("✅ Strategy %s is READY.", strategy_id)

            except Exception as e:
                error_trace = traceback.format_exc()
                state.status = StrategyStatus.ERROR
                state.performance_json = json.dumps({"error": error_trace})
                state.last_error_message = error_trace
                state.entered_error_at = datetime.now(UTC)
                db.commit()
                logger.error("❌ Test Run failed for %s: %s", strategy_id, e)

    def activate_strategy(
        self,
        strategy_id: str,
        *,
        actor: str = "operator",
        reason: Optional[str] = None,
        force: bool = False,
    ) -> None:
        """Instantiate/register a strategy and transition it to ACTIVE."""
        logger.info("🚀 Starting Strategy: %s", strategy_id)
        if strategy_id not in self.loaded_classes:
            logger.error("Strategy %s not loaded.", strategy_id)
            return

        with self._db_session_factory() as db:
            state = db.query(StrategyState).filter(StrategyState.strategy_id == strategy_id).first()
            # Allow READY or WARNING (with manual override implied by START command)
            startable = {StrategyStatus.READY, StrategyStatus.WARNING, StrategyStatus.STOPPED, StrategyStatus.DISCOVERED}
            if not state or (state.status not in startable and not force):
                 logger.error("Strategy %s is not in startable state (Current: %s)", strategy_id, state.status if state else 'None')
                 return

            try:
                config = json.loads(state.config_json or "{}")
                product_id = self._strategy_product_id(config)
                
                strategy_cls = self.loaded_classes[strategy_id]
                instance = strategy_cls(strategy_id, product_id)
                self._warm_up_strategy_instance(db, instance)
                # Registration must follow warm-up — on restart-restore the lifecycle
                # cache is already ACTIVE, so a registered instance is immediately
                # live to on_market_data and could emit signals from partial state.
                self._register_strategy_instance(instance)
                state.uptime_start = int(time.time() * 1000)
                db.commit()
                logger.info("🔥 Strategy %s is now ACTIVE for %s", strategy_id, product_id)

            except Exception as e:
                self._unregister_strategy_instance(strategy_id)
                state.performance_json = json.dumps({"error": str(e)})
                db.commit()
                self._strategy_state_manager.transition_to_error(
                    strategy_id,
                    str(e),
                    actor="system",
                )
                logger.error("❌ Failed to start %s: %s", strategy_id, e)
                return

        try:
            self._strategy_state_manager.transition_to_running(
                strategy_id,
                actor=actor,
                force=force,
                reason=reason,
            )
        except Exception as e:
            self._unregister_strategy_instance(strategy_id)
            logger.error("❌ Failed to transition %s to ACTIVE: %s", strategy_id, e)

    def _strategy_product_id(self, config: dict) -> str:
        product_id = str(config.get("product_id") or "").strip()
        if self._live_product_ids is None:
            return product_id or "BINANCE:BTCUSDT-PERP"
        if not product_id:
            raise ValueError("strategy product_id must be set explicitly in live")
        if product_id not in self._live_product_ids:
            raise ValueError(
                f"strategy product_id is not enabled for live adapter: {product_id}"
            )
        return product_id

    def _warm_up_strategy_instance(self, db: Session, instance: BaseStrategy) -> int:
        """Replay recent candles into a strategy without emitting signals."""
        reqs = instance.requirements
        lookback = max(int(reqs.lookback_window), 0)
        if lookback == 0:
            # No candle warm-up needed, but restored live positions must still
            # be synced or the strategy activates with flat internal state.
            self._sync_strategy_position_state(instance)
            return 0

        rows = (
            db.query(ORMCandlestick)
            .filter(
                ORMCandlestick.product_id == reqs.product_id,
                ORMCandlestick.timeframe == reqs.timeframe,
            )
            .order_by(ORMCandlestick.timestamp.desc())
            .limit(lookback)
            .all()
        )
        rows = sorted(rows, key=lambda row: row.timestamp)
        if len(rows) < lookback:
            raise RuntimeError(
                "warmup_insufficient_candles: "
                f"strategy_id={instance.strategy_id} "
                f"available={len(rows)} required={lookback}"
            )
        candles = [
            Candlestick(
                product_id=row.product_id,
                timeframe=row.timeframe,
                timestamp=row.timestamp,
                open=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume,
            )
            for row in rows
        ]
        self._signal_processor.warm_up(instance, candles)
        self._sync_strategy_position_state(instance)
        return len(candles)

    def _sync_strategy_position_state(self, instance: BaseStrategy) -> None:
        """Align warmed strategy trade flags with the current account position."""
        try:
            position = self.account_service.get_position(
                instance.strategy_id,
                instance.product_id,
            )
        except Exception as e:
            raise RuntimeError(
                "position_state_sync_failed: "
                f"strategy_id={instance.strategy_id} error={e}"
            ) from e
        position_side = None if position is None else getattr(position.side, "value", position.side)
        applied = self._signal_processor.set_position_state(instance, position_side)
        if position_side is not None and not applied:
            raise RuntimeError(
                "position_state_sync_unsupported: "
                f"strategy_id={instance.strategy_id} side={position_side}"
            )

    def start_strategy(self, strategy_id: str):
        """Backward-compatible wrapper for legacy callers."""
        self.activate_strategy(strategy_id)

    def deactivate_strategy(
        self,
        strategy_id: str,
        *,
        actor: str = "operator",
        reason: Optional[str] = None,
    ) -> None:
        """Unregister a strategy and transition it to STOPPED."""
        logger.info("🛑 Stopping Strategy: %s", strategy_id)
        if not self._unregister_strategy_instance(strategy_id):
            logger.warning("Strategy %s is not active.", strategy_id)
            return

        self._strategy_state_manager.transition_to_stopped(
            strategy_id,
            actor=actor,
            reason=reason,
        )

        logger.info("✅ Strategy %s stopped.", strategy_id)

    def stop_strategy(self, strategy_id: str):
        """Backward-compatible wrapper for legacy callers."""
        self.deactivate_strategy(strategy_id)

    def _register_strategy_instance(self, instance: BaseStrategy) -> None:
        """Register a live strategy instance in runtime-only structures."""
        with self._strategy_lock:
            old = self.strategy_instances.get(instance.strategy_id)
            if old is not None and old.product_id in self.strategies:
                self.strategies[old.product_id] = [
                    s for s in self.strategies[old.product_id]
                    if s.strategy_id != instance.strategy_id
                ]
            self.strategy_instances[instance.strategy_id] = instance
            if instance.product_id not in self.strategies:
                self.strategies[instance.product_id] = []
            self.strategies[instance.product_id].append(instance)
            self._registry.register(instance)
            ACTIVE_STRATEGIES.set(len(self.strategy_instances))

    def _unregister_strategy_instance(self, strategy_id: str) -> bool:
        """Remove a live strategy instance from runtime-only structures."""
        with self._strategy_lock:
            instance = self.strategy_instances.pop(strategy_id, None)
            if instance is None:
                return False
            product_id = instance.product_id
            if product_id in self.strategies:
                self.strategies[product_id] = [
                    s for s in self.strategies[product_id]
                    if s.strategy_id != strategy_id
                ]
            self._registry.unregister(strategy_id)
            ACTIVE_STRATEGIES.set(len(self.strategy_instances))
            return True

    def _reconcile_balance(self):
        """
        Startup Reconciliation
        Force overwrite Redis balance from actual Exchange API.
        """
        logger.info("💰 Reconciling Balance...")
        try:
            balance = self.account_service.get_balance()
            self.redis_client.set("state:balance:USDT", str(balance))
            logger.info("✅ Balance Reconciled: %s USDT", balance)
        except Exception as e:
            logger.warning("⚠️ Balance Reconciliation Failed: %s. Using DB/Redis state.", e)

    def _check_system_state(self) -> bool:
        """
        Checks 'system:state'. If 'LOCKDOWN', enters a paused loop.
        """
        logger.info("🔍 Checking System State...")
        self._boot_started = True
        read_failed = False
        try:
            db_state = self.ops_safety.latest_kill_switch_state()
            redis_state = self.redis_client.get(self._system_state_key)
            if isinstance(redis_state, bytes):
                redis_state = redis_state.decode("utf-8")
            db_boot = self.ops_safety.latest_engine_boot_state()
            redis_boot = self._decode_boot_state(
                self.redis_client.get(self._system_boot_state_key)
            )
        except Exception as exc:
            logger.error("System state unavailable; starting in LOCKDOWN: %s", exc)
            read_failed = True
            db_state = redis_state = None
            db_boot = redis_boot = None

        boot_marker_persisted = self._persist_engine_boot_state("UNCLEAN")

        states_disagree = db_state != redis_state
        state = db_state if db_state is not None else redis_state
        kill_state_clear = (
            db_state == SYSTEM_STATE_OK and redis_state == SYSTEM_STATE_OK
        )
        previous_boot_clean = (
            db_boot is not None
            and db_boot == redis_boot
            and db_boot.get("state") == "CLEAN"
        )
        self._startup_auto_recovery_allowed = bool(
            not read_failed
            and boot_marker_persisted
            and not states_disagree
            and kill_state_clear
            and not previous_boot_clean
        )
        if SYSTEM_STATE_LOCKDOWN in {db_state, redis_state}:
            self._startup_lock_cause = "explicit_lockdown"
        elif self._startup_auto_recovery_allowed:
            self._startup_lock_cause = "unclean_boot"
        else:
            self._startup_lock_cause = "state_verification_failed"
        if (
            read_failed
            or not boot_marker_persisted
            or states_disagree
            or not kill_state_clear
            or not previous_boot_clean
        ):
            self._halt_for_kill_switch()
            logger.warning(
                "SYSTEM LOCKED (db=%s redis=%s db_boot=%s redis_boot=%s); "
                "startup recovery required",
                db_state,
                redis_state,
                db_boot,
                redis_boot,
            )
            return True

        self._resume_after_kill_switch()
        self._startup_lock_cause = None
        logger.info("System State: %s. Proceeding.", state or SYSTEM_STATE_OK)
        return False

    @staticmethod
    def _decode_boot_state(value: object) -> dict[str, str] | None:
        if isinstance(value, bytes):
            value = value.decode("utf-8")
        if not isinstance(value, str):
            return None
        try:
            payload = json.loads(value)
        except (TypeError, ValueError):
            return None
        if not isinstance(payload, dict):
            return None
        state = payload.get("state")
        boot_id = payload.get("boot_id")
        if state not in {"CLEAN", "UNCLEAN"} or not isinstance(boot_id, str):
            return None
        return {"state": state, "boot_id": boot_id}

    def _persist_engine_boot_state(self, state: str) -> bool:
        db_persisted = redis_persisted = False
        try:
            self.ops_safety.persist_engine_boot_state(state, boot_id=self._boot_id)
            db_persisted = True
        except Exception:
            logger.exception("Failed to persist engine boot state to database")
        try:
            self.redis_client.set(
                self._system_boot_state_key,
                json.dumps(
                    {"state": state, "boot_id": self._boot_id},
                    separators=(",", ":"),
                ),
            )
            redis_persisted = True
        except Exception:
            logger.exception("Failed to persist engine boot state to Redis")
        return db_persisted or redis_persisted

    def _halt_for_kill_switch(self) -> None:
        self._kill_switch_halted = True
        self.execution_engine.halt_and_drain(timeout=0)

    def _resume_after_kill_switch(self) -> None:
        self.execution_engine.resume_submissions()
        self._kill_switch_halted = False

    def _start_heartbeat(self):
        """
        Starts the heartbeat background thread.
        """
        def heartbeat_loop():
            logger.info("💓 Heartbeat Service Started.")
            while self.running:
                try:
                    self.redis_client.setex(
                        self._heartbeat_key,
                        3,
                        str(int(time.time() * 1000)),
                    )
                    # Expose balance to Prometheus
                    try:
                        balance = self.account_service.get_balance()
                        BALANCE_USDT.set(float(balance))
                    except Exception:
                        pass
                    # Update DB heartbeats for active strategies (snapshot for thread safety)
                    with self._strategy_lock:
                        active_sids = list(self.strategy_instances.keys())
                    self._record_strategy_heartbeats(active_sids)
                    time.sleep(1.0)
                except Exception as e:
                    logger.error("💓 Heartbeat Failed: %s", e)
                    time.sleep(1.0)
        
        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def _start_runtime_reconciliation(self):
        """Start periodic runtime reconciliation in a daemon thread."""
        interval = (
            self._runtime_reconcile_interval
            if self._runtime_reconcile_interval is not None
            else _runtime_reconciliation_interval_from_env()
        )
        self._runtime_reconcile_stop.clear()

        def reconcile_loop():
            logger.info("Runtime reconciliation service started.")
            while self.running and not self._runtime_reconcile_stop.is_set():
                try:
                    self.runtime_reconciliation_job.run_once()
                except Exception as e:
                    logger.error("Runtime reconciliation loop failed: %s", e)
                if self._runtime_reconcile_stop.wait(interval):
                    break

        self.runtime_reconcile_thread = threading.Thread(
            target=reconcile_loop,
            daemon=True,
        )
        self.runtime_reconcile_thread.start()

    def _record_strategy_heartbeats(self, strategy_ids: list[str]) -> None:
        """Record strategy heartbeat state in HealthMonitor and DB."""
        with self._db_session_factory() as db:
            now_ms = int(time.time() * 1000)
            for sid in strategy_ids:
                try:
                    self._health_monitor.update_heartbeat(sid)
                except Exception as e:
                    logger.warning("Failed to update health monitor for %s: %s", sid, e)
                db.query(StrategyState).filter(StrategyState.strategy_id == sid).update({
                    "last_heartbeat": now_ms
                })
            db.commit()

    def add_strategy(self, strategy: BaseStrategy):
        """
        Legacy support for static registration.
        """
        with self._strategy_lock:
            if strategy.product_id not in self.strategies:
                self.strategies[strategy.product_id] = []
            self.strategies[strategy.product_id].append(strategy)
            self.strategy_instances[strategy.strategy_id] = strategy
            self._registry.register(strategy)
            self._strategy_state_manager.on_state_change_message(
                {"strategy_id": strategy.strategy_id, "status": StrategyStatus.ACTIVE.value}
            )
            ACTIVE_STRATEGIES.set(len(self.strategy_instances))
        logger.info("Registered strategy (legacy): %s for %s", strategy.strategy_id, strategy.product_id)

    def build_stream_channels(self) -> list:
        """Derive Redis stream keys from registered strategy requirements."""
        channels = set()
        for strat in self._registry.list_active():
            product_id = strat.product_id
            parts = product_id.split(":")
            exchange = parts[0].lower()
            symbol = parts[1].replace("-PERP", "").lower()
            tf = strat.requirements.timeframe
            channels.add(f"stream:market:{exchange}:{symbol}:{tf}")
        return sorted(channels)

    def on_market_data(self, data: Union[Candlestick, Trade]):
        """
        Callback triggered by DataConsumer when new market data arrives.
        """
        if isinstance(data, Candlestick):
            # Simulation/Backtest: check fills before strategy signals can create new orders.
            self.execution_engine.process_market_data(data)
            self._signal_processor.on_candle(data)
            return
        if isinstance(data, Trade):
            self._signal_processor.on_trade(data)

    def process_signal(self, signal: Signal, candle: Optional[Candlestick]):
        """
        Handle the signal generated by a strategy.
        """
        if signal.type == SignalType.NO_SIGNAL:
            return
        if self._kill_switch_halted:
            logger.warning(
                "Signal rejected because kill switch is active: strategy=%s type=%s",
                signal.strategy_id,
                signal.type,
            )
            return

        import structlog.contextvars
        structlog.contextvars.bind_contextvars(trace_id=uuid.uuid4().hex[:16])

        current_price = candle.close if candle else None
        is_passed, risk_msg = self.risk_manager.check_risk(
            signal,
            current_price=current_price,
        )

        risk_status = "PASS" if is_passed else "REJECT"
        SIGNALS_TOTAL.labels(
            strategy_id=signal.strategy_id,
            signal_type=signal.type.value,
            risk_status=risk_status,
        ).inc()

        order_id = None
        if is_passed:
            logger.info("✅ SIGNAL ACCEPTED: %s. Forwarding to Execution Engine...", signal.type)
            if (
                isinstance(self.execution_engine.adapter, RithmicExchangeAdapter)
                and signal.type in (SignalType.EXIT_LONG, SignalType.EXIT_SHORT)
            ):
                order_id = self.execution_engine.execute_authoritative_exit_signal(
                    signal,
                    candle,
                    self._run_rithmic_strategy_exit,
                )
            else:
                order_id = self.execution_engine.execute_signal(signal, candle)
            if self.execution_engine.audit_external_orders:
                return
        
        audit = build_signal_audit(
            clock=self.clock,
            signal=signal,
            candle=candle,
            risk_passed=is_passed,
            risk_message=risk_msg,
            order_id=order_id,
        )
        with self._db_session_factory() as db:
            commit_signal_audit(db, audit)

    def _run_rithmic_strategy_exit(
        self,
        signal: Signal,
        decision: ExitDecision,
    ) -> dict[str, object]:
        """Cancel owned orders, exit one Rithmic position, and verify remote flat."""
        adapter = self.execution_engine.adapter
        if not isinstance(adapter, RithmicExchangeAdapter):
            raise RuntimeError("authoritative_strategy_exit_requires_rithmic")
        if decision.position_quantity is None or decision.quantity != decision.position_quantity:
            raise RuntimeError("rithmic_partial_strategy_exit_unsupported")

        verified = False
        operation_failed = False
        outcome: dict[str, object] | None = None
        cancelled_orders = 0
        self._order_event_stop.set()
        thread = self.order_event_thread
        try:
            if thread is not None and thread.is_alive():
                thread.join(timeout=30.0)
                if thread.is_alive():
                    raise RuntimeError(
                        "rithmic_strategy_exit_event_stream_stop_timeout"
                    )

            active_statuses = {
                OrderStatus.NEW.value,
                OrderStatus.SUBMITTED_UNCONFIRMED.value,
                OrderStatus.SUBMITTED.value,
                OrderStatus.PARTIALLY_FILLED.value,
            }
            active_orders = [
                order
                for order in self.execution_engine.order_manager.repo.list_orders_by_statuses(
                    active_statuses
                )
                if order.strategy_id == signal.strategy_id
                and order.product_id == signal.product_id
            ]
            adapter.start_order_event_stream()
            for order in active_orders:
                if order.status == OrderStatus.NEW.value:
                    self.execution_engine.order_manager.fail_order(
                        order,
                        "strategy_exit",
                    )
                    cancelled_orders += 1
                    continue
                if not order.client_order_id:
                    raise RuntimeError(
                        "rithmic_strategy_exit_cancel_identity_missing:"
                        f"order_id={order.id}"
                    )
                remote = adapter.get_order_by_client_id(
                    order.client_order_id,
                    order.product_id,
                    order_type=order.type,
                )
                if remote is None:
                    raise RuntimeError(
                        "rithmic_strategy_exit_cancel_lookup_missing:"
                        f"order_id={order.id}"
                    )
                if remote.status in {"filled", "cancelled", "rejected"}:
                    continue
                if not adapter.cancel_order(
                    remote.exchange_order_id,
                    order.product_id,
                    order_type=order.type,
                ):
                    raise RuntimeError(
                        "rithmic_strategy_exit_cancel_failed:"
                        f"order_id={order.id}"
                    )
                cancelled_orders += 1

            def load_snapshot():
                adapter.close()
                recoverable_orders = [
                    order
                    for order in self.execution_engine.list_recoverable_client_orders()
                    if str(order.exchange_id).lower() == "rithmic"
                ]
                return load_rithmic_recovery_snapshot(
                    self._rithmic_recovery_profile,
                    self._rithmic_recovery_account_id,
                    recoverable_orders,
                    int(self.execution_engine.clock.now()),
                )

            snapshot = load_snapshot()
            if any(rithmic_order_may_be_working(order) for order in snapshot.orders):
                raise RuntimeError("rithmic_strategy_exit_working_orders_remain")
            positions = adapter.positions_from_ledger_snapshot(snapshot)
            reconciliation = self.execution_engine.reconcile_rithmic_owned_orders(
                self._rithmic_recovery_profile,
                self._rithmic_recovery_account_id,
                snapshot_loader=lambda *_args, **_kwargs: snapshot,
            )
            if reconciliation.get("auto_resume_safe") is not True:
                raise RuntimeError(
                    "rithmic_strategy_exit_preflight_reconciliation_blocked"
                )
            self.account_service.replace_positions_for_products(
                positions,
                adapter.configured_product_ids,
                timestamp_ms=int(self.execution_engine.clock.now() * 1000),
            )
            remote_position = next(
                (
                    position
                    for position in positions
                    if position.product_id == signal.product_id
                ),
                None,
            )
            if remote_position is not None:
                remote_side = str(
                    getattr(remote_position.side, "value", remote_position.side)
                ).upper()
                expected_side = (
                    "LONG"
                    if signal.type == SignalType.EXIT_LONG
                    else "SHORT"
                )
                if (
                    remote_side != expected_side
                    or remote_position.quantity > decision.position_quantity
                ):
                    raise RuntimeError(
                        "rithmic_strategy_exit_position_drift:"
                        f"expected_side={expected_side} remote_side={remote_side} "
                        f"expected_quantity={decision.position_quantity} "
                        f"remote_quantity={remote_position.quantity}"
                    )
                adapter.start_order_event_stream()
                self.execution_engine.exit_authoritative_position(
                    signal.product_id,
                    account_id=adapter.account_id,
                )

            for _attempt in range(6):
                snapshot = load_snapshot()
                remaining_positions = adapter.positions_from_ledger_snapshot(snapshot)
                working_orders_remain = any(
                    rithmic_order_may_be_working(order)
                    for order in snapshot.orders
                )
                if working_orders_remain:
                    continue
                reconciliation = self.execution_engine.reconcile_rithmic_owned_orders(
                    self._rithmic_recovery_profile,
                    self._rithmic_recovery_account_id,
                    snapshot_loader=lambda *_args, **_kwargs: snapshot,
                )
                self.account_service.replace_positions_for_products(
                    remaining_positions,
                    adapter.configured_product_ids,
                    timestamp_ms=int(self.execution_engine.clock.now() * 1000),
                )
                target_position = next(
                    (
                        position
                        for position in remaining_positions
                        if position.product_id == signal.product_id
                    ),
                    None,
                )
                if (
                    target_position is None
                    and reconciliation.get("auto_resume_safe") is True
                ):
                    verified = True
                    outcome = {
                        "status": "verified_flat",
                        "cancelled_orders": cancelled_orders,
                        "product_id": signal.product_id,
                    }
                    break
            if not verified:
                raise RuntimeError("rithmic_strategy_exit_flat_not_verified")
        except Exception as error:
            operation_failed = True
            self._lockdown_for_rithmic_order_drift(
                "rithmic_strategy_exit_requires_reconciliation:"
                f"{type(error).__name__}"
            )
            raise
        finally:
            try:
                self._start_exchange_order_event_stream()
            except Exception:
                logger.exception(
                    "Rithmic strategy exit failed to restart order event stream"
                )
                self._lockdown_for_rithmic_order_drift(
                    "rithmic_strategy_exit_order_stream_restart_failed"
                )
                if not operation_failed:
                    raise RuntimeError(
                        "rithmic_strategy_exit_order_stream_restart_failed"
                    )
        if outcome is None:
            raise RuntimeError("rithmic_strategy_exit_outcome_missing")
        return outcome

    def shutdown(self, timeout: float = 30.0):
        """Graceful shutdown: stop threads, drain executor, close Redis."""
        logger.info("StrategyEngine shutting down...")
        self.running = False
        self._runtime_reconcile_stop.set()
        self._order_event_stop.set()

        self.executor.shutdown(wait=True, cancel_futures=False)

        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=timeout)
        if self.command_thread and self.command_thread.is_alive():
            self.command_thread.join(timeout=timeout)
        if self.runtime_reconcile_thread and self.runtime_reconcile_thread.is_alive():
            self.runtime_reconcile_thread.join(timeout=timeout)
        if self.order_event_thread and self.order_event_thread.is_alive():
            self.order_event_thread.join(timeout=timeout)

        close_adapter = getattr(self.execution_engine.adapter, "close", None)
        if callable(close_adapter):
            close_adapter()

        self._strategy_state_manager.shutdown()

        if (
            self._boot_started
            and not self._kill_switch_halted
            and not self.ops_safety.recovery_pending
        ):
            self._persist_engine_boot_state("CLEAN")

        try:
            self.redis_client.close()
        except Exception as e:
            logger.warning("Error closing Redis: %s", e)

        logger.info("StrategyEngine shutdown complete.")
