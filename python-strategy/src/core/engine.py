import math
import os
import time
import threading
import logging
import uuid
import weakref
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from typing import (
    Callable,
    ContextManager,
    Dict,
    Optional,
    Type,
    Union,
    cast,
)
from sqlalchemy.orm import Session
from src.core.models import (
    Candlestick,
    Trade,
    Signal,
    SignalType,
    StrategyStatus,
)
from src.core.orm_models import StrategyState
from src.strategies.base import BaseStrategy
from src.core.risk_manager import RiskManager, AccountService
from src.core.execution import ExecutionEngine
from src.core.clock import Clock
from src.core.interfaces import IExchangeAdapter, IOrderRepository
from src.core.interfaces.exchange import EntryAdmissionGate, ExchangeOrderEvent
from src.core.daily_nav_snapshot import DailyNavSnapshotService
from src.core.portfolio_runtime import (
    PortfolioCoordinator,
    PortfolioDefinition,
    PortfolioFactory,
    build_portfolio_artifact,
)
from src.core.data_provider import check_data_availability
from src.core.adapters.simulated import (
    SimulatedAdapter,
    create_simulated_adapter,
)
from src.core.journal import StrategyJournal
from src.core.redis_factory import create_redis_client
from src.core.metrics import ACTIVE_STRATEGIES, BALANCE_USDT
from src.core.command_router import CommandRouter
from src.core.health_monitor import HealthMonitor
from src.core.engine_heartbeat_service import EngineHeartbeatService
from src.core.engine_boot_state_service import EngineBootStateService
from src.core.engine_shutdown import EngineShutdownService
from src.core.generic_order_event_stream import GenericOrderEventStream
from src.core.engine_runtime_reconciliation_service import (
    EngineRuntimeReconciliationService,
)
from src.core.live_candle_application import LiveCandleApplicationService
from src.core.pending_market_replay import PendingMarketReplayService
from src.core.ops_safety import OpsSafetyService
from src.core.ops_command_service import OpsCommandService
from src.core.runtime_reconcile import RuntimeReconciliationJob
from src.core.runtime_artifact_registry import RuntimeArtifactRegistry
from src.core.signal_processor import SignalProcessor
from src.core.signal_execution_service import SignalExecutionService
from src.core.strategy_registry import StrategyRegistry
from src.core.strategy_artifact_discovery import synchronize_strategy_artifacts
from src.core.strategy_command_listener import build_strategy_command_listener
from src.core.strategy_command_idempotency import (
    claim_strategy_command_operation,
    kill_switch_operation_completed,
    mark_kill_switch_operation_completed,
    mark_strategy_command_operation_completed,
)
from src.core.strategy_command_dispatch_service import (
    StrategyCommandDispatchService,
)
from src.core.strategy_startup_restore import restore_active_strategies
from src.core.strategy_state_manager import (
    InvalidStrategyStateTransition,
    StaleStrategyStateVersion,
    StrategyStateManager,
    available_strategy_commands,
)
from src.core.strategy_hydration_service import StrategyHydrationService
from src.core.strategy_activation_service import StrategyActivationService
from src.core.strategy_deactivation_service import StrategyDeactivationService
from src.core.strategy_test_run_service import StrategyTestRunService
from src.core.runtime_environment import RuntimeEnvironment
from src.core.runtime_capabilities import (
    DefaultRuntimeBootstrap,
    NoopRuntimeCapabilities,
    RuntimeBootstrapFactory,
    RuntimeCallbacks,
    RuntimeCapabilitiesFactory,
)
from src.core.product_registry import to_stream_key

_DEFAULT_RUNTIME_ENVIRONMENT = RuntimeEnvironment("live")
SYSTEM_STATE_KEY = _DEFAULT_RUNTIME_ENVIRONMENT.key("system:state")
SYSTEM_BOOT_STATE_KEY = _DEFAULT_RUNTIME_ENVIRONMENT.key("system:engine_boot_state")
SYSTEM_STATE_LOCKDOWN = "LOCKDOWN"
SYSTEM_STATE_OK = "OK"
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
    if any(type(result.get(key)) is not int or result[key] < 0 for key in count_keys):
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


def _is_runtime_reconciliation_enabled(
    adapter: IExchangeAdapter,
) -> bool:
    return adapter.supports_runtime_reconciliation() is True


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
        if self._engine.activate_strategy(strategy_id, **kwargs) is False:
            raise RuntimeError(f"strategy activation rejected: {strategy_id}")

    def transition_to_stopped(self, strategy_id: str, **kwargs) -> None:
        if self._engine.deactivate_strategy(strategy_id, **kwargs) is False:
            raise RuntimeError(f"strategy deactivation rejected: {strategy_id}")

    def is_running(self, strategy_id: str) -> bool:
        return strategy_id in self._engine.strategy_instances


class StrategyEngine:
    def __init__(
        self,
        db_session: Session | None,
        clock: Clock,
        order_repository: Optional[IOrderRepository] = None,
        account_service: Optional[AccountService] = None,
        adapter_config: Optional[Dict] = None,
        adapter: Optional[IExchangeAdapter] = None,
        journal: Optional[StrategyJournal] = None,
        db_session_factory: Optional[Callable[[], ContextManager[Session]]] = None,
        audit_external_orders: bool = False,
        is_backtest: bool | None = None,
        leadership_guard: Callable[[], None] | None = None,
        signal_batch_observer: Callable[[tuple[Signal, ...]], None] | None = None,
        runtime_bootstrap_factory: RuntimeBootstrapFactory | None = None,
        runtime_capabilities_factory: RuntimeCapabilitiesFactory | None = None,
        strategy_artifact_loader: Callable[
            [],
            dict[str, Type[BaseStrategy] | Type[PortfolioFactory] | str],
        ]
        | None = None,
    ):
        if db_session_factory is None:
            if db_session is None:
                raise ValueError(
                    "StrategyEngine requires db_session or db_session_factory"
                )
            self._db_session_factory = lambda: nullcontext(db_session)
        else:
            self._db_session_factory = db_session_factory
        self.clock = clock
        self.loaded_classes: Dict[
            str,
            Type[BaseStrategy] | Type[PortfolioFactory],
        ] = {}
        self._strategy_artifact_loader = strategy_artifact_loader or (lambda: {})
        self._strategy_lock = threading.Lock()
        self._strategy_lifecycle_locks: weakref.WeakValueDictionary[
            str, threading.Lock
        ] = weakref.WeakValueDictionary()
        self._strategy_lifecycle_locks_lock = threading.Lock()
        self._ops_command_lock = threading.Lock()
        self._market_processing_lock = threading.Lock()
        self._boot_id = uuid.uuid4().hex
        self._boot_started = False
        self._leadership_guard = leadership_guard or (lambda: None)
        self.runtime_environment = RuntimeEnvironment.from_env()
        self._live_candle_application = LiveCandleApplicationService(
            environment_identity=lambda: self.runtime_environment.identity,
            db_session_factory=lambda: self._db_session_factory(),
        )
        self._system_state_key = self.runtime_environment.key("system:state")
        self._system_boot_state_key = self.runtime_environment.key(
            "system:engine_boot_state"
        )
        self._heartbeat_key = self.runtime_environment.key("heartbeat:python")
        self._registry = StrategyRegistry()
        self._portfolio_coordinator = PortfolioCoordinator()
        self.redis_client = create_redis_client()
        self._strategy_state_manager = StrategyStateManager(
            self._db_session_factory,
            self.redis_client,
        )
        self._strategy_test_run = StrategyTestRunService(
            db_session_factory=lambda: self._db_session_factory(),
            state_manager=self._strategy_state_manager,
            event_logger=logger,
        )
        self._runtime_artifacts = RuntimeArtifactRegistry(
            strategy_registry=self._registry,
            portfolio_coordinator=self._portfolio_coordinator,
            state_lock=self._strategy_lock,
            market_processing_lock=self._market_processing_lock,
            publish_active_state=self._strategy_state_manager.on_state_change_message,
            record_active_count=ACTIVE_STRATEGIES.set,
            event_logger=logger,
        )
        self.strategies = self._runtime_artifacts.strategies
        self.strategy_instances = self._runtime_artifacts.strategy_instances
        self.portfolio_instances = self._runtime_artifacts.portfolio_instances
        self._runtime_registration_lock = self._runtime_artifacts.registration_lock
        self._strategy_deactivation = StrategyDeactivationService(
            state_manager=self._strategy_state_manager,
            portfolio_coordinator=self._portfolio_coordinator,
            runtime_artifacts=self._runtime_artifacts,
            registration_lock=self._runtime_registration_lock,
            market_processing_lock=self._market_processing_lock,
            event_logger=logger,
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
            lifecycle_id_resolver=(
                self._portfolio_coordinator.lifecycle_id_for_strategy
            ),
        )

        # Live adapters are composed by the service entrypoint. Keep the local
        # simulated default for library and backtest callers.
        effective_adapter_config = adapter_config or {"mode": "simulated"}
        self._live_product_ids = (
            frozenset(effective_adapter_config.get("instrument_product_ids") or [])
            if effective_adapter_config.get("mode") == "live"
            else None
        )
        if self._live_product_ids is not None and not self._live_product_ids:
            raise ValueError("live adapter requires instrument_product_ids")
        self._startup_auto_recovery_allowed = False
        self._startup_lock_cause: str | None = None
        if adapter is None:
            if effective_adapter_config.get("mode", "simulated") != "simulated":
                raise ValueError(
                    "live adapter must be composed by the service entrypoint"
                )
            adapter = create_simulated_adapter(effective_adapter_config)
            logger.info("StrategyEngine: Using %s", type(adapter).__name__)
        else:
            logger.info(
                "StrategyEngine: Using provided adapter %s", type(adapter).__name__
            )
        if is_backtest is True and not isinstance(adapter, SimulatedAdapter):
            raise ValueError("backtest mode requires SimulatedAdapter")
        if (runtime_bootstrap_factory is None) != (
            runtime_capabilities_factory is None
        ):
            raise ValueError("runtime capability factories must be provided together")
        if (
            getattr(adapter, "requires_runtime_capabilities", False) is True
            and runtime_bootstrap_factory is None
        ):
            raise ValueError("adapter requires runtime capability composition")
        runtime_bootstrap = (
            runtime_bootstrap_factory(
                adapter=adapter,
                adapter_config=effective_adapter_config,
                audit_external_orders=audit_external_orders,
                account_service=self.account_service,
                runtime_environment=self.runtime_environment,
            )
            if runtime_bootstrap_factory is not None
            else DefaultRuntimeBootstrap()
        )
        self._runtime_profile = runtime_bootstrap.profile
        self._runtime_account_id = runtime_bootstrap.account_id
        self.risk_manager.instrument_spec_resolver = getattr(
            adapter,
            "get_instrument_spec",
            None,
        )
        (
            self._runtime_reconciliation_enabled,
            self._runtime_reconcile_interval,
        ) = runtime_bootstrap.resolve_reconciliation_schedule(
            generic_enabled=_is_runtime_reconciliation_enabled(
                adapter,
            ),
            generic_interval_resolver=_runtime_reconciliation_interval_from_env,
        )

        self.execution_engine = ExecutionEngine(
            db_session,
            clock,
            adapter,
            order_repository,
            journal=journal,
            is_backtest=is_backtest,
            db_session_factory=self._db_session_factory,
            audit_external_orders=audit_external_orders,
            account_service=self.account_service,
            order_account_identity_resolver=(
                runtime_bootstrap.resolve_order_account_identity
            ),
            operation_guard=self._assert_runtime_leadership,
            order_event_processor=runtime_bootstrap.process_order_event,
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
            lambda signal, candle: self.process_signal(
                signal,
                candle,
                _entry_admitted=True,
            ),
            position_loader=getattr(
                self.account_service,
                "get_position_for_exit",
                self.account_service.get_position,
            ),
            exposure_loader=self.execution_engine.portfolio_exposure_snapshot,
            portfolio_coordinator=self._portfolio_coordinator,
            signal_batch_observer=signal_batch_observer,
        )
        self._strategy_hydration = StrategyHydrationService(
            signal_processor=self._signal_processor,
            account_service=self.account_service,
        )
        self._strategy_activation = StrategyActivationService(
            db_session_factory=lambda: self._db_session_factory(),
            transition_to_running=lambda *args, **kwargs: (
                self._strategy_state_manager.transition_to_running(*args, **kwargs)
            ),
            transition_to_error=lambda *args, **kwargs: (
                self._strategy_state_manager.transition_to_error(*args, **kwargs)
            ),
            hydration=self._strategy_hydration,
            register_strategy=lambda instance: self._register_strategy_instance(
                instance
            ),
            register_portfolio=lambda definition: (
                self._register_portfolio_definition(definition)
            ),
            unregister_runtime_artifact=lambda strategy_id: (
                self._unregister_runtime_artifact(strategy_id)
            ),
            environment_identity=lambda: self.runtime_environment.identity,
            event_logger=logger,
        )
        self._pending_market_replay = PendingMarketReplayService(
            db_session_factory=lambda: self._db_session_factory(),
            live_candle_application=self._live_candle_application,
            strategy_hydration=self._strategy_hydration,
            list_active_strategies=self._registry.list_active,
            publish_replacement=lambda replacement: self._register_strategy_instance(
                replacement
            ),
        )
        self.ops_safety = OpsSafetyService(
            self.execution_engine,
            self.account_service,
            self._db_session_factory,
        )
        self._boot_state_service = EngineBootStateService(
            ops_safety=self.ops_safety,
            redis_client=self.redis_client,
            system_state_key=self._system_state_key,
            system_boot_state_key=self._system_boot_state_key,
            boot_id=self._boot_id,
            logger=logger,
        )
        self.order_event_thread = None
        self._order_event_stop = threading.Event()
        self._generic_order_event_stream = GenericOrderEventStream(
            adapter_loader=lambda: self.execution_engine.adapter,
            is_running=lambda: self.running,
            stop_event=lambda: self._order_event_stop,
            assert_leadership=lambda: self._assert_runtime_leadership(),
            process_event=lambda event: (
                self.execution_engine.process_exchange_order_event(
                    cast(ExchangeOrderEvent, event)
                )
            ),
            halt_submissions=lambda: self._halt_for_kill_switch(),
            publish_worker=lambda worker: setattr(
                self,
                "order_event_thread",
                worker,
            ),
            current_worker=lambda: self.order_event_thread,
            event_logger=logger,
            thread_factory=lambda **values: threading.Thread(**values),
        )
        runtime_callbacks = RuntimeCallbacks(
            is_running=lambda: self.running,
            publish_worker=lambda worker: setattr(
                self,
                "order_event_thread",
                worker,
            ),
            on_runtime_started=lambda: (self._venue_runtime.on_order_runtime_started()),
            reconcile_if_needed=lambda: (self._reconcile_owned_orders_on_reconnect()),
            process_event=lambda event: (
                self.execution_engine.process_exchange_order_event(event)
            ),
            lockdown=lambda reason: (self._detect_external_order_drift(reason)),
            assert_runtime_leadership=lambda: (self._assert_runtime_leadership()),
            halt_submissions=lambda: self._halt_for_kill_switch(),
            clear_local_halt=lambda: self._clear_local_kill_switch_halt(),
            persist_lockdown_state=lambda actor, reason: (
                self.ops_safety.persist_kill_switch_state(
                    SYSTEM_STATE_LOCKDOWN,
                    actor=actor,
                    reason=reason,
                )
            ),
            persist_redis_lockdown=lambda: self.redis_client.set(
                self._system_state_key,
                SYSTEM_STATE_LOCKDOWN,
            ),
            stop_order_event_stream=lambda **values: (
                self._stop_exchange_order_event_stream(**values)
            ),
            start_order_event_stream=lambda: (
                self._start_exchange_order_event_stream()
            ),
            current_order_event_thread=lambda: self.order_event_thread,
            publish_authoritative_summary=lambda summary: (
                self._publish_authoritative_account_summary(summary)
            ),
        )
        self._venue_runtime = (
            runtime_capabilities_factory(
                adapter=adapter,
                profile=self._runtime_profile,
                account_id=self._runtime_account_id,
                execution_engine=self.execution_engine,
                account_service=self.account_service,
                ops_safety=self.ops_safety,
                stop_event=self._order_event_stop,
                callbacks=runtime_callbacks,
                logger=logger,
            )
            if runtime_capabilities_factory is not None
            else NoopRuntimeCapabilities()
        )
        self._signal_execution = SignalExecutionService(
            clock=self.clock,
            default_entry_quantity=lambda: self.execution_engine.default_quantity,
            check_risk=lambda signal, *, current_price: self.risk_manager.check_risk(
                signal,
                current_price=current_price,
            ),
            route_authoritative_exit=lambda signal, candle, portfolio_id, execute: (
                self._venue_runtime.route_authoritative_exit(
                    signal,
                    candle,
                    portfolio_id,
                    execute,
                )
            ),
            execute_signal=lambda signal, candle: (
                self.execution_engine.execute_signal(signal, candle)
            ),
            execute_authoritative_exit_signal=(
                lambda: self.execution_engine.execute_authoritative_exit_signal
            ),
            portfolio_id_for_sleeve=(
                lambda: self._portfolio_coordinator.portfolio_id_for_sleeve
            ),
            audit_external_orders=lambda: self.execution_engine.audit_external_orders,
            db_session_factory=lambda: self._db_session_factory(),
            event_logger=logger,
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
        self.executor = ThreadPoolExecutor(max_workers=5)
        self._entry_admission_gate: EntryAdmissionGate | None = None
        if self.runtime_environment.identity == "live":
            self._entry_admission_gate = adapter.create_entry_admission_gate(
                self.runtime_environment,
                logger=logger,
            )
        if self._entry_admission_gate is not None:
            self._signal_processor.entry_admission_handler = (
                self._entry_signal_allowed_for_processor
            )
        self._heartbeat_service = EngineHeartbeatService(
            is_running=lambda: self.running,
            assert_leadership=lambda: self._assert_runtime_leadership(),
            write_process_heartbeat=lambda: self.redis_client.setex(
                self._heartbeat_key,
                3,
                str(int(time.time() * 1000)),
            ),
            observe_entry_admission=lambda: (
                self._entry_admission_gate is None
                or self._entry_admission_gate.observe()
            ),
            record_balance_metric=lambda: BALANCE_USDT.set(
                float(self.account_service.get_balance())
            ),
            load_active_strategy_ids=lambda: (self._active_heartbeat_lifecycle_ids()),
            record_strategy_heartbeats=lambda strategy_ids: (
                self._record_strategy_heartbeats(strategy_ids)
            ),
            event_logger=logger,
        )
        self._runtime_reconciliation_service = EngineRuntimeReconciliationService(
            is_running=lambda: self.running,
            select_reconciliation=lambda: (
                self._venue_runtime.select_runtime_reconciliation(
                    self.runtime_reconciliation_job.run_once,
                    self._run_runtime_recovery_exclusive,
                )
            ),
            assert_leadership=lambda: self._assert_runtime_leadership(),
            event_logger=logger,
        )
        self._shutdown_service = EngineShutdownService(
            mark_stopping=self._mark_stopping_for_shutdown,
            shutdown_executor=lambda: self.executor.shutdown(
                wait=True,
                cancel_futures=False,
            ),
            thread_loaders=(
                lambda: self.heartbeat_thread,
                lambda: self.command_thread,
                lambda: self.runtime_reconcile_thread,
                lambda: self.order_event_thread,
            ),
            adapter_loader=lambda: self.execution_engine.adapter,
            shutdown_strategy_state=lambda: self._strategy_state_manager.shutdown(),
            clean_persistence_allowed=lambda: (
                self._boot_started
                and not self._kill_switch_halted
                and not self.ops_safety.recovery_pending
            ),
            persist_clean=lambda: self._boot_state_service.persist("CLEAN"),
            close_redis=lambda: self.redis_client.close(),
            close_entry_gate=lambda: (
                self._entry_admission_gate.close()
                if self._entry_admission_gate is not None
                else None
            ),
            event_logger=logger,
        )
        self._ops_command_service = OpsCommandService(
            operation_lock=lambda: self._ops_command_lock,
            kill_switch_operation_completed=lambda **kwargs: (
                self._kill_switch_operation_completed(**kwargs)
            ),
            halt_for_kill_switch=lambda: self._halt_for_kill_switch(),
            persist_lockdown_database=lambda **kwargs: (
                self.ops_safety.persist_kill_switch_state(
                    SYSTEM_STATE_LOCKDOWN,
                    **kwargs,
                )
            ),
            persist_lockdown_redis=lambda: self.redis_client.set(
                self._system_state_key,
                SYSTEM_STATE_LOCKDOWN,
            ),
            run_kill_switch=lambda **kwargs: self._run_ops_kill_switch(**kwargs),
            mark_kill_switch_halted=lambda: setattr(
                self,
                "_kill_switch_halted",
                True,
            ),
            requires_authoritative_verification=(
                lambda: self._venue_runtime.requires_authoritative_flatten_verification()
            ),
            kill_switch_result_is_complete=(
                lambda *args, **kwargs: _kill_switch_result_is_complete(
                    *args,
                    **kwargs,
                )
            ),
            mark_kill_switch_operation_completed=lambda **kwargs: (
                self._mark_kill_switch_operation_completed(**kwargs)
            ),
            prepare_kill_switch_clear=(
                lambda: self._venue_runtime.prepare_kill_switch_clear()
            ),
            assert_leadership=lambda: self._assert_runtime_leadership(),
            clear_kill_switch=lambda **kwargs: self.ops_safety.clear_kill_switch(
                **kwargs
            ),
            persist_clear_database=lambda **kwargs: (
                self.ops_safety.persist_kill_switch_state(
                    SYSTEM_STATE_OK,
                    **kwargs,
                )
            ),
            persist_clear_redis=lambda: self.redis_client.set(
                self._system_state_key,
                SYSTEM_STATE_OK,
            ),
            clear_local_halt=lambda: setattr(
                self,
                "_kill_switch_halted",
                False,
            ),
            finalize_external_drift_clear=lambda **kwargs: (
                self._venue_runtime.finalize_external_order_drift_clear(**kwargs)
            ),
            event_logger=lambda: logger,
        )
        self._strategy_command_dispatch = StrategyCommandDispatchService(
            assert_leadership=lambda: self._assert_runtime_leadership(),
            scan_strategies=lambda: self.scan_strategies(),
            test_run_strategy=lambda strategy_id, days: self.test_run_strategy(
                strategy_id,
                days,
            ),
            ops_command_service=lambda: self._ops_command_service,
            assert_strategy_command_allowed=lambda **kwargs: (
                self._assert_strategy_command_allowed(**kwargs)
            ),
            claim_strategy_command_operation=lambda **kwargs: (
                self._claim_strategy_command_operation(**kwargs)
            ),
            mark_strategy_command_operation_completed=lambda **kwargs: (
                self._mark_strategy_command_operation_completed(**kwargs)
            ),
            command_router=lambda: self._command_router,
            event_logger=lambda: logger,
        )

    def startup(
        self,
        *,
        leadership_guard: Callable[[], None] | None = None,
    ):
        """
        Runs startup checks and starts background services.
        """
        guard = leadership_guard or self._leadership_guard

        def run_phase(phase):
            try:
                guard()
            except Exception:
                self._halt_for_kill_switch()
                raise
            try:
                result = phase()
            except Exception:
                self._halt_for_kill_switch()
                raise
            try:
                guard()
            except Exception:
                self._halt_for_kill_switch()
                raise
            return result

        # Start fail-closed so the command listener can accept a manual clear
        # while startup waits on a persisted LOCKDOWN state.
        run_phase(self._halt_for_kill_switch)
        run_phase(self._start_command_listener)

        def check_system_state() -> bool:
            with self._ops_command_lock:
                return self._check_system_state()

        persisted_lockdown = run_phase(check_system_state)
        run_phase(self._reconcile_startup_balance)
        run_phase(self._initialize_strategy_state_cache_on_startup)
        run_phase(self._start_strategy_state_subscriber_on_startup)
        reconciliation = run_phase(self._reconcile_recoverable_orders_on_startup)
        run_phase(self._start_exchange_order_event_stream)

        def apply_reconciliation_result() -> bool:
            lockdown = persisted_lockdown
            reconciliation_state = self._venue_runtime.classify_startup_reconciliation(
                reconciliation
            )
            if (
                reconciliation_state.entry_admission_safe
                and self._entry_admission_gate is not None
            ):
                self._entry_admission_gate.arm()
            if (
                reconciliation_state.owner_handled
                and not reconciliation_state.entry_admission_safe
            ):
                self._halt_for_kill_switch()
                self._startup_lock_cause = reconciliation_state.blocking_reason
                lockdown = True
            if lockdown:
                if self._can_auto_resume_after_startup_recovery(reconciliation):
                    self._resume_after_kill_switch()
                    lockdown = False
                    logger.info(
                        "Startup reconciliation passed; submissions resumed "
                        "automatically"
                    )
                elif self._startup_lock_cause == "explicit_lockdown":
                    with self._ops_command_lock:
                        if self._kill_switch_halted:
                            self._run_ops_kill_switch(
                                actor="startup_recovery",
                                reason="persisted_lockdown",
                            )
            return lockdown

        run_phase(apply_reconciliation_result)
        run_phase(self._start_heartbeat)
        if self._runtime_reconciliation_enabled:
            run_phase(self._start_runtime_reconciliation)

        # Initial scan to discover strategies
        run_phase(self.scan_strategies)
        if not self._kill_switch_halted:
            run_phase(self._restore_active_strategies_on_startup)

    def _initialize_strategy_state_cache_on_startup(self) -> None:
        """Load strategy lifecycle state into the manager cache."""
        self._strategy_state_manager.initialize_cache_from_db()

    def _assert_runtime_leadership(self) -> None:
        """Fence every background side-effect path after lease handoff."""
        try:
            self._leadership_guard()
        except Exception:
            self._halt_for_kill_switch()
            self.running = False
            self._order_event_stop.set()
            self._runtime_reconcile_stop.set()
            raise

    def _start_strategy_state_subscriber_on_startup(self) -> None:
        """Listen for cross-process strategy state updates."""
        self._strategy_state_manager.start_subscriber()

    def _start_exchange_order_event_stream(self) -> None:
        if self._venue_runtime.start_order_event_stream():
            return
        self._generic_order_event_stream.start()

    def _stop_exchange_order_event_stream(self, *, timeout: float) -> bool:
        """Stop the generic order-event worker within a bounded timeout."""
        return self._generic_order_event_stream.stop(timeout=timeout)

    def _detect_external_order_drift(self, reason: str) -> None:
        self._venue_runtime.detect_external_order_drift(reason)

    def _run_ops_kill_switch(
        self,
        *,
        actor: str,
        reason: str | None,
        operation_id: str | None = None,
    ) -> dict:
        def run_generic_kill_switch() -> dict:
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

        return self._venue_runtime.run_emergency_flatten(
            run_generic_kill_switch,
            actor=actor,
            reason=reason,
            operation_id=operation_id,
        )

    def _reconcile_recoverable_orders_on_startup(self) -> dict | None:
        """Record startup order reconciliation for audited external orders."""
        if not self.execution_engine.audit_external_orders:
            return None

        def reconcile_generic() -> dict:
            try:
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

        return self._venue_runtime.reconcile_startup(reconcile_generic)

    def _publish_authoritative_account_summary(self, summary: dict) -> None:
        """Delegate authoritative account publication to its runtime owner."""
        self._venue_runtime.publish_authoritative_summary(summary)

    def _reconcile_owned_orders_on_reconnect(self) -> bool:
        """Delegate ORDER reconnect recovery to its venue owner."""
        reconciled = self._venue_runtime.reconcile_order_reconnect()
        if reconciled is None:
            logger.error(
                "Reconnect order reconciliation is unavailable; "
                "submissions remain gated"
            )
            return False
        return reconciled

    def _can_auto_resume_after_startup_recovery(self, summary: dict | None) -> bool:
        return bool(
            self._startup_auto_recovery_allowed
            and summary
            and summary.get("auto_resume_safe") is True
        )

    def _start_command_listener(self):
        """Start the Redis strategy-control listener."""
        self.command_thread = build_strategy_command_listener(
            pubsub_factory=lambda: self.redis_client.pubsub(),
            is_running=lambda: self.running,
            assert_leadership=lambda: self._assert_runtime_leadership(),
            submit_command=lambda data: self.executor.submit(
                self._handle_command,
                data,
            ),
            event_logger=logger,
        )
        self.command_thread.start()

    def _handle_command(self, data: object):
        """Route one command through the venue-neutral dispatch owner."""
        self._strategy_command_dispatch.dispatch(data)

    def _claim_strategy_command_operation(
        self,
        *,
        actor: str,
        idempotency_key: str,
    ) -> bool:
        return claim_strategy_command_operation(
            redis_client=self.redis_client,
            key_builder=self.runtime_environment.key,
            actor=actor,
            idempotency_key=idempotency_key,
            event_logger=logger,
        )

    def _mark_strategy_command_operation_completed(
        self,
        *,
        actor: str,
        idempotency_key: str,
    ) -> None:
        mark_strategy_command_operation_completed(
            redis_client=self.redis_client,
            key_builder=self.runtime_environment.key,
            actor=actor,
            idempotency_key=idempotency_key,
            event_logger=logger,
        )

    def _kill_switch_operation_completed(
        self,
        *,
        actor: str,
        idempotency_key: str,
    ) -> bool:
        return kill_switch_operation_completed(
            redis_client=self.redis_client,
            key_builder=self.runtime_environment.key,
            actor=actor,
            idempotency_key=idempotency_key,
            event_logger=logger,
        )

    def _mark_kill_switch_operation_completed(
        self,
        *,
        actor: str,
        idempotency_key: str,
    ) -> None:
        mark_kill_switch_operation_completed(
            redis_client=self.redis_client,
            key_builder=self.runtime_environment.key,
            actor=actor,
            idempotency_key=idempotency_key,
            event_logger=logger,
        )

    def scan_strategies(self):
        """Scan configured artifacts and synchronize their durable state."""
        synchronize_strategy_artifacts(
            artifact_loader=self._strategy_artifact_loader,
            publish_loaded_classes=self._replace_loaded_strategy_classes,
            db_session_factory=self._db_session_factory,
            transition_to_error=self._strategy_state_manager.transition_to_error,
            event_logger=logger,
        )

    def _replace_loaded_strategy_classes(
        self,
        new_classes: dict[str, type[BaseStrategy] | type[PortfolioFactory]],
    ) -> None:
        with self._strategy_lock:
            self.loaded_classes = new_classes

    def _restore_active_strategies_on_startup(self) -> None:
        """Re-instantiate strategies that were ACTIVE before process restart."""
        restore_active_strategies(
            db_session_factory=self._db_session_factory,
            is_strategy_loaded=lambda strategy_id: strategy_id in self.loaded_classes,
            activate_strategy=self.activate_strategy,
            transition_to_error=self._strategy_state_manager.transition_to_error,
            event_logger=logger,
        )

    def test_run_strategy(self, strategy_id: str, days: int):
        """Evaluate historical data readiness without registering runtime state."""
        self._strategy_test_run.run(
            strategy_id,
            days=days,
            artifact_cls=self._get_loaded_strategy_class(strategy_id),
            resolve_product_id=self._strategy_product_id,
            build_artifact_instances=self._build_artifact_instances,
            check_data_availability=check_data_availability,
        )

    def activate_strategy(
        self,
        strategy_id: str,
        *,
        actor: str = "operator",
        reason: Optional[str] = None,
        force: bool = False,
        expected_version: int | None = None,
    ) -> bool:
        """Instantiate/register a strategy and transition it to ACTIVE."""
        with self._strategy_lifecycle_lock(strategy_id):
            return self._strategy_activation.activate_locked(
                strategy_id,
                artifact_cls=self._get_loaded_strategy_class(strategy_id),
                actor=actor,
                reason=reason,
                force=force,
                expected_version=expected_version,
                resolve_product_id=self._strategy_product_id,
                assert_live_readiness=self._assert_strategy_live_readiness,
                build_portfolio_definition=self._build_portfolio_definition,
            )

    def _strategy_lifecycle_lock(self, strategy_id: str) -> threading.Lock:
        with self._strategy_lifecycle_locks_lock:
            return self._strategy_lifecycle_locks.setdefault(
                strategy_id,
                threading.Lock(),
            )

    def _strategy_product_id(self, config: dict) -> str:
        raw_product_id = config.get("product_id")
        if raw_product_id is None:
            raise ValueError("strategy product_id must be set explicitly")
        if not isinstance(raw_product_id, str):
            raise TypeError("strategy product_id must be a string")
        product_id = raw_product_id.strip()
        if not product_id:
            raise ValueError("strategy product_id must be set explicitly")
        if self._live_product_ids is None:
            return product_id
        if product_id not in self._live_product_ids:
            raise ValueError(
                f"strategy product_id is not enabled for live adapter: {product_id}"
            )
        return product_id

    def _get_loaded_strategy_class(
        self,
        strategy_id: str,
    ) -> type[BaseStrategy] | type[PortfolioFactory] | None:
        with self._strategy_lock:
            return self.loaded_classes.get(strategy_id)

    def _assert_strategy_command_allowed(
        self,
        *,
        strategy_id: str,
        command: str,
        expected_version: int | None,
    ) -> None:
        with self._db_session_factory() as db:
            state = (
                db.query(StrategyState)
                .filter(StrategyState.strategy_id == strategy_id)
                .first()
            )
            if state is None:
                raise KeyError(f"strategy state not found: {strategy_id}")
            current_version = int(state.version or 0)
            status = StrategyStatus(state.status)
        if expected_version is not None and current_version != expected_version:
            raise StaleStrategyStateVersion(
                f"{strategy_id} expected version {expected_version}, "
                f"found {current_version}"
            )
        if command not in available_strategy_commands(status):
            raise InvalidStrategyStateTransition(
                f"{command} is not allowed while {strategy_id} is {status.value}"
            )

    def _assert_strategy_live_readiness(
        self,
        strategy_cls: type[BaseStrategy] | type[PortfolioFactory],
    ) -> None:
        if self.runtime_environment.identity != "live":
            return
        readiness = getattr(strategy_cls, "__fluxtrade_readiness__", None)
        if readiness != "LIVE_APPROVED":
            raise RuntimeError(
                f"strategy_live_approval_required: readiness={readiness}"
            )

    @staticmethod
    def _build_portfolio_definition(
        factory_cls: type[PortfolioFactory],
        *,
        portfolio_id: str,
        product_id: str,
        config: dict,
    ) -> PortfolioDefinition:
        return build_portfolio_artifact(
            factory_cls,
            portfolio_id=portfolio_id,
            product_id=product_id,
            config=config,
        )

    @classmethod
    def _build_artifact_instances(
        cls,
        artifact_cls: type[BaseStrategy] | type[PortfolioFactory],
        *,
        strategy_id: str,
        product_id: str,
        config: dict,
    ) -> tuple[BaseStrategy, ...]:
        if issubclass(artifact_cls, PortfolioFactory):
            return tuple(
                sleeve.strategy
                for sleeve in cls._build_portfolio_definition(
                    artifact_cls,
                    portfolio_id=strategy_id,
                    product_id=product_id,
                    config=config,
                ).sleeves
            )
        return (artifact_cls(strategy_id, product_id),)

    def _apply_unpersisted_candle(self, candle: Candlestick) -> None:
        self.execution_engine.process_market_data(candle)
        self._signal_processor.on_candle(candle)

    def replay_pending_market_data(
        self,
        data: Union[Candlestick, Trade],
    ) -> None:
        """Recover one claimed candle under the durable application fence."""
        if not isinstance(data, Candlestick):
            raise RuntimeError(
                "pending trade replay has no durable strategy-state boundary"
            )
        with self._market_processing_lock:
            self._pending_market_replay.replay(
                data,
                apply_new=self._apply_unpersisted_candle,
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
        expected_version: int | None = None,
    ) -> bool:
        """Unregister a strategy and transition it to STOPPED."""
        with self._strategy_lifecycle_lock(strategy_id):
            return self._strategy_deactivation.deactivate_locked(
                strategy_id,
                actor=actor,
                reason=reason,
                expected_version=expected_version,
            )

    def stop_strategy(self, strategy_id: str):
        """Backward-compatible wrapper for legacy callers."""
        self.deactivate_strategy(strategy_id)

    def _register_strategy_instance(self, instance: BaseStrategy) -> None:
        """Register a live strategy instance in runtime-only structures."""
        self._runtime_artifacts.register_strategy(instance)

    def _register_portfolio_definition(
        self,
        definition: PortfolioDefinition,
        *,
        publish_active_state: bool = False,
    ) -> None:
        """Atomically expose a complete portfolio at the market event boundary."""
        self._runtime_artifacts.register_portfolio(
            definition,
            publish_active_state=publish_active_state,
        )

    def _unregister_runtime_artifact(self, runtime_id: str) -> bool:
        """Remove a parent portfolio or one standalone strategy."""
        return self._runtime_artifacts.unregister(runtime_id)

    def _unregister_strategy_instance(self, strategy_id: str) -> bool:
        """Remove a live strategy instance from runtime-only structures."""
        return self._runtime_artifacts.unregister_strategy(strategy_id)

    def _reconcile_startup_balance(self) -> object | None:
        """Dispatch startup balance policy through the venue composition."""
        return self._venue_runtime.run_startup_balance_reconciliation(
            self._reconcile_balance
        )

    def _reconcile_balance(self) -> None:
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
            logger.warning(
                "⚠️ Balance Reconciliation Failed: %s. Using DB/Redis state.", e
            )

    def _check_system_state(self) -> bool:
        """
        Checks 'system:state'. If 'LOCKDOWN', enters a paused loop.
        """
        logger.info("🔍 Checking System State...")
        self._boot_started = True
        assessment = self._boot_state_service.assess_startup()
        self._startup_auto_recovery_allowed = assessment.auto_recovery_allowed
        self._startup_lock_cause = assessment.lock_cause
        if assessment.locked:
            self._halt_for_kill_switch()
            logger.warning(
                "SYSTEM LOCKED (db=%s redis=%s db_boot=%s redis_boot=%s); "
                "startup recovery required",
                assessment.db_state,
                assessment.redis_state,
                assessment.db_boot,
                assessment.redis_boot,
            )
            return True

        self._resume_after_kill_switch()
        logger.info(
            "System State: %s. Proceeding.",
            assessment.db_state or assessment.redis_state or SYSTEM_STATE_OK,
        )
        return False

    def _halt_for_kill_switch(self) -> None:
        self._kill_switch_halted = True
        self.execution_engine.halt_and_drain(timeout=0)

    def _clear_local_kill_switch_halt(self) -> None:
        self._kill_switch_halted = False

    def _resume_after_kill_switch(self) -> None:
        self.execution_engine.resume_submissions()
        self._kill_switch_halted = False

    def _start_heartbeat(self):
        """Start the provider-neutral heartbeat worker."""
        self.heartbeat_thread = self._heartbeat_service.start()

    def _active_heartbeat_lifecycle_ids(self) -> list[str]:
        """Return a stable lifecycle-ID snapshot for strategy heartbeats."""
        with self._strategy_lock:
            return sorted(
                {
                    self._portfolio_coordinator.lifecycle_id_for_strategy(strategy_id)
                    for strategy_id in self.strategy_instances
                }
            )

    def _start_runtime_reconciliation(self):
        """Start periodic runtime reconciliation in a daemon thread."""
        interval = (
            self._runtime_reconcile_interval
            if self._runtime_reconcile_interval is not None
            else _runtime_reconciliation_interval_from_env()
        )
        self.runtime_reconcile_thread = self._runtime_reconciliation_service.start(
            interval=interval,
            stop_event=self._runtime_reconcile_stop,
        )

    def _run_runtime_recovery_exclusive(
        self,
        operation: Callable[[], object],
    ) -> object:
        """Run one selected recovery operation under generic exclusion locks."""
        with self._market_processing_lock, self._ops_command_lock:
            return operation()

    def _record_strategy_heartbeats(self, strategy_ids: list[str]) -> None:
        """Record strategy heartbeat state in HealthMonitor and DB."""
        with self._db_session_factory() as db:
            now_ms = int(time.time() * 1000)
            for sid in strategy_ids:
                try:
                    self._health_monitor.update_heartbeat(sid)
                except Exception as e:
                    logger.warning("Failed to update health monitor for %s: %s", sid, e)
                db.query(StrategyState).filter(StrategyState.strategy_id == sid).update(
                    {"last_heartbeat": now_ms}
                )
            db.commit()

    def add_strategy(self, strategy: BaseStrategy):
        """
        Legacy support for static registration.
        """
        self._assert_strategy_live_readiness(type(strategy))
        if self.runtime_environment.identity == "live":
            self._strategy_hydration.fresh_instance_for_replay(strategy)
        with self._runtime_registration_lock:
            with self._strategy_lock:
                if (
                    strategy.strategy_id in self.strategy_instances
                    or strategy.strategy_id in self.portfolio_instances
                    or self._portfolio_coordinator.portfolio_id_for_sleeve(
                        strategy.strategy_id
                    )
                    is not None
                ):
                    raise ValueError(
                        f"strategy runtime ID is already active: {strategy.strategy_id}"
                    )
            self._register_strategy_instance(strategy)
            self._strategy_state_manager.on_state_change_message(
                {
                    "strategy_id": strategy.strategy_id,
                    "status": StrategyStatus.ACTIVE.value,
                }
            )
        logger.info(
            "Registered strategy (legacy): %s for %s",
            strategy.strategy_id,
            strategy.product_id,
        )

    def add_portfolio(self, definition: PortfolioDefinition) -> None:
        """Register one pre-built portfolio for backtest or static runtimes."""
        if (
            self.runtime_environment.identity == "live"
            and definition.readiness != "LIVE_APPROVED"
        ):
            raise RuntimeError(
                f"portfolio_live_approval_required: readiness={definition.readiness}"
            )
        if self.runtime_environment.identity == "live":
            for sleeve in definition.sleeves:
                self._strategy_hydration.fresh_instance_for_replay(sleeve.strategy)
        self._register_portfolio_definition(
            definition,
            publish_active_state=True,
        )

    def build_stream_channels(self) -> list:
        """Derive Redis stream keys from registered strategy requirements."""
        channels = set()
        for strat in self._registry.list_active():
            channels.add(
                to_stream_key(
                    strat.product_id,
                    strat.requirements.timeframe,
                )
            )
        return sorted(channels)

    def on_market_data(self, data: Union[Candlestick, Trade]):
        """
        Callback triggered by DataConsumer when new market data arrives.
        """
        with self._market_processing_lock:
            if isinstance(data, Candlestick):
                with self._live_candle_application.application_fence(data):
                    self._live_candle_application.apply(
                        data,
                        apply_new=self._apply_unpersisted_candle,
                        rebuild_applied=self._pending_market_replay.rebuild_applied,
                    )
                return
            if isinstance(data, Trade):
                self._signal_processor.on_trade(data)

    def on_backtest_market_data(
        self,
        execution_candle: Candlestick,
        decision_candle: Candlestick | None = None,
    ) -> None:
        """Apply simulated fills before an optional completed decision candle.

        Live venues drive fills from authoritative order events and expose only
        completed Rust-aggregated decision candles to strategies. The split
        candle path exists solely to reproduce those venue semantics in a
        simulated backtest adapter.
        """
        if self.runtime_environment.identity == "live":
            raise RuntimeError("split market routing is backtest-only")
        with self._market_processing_lock:
            self.execution_engine.process_market_data(execution_candle)
            if decision_candle is not None:
                self._signal_processor.on_candle(decision_candle)

    def on_backtest_decision_candle(self, decision_candle: Candlestick) -> None:
        """Apply an asynchronously delivered completed decision candle."""
        if self.runtime_environment.identity == "live":
            raise RuntimeError("split market routing is backtest-only")
        with self._market_processing_lock:
            self._signal_processor.on_candle(decision_candle)

    def process_signal(
        self,
        signal: Signal,
        candle: Optional[Candlestick],
        *,
        _entry_admitted: bool = False,
    ) -> bool:
        """
        Handle the signal generated by a strategy and report submission success.
        """
        if not self._runtime_signal_allowed(signal):
            return False
        if not _entry_admitted and not self._entry_signal_allowed(signal):
            return False
        return self._signal_execution.process(signal, candle)

    def _runtime_signal_allowed(self, signal: Signal) -> bool:
        """Apply runtime-wide submission gates before risk or execution."""
        if signal.type == SignalType.NO_SIGNAL:
            return True
        if self._kill_switch_halted:
            logger.warning(
                "Signal rejected because kill switch is active: strategy=%s type=%s",
                signal.strategy_id,
                signal.type,
            )
            return False
        return True

    def _entry_signal_allowed(self, signal: Signal) -> bool:
        """Apply the venue-owned health gate to new exposure only."""
        if (
            signal.type in (SignalType.LONG, SignalType.SHORT)
            and self._entry_admission_gate is not None
            and not self._entry_admission_gate.observe()
        ):
            logger.warning(
                "Entry signal rejected by venue admission gate",
                extra={
                    "component": "strategy_engine",
                    "event_code": "entry_admission_rejected",
                    "strategy_id": signal.strategy_id,
                    "product_id": signal.product_id,
                    "signal_type": signal.type.value,
                },
            )
            return False
        return True

    def _entry_signal_allowed_for_processor(self, signal: Signal) -> bool:
        """Avoid consuming a kill-switch rejection as benign health suppression."""
        if self._kill_switch_halted:
            return True
        return self._entry_signal_allowed(signal)

    def shutdown(
        self,
        timeout: float = 30.0,
        *,
        clean_exit: bool = False,
    ):
        """Graceful shutdown: stop threads, drain executor, close Redis."""
        self._shutdown_service.shutdown(timeout=timeout, clean_exit=clean_exit)

    def _mark_stopping_for_shutdown(self) -> None:
        self.running = False
        self._runtime_reconcile_stop.set()
        self._order_event_stop.set()
