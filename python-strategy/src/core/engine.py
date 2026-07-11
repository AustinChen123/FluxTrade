import json
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
from src.core.models import Candlestick, Trade, Signal, SignalType, StrategyStatus
from src.core.orm_models import Candlestick as ORMCandlestick
from src.core.orm_models import StrategyState
from src.strategies.base import BaseStrategy
from src.core.risk_manager import RiskManager, AccountService
from src.core.execution import ExecutionEngine
from src.core.clock import Clock
from src.core.interfaces import IExchangeAdapter, IOrderRepository
from src.core.daily_nav_snapshot import DailyNavSnapshotService
from src.core.strategy_loader import StrategyLoader
from src.core.data_provider import check_data_availability
from src.core.adapters import CcxtExchangeAdapter, SimulatedAdapter, create_adapter
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

HOT_STRATEGIES_PATH = os.getenv('HOT_STRATEGIES_PATH', '/app/strategies_hot')
SYSTEM_STATE_KEY = "system:state"
SYSTEM_STATE_LOCKDOWN = "LOCKDOWN"
SYSTEM_STATE_OK = "OK"

logger = logging.getLogger(__name__)


def _is_runtime_reconciliation_enabled(
    adapter: IExchangeAdapter,
    adapter_config: Optional[Dict],
) -> bool:
    if isinstance(adapter, SimulatedAdapter):
        return False
    if isinstance(adapter, CcxtExchangeAdapter):
        return True
    return bool(adapter_config and adapter_config.get("mode") == "live")


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
        if adapter is None:
            try:
                adapter = create_adapter(effective_adapter_config)
                logger.info("StrategyEngine: Using %s", type(adapter).__name__)
            except Exception as e:
                logger.critical("Failed to init adapter: %s. NOT falling back silently.", e)
                raise
        else:
            logger.info("StrategyEngine: Using provided adapter %s", type(adapter).__name__)
        self._runtime_reconciliation_enabled = _is_runtime_reconciliation_enabled(
            adapter,
            adapter_config,
        )

        self.execution_engine = ExecutionEngine(
            db_session,
            clock,
            adapter,
            order_repository,
            journal=journal,
            db_session_factory=self._db_session_factory,
            audit_external_orders=audit_external_orders,
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
        self.executor = ThreadPoolExecutor(max_workers=5)

    def startup(self):
        """
        Runs startup checks and starts background services.
        """
        # Start fail-closed so the command listener can accept a manual clear
        # while startup waits on a persisted LOCKDOWN state.
        self._halt_for_kill_switch()
        self._start_command_listener()
        self._check_system_state()
        self._reconcile_balance()
        self._initialize_strategy_state_cache_on_startup()
        self._start_strategy_state_subscriber_on_startup()
        self._reconcile_recoverable_orders_on_startup()
        self._start_heartbeat()
        if self._runtime_reconciliation_enabled:
            self._start_runtime_reconciliation()
        
        # Initial scan to discover strategies
        self.scan_strategies()
        self._restore_active_strategies_on_startup()

    def _initialize_strategy_state_cache_on_startup(self) -> None:
        """Load strategy lifecycle state into the manager cache."""
        self._strategy_state_manager.initialize_cache_from_db()

    def _start_strategy_state_subscriber_on_startup(self) -> None:
        """Listen for cross-process strategy state updates."""
        self._strategy_state_manager.start_subscriber()

    def _reconcile_recoverable_orders_on_startup(self) -> None:
        """Record startup order reconciliation for audited external orders."""
        if not self.execution_engine.audit_external_orders:
            return

        summary = self.execution_engine.reconcile_recoverable_client_orders()
        logger.info(
            "Startup order reconciliation complete: %s recoverable orders",
            summary["recoverable_count"],
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
                self._halt_for_kill_switch()
                try:
                    self.redis_client.set(SYSTEM_STATE_KEY, SYSTEM_STATE_LOCKDOWN)
                except Exception:
                    logger.exception(
                        "Failed to persist kill switch state; local halt remains active"
                    )
                self.ops_safety.kill_switch(
                    actor=params.get("actor", "operator"),
                    reason=params.get("reason"),
                )
            elif cmd == "CLEAR_KILL_SWITCH":
                self.redis_client.set(SYSTEM_STATE_KEY, SYSTEM_STATE_OK)
                self._resume_after_kill_switch()
            else:
                result = self._command_router.handle(data)
                if result.success:
                    logger.info("Command %s succeeded: %s", cmd, result.message)
                else:
                    logger.warning("Command %s failed: %s", cmd, result.message)
        except Exception as e:
            logger.error("Error executing command %s: %s\n%s", cmd, e, traceback.format_exc())

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
                product_id = config.get("product_id", "BINANCE:BTCUSDT-PERP")
                
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
                product_id = config.get("product_id", "BINANCE:BTCUSDT-PERP")
                
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

    def _check_system_state(self):
        """
        Checks 'system:state'. If 'LOCKDOWN', enters a paused loop.
        """
        logger.info("🔍 Checking System State...")
        while True:
            try:
                state = self.redis_client.get(SYSTEM_STATE_KEY)
                if isinstance(state, bytes):
                    state = state.decode("utf-8")
                if state == SYSTEM_STATE_LOCKDOWN:
                    self._halt_for_kill_switch()
                    logger.warning("⚠️ SYSTEM LOCKED (LOCKDOWN). Waiting for manual resume...")
                    time.sleep(5)
                else:
                    self._resume_after_kill_switch()
                    logger.info("✅ System State: %s. Proceeding.", state or 'OK')
                    break
            except Exception as e:
                logger.error("❌ Error checking system state: %s. Retrying...", e)
                time.sleep(2)

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
                    self.redis_client.setex("heartbeat:python", 3, "1")
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
        interval = float(os.getenv("RUNTIME_RECONCILE_INTERVAL_SECONDS", "3600"))
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

    def shutdown(self, timeout: float = 30.0):
        """Graceful shutdown: stop threads, drain executor, close Redis."""
        logger.info("StrategyEngine shutting down...")
        self.running = False
        self._runtime_reconcile_stop.set()

        self.executor.shutdown(wait=True, cancel_futures=False)

        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=timeout)
        if self.command_thread and self.command_thread.is_alive():
            self.command_thread.join(timeout=timeout)
        if self.runtime_reconcile_thread and self.runtime_reconcile_thread.is_alive():
            self.runtime_reconcile_thread.join(timeout=timeout)

        self._strategy_state_manager.shutdown()

        try:
            self.redis_client.close()
        except Exception as e:
            logger.warning("Error closing Redis: %s", e)

        logger.info("StrategyEngine shutdown complete.")
