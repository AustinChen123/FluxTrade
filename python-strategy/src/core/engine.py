import json
import os
import time
import threading
import logging
import traceback
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union, Optional, Dict, Type
from sqlalchemy.orm import Session
from src.core.models import Candlestick, Trade, Signal, SignalType, StrategyStatus
from src.core.orm_models import SignalAudit, StrategyState
from src.strategies.base import BaseStrategy
from src.core.risk_manager import RiskManager, AccountService
from src.core.execution import ExecutionEngine
from src.core.clock import Clock
from src.core.interfaces import IExchangeAdapter, IOrderRepository
from src.core.strategy_loader import StrategyLoader
from src.core.data_provider import check_data_availability
from src.core.db import SessionLocal
from src.core.adapters import create_adapter
from src.core.journal import StrategyJournal
from src.core.redis_factory import create_redis_client
from src.core.metrics import SIGNALS_TOTAL, ACTIVE_STRATEGIES, BALANCE_USDT
from src.core.command_router import CommandRouter
from src.core.health_monitor import HealthMonitor
from src.core.signal_processor import SignalProcessor
from src.core.strategy_registry import StrategyRegistry

HOT_STRATEGIES_PATH = os.getenv('HOT_STRATEGIES_PATH', '/app/strategies_hot')

logger = logging.getLogger(__name__)


class _EngineStateAdapter:
    """Temporary state-manager adapter until Phase 5 lands."""

    def __init__(self, engine: "StrategyEngine") -> None:
        self._engine = engine

    def transition_to_running(self, strategy_id: str) -> None:
        self._engine.start_strategy(strategy_id)

    def transition_to_stopped(self, strategy_id: str) -> None:
        self._engine.stop_strategy(strategy_id)

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
    ):
        self.db = db_session
        self.clock = clock
        self.strategies: Dict[str, List[BaseStrategy]] = {}
        self.strategy_instances: Dict[str, BaseStrategy] = {}
        self.loaded_classes: Dict[str, Type[BaseStrategy]] = {}
        self._strategy_lock = threading.Lock()
        self._registry = StrategyRegistry()

        # Initialize Services
        self.account_service = account_service if account_service else AccountService()
        self.risk_manager = RiskManager(self.account_service)

        # Use pre-created adapter or build from config
        if adapter is None:
            if adapter_config is None:
                adapter_config = {"mode": "simulated"}
            try:
                adapter = create_adapter(adapter_config)
                logger.info("StrategyEngine: Using %s", type(adapter).__name__)
            except Exception as e:
                logger.critical("Failed to init adapter: %s. NOT falling back silently.", e)
                raise
        else:
            logger.info("StrategyEngine: Using provided adapter %s", type(adapter).__name__)

        self.execution_engine = ExecutionEngine(db_session, clock, adapter, order_repository, journal=journal)
        self._state_manager = _EngineStateAdapter(self)
        self._health_monitor = HealthMonitor(self._registry)
        self._command_router = CommandRouter(
            self._registry,
            self._state_manager,
            self._health_monitor,
        )
        self._signal_processor = SignalProcessor(
            self._registry,
            self.execution_engine,
            self._state_manager,
        )
        
        # System State & Heartbeat
        self.redis_client = create_redis_client()
        self.running = True
        self.heartbeat_thread = None
        self.command_thread = None
        self.executor = ThreadPoolExecutor(max_workers=5)

    def startup(self):
        """
        Runs startup checks and starts background services.
        """
        self._check_system_state()
        self._reconcile_balance()
        self._start_heartbeat()
        self._start_command_listener()
        
        # Initial scan to discover strategies
        self.scan_strategies()

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
        cmd = data.get("command")
        params = data.get("params", {})
        
        logger.info("Received Command: %s with params %s", cmd, params)
        
        try:
            if cmd == "SCAN":
                self.scan_strategies()
            elif cmd == "TEST_RUN":
                self.test_run_strategy(params.get("id"), params.get("days", 1))
            elif cmd == "START":
                self.start_strategy(params.get("id"))
            elif cmd == "STOP":
                self.stop_strategy(params.get("id"))
            else:
                logger.warning("Unknown command: %s", cmd)
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
        with SessionLocal() as db:
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
                
                db.commit()
        logger.info("✅ Scan Complete. Total loaded: %s", len(self.loaded_classes))

    def test_run_strategy(self, strategy_id: str, days: int):
        """
        Performs a test run/warm-up for a strategy.
        """
        logger.info("🧪 Test Run for %s (days=%s)", strategy_id, days)
        if strategy_id not in self.loaded_classes:
            logger.error("Strategy %s not loaded.", strategy_id)
            return

        with SessionLocal() as db:
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
                db.commit()
                logger.error("❌ Test Run failed for %s: %s", strategy_id, e)

    def start_strategy(self, strategy_id: str):
        """
        Activates a strategy for live execution.
        """
        logger.info("🚀 Starting Strategy: %s", strategy_id)
        if strategy_id not in self.loaded_classes:
            logger.error("Strategy %s not loaded.", strategy_id)
            return

        with SessionLocal() as db:
            state = db.query(StrategyState).filter(StrategyState.strategy_id == strategy_id).first()
            # Allow READY or WARNING (with manual override implied by START command)
            startable = {StrategyStatus.READY, StrategyStatus.WARNING, StrategyStatus.STOPPED, StrategyStatus.DISCOVERED}
            if not state or state.status not in startable:
                 logger.error("Strategy %s is not in startable state (Current: %s)", strategy_id, state.status if state else 'None')
                 return

            try:
                config = json.loads(state.config_json or "{}")
                product_id = config.get("product_id", "BINANCE:BTCUSDT-PERP")
                
                strategy_cls = self.loaded_classes[strategy_id]
                instance = strategy_cls(strategy_id, product_id)
                
                # Register (thread-safe)
                with self._strategy_lock:
                    self.strategy_instances[strategy_id] = instance
                    if product_id not in self.strategies:
                        self.strategies[product_id] = []
                    self.strategies[product_id].append(instance)
                    ACTIVE_STRATEGIES.set(len(self.strategy_instances))
                
                state.status = StrategyStatus.ACTIVE
                state.uptime_start = int(time.time() * 1000)
                db.commit()
                logger.info("🔥 Strategy %s is now ACTIVE for %s", strategy_id, product_id)

            except Exception as e:
                state.status = StrategyStatus.ERROR
                state.performance_json = json.dumps({"error": str(e)})
                db.commit()
                logger.error("❌ Failed to start %s: %s", strategy_id, e)

    def stop_strategy(self, strategy_id: str):
        """
        Deactivates an active strategy.
        """
        logger.info("🛑 Stopping Strategy: %s", strategy_id)
        with self._strategy_lock:
            if strategy_id not in self.strategy_instances:
                logger.warning("Strategy %s is not active.", strategy_id)
                return

            instance = self.strategy_instances.pop(strategy_id)
            product_id = instance.product_id
            if product_id in self.strategies:
                self.strategies[product_id] = [s for s in self.strategies[product_id] if s.strategy_id != strategy_id]
            ACTIVE_STRATEGIES.set(len(self.strategy_instances))
        
        with SessionLocal() as db:
            state = db.query(StrategyState).filter(StrategyState.strategy_id == strategy_id).first()
            if state:
                state.status = StrategyStatus.STOPPED
                db.commit()
        
        logger.info("✅ Strategy %s stopped.", strategy_id)

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
                state = self.redis_client.get("system:state")
                if state == "LOCKDOWN":
                    logger.warning("⚠️ SYSTEM LOCKED (LOCKDOWN). Waiting for manual resume...")
                    time.sleep(5)
                else:
                    logger.info("✅ System State: %s. Proceeding.", state or 'OK')
                    break
            except Exception as e:
                logger.error("❌ Error checking system state: %s. Retrying...", e)
                time.sleep(2)

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
                    with SessionLocal() as db:
                        now_ms = int(time.time() * 1000)
                        for sid in active_sids:
                            db.query(StrategyState).filter(StrategyState.strategy_id == sid).update({
                                "last_heartbeat": now_ms
                            })
                        db.commit()
                    time.sleep(1.0)
                except Exception as e:
                    logger.error("💓 Heartbeat Failed: %s", e)
                    time.sleep(1.0)
        
        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

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
        # Simulation/Backtest: Check for pending order fills
        if isinstance(data, Candlestick):
            self.execution_engine.process_market_data(data)

        # Copy strategy list under lock to avoid race with stop_strategy
        with self._strategy_lock:
            strategies = list(self.strategies.get(data.product_id, []))
        for strategy in strategies:
            try:
                if isinstance(data, Trade):
                    signal = strategy.on_trade(data)
                elif isinstance(data, Candlestick):
                    if data.timeframe != strategy.requirements.timeframe:
                        continue
                    signal = strategy.on_candle(data)
                else:
                    signal = strategy.on_candle(data)
                
                if signal:
                    self.process_signal(signal, data if isinstance(data, Candlestick) else None)
            except Exception as e:
                logger.error("Error in strategy %s: %s", strategy.strategy_id, e)

    def process_signal(self, signal: Signal, candle: Optional[Candlestick]):
        """
        Handle the signal generated by a strategy.
        """
        if signal.type == SignalType.NO_SIGNAL:
            return

        import structlog.contextvars
        structlog.contextvars.bind_contextvars(trace_id=uuid.uuid4().hex[:16])

        current_price = candle.close if candle else None
        is_passed, risk_msg = self.risk_manager.check_risk(signal, current_price=current_price)

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
        
        try:
            audit = SignalAudit(
                timestamp=int(self.clock.now() * 1000),
                strategy_id=signal.strategy_id,
                product_id=signal.product_id,
                signal_type=signal.type.value,
                risk_status="PASS" if is_passed else "REJECT",
                risk_message=risk_msg,
                order_id=order_id,
                details_json=json.dumps({
                    "candle": candle.model_dump(mode='json') if candle else None,
                    "signal_metadata": signal.metadata
                })
            )
            self.db.add(audit)
            self.db.commit()
        except Exception as e:
            logger.error("Failed to log audit trail: %s", e)
            self.db.rollback()

    def shutdown(self, timeout: float = 30.0):
        """Graceful shutdown: stop threads, drain executor, close Redis."""
        logger.info("StrategyEngine shutting down...")
        self.running = False

        self.executor.shutdown(wait=True, cancel_futures=False)

        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=timeout)
        if self.command_thread and self.command_thread.is_alive():
            self.command_thread.join(timeout=timeout)

        try:
            self.redis_client.close()
        except Exception as e:
            logger.warning("Error closing Redis: %s", e)

        logger.info("StrategyEngine shutdown complete.")
