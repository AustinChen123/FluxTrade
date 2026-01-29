import json
import os
import time
import threading
import logging
import traceback
from concurrent.futures import ThreadPoolExecutor
from typing import List, Union, Optional, Dict, Type
import redis
from sqlalchemy.orm import Session
from src.core.models import Candlestick, Trade, Signal, SignalType
from src.core.orm_models import SignalAudit, StrategyState
from src.strategies.base import BaseStrategy
from src.core.risk_manager import RiskManager, AccountService
from src.core.execution import ExecutionEngine
from src.core.clock import Clock
from src.core.interfaces import IOrderRepository
from src.core.strategy_loader import StrategyLoader
from src.core.data_provider import check_data_availability
from src.core.db import SessionLocal
from src.core.adapters import create_adapter

# Redis Config
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
HOT_STRATEGIES_PATH = os.getenv('HOT_STRATEGIES_PATH', '/app/strategies_hot')

logger = logging.getLogger(__name__)

class StrategyEngine:
    def __init__(
        self,
        db_session: Session,
        clock: Clock,
        order_repository: Optional[IOrderRepository] = None,
        account_service: Optional[AccountService] = None,
        adapter_config: Optional[Dict] = None,
    ):
        self.db = db_session
        self.clock = clock
        self.strategies: Dict[str, List[BaseStrategy]] = {}
        self.strategy_instances: Dict[str, BaseStrategy] = {}
        self.loaded_classes: Dict[str, Type[BaseStrategy]] = {}

        # Initialize Services
        self.account_service = account_service if account_service else AccountService()
        self.risk_manager = RiskManager(self.account_service)

        # Initialize Adapter via factory
        if adapter_config is None:
            adapter_config = {"mode": "simulated"}
        try:
            adapter = create_adapter(adapter_config)
            logger.info("StrategyEngine: Using %s", type(adapter).__name__)
        except Exception as e:
            logger.error("Failed to init adapter: %s. Falling back to simulated.", e)
            adapter = create_adapter({"mode": "simulated"})

        self.execution_engine = ExecutionEngine(db_session, clock, adapter, order_repository)
        
        # System State & Heartbeat
        self.redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
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
                        logger.error(f"Error parsing command: {e}")
        
        self.command_thread = threading.Thread(target=command_loop, daemon=True)
        self.command_thread.start()

    def _handle_command(self, data: dict):
        """
        Routes commands to specific handlers.
        """
        cmd = data.get("command")
        params = data.get("params", {})
        
        logger.info(f"Received Command: {cmd} with params {params}")
        
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
                logger.warning(f"Unknown command: {cmd}")
        except Exception as e:
            logger.error(f"Error executing command {cmd}: {e}\n{traceback.format_exc()}")

    def scan_strategies(self):
        """
        Scans for strategy files and syncs with DB.
        """
        logger.info(f"🔍 Scanning for strategies in {HOT_STRATEGIES_PATH}...")
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
                        status="DISCOVERED",
                        config_json="{}"
                    )
                    db.add(state)
                
                if isinstance(result, str):
                    # It was a LoadError (traceback string)
                    state.status = "ERROR"
                    state.performance_json = json.dumps({"error": result})
                
                db.commit()
        logger.info(f"✅ Scan Complete. Total loaded: {len(self.loaded_classes)}")

    def test_run_strategy(self, strategy_id: str, days: int):
        """
        Performs a test run/warm-up for a strategy.
        """
        logger.info(f"🧪 Test Run for {strategy_id} (days={days})")
        if strategy_id not in self.loaded_classes:
            logger.error(f"Strategy {strategy_id} not loaded.")
            return

        with SessionLocal() as db:
            state = db.query(StrategyState).filter(StrategyState.strategy_id == strategy_id).first()
            if not state:
                logger.error(f"Strategy {strategy_id} not in DB.")
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
                    logger.warning(f"⚠️ Insufficient data for {strategy_id}. Command: {backfill_cmd}")
                    state.status = "WARNING"
                    state.performance_json = json.dumps({"backfill_command": backfill_cmd})
                    db.commit()
                    return

                # If OK, update status to READY
                state.status = "READY"
                db.commit()
                logger.info(f"✅ Strategy {strategy_id} is READY.")

            except Exception as e:
                error_trace = traceback.format_exc()
                state.status = "ERROR"
                state.performance_json = json.dumps({"error": error_trace})
                db.commit()
                logger.error(f"❌ Test Run failed for {strategy_id}: {e}")

    def start_strategy(self, strategy_id: str):
        """
        Activates a strategy for live execution.
        """
        logger.info(f"🚀 Starting Strategy: {strategy_id}")
        if strategy_id not in self.loaded_classes:
            logger.error(f"Strategy {strategy_id} not loaded.")
            return

        with SessionLocal() as db:
            state = db.query(StrategyState).filter(StrategyState.strategy_id == strategy_id).first()
            # Allow READY or WARNING (with manual override implied by START command)
            if not state or state.status not in ["READY", "WARNING", "STOPPED", "DISCOVERED"]:
                 logger.error(f"Strategy {strategy_id} is not in startable state (Current: {state.status if state else 'None'})")
                 return

            try:
                config = json.loads(state.config_json or "{}")
                product_id = config.get("product_id", "BINANCE:BTCUSDT-PERP")
                
                strategy_cls = self.loaded_classes[strategy_id]
                instance = strategy_cls(strategy_id, product_id)
                
                # Register
                self.strategy_instances[strategy_id] = instance
                if product_id not in self.strategies:
                    self.strategies[product_id] = []
                self.strategies[product_id].append(instance)
                
                state.status = "ACTIVE"
                state.uptime_start = int(time.time() * 1000)
                db.commit()
                logger.info(f"🔥 Strategy {strategy_id} is now ACTIVE for {product_id}")

            except Exception as e:
                state.status = "ERROR"
                state.performance_json = json.dumps({"error": str(e)})
                db.commit()
                logger.error(f"❌ Failed to start {strategy_id}: {e}")

    def stop_strategy(self, strategy_id: str):
        """
        Deactivates an active strategy.
        """
        logger.info(f"🛑 Stopping Strategy: {strategy_id}")
        if strategy_id not in self.strategy_instances:
            logger.warning(f"Strategy {strategy_id} is not active.")
            return

        instance = self.strategy_instances.pop(strategy_id)
        product_id = instance.product_id
        if product_id in self.strategies:
            self.strategies[product_id] = [s for s in self.strategies[product_id] if s.strategy_id != strategy_id]
        
        with SessionLocal() as db:
            state = db.query(StrategyState).filter(StrategyState.strategy_id == strategy_id).first()
            if state:
                state.status = "STOPPED"
                db.commit()
        
        logger.info(f"✅ Strategy {strategy_id} stopped.")

    def _reconcile_balance(self):
        """
        Startup Reconciliation
        Force overwrite Redis balance from actual Exchange API.
        """
        logger.info("💰 Reconciling Balance...")
        try:
            balance = self.account_service.get_balance("USDT")
            self.redis_client.set("state:balance:USDT", str(balance))
            logger.info(f"✅ Balance Reconciled: {balance} USDT")
        except Exception as e:
            logger.warning(f"⚠️ Balance Reconciliation Failed: {e}. Using DB/Redis state.")

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
                    logger.info(f"✅ System State: {state or 'OK'}. Proceeding.")
                    break
            except Exception as e:
                logger.error(f"❌ Error checking system state: {e}. Retrying...")
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
                    # Update DB heartbeats for active strategies
                    with SessionLocal() as db:
                        now_ms = int(time.time() * 1000)
                        for sid in self.strategy_instances:
                            db.query(StrategyState).filter(StrategyState.strategy_id == sid).update({
                                "last_heartbeat": now_ms
                            })
                        db.commit()
                    time.sleep(1.0)
                except Exception as e:
                    logger.error(f"💓 Heartbeat Failed: {e}")
                    time.sleep(1.0)
        
        self.heartbeat_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        self.heartbeat_thread.start()

    def add_strategy(self, strategy: BaseStrategy):
        """
        Legacy support for static registration.
        """
        if strategy.product_id not in self.strategies:
            self.strategies[strategy.product_id] = []
        self.strategies[strategy.product_id].append(strategy)
        self.strategy_instances[strategy.strategy_id] = strategy
        logger.info(f"Registered strategy (legacy): {strategy.strategy_id} for {strategy.product_id}")

    def build_stream_channels(self) -> list:
        """Derive Redis stream keys from registered strategy requirements."""
        channels = set()
        for product_id, strategies in self.strategies.items():
            parts = product_id.split(":")
            exchange = parts[0].lower()
            symbol = parts[1].replace("-PERP", "").lower()
            for strat in strategies:
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

        strategies = self.strategies.get(data.product_id, [])
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
                logger.error(f"Error in strategy {strategy.strategy_id}: {e}")

    def process_signal(self, signal: Signal, candle: Optional[Candlestick]):
        """
        Handle the signal generated by a strategy.
        """
        if signal.type == SignalType.NO_SIGNAL:
            return

        is_passed, risk_msg = self.risk_manager.check_risk(signal)
        
        order_id = None
        if is_passed:
            logger.info(f"✅ SIGNAL ACCEPTED: {signal.type}. Forwarding to Execution Engine...")
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
            logger.error(f"Failed to log audit trail: {e}")
            self.db.rollback()

    def shutdown(self, timeout: float = 10.0):
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