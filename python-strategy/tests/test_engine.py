"""
Tests for src/core/engine.py — StrategyEngine

Covers:
- Initialization and adapter fallbacks
- add_strategy (legacy registration)
- build_stream_channels derivation
- on_market_data: timeframe guard, signal routing, exception handling
- process_signal: risk pass/reject, audit trail, DB rollback
- _handle_command: SCAN, TEST_RUN, START, STOP, unknown
- shutdown
"""

from contextlib import nullcontext
from decimal import Decimal
from unittest.mock import MagicMock, call, patch

import pytest

from src.core.models import Candlestick, Signal, SignalType
from src.core.engine import StrategyEngine
from src.core.strategy_state_manager import StrategyStateManager


# =============================================================================
# Helpers
# =============================================================================

@pytest.fixture
def engine(engine_factory):
    """Default engine with all mocks."""
    return engine_factory()


@pytest.fixture
def strategy_instance(mock_strategy_class):
    """A concrete strategy instance."""
    return mock_strategy_class("test_strat", "BINANCE:BTCUSDT-PERP")


def _make_candle(
    product_id="BINANCE:BTCUSDT-PERP",
    timeframe="1m",
    ts=1704067200000,
    close=Decimal("42000"),
):
    return Candlestick(
        product_id=product_id,
        timeframe=timeframe,
        timestamp=ts,
        open=close - Decimal("100"),
        high=close + Decimal("200"),
        low=close - Decimal("200"),
        close=close,
        volume=Decimal("500"),
    )


# =============================================================================
# Initialization
# =============================================================================


class TestEngineInit:

    def test_default_adapter_simulated(self, mock_db_session, mock_clock):
        """When no adapter_config, should default to simulated mode."""
        with patch("src.core.engine.create_redis_client") as mock_factory, \
             patch("src.core.engine.create_adapter") as mock_create:
            mock_factory.return_value = MagicMock()
            mock_adapter = MagicMock()
            mock_create.return_value = mock_adapter

            StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
            )

            mock_create.assert_called_once_with({"mode": "simulated"})

    def test_adapter_config_passed_through(self, mock_db_session, mock_clock):
        """Custom adapter_config should be forwarded to create_adapter."""
        with patch("src.core.engine.create_redis_client") as mock_factory, \
             patch("src.core.engine.create_adapter") as mock_create:
            mock_factory.return_value = MagicMock()
            mock_create.return_value = MagicMock()

            cfg = {"mode": "live", "exchange": "bybit"}
            StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter_config=cfg,
            )

            mock_create.assert_called_once_with(cfg)

    def test_adapter_create_failure_raises(self, mock_db_session, mock_clock):
        """If create_adapter fails, should log critical and re-raise."""
        with patch("src.core.engine.create_redis_client") as mock_factory, \
             patch("src.core.engine.create_adapter") as mock_create:
            mock_factory.return_value = MagicMock()
            mock_create.side_effect = RuntimeError("boom")

            with pytest.raises(RuntimeError, match="boom"):
                StrategyEngine(
                    db_session=mock_db_session,
                    clock=mock_clock,
                    adapter_config={"mode": "live"},
                )

    def test_provided_adapter_used_directly(self, mock_db_session, mock_clock):
        """Pre-created adapter should be used without calling create_adapter."""
        with patch("src.core.engine.create_redis_client") as mock_factory, \
             patch("src.core.engine.create_adapter") as mock_create:
            mock_factory.return_value = MagicMock()
            mock_adapter = MagicMock()

            StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter=mock_adapter,
            )

            mock_create.assert_not_called()

    def test_db_session_factory_passed_to_execution_engine(self, mock_db_session, mock_clock):
        """Injected DB session factory should be shared with ExecutionEngine."""
        db_session_factory = MagicMock()

        with patch("src.core.engine.create_redis_client") as mock_factory:
            mock_factory.return_value = MagicMock()
            engine = StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter=MagicMock(),
                db_session_factory=db_session_factory,
            )

        assert engine._db_session_factory is db_session_factory
        assert engine.execution_engine._db_session_factory is db_session_factory

    def test_audit_external_orders_passed_to_execution_engine(self, mock_db_session, mock_clock):
        """Accepted-signal audit mode should be delegated to ExecutionEngine."""
        with patch("src.core.engine.create_redis_client") as mock_factory:
            mock_factory.return_value = MagicMock()
            engine = StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter=MagicMock(),
                audit_external_orders=True,
            )

        assert engine.execution_engine.audit_external_orders is True

    def test_strategies_start_empty(self, engine):
        """Strategies dicts should start empty."""
        assert engine.strategies == {}
        assert engine.strategy_instances == {}

    def test_component_scaffold_created(self, engine):
        """Engine should wire the extracted Phase 1 components."""
        assert engine._registry.list_active() == []
        assert engine._health_monitor.registry is engine._registry
        assert engine._command_router.registry is engine._registry
        assert engine._signal_processor.registry is engine._registry
        assert engine._signal_processor.execution_engine is engine.execution_engine
        assert isinstance(engine._strategy_state_manager, StrategyStateManager)
        assert engine._strategy_state_manager._redis_client is engine.redis_client
        assert engine._signal_processor.state_manager is engine._strategy_state_manager

    def test_startup_initializes_strategy_state_cache(self, engine):
        """Startup should load strategy state into the manager cache."""
        engine._strategy_state_manager.initialize_cache_from_db = MagicMock()

        engine._initialize_strategy_state_cache_on_startup()

        engine._strategy_state_manager.initialize_cache_from_db.assert_called_once_with()

    def test_startup_starts_strategy_state_subscriber(self, engine):
        """Startup should subscribe to cross-process strategy state changes."""
        engine._strategy_state_manager.start_subscriber = MagicMock()

        engine._start_strategy_state_subscriber_on_startup()

        engine._strategy_state_manager.start_subscriber.assert_called_once_with()

    def test_startup_reconcile_skipped_when_audit_external_orders_disabled(self, engine):
        """Startup order reconciliation should only run for audited external orders."""
        engine.execution_engine.reconcile_recoverable_client_orders = MagicMock()

        engine._reconcile_recoverable_orders_on_startup()

        engine.execution_engine.reconcile_recoverable_client_orders.assert_not_called()

    def test_startup_reconcile_runs_when_audit_external_orders_enabled(self, mock_db_session, mock_clock):
        """Audited external order mode should reconcile recoverable orders on startup."""
        with patch("src.core.engine.create_redis_client") as mock_factory:
            mock_factory.return_value = MagicMock()
            engine = StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter=MagicMock(),
                audit_external_orders=True,
            )
        engine.execution_engine.reconcile_recoverable_client_orders = MagicMock(
            return_value={"recoverable_count": 2}
        )

        engine._reconcile_recoverable_orders_on_startup()

        engine.execution_engine.reconcile_recoverable_client_orders.assert_called_once_with()


# =============================================================================
# add_strategy (legacy)
# =============================================================================


class TestAddStrategy:

    def test_registers_strategy_by_product(self, engine, strategy_instance):
        """Should register strategy in strategies dict keyed by product_id."""
        engine.add_strategy(strategy_instance)

        assert "BINANCE:BTCUSDT-PERP" in engine.strategies
        assert strategy_instance in engine.strategies["BINANCE:BTCUSDT-PERP"]

    def test_registers_strategy_instance(self, engine, strategy_instance):
        """Should register in strategy_instances dict."""
        engine.add_strategy(strategy_instance)

        assert "test_strat" in engine.strategy_instances

    def test_registers_strategy_in_registry(self, engine, strategy_instance):
        """Should keep the new registry in sync with legacy dicts."""
        engine.add_strategy(strategy_instance)

        assert engine._registry.get("test_strat") is strategy_instance

    def test_registers_strategy_as_active_in_state_cache(self, engine, strategy_instance):
        """Legacy static registration should keep state guard cache active."""
        engine.add_strategy(strategy_instance)

        assert engine._strategy_state_manager.is_running("test_strat") is True

    def test_multiple_strategies_same_product(self, engine, mock_strategy_class):
        """Multiple strategies on same product should coexist."""
        s1 = mock_strategy_class("strat_a", "BINANCE:BTCUSDT-PERP")
        s2 = mock_strategy_class("strat_b", "BINANCE:BTCUSDT-PERP")

        engine.add_strategy(s1)
        engine.add_strategy(s2)

        assert len(engine.strategies["BINANCE:BTCUSDT-PERP"]) == 2

    def test_strategies_different_products(self, engine, mock_strategy_class):
        """Strategies on different products should be in separate keys."""
        s1 = mock_strategy_class("strat_btc", "BINANCE:BTCUSDT-PERP")
        s2 = mock_strategy_class("strat_eth", "BINANCE:ETHUSDT-PERP")

        engine.add_strategy(s1)
        engine.add_strategy(s2)

        assert "BINANCE:BTCUSDT-PERP" in engine.strategies
        assert "BINANCE:ETHUSDT-PERP" in engine.strategies


# =============================================================================
# build_stream_channels
# =============================================================================


class TestBuildStreamChannels:

    def test_empty_when_no_strategies(self, engine):
        """Should return empty list when no strategies registered."""
        assert engine.build_stream_channels() == []

    def test_channels_use_registry(self, engine, strategy_instance):
        """Stream channels should be derived from StrategyRegistry."""
        engine._registry.register(strategy_instance)

        assert engine.build_stream_channels() == ["stream:market:binance:btcusdt:1m"]

    def test_single_strategy_channel(self, engine, strategy_instance):
        """Should derive correct Redis stream key."""
        engine.add_strategy(strategy_instance)
        channels = engine.build_stream_channels()

        assert channels == ["stream:market:binance:btcusdt:1m"]

    def test_deduplicates_channels(self, engine, mock_strategy_class):
        """Same product+timeframe from multiple strategies should produce one channel."""
        s1 = mock_strategy_class("a", "BINANCE:BTCUSDT-PERP")
        s2 = mock_strategy_class("b", "BINANCE:BTCUSDT-PERP")

        engine.add_strategy(s1)
        engine.add_strategy(s2)
        channels = engine.build_stream_channels()

        assert len(channels) == 1
        assert channels == ["stream:market:binance:btcusdt:1m"]

    def test_multiple_products(self, engine, mock_strategy_class):
        """Different products should produce different channels."""
        s1 = mock_strategy_class("btc_strat", "BINANCE:BTCUSDT-PERP")
        s2 = mock_strategy_class("eth_strat", "BINANCE:ETHUSDT-PERP")

        engine.add_strategy(s1)
        engine.add_strategy(s2)
        channels = engine.build_stream_channels()

        assert len(channels) == 2
        assert "stream:market:binance:btcusdt:1m" in channels
        assert "stream:market:binance:ethusdt:1m" in channels

    def test_channels_sorted(self, engine, mock_strategy_class):
        """Channels should be returned in sorted order."""
        s1 = mock_strategy_class("z_strat", "BINANCE:ZZUSDT-PERP")
        s2 = mock_strategy_class("a_strat", "BINANCE:AAUSDT-PERP")

        engine.add_strategy(s1)
        engine.add_strategy(s2)
        channels = engine.build_stream_channels()

        assert channels == sorted(channels)


# =============================================================================
# on_market_data
# =============================================================================


class TestOnMarketData:

    def test_calls_process_market_data_for_candlestick(self, engine, strategy_instance):
        """Should call execution_engine.process_market_data for Candlestick."""
        engine.add_strategy(strategy_instance)
        engine.execution_engine.process_market_data = MagicMock()

        candle = _make_candle()
        engine.on_market_data(candle)

        engine.execution_engine.process_market_data.assert_called_once_with(candle)

    def test_timeframe_guard_skips_wrong_timeframe(self, engine, strategy_instance):
        """Strategy should NOT receive candle with non-matching timeframe."""
        engine.add_strategy(strategy_instance)
        strategy_instance.on_candle = MagicMock()

        candle = _make_candle(timeframe="5m")  # strategy requires "1m"
        engine.on_market_data(candle)

        strategy_instance.on_candle.assert_not_called()

    def test_matching_timeframe_calls_on_candle(self, engine, strategy_instance):
        """Strategy should receive candle with matching timeframe."""
        engine.add_strategy(strategy_instance)
        strategy_instance.on_candle = MagicMock(return_value=None)

        candle = _make_candle(timeframe="1m")
        engine.on_market_data(candle)

        strategy_instance.on_candle.assert_called_once_with(candle)

    def test_signal_forwarded_to_process_signal(self, engine, strategy_instance):
        """Signal from strategy should be forwarded to process_signal."""
        engine.add_strategy(strategy_instance)
        signal = Signal(
            strategy_id="test_strat",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        strategy_instance._signal = signal
        engine.process_signal = MagicMock()

        candle = _make_candle()
        engine.on_market_data(candle)

        engine.process_signal.assert_called_once()

    def test_no_signal_not_forwarded(self, engine, strategy_instance):
        """NO_SIGNAL should not trigger process_signal."""
        engine.add_strategy(strategy_instance)
        # Default behavior returns NO_SIGNAL
        engine.process_signal = MagicMock()

        candle = _make_candle()
        engine.on_market_data(candle)

        engine.process_signal.assert_not_called()

    def test_strategy_exception_logged_not_raised(self, engine, strategy_instance):
        """Exception in strategy.on_candle should be caught, not propagate."""
        engine.add_strategy(strategy_instance)
        strategy_instance.on_candle = MagicMock(side_effect=RuntimeError("boom"))

        candle = _make_candle()
        # Should not raise
        engine.on_market_data(candle)

    def test_no_strategies_for_product(self, engine):
        """Candle for unregistered product should not error."""
        candle = _make_candle(product_id="BINANCE:XYZUSDT-PERP")
        engine.execution_engine.process_market_data = MagicMock()

        # Should not raise
        engine.on_market_data(candle)


# =============================================================================
# process_signal
# =============================================================================


class TestProcessSignal:

    def test_no_signal_returns_early(self, engine, mock_clock):
        """NO_SIGNAL should return immediately."""
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.NO_SIGNAL,
            value=Decimal("42000"),
        )
        engine.execution_engine.execute_signal = MagicMock()

        engine.process_signal(signal, None)

        engine.execution_engine.execute_signal.assert_not_called()

    def test_risk_pass_executes_signal(self, engine):
        """When risk check passes, signal should be executed."""
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock(return_value="order-123")

        engine.process_signal(signal, _make_candle())

        engine.execution_engine.execute_signal.assert_called_once()

    def test_risk_reject_skips_execution(self, engine):
        """When risk check fails, signal should NOT be executed."""
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        engine.risk_manager.check_risk = MagicMock(
            return_value=(False, "REJECT: Max exposure")
        )
        engine.execution_engine.execute_signal = MagicMock()

        engine.process_signal(signal, _make_candle())

        engine.execution_engine.execute_signal.assert_not_called()

    def test_audit_trail_written_on_pass(self, engine, mock_db_session):
        """Audit entry should be committed to DB on risk pass."""
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock(return_value="order-1")

        engine.process_signal(signal, _make_candle())

        mock_db_session.add.assert_called()
        mock_db_session.commit.assert_called()

    def test_audit_trail_written_on_reject(self, engine, mock_db_session):
        """Audit entry should also be committed on risk reject."""
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        engine.risk_manager.check_risk = MagicMock(
            return_value=(False, "REJECT: No balance")
        )

        engine.process_signal(signal, None)

        mock_db_session.add.assert_called()

    def test_audit_db_failure_triggers_rollback(self, engine, mock_db_session):
        """If audit commit fails, rollback and raise."""
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock(return_value="order-1")
        mock_db_session.commit.side_effect = Exception("DB write fail")

        with pytest.raises(Exception, match="DB write fail"):
            engine.process_signal(signal, _make_candle())

        mock_db_session.rollback.assert_called()

    def test_audited_execution_skips_legacy_pass_audit(self, engine_factory, mock_db_session):
        """When execution writes intent/outcome, accepted signals should not duplicate audit rows."""
        engine = engine_factory(audit_external_orders=True)
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock(return_value="order-1")

        engine.process_signal(signal, _make_candle())

        engine.execution_engine.execute_signal.assert_called_once()
        mock_db_session.add.assert_not_called()
        mock_db_session.commit.assert_not_called()

    def test_audited_execution_still_audits_risk_reject(self, engine_factory, mock_db_session):
        """Risk rejects have no external order outcome, so legacy risk audit still applies."""
        engine = engine_factory(audit_external_orders=True)
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(False, "REJECT"))
        engine.execution_engine.execute_signal = MagicMock()

        engine.process_signal(signal, _make_candle())

        engine.execution_engine.execute_signal.assert_not_called()
        mock_db_session.add.assert_called_once()
        mock_db_session.commit.assert_called_once()


# =============================================================================
# _handle_command
# =============================================================================


class TestHandleCommand:

    def test_scan_command(self, engine):
        """SCAN command should call scan_strategies."""
        engine.scan_strategies = MagicMock()

        engine._handle_command({"command": "SCAN"})

        engine.scan_strategies.assert_called_once()

    def test_start_command(self, engine):
        """START command should call start_strategy with id."""
        engine.start_strategy = MagicMock()

        engine._handle_command({"command": "START", "params": {"id": "strat_1"}})

        engine.start_strategy.assert_called_once_with("strat_1")

    def test_stop_command(self, engine):
        """STOP command should call stop_strategy with id."""
        engine.stop_strategy = MagicMock()

        engine._handle_command({"command": "STOP", "params": {"id": "strat_1"}})

        engine.stop_strategy.assert_called_once_with("strat_1")

    def test_router_command_delegated(self, engine):
        """Router-owned commands should be delegated to CommandRouter."""
        engine._command_router.handle = MagicMock()

        data = {"command": "LIST"}
        engine._handle_command(data)

        engine._command_router.handle.assert_called_once_with(data)

    def test_test_run_command(self, engine):
        """TEST_RUN command should call test_run_strategy with id and days."""
        engine.test_run_strategy = MagicMock()

        engine._handle_command({
            "command": "TEST_RUN",
            "params": {"id": "strat_1", "days": 3}
        })

        engine.test_run_strategy.assert_called_once_with("strat_1", 3)

    def test_test_run_default_days(self, engine):
        """TEST_RUN without days param should default to 1."""
        engine.test_run_strategy = MagicMock()

        engine._handle_command({
            "command": "TEST_RUN",
            "params": {"id": "strat_1"}
        })

        engine.test_run_strategy.assert_called_once_with("strat_1", 1)

    def test_unknown_command_does_not_raise(self, engine):
        """Unknown commands should be logged but not raise."""
        engine._handle_command({"command": "NONEXISTENT"})

    def test_command_exception_caught(self, engine):
        """Exceptions in command handlers should be caught."""
        engine.scan_strategies = MagicMock(side_effect=RuntimeError("scan fail"))

        # Should not raise
        engine._handle_command({"command": "SCAN"})


# =============================================================================
# heartbeat recording
# =============================================================================


class TestHeartbeatRecording:

    def test_record_strategy_heartbeats_updates_health_monitor_and_db(self, engine):
        """Strategy heartbeat recording should update HealthMonitor and DB state."""
        engine._health_monitor.update_heartbeat = MagicMock()
        mock_db = MagicMock()
        engine._db_session_factory = lambda: nullcontext(mock_db)

        with patch("src.core.engine.time.time", return_value=100.0):
            engine._record_strategy_heartbeats(["s1", "s2"])

        assert engine._health_monitor.update_heartbeat.call_args_list == [
            call("s1"),
            call("s2"),
        ]
        assert mock_db.query.return_value.filter.return_value.update.call_count == 2
        mock_db.commit.assert_called_once()

    def test_record_strategy_heartbeats_commits_when_health_monitor_fails(self, engine):
        """DB heartbeat updates should still commit if HealthMonitor update fails."""
        engine._health_monitor.update_heartbeat = MagicMock(side_effect=RuntimeError("boom"))
        mock_db = MagicMock()
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine._record_strategy_heartbeats(["s1"])

        mock_db.query.return_value.filter.return_value.update.assert_called_once()
        mock_db.commit.assert_called_once()


# =============================================================================
# shutdown
# =============================================================================


class TestScanStrategies:

    def test_scan_updates_loaded_classes(self, engine, mock_strategy_class):
        """scan_strategies should update loaded_classes from StrategyLoader results."""
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        engine._db_session_factory = lambda: nullcontext(mock_db)

        with patch("src.core.engine.StrategyLoader.scan_directory") as mock_scan:
            mock_scan.return_value = {"test.py::MyStrat": mock_strategy_class}

            engine.scan_strategies()

        assert "test.py::MyStrat" in engine.loaded_classes

    def test_scan_creates_db_state_for_new_strategy(self, engine, mock_strategy_class):
        """Newly discovered strategies should get a StrategyState record."""
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        engine._db_session_factory = lambda: nullcontext(mock_db)

        with patch("src.core.engine.StrategyLoader.scan_directory") as mock_scan:
            mock_scan.return_value = {"new.py::NewStrat": mock_strategy_class}

            engine.scan_strategies()

        mock_db.add.assert_called()
        mock_db.commit.assert_called()

    def test_scan_marks_load_errors(self, engine):
        """Strategy with LoadError should get ERROR status in DB."""
        mock_state = MagicMock()
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        with patch("src.core.engine.StrategyLoader.scan_directory") as mock_scan:
            mock_scan.return_value = {"bad.py::LoadError": "traceback string"}
            engine.scan_strategies()

        assert mock_state.status == "ERROR"


class TestStartStrategy:

    def test_start_unloaded_strategy_does_nothing(self, engine):
        """Starting an unloaded strategy should return early."""
        engine.start_strategy("nonexistent.py::X")
        assert "nonexistent.py::X" not in engine.strategy_instances

    def test_start_loaded_strategy_activates(self, engine, mock_strategy_class):
        """Starting a loaded strategy should register instance and set ACTIVE."""
        engine.loaded_classes["test.py::MyStrat"] = mock_strategy_class

        mock_state = MagicMock()
        mock_state.status = "READY"
        mock_state.config_json = "{}"
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine.start_strategy("test.py::MyStrat")

        assert "test.py::MyStrat" in engine.strategy_instances
        assert mock_state.status == "ACTIVE"

    def test_start_wrong_state_rejected(self, engine, mock_strategy_class):
        """Strategy in ERROR state should not be started."""
        engine.loaded_classes["test.py::MyStrat"] = mock_strategy_class

        mock_state = MagicMock()
        mock_state.status = "ERROR"
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine.start_strategy("test.py::MyStrat")

        assert "test.py::MyStrat" not in engine.strategy_instances


class TestStopStrategy:

    def test_stop_active_strategy(self, engine, strategy_instance):
        """Stopping an active strategy should remove it from instances."""
        engine.add_strategy(strategy_instance)

        mock_state = MagicMock()
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine.stop_strategy("test_strat")

        assert "test_strat" not in engine.strategy_instances
        assert mock_state.status == "STOPPED"

    def test_stop_inactive_strategy_warns(self, engine):
        """Stopping a non-active strategy should not crash."""
        engine.stop_strategy("nonexistent")
        # Should complete without error


class TestTestRunStrategy:

    def test_test_run_unloaded_returns_early(self, engine):
        """test_run on unloaded strategy should return."""
        engine.test_run_strategy("nonexistent", 1)
        # No crash

    def test_test_run_data_available_sets_ready(self, engine, mock_strategy_class):
        """When data is available, strategy should be set to READY."""
        engine.loaded_classes["test.py::MyStrat"] = mock_strategy_class

        mock_state = MagicMock()
        mock_state.config_json = "{}"
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        with patch("src.core.engine.check_data_availability", return_value=(True, "")):
            engine.test_run_strategy("test.py::MyStrat", 1)

        assert mock_state.status == "READY"

    def test_test_run_data_insufficient_sets_warning(self, engine, mock_strategy_class):
        """When data is insufficient, strategy should be set to WARNING."""
        engine.loaded_classes["test.py::MyStrat"] = mock_strategy_class

        mock_state = MagicMock()
        mock_state.config_json = "{}"
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        with patch("src.core.engine.check_data_availability", return_value=(False, "docker exec ...")):
            engine.test_run_strategy("test.py::MyStrat", 1)

        assert mock_state.status == "WARNING"


# =============================================================================
# shutdown
# =============================================================================


class TestShutdown:

    def test_sets_running_false(self, engine):
        """Shutdown should set running to False."""
        engine.shutdown(timeout=0.1)
        assert engine.running is False

    def test_closes_redis(self, engine):
        """Shutdown should close Redis client."""
        engine.shutdown(timeout=0.1)
        engine.redis_client.close.assert_called()

    def test_shuts_down_executor(self, engine):
        """Shutdown should shutdown the thread pool executor."""
        engine.executor = MagicMock()
        engine.shutdown(timeout=0.1)
        engine.executor.shutdown.assert_called_once()

    def test_shutdown_stops_strategy_state_manager(self, engine):
        """Shutdown should stop the strategy state subscriber."""
        engine._strategy_state_manager.shutdown = MagicMock()

        engine.shutdown(timeout=0.1)

        engine._strategy_state_manager.shutdown.assert_called_once_with()

    def test_redis_close_error_handled(self, engine):
        """Redis close error should not propagate."""
        engine.redis_client.close.side_effect = Exception("close fail")
        # Should not raise
        engine.shutdown(timeout=0.1)
