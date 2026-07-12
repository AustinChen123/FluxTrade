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
import json
from unittest.mock import MagicMock, call, patch

import pytest

from src.core.models import Candlestick, Position, PositionSide, Signal, SignalType, StrategyStatus
from src.core.orm_models import Candlestick as ORMCandlestick, StrategyState
from src.core.daily_nav_snapshot import DailyNavSnapshotService
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.adapters.simulated import SimulatedAdapter
from src.core.product_registry import InstrumentSpec
from src.core.engine import (
    SYSTEM_BOOT_STATE_KEY,
    StrategyEngine,
    _is_runtime_reconciliation_enabled,
)
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

    def test_risk_manager_uses_adapter_instrument_specs(self, engine_factory):
        spec = InstrumentSpec(
            product_id="BINANCE:BTCUSDT-PERP",
            exchange="test",
            symbol="MNQ",
            base="MNQ",
            quote="USD",
            multiplier=Decimal("2"),
        )
        adapter = SimulatedAdapter(instrument_spec=spec)

        engine = engine_factory(adapter=adapter)

        assert engine.risk_manager.instrument_spec_resolver(spec.product_id) is spec

    @pytest.mark.parametrize(
        ("adapter", "adapter_config", "expected"),
        [
            (object.__new__(SimulatedAdapter), None, False),
            (object.__new__(CcxtExchangeAdapter), None, True),
            (object.__new__(SimulatedAdapter), {"mode": "live"}, False),
            (object.__new__(CcxtExchangeAdapter), {"mode": "simulated"}, True),
            (MagicMock(), {"mode": "live"}, True),
            (MagicMock(), None, False),
        ],
    )
    def test_runtime_reconciliation_mode_uses_actual_adapter(
        self, adapter, adapter_config, expected
    ):
        assert _is_runtime_reconciliation_enabled(adapter, adapter_config) is expected

    @pytest.mark.parametrize(
        "interval",
        ["0", "-1", "nan", "inf", "-inf", "", "not-a-number"],
    )
    def test_live_engine_rejects_invalid_runtime_reconciliation_interval(
        self, mock_db_session, mock_clock, monkeypatch, interval
    ):
        monkeypatch.setenv("RUNTIME_RECONCILE_INTERVAL_SECONDS", interval)
        with patch("src.core.engine.create_redis_client") as redis_factory, patch(
            "src.core.engine.create_adapter"
        ) as create_adapter:
            redis_factory.return_value = MagicMock()
            create_adapter.return_value = MagicMock()

            with pytest.raises(
                ValueError,
                match="RUNTIME_RECONCILE_INTERVAL_SECONDS",
            ):
                StrategyEngine(
                    db_session=mock_db_session,
                    clock=mock_clock,
                    adapter_config={"mode": "live"},
                )

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

    def test_runtime_reconciliation_uses_configured_product_universe(
        self, mock_db_session, mock_clock
    ):
        """Runtime reconciliation must scan configured products even when local is flat."""
        with patch("src.core.engine.create_redis_client") as mock_factory, \
             patch("src.core.engine.create_adapter") as mock_create:
            mock_factory.return_value = MagicMock()
            mock_create.return_value = MagicMock()

            engine = StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter_config={
                    "mode": "live",
                    "exchange": "binance",
                    "instrument_product_ids": ["BINANCE:BTCUSDT-PERP"],
                },
            )

        assert engine.runtime_reconciliation_job._product_ids == (
            "BINANCE:BTCUSDT-PERP",
        )

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
        assert engine.risk_manager.state_manager is engine._strategy_state_manager
        assert isinstance(engine._daily_nav_snapshot_service, DailyNavSnapshotService)
        assert engine.risk_manager.daily_nav_service is engine._daily_nav_snapshot_service

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

    def test_startup_restores_loaded_active_strategies(self, engine):
        """Restart should re-instantiate previously ACTIVE strategies."""
        active_state = MagicMock()
        active_state.strategy_id = "test.py::ActiveStrategy"
        missing_state = MagicMock()
        missing_state.strategy_id = "test.py::MissingStrategy"

        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.all.return_value = [
            active_state,
            missing_state,
        ]
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine.loaded_classes["test.py::ActiveStrategy"] = MagicMock()
        engine.activate_strategy = MagicMock()
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine._restore_active_strategies_on_startup()

        engine.activate_strategy.assert_called_once_with(
            "test.py::ActiveStrategy",
            actor="system",
            reason="startup_restore",
            force=True,
        )
        engine._strategy_state_manager.transition_to_error.assert_called_once_with(
            "test.py::MissingStrategy",
            "startup_restore_class_missing",
            actor="system",
        )


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

    def test_same_direction_entry_with_existing_position_still_uses_risk(self, engine):
        """Scale-ins are normal live signals and stay under risk-manager control."""
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        engine.account_service.set_position(
            Position(
                strategy_id="test",
                product_id="BINANCE:BTCUSDT-PERP",
                side=PositionSide.LONG,
                quantity=Decimal("0.01"),
                entry_price=Decimal("42000"),
                unrealized_pnl=Decimal("0"),
            )
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock()

        engine.process_signal(signal, _make_candle())

        engine.risk_manager.check_risk.assert_called_once()
        engine.execution_engine.execute_signal.assert_called_once()

    def test_exit_signal_with_existing_position_still_executes(self, engine):
        """Restart idempotency only blocks duplicate entries, not exits."""
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.EXIT_LONG,
            value=Decimal("42000"),
        )
        engine.account_service.set_position(
            Position(
                strategy_id="test",
                product_id="BINANCE:BTCUSDT-PERP",
                side=PositionSide.LONG,
                quantity=Decimal("0.01"),
                entry_price=Decimal("42000"),
                unrealized_pnl=Decimal("0"),
            )
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock(return_value="order-123")

        engine.process_signal(signal, _make_candle())

        engine.risk_manager.check_risk.assert_called_once()
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
        """START command should activate the strategy through lifecycle orchestration."""
        engine.activate_strategy = MagicMock()

        engine._handle_command({"command": "START", "params": {"id": "strat_1"}})

        engine.activate_strategy.assert_called_once_with("strat_1")

    def test_stop_command(self, engine):
        """STOP command should deactivate the strategy through lifecycle orchestration."""
        engine.deactivate_strategy = MagicMock()

        engine._handle_command({
            "command": "STOP",
            "params": {"id": "strat_1", "reason": "maintenance"},
        })

        engine.deactivate_strategy.assert_called_once_with(
            "strat_1",
            actor="operator",
            reason="maintenance",
        )

    def test_resume_command_forces_activation(self, engine):
        """RESUME command should pass force and reason to lifecycle orchestration."""
        engine.activate_strategy = MagicMock()

        engine._handle_command({
            "cmd": "RESUME",
            "strategy_id": "strat_1",
            "reason": "operator confirmed",
        })

        engine.activate_strategy.assert_called_once_with(
            "strat_1",
            actor="operator",
            force=True,
            reason="operator confirmed",
        )

    def test_force_recover_command_forces_activation(self, engine):
        """FORCE_RECOVER command should pass force and reason to lifecycle orchestration."""
        engine.activate_strategy = MagicMock()

        engine._handle_command({
            "cmd": "FORCE_RECOVER",
            "params": {
                "strategy_id": "strat_1",
                "reason": "manual reset",
            },
        })

        engine.activate_strategy.assert_called_once_with(
            "strat_1",
            actor="operator",
            force=True,
            reason="manual reset",
        )

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
        assert "traceback string" in mock_state.last_error_message
        assert mock_state.entered_error_at is not None


class TestStartStrategy:

    def test_start_unloaded_strategy_does_nothing(self, engine):
        """Starting an unloaded strategy should return early."""
        engine.start_strategy("nonexistent.py::X")
        assert "nonexistent.py::X" not in engine.strategy_instances

    def test_start_loaded_strategy_activates(self, engine, mock_strategy_class):
        """Starting a loaded strategy should register instance and transition ACTIVE."""
        engine.loaded_classes["test.py::MyStrat"] = mock_strategy_class
        engine._strategy_state_manager.transition_to_running = MagicMock()

        mock_state = MagicMock()
        mock_state.status = "READY"
        mock_state.config_json = "{}"
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._warm_up_strategy_instance = MagicMock(return_value=0)

        engine.start_strategy("test.py::MyStrat")

        assert "test.py::MyStrat" in engine.strategy_instances
        engine._warm_up_strategy_instance.assert_called_once()
        engine._strategy_state_manager.transition_to_running.assert_called_once_with(
            "test.py::MyStrat",
            actor="operator",
            force=False,
            reason=None,
        )

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
        engine._strategy_state_manager.transition_to_stopped = MagicMock()

        mock_state = MagicMock()
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine.stop_strategy("test_strat")

        assert "test_strat" not in engine.strategy_instances
        engine._strategy_state_manager.transition_to_stopped.assert_called_once_with(
            "test_strat",
            actor="operator",
            reason=None,
        )

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

    def test_test_run_exception_sets_error_metadata(self, engine):
        """Warm-up failures should satisfy ERROR state metadata constraints."""
        class FailingStrategy:
            def __init__(self, strategy_id, product_id):
                raise RuntimeError("warm-up failed")

        engine.loaded_classes["test.py::FailingStrat"] = FailingStrategy

        mock_state = MagicMock()
        mock_state.config_json = "{}"
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine.test_run_strategy("test.py::FailingStrat", 1)

        assert mock_state.status == "ERROR"
        assert "warm-up failed" in mock_state.last_error_message
        assert mock_state.entered_error_at is not None


class TestStrategyWarmup:

    class _FakeQuery:
        def __init__(self, model, state, candles):
            self.model = model
            self.state = state
            self.candles = candles

        def filter(self, *args, **kwargs):
            return self

        def order_by(self, *args, **kwargs):
            return self

        def limit(self, *args, **kwargs):
            return self

        def first(self):
            return self.state if self.model is StrategyState else None

        def all(self):
            return self.candles if self.model is ORMCandlestick else []

    def test_activate_strategy_replays_recent_candles_without_orders(self, engine):
        """Activation warm-up rebuilds strategy memory before live signals."""
        class WarmupStrategy:
            def __init__(self, strategy_id, product_id):
                from src.strategies.base import StrategyRequirements

                self.strategy_id = strategy_id
                self.product_id = product_id
                self._requirements = StrategyRequirements(product_id, "1m", 2)
                self.candles_received = []
                self.position = 0
                self._in_position = False

            @property
            def requirements(self):
                return self._requirements

            def on_candle(self, candle):
                self.candles_received.append(candle)
                self.position = 1
                self._in_position = True
                return Signal(
                    strategy_id=self.strategy_id,
                    product_id=self.product_id,
                    timeframe="1m",
                    timestamp=candle.timestamp,
                    type=SignalType.LONG,
                    value=candle.close,
                )

        state = MagicMock()
        state.status = StrategyStatus.READY
        state.config_json = "{}"
        rows = [
            ORMCandlestick(
                product_id="BINANCE:BTCUSDT-PERP",
                timeframe="1m",
                timestamp=1704067200000,
                open=Decimal("41900"),
                high=Decimal("42100"),
                low=Decimal("41800"),
                close=Decimal("42000"),
                volume=Decimal("10"),
            ),
            ORMCandlestick(
                product_id="BINANCE:BTCUSDT-PERP",
                timeframe="1m",
                timestamp=1704067260000,
                open=Decimal("42000"),
                high=Decimal("42200"),
                low=Decimal("41900"),
                close=Decimal("42100"),
                volume=Decimal("11"),
            ),
        ]
        mock_db = MagicMock()
        mock_db.query.side_effect = lambda model: self._FakeQuery(model, state, rows)
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine.loaded_classes["test.py::WarmupStrategy"] = WarmupStrategy
        engine.process_signal = MagicMock()
        engine._strategy_state_manager.transition_to_running = MagicMock()

        engine.activate_strategy("test.py::WarmupStrategy")

        instance = engine.strategy_instances["test.py::WarmupStrategy"]
        assert [c.timestamp for c in instance.candles_received] == [
            1704067200000,
            1704067260000,
        ]
        assert instance.position == 0
        assert instance._in_position is False
        engine.process_signal.assert_not_called()
        engine._strategy_state_manager.transition_to_running.assert_called_once()

    def test_activate_strategy_fails_closed_when_warmup_replay_fails(self, engine):
        """Incomplete warm-up state must not transition a strategy to running."""
        class FailingWarmupStrategy:
            def __init__(self, strategy_id, product_id):
                from src.strategies.base import StrategyRequirements

                self.strategy_id = strategy_id
                self.product_id = product_id
                self._requirements = StrategyRequirements(product_id, "1m", 1)

            @property
            def requirements(self):
                return self._requirements

            def on_candle(self, candle):
                raise RuntimeError("warm-up replay failed")

        state = MagicMock()
        state.status = StrategyStatus.READY
        state.config_json = "{}"
        rows = [
            ORMCandlestick(
                product_id="BINANCE:BTCUSDT-PERP",
                timeframe="1m",
                timestamp=1704067200000,
                open=Decimal("41900"),
                high=Decimal("42100"),
                low=Decimal("41800"),
                close=Decimal("42000"),
                volume=Decimal("10"),
            ),
        ]
        mock_db = MagicMock()
        mock_db.query.side_effect = lambda model: self._FakeQuery(model, state, rows)
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine.loaded_classes["test.py::FailingWarmupStrategy"] = FailingWarmupStrategy
        engine._strategy_state_manager.transition_to_running = MagicMock()
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine.activate_strategy("test.py::FailingWarmupStrategy")

        assert "test.py::FailingWarmupStrategy" not in engine.strategy_instances
        engine._strategy_state_manager.transition_to_running.assert_not_called()
        engine._strategy_state_manager.transition_to_error.assert_called_once()
        assert "warm-up replay failed" in state.performance_json

    def test_activate_strategy_fails_closed_when_warmup_data_is_insufficient(self, engine):
        """Warm-up must have the declared lookback before activation."""
        class WarmupStrategy:
            def __init__(self, strategy_id, product_id):
                from src.strategies.base import StrategyRequirements

                self.strategy_id = strategy_id
                self.product_id = product_id
                self._requirements = StrategyRequirements(product_id, "1m", 2)

            @property
            def requirements(self):
                return self._requirements

            def on_candle(self, candle):
                return Signal(
                    strategy_id=self.strategy_id,
                    product_id=self.product_id,
                    timeframe="1m",
                    timestamp=candle.timestamp,
                    type=SignalType.NO_SIGNAL,
                )

        state = MagicMock()
        state.status = StrategyStatus.READY
        state.config_json = "{}"
        rows = [
            ORMCandlestick(
                product_id="BINANCE:BTCUSDT-PERP",
                timeframe="1m",
                timestamp=1704067200000,
                open=Decimal("41900"),
                high=Decimal("42100"),
                low=Decimal("41800"),
                close=Decimal("42000"),
                volume=Decimal("10"),
            )
        ]
        mock_db = MagicMock()
        mock_db.query.side_effect = lambda model: self._FakeQuery(model, state, rows)
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine.loaded_classes["test.py::WarmupStrategy"] = WarmupStrategy
        engine._strategy_state_manager.transition_to_running = MagicMock()
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine.activate_strategy("test.py::WarmupStrategy")

        assert "test.py::WarmupStrategy" not in engine.strategy_instances
        engine._strategy_state_manager.transition_to_running.assert_not_called()
        engine._strategy_state_manager.transition_to_error.assert_called_once()
        assert "warmup_insufficient_candles" in state.performance_json

    def test_warmup_syncs_strategy_trade_state_to_existing_position(self, engine):
        """A restarted strategy should reflect the real position after warm-up."""
        class StatefulStrategy:
            strategy_id = "test"
            product_id = "BINANCE:BTCUSDT-PERP"
            position = 0
            _in_position = False

        strategy = StatefulStrategy()
        engine.account_service.set_position(
            Position(
                strategy_id="test",
                product_id="BINANCE:BTCUSDT-PERP",
                side=PositionSide.LONG,
                quantity=Decimal("0.01"),
                entry_price=Decimal("42000"),
                unrealized_pnl=Decimal("0"),
            )
        )

        engine._sync_strategy_position_state(strategy)

        assert strategy.position == 1
        assert strategy._in_position is True

    def _make_warmup_db(self, state, rows):
        """Helper: return a mock DB factory that yields one candle row and the given state."""
        mock_db = MagicMock()
        mock_db.query.side_effect = lambda model: self._FakeQuery(model, state, rows)
        return lambda: nullcontext(mock_db)

    def test_activation_fails_closed_when_get_position_raises(self, engine):
        """get_position error during sync must land the strategy in ERROR, not RUNNING."""
        class MinimalStrategy:
            def __init__(self, strategy_id, product_id):
                from src.strategies.base import StrategyRequirements
                self.strategy_id = strategy_id
                self.product_id = product_id
                self._requirements = StrategyRequirements(product_id, "1m", 1)
                self.position = 0
                self._in_position = False

            @property
            def requirements(self):
                return self._requirements

            def on_candle(self, candle):
                return None

        state = MagicMock()
        state.status = StrategyStatus.READY
        state.config_json = "{}"
        rows = [
            ORMCandlestick(
                product_id="BINANCE:BTCUSDT-PERP",
                timeframe="1m",
                timestamp=1704067200000,
                open=Decimal("41900"),
                high=Decimal("42100"),
                low=Decimal("41800"),
                close=Decimal("42000"),
                volume=Decimal("10"),
            )
        ]
        engine._db_session_factory = self._make_warmup_db(state, rows)
        engine.account_service.get_position = MagicMock(
            side_effect=ConnectionError("db down")
        )
        engine.loaded_classes["test.py::MinimalStrategy"] = MinimalStrategy
        engine._strategy_state_manager.transition_to_running = MagicMock()
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine.activate_strategy("test.py::MinimalStrategy")

        assert "test.py::MinimalStrategy" not in engine.strategy_instances
        engine._strategy_state_manager.transition_to_running.assert_not_called()
        engine._strategy_state_manager.transition_to_error.assert_called_once()
        assert "position_state_sync_failed" in state.performance_json

    def test_activation_fails_closed_when_live_position_has_no_sync_hook(self, engine):
        """Live position + strategy with no sync attrs/hook must land in ERROR, not RUNNING."""
        class NoSyncStrategy:
            """A strategy with no _in_position, position, or sync_position_state."""
            def __init__(self, strategy_id, product_id):
                from src.strategies.base import StrategyRequirements
                self.strategy_id = strategy_id
                self.product_id = product_id
                self._requirements = StrategyRequirements(product_id, "1m", 1)

            @property
            def requirements(self):
                return self._requirements

            def on_candle(self, candle):
                return None

        state = MagicMock()
        state.status = StrategyStatus.READY
        state.config_json = "{}"
        rows = [
            ORMCandlestick(
                product_id="BINANCE:BTCUSDT-PERP",
                timeframe="1m",
                timestamp=1704067200000,
                open=Decimal("41900"),
                high=Decimal("42100"),
                low=Decimal("41800"),
                close=Decimal("42000"),
                volume=Decimal("10"),
            )
        ]
        engine._db_session_factory = self._make_warmup_db(state, rows)
        engine.account_service.set_position(
            Position(
                strategy_id="test.py::NoSyncStrategy",
                product_id="BINANCE:BTCUSDT-PERP",
                side=PositionSide.LONG,
                quantity=Decimal("0.01"),
                entry_price=Decimal("42000"),
                unrealized_pnl=Decimal("0"),
            )
        )
        engine.loaded_classes["test.py::NoSyncStrategy"] = NoSyncStrategy
        engine._strategy_state_manager.transition_to_running = MagicMock()
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine.activate_strategy("test.py::NoSyncStrategy")

        assert "test.py::NoSyncStrategy" not in engine.strategy_instances
        engine._strategy_state_manager.transition_to_running.assert_not_called()
        engine._strategy_state_manager.transition_to_error.assert_called_once()
        assert "position_state_sync_unsupported" in state.performance_json

    def test_zero_lookback_strategy_still_syncs_position_state(self, engine):
        """lookback_window == 0 skips candle warm-up but must not skip position sync."""
        class ZeroLookbackNoSyncStrategy:
            """No warm-up needed, and no sync hook/attrs — live position must fail closed."""
            def __init__(self, strategy_id, product_id):
                from src.strategies.base import StrategyRequirements
                self.strategy_id = strategy_id
                self.product_id = product_id
                self._requirements = StrategyRequirements(product_id, "1m", 0)

            @property
            def requirements(self):
                return self._requirements

            def on_candle(self, candle):
                return None

        state = MagicMock()
        state.status = StrategyStatus.READY
        state.config_json = "{}"
        engine._db_session_factory = self._make_warmup_db(state, [])
        engine.account_service.set_position(
            Position(
                strategy_id="test.py::ZeroLookbackNoSyncStrategy",
                product_id="BINANCE:BTCUSDT-PERP",
                side=PositionSide.LONG,
                quantity=Decimal("0.01"),
                entry_price=Decimal("42000"),
                unrealized_pnl=Decimal("0"),
            )
        )
        engine.loaded_classes["test.py::ZeroLookbackNoSyncStrategy"] = ZeroLookbackNoSyncStrategy
        engine._strategy_state_manager.transition_to_running = MagicMock()
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine.activate_strategy("test.py::ZeroLookbackNoSyncStrategy")

        assert "test.py::ZeroLookbackNoSyncStrategy" not in engine.strategy_instances
        engine._strategy_state_manager.transition_to_running.assert_not_called()
        engine._strategy_state_manager.transition_to_error.assert_called_once()
        assert "position_state_sync_unsupported" in state.performance_json

    # test_activate_strategy_replays_recent_candles_without_orders (line ~1011) already
    # covers the flat/no-position happy path: account_service returns None, position_side
    # is None, set_position_state returns True → activation succeeds.  No duplicate needed.

    def test_warm_up_precedes_register_during_activation(self, engine):
        """warm-up must complete before the instance is registered as live.

        Rationale: on restart-restore the lifecycle cache is already ACTIVE, so a
        registered instance is immediately visible to on_market_data.  Registering
        before warm-up is complete would allow signals to be emitted from partial state.
        """
        call_order: list[str] = []

        class OrderTrackingStrategy:
            def __init__(self, strategy_id, product_id):
                from src.strategies.base import StrategyRequirements

                self.strategy_id = strategy_id
                self.product_id = product_id
                self._requirements = StrategyRequirements(product_id, "1m", 1)
                self.position = 0
                self._in_position = False

            @property
            def requirements(self):
                return self._requirements

            def on_candle(self, candle):
                return None

        state = MagicMock()
        state.status = StrategyStatus.READY
        state.config_json = "{}"
        rows = [
            ORMCandlestick(
                product_id="BINANCE:BTCUSDT-PERP",
                timeframe="1m",
                timestamp=1704067200000,
                open=Decimal("41900"),
                high=Decimal("42100"),
                low=Decimal("41800"),
                close=Decimal("42000"),
                volume=Decimal("10"),
            )
        ]
        mock_db = MagicMock()
        mock_db.query.side_effect = lambda model: self._FakeQuery(model, state, rows)
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine.loaded_classes["test.py::OrderTrackingStrategy"] = OrderTrackingStrategy
        engine._strategy_state_manager.transition_to_running = MagicMock()

        original_warmup = engine._warm_up_strategy_instance
        original_register = engine._register_strategy_instance

        def tracking_warmup(db, instance):
            call_order.append("warm_up")
            return original_warmup(db, instance)

        def tracking_register(instance):
            call_order.append("register")
            return original_register(instance)

        engine._warm_up_strategy_instance = tracking_warmup
        engine._register_strategy_instance = tracking_register

        engine.activate_strategy("test.py::OrderTrackingStrategy")

        assert call_order == ["warm_up", "register"], (
            f"Expected warm_up before register, got: {call_order}"
        )
        assert "test.py::OrderTrackingStrategy" in engine.strategy_instances
        engine._strategy_state_manager.transition_to_running.assert_called_once()


# =============================================================================
# Restore active strategies — matrix tests
# =============================================================================


class TestRestoreActiveStrategiesMatrix:
    """Decision-table tests for _restore_active_strategies_on_startup."""

    def _make_state(self, strategy_id: str):
        s = MagicMock()
        s.strategy_id = strategy_id
        return s

    def _db_ctx(self, states):
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.all.return_value = states
        return lambda: nullcontext(mock_db)

    def test_class_missing_transitions_to_error_not_activated(self, engine):
        """Missing class → transition_to_error called, strategy NOT activated (P2 cell)."""
        state = self._make_state("test.py::MissingStrategy")
        engine._db_session_factory = self._db_ctx([state])
        engine.activate_strategy = MagicMock()
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine._restore_active_strategies_on_startup()

        engine._strategy_state_manager.transition_to_error.assert_called_once_with(
            "test.py::MissingStrategy",
            "startup_restore_class_missing",
            actor="system",
        )
        engine.activate_strategy.assert_not_called()
        assert "test.py::MissingStrategy" not in engine.strategy_instances

    def test_per_strategy_isolation_first_raises_second_succeeds(self, engine):
        """activate_strategy raising for A must not prevent B from restoring (isolation cell)."""
        state_a = self._make_state("test.py::StratA")
        state_b = self._make_state("test.py::StratB")
        engine._db_session_factory = self._db_ctx([state_a, state_b])

        # Both classes are loaded; A's activation raises, B's does not.
        engine.loaded_classes["test.py::StratA"] = MagicMock()
        engine.loaded_classes["test.py::StratB"] = MagicMock()

        activate_calls = []

        def _activate(strategy_id, *, actor, reason, force):
            activate_calls.append(strategy_id)
            if strategy_id == "test.py::StratA":
                raise RuntimeError("StratA boom")

        engine.activate_strategy = _activate
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine._restore_active_strategies_on_startup()

        # A → transitioned to error
        engine._strategy_state_manager.transition_to_error.assert_called_once()
        error_args = engine._strategy_state_manager.transition_to_error.call_args
        assert error_args[0][0] == "test.py::StratA"
        assert "startup_restore_failed" in error_args[0][1]

        # B → activate_strategy was reached
        assert "test.py::StratB" in activate_calls


class TestPersistentKillSwitchState:
    def test_db_lockdown_overrides_stale_redis_ok(self, engine):
        engine.redis_client.get.return_value = "OK"
        engine.ops_safety.latest_kill_switch_state = MagicMock(
            return_value="LOCKDOWN"
        )

        locked = engine._check_system_state()

        assert locked is True
        assert engine._kill_switch_halted is True

    def test_db_redis_state_disagreement_fails_closed(self, engine):
        engine.redis_client.get.return_value = "LOCKDOWN"
        engine.ops_safety.latest_kill_switch_state = MagicMock(return_value="OK")

        assert engine._check_system_state() is True

    def test_matching_db_and_redis_clear_state_resumes(self, engine):
        previous_boot = {"state": "CLEAN", "boot_id": "previous-boot"}
        engine.redis_client.get.side_effect = lambda key: (
            "OK" if key == "system:state" else json.dumps(previous_boot)
        )
        engine.ops_safety.latest_kill_switch_state = MagicMock(return_value="OK")
        engine.ops_safety.latest_engine_boot_state = MagicMock(
            return_value=previous_boot
        )
        engine.ops_safety.persist_engine_boot_state = MagicMock()

        assert engine._check_system_state() is False
        assert engine._kill_switch_halted is False
        engine.ops_safety.persist_engine_boot_state.assert_called_once_with(
            "UNCLEAN",
            boot_id=engine._boot_id,
        )
        redis_boot = json.loads(
            next(
                call.args[1]
                for call in engine.redis_client.set.call_args_list
                if call.args[0] == SYSTEM_BOOT_STATE_KEY
            )
        )
        assert redis_boot == {"state": "UNCLEAN", "boot_id": engine._boot_id}

    @pytest.mark.parametrize(
        "db_boot, redis_boot",
        [
            (None, None),
            (
                {"state": "UNCLEAN", "boot_id": "previous"},
                {"state": "UNCLEAN", "boot_id": "previous"},
            ),
            (
                {"state": "CLEAN", "boot_id": "db-boot"},
                {"state": "CLEAN", "boot_id": "redis-boot"},
            ),
        ],
        ids=["missing", "unclean", "disagree"],
    )
    def test_untrusted_previous_boot_fails_closed(
        self, engine, db_boot, redis_boot
    ):
        engine.redis_client.get.side_effect = lambda key: (
            "OK"
            if key == "system:state"
            else json.dumps(redis_boot) if redis_boot is not None else None
        )
        engine.ops_safety.latest_kill_switch_state = MagicMock(return_value="OK")
        engine.ops_safety.latest_engine_boot_state = MagicMock(return_value=db_boot)
        engine.ops_safety.persist_engine_boot_state = MagicMock()

        assert engine._check_system_state() is True
        assert engine._kill_switch_halted is True

    def test_current_boot_marker_dual_write_failure_fails_closed(self, engine):
        previous_boot = {"state": "CLEAN", "boot_id": "previous"}
        engine.redis_client.get.side_effect = lambda key: (
            "OK" if key == "system:state" else json.dumps(previous_boot)
        )
        engine.redis_client.set.side_effect = RuntimeError("redis unavailable")
        engine.ops_safety.latest_kill_switch_state = MagicMock(return_value="OK")
        engine.ops_safety.latest_engine_boot_state = MagicMock(
            return_value=previous_boot
        )
        engine.ops_safety.persist_engine_boot_state = MagicMock(
            side_effect=RuntimeError("database unavailable")
        )

        assert engine._check_system_state() is True
        assert engine._kill_switch_halted is True

    def test_startup_opens_command_listener_before_waiting_on_persisted_state(
        self, engine
    ):
        calls = []
        engine._start_command_listener = MagicMock(
            side_effect=lambda: calls.append("listener")
        )
        engine._check_system_state = MagicMock(
            side_effect=lambda: calls.append("state")
        )
        for name in (
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_reconcile_recoverable_orders_on_startup",
            "_start_heartbeat",
            "scan_strategies",
            "_restore_active_strategies_on_startup",
        ):
            setattr(engine, name, MagicMock())

        engine.startup()

        assert calls == ["listener", "state"]

    def test_persisted_lockdown_keeps_both_submission_gates_halted(self, engine):
        engine.redis_client.get.return_value = "LOCKDOWN"

        locked = engine._check_system_state()

        assert locked is True
        assert engine._kill_switch_halted is True
        assert engine.execution_engine._submissions_halted is True

    def test_startup_recovers_lockdown_without_restoring_strategies(self, engine):
        engine.redis_client.get.return_value = "LOCKDOWN"
        engine.ops_safety.kill_switch = MagicMock(return_value={})
        engine._reconcile_balance = MagicMock()
        engine._initialize_strategy_state_cache_on_startup = MagicMock()
        engine._start_strategy_state_subscriber_on_startup = MagicMock()
        engine._reconcile_recoverable_orders_on_startup = MagicMock()
        engine._start_heartbeat = MagicMock()
        engine._start_command_listener = MagicMock()
        engine.scan_strategies = MagicMock()
        engine._restore_active_strategies_on_startup = MagicMock()

        engine.startup()

        engine.ops_safety.kill_switch.assert_called_once_with(
            actor="startup_recovery",
            reason="persisted_lockdown",
        )
        engine._restore_active_strategies_on_startup.assert_not_called()

    def test_startup_does_not_recover_after_concurrent_manual_clear(self, engine):
        def cleared_after_read():
            engine._kill_switch_halted = False
            return True

        engine._check_system_state = MagicMock(side_effect=cleared_after_read)
        engine.ops_safety.kill_switch = MagicMock(return_value={})
        for name in (
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_reconcile_recoverable_orders_on_startup",
            "_start_heartbeat",
            "_start_command_listener",
            "scan_strategies",
            "_restore_active_strategies_on_startup",
        ):
            setattr(engine, name, MagicMock())

        engine.startup()

        engine.ops_safety.kill_switch.assert_not_called()


class TestRuntimeReconciliationThread:
    def test_startup_skips_runtime_reconciliation_for_simulated_mode(self, engine):
        """Runtime reconciliation is live-only; simulated runs must not emit false drift."""
        startup_steps = [
            "_check_system_state",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_reconcile_recoverable_orders_on_startup",
            "_start_heartbeat",
            "_start_command_listener",
            "scan_strategies",
            "_restore_active_strategies_on_startup",
        ]
        for name in startup_steps:
            setattr(engine, name, MagicMock())
        engine._start_runtime_reconciliation = MagicMock()

        engine.startup()

        engine._start_runtime_reconciliation.assert_not_called()

    def test_startup_starts_runtime_reconciliation_for_live_mode(
        self, mock_db_session, mock_clock
    ):
        """Live runs should start periodic runtime reconciliation."""
        with patch("src.core.engine.create_redis_client") as mock_factory, patch(
            "src.core.engine.create_adapter"
        ) as mock_create:
            mock_factory.return_value = MagicMock()
            mock_create.return_value = MagicMock()
            engine = StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter_config={"mode": "live"},
            )

        startup_steps = [
            "_check_system_state",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_reconcile_recoverable_orders_on_startup",
            "_start_heartbeat",
            "_start_command_listener",
            "scan_strategies",
            "_restore_active_strategies_on_startup",
        ]
        for name in startup_steps:
            setattr(engine, name, MagicMock())
        engine._start_runtime_reconciliation = MagicMock()

        engine.startup()

        engine._start_runtime_reconciliation.assert_called_once()

    def test_start_runtime_reconciliation_runs_job_in_daemon_thread(self, engine):
        """Runtime reconciliation should run in a daemon background loop."""
        engine.runtime_reconciliation_job = MagicMock()
        engine._runtime_reconcile_stop = MagicMock()
        engine._runtime_reconcile_stop.is_set.return_value = False
        engine._runtime_reconcile_stop.wait.return_value = True
        created_threads = []

        class ImmediateThread:
            def __init__(self, *, target, daemon):
                self.target = target
                self.daemon = daemon
                created_threads.append(self)

            def start(self):
                self.target()

        with patch("src.core.engine.threading.Thread", ImmediateThread), patch(
            "src.core.engine.time.sleep",
            side_effect=AssertionError("runtime reconciliation sleep must be interruptible"),
        ):
            engine._start_runtime_reconciliation()

        assert created_threads[0].daemon is True
        engine.runtime_reconciliation_job.run_once.assert_called_once()
        engine._runtime_reconcile_stop.wait.assert_called_once_with(3600.0)


# =============================================================================
# shutdown
# =============================================================================


class TestShutdown:

    def test_stops_and_joins_runtime_reconciliation_before_redis_close(self, engine):
        engine._runtime_reconcile_stop = MagicMock()
        engine.runtime_reconcile_thread = MagicMock()
        engine.runtime_reconcile_thread.is_alive.return_value = True

        engine.shutdown(timeout=0.1)

        engine._runtime_reconcile_stop.set.assert_called_once()
        engine.runtime_reconcile_thread.join.assert_called_once_with(timeout=0.1)
        engine.redis_client.close.assert_called_once()

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
