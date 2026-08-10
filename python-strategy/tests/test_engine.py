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
import logging
import threading
from types import SimpleNamespace
from unittest.mock import MagicMock, call, patch

import pytest

from src.core.models import (
    Candlestick,
    OrderSide,
    OrderStatus,
    Position,
    PositionSide,
    Signal,
    SignalType,
    StrategyStatus,
    Trade,
)
from src.core.orm_models import Candlestick as ORMCandlestick, StrategyState
from src.core.daily_nav_snapshot import DailyNavSnapshotService
from src.core.adapters.ccxt_adapter import CcxtExchangeAdapter
from src.core.adapters.rithmic_adapter import (
    RithmicExchangeAdapter,
    RithmicUnmappedOrderEvent,
)
from src.core.adapters.rithmic_ledger_recovery import (
    RithmicLedgerRecoveryService,
)
from src.core.adapters.rithmic_kill_switch_clear import (
    RithmicKillSwitchClearPreparationService,
)
from src.core.adapters.rithmic_order_reconnect import (
    RithmicOrderReconnectService,
)
from src.core.adapters.rithmic_order_event_lifecycle import (
    RithmicOrderEventLifecycleGate,
)
from src.core.adapters.rithmic_order_event_stream import (
    RithmicOrderEventStreamService,
)
from src.core.adapters.rithmic_runtime_composition import (
    RithmicRuntimeOwners,
    build_rithmic_portfolio_exit_owner,
)
from src.core.adapters.rithmic_runtime_recovery import (
    RithmicRuntimeRecoveryService,
)
from src.core.adapters.rithmic_emergency_flatten import (
    RithmicEmergencyFlattenService,
)
from src.core.adapters.rithmic_external_order_drift import (
    RithmicExternalOrderDriftService,
)
from src.core.adapters.rithmic_strategy_exit import RithmicStrategyExitService
from src.core.adapters.simulated import SimulatedAdapter
from src.core.product_registry import InstrumentSpec
from src.core.portfolio_runtime import (
    PortfolioDecisionRejected,
    PortfolioDefinition,
    PortfolioExclusiveSlot,
    PortfolioExposureSnapshot,
    PortfolioFactory,
    PortfolioSleeve,
)
from src.core.engine import (
    SYSTEM_STATE_OK,
    StrategyEngine,
    _is_runtime_reconciliation_enabled,
    _kill_switch_result_is_complete,
)
from src.core.execution import ExitDecision
from src.core.interfaces.exchange import (
    ExchangeError,
    ExchangeOrderEvent,
    ExchangeOrderSnapshot,
    NetworkError,
)
from src.core.strategy_state_manager import (
    InvalidStrategyStateTransition,
    StaleStrategyStateVersion,
    StrategyStateManager,
)
from src.core.runtime_environment import RuntimeEnvironment
from src.core.rithmic_publisher_liveness_gate import (
    RithmicPublisherLivenessGate,
)


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


def _authoritative_rithmic_summary(
    *,
    account_id="ACCOUNT",
    balance="50000.25",
):
    return {
        "recoverable_count": 0,
        "auto_resume_safe": True,
        "ledger_verification": {
            "account_id": account_id,
            "account_currency": "USD",
            "account_summary": {
                "account_balance": balance,
                "cash_on_hand": balance,
                "available_buying_power": balance,
                "day_pnl": "0",
                "net_quantity": "0",
                "timestamp_ms": 1704067200000,
            },
            "position_drifts": [],
            "errors": [],
            "verification_blocked": False,
        },
    }


def _attach_rithmic_ledger_recovery(engine) -> None:
    engine._rithmic_runtime.ledger_recovery = RithmicLedgerRecoveryService(
        profile=engine._rithmic_recovery_profile or "test",
        account_id=engine._rithmic_recovery_account_id,
        reconcile_owned_orders=lambda profile, account_id: (
            engine.execution_engine.reconcile_rithmic_owned_orders(
                profile,
                account_id,
            )
        ),
        now_seconds=lambda: engine.execution_engine.clock.now(),
        publish_authoritative_balance=lambda **values: (
            engine.account_service.replace_authoritative_balance(**values)
        ),
        logger=logging.getLogger("src.core.engine"),
    )


# =============================================================================
# Initialization
# =============================================================================


class TestEngineInit:
    def test_engine_delegates_rithmic_owner_graph_construction_once(
        self,
        engine_factory,
    ):
        lifecycle = RithmicOrderEventLifecycleGate()
        owners = RithmicRuntimeOwners(order_event_lifecycle=lifecycle)

        with patch(
            "src.core.engine.build_rithmic_runtime_owners",
            return_value=owners,
        ) as build:
            engine = engine_factory()

        build.assert_called_once()
        build_kwargs = build.call_args.kwargs
        assert build_kwargs["adapter"] is engine.execution_engine.adapter
        assert build_kwargs["execution_engine"] is engine.execution_engine
        assert build_kwargs["account_service"] is engine.account_service
        assert build_kwargs["ops_safety"] is engine.ops_safety
        assert build_kwargs["stop_event"] is engine._order_event_stop
        callbacks = build_kwargs["callbacks"]
        replacement_stop = MagicMock(return_value=True)
        replacement_start = MagicMock()
        engine._stop_exchange_order_event_stream = replacement_stop
        engine._start_exchange_order_event_stream = replacement_start
        assert callbacks.stop_order_event_stream(timeout=30.0)
        callbacks.start_order_event_stream()
        replacement_stop.assert_called_once_with(timeout=30.0)
        replacement_start.assert_called_once_with()
        assert engine._rithmic_runtime is owners
        assert not {
            "_rithmic_ledger_recovery",
            "_rithmic_order_reconnect",
            "_rithmic_runtime_recovery",
            "_rithmic_order_event_lifecycle",
            "_rithmic_external_order_drift",
            "_rithmic_strategy_exit",
            "_rithmic_order_event_stream",
            "_rithmic_kill_switch_clear_preparation",
            "_rithmic_emergency_flatten",
            "_rithmic_portfolio_exit_factory",
        }.intersection(vars(engine))
        assert engine._rithmic_runtime.order_event_lifecycle is lifecycle
        assert engine._rithmic_runtime.ledger_recovery is None
        assert engine._rithmic_runtime.order_reconnect is None
        assert engine._rithmic_runtime.runtime_recovery is None
        assert engine._rithmic_runtime.external_order_drift is None
        assert engine._rithmic_runtime.strategy_exit is None
        assert engine._rithmic_runtime.order_event_stream is None
        assert engine._rithmic_runtime.kill_switch_clear_preparation is None
        assert engine._rithmic_runtime.emergency_flatten is None
        assert engine._rithmic_runtime.portfolio_exit_factory is None

    def test_rithmic_engine_constructs_order_reconnect_owner(self, engine_factory):
        adapter = _rithmic_adapter_for_reconnect_test()

        engine = engine_factory(adapter=adapter, audit_external_orders=True)

        assert isinstance(
            engine._rithmic_runtime.order_reconnect,
            RithmicOrderReconnectService,
        )

    def test_rithmic_engine_constructs_runtime_recovery_owner(self, engine_factory):
        adapter = _rithmic_adapter_for_reconnect_test()

        engine = engine_factory(adapter=adapter, audit_external_orders=True)

        assert isinstance(
            engine._rithmic_runtime.runtime_recovery,
            RithmicRuntimeRecoveryService,
        )

    def test_rithmic_engine_constructs_strategy_exit_owner(self, engine_factory):
        adapter = _rithmic_adapter_for_reconnect_test()

        engine = engine_factory(adapter=adapter, audit_external_orders=True)

        assert isinstance(
            engine._rithmic_runtime.strategy_exit,
            RithmicStrategyExitService,
        )

    def test_rithmic_engine_constructs_emergency_flatten_owner(self, engine_factory):
        adapter = _rithmic_adapter_for_reconnect_test()

        engine = engine_factory(adapter=adapter, audit_external_orders=True)

        assert isinstance(
            engine._rithmic_runtime.emergency_flatten,
            RithmicEmergencyFlattenService,
        )

    def test_rithmic_engine_constructs_kill_switch_clear_preparation_owner(
        self,
        engine_factory,
    ):
        adapter = _rithmic_adapter_for_reconnect_test()

        engine = engine_factory(adapter=adapter, audit_external_orders=True)

        owner = engine._rithmic_runtime.kill_switch_clear_preparation
        assert isinstance(owner, RithmicKillSwitchClearPreparationService)
        assert owner._operation_gate is engine._rithmic_runtime.order_event_lifecycle

        replacement_worker = MagicMock()
        engine.order_event_thread = replacement_worker
        assert owner._current_order_event_thread() is replacement_worker

        engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
        assert owner._halt_for_reconcile(timeout=30.0) is True
        engine.execution_engine.halt_for_reconcile.assert_called_once_with(timeout=30.0)

        assert engine._rithmic_runtime.external_order_drift is not None
        engine._rithmic_runtime.external_order_drift.current_generation = MagicMock(
            return_value=11
        )
        assert owner._current_drift_generation() == 11

        summary = {"auto_resume_safe": True}
        assert engine._rithmic_runtime.ledger_recovery is not None
        engine._rithmic_runtime.ledger_recovery.publish_authoritative_summary = (
            MagicMock()
        )
        owner._publish_authoritative_summary(summary)
        engine._rithmic_runtime.ledger_recovery.publish_authoritative_summary.assert_called_once_with(
            summary
        )

    def test_rithmic_engine_constructs_external_order_drift_owner(
        self,
        engine_factory,
    ):
        adapter = _rithmic_adapter_for_reconnect_test()

        engine = engine_factory(adapter=adapter, audit_external_orders=True)

        assert isinstance(
            engine._rithmic_runtime.external_order_drift,
            RithmicExternalOrderDriftService,
        )

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
        with (
            patch("src.core.engine.create_redis_client") as redis_factory,
            patch("src.core.engine.create_adapter") as create_adapter,
        ):
            redis_factory.return_value = MagicMock()
            create_adapter.return_value = MagicMock()

            with pytest.raises(
                ValueError,
                match="RUNTIME_RECONCILE_INTERVAL_SECONDS",
            ):
                StrategyEngine(
                    db_session=mock_db_session,
                    clock=mock_clock,
                    adapter_config={
                        "mode": "live",
                        "instrument_product_ids": ["BINANCE:BTCUSDT-PERP"],
                    },
                )

    def test_default_adapter_simulated(self, mock_db_session, mock_clock):
        """When no adapter_config, should default to simulated mode."""
        with (
            patch("src.core.engine.create_redis_client") as mock_factory,
            patch("src.core.engine.create_adapter") as mock_create,
        ):
            mock_factory.return_value = MagicMock()
            mock_adapter = MagicMock()
            mock_create.return_value = mock_adapter

            StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
            )

            assert mock_create.call_args.args == ({"mode": "simulated"},)
            assert callable(mock_create.call_args.kwargs["operation_guard"])

    def test_adapter_config_passed_through(self, mock_db_session, mock_clock):
        """Custom adapter_config should be forwarded to create_adapter."""
        with (
            patch("src.core.engine.create_redis_client") as mock_factory,
            patch("src.core.engine.create_adapter") as mock_create,
        ):
            mock_factory.return_value = MagicMock()
            mock_create.return_value = MagicMock()

            cfg = {
                "mode": "live",
                "exchange": "bybit",
                "instrument_product_ids": ["BYBIT:BTCUSDT-PERP"],
            }
            StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter_config=cfg,
            )

            assert mock_create.call_args.args == (cfg,)
            assert callable(mock_create.call_args.kwargs["operation_guard"])

    def test_runtime_reconciliation_uses_configured_product_universe(
        self, mock_db_session, mock_clock
    ):
        """Runtime reconciliation must scan configured products even when local is flat."""
        with (
            patch("src.core.engine.create_redis_client") as mock_factory,
            patch("src.core.engine.create_adapter") as mock_create,
        ):
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
        with (
            patch("src.core.engine.create_redis_client") as mock_factory,
            patch("src.core.engine.create_adapter") as mock_create,
        ):
            mock_factory.return_value = MagicMock()
            mock_create.side_effect = RuntimeError("boom")

            with pytest.raises(RuntimeError, match="boom"):
                StrategyEngine(
                    db_session=mock_db_session,
                    clock=mock_clock,
                    adapter_config={
                        "mode": "live",
                        "instrument_product_ids": ["BINANCE:BTCUSDT-PERP"],
                    },
                )

    def test_provided_adapter_used_directly(self, mock_db_session, mock_clock):
        """Pre-created adapter should be used without calling create_adapter."""
        with (
            patch("src.core.engine.create_redis_client") as mock_factory,
            patch("src.core.engine.create_adapter") as mock_create,
        ):
            mock_factory.return_value = MagicMock()
            mock_adapter = MagicMock()

            StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter=mock_adapter,
            )

            mock_create.assert_not_called()

    def test_db_session_factory_passed_to_execution_engine(
        self, mock_db_session, mock_clock
    ):
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

    def test_audit_external_orders_passed_to_execution_engine(
        self, mock_db_session, mock_clock
    ):
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
        assert (
            engine.risk_manager.daily_nav_service is engine._daily_nav_snapshot_service
        )

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

    def test_startup_reconcile_skipped_when_audit_external_orders_disabled(
        self, engine
    ):
        """Startup order reconciliation should only run for audited external orders."""
        engine.execution_engine.reconcile_recoverable_client_orders = MagicMock()

        engine._reconcile_recoverable_orders_on_startup()

        engine.execution_engine.reconcile_recoverable_client_orders.assert_not_called()

    def test_startup_reconcile_runs_when_audit_external_orders_enabled(
        self, mock_db_session, mock_clock
    ):
        """Audited external order mode should reconcile recoverable orders on startup."""
        with patch("src.core.engine.create_redis_client") as mock_factory:
            mock_factory.return_value = MagicMock()
            engine = StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter=MagicMock(),
                audit_external_orders=True,
                account_service=MagicMock(),
            )
        engine.execution_engine.reconcile_recoverable_client_orders = MagicMock(
            return_value={"recoverable_count": 2}
        )

        engine._reconcile_recoverable_orders_on_startup()

        engine.execution_engine.reconcile_recoverable_client_orders.assert_called_once_with()
        assert engine._rithmic_runtime.ledger_recovery is None

    def test_startup_reconcile_uses_rithmic_owned_recovery_when_configured(
        self,
        engine_factory,
    ):
        engine = engine_factory(
            audit_external_orders=True,
            adapter_config={
                "mode": "live",
                "instrument_product_ids": ["RITHMIC:NQ-202609"],
                "rithmic_recovery_profile": "test",
                "rithmic_recovery_account_id": "ACCOUNT",
            },
        )
        summary = _authoritative_rithmic_summary()
        engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
            return_value=summary
        )
        engine.execution_engine.reconcile_recoverable_client_orders = MagicMock()
        engine.execution_engine.clock.now = MagicMock(return_value=1704067201)
        engine.account_service.replace_authoritative_balance = MagicMock()

        result = engine._reconcile_recoverable_orders_on_startup()

        assert isinstance(
            engine._rithmic_runtime.ledger_recovery,
            RithmicLedgerRecoveryService,
        )
        assert result is summary
        engine.execution_engine.reconcile_rithmic_owned_orders.assert_called_once_with(
            "test",
            "ACCOUNT",
        )
        engine.execution_engine.reconcile_recoverable_client_orders.assert_not_called()
        engine.account_service.replace_authoritative_balance.assert_called_once_with(
            venue="rithmic",
            account_id="ACCOUNT",
            currency="USD",
            balance=Decimal("50000.25"),
            day_pnl=Decimal("0"),
            observed_at_ms=1704067201000,
            source_timestamp_ms=1704067200000,
        )

    def test_startup_rithmic_reconciliation_delegates_only_to_venue_owner(
        self,
        engine,
    ):
        result = object()
        service = MagicMock()
        service.reconcile_startup.return_value = result
        engine.execution_engine.audit_external_orders = True
        engine._rithmic_runtime.ledger_recovery = service
        engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock()
        engine.execution_engine.reconcile_recoverable_client_orders = MagicMock()

        actual = engine._reconcile_recoverable_orders_on_startup()

        assert actual is result
        service.reconcile_startup.assert_called_once_with()
        engine.execution_engine.reconcile_rithmic_owned_orders.assert_not_called()
        engine.execution_engine.reconcile_recoverable_client_orders.assert_not_called()

    def test_startup_rithmic_owner_none_result_never_falls_through(self, engine):
        service = MagicMock()
        service.reconcile_startup.return_value = None
        engine.execution_engine.audit_external_orders = True
        engine._rithmic_runtime.ledger_recovery = service
        engine.execution_engine.reconcile_recoverable_client_orders = MagicMock()

        assert engine._reconcile_recoverable_orders_on_startup() is None

        service.reconcile_startup.assert_called_once_with()
        engine.execution_engine.reconcile_recoverable_client_orders.assert_not_called()

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

    def test_registers_strategy_as_active_in_state_cache(
        self, engine, strategy_instance
    ):
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

    def test_portfolio_parent_cannot_reuse_standalone_strategy_id(
        self,
        engine,
        mock_strategy_class,
    ):
        engine.add_strategy(mock_strategy_class("portfolio_v1", "BINANCE:BTCUSDT-PERP"))
        definition = PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id="BINANCE:BTCUSDT-PERP",
            sleeves=(
                PortfolioSleeve(
                    mock_strategy_class(
                        "portfolio_v1.sleeve",
                        "BINANCE:BTCUSDT-PERP",
                    )
                ),
            ),
            max_gross_quantity=Decimal("1"),
        )

        with pytest.raises(ValueError, match="already active"):
            engine.add_portfolio(definition)

    def test_portfolio_sleeve_cannot_reuse_existing_parent_id(
        self,
        engine,
        mock_strategy_class,
    ):
        first = PortfolioDefinition(
            portfolio_id="first",
            product_id="BINANCE:BTCUSDT-PERP",
            sleeves=(
                PortfolioSleeve(
                    mock_strategy_class(
                        "first.sleeve",
                        "BINANCE:BTCUSDT-PERP",
                    )
                ),
            ),
            max_gross_quantity=Decimal("1"),
        )
        second = PortfolioDefinition(
            portfolio_id="second",
            product_id="BINANCE:BTCUSDT-PERP",
            sleeves=(
                PortfolioSleeve(
                    mock_strategy_class(
                        "first",
                        "BINANCE:BTCUSDT-PERP",
                    )
                ),
            ),
            max_gross_quantity=Decimal("1"),
        )
        engine.add_portfolio(first)

        with pytest.raises(ValueError, match="already active"):
            engine.add_portfolio(second)

    def test_standalone_strategy_cannot_reuse_portfolio_parent_id(
        self,
        engine,
        mock_strategy_class,
    ):
        definition = PortfolioDefinition(
            portfolio_id="portfolio_v1",
            product_id="BINANCE:BTCUSDT-PERP",
            sleeves=(
                PortfolioSleeve(
                    mock_strategy_class(
                        "portfolio_v1.sleeve",
                        "BINANCE:BTCUSDT-PERP",
                    )
                ),
            ),
            max_gross_quantity=Decimal("1"),
        )
        engine.add_portfolio(definition)

        with pytest.raises(ValueError, match="already active"):
            engine.add_strategy(
                mock_strategy_class(
                    "portfolio_v1",
                    "BINANCE:BTCUSDT-PERP",
                )
            )


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

    def test_research_only_product_cannot_enter_live_stream_path(
        self,
        engine,
        mock_strategy_class,
    ):
        engine.add_strategy(mock_strategy_class("research", "RITHMIC:MNQ-CONTINUOUS"))

        with pytest.raises(ValueError, match="live stream mapping is unavailable"):
            engine.build_stream_channels()


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

    def test_strategy_exception_propagates_to_market_delivery_gate(
        self,
        engine,
        strategy_instance,
    ):
        """A failed strategy must prevent the market delivery from being ACKed."""
        engine.add_strategy(strategy_instance)
        strategy_instance.on_candle = MagicMock(side_effect=RuntimeError("boom"))

        candle = _make_candle()
        with pytest.raises(RuntimeError, match="boom"):
            engine.on_market_data(candle)

    def test_trade_strategy_exception_propagates_to_market_delivery_gate(
        self,
        engine,
        strategy_instance,
    ):
        engine.add_strategy(strategy_instance)
        strategy_instance.on_trade = MagicMock(side_effect=RuntimeError("boom"))
        trade = Trade(
            id="trade-1",
            product_id="BINANCE:BTCUSDT-PERP",
            price=Decimal("42000"),
            quantity=Decimal("1"),
            side="buy",
            timestamp=1704067200000,
        )

        with pytest.raises(RuntimeError, match="boom"):
            engine.on_market_data(trade)

    def test_no_strategies_for_product(self, engine):
        """Candle for unregistered product should not error."""
        candle = _make_candle(product_id="BINANCE:XYZUSDT-PERP")
        engine.execution_engine.process_market_data = MagicMock()

        # Should not raise
        engine.on_market_data(candle)

    def test_split_backtest_routes_fill_before_completed_decision(self, engine):
        engine.runtime_environment = RuntimeEnvironment("test")
        execution_candle = _make_candle(timeframe="1m")
        decision_candle = _make_candle(timeframe="5m")
        calls = MagicMock()
        engine.execution_engine.process_market_data = calls.process_market_data
        engine._signal_processor.on_candle = calls.on_candle

        engine.on_backtest_market_data(execution_candle, decision_candle)

        assert calls.mock_calls == [
            call.process_market_data(execution_candle),
            call.on_candle(decision_candle),
        ]

    def test_split_backtest_route_is_rejected_in_live_runtime(self, engine):
        engine.runtime_environment = RuntimeEnvironment("live")
        execution_candle = _make_candle(timeframe="1m")
        engine.execution_engine.process_market_data = MagicMock()

        with pytest.raises(RuntimeError, match="backtest-only"):
            engine.on_backtest_market_data(execution_candle)

        engine.execution_engine.process_market_data.assert_not_called()

    def test_async_backtest_decision_does_not_reprocess_execution_candle(
        self,
        engine,
    ):
        engine.runtime_environment = RuntimeEnvironment("test")
        decision_candle = _make_candle(timeframe="5m")
        engine.execution_engine.process_market_data = MagicMock()
        engine._signal_processor.on_candle = MagicMock()

        engine.on_backtest_decision_candle(decision_candle)

        engine.execution_engine.process_market_data.assert_not_called()
        engine._signal_processor.on_candle.assert_called_once_with(decision_candle)

    def test_async_backtest_decision_is_rejected_in_live_runtime(self, engine):
        engine.runtime_environment = RuntimeEnvironment("live")
        decision_candle = _make_candle(timeframe="5m")
        engine._signal_processor.on_candle = MagicMock()

        with pytest.raises(RuntimeError, match="backtest-only"):
            engine.on_backtest_decision_candle(decision_candle)

        engine._signal_processor.on_candle.assert_not_called()

    def test_portfolio_exposure_snapshot_excludes_protection_and_exits(
        self,
        engine,
    ):
        product_id = "RITHMIC:MNQ-202609"
        orders = [
            SimpleNamespace(
                id="entry",
                strategy_id="sleeve_a",
                product_id=product_id,
                side="buy",
                quantity=Decimal("2"),
                filled_quantity=Decimal("0.5"),
                intent_payload={},
            ),
            SimpleNamespace(
                id="protection",
                strategy_id="sleeve_a",
                product_id=product_id,
                side="sell",
                quantity=Decimal("1"),
                filled_quantity=Decimal("0"),
                intent_payload={"pending_entry_order_id": "entry"},
            ),
            SimpleNamespace(
                id="exit",
                strategy_id="sleeve_a",
                product_id=product_id,
                side="sell",
                quantity=Decimal("1"),
                filled_quantity=Decimal("0"),
                intent_payload={"reduce_only": True},
            ),
        ]
        repo = engine.execution_engine.order_manager.repo
        repo.list_orders_by_statuses = MagicMock(return_value=orders)
        engine.execution_engine._position_loader = lambda *_args: None

        result = engine.execution_engine.portfolio_exposure_snapshot(
            ("sleeve_a",),
            product_id,
            {},
        )

        assert result.quantities == {"sleeve_a": Decimal("1.5")}


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

        assert engine.process_signal(signal, _make_candle()) is True

        engine.execution_engine.execute_signal.assert_called_once()

    @pytest.mark.parametrize(
        ("signal_type", "quantity", "expected_quantity"),
        [
            (SignalType.LONG, None, Decimal("0.25")),
            (SignalType.LONG, Decimal("0"), Decimal("0.25")),
            (SignalType.LONG, Decimal("-1"), Decimal("0.25")),
            (SignalType.SHORT, None, Decimal("0.25")),
            (SignalType.SHORT, Decimal("0"), Decimal("0.25")),
            (SignalType.SHORT, Decimal("-1"), Decimal("0.25")),
            (SignalType.LONG, Decimal("0.5"), Decimal("0.5")),
            (SignalType.SHORT, Decimal("0.5"), Decimal("0.5")),
            (SignalType.EXIT_LONG, None, None),
            (SignalType.EXIT_SHORT, None, None),
        ],
    )
    def test_effective_quantity_is_shared_by_risk_and_execution(
        self,
        engine,
        signal_type,
        quantity,
        expected_quantity,
    ):
        """Only entries receive defaults, and every consumer sees the same quantity."""
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=signal_type,
            value=Decimal("42000"),
            quantity=quantity,
        )
        engine.execution_engine.default_quantity = Decimal("0.25")
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock(return_value="order-123")

        assert engine.process_signal(signal, _make_candle()) is True

        risk_signal = engine.risk_manager.check_risk.call_args.args[0]
        execution_signal = engine.execution_engine.execute_signal.call_args.args[0]
        assert risk_signal.quantity == expected_quantity
        assert execution_signal.quantity == expected_quantity

    @pytest.mark.parametrize(
        ("signal_type", "quantity", "default_quantity", "expected_field"),
        [
            (SignalType.LONG, Decimal("NaN"), Decimal("0.25"), "signal.quantity"),
            (SignalType.LONG, Decimal("Infinity"), Decimal("0.25"), "signal.quantity"),
            (SignalType.LONG, Decimal("-Infinity"), Decimal("0.25"), "signal.quantity"),
            (SignalType.SHORT, Decimal("NaN"), Decimal("0.25"), "signal.quantity"),
            (SignalType.SHORT, Decimal("Infinity"), Decimal("0.25"), "signal.quantity"),
            (
                SignalType.SHORT,
                Decimal("-Infinity"),
                Decimal("0.25"),
                "signal.quantity",
            ),
            (SignalType.LONG, None, Decimal("0"), "default_entry_quantity"),
            (SignalType.SHORT, None, Decimal("-1"), "default_entry_quantity"),
            (SignalType.LONG, None, Decimal("NaN"), "default_entry_quantity"),
            (SignalType.SHORT, None, Decimal("Infinity"), "default_entry_quantity"),
        ],
    )
    def test_invalid_effective_entry_quantity_is_audited_and_not_executed(
        self,
        engine,
        mock_db_session,
        signal_type,
        quantity,
        default_quantity,
        expected_field,
    ):
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=signal_type,
            value=Decimal("42000"),
        ).model_copy(update={"quantity": quantity})
        engine.execution_engine.default_quantity = default_quantity
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock(return_value="order-123")

        assert engine.process_signal(signal, _make_candle()) is False

        engine.risk_manager.check_risk.assert_not_called()
        engine.execution_engine.execute_signal.assert_not_called()
        audit = mock_db_session.add.call_args.args[0]
        assert audit.risk_status == "REJECT"
        assert expected_field in audit.risk_message

    def test_market_signal_is_fenced_after_leadership_loss(self, engine):
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            value=Decimal("42000"),
        )
        owns_service = True

        def check_risk_then_lose_leadership(*_args, **_kwargs):
            nonlocal owns_service
            owns_service = False
            return True, "PASS"

        def assert_leadership():
            if not owns_service:
                raise RuntimeError("leadership lost")

        engine._leadership_guard = assert_leadership
        engine.risk_manager.check_risk = MagicMock(
            side_effect=check_risk_then_lose_leadership
        )
        engine.execution_engine.order_manager.create_order = MagicMock()
        engine.execution_engine.adapter.place_order = MagicMock()

        with pytest.raises(ExchangeError, match="external_operation_fenced"):
            engine.process_signal(signal, _make_candle())

        engine.execution_engine.order_manager.create_order.assert_not_called()
        engine.execution_engine.adapter.place_order.assert_not_called()
        assert engine.execution_engine._submissions_in_flight == 0
        assert engine._kill_switch_halted is True

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

    def test_rithmic_authoritative_exit_rejection_reports_failure(self, engine):
        signal = Signal(
            strategy_id="test",
            product_id="RITHMIC:MNQ-202609",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.EXIT_LONG,
            quantity=Decimal("1"),
        )
        engine.execution_engine.adapter = _rithmic_adapter_for_reconnect_test()
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_authoritative_exit_signal = MagicMock(
            return_value=False
        )
        engine._rithmic_runtime.route_authoritative_exit = MagicMock(
            return_value=(True, False)
        )
        engine.execution_engine.execute_signal = MagicMock()

        assert engine.process_signal(signal, None) is False

        engine._rithmic_runtime.route_authoritative_exit.assert_called_once_with(
            signal,
            None,
            engine._portfolio_coordinator.portfolio_id_for_sleeve,
            engine.execution_engine.execute_authoritative_exit_signal,
        )
        engine.execution_engine.execute_signal.assert_not_called()

    def test_unhandled_signal_uses_generic_execution_exactly_once(
        self,
        engine,
        mock_db_session,
    ):
        candle = _make_candle()
        signal = Signal(
            strategy_id="test",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.LONG,
            quantity=Decimal("1"),
            value=Decimal("42000"),
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine._rithmic_runtime.route_authoritative_exit = MagicMock(
            return_value=(False, False)
        )
        engine.execution_engine.execute_signal = MagicMock(return_value="order-123")

        assert engine.process_signal(signal, candle) is True

        engine._rithmic_runtime.route_authoritative_exit.assert_called_once_with(
            signal,
            candle,
            engine._portfolio_coordinator.portfolio_id_for_sleeve,
            engine.execution_engine.execute_authoritative_exit_signal,
        )
        engine.execution_engine.execute_signal.assert_called_once_with(
            signal,
            candle,
        )
        audit = mock_db_session.add.call_args.args[0]
        assert audit.order_id == "order-123"
        mock_db_session.commit.assert_called_once_with()

    def test_handled_success_keeps_audited_early_return(
        self,
        engine,
        mock_db_session,
    ):
        signal = Signal(
            strategy_id="test",
            product_id="RITHMIC:MNQ-202609",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.EXIT_LONG,
            quantity=Decimal("1"),
        )
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.audit_external_orders = True
        engine._rithmic_runtime.route_authoritative_exit = MagicMock(
            return_value=(True, True)
        )
        engine.execution_engine.execute_signal = MagicMock()

        assert engine.process_signal(signal, None) is True

        engine._rithmic_runtime.route_authoritative_exit.assert_called_once_with(
            signal,
            None,
            engine._portfolio_coordinator.portfolio_id_for_sleeve,
            engine.execution_engine.execute_authoritative_exit_signal,
        )
        engine.execution_engine.execute_signal.assert_not_called()
        mock_db_session.add.assert_not_called()
        mock_db_session.commit.assert_not_called()

    @pytest.mark.parametrize("rejection", ["risk", "intent"])
    def test_rejection_stops_before_exit_facade_and_generic_execution(
        self,
        engine,
        rejection,
    ):
        signal = Signal(
            strategy_id="test",
            product_id="RITHMIC:MNQ-202609",
            timeframe="1m",
            timestamp=1704067200000,
            type=SignalType.EXIT_LONG,
            quantity=Decimal("1"),
            price=Decimal("0") if rejection == "intent" else None,
        )
        if rejection == "risk":
            engine.risk_manager.check_risk = MagicMock(return_value=(False, "REJECT"))
        engine._rithmic_runtime.route_authoritative_exit = MagicMock()
        engine.execution_engine.execute_signal = MagicMock()

        assert engine.process_signal(signal, None) is False

        engine._rithmic_runtime.route_authoritative_exit.assert_not_called()
        engine.execution_engine.execute_signal.assert_not_called()

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

        assert engine.process_signal(signal, _make_candle()) is False

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

    def test_audited_execution_skips_legacy_pass_audit(
        self, engine_factory, mock_db_session
    ):
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

    def test_audited_execution_still_audits_risk_reject(
        self, engine_factory, mock_db_session
    ):
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
    def test_queued_command_is_rejected_after_leadership_loss(self, engine):
        engine._leadership_guard = MagicMock(
            side_effect=RuntimeError("leadership lost")
        )
        engine.scan_strategies = MagicMock()

        with pytest.raises(RuntimeError, match="leadership lost"):
            engine._handle_command({"command": "SCAN"})

        engine.scan_strategies.assert_not_called()
        assert engine.running is False
        assert engine._order_event_stop.is_set()
        assert engine._runtime_reconcile_stop.is_set()
        assert engine._kill_switch_halted is True

    def test_scan_command(self, engine):
        """SCAN command should call scan_strategies."""
        engine.scan_strategies = MagicMock()

        engine._handle_command({"command": "SCAN"})

        engine.scan_strategies.assert_called_once()

    def test_start_command(self, engine):
        """START command should activate the strategy through lifecycle orchestration."""
        engine.activate_strategy = MagicMock()
        engine._assert_strategy_command_allowed = MagicMock()

        engine._handle_command(
            {
                "command": "START",
                "params": {
                    "id": "strat_1",
                    "actor": "operator@example.com",
                    "reason": "deployment",
                },
            }
        )

        engine.activate_strategy.assert_called_once_with(
            "strat_1",
            actor="operator@example.com",
            reason="deployment",
        )

    def test_strategy_command_replay_uses_one_idempotent_versioned_transition(
        self,
        engine,
    ):
        claimed = set()

        def claim_once(key, value, *, nx=False, **kwargs):
            if nx and key in claimed:
                return None
            claimed.add(key)
            return True

        engine.redis_client.set.side_effect = claim_once
        engine.activate_strategy = MagicMock()
        mock_state = MagicMock(status="ERROR", version=3)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        command = {
            "command": "FORCE_RECOVER",
            "params": {
                "strategy_id": "strat_1",
                "actor": "operator@example.com",
                "expected_version": 3,
                "idempotency_key": "strategy-recover-1",
            },
        }

        engine._handle_command(command)
        engine._handle_command(command)

        engine.activate_strategy.assert_called_once_with(
            "strat_1",
            actor="operator@example.com",
            force=True,
            reason=None,
            expected_version=3,
        )
        first_set = engine.redis_client.set.call_args_list[0]
        assert first_set.args[1] == "claimed"
        assert first_set.kwargs == {"nx": True, "ex": 60}
        completed_set = engine.redis_client.set.call_args_list[1]
        assert completed_set.args[1] == "completed"
        assert completed_set.kwargs == {"ex": 86_400}

    def test_failed_strategy_command_leaves_only_a_short_claim_lease(
        self,
        engine,
    ):
        engine.activate_strategy = MagicMock(
            side_effect=RuntimeError("activation failed")
        )
        mock_state = MagicMock(status="READY", version=3)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine._handle_command(
            {
                "command": "START",
                "params": {
                    "strategy_id": "strat_1",
                    "expected_version": 3,
                    "idempotency_key": "strategy-start-fails",
                },
            }
        )

        engine.redis_client.set.assert_called_once()
        claim = engine.redis_client.set.call_args
        assert claim.args[1] == "claimed"
        assert claim.kwargs == {"nx": True, "ex": 60}

    def test_strategy_command_rejects_when_idempotency_claim_fails(
        self,
        engine,
    ):
        engine.redis_client.set.side_effect = RuntimeError("redis unavailable")
        engine.activate_strategy = MagicMock()
        mock_state = MagicMock(status="READY", version=3)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine._handle_command(
            {
                "command": "START",
                "params": {
                    "strategy_id": "strat_1",
                    "expected_version": 3,
                    "idempotency_key": "strategy-start-1",
                },
            }
        )

        engine.activate_strategy.assert_not_called()

    @pytest.mark.parametrize("status", list(StrategyStatus))
    @pytest.mark.parametrize(
        "command",
        ["START", "STOP", "RESUME", "FORCE_RECOVER"],
    )
    def test_strategy_command_state_matrix_is_enforced_before_execution(
        self,
        engine,
        status,
        command,
    ):
        expected = {
            StrategyStatus.DISCOVERED: {"START"},
            StrategyStatus.READY: {"START"},
            StrategyStatus.WARNING: {"START"},
            StrategyStatus.ACTIVE: {"STOP"},
            StrategyStatus.STOPPED: {"RESUME"},
            StrategyStatus.ERROR: {"FORCE_RECOVER"},
        }
        mock_state = MagicMock(status=status.value, version=3)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        if command in expected[status]:
            engine._assert_strategy_command_allowed(
                strategy_id="strat_1",
                command=command,
                expected_version=3,
            )
        else:
            with pytest.raises(InvalidStrategyStateTransition):
                engine._assert_strategy_command_allowed(
                    strategy_id="strat_1",
                    command=command,
                    expected_version=3,
                )

    @pytest.mark.parametrize("expected_version", [None, 3])
    def test_invalid_strategy_command_is_rejected_before_idempotency_claim(
        self,
        engine,
        expected_version,
    ):
        engine.activate_strategy = MagicMock()
        mock_state = MagicMock(status="ACTIVE", version=3)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine._handle_command(
            {
                "command": "FORCE_RECOVER",
                "params": {
                    "strategy_id": "strat_1",
                    "expected_version": expected_version,
                    "idempotency_key": "strategy-recover-active",
                },
            }
        )

        engine.activate_strategy.assert_not_called()
        engine.redis_client.set.assert_not_called()

    def test_rejected_strategy_activation_is_not_logged_as_success(
        self,
        engine,
        caplog,
    ):
        engine.activate_strategy = MagicMock(return_value=False)
        engine._assert_strategy_command_allowed = MagicMock()

        engine._handle_command(
            {
                "command": "START",
                "params": {"strategy_id": "strat_1"},
            }
        )

        assert "strategy activation rejected: strat_1" in caplog.text
        assert "Command START succeeded" not in caplog.text

    def test_kill_switch_replays_completed_idempotency_key_once(self, engine):
        redis_state = {}
        engine.redis_client.get.side_effect = redis_state.get
        engine.redis_client.set.side_effect = (
            lambda key, value, **kwargs: redis_state.__setitem__(key, value)
        )
        engine._halt_for_kill_switch = MagicMock()
        engine.ops_safety.persist_kill_switch_state = MagicMock()
        engine._run_ops_kill_switch = MagicMock(return_value=_kill_switch_result())
        command = {
            "command": "KILL_SWITCH",
            "params": {
                "actor": "operator@example.com",
                "idempotency_key": "mobile-lockdown-1",
            },
        }

        engine._handle_command(command)
        engine._handle_command(command)

        engine._halt_for_kill_switch.assert_called_once_with()
        engine._run_ops_kill_switch.assert_called_once_with(
            actor="operator@example.com",
            reason=None,
            operation_id="mobile-lockdown-1",
        )
        engine.ops_safety.persist_kill_switch_state.assert_called_once_with(
            "LOCKDOWN",
            actor="operator@example.com",
            reason=None,
            operation_id="mobile-lockdown-1",
        )

    @pytest.mark.parametrize(
        "first_result",
        [
            RuntimeError("exchange unavailable"),
            {"flatten_failures": [{"reason": "exchange unavailable"}]},
        ],
    )
    def test_kill_switch_failed_attempt_does_not_consume_idempotency_key(
        self,
        engine,
        first_result,
    ):
        redis_state = {}
        engine.redis_client.get.side_effect = redis_state.get
        engine.redis_client.set.side_effect = (
            lambda key, value, **kwargs: redis_state.__setitem__(key, value)
        )
        engine._halt_for_kill_switch = MagicMock()
        engine.ops_safety.persist_kill_switch_state = MagicMock()
        engine._run_ops_kill_switch = MagicMock(
            side_effect=[first_result, _kill_switch_result()]
        )
        command = {
            "command": "KILL_SWITCH",
            "params": {
                "actor": "operator@example.com",
                "idempotency_key": "mobile-lockdown-retry",
            },
        }

        engine._handle_command(command)
        engine._handle_command(command)
        engine._handle_command(command)

        assert engine._run_ops_kill_switch.call_count == 2

    @pytest.mark.parametrize("failed_path", ["database", "redis", "both"])
    def test_kill_switch_persistence_failure_keeps_retry_available(
        self,
        engine,
        failed_path,
    ):
        redis_state = {}

        def redis_set(key, value, **kwargs):
            if failed_path in {"redis", "both"} and key == engine._system_state_key:
                raise RuntimeError("redis unavailable")
            redis_state[key] = value

        engine.redis_client.get.side_effect = redis_state.get
        engine.redis_client.set.side_effect = redis_set
        engine._halt_for_kill_switch = MagicMock()
        engine.ops_safety.persist_kill_switch_state = MagicMock()
        if failed_path in {"database", "both"}:
            engine.ops_safety.persist_kill_switch_state.side_effect = [
                RuntimeError("database unavailable"),
                None,
            ]
        engine._run_ops_kill_switch = MagicMock(return_value=_kill_switch_result())
        command = {
            "command": "KILL_SWITCH",
            "params": {
                "actor": "operator@example.com",
                "idempotency_key": "mobile-lockdown-persistence-retry",
            },
        }

        engine._handle_command(command)
        if failed_path in {"redis", "both"}:
            engine.redis_client.set.side_effect = (
                lambda key, value, **kwargs: redis_state.__setitem__(key, value)
            )
        engine._handle_command(command)
        engine._handle_command(command)

        assert engine._run_ops_kill_switch.call_count == 2

    @pytest.mark.parametrize(
        ("marker", "expected_calls"),
        [
            ("completed", 0),
            (b"completed", 0),
            ("unexpected", 1),
            (b"unexpected", 1),
        ],
    )
    def test_kill_switch_only_accepts_exact_completed_marker(
        self,
        engine,
        marker,
        expected_calls,
    ):
        engine.redis_client.get.return_value = marker
        engine._halt_for_kill_switch = MagicMock()
        engine.ops_safety.persist_kill_switch_state = MagicMock()
        engine._run_ops_kill_switch = MagicMock(return_value=_kill_switch_result())

        engine._handle_command(
            {
                "command": "KILL_SWITCH",
                "params": {
                    "actor": "operator@example.com",
                    "idempotency_key": "mobile-lockdown-marker",
                },
            }
        )

        assert engine._run_ops_kill_switch.call_count == expected_calls

    def test_stop_command(self, engine):
        """STOP command should deactivate the strategy through lifecycle orchestration."""
        engine.deactivate_strategy = MagicMock()
        engine._assert_strategy_command_allowed = MagicMock()

        engine._handle_command(
            {
                "command": "STOP",
                "params": {"id": "strat_1", "reason": "maintenance"},
            }
        )

        engine.deactivate_strategy.assert_called_once_with(
            "strat_1",
            actor="operator",
            reason="maintenance",
        )

    def test_resume_command_forces_activation(self, engine):
        """RESUME command should pass force and reason to lifecycle orchestration."""
        engine.activate_strategy = MagicMock()
        engine._assert_strategy_command_allowed = MagicMock()

        engine._handle_command(
            {
                "cmd": "RESUME",
                "strategy_id": "strat_1",
                "reason": "operator confirmed",
            }
        )

        engine.activate_strategy.assert_called_once_with(
            "strat_1",
            actor="operator",
            force=True,
            reason="operator confirmed",
        )

    def test_force_recover_command_forces_activation(self, engine):
        """FORCE_RECOVER command should pass force and reason to lifecycle orchestration."""
        engine.activate_strategy = MagicMock()
        engine._assert_strategy_command_allowed = MagicMock()

        engine._handle_command(
            {
                "cmd": "FORCE_RECOVER",
                "params": {
                    "strategy_id": "strat_1",
                    "reason": "manual reset",
                },
            }
        )

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

        engine._handle_command(
            {"command": "TEST_RUN", "params": {"id": "strat_1", "days": 3}}
        )

        engine.test_run_strategy.assert_called_once_with("strat_1", 3)

    def test_test_run_default_days(self, engine):
        """TEST_RUN without days param should default to 1."""
        engine.test_run_strategy = MagicMock()

        engine._handle_command({"command": "TEST_RUN", "params": {"id": "strat_1"}})

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
    def test_live_rithmic_engine_builds_venue_owned_liveness_gate(
        self,
        mock_db_session,
        mock_clock,
        mock_account_service,
        mock_order_repo,
    ):
        shared_redis = MagicMock()
        liveness_gate = MagicMock(spec=RithmicPublisherLivenessGate)
        adapter = _rithmic_adapter_for_reconnect_test()

        with (
            patch(
                "src.core.engine.RuntimeEnvironment.from_env",
                return_value=RuntimeEnvironment("live"),
            ),
            patch(
                "src.core.engine.create_redis_client",
                return_value=shared_redis,
            ) as redis_factory,
            patch.object(
                RithmicPublisherLivenessGate,
                "for_environment",
                return_value=liveness_gate,
            ) as liveness_factory,
        ):
            engine = StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                order_repository=mock_order_repo,
                account_service=mock_account_service,
                adapter=adapter,
                audit_external_orders=True,
            )

        redis_factory.assert_called_once_with()
        assert engine._entry_admission_gate is liveness_gate
        assert (
            engine._signal_processor.entry_admission_handler
            == engine._entry_signal_allowed_for_processor
        )
        liveness_factory.assert_called_once()
        assert liveness_factory.call_args.args == (engine.runtime_environment,)
        assert liveness_factory.call_args.kwargs["logger"].name == "src.core.engine"

    @pytest.mark.parametrize("environment", ["test", "backtest"])
    def test_non_live_rithmic_engine_does_not_build_liveness_gate(
        self,
        engine_factory,
        environment,
    ):
        with patch(
            "src.core.engine.RuntimeEnvironment.from_env",
            return_value=RuntimeEnvironment(environment),
        ):
            engine = engine_factory(
                adapter=_rithmic_adapter_for_reconnect_test(),
                audit_external_orders=True,
            )

        assert engine._entry_admission_gate is None
        assert engine._signal_processor.entry_admission_handler is None

    def test_live_non_rithmic_engine_has_no_gate_or_extra_redis_read(
        self,
        engine_factory,
    ):
        with patch(
            "src.core.engine.RuntimeEnvironment.from_env",
            return_value=RuntimeEnvironment("live"),
        ):
            engine = engine_factory()

        assert engine._entry_admission_gate is None
        assert engine._signal_processor.entry_admission_handler is None

        engine.process_signal(
            Signal(
                strategy_id="test",
                product_id="BINANCE:BTCUSDT-PERP",
                timeframe="1m",
                timestamp=1704067200000,
                type=SignalType.NO_SIGNAL,
                value=Decimal("42000"),
            ),
            None,
        )

        engine.redis_client.get.assert_not_called()

    def test_safe_startup_arms_liveness_before_resume_and_heartbeat(
        self,
        engine,
    ):
        events: list[str] = []
        gate = MagicMock(spec=RithmicPublisherLivenessGate)
        gate.arm.side_effect = lambda: events.append("arm")
        engine._entry_admission_gate = gate
        engine.execution_engine.adapter = _rithmic_adapter_for_reconnect_test()
        engine._check_system_state = MagicMock(return_value=True)
        engine._reconcile_recoverable_orders_on_startup = MagicMock(
            side_effect=lambda: (
                events.append("reconcile") or {"auto_resume_safe": True}
            )
        )
        engine._can_auto_resume_after_startup_recovery = MagicMock(return_value=True)
        engine._resume_after_kill_switch = MagicMock(
            side_effect=lambda: events.append("resume")
        )
        engine._start_heartbeat = MagicMock(
            side_effect=lambda: events.append("heartbeat")
        )
        for name in (
            "_halt_for_kill_switch",
            "_start_command_listener",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_start_exchange_order_event_stream",
            "_start_runtime_reconciliation",
            "scan_strategies",
            "_restore_active_strategies_on_startup",
        ):
            setattr(engine, name, MagicMock())

        engine.startup()

        assert events == ["reconcile", "arm", "resume", "heartbeat"]

    def test_unsafe_startup_never_arms_liveness(self, engine):
        gate = MagicMock(spec=RithmicPublisherLivenessGate)
        engine._entry_admission_gate = gate
        engine.execution_engine.adapter = _rithmic_adapter_for_reconnect_test()
        engine._check_system_state = MagicMock(return_value=False)
        engine._reconcile_recoverable_orders_on_startup = MagicMock(
            return_value={"auto_resume_safe": False}
        )
        for name in (
            "_halt_for_kill_switch",
            "_start_command_listener",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_start_exchange_order_event_stream",
            "_start_heartbeat",
            "_start_runtime_reconciliation",
            "scan_strategies",
            "_restore_active_strategies_on_startup",
        ):
            setattr(engine, name, MagicMock())

        engine.startup()

        gate.arm.assert_not_called()

    @pytest.mark.parametrize("liveness_allows_heartbeat", [False, True])
    def test_process_heartbeat_continues_while_strategy_heartbeat_is_gated(
        self,
        engine,
        liveness_allows_heartbeat,
    ):
        gate = MagicMock(spec=RithmicPublisherLivenessGate)
        gate.observe.side_effect = lambda: (
            setattr(engine, "running", False) or liveness_allows_heartbeat
        )
        engine._entry_admission_gate = gate
        engine._record_strategy_heartbeats = MagicMock()
        engine.strategy_instances = {}
        engine.running = True

        class ImmediateThread:
            def __init__(self, *, target, daemon):
                self._target = target

            def start(self):
                self._target()

        with (
            patch("src.core.engine.threading.Thread", ImmediateThread),
            patch(
                "src.core.engine.time.sleep",
                side_effect=lambda _seconds: setattr(engine, "running", False),
            ),
        ):
            engine._start_heartbeat()

        engine.redis_client.setex.assert_called_once()
        if liveness_allows_heartbeat:
            engine._record_strategy_heartbeats.assert_called_once_with([])
        else:
            engine._record_strategy_heartbeats.assert_not_called()

    @pytest.mark.parametrize("signal_type", [SignalType.LONG, SignalType.SHORT])
    def test_closed_liveness_gate_rejects_entry_before_risk(
        self,
        engine,
        signal_type,
        caplog,
    ):
        gate = MagicMock(spec=RithmicPublisherLivenessGate)
        gate.observe.return_value = False
        engine._entry_admission_gate = gate
        engine._signal_processor.entry_admission_handler = (
            engine._entry_signal_allowed_for_processor
        )
        engine.risk_manager.check_risk = MagicMock()
        engine.execution_engine.execute_signal = MagicMock()
        signal = Signal(
            strategy_id="test",
            product_id="RITHMIC:NQ-202609",
            timeframe="1m",
            timestamp=1704067200000,
            type=signal_type,
            value=Decimal("20000"),
        )

        with (
            caplog.at_level(logging.WARNING, logger="src.core.engine"),
            patch("src.core.engine.normalize_signal_quantity") as normalize,
        ):
            assert engine.process_signal(signal, _make_candle()) is False

        normalize.assert_not_called()
        engine.risk_manager.check_risk.assert_not_called()
        engine.execution_engine.execute_signal.assert_not_called()
        rejection = next(
            record
            for record in caplog.records
            if getattr(record, "event_code", None) == "entry_admission_rejected"
        )
        assert {
            "level": rejection.levelname,
            "message": rejection.getMessage(),
            "component": vars(rejection)["component"],
            "event_code": vars(rejection)["event_code"],
            "strategy_id": vars(rejection)["strategy_id"],
            "product_id": vars(rejection)["product_id"],
            "signal_type": vars(rejection)["signal_type"],
        } == {
            "level": "WARNING",
            "message": "Entry signal rejected by venue admission gate",
            "component": "strategy_engine",
            "event_code": "entry_admission_rejected",
            "strategy_id": "test",
            "product_id": "RITHMIC:NQ-202609",
            "signal_type": signal_type.value,
        }

    @pytest.mark.parametrize("kill_switch_active", [False, True])
    def test_portfolio_entry_admission_preserves_runtime_dispositions_and_exits(
        self,
        engine,
        mock_strategy_class,
        kill_switch_active,
    ):
        class StatefulSleeve(mock_strategy_class):
            def __init__(self, strategy_id: str):
                super().__init__(strategy_id, "RITHMIC:NQ-202609")
                self._in_position = False
                self.restore_calls = 0

            def on_candle(self, candle):
                signal_type = (
                    SignalType.LONG
                    if candle.timestamp == 1_704_067_200_000
                    else SignalType.EXIT_LONG
                )
                if signal_type == SignalType.LONG:
                    self._in_position = True
                return Signal(
                    strategy_id=self.strategy_id,
                    product_id=self.product_id,
                    timeframe=candle.timeframe,
                    timestamp=candle.timestamp,
                    type=signal_type,
                    quantity=Decimal("1"),
                    value=candle.close,
                )

            def snapshot_walk_forward_trade_state(self):
                return self._in_position

            def restore_walk_forward_trade_state(self, state):
                self.restore_calls += 1
                self._in_position = state

        class PassiveSleeve(StatefulSleeve):
            def on_candle(self, candle):
                return Signal(
                    strategy_id=self.strategy_id,
                    product_id=self.product_id,
                    timeframe=candle.timeframe,
                    timestamp=candle.timestamp,
                    type=SignalType.EXIT_LONG,
                    quantity=Decimal("1"),
                    value=candle.close,
                )

        active = StatefulSleeve("portfolio_v1.active")
        passive = PassiveSleeve("portfolio_v1.passive")
        engine.add_portfolio(
            PortfolioDefinition(
                portfolio_id="portfolio_v1",
                product_id=active.product_id,
                sleeves=(PortfolioSleeve(active), PortfolioSleeve(passive)),
                max_gross_quantity=Decimal("1"),
                exclusive_slots=(
                    PortfolioExclusiveSlot(
                        slot_id="shared",
                        strategy_ids=(active.strategy_id, passive.strategy_id),
                    ),
                ),
            )
        )
        engine._signal_processor.exposure_loader = (
            lambda *_args: PortfolioExposureSnapshot({})
        )
        gate = MagicMock(spec=RithmicPublisherLivenessGate)
        gate.observe.return_value = False
        engine._entry_admission_gate = gate
        engine._signal_processor.entry_admission_handler = (
            engine._entry_signal_allowed_for_processor
        )
        engine._kill_switch_halted = kill_switch_active
        engine._persist_live_candle = MagicMock()
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock(return_value="exit-order")
        entry_candle = _make_candle(
            product_id=active.product_id,
            ts=1_704_067_200_000,
        )
        if kill_switch_active:
            with pytest.raises(
                PortfolioDecisionRejected,
                match="portfolio_submission_rejected",
            ):
                engine.on_market_data(entry_candle)
            gate.observe.assert_not_called()
            engine.execution_engine.execute_signal.assert_not_called()
            engine._persist_live_candle.assert_not_called()
            return

        engine.on_market_data(entry_candle)

        assert active._in_position is False
        assert active.restore_calls == 1
        engine._persist_live_candle.assert_called_once_with(entry_candle)
        engine.risk_manager.check_risk.assert_called_once()
        engine.execution_engine.execute_signal.assert_called_once()
        assert (
            engine.execution_engine.execute_signal.call_args.args[0].type
            == SignalType.EXIT_LONG
        )
        gate.observe.assert_called_once_with()

    def test_trade_entry_kill_switch_rejection_does_not_read_liveness_gate(
        self,
        engine,
        mock_strategy_class,
    ):
        class TradeEntryStrategy(mock_strategy_class):
            def __init__(self):
                super().__init__("trade_entry", "BINANCE:BTCUSDT-PERP")
                self.trade_calls = 0

            def on_trade(self, _trade):
                self.trade_calls += 1
                return signal

        signal = Signal(
            strategy_id="trade_entry",
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="1m",
            timestamp=1_704_067_200_000,
            type=SignalType.LONG,
            quantity=Decimal("1"),
            value=Decimal("42000"),
        )
        strategy = TradeEntryStrategy()
        engine.add_strategy(strategy)
        gate = MagicMock(spec=RithmicPublisherLivenessGate)
        gate.observe.return_value = False
        engine._entry_admission_gate = gate
        engine._signal_processor.entry_admission_handler = (
            engine._entry_signal_allowed_for_processor
        )
        engine._kill_switch_halted = True
        engine.risk_manager.check_risk = MagicMock()
        engine.execution_engine.execute_signal = MagicMock()

        engine.on_market_data(
            Trade(
                id="trade-1",
                product_id=strategy.product_id,
                price=Decimal("42000"),
                quantity=Decimal("1"),
                side=OrderSide.BUY,
                timestamp=1_704_067_200_000,
            )
        )

        assert strategy.trade_calls == 1
        gate.observe.assert_not_called()
        engine.risk_manager.check_risk.assert_not_called()
        engine.execution_engine.execute_signal.assert_not_called()

    @pytest.mark.parametrize(
        "signal_type",
        [SignalType.NO_SIGNAL, SignalType.EXIT_LONG, SignalType.EXIT_SHORT],
    )
    def test_non_entry_signal_bypasses_liveness_gate(
        self,
        engine,
        signal_type,
    ):
        gate = MagicMock(spec=RithmicPublisherLivenessGate)
        engine._entry_admission_gate = gate
        engine.risk_manager.check_risk = MagicMock(return_value=(True, "PASS"))
        engine.execution_engine.execute_signal = MagicMock(return_value="order-1")
        signal = Signal(
            strategy_id="test",
            product_id="RITHMIC:NQ-202609",
            timeframe="1m",
            timestamp=1704067200000,
            type=signal_type,
            value=Decimal("20000"),
        )

        assert engine.process_signal(signal, _make_candle()) is True

        gate.observe.assert_not_called()

    def test_shutdown_closes_liveness_gate(self, engine):
        gate = MagicMock(spec=RithmicPublisherLivenessGate)
        engine._entry_admission_gate = gate

        engine.shutdown(timeout=0.1)

        gate.close.assert_called_once_with()

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
        engine._health_monitor.update_heartbeat = MagicMock(
            side_effect=RuntimeError("boom")
        )
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

    def test_scan_removes_classes_missing_from_latest_artifact(
        self,
        engine,
        mock_strategy_class,
    ):
        engine.loaded_classes["stale.py::OldStrat"] = mock_strategy_class
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        engine._db_session_factory = lambda: nullcontext(mock_db)

        with patch("src.core.engine.StrategyLoader.scan_directory") as mock_scan:
            mock_scan.return_value = {"new.py::NewStrat": mock_strategy_class}

            engine.scan_strategies()

        assert set(engine.loaded_classes) == {"new.py::NewStrat"}

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
        mock_state.version = 3
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._strategy_state_manager.transition_to_error = MagicMock()

        with patch("src.core.engine.StrategyLoader.scan_directory") as mock_scan:
            mock_scan.return_value = {"bad.py::LoadError": "traceback string"}
            engine.scan_strategies()

        engine._strategy_state_manager.transition_to_error.assert_called_once_with(
            "bad.py::LoadError",
            "traceback string",
            actor="system",
            expected_version=3,
        )


class TestStartStrategy:
    @pytest.mark.parametrize(
        ("product_id", "allowed"),
        [
            ("BINANCE:BTCUSDT-PERP", True),
            ("BINANCE:ETHUSDT-PERP", False),
            ("BYBIT:BTCUSDT-PERP", False),
            ("", False),
        ],
    )
    def test_live_strategy_product_must_match_adapter_allowlist(
        self,
        engine_factory,
        product_id,
        allowed,
    ):
        engine = engine_factory(
            adapter_config={
                "mode": "live",
                "instrument_product_ids": ["BINANCE:BTCUSDT-PERP"],
            }
        )

        if allowed:
            assert engine._strategy_product_id({"product_id": product_id}) == product_id
        else:
            with pytest.raises(ValueError):
                engine._strategy_product_id({"product_id": product_id})

    def test_stale_expected_version_rejects_before_runtime_mutation(
        self,
        engine,
        mock_strategy_class,
    ):
        engine.loaded_classes["test.py::MyStrat"] = mock_strategy_class
        engine._warm_up_strategy_instance = MagicMock()
        engine._register_strategy_instance = MagicMock()
        engine._strategy_state_manager.transition_to_running = MagicMock()
        mock_state = MagicMock()
        mock_state.version = 4
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        with pytest.raises(
            StaleStrategyStateVersion,
            match="expected version 3, found 4",
        ):
            engine.activate_strategy(
                "test.py::MyStrat",
                force=True,
                expected_version=3,
            )

        engine._warm_up_strategy_instance.assert_not_called()
        engine._register_strategy_instance.assert_not_called()
        engine._strategy_state_manager.transition_to_running.assert_not_called()

    def test_concurrent_start_with_same_version_keeps_winner_registered(
        self,
        engine,
        mock_strategy_class,
    ):
        strategy_id = "test.py::MyStrat"
        engine.loaded_classes[strategy_id] = mock_strategy_class
        state = MagicMock(
            status=StrategyStatus.READY,
            version=3,
            config_json='{"product_id":"BINANCE:BTCUSDT-PERP"}',
        )
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = state
        engine._db_session_factory = lambda: nullcontext(mock_db)

        warmup_started = threading.Event()
        second_warmup_started = threading.Event()
        finish_warmup = threading.Event()
        warmup_calls = 0

        def warm_up(*_args):
            nonlocal warmup_calls
            warmup_calls += 1
            if warmup_calls == 1:
                warmup_started.set()
            else:
                second_warmup_started.set()
            assert finish_warmup.wait(timeout=1)

        def transition_to_running(*_args, **_kwargs):
            state.status = StrategyStatus.ACTIVE
            state.version = 4

        engine._warm_up_strategy_instance = MagicMock(side_effect=warm_up)
        engine._strategy_state_manager.transition_to_running = MagicMock(
            side_effect=transition_to_running
        )

        results = []
        errors = []

        def activate():
            try:
                results.append(
                    engine.activate_strategy(
                        strategy_id,
                        expected_version=3,
                    )
                )
            except Exception as error:
                errors.append(error)

        first = threading.Thread(target=activate)
        second = threading.Thread(target=activate)
        first.start()
        assert warmup_started.wait(timeout=1)
        second.start()
        assert second_warmup_started.wait(timeout=0.1) is False
        assert engine._warm_up_strategy_instance.call_count == 1
        finish_warmup.set()
        first.join(timeout=1)
        second.join(timeout=1)

        assert not first.is_alive()
        assert not second.is_alive()
        assert results == [True]
        assert len(errors) == 1
        assert isinstance(errors[0], StaleStrategyStateVersion)
        assert state.status == StrategyStatus.ACTIVE
        assert strategy_id in engine.strategy_instances

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
        mock_state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
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

    def test_start_portfolio_factory_registers_complete_parent_lifecycle(
        self,
        engine,
        mock_strategy_class,
    ):
        class ReplaySafePortfolioSleeve(mock_strategy_class):
            def replay_configuration(self):
                return {
                    "strategy_id": self.strategy_id,
                    "product_id": self.product_id,
                }

        class TestPortfolioFactory(PortfolioFactory):
            def build(self, *, portfolio_id, product_id, config):
                return PortfolioDefinition(
                    portfolio_id=portfolio_id,
                    product_id=product_id,
                    sleeves=tuple(
                        PortfolioSleeve(
                            ReplaySafePortfolioSleeve(
                                f"{portfolio_id}.sleeve_{index}",
                                product_id,
                            )
                        )
                        for index in range(2)
                    ),
                    max_gross_quantity=Decimal(str(config["max_gross_quantity"])),
                )

        engine.loaded_classes["portfolio_v1"] = TestPortfolioFactory
        engine._strategy_state_manager.transition_to_running = MagicMock()
        engine._strategy_state_manager.on_state_change_message = MagicMock()
        mock_state = MagicMock()
        mock_state.status = "READY"
        mock_state.config_json = json.dumps(
            {
                "product_id": "BINANCE:BTCUSDT-PERP",
                "max_gross_quantity": "2",
            }
        )
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._warm_up_strategy_instance = MagicMock(return_value=0)

        engine.start_strategy("portfolio_v1")

        assert set(engine.strategy_instances) == {
            "portfolio_v1.sleeve_0",
            "portfolio_v1.sleeve_1",
        }
        assert "portfolio_v1" in engine.portfolio_instances
        assert engine._warm_up_strategy_instance.call_count == 2
        engine._strategy_state_manager.on_state_change_message.assert_not_called()
        engine._strategy_state_manager.transition_to_running.assert_called_once_with(
            "portfolio_v1",
            actor="operator",
            force=False,
            reason=None,
        )

        engine._strategy_state_manager.transition_to_stopped = MagicMock()
        engine.stop_strategy("portfolio_v1")

        assert engine.strategy_instances == {}
        assert engine.portfolio_instances == {}
        engine._strategy_state_manager.transition_to_stopped.assert_called_once_with(
            "portfolio_v1",
            actor="operator",
            reason=None,
        )

    @pytest.mark.parametrize(
        ("readiness", "starts"),
        [
            (None, False),
            ("RESEARCH_VALIDATED", False),
            ("RESEARCH_FROZEN", False),
            ("LIVE_APPROVED", True),
        ],
    )
    def test_catalog_readiness_gates_live_activation(
        self,
        engine,
        mock_strategy_class,
        readiness,
        starts,
    ):
        mock_strategy_class.__fluxtrade_readiness__ = readiness
        engine.runtime_environment = RuntimeEnvironment("live")
        engine.loaded_classes["stable_strategy_v1"] = mock_strategy_class
        engine._strategy_state_manager.transition_to_running = MagicMock()
        engine._strategy_state_manager.transition_to_error = MagicMock()
        mock_state = MagicMock()
        mock_state.status = "READY"
        mock_state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._warm_up_strategy_instance = MagicMock(return_value=0)
        engine._fresh_strategy_instance_for_replay = MagicMock()

        engine.start_strategy("stable_strategy_v1")

        assert ("stable_strategy_v1" in engine.strategy_instances) is starts
        assert engine._warm_up_strategy_instance.called is starts

    def test_start_uses_loaded_class_snapshot_during_rescan(
        self,
        engine,
        mock_strategy_class,
    ):
        engine.loaded_classes["stable_strategy_v1"] = mock_strategy_class
        mock_state = MagicMock()
        mock_state.status = "READY"
        mock_state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._warm_up_strategy_instance = MagicMock(return_value=0)
        engine._strategy_state_manager.transition_to_running = MagicMock()

        def snapshot_then_rescan(_strategy_id):
            strategy_cls = engine.loaded_classes["stable_strategy_v1"]
            engine.loaded_classes = {}
            return strategy_cls

        engine._get_loaded_strategy_class = snapshot_then_rescan

        engine.start_strategy("stable_strategy_v1")

        assert "stable_strategy_v1" in engine.strategy_instances

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
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = None
        engine._db_session_factory = lambda: nullcontext(mock_db)

        engine.stop_strategy("nonexistent")
        # Should complete without error

    def test_stop_reconciles_active_state_when_runtime_is_missing(self, engine):
        state = MagicMock(status=StrategyStatus.ACTIVE, version=3)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._strategy_state_manager.transition_to_stopped = MagicMock()

        stopped = engine.deactivate_strategy(
            "missing-runtime",
            expected_version=3,
        )

        assert stopped is True
        engine._strategy_state_manager.transition_to_stopped.assert_called_once_with(
            "missing-runtime",
            actor="operator",
            reason=None,
            expected_version=3,
        )

    def test_stop_transition_failure_keeps_active_runtime_registered(
        self,
        engine,
        strategy_instance,
    ):
        engine.add_strategy(strategy_instance)
        state = MagicMock(status=StrategyStatus.ACTIVE, version=3)
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._strategy_state_manager.transition_to_stopped = MagicMock(
            side_effect=RuntimeError("database unavailable")
        )

        with pytest.raises(RuntimeError, match="database unavailable"):
            engine.deactivate_strategy("test_strat", expected_version=3)

        assert engine.strategy_instances["test_strat"] is strategy_instance

    def test_stop_blocks_market_processing_until_runtime_is_removed(
        self,
        engine,
        strategy_instance,
    ):
        engine.add_strategy(strategy_instance)
        transition_started = threading.Event()
        release_transition = threading.Event()
        market_attempted = threading.Event()
        market_finished = threading.Event()
        active_at_decision = []

        def transition_to_stopped(*_args, **_kwargs):
            transition_started.set()
            assert release_transition.wait(timeout=1)

        def observe_active_strategies(_candle):
            active_at_decision.extend(
                strategy.strategy_id for strategy in engine._registry.list_active()
            )

        engine._strategy_state_manager.transition_to_stopped = MagicMock(
            side_effect=transition_to_stopped
        )
        engine._signal_processor.on_candle = MagicMock(
            side_effect=observe_active_strategies
        )

        def process_market():
            market_attempted.set()
            engine.on_market_data(_make_candle())
            market_finished.set()

        stop_thread = threading.Thread(
            target=engine.deactivate_strategy,
            args=("test_strat",),
        )
        market_thread = threading.Thread(target=process_market)
        stop_thread.start()
        assert transition_started.wait(timeout=1)
        market_thread.start()
        assert market_attempted.wait(timeout=1)
        assert market_finished.wait(timeout=0.05) is False
        assert engine._signal_processor.on_candle.call_count == 0

        release_transition.set()
        stop_thread.join(timeout=1)
        market_thread.join(timeout=1)

        assert not stop_thread.is_alive()
        assert not market_thread.is_alive()
        assert market_finished.is_set()
        assert active_at_decision == []

    def test_stop_rejects_portfolio_sleeve_before_state_transition(
        self,
        engine,
    ):
        engine._portfolio_coordinator.portfolio_id_for_sleeve = MagicMock(
            return_value="portfolio-1"
        )
        engine._strategy_state_manager.transition_to_stopped = MagicMock()

        with pytest.raises(
            ValueError,
            match="portfolio sleeves must be controlled",
        ):
            engine.deactivate_strategy("portfolio-1.sleeve-1")

        engine._strategy_state_manager.transition_to_stopped.assert_not_called()


class TestTestRunStrategy:
    def test_test_run_unloaded_returns_early(self, engine):
        """test_run on unloaded strategy should return."""
        engine.test_run_strategy("nonexistent", 1)
        # No crash

    def test_test_run_data_available_sets_ready(self, engine, mock_strategy_class):
        """When data is available, strategy should be set to READY."""
        engine.loaded_classes["test.py::MyStrat"] = mock_strategy_class

        mock_state = MagicMock()
        mock_state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
        mock_state.version = 3
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._strategy_state_manager.transition_to_status = MagicMock()

        with patch("src.core.engine.check_data_availability", return_value=(True, "")):
            engine.test_run_strategy("test.py::MyStrat", 1)

        engine._strategy_state_manager.transition_to_status.assert_called_once_with(
            "test.py::MyStrat",
            StrategyStatus.READY,
            actor="system",
            reason="test_run_completed",
            expected_version=3,
        )

    def test_test_run_data_insufficient_sets_warning(self, engine, mock_strategy_class):
        """When data is insufficient, strategy should be set to WARNING."""
        engine.loaded_classes["test.py::MyStrat"] = mock_strategy_class

        mock_state = MagicMock()
        mock_state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
        mock_state.version = 3
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._strategy_state_manager.transition_to_status = MagicMock()

        with patch(
            "src.core.engine.check_data_availability",
            return_value=(False, "docker exec ..."),
        ):
            engine.test_run_strategy("test.py::MyStrat", 1)

        engine._strategy_state_manager.transition_to_status.assert_called_once_with(
            "test.py::MyStrat",
            StrategyStatus.WARNING,
            actor="system",
            reason="insufficient_data",
            expected_version=3,
        )

    def test_test_run_exception_sets_error_metadata(self, engine):
        """Warm-up failures should satisfy ERROR state metadata constraints."""

        class FailingStrategy:
            def __init__(self, strategy_id, product_id):
                raise RuntimeError("warm-up failed")

        engine.loaded_classes["test.py::FailingStrat"] = FailingStrategy

        mock_state = MagicMock()
        mock_state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
        mock_state.version = 3
        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.first.return_value = mock_state
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine.test_run_strategy("test.py::FailingStrat", 1)

        engine._strategy_state_manager.transition_to_error.assert_called_once()
        args = engine._strategy_state_manager.transition_to_error.call_args.args
        assert args[0] == "test.py::FailingStrat"
        assert "warm-up failed" in args[1]
        assert (
            engine._strategy_state_manager.transition_to_error.call_args.kwargs[
                "expected_version"
            ]
            == 3
        )


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
        state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
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

    def test_live_activation_rejects_strategy_without_replay_contract(
        self,
        engine,
    ):
        from src.strategies.base import BaseStrategy, StrategyRequirements

        class UnrecoverableStrategy(BaseStrategy):
            __fluxtrade_readiness__ = "LIVE_APPROVED"

            @property
            def requirements(self):
                return StrategyRequirements(self.product_id, "1m", 0)

            def on_candle(self, candle, context=None):
                return None

        state = MagicMock()
        state.status = StrategyStatus.READY
        state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
        mock_db = MagicMock()
        mock_db.query.side_effect = lambda model: self._FakeQuery(
            model,
            state,
            [],
        )
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine.runtime_environment = RuntimeEnvironment("live")
        engine.account_service.get_position = MagicMock(return_value=None)
        engine.loaded_classes["test.py::UnrecoverableStrategy"] = UnrecoverableStrategy
        engine._strategy_state_manager.transition_to_running = MagicMock()
        engine._strategy_state_manager.transition_to_error = MagicMock()

        engine.activate_strategy("test.py::UnrecoverableStrategy")

        assert "test.py::UnrecoverableStrategy" not in engine.strategy_instances
        engine._strategy_state_manager.transition_to_running.assert_not_called()
        engine._strategy_state_manager.transition_to_error.assert_called_once()
        assert "pending-market replay configuration" in state.performance_json

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
        state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
        state.version = 3
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

        engine.activate_strategy(
            "test.py::FailingWarmupStrategy",
            expected_version=3,
        )

        assert "test.py::FailingWarmupStrategy" not in engine.strategy_instances
        engine._strategy_state_manager.transition_to_running.assert_not_called()
        engine._strategy_state_manager.transition_to_error.assert_called_once_with(
            "test.py::FailingWarmupStrategy",
            "warm-up replay failed",
            actor="system",
            expected_version=3,
        )
        assert "warm-up replay failed" in state.performance_json

    def test_activation_keeps_runtime_when_state_notification_fails(
        self,
        engine,
        mock_strategy_class,
    ):
        state = MagicMock(
            status=StrategyStatus.READY,
            version=3,
            config_json='{"product_id":"BINANCE:BTCUSDT-PERP"}',
        )
        query = MagicMock()
        query.filter.return_value.first.return_value = state
        query.filter_by.return_value.first.return_value = state
        query.filter_by.return_value.filter.return_value.update.return_value = 1
        mock_db = MagicMock()
        mock_db.query.return_value = query
        engine._db_session_factory = lambda: nullcontext(mock_db)
        engine._strategy_state_manager = StrategyStateManager(
            engine._db_session_factory,
            engine.redis_client,
        )
        engine.redis_client.publish.side_effect = RuntimeError("redis unavailable")
        engine.loaded_classes["test.py::MyStrat"] = mock_strategy_class
        engine._warm_up_strategy_instance = MagicMock(return_value=0)

        activated = engine.activate_strategy(
            "test.py::MyStrat",
            expected_version=3,
        )

        assert activated is True
        assert "test.py::MyStrat" in engine.strategy_instances
        assert engine._strategy_state_manager.is_running("test.py::MyStrat")

    def test_activate_strategy_fails_closed_when_warmup_data_is_insufficient(
        self, engine
    ):
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
        state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
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
        state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
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
        state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
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
        state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
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
        engine.loaded_classes["test.py::ZeroLookbackNoSyncStrategy"] = (
            ZeroLookbackNoSyncStrategy
        )
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
        state.config_json = '{"product_id":"BINANCE:BTCUSDT-PERP"}'
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
        engine.ops_safety.latest_kill_switch_state = MagicMock(return_value="LOCKDOWN")

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
            "OK" if key == engine._system_state_key else json.dumps(previous_boot)
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
                if call.args[0] == engine._system_boot_state_key
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
    def test_untrusted_previous_boot_fails_closed(self, engine, db_boot, redis_boot):
        engine.redis_client.get.side_effect = lambda key: (
            "OK"
            if key == engine._system_state_key
            else json.dumps(redis_boot)
            if redis_boot is not None
            else None
        )
        engine.ops_safety.latest_kill_switch_state = MagicMock(return_value="OK")
        engine.ops_safety.latest_engine_boot_state = MagicMock(return_value=db_boot)
        engine.ops_safety.persist_engine_boot_state = MagicMock()

        assert engine._check_system_state() is True
        assert engine._kill_switch_halted is True

    def test_unclean_boot_with_clear_kill_state_allows_automatic_recovery(self, engine):
        previous_boot = {"state": "UNCLEAN", "boot_id": "previous"}
        engine.redis_client.get.side_effect = lambda key: (
            "OK" if key == engine._system_state_key else json.dumps(previous_boot)
        )
        engine.ops_safety.latest_kill_switch_state = MagicMock(return_value="OK")
        engine.ops_safety.latest_engine_boot_state = MagicMock(
            return_value=previous_boot
        )
        engine.ops_safety.persist_engine_boot_state = MagicMock()

        assert engine._check_system_state() is True
        assert engine._startup_auto_recovery_allowed is True
        assert engine._startup_lock_cause == "unclean_boot"

    @pytest.mark.parametrize(
        "summary, resumes",
        [
            (
                {
                    "recoverable_count": 1,
                    "unresolved_count": 0,
                    "verification_blocked_count": 0,
                    "auto_resume_safe": True,
                },
                True,
            ),
            (
                {
                    "recoverable_count": 1,
                    "unresolved_count": 1,
                    "verification_blocked_count": 1,
                    "auto_resume_safe": False,
                },
                False,
            ),
        ],
    )
    def test_unclean_startup_only_auto_resumes_after_clean_reconciliation(
        self,
        engine,
        summary,
        resumes,
    ):
        engine._check_system_state = MagicMock(return_value=True)
        engine._startup_auto_recovery_allowed = True
        engine._startup_lock_cause = "unclean_boot"
        engine._reconcile_recoverable_orders_on_startup = MagicMock(
            return_value=summary
        )
        engine._resume_after_kill_switch = MagicMock(
            side_effect=lambda: setattr(engine, "_kill_switch_halted", False)
        )
        engine.ops_safety.kill_switch = MagicMock()
        for name in (
            "_start_command_listener",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_start_heartbeat",
            "scan_strategies",
            "_restore_active_strategies_on_startup",
        ):
            setattr(engine, name, MagicMock())

        engine.startup()

        assert engine._resume_after_kill_switch.called is resumes
        engine.ops_safety.kill_switch.assert_not_called()
        assert engine._restore_active_strategies_on_startup.called is resumes

    def test_generic_reconciliation_cannot_auto_resume_unclean_startup(self, engine):
        engine._startup_auto_recovery_allowed = True

        assert (
            engine._can_auto_resume_after_startup_recovery(
                {
                    "recoverable_count": 0,
                    "unresolved_count": 0,
                    "verification_blocked_count": 0,
                }
            )
            is False
        )

    def test_current_boot_marker_dual_write_failure_fails_closed(self, engine):
        previous_boot = {"state": "CLEAN", "boot_id": "previous"}
        engine.redis_client.get.side_effect = lambda key: (
            "OK" if key == engine._system_state_key else json.dumps(previous_boot)
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

    def test_startup_stops_after_leadership_loss_between_stateful_phases(
        self,
        engine,
    ):
        owns_service = True

        def assert_leadership():
            if not owns_service:
                raise RuntimeError("leadership lost")

        def reconcile_then_lose_leadership():
            nonlocal owns_service
            owns_service = False
            return {
                "recoverable_count": 0,
                "unresolved_count": 0,
                "verification_blocked_count": 0,
                "auto_resume_safe": True,
            }

        engine._check_system_state = MagicMock(return_value=False)
        engine._reconcile_recoverable_orders_on_startup = MagicMock(
            side_effect=reconcile_then_lose_leadership
        )
        for name in (
            "_start_command_listener",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_start_exchange_order_event_stream",
            "_start_heartbeat",
            "scan_strategies",
            "_restore_active_strategies_on_startup",
        ):
            setattr(engine, name, MagicMock())

        with pytest.raises(RuntimeError, match="leadership lost"):
            engine.startup(leadership_guard=assert_leadership)

        engine._start_exchange_order_event_stream.assert_not_called()
        engine._start_heartbeat.assert_not_called()
        engine.scan_strategies.assert_not_called()
        assert engine._kill_switch_halted is True
        assert engine.execution_engine._submissions_halted is True

    def test_startup_phase_failure_halts_after_reconciliation_resume(
        self,
        engine,
    ):
        summary = {
            "recoverable_count": 0,
            "unresolved_count": 0,
            "verification_blocked_count": 0,
            "auto_resume_safe": True,
        }
        engine._check_system_state = MagicMock(return_value=True)
        engine._can_auto_resume_after_startup_recovery = MagicMock(return_value=True)
        engine._resume_after_kill_switch = MagicMock(
            side_effect=lambda: setattr(engine, "_kill_switch_halted", False)
        )
        engine._reconcile_recoverable_orders_on_startup = MagicMock(
            return_value=summary
        )
        for name in (
            "_start_command_listener",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_start_exchange_order_event_stream",
        ):
            setattr(engine, name, MagicMock())
        engine._start_heartbeat = MagicMock(
            side_effect=RuntimeError("heartbeat startup failed")
        )
        engine.scan_strategies = MagicMock()

        with pytest.raises(RuntimeError, match="heartbeat startup failed"):
            engine.startup()

        engine.scan_strategies.assert_not_called()
        assert engine._kill_switch_halted is True
        assert engine.execution_engine._submissions_halted is True


class TestRuntimeReconciliationThread:
    def test_startup_skips_runtime_reconciliation_for_simulated_mode(self, engine):
        """Runtime reconciliation is live-only; simulated runs must not emit false drift."""
        startup_steps = [
            "_check_system_state",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_reconcile_recoverable_orders_on_startup",
            # Must be mocked: otherwise live startup leaks a real order-event
            # daemon thread that spins on the MagicMock adapter's poll and hangs
            # later tests in the same process.
            "_start_exchange_order_event_stream",
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
        with (
            patch("src.core.engine.create_redis_client") as mock_factory,
            patch("src.core.engine.create_adapter") as mock_create,
        ):
            mock_factory.return_value = MagicMock()
            mock_create.return_value = MagicMock()
            engine = StrategyEngine(
                db_session=mock_db_session,
                clock=mock_clock,
                adapter_config={
                    "mode": "live",
                    "instrument_product_ids": ["BINANCE:BTCUSDT-PERP"],
                },
            )

        startup_steps = [
            "_check_system_state",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_reconcile_recoverable_orders_on_startup",
            # Must be mocked: otherwise live startup leaks a real order-event
            # daemon thread that spins on the MagicMock adapter's poll and hangs
            # later tests in the same process.
            "_start_exchange_order_event_stream",
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

        with (
            patch("src.core.engine.threading.Thread", ImmediateThread),
            patch(
                "src.core.engine.time.sleep",
                side_effect=AssertionError(
                    "runtime reconciliation sleep must be interruptible"
                ),
            ),
        ):
            engine._start_runtime_reconciliation()

        assert created_threads[0].daemon is True
        engine.runtime_reconciliation_job.run_once.assert_called_once()
        engine._runtime_reconcile_stop.wait.assert_called_once_with(3600.0)

    def test_runtime_reconciliation_stops_before_work_after_leadership_loss(
        self,
        engine,
    ):
        engine._leadership_guard = MagicMock(
            side_effect=RuntimeError("leadership lost")
        )
        engine.runtime_reconciliation_job.run_once = MagicMock()

        class ImmediateThread:
            def __init__(self, *, target, daemon):
                self.target = target

            def start(self):
                self.target()

        with patch("src.core.engine.threading.Thread", ImmediateThread):
            engine._start_runtime_reconciliation()

        engine.runtime_reconciliation_job.run_once.assert_not_called()
        assert engine.running is False
        assert engine._kill_switch_halted is True

    def test_rithmic_runtime_reconciliation_waits_before_first_session_restart(
        self,
        engine,
    ):
        engine.execution_engine.adapter = _rithmic_adapter_for_reconnect_test()
        engine._runtime_reconcile_interval = 300.0
        engine._runtime_reconcile_stop = MagicMock()
        engine._runtime_reconcile_stop.is_set.return_value = False
        events = []
        wait_count = 0

        def wait_for_interval(interval):
            nonlocal wait_count
            wait_count += 1
            events.append(("wait", interval))
            return wait_count == 2

        engine._runtime_reconcile_stop.wait.side_effect = wait_for_interval
        engine._run_rithmic_runtime_reconciliation_once = MagicMock(
            side_effect=lambda: events.append(("reconcile", None))
        )

        class ImmediateThread:
            def __init__(self, *, target, daemon):
                self.target = target
                self.daemon = daemon

            def start(self):
                self.target()

        with patch("src.core.engine.threading.Thread", ImmediateThread):
            engine._start_runtime_reconciliation()

        assert events == [
            ("wait", 300.0),
            ("reconcile", None),
            ("wait", 300.0),
        ]
        engine._run_rithmic_runtime_reconciliation_once.assert_called_once_with()

    def test_runtime_recovery_owner_runs_once_inside_market_then_ops_locks(
        self,
        engine,
    ):
        events: list[str] = []

        class ObservableLock:
            def __init__(self, name: str) -> None:
                self.name = name

            def __enter__(self):
                events.append(f"enter:{self.name}")
                return self

            def __exit__(self, *_args):
                events.append(f"exit:{self.name}")

        adapter = _rithmic_adapter_for_reconnect_test()
        owner = MagicMock()
        owner.run_once.side_effect = lambda: events.append("owner") or True
        engine.execution_engine.adapter = adapter
        engine._rithmic_runtime.runtime_recovery = owner
        engine._market_processing_lock = ObservableLock("market")
        engine._ops_command_lock = ObservableLock("ops")
        engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock()

        assert engine._run_rithmic_runtime_reconciliation_once() is True

        assert events == [
            "enter:market",
            "enter:ops",
            "owner",
            "exit:ops",
            "exit:market",
        ]
        owner.run_once.assert_called_once_with()
        engine.execution_engine.reconcile_rithmic_owned_orders.assert_not_called()

    def test_runtime_recovery_delegate_rejects_non_rithmic_adapter(self, engine):
        owner = MagicMock()
        engine._rithmic_runtime.runtime_recovery = owner

        with pytest.raises(
            RuntimeError,
            match="rithmic_runtime_reconciliation_adapter_mismatch",
        ):
            engine._run_rithmic_runtime_reconciliation_once()

        owner.run_once.assert_not_called()

    def test_runtime_recovery_delegate_fails_closed_without_owner(self, engine):
        engine.execution_engine.adapter = _rithmic_adapter_for_reconnect_test()
        engine._rithmic_runtime.runtime_recovery = None
        engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock()
        engine._market_processing_lock = MagicMock()
        engine._ops_command_lock = MagicMock()

        with pytest.raises(
            RuntimeError,
            match="rithmic_runtime_reconciliation_unavailable",
        ):
            engine._run_rithmic_runtime_reconciliation_once()

        engine.execution_engine.reconcile_rithmic_owned_orders.assert_not_called()
        engine._market_processing_lock.__enter__.assert_not_called()
        engine._ops_command_lock.__enter__.assert_not_called()

    @pytest.mark.parametrize("still_alive", (False, True))
    def test_stop_order_event_stream_is_bounded_and_reports_completion(
        self,
        engine,
        still_alive,
    ):
        thread = MagicMock()
        thread.is_alive.side_effect = [True, still_alive]
        engine.order_event_thread = thread
        engine._order_event_stop = MagicMock()

        assert engine._stop_exchange_order_event_stream(timeout=30.0) is not still_alive

        engine._order_event_stop.set.assert_called_once_with()
        thread.join.assert_called_once_with(timeout=30.0)

    def test_authoritative_rithmic_summary_publishes_exact_account_balance(
        self,
        engine,
    ):
        engine._rithmic_recovery_account_id = "ACCOUNT"
        engine.account_service.replace_authoritative_balance = MagicMock()
        engine.execution_engine.clock.now = MagicMock(return_value=1704067201)
        _attach_rithmic_ledger_recovery(engine)

        engine._apply_rithmic_authoritative_account_summary(
            _authoritative_rithmic_summary()
        )

        engine.account_service.replace_authoritative_balance.assert_called_once_with(
            venue="rithmic",
            account_id="ACCOUNT",
            currency="USD",
            balance=Decimal("50000.25"),
            day_pnl=Decimal("0"),
            observed_at_ms=1704067201000,
            source_timestamp_ms=1704067200000,
        )

    def test_authoritative_rithmic_summary_delegates_to_venue_owner(
        self,
        engine,
    ):
        summary = object()
        service = MagicMock()
        engine._rithmic_runtime.ledger_recovery = service

        engine._apply_rithmic_authoritative_account_summary(summary)

        service.publish_authoritative_summary.assert_called_once_with(summary)

    def test_authoritative_rithmic_summary_preserves_service_exception_identity(
        self,
        engine,
    ):
        error = RuntimeError("projection failed")
        service = MagicMock()
        service.publish_authoritative_summary.side_effect = error
        engine._rithmic_runtime.ledger_recovery = service

        with pytest.raises(RuntimeError) as caught:
            engine._apply_rithmic_authoritative_account_summary({})

        assert caught.value is error

    def test_authoritative_rithmic_summary_rejects_wrong_account(self, engine):
        engine._rithmic_recovery_account_id = "ACCOUNT"
        engine.account_service.replace_authoritative_balance = MagicMock()
        _attach_rithmic_ledger_recovery(engine)

        with pytest.raises(
            RuntimeError,
            match="rithmic_account_summary_identity_mismatch",
        ):
            engine._apply_rithmic_authoritative_account_summary(
                _authoritative_rithmic_summary(account_id="OTHER")
            )

        engine.account_service.replace_authoritative_balance.assert_not_called()

    def test_periodic_rithmic_reconciliation_refreshes_then_reopens_gate(
        self,
        engine,
    ):
        adapter = _rithmic_adapter_for_reconnect_test()
        engine.execution_engine.adapter = adapter
        engine._rithmic_recovery_profile = "test"
        engine._rithmic_recovery_account_id = "ACCOUNT"
        engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
        summary = _authoritative_rithmic_summary()
        engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
            return_value=summary
        )
        engine._apply_rithmic_authoritative_account_summary = MagicMock()
        engine._start_exchange_order_event_stream = MagicMock()
        engine.execution_engine.resume_after_reconcile = MagicMock()
        engine._lockdown_for_rithmic_order_drift = MagicMock()
        _install_rithmic_runtime_recovery_service(engine, adapter)

        assert engine._run_rithmic_runtime_reconciliation_once() is True

        engine.execution_engine.halt_for_reconcile.assert_called_once_with(timeout=30.0)
        engine.execution_engine.reconcile_rithmic_owned_orders.assert_called_once_with(
            "test",
            "ACCOUNT",
        )
        engine._apply_rithmic_authoritative_account_summary.assert_called_once_with(
            summary
        )
        engine._start_exchange_order_event_stream.assert_called_once_with()
        engine.execution_engine.resume_after_reconcile.assert_called_once_with()
        engine._lockdown_for_rithmic_order_drift.assert_not_called()

    def test_periodic_rithmic_reconciliation_does_not_restart_after_takeover(
        self,
        engine,
    ):
        adapter = _rithmic_adapter_for_reconnect_test()
        adapter.close = MagicMock()
        engine.execution_engine.adapter = adapter
        engine._rithmic_recovery_profile = "test"
        engine._rithmic_recovery_account_id = "ACCOUNT"
        engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
        owns_service = True

        def assert_leadership():
            if not owns_service:
                raise RuntimeError("leadership lost")

        def reconcile_then_lose_leadership(*_args):
            nonlocal owns_service
            owns_service = False
            return _authoritative_rithmic_summary()

        engine._leadership_guard = assert_leadership
        engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
            side_effect=reconcile_then_lose_leadership
        )
        engine._apply_rithmic_authoritative_account_summary = MagicMock()
        engine._start_exchange_order_event_stream = MagicMock()
        engine.execution_engine.resume_after_reconcile = MagicMock()
        _install_rithmic_runtime_recovery_service(engine, adapter)

        with pytest.raises(RuntimeError, match="leadership lost"):
            engine._run_rithmic_runtime_reconciliation_once()

        engine._start_exchange_order_event_stream.assert_not_called()
        engine.execution_engine.resume_after_reconcile.assert_not_called()
        assert adapter.close.call_count >= 2
        assert engine._kill_switch_halted is True

    def test_periodic_rithmic_reconciliation_holds_market_delivery_until_complete(
        self,
        engine,
    ):
        adapter = _rithmic_adapter_for_reconnect_test()
        engine.execution_engine.adapter = adapter
        engine._rithmic_recovery_profile = "test"
        engine._rithmic_recovery_account_id = "ACCOUNT"
        engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
        engine._apply_rithmic_authoritative_account_summary = MagicMock()
        engine._start_exchange_order_event_stream = MagicMock()
        engine.execution_engine.resume_after_reconcile = MagicMock()
        engine.execution_engine.process_market_data = MagicMock()
        engine._signal_processor.on_candle = MagicMock()
        _install_rithmic_runtime_recovery_service(engine, adapter)
        reconcile_started = threading.Event()
        allow_reconcile = threading.Event()

        def reconcile(*_args):
            reconcile_started.set()
            assert allow_reconcile.wait(timeout=5.0)
            return _authoritative_rithmic_summary()

        engine.execution_engine.reconcile_rithmic_owned_orders = reconcile
        reconcile_thread = threading.Thread(
            target=engine._run_rithmic_runtime_reconciliation_once
        )
        reconcile_thread.start()
        assert reconcile_started.wait(timeout=5.0)

        market_done = threading.Event()

        def process_market_data():
            engine.on_market_data(_make_candle())
            market_done.set()

        market_thread = threading.Thread(target=process_market_data)
        market_thread.start()
        assert not market_done.wait(timeout=0.05)
        engine.execution_engine.process_market_data.assert_not_called()
        engine._signal_processor.on_candle.assert_not_called()

        allow_reconcile.set()
        reconcile_thread.join(timeout=5.0)
        market_thread.join(timeout=5.0)

        assert not reconcile_thread.is_alive()
        assert not market_thread.is_alive()
        assert market_done.is_set()
        engine.execution_engine.process_market_data.assert_called_once()
        engine._signal_processor.on_candle.assert_called_once()

    def test_periodic_reconciliation_wait_does_not_block_kill_switch(
        self,
        engine,
    ):
        adapter = _rithmic_adapter_for_reconnect_test()
        engine.execution_engine.adapter = adapter
        engine._rithmic_recovery_profile = "test"
        engine._rithmic_recovery_account_id = "ACCOUNT"
        engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
        engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
            return_value=_authoritative_rithmic_summary()
        )
        engine._apply_rithmic_authoritative_account_summary = MagicMock()
        engine._start_exchange_order_event_stream = MagicMock()
        engine.execution_engine.resume_after_reconcile = MagicMock()
        engine._run_ops_kill_switch = MagicMock(return_value=_kill_switch_result())
        engine.ops_safety.persist_kill_switch_state = MagicMock()
        _install_rithmic_runtime_recovery_service(engine, adapter)

        market_started = threading.Event()
        release_market = threading.Event()
        reconcile_waiting = threading.Event()
        kill_halted = threading.Event()

        class ObservableLock:
            def __init__(self):
                self._lock = threading.Lock()

            def __enter__(self):
                if threading.current_thread().name == "periodic-reconcile":
                    reconcile_waiting.set()
                self._lock.acquire()
                return self

            def __exit__(self, *_args):
                self._lock.release()

        engine._market_processing_lock = ObservableLock()

        def hold_market_callback(_candle):
            market_started.set()
            assert release_market.wait(timeout=5.0)

        engine.execution_engine.process_market_data = MagicMock(
            side_effect=hold_market_callback
        )
        engine._signal_processor.on_candle = MagicMock()
        engine._halt_for_kill_switch = MagicMock(side_effect=lambda: kill_halted.set())

        market_thread = threading.Thread(
            name="market-callback",
            target=lambda: engine.on_market_data(_make_candle()),
        )
        reconcile_thread = threading.Thread(
            name="periodic-reconcile",
            target=engine._run_rithmic_runtime_reconciliation_once,
        )
        kill_thread = threading.Thread(
            name="kill-switch",
            target=lambda: engine._handle_command(
                {"command": "KILL_SWITCH", "params": {"actor": "operator"}}
            ),
        )

        market_thread.start()
        assert market_started.wait(timeout=5.0)
        reconcile_thread.start()
        assert reconcile_waiting.wait(timeout=5.0)
        kill_thread.start()
        try:
            assert kill_halted.wait(timeout=1.0)
        finally:
            release_market.set()
            market_thread.join(timeout=5.0)
            kill_thread.join(timeout=5.0)
            reconcile_thread.join(timeout=5.0)

        assert not market_thread.is_alive()
        assert not kill_thread.is_alive()
        assert not reconcile_thread.is_alive()
        engine._halt_for_kill_switch.assert_called_once_with()

    @pytest.mark.parametrize(
        ("summary", "apply_error", "expected_reason"),
        [
            (
                {"recoverable_count": 1, "auto_resume_safe": False},
                None,
                "rithmic_runtime_reconciliation_unresolved",
            ),
            (
                _authoritative_rithmic_summary(),
                RuntimeError("bad balance"),
                "rithmic_runtime_reconciliation_failed",
            ),
        ],
    )
    def test_periodic_rithmic_reconciliation_failure_enters_lockdown(
        self,
        engine,
        summary,
        apply_error,
        expected_reason,
    ):
        adapter = _rithmic_adapter_for_reconnect_test()
        engine.execution_engine.adapter = adapter
        engine._rithmic_recovery_profile = "test"
        engine._rithmic_recovery_account_id = "ACCOUNT"
        engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
        engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
            return_value=summary
        )
        engine._apply_rithmic_authoritative_account_summary = MagicMock(
            side_effect=apply_error
        )
        engine._start_exchange_order_event_stream = MagicMock()
        engine.execution_engine.resume_after_reconcile = MagicMock()
        engine._lockdown_for_rithmic_order_drift = MagicMock()
        _install_rithmic_runtime_recovery_service(engine, adapter)

        assert engine._run_rithmic_runtime_reconciliation_once() is False

        engine._start_exchange_order_event_stream.assert_called_once_with()
        engine.execution_engine.resume_after_reconcile.assert_not_called()
        engine._lockdown_for_rithmic_order_drift.assert_called_once_with(
            expected_reason
        )


class TestExchangeOrderEventThread:
    def test_rithmic_runtime_reconciliation_uses_authoritative_ledger(self):
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(),
        )

        assert (
            _is_runtime_reconciliation_enabled(
                adapter,
                {"mode": "live"},
            )
            is True
        )

    def test_rithmic_engine_requires_owned_order_audit(
        self,
        mock_db_session,
        mock_clock,
    ):
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(),
        )

        with patch("src.core.engine.create_redis_client", return_value=MagicMock()):
            with pytest.raises(
                ValueError,
                match="Rithmic live trading requires audit_external_orders",
            ):
                StrategyEngine(
                    db_session=mock_db_session,
                    clock=mock_clock,
                    adapter=adapter,
                )

    def test_rithmic_engine_rejects_backtest_identity(
        self,
        engine_factory,
    ):
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(),
        )

        with pytest.raises(
            ValueError,
            match="backtest mode requires SimulatedAdapter",
        ):
            engine_factory(
                adapter=adapter,
                audit_external_orders=True,
                is_backtest=True,
            )

    def test_rithmic_startup_stays_halted_when_recovery_is_not_safe(
        self,
        engine_factory,
    ):
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(),
        )
        engine = engine_factory(adapter=adapter, audit_external_orders=True)
        engine._check_system_state = MagicMock(return_value=False)
        engine._reconcile_recoverable_orders_on_startup = MagicMock(
            return_value={"auto_resume_safe": False}
        )
        engine._start_exchange_order_event_stream = MagicMock()
        engine._halt_for_kill_switch = MagicMock(
            side_effect=lambda: setattr(engine, "_kill_switch_halted", True)
        )
        for name in (
            "_start_command_listener",
            "_reconcile_balance",
            "_initialize_strategy_state_cache_on_startup",
            "_start_strategy_state_subscriber_on_startup",
            "_start_heartbeat",
            "_start_runtime_reconciliation",
            "scan_strategies",
            "_restore_active_strategies_on_startup",
        ):
            setattr(engine, name, MagicMock())

        engine.startup()

        assert engine._halt_for_kill_switch.call_count == 2
        engine._restore_active_strategies_on_startup.assert_not_called()
        assert engine._startup_lock_cause == "rithmic_reconciliation_blocked"

    def test_event_stream_starts_and_applies_events(self, engine):
        adapter = MagicMock()
        remote_event = MagicMock()
        engine.execution_engine.adapter = adapter
        engine.execution_engine.process_exchange_order_event = MagicMock()

        def poll_once():
            engine._order_event_stop.set()
            return remote_event

        adapter.poll_order_event.side_effect = poll_once

        class ImmediateThread:
            def __init__(self, *, target, name, daemon):
                self.target = target
                self.name = name
                self.daemon = daemon

            def start(self):
                self.target()

        with patch("src.core.engine.threading.Thread", ImmediateThread):
            engine._start_exchange_order_event_stream()

        adapter.start_order_event_stream.assert_called_once_with()
        engine.execution_engine.process_exchange_order_event.assert_called_once_with(
            remote_event
        )

    def test_queued_order_event_is_rejected_after_leadership_loss(self, engine):
        adapter = MagicMock()
        remote_event = MagicMock()
        owns_service = True

        def assert_leadership():
            if not owns_service:
                raise RuntimeError("leadership lost")

        def poll_then_lose_leadership():
            nonlocal owns_service
            owns_service = False
            return remote_event

        engine._leadership_guard = assert_leadership
        engine.execution_engine.adapter = adapter
        engine.execution_engine.process_exchange_order_event = MagicMock()
        adapter.poll_order_event.side_effect = poll_then_lose_leadership

        class ImmediateThread:
            def __init__(self, *, target, name, daemon):
                self.target = target

            def start(self):
                self.target()

        with patch("src.core.engine.threading.Thread", ImmediateThread):
            engine._start_exchange_order_event_stream()

        engine.execution_engine.process_exchange_order_event.assert_not_called()
        assert engine.running is False
        assert engine._kill_switch_halted is True

    def test_event_stream_start_failure_halts_before_propagating(self, engine):
        adapter = MagicMock()
        adapter.start_order_event_stream.side_effect = NetworkError("offline")
        engine.execution_engine.adapter = adapter
        engine._halt_for_kill_switch = MagicMock()

        with pytest.raises(NetworkError, match="offline"):
            engine._start_exchange_order_event_stream()

        engine._halt_for_kill_switch.assert_called_once_with()

    def test_rithmic_event_stream_start_delegates_only_to_venue_owner(self, engine):
        owner = MagicMock()
        engine._rithmic_runtime.order_event_stream = owner
        engine.execution_engine.adapter.start_order_event_stream = MagicMock()

        engine._start_exchange_order_event_stream()

        owner.start.assert_called_once_with()
        engine.execution_engine.adapter.start_order_event_stream.assert_not_called()

    def test_event_stream_failure_halts_local_submissions(self, engine):
        adapter = MagicMock()
        adapter.poll_order_event.side_effect = ExchangeError("invalid event")
        engine.execution_engine.adapter = adapter
        engine._halt_for_kill_switch = MagicMock()

        class ImmediateThread:
            def __init__(self, *, target, name, daemon):
                self.target = target

            def start(self):
                self.target()

        with patch("src.core.engine.threading.Thread", ImmediateThread):
            engine._start_exchange_order_event_stream()

        engine._halt_for_kill_switch.assert_called_once_with()

    @pytest.mark.parametrize(
        ("action", "requires_reconciliation"),
        [
            ("applied", False),
            ("unknown_order", True),
            ("unknown_status", True),
            ("unresolved_last_fill_without_cumulative_quantity", True),
            ("unresolved_local_fill_exceeds_exchange", True),
            ("unresolved_missing_fill_price", True),
            ("unresolved_exchange_fill_exceeds_order_quantity", True),
            ("unresolved_missing_terminal_fill_quantity", True),
            ("unresolved_terminal_fill_quantity_below_order_quantity", True),
            ("unresolved_protective_terminal_without_fill", True),
            ("unresolved_remote_actions_suppressed", True),
            ("unresolved_conditional_order_placement_failed", True),
            ("unresolved_protective_partial_fill_requires_resize", True),
            ("unresolved_linked_conditional_cancel_failed", True),
            ("unexpected_future_action", True),
            (None, True),
        ],
    )
    def test_rithmic_order_event_action_matrix_fails_closed(
        self,
        action,
        requires_reconciliation,
    ):
        result = {} if action is None else {"action": action}

        assert (
            RithmicOrderEventStreamService.requires_reconciliation(result)
            is requires_reconciliation
        )

    @pytest.mark.parametrize("flag", ["verification_blocked", "unresolved"])
    def test_rithmic_applied_event_with_unsafe_flag_fails_closed(self, flag):
        assert RithmicOrderEventStreamService.requires_reconciliation(
            {"action": "applied", flag: True}
        )

    def test_rithmic_unresolved_order_event_locks_down_and_keeps_streaming(
        self,
        engine_factory,
    ):
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=MagicMock()),
        )
        engine = engine_factory(adapter=adapter, audit_external_orders=True)
        remote_event = ExchangeOrderEvent(
            status="partially_filled",
            product_id="RITHMIC:NQ-202609",
            client_order_id="owned-order",
            exchange_order_id="basket-1",
        )
        poll_count = 0

        def poll_event():
            nonlocal poll_count
            poll_count += 1
            if poll_count == 1:
                return remote_event
            engine._order_event_stop.set()
            return None

        adapter.poll_order_event = MagicMock(side_effect=poll_event)
        engine.execution_engine.process_exchange_order_event = MagicMock(
            return_value={"action": "unresolved_missing_fill_price"}
        )
        engine._reconcile_owned_orders_on_reconnect = MagicMock(return_value=True)
        engine._rithmic_runtime.external_order_drift = MagicMock()

        class ImmediateThread:
            def __init__(self, *, target, name, daemon):
                self.target = target

            def start(self):
                self.target()

        with patch(
            "src.core.adapters.rithmic_order_event_stream.threading.Thread",
            ImmediateThread,
        ):
            engine._start_exchange_order_event_stream()

        assert adapter.poll_order_event.call_count == 2
        engine._rithmic_runtime.external_order_drift.detect.assert_called_once_with(
            "rithmic_order_event_requires_reconciliation: "
            "action=unresolved_missing_fill_price "
            "product_id=RITHMIC:NQ-202609 client_order_id=owned-order "
            "exchange_order_id=basket-1"
        )

    @pytest.mark.parametrize(
        "poll_error",
        [
            None,
            RithmicUnmappedOrderEvent(
                account_id="ACCOUNT",
                exchange="CME",
                symbol="ESZ6",
            ),
        ],
        ids=["configured-external-order", "unmapped-external-order"],
    )
    def test_rithmic_external_order_locks_down_without_stopping_stream(
        self,
        engine_factory,
        poll_error,
    ):
        adapter = RithmicExchangeAdapter(
            profile="test",
            account_id="ACCOUNT",
            instruments={
                "RITHMIC:NQ-202609": {
                    "exchange": "CME",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                }
            },
            client_factory=MagicMock(return_value=MagicMock()),
        )
        engine = engine_factory(adapter=adapter, audit_external_orders=True)
        external_event = ExchangeOrderEvent(
            status="open",
            product_id="RITHMIC:NQ-202609",
            client_order_id="manual-order",
            exchange_order_id="basket-manual",
            raw={"account_id": "ACCOUNT", "exchange": "CME", "symbol": "NQU6"},
        )

        poll_count = 0

        def poll_event():
            nonlocal poll_count
            poll_count += 1
            if poll_count == 1:
                if poll_error is not None:
                    raise poll_error
                return external_event
            engine._order_event_stop.set()
            return None

        adapter.poll_order_event = MagicMock(side_effect=poll_event)
        engine.execution_engine.process_exchange_order_event = MagicMock(
            return_value={"action": "unknown_order"}
        )
        engine._reconcile_owned_orders_on_reconnect = MagicMock(return_value=True)
        engine.ops_safety.persist_kill_switch_state = MagicMock()

        class ImmediateThread:
            def __init__(self, *, target, name, daemon):
                self.target = target

            def start(self):
                self.target()

        with patch(
            "src.core.adapters.rithmic_order_event_stream.threading.Thread",
            ImmediateThread,
        ):
            engine._start_exchange_order_event_stream()

        assert adapter.poll_order_event.call_count == 2
        assert engine._rithmic_runtime.external_order_drift is not None
        assert engine._rithmic_runtime.external_order_drift.pending is True
        assert engine._kill_switch_halted is True
        engine.ops_safety.persist_kill_switch_state.assert_called_once()
        engine.redis_client.set.assert_called_with(
            engine._system_state_key,
            "LOCKDOWN",
        )
        if poll_error is None:
            engine.execution_engine.process_exchange_order_event.assert_called_once_with(
                external_event
            )
        else:
            engine.execution_engine.process_exchange_order_event.assert_not_called()

    def test_rithmic_clear_delegates_to_venue_owner(self, engine):
        sentinel = (False, 17)
        owner = MagicMock()
        owner.prepare.return_value = sentinel
        engine._rithmic_runtime.kill_switch_clear_preparation = owner

        assert engine._prepare_rithmic_kill_switch_clear() is sentinel

        owner.prepare.assert_called_once_with()

    def test_non_rithmic_clear_does_not_construct_or_call_rithmic_owner(self, engine):
        assert engine._rithmic_runtime.kill_switch_clear_preparation is None

        assert engine._prepare_rithmic_kill_switch_clear() == (True, None)

    @pytest.mark.parametrize(
        ("prepared", "cleared"),
        [
            ((False, None), None),
            ((True, 0), False),
            ((True, 0), True),
        ],
    )
    def test_external_order_drift_blocks_clear_until_both_checks_pass(
        self,
        engine,
        prepared,
        cleared,
    ):
        engine._kill_switch_halted = True
        engine._prepare_rithmic_kill_switch_clear = MagicMock(return_value=prepared)
        engine._rithmic_runtime.external_order_drift = MagicMock()
        engine.ops_safety.clear_kill_switch = MagicMock(
            return_value={"cleared": cleared, "reason": "still_open"}
        )

        engine._handle_command({"command": "CLEAR_KILL_SWITCH", "params": {}})

        if prepared[0]:
            engine.ops_safety.clear_kill_switch.assert_called_once()
            engine._rithmic_runtime.external_order_drift.finalize_clear.assert_called_once_with(
                prepared_generation=0,
                clear_succeeded=bool(cleared),
            )
        else:
            engine.ops_safety.clear_kill_switch.assert_not_called()
            engine._rithmic_runtime.external_order_drift.finalize_clear.assert_not_called()

    def test_rithmic_clear_does_not_persist_or_resume_after_leadership_loss(
        self,
        engine,
    ):
        owns_service = True

        def assert_leadership():
            if not owns_service:
                raise RuntimeError("leadership lost")

        def reconcile_then_lose_leadership():
            nonlocal owns_service
            owns_service = False
            return True, 0

        engine._leadership_guard = assert_leadership
        engine._kill_switch_halted = True
        engine._prepare_rithmic_kill_switch_clear = MagicMock(
            side_effect=reconcile_then_lose_leadership
        )
        engine.ops_safety.clear_kill_switch = MagicMock()
        engine.ops_safety.persist_kill_switch_state = MagicMock()
        engine.execution_engine.resume_after_reconcile = MagicMock()

        engine._handle_command({"command": "CLEAR_KILL_SWITCH", "params": {}})

        engine.ops_safety.clear_kill_switch.assert_not_called()
        engine.ops_safety.persist_kill_switch_state.assert_not_called()
        assert call(engine._system_state_key, SYSTEM_STATE_OK) not in (
            engine.redis_client.set.call_args_list
        )
        engine.execution_engine.resume_after_reconcile.assert_not_called()
        assert engine._kill_switch_halted is True

    def test_non_rithmic_clear_does_not_run_ledger_reconciliation(self, engine):
        engine._kill_switch_halted = True
        engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock()
        engine.execution_engine.halt_for_reconcile = MagicMock()
        engine.execution_engine.resume_after_reconcile = MagicMock()
        engine.ops_safety.clear_kill_switch = MagicMock(
            return_value={"cleared": True, "reason": "cleared"}
        )

        engine._handle_command({"command": "CLEAR_KILL_SWITCH", "params": {}})

        assert engine._kill_switch_halted is False
        engine.execution_engine.reconcile_rithmic_owned_orders.assert_not_called()
        engine.execution_engine.halt_for_reconcile.assert_not_called()
        engine.execution_engine.resume_after_reconcile.assert_not_called()

    def test_rithmic_clear_reasserts_lockdown_when_new_drift_is_detected(
        self,
        engine_factory,
    ):
        engine = engine_factory(
            adapter=_rithmic_adapter_for_reconnect_test(),
            audit_external_orders=True,
        )
        owner = engine._rithmic_runtime.external_order_drift
        assert owner is not None
        owner.detect("before clear")
        prepared_generation = owner.current_generation()
        engine._prepare_rithmic_kill_switch_clear = MagicMock(
            return_value=(True, prepared_generation)
        )
        engine.execution_engine.resume_after_reconcile = MagicMock()
        engine.ops_safety.persist_kill_switch_state = MagicMock()
        engine.redis_client.set.reset_mock()

        def clear_kill_switch(*, persist_clear):
            persist_clear()
            assert engine._rithmic_runtime.order_event_stream is not None
            engine._rithmic_runtime.order_event_stream._lockdown(
                "rithmic_external_order_detected: account_id=ACCOUNT "
                "exchange=CME symbol=NQU6 client_order_id=unknown "
                "exchange_order_id=manual-order"
            )
            return {"cleared": True, "reason": "cleared"}

        engine.ops_safety.clear_kill_switch = MagicMock(side_effect=clear_kill_switch)

        engine._handle_command({"command": "CLEAR_KILL_SWITCH", "params": {}})

        assert owner.current_generation() == prepared_generation + 1
        assert owner.pending is True
        assert engine._kill_switch_halted is True
        assert engine.redis_client.set.call_args_list[-1] == call(
            engine._system_state_key,
            "LOCKDOWN",
        )
        assert engine.ops_safety.persist_kill_switch_state.call_args_list[-1] == call(
            "LOCKDOWN",
            actor="rithmic_order_stream",
            reason="rithmic_external_order_detected_during_clear",
        )
        engine.execution_engine.resume_after_reconcile.assert_called_once_with()

    def test_external_order_detection_delegates_to_rithmic_owner(self, engine):
        engine._rithmic_runtime.external_order_drift = MagicMock()
        engine._rithmic_runtime.order_event_stream = MagicMock()
        engine._rithmic_runtime.order_event_stream._lockdown = (
            engine._lockdown_for_rithmic_order_drift
        )

        engine._rithmic_runtime.order_event_stream._lockdown(
            "rithmic_external_order_detected: "
            "account_id=ACCOUNT exchange=CME symbol=NQU6 "
            "client_order_id=unknown exchange_order_id=unknown"
        )

        engine._rithmic_runtime.external_order_drift.detect.assert_called_once_with(
            "rithmic_external_order_detected: "
            "account_id=ACCOUNT exchange=CME symbol=NQU6 "
            "client_order_id=unknown exchange_order_id=unknown"
        )

    def test_rithmic_clear_releases_reconcile_gate_after_final_clean_decision(
        self,
        engine_factory,
    ):
        engine = engine_factory(
            adapter=_rithmic_adapter_for_reconnect_test(),
            audit_external_orders=True,
        )
        owner = engine._rithmic_runtime.external_order_drift
        assert owner is not None
        owner.detect("before clear")
        prepared_generation = owner.current_generation()
        engine._prepare_rithmic_kill_switch_clear = MagicMock(
            return_value=(True, prepared_generation)
        )
        engine.ops_safety.clear_kill_switch = MagicMock(
            return_value={"cleared": True, "reason": "cleared"}
        )
        engine.execution_engine.resume_after_reconcile = MagicMock(
            side_effect=lambda: (
                engine._kill_switch_halted is False
                or pytest.fail("reconcile gate released before clear decision")
            )
        )

        engine._handle_command({"command": "CLEAR_KILL_SWITCH", "params": {}})

        assert owner.pending is False
        assert engine._kill_switch_halted is False
        engine.execution_engine.resume_after_reconcile.assert_called_once_with()

    def test_rithmic_clear_exception_releases_only_reconcile_gate(
        self,
        engine_factory,
    ):
        engine = engine_factory(
            adapter=_rithmic_adapter_for_reconnect_test(),
            audit_external_orders=True,
        )
        owner = engine._rithmic_runtime.external_order_drift
        assert owner is not None
        owner.detect("before clear")
        prepared_generation = owner.current_generation()
        engine._prepare_rithmic_kill_switch_clear = MagicMock(
            return_value=(True, prepared_generation)
        )
        engine.ops_safety.persist_kill_switch_state = MagicMock()
        engine.redis_client.set.reset_mock()

        def fail_after_partial_clear(*, persist_clear):
            persist_clear()
            assert engine._rithmic_runtime.order_event_stream is not None
            engine._rithmic_runtime.order_event_stream._lockdown(
                "rithmic_external_order_detected: account_id=ACCOUNT "
                "exchange=CME symbol=NQU6 client_order_id=unknown "
                "exchange_order_id=unknown"
            )
            raise RuntimeError("database unavailable")

        engine.ops_safety.clear_kill_switch = MagicMock(
            side_effect=fail_after_partial_clear
        )
        engine.execution_engine.resume_after_reconcile = MagicMock()

        engine._handle_command({"command": "CLEAR_KILL_SWITCH", "params": {}})

        assert owner.pending is True
        assert engine._kill_switch_halted is True
        assert engine.redis_client.set.call_args_list[-1] == call(
            engine._system_state_key,
            "LOCKDOWN",
        )
        assert engine.ops_safety.persist_kill_switch_state.call_args_list[-1] == call(
            "LOCKDOWN",
            actor="rithmic_order_stream",
            reason="rithmic_external_order_detected_during_clear",
        )
        engine.execution_engine.resume_after_reconcile.assert_called_once_with()


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


def _rithmic_adapter_for_reconnect_test(client=None):
    return RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments={
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        client_factory=MagicMock(return_value=client or MagicMock()),
    )


def _install_rithmic_order_reconnect_service(
    engine: StrategyEngine,
    adapter: RithmicExchangeAdapter,
) -> RithmicOrderReconnectService:
    service = RithmicOrderReconnectService(
        adapter=adapter,
        profile=engine._rithmic_recovery_profile or "",
        account_id=engine._rithmic_recovery_account_id,
        audit_external_orders=lambda: engine.execution_engine.audit_external_orders,
        reconcile_owned_orders=lambda profile, account_id: (
            engine.execution_engine.reconcile_rithmic_owned_orders(
                profile,
                account_id,
            )
        ),
        publish_authoritative_summary=lambda summary: (
            engine._apply_rithmic_authoritative_account_summary(summary)
        ),
        halt_for_reconcile=lambda **values: (
            engine.execution_engine.halt_for_reconcile(**values)
        ),
        resume_after_reconcile=lambda: (
            engine.execution_engine.resume_after_reconcile()
        ),
        assert_runtime_leadership=engine._assert_runtime_leadership,
        logger=logging.getLogger("test.rithmic_order_reconnect"),
    )
    service.on_runtime_started()
    engine._rithmic_runtime.order_reconnect = service
    return service


def _install_rithmic_runtime_recovery_service(
    engine: StrategyEngine,
    adapter: RithmicExchangeAdapter,
) -> RithmicRuntimeRecoveryService:
    service = RithmicRuntimeRecoveryService(
        adapter=adapter,
        profile=engine._rithmic_recovery_profile or "",
        account_id=engine._rithmic_recovery_account_id,
        halt_for_reconcile=lambda **values: (
            engine.execution_engine.halt_for_reconcile(**values)
        ),
        stop_order_event_stream=lambda **values: (
            engine._stop_exchange_order_event_stream(**values)
        ),
        reconcile_owned_orders=lambda profile, account_id: (
            engine.execution_engine.reconcile_rithmic_owned_orders(
                profile,
                account_id,
            )
        ),
        publish_authoritative_summary=lambda summary: (
            engine._apply_rithmic_authoritative_account_summary(summary)
        ),
        assert_runtime_leadership=engine._assert_runtime_leadership,
        start_order_event_stream=lambda: engine._start_exchange_order_event_stream(),
        resume_after_reconcile=lambda: (
            engine.execution_engine.resume_after_reconcile()
        ),
        lockdown=lambda reason: engine._lockdown_for_rithmic_order_drift(reason),
        logger=logging.getLogger("test.rithmic_runtime_recovery"),
    )
    engine._rithmic_runtime.runtime_recovery = service
    return service


def _install_rithmic_strategy_exit_service(
    engine: StrategyEngine,
    adapter: RithmicExchangeAdapter,
) -> RithmicStrategyExitService:
    service = RithmicStrategyExitService(
        adapter=adapter,
        execution_engine=engine.execution_engine,
        account_service=engine.account_service,
        profile=engine._rithmic_recovery_profile or "",
        account_id=engine._rithmic_recovery_account_id,
        operation_gate=engine._rithmic_runtime.order_event_lifecycle,
        stop_order_event_stream=engine._stop_exchange_order_event_stream,
        assert_leadership=engine._assert_runtime_leadership,
        restart_order_stream=engine._start_exchange_order_event_stream,
        lockdown=engine._lockdown_for_rithmic_order_drift,
        logger=logging.getLogger("test.rithmic_strategy_exit"),
    )
    engine._rithmic_runtime.strategy_exit = service
    return service


def _install_rithmic_emergency_flatten_service(
    engine: StrategyEngine,
    adapter: RithmicExchangeAdapter,
) -> RithmicEmergencyFlattenService:
    service = RithmicEmergencyFlattenService(
        adapter=adapter,
        execution_engine=engine.execution_engine,
        account_service=engine.account_service,
        ops_safety=engine.ops_safety,
        profile=engine._rithmic_recovery_profile or "test",
        account_id=engine._rithmic_recovery_account_id or "ACCOUNT",
        operation_gate=engine._rithmic_runtime.order_event_lifecycle,
        stop_current_worker=engine._stop_exchange_order_event_stream,
        clear_polling_stop=engine._order_event_stop.clear,
        restart_generic_worker=engine._start_exchange_order_event_stream,
        run_when_submissions_drained=(
            engine.execution_engine.run_when_submissions_drained
        ),
        logger=logging.getLogger("test.rithmic_emergency_flatten"),
    )
    _install_rithmic_portfolio_exit_factory(engine, adapter, service)
    return service


def _install_rithmic_portfolio_exit_factory(
    engine: StrategyEngine,
    adapter: RithmicExchangeAdapter,
    emergency_flatten: RithmicEmergencyFlattenService | MagicMock | None = None,
) -> RithmicEmergencyFlattenService | MagicMock:
    if emergency_flatten is None:
        emergency_flatten = MagicMock()
    engine._rithmic_runtime.emergency_flatten = emergency_flatten
    engine._rithmic_runtime.is_rithmic_runtime = True
    engine._rithmic_runtime.profile = engine._rithmic_recovery_profile or "test"
    engine._rithmic_runtime.account_id = (
        engine._rithmic_recovery_account_id or "ACCOUNT"
    )

    def build(portfolio_id_for_sleeve):
        return build_rithmic_portfolio_exit_owner(
            adapter=adapter,
            execution_engine=engine.execution_engine,
            account_service=engine.account_service,
            profile=engine._rithmic_recovery_profile or "test",
            account_id=engine._rithmic_recovery_account_id or "ACCOUNT",
            operation_gate=engine._rithmic_runtime.order_event_lifecycle,
            stop_order_event_stream=engine._stop_exchange_order_event_stream,
            assert_leadership=engine._assert_runtime_leadership,
            restart_order_stream=engine._start_exchange_order_event_stream,
            lockdown=engine._lockdown_for_rithmic_order_drift,
            schedule_emergency_flatten=(
                emergency_flatten.schedule_portfolio_exit_compensation
            ),
            portfolio_id_for_sleeve=portfolio_id_for_sleeve,
        )

    engine._rithmic_runtime.portfolio_exit_factory = build
    return emergency_flatten


def _rithmic_emergency_snapshot(*, net_quantity=None, orders=None):
    positions = []
    if net_quantity is not None:
        positions.append(
            SimpleNamespace(
                exchange="CME",
                symbol="NQU6",
                net_quantity=str(net_quantity),
                average_open_fill_price="20000",
                open_pnl="0",
            )
        )
    return SimpleNamespace(
        account_id="ACCOUNT",
        positions=positions,
        orders=orders or [],
    )


def _kill_switch_result(**overrides):
    result = {
        "cancelled_orders": 0,
        "cancel_failures": [],
        "flattened_positions": 1,
        "flatten_pending": [],
        "flatten_failures": [],
        "recovery_failures": [],
        "already_flat": False,
        "drain_timeout": False,
    }
    result.update(overrides)
    return result


@pytest.mark.parametrize(
    "missing_key",
    [
        "cancelled_orders",
        "cancel_failures",
        "flattened_positions",
        "flatten_pending",
        "flatten_failures",
        "recovery_failures",
        "already_flat",
        "drain_timeout",
    ],
)
def test_kill_switch_completion_requires_full_schema(missing_key):
    result = _kill_switch_result()
    result.pop(missing_key)

    assert (
        _kill_switch_result_is_complete(
            result,
            authoritative_required=False,
        )
        is False
    )


@pytest.mark.parametrize(
    "failure_key",
    [
        "cancel_failures",
        "flatten_pending",
        "flatten_failures",
        "recovery_failures",
    ],
)
def test_kill_switch_completion_rejects_each_failure_list(failure_key):
    result = _kill_switch_result(**{failure_key: [{"reason": "incomplete"}]})

    assert (
        _kill_switch_result_is_complete(
            result,
            authoritative_required=False,
        )
        is False
    )


@pytest.mark.parametrize(
    ("result", "authoritative_required", "expected"),
    [
        (_kill_switch_result(), False, True),
        (_kill_switch_result(drain_timeout=True), False, False),
        (_kill_switch_result(), True, False),
        (
            _kill_switch_result(authoritative_flatten_verified=False),
            True,
            False,
        ),
        (
            _kill_switch_result(authoritative_flatten_verified=True),
            True,
            True,
        ),
    ],
)
def test_kill_switch_completion_matrix(
    result,
    authoritative_required,
    expected,
):
    assert (
        _kill_switch_result_is_complete(
            result,
            authoritative_required=authoritative_required,
        )
        is expected
    )


@pytest.mark.parametrize("remote_quantity", ["1", "0.5"])
def test_rithmic_strategy_exit_uses_native_exit_and_verifies_flat(
    engine,
    remote_quantity,
):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.order_event_thread = None
    engine.execution_engine.list_recoverable_client_orders = MagicMock(return_value=[])
    engine.execution_engine.order_manager.repo.list_orders_by_statuses = MagicMock(
        return_value=[]
    )
    engine.execution_engine.exit_authoritative_position = MagicMock(return_value=True)
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": True}
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    engine._start_exchange_order_event_stream = MagicMock()
    engine.account_service.replace_positions_for_products = MagicMock()
    _install_rithmic_strategy_exit_service(engine, adapter)
    signal = Signal(
        strategy_id="strategy",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=[
            _rithmic_emergency_snapshot(net_quantity=remote_quantity),
            _rithmic_emergency_snapshot(),
        ],
    ):
        result = engine._run_rithmic_strategy_exit(signal, decision)

    assert result == {
        "status": "verified_flat",
        "cancelled_orders": 0,
        "product_id": "RITHMIC:NQ-202609",
    }
    engine.execution_engine.exit_authoritative_position.assert_called_once_with(
        "RITHMIC:NQ-202609",
        account_id="ACCOUNT",
    )
    assert engine.account_service.replace_positions_for_products.call_args_list == [
        call(
            [
                Position(
                    strategy_id="LIVE",
                    product_id="RITHMIC:NQ-202609",
                    side=PositionSide.LONG,
                    quantity=Decimal(remote_quantity),
                    entry_price=Decimal("20000"),
                    unrealized_pnl=Decimal("0"),
                )
            ],
            ("RITHMIC:NQ-202609",),
            timestamp_ms=engine.execution_engine.clock.now() * 1000,
        ),
        call(
            [],
            ("RITHMIC:NQ-202609",),
            timestamp_ms=engine.execution_engine.clock.now() * 1000,
        ),
    ]
    engine._start_exchange_order_event_stream.assert_called_once_with()


def test_rithmic_strategy_exit_engine_seam_delegates_exactly_once(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    result = {"status": "verified_flat"}
    engine._rithmic_runtime.strategy_exit = MagicMock()
    engine._rithmic_runtime.strategy_exit.execute.return_value = result
    signal = Signal(
        strategy_id="strategy",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    assert engine._run_rithmic_strategy_exit(signal, decision) is result

    engine._rithmic_runtime.strategy_exit.execute.assert_called_once_with(
        signal, decision
    )


def test_rithmic_strategy_exit_engine_seam_uses_runtime_facade(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    result = {"status": "verified_flat"}
    engine._rithmic_runtime.execute_strategy_exit = MagicMock(return_value=result)
    engine._rithmic_runtime.strategy_exit = MagicMock()
    signal = Signal(
        strategy_id="strategy",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    assert engine._run_rithmic_strategy_exit(signal, decision) is result

    engine._rithmic_runtime.execute_strategy_exit.assert_called_once_with(
        signal, decision
    )
    engine._rithmic_runtime.strategy_exit.execute.assert_not_called()


def test_non_rithmic_strategy_exit_rejects_before_runtime_facade(engine):
    engine._rithmic_runtime.execute_strategy_exit = MagicMock()

    with pytest.raises(
        RuntimeError,
        match="^authoritative_strategy_exit_requires_rithmic$",
    ):
        engine._run_rithmic_strategy_exit(MagicMock(), MagicMock())

    engine._rithmic_runtime.execute_strategy_exit.assert_not_called()


def test_strategy_exit_owner_stops_current_replacement_thread(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._assert_runtime_leadership = MagicMock()
    engine._start_exchange_order_event_stream = MagicMock()
    engine._lockdown_for_rithmic_order_drift = MagicMock()
    stale_thread = MagicMock()
    stale_thread.is_alive.return_value = True
    engine.order_event_thread = stale_thread
    _install_rithmic_strategy_exit_service(engine, adapter)
    current_thread = MagicMock()
    current_thread.is_alive.side_effect = [True, True]
    engine.order_event_thread = current_thread
    signal = Signal(
        strategy_id="strategy",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    with pytest.raises(
        RuntimeError,
        match="rithmic_strategy_exit_event_stream_stop_timeout",
    ):
        engine._run_rithmic_strategy_exit(signal, decision)

    stale_thread.is_alive.assert_not_called()
    stale_thread.join.assert_not_called()
    current_thread.join.assert_called_once_with(timeout=30.0)
    engine._start_exchange_order_event_stream.assert_not_called()
    engine._lockdown_for_rithmic_order_drift.assert_called_once_with(
        "rithmic_strategy_exit_requires_reconciliation:RuntimeError"
    )


def test_rithmic_exit_owners_share_one_order_event_lifecycle_gate(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    strategy_owner = _install_rithmic_strategy_exit_service(engine, adapter)
    flatten_owner = _install_rithmic_emergency_flatten_service(engine, adapter)

    assert isinstance(
        engine._rithmic_runtime.order_event_lifecycle,
        RithmicOrderEventLifecycleGate,
    )
    assert (
        strategy_owner.operation_gate is engine._rithmic_runtime.order_event_lifecycle
    )
    assert flatten_owner.operation_gate is engine._rithmic_runtime.order_event_lifecycle

    sentinel = {"status": "serialized_portfolio_exit"}
    engine._rithmic_runtime.order_event_lifecycle.run = MagicMock(return_value=sentinel)
    signal = Signal(
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    assert engine._run_rithmic_portfolio_exit(signal, decision, None) is sentinel
    engine._rithmic_runtime.order_event_lifecycle.run.assert_called_once()


def test_rithmic_portfolio_exit_engine_seam_uses_runtime_facade(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    result = {"status": "verified_reduced"}
    engine._rithmic_runtime.execute_portfolio_exit = MagicMock(return_value=result)
    engine._rithmic_runtime.emergency_flatten = MagicMock()
    engine._rithmic_runtime.portfolio_exit_factory = MagicMock()
    portfolio_id_for_sleeve = MagicMock(return_value="portfolio")
    engine._portfolio_coordinator.portfolio_id_for_sleeve = portfolio_id_for_sleeve
    signal = MagicMock()
    decision = MagicMock()
    candle = MagicMock()

    assert engine._run_rithmic_portfolio_exit(signal, decision, candle) is result

    engine._rithmic_runtime.execute_portfolio_exit.assert_called_once_with(
        signal,
        decision,
        candle,
        portfolio_id_for_sleeve,
    )
    engine._rithmic_runtime.portfolio_exit_factory.assert_not_called()


def test_non_rithmic_portfolio_exit_rejects_before_runtime_facade(engine):
    engine._rithmic_runtime.execute_portfolio_exit = MagicMock()

    with pytest.raises(
        RuntimeError,
        match="^authoritative_portfolio_exit_requires_rithmic$",
    ):
        engine._run_rithmic_portfolio_exit(MagicMock(), MagicMock(), None)

    engine._rithmic_runtime.execute_portfolio_exit.assert_not_called()


@pytest.mark.parametrize(
    ("profile", "account_id"),
    [(None, "ACCOUNT"), ("test", None)],
)
def test_portfolio_exit_delegates_missing_identity_error_from_runtime_facade(
    engine,
    profile,
    account_id,
):
    engine.execution_engine.adapter = _rithmic_adapter_for_reconnect_test()
    engine._rithmic_recovery_profile = profile
    engine._rithmic_recovery_account_id = account_id
    error = RuntimeError("rithmic_portfolio_exit_account_identity_missing")
    engine._rithmic_runtime.execute_portfolio_exit = MagicMock(side_effect=error)

    with pytest.raises(RuntimeError) as caught:
        engine._run_rithmic_portfolio_exit(MagicMock(), MagicMock(), None)

    assert caught.value is error
    engine._rithmic_runtime.execute_portfolio_exit.assert_called_once()


def test_invalid_portfolio_exit_fails_before_lifecycle_gate(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    _install_rithmic_portfolio_exit_factory(engine, adapter)
    engine._rithmic_runtime.order_event_lifecycle.run = MagicMock()
    signal = Signal(
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    with pytest.raises(
        ValueError,
        match="rithmic_portfolio_exit_requires_exit_signal",
    ):
        engine._run_rithmic_portfolio_exit(signal, decision, None)

    engine._rithmic_runtime.order_event_lifecycle.run.assert_not_called()


def test_portfolio_exit_stop_timeout_does_not_start_replacement_worker(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    _install_rithmic_portfolio_exit_factory(engine, adapter)
    engine._start_exchange_order_event_stream = MagicMock()
    engine._lockdown_for_rithmic_order_drift = MagicMock()
    current_thread = MagicMock()
    current_thread.is_alive.side_effect = [True, True]
    engine.order_event_thread = current_thread
    signal = Signal(
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    with pytest.raises(
        RuntimeError,
        match="rithmic_portfolio_exit_event_stream_stop_timeout",
    ):
        engine._run_rithmic_portfolio_exit(signal, decision, None)

    current_thread.join.assert_called_once_with(timeout=30.0)
    assert engine._order_event_stop.is_set()
    engine._start_exchange_order_event_stream.assert_not_called()
    engine._rithmic_runtime.emergency_flatten.schedule_portfolio_exit_compensation.assert_not_called()
    engine._lockdown_for_rithmic_order_drift.assert_called_once_with(
        "rithmic_portfolio_exit_requires_reconciliation:RuntimeError"
    )


def test_portfolio_exit_resolves_current_worker_after_acquiring_gate(engine_factory):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine = engine_factory(adapter=adapter, audit_external_orders=True)
    _install_rithmic_portfolio_exit_factory(engine, adapter)
    engine._start_exchange_order_event_stream = MagicMock()
    stale_thread = MagicMock()
    stale_thread.is_alive.return_value = True
    current_thread = MagicMock()
    current_thread.is_alive.side_effect = [True, True]
    engine.order_event_thread = stale_thread

    def replace_worker_then_enter(operation, *args):
        engine.order_event_thread = current_thread
        return operation(*args)

    engine._rithmic_runtime.order_event_lifecycle.run = MagicMock(
        side_effect=replace_worker_then_enter
    )
    signal = Signal(
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    with pytest.raises(
        RuntimeError,
        match="rithmic_portfolio_exit_event_stream_stop_timeout",
    ):
        engine._run_rithmic_portfolio_exit(signal, decision, None)

    stale_thread.is_alive.assert_not_called()
    stale_thread.join.assert_not_called()
    current_thread.join.assert_called_once_with(timeout=30.0)
    engine._start_exchange_order_event_stream.assert_not_called()


def test_rithmic_strategy_exit_cancels_protection_before_native_exit(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.order_event_thread = None
    protection = SimpleNamespace(
        id="protection-1",
        strategy_id="strategy",
        product_id="RITHMIC:NQ-202609",
        status=OrderStatus.SUBMITTED.value,
        client_order_id="strategy-execution-sl-1",
        exchange_id="rithmic",
        type="stop_loss",
    )
    engine.execution_engine.list_recoverable_client_orders = MagicMock(
        return_value=[protection]
    )
    engine.execution_engine.order_manager.repo.list_orders_by_statuses = MagicMock(
        return_value=[protection]
    )
    adapter.get_order_by_client_id = MagicMock(
        return_value=ExchangeOrderSnapshot(
            client_order_id=protection.client_order_id,
            exchange_order_id="basket-1",
            status="partially_filled",
        )
    )
    adapter.cancel_order = MagicMock(return_value=True)
    engine.execution_engine.exit_authoritative_position = MagicMock(return_value=True)
    operation_order = []
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        side_effect=lambda *_args, **_kwargs: (
            operation_order.append("reconcile") or {"auto_resume_safe": True}
        )
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    engine._start_exchange_order_event_stream = MagicMock()
    engine.account_service.replace_positions_for_products = MagicMock(
        side_effect=lambda *_args, **_kwargs: operation_order.append("replace")
    )
    _install_rithmic_strategy_exit_service(engine, adapter)
    signal = Signal(
        strategy_id="strategy",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    with patch(
        "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
        side_effect=[
            _rithmic_emergency_snapshot(net_quantity="0.5"),
            _rithmic_emergency_snapshot(),
        ],
    ):
        result = engine._run_rithmic_strategy_exit(signal, decision)

    assert result["cancelled_orders"] == 1
    adapter.cancel_order.assert_called_once_with(
        "basket-1",
        "RITHMIC:NQ-202609",
        order_type="stop_loss",
    )
    engine.execution_engine.exit_authoritative_position.assert_called_once_with(
        "RITHMIC:NQ-202609",
        account_id="ACCOUNT",
    )
    assert operation_order == [
        "reconcile",
        "replace",
        "reconcile",
        "replace",
    ]


@pytest.mark.parametrize("record_failure", [False, True])
def test_rithmic_portfolio_exit_reduces_only_owned_sleeve(
    engine,
    mock_strategy_class,
    record_failure,
):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.order_event_thread = None
    engine.execution_engine.list_recoverable_client_orders = MagicMock(return_value=[])
    engine.execution_engine.order_manager.repo.list_orders_by_statuses = MagicMock(
        return_value=[]
    )
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": True}
    )
    engine.execution_engine.submit_verified_net_reduction = MagicMock(
        return_value="exit-order"
    )
    engine.execution_engine.record_verified_net_reduction = MagicMock(
        side_effect=(
            RuntimeError("durable marker unavailable") if record_failure else None
        )
    )
    engine._start_exchange_order_event_stream = MagicMock()
    engine._lockdown_for_rithmic_order_drift = MagicMock()
    _install_rithmic_portfolio_exit_factory(engine, adapter)
    sleeve_position = Position(
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        side=PositionSide.LONG,
        quantity=Decimal("1"),
        entry_price=Decimal("20000"),
        unrealized_pnl=Decimal("0"),
    )
    engine.account_service.get_position_for_exit = MagicMock(
        side_effect=[
            sleeve_position,
            sleeve_position,
            None,
        ]
    )
    engine.account_service.replace_positions_for_products = MagicMock()
    definition = PortfolioDefinition(
        portfolio_id="portfolio_v1",
        product_id="RITHMIC:NQ-202609",
        sleeves=(
            PortfolioSleeve(
                mock_strategy_class(
                    "portfolio_v1.sleeve_a",
                    "RITHMIC:NQ-202609",
                )
            ),
            PortfolioSleeve(
                mock_strategy_class(
                    "portfolio_v1.sleeve_b",
                    "RITHMIC:NQ-202609",
                )
            ),
        ),
        max_gross_quantity=Decimal("5"),
    )
    engine._portfolio_coordinator.register(definition)
    signal = Signal(
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )
    candle = _make_candle(
        product_id="RITHMIC:NQ-202609",
        close=Decimal("20000"),
    )

    snapshot_loader = patch(
        "src.core.adapters.rithmic_portfolio_exit.load_rithmic_recovery_snapshot",
        side_effect=[
            _rithmic_emergency_snapshot(net_quantity="3"),
            _rithmic_emergency_snapshot(net_quantity="3"),
            _rithmic_emergency_snapshot(net_quantity="2"),
        ],
    )
    if record_failure:
        with (
            snapshot_loader,
            pytest.raises(
                RuntimeError,
                match="durable marker unavailable",
            ),
        ):
            engine._run_rithmic_portfolio_exit(
                signal,
                decision,
                candle,
            )
        engine._rithmic_runtime.emergency_flatten.schedule_portfolio_exit_compensation.assert_not_called()
        engine._lockdown_for_rithmic_order_drift.assert_called_once()
        return
    with snapshot_loader:
        result = engine._run_rithmic_portfolio_exit(signal, decision, candle)

    assert result == {
        "status": "verified_portfolio_reduction",
        "portfolio_id": "portfolio_v1",
        "strategy_id": "portfolio_v1.sleeve_a",
        "product_id": "RITHMIC:NQ-202609",
        "order_id": "exit-order",
        "cancelled_orders": 0,
        "preflight_remote_quantity": "3",
        "remaining_remote_quantity": "2",
    }
    engine.execution_engine.submit_verified_net_reduction.assert_called_once_with(
        signal,
        decision,
        candle=candle,
        preflight_remote_quantity=Decimal("3"),
    )
    engine.execution_engine.record_verified_net_reduction.assert_called_once_with(
        signal,
        "exit-order",
        remaining_remote_quantity=Decimal("2"),
    )
    engine.account_service.replace_positions_for_products.assert_not_called()
    engine._rithmic_runtime.emergency_flatten.schedule_portfolio_exit_compensation.assert_not_called()
    engine._lockdown_for_rithmic_order_drift.assert_not_called()


def test_rithmic_portfolio_exit_does_not_reduce_another_sleeve_after_own_fill(
    engine,
    mock_strategy_class,
):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.order_event_thread = None
    engine.execution_engine.list_recoverable_client_orders = MagicMock(return_value=[])
    engine.execution_engine.order_manager.repo.list_orders_by_statuses = MagicMock(
        return_value=[]
    )
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": True}
    )
    engine.execution_engine.submit_verified_net_reduction = MagicMock()
    engine.execution_engine.record_verified_net_reduction = MagicMock()
    engine._start_exchange_order_event_stream = MagicMock()
    engine._lockdown_for_rithmic_order_drift = MagicMock()
    _install_rithmic_portfolio_exit_factory(engine, adapter)
    engine.account_service.get_position_for_exit = MagicMock(
        side_effect=[
            Position(
                strategy_id="portfolio_v1.sleeve_a",
                product_id="RITHMIC:NQ-202609",
                side=PositionSide.LONG,
                quantity=Decimal("1"),
                entry_price=Decimal("20000"),
                unrealized_pnl=Decimal("0"),
            ),
            None,
        ]
    )
    definition = PortfolioDefinition(
        portfolio_id="portfolio_v1",
        product_id="RITHMIC:NQ-202609",
        sleeves=(
            PortfolioSleeve(
                mock_strategy_class(
                    "portfolio_v1.sleeve_a",
                    "RITHMIC:NQ-202609",
                )
            ),
            PortfolioSleeve(
                mock_strategy_class(
                    "portfolio_v1.sleeve_b",
                    "RITHMIC:NQ-202609",
                )
            ),
        ),
        max_gross_quantity=Decimal("2"),
    )
    engine._portfolio_coordinator.register(definition)
    signal = Signal(
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    with (
        patch(
            "src.core.adapters.rithmic_portfolio_exit.load_rithmic_recovery_snapshot",
            return_value=_rithmic_emergency_snapshot(net_quantity="1"),
        ),
        pytest.raises(
            RuntimeError,
            match="rithmic_portfolio_exit_local_position_changed",
        ),
    ):
        engine._run_rithmic_portfolio_exit(
            signal,
            decision,
            _make_candle(
                product_id="RITHMIC:NQ-202609",
                close=Decimal("20000"),
            ),
        )

    engine.execution_engine.submit_verified_net_reduction.assert_not_called()
    engine.execution_engine.record_verified_net_reduction.assert_not_called()
    engine._lockdown_for_rithmic_order_drift.assert_called_once()


def test_rithmic_portfolio_exit_does_not_cancel_before_safe_preflight(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.cancel_order = MagicMock(return_value=True)
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.order_event_thread = None
    engine.execution_engine.list_recoverable_client_orders = MagicMock(return_value=[])
    protection = SimpleNamespace(
        id="stop-order",
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        status=OrderStatus.SUBMITTED.value,
        client_order_id="stop-client",
        exchange_order_id="stop-basket",
        type="stop_loss",
    )
    engine.execution_engine.order_manager.repo.list_orders_by_statuses = MagicMock(
        return_value=[protection]
    )
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": False}
    )
    engine.execution_engine.submit_verified_net_reduction = MagicMock()
    engine._start_exchange_order_event_stream = MagicMock()
    engine._lockdown_for_rithmic_order_drift = MagicMock()
    _install_rithmic_portfolio_exit_factory(engine, adapter)
    engine.account_service.get_position_for_exit = MagicMock()
    signal = Signal(
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    with (
        patch(
            "src.core.adapters.rithmic_portfolio_exit.load_rithmic_recovery_snapshot",
            return_value=_rithmic_emergency_snapshot(net_quantity="1"),
        ),
        pytest.raises(
            RuntimeError,
            match="rithmic_portfolio_exit_preflight_reconciliation_blocked",
        ),
    ):
        engine._run_rithmic_portfolio_exit(
            signal,
            decision,
            _make_candle(
                product_id="RITHMIC:NQ-202609",
                close=Decimal("20000"),
            ),
        )

    adapter.cancel_order.assert_not_called()
    engine.execution_engine.submit_verified_net_reduction.assert_not_called()
    engine.account_service.get_position_for_exit.assert_not_called()
    engine._rithmic_runtime.emergency_flatten.schedule_portfolio_exit_compensation.assert_not_called()


@pytest.mark.parametrize("schedule_fails", [False, True])
def test_rithmic_portfolio_exit_schedules_flatten_after_protection_mutation(
    engine,
    schedule_fails,
):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.get_order_by_client_id = MagicMock(
        return_value=SimpleNamespace(
            status="open",
            exchange_order_id="stop-basket",
        )
    )
    adapter.cancel_order = MagicMock(return_value=True)
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.order_event_thread = None
    engine.execution_engine.list_recoverable_client_orders = MagicMock(return_value=[])
    protection = SimpleNamespace(
        id="stop-order",
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        status=OrderStatus.SUBMITTED.value,
        client_order_id="stop-client",
        exchange_order_id="stop-basket",
        type="stop_loss",
    )
    engine.execution_engine.order_manager.repo.list_orders_by_statuses = MagicMock(
        return_value=[protection]
    )
    primary = RuntimeError("verification failed after protection mutation")
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        side_effect=[{"auto_resume_safe": True}, primary]
    )
    engine.execution_engine.submit_verified_net_reduction = MagicMock()
    lifecycle_calls: list[str] = []
    engine._start_exchange_order_event_stream = MagicMock(
        side_effect=lambda: lifecycle_calls.append("restart")
    )
    engine._lockdown_for_rithmic_order_drift = MagicMock()
    _install_rithmic_portfolio_exit_factory(engine, adapter)
    compensation_calls: list[str] = []
    if schedule_fails:
        engine._rithmic_runtime.emergency_flatten.schedule_portfolio_exit_compensation.side_effect = RuntimeError(
            "callback registration failed"
        )
    else:
        engine._rithmic_runtime.emergency_flatten.schedule_portfolio_exit_compensation.side_effect = (
            lambda reason: engine._rithmic_runtime.order_event_lifecycle.run(
                lambda: (
                    lifecycle_calls.append("compensation"),
                    compensation_calls.append(reason),
                )
            )
        )
    engine.account_service.get_position_for_exit = MagicMock(
        return_value=Position(
            strategy_id="portfolio_v1.sleeve_a",
            product_id="RITHMIC:NQ-202609",
            side=PositionSide.LONG,
            quantity=Decimal("1"),
            entry_price=Decimal("20000"),
            unrealized_pnl=Decimal("0"),
        )
    )
    signal = Signal(
        strategy_id="portfolio_v1.sleeve_a",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    errors: list[Exception] = []

    def run_exit() -> None:
        try:
            engine._run_rithmic_portfolio_exit(
                signal,
                decision,
                _make_candle(
                    product_id="RITHMIC:NQ-202609",
                    close=Decimal("20000"),
                ),
            )
        except Exception as error:
            errors.append(error)

    with patch(
        "src.core.adapters.rithmic_portfolio_exit.load_rithmic_recovery_snapshot",
        return_value=_rithmic_emergency_snapshot(net_quantity="1"),
    ):
        worker = threading.Thread(target=run_exit, daemon=True)
        worker.start()
        worker.join(timeout=1.0)

    assert not worker.is_alive(), "portfolio-exit compensation deadlocked"
    assert len(errors) == 1
    if schedule_fails:
        assert str(errors[0]) == ("rithmic_portfolio_exit_compensation_schedule_failed")
    else:
        assert errors[0] is primary

    adapter.cancel_order.assert_called_once_with(
        "stop-basket",
        "RITHMIC:NQ-202609",
        order_type="stop_loss",
    )
    engine.execution_engine.submit_verified_net_reduction.assert_not_called()
    engine._rithmic_runtime.emergency_flatten.schedule_portfolio_exit_compensation.assert_called_once_with(
        "rithmic_portfolio_exit_requires_reconciliation:RuntimeError"
    )
    assert compensation_calls == (
        []
        if schedule_fails
        else ["rithmic_portfolio_exit_requires_reconciliation:RuntimeError"]
    )
    assert lifecycle_calls == ["restart"] + ([] if schedule_fails else ["compensation"])
    expected_lockdowns = [
        call("rithmic_portfolio_exit_requires_reconciliation:RuntimeError")
    ]
    if schedule_fails:
        expected_lockdowns.append(
            call("rithmic_portfolio_exit_compensation_schedule_failed:RuntimeError")
        )
    assert engine._lockdown_for_rithmic_order_drift.call_args_list == expected_lockdowns

    if not schedule_fails:
        compensation_calls.clear()
        errors.clear()
        engine._rithmic_runtime.emergency_flatten.schedule_portfolio_exit_compensation.reset_mock()
        engine.execution_engine.reconcile_rithmic_owned_orders.side_effect = [
            {"auto_resume_safe": False} for _ in range(6)
        ]
        with patch(
            "src.core.adapters.rithmic_portfolio_exit.load_rithmic_recovery_snapshot",
            return_value=_rithmic_emergency_snapshot(net_quantity="1"),
        ):
            worker = threading.Thread(target=run_exit, daemon=True)
            worker.start()
            worker.join(timeout=1.0)

        assert not worker.is_alive()
        assert len(errors) == 1
        assert str(errors[0]) == (
            "rithmic_portfolio_exit_preflight_reconciliation_blocked"
        )
        engine._rithmic_runtime.emergency_flatten.schedule_portfolio_exit_compensation.assert_not_called()
        assert compensation_calls == []


def test_rithmic_strategy_exit_blocks_when_remote_order_remains_working(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.order_event_thread = None
    engine.execution_engine.list_recoverable_client_orders = MagicMock(return_value=[])
    engine.execution_engine.order_manager.repo.list_orders_by_statuses = MagicMock(
        return_value=[]
    )
    engine.execution_engine.exit_authoritative_position = MagicMock()
    engine._start_exchange_order_event_stream = MagicMock()
    engine._lockdown_for_rithmic_order_drift = MagicMock()
    _install_rithmic_strategy_exit_service(engine, adapter)
    signal = Signal(
        strategy_id="strategy",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=Decimal("1"),
        position_quantity=Decimal("1"),
    )

    with (
        patch(
            "src.core.adapters.rithmic_strategy_exit.load_rithmic_recovery_snapshot",
            return_value=_rithmic_emergency_snapshot(
                net_quantity="1",
                orders=[SimpleNamespace(basket_id="working-1")],
            ),
        ),
        pytest.raises(
            RuntimeError,
            match="rithmic_strategy_exit_working_orders_remain",
        ),
    ):
        engine._run_rithmic_strategy_exit(signal, decision)

    engine.execution_engine.exit_authoritative_position.assert_not_called()
    engine._lockdown_for_rithmic_order_drift.assert_called_once()
    engine._start_exchange_order_event_stream.assert_called_once_with()


@pytest.mark.parametrize("operation_id", [None, "operation-1"])
def test_non_rithmic_kill_switch_keeps_generic_ops_dispatch(engine, operation_id):
    engine.ops_safety.kill_switch = MagicMock(return_value=_kill_switch_result())
    engine._rithmic_runtime.execute_emergency_flatten = MagicMock()

    result = engine._run_ops_kill_switch(
        actor="ops",
        reason="drill",
        operation_id=operation_id,
    )

    expected = {"actor": "ops", "reason": "drill"}
    if operation_id is not None:
        expected["operation_id"] = operation_id
    engine.ops_safety.kill_switch.assert_called_once_with(**expected)
    engine._rithmic_runtime.execute_emergency_flatten.assert_not_called()
    assert result == _kill_switch_result()


def test_rithmic_kill_switch_delegates_exactly_once_to_venue_owner(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    expected = _kill_switch_result(authoritative_flatten_verified=True)
    engine._rithmic_runtime.emergency_flatten = MagicMock()
    engine._rithmic_runtime.emergency_flatten.execute.return_value = expected
    engine.ops_safety.kill_switch = MagicMock()

    result = engine._run_ops_kill_switch(
        actor="ops",
        reason="drill",
        operation_id="operation-1",
    )

    assert result is expected
    engine._rithmic_runtime.emergency_flatten.execute.assert_called_once_with(
        actor="ops",
        reason="drill",
        operation_id="operation-1",
    )
    engine.ops_safety.kill_switch.assert_not_called()


def test_rithmic_kill_switch_uses_runtime_execution_facade(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    expected = _kill_switch_result(authoritative_flatten_verified=True)
    engine._rithmic_runtime.execute_emergency_flatten = MagicMock(return_value=expected)
    engine._rithmic_runtime.emergency_flatten = MagicMock()
    engine.ops_safety.kill_switch = MagicMock()

    result = engine._run_ops_kill_switch(
        actor="ops",
        reason="drill",
        operation_id="operation-1",
    )

    assert result is expected
    engine._rithmic_runtime.execute_emergency_flatten.assert_called_once_with(
        actor="ops",
        reason="drill",
        operation_id="operation-1",
    )
    engine._rithmic_runtime.emergency_flatten.execute.assert_not_called()
    engine.ops_safety.kill_switch.assert_not_called()


def test_rithmic_kill_switch_uses_and_verifies_authoritative_positions(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.start_order_event_stream()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": True}
    )
    engine.account_service.replace_positions_for_products = MagicMock()
    engine.ops_safety.record_kill_switch_result = MagicMock()
    flatten_order = SimpleNamespace(exchange_id="RITHMIC")
    engine.execution_engine.list_recoverable_client_orders = MagicMock(
        side_effect=[[], [flatten_order]]
    )

    def flatten(**kwargs):
        positions = kwargs["position_loader"]()
        assert positions[0].side == PositionSide.LONG
        assert positions[0].quantity == Decimal("1")
        assert kwargs["account_id"] == "ACCOUNT"
        return _kill_switch_result()

    engine.ops_safety.kill_switch_with_authoritative_positions = MagicMock(
        side_effect=flatten
    )
    snapshots = [
        _rithmic_emergency_snapshot(net_quantity="1"),
        _rithmic_emergency_snapshot(),
    ]
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=snapshots,
    ) as snapshot_loader:
        result = engine._run_ops_kill_switch(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is True
    recorded = engine.ops_safety.record_kill_switch_result.call_args.kwargs["result"]
    assert recorded["authoritative_flatten_verified"] is True
    assert snapshot_loader.call_args_list[1].args[2] == [flatten_order]
    assert engine.execution_engine.reconcile_rithmic_owned_orders.call_count == 2
    engine.account_service.replace_positions_for_products.assert_called_once_with(
        [],
        ("RITHMIC:NQ-202609",),
        timestamp_ms=1704067200000,
    )
    engine._start_exchange_order_event_stream.assert_called_once_with()


def test_rithmic_kill_switch_real_path_uses_native_exit_not_market_submit(engine):
    order_client = MagicMock()
    order_client.exit_position.return_value = True
    adapter = _rithmic_adapter_for_reconnect_test(order_client)
    adapter.start_order_event_stream()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": True}
    )
    engine.account_service.replace_positions_for_products = MagicMock()
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=[
            _rithmic_emergency_snapshot(net_quantity="1"),
            _rithmic_emergency_snapshot(),
        ],
    ):
        result = engine._run_ops_kill_switch(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is True
    order_client.exit_position.assert_called_once_with("CME", "NQU6")
    order_client.submit.assert_not_called()


@pytest.mark.parametrize(
    ("post_quantity", "verified"),
    [(None, True), ("1", False)],
)
def test_rithmic_kill_switch_ambiguous_native_exit_never_resubmits(
    engine,
    post_quantity,
    verified,
):
    order_client = MagicMock()
    order_client.exit_position.side_effect = RuntimeError(
        "Rithmic exit-position result is ambiguous"
    )
    adapter = _rithmic_adapter_for_reconnect_test(order_client)
    adapter.start_order_event_stream()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": True}
    )
    engine.account_service.replace_positions_for_products = MagicMock()
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=[
            _rithmic_emergency_snapshot(net_quantity="1"),
            _rithmic_emergency_snapshot(net_quantity=post_quantity),
        ],
    ):
        result = engine._run_ops_kill_switch(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is verified
    order_client.exit_position.assert_called_once_with("CME", "NQU6")
    order_client.submit.assert_not_called()


def test_rithmic_kill_switch_does_not_snapshot_after_drain_timeout(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.ops_safety.record_kill_switch_result = MagicMock()
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock()
    engine.ops_safety.kill_switch_with_authoritative_positions = MagicMock(
        return_value=_kill_switch_result(
            flattened_positions=0,
            drain_timeout=True,
        )
    )
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot"
    ) as snapshot_loader:
        result = engine._run_ops_kill_switch(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is False
    recorded = engine.ops_safety.record_kill_switch_result.call_args.kwargs["result"]
    assert recorded["authoritative_flatten_verified"] is False
    snapshot_loader.assert_not_called()
    engine.execution_engine.reconcile_rithmic_owned_orders.assert_not_called()


def test_rithmic_kill_switch_audits_verification_failure_before_raising(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.start_order_event_stream()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.ops_safety.record_kill_switch_result = MagicMock()
    engine.ops_safety.kill_switch_with_authoritative_positions = MagicMock(
        side_effect=lambda **kwargs: kwargs["position_loader"]()
    )
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with (
        patch(
            "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
            side_effect=RuntimeError("ledger unavailable"),
        ),
        pytest.raises(RuntimeError, match="ledger unavailable"),
    ):
        engine._run_ops_kill_switch(actor="ops", reason="drill")

    recorded = engine.ops_safety.record_kill_switch_result.call_args.kwargs["result"]
    assert recorded["authoritative_flatten_verified"] is False
    assert recorded["flatten_failures"][-1]["reason"].endswith(":RuntimeError")
    engine._start_exchange_order_event_stream.assert_called_once_with()


def test_rithmic_kill_switch_audits_event_stream_stop_timeout(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine.order_event_thread = MagicMock()
    engine.order_event_thread.is_alive.return_value = True
    engine.ops_safety.record_kill_switch_result = MagicMock()
    engine.ops_safety.kill_switch_with_authoritative_positions = MagicMock()
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with pytest.raises(
        RuntimeError,
        match="rithmic_emergency_flatten_event_stream_stop_timeout",
    ):
        engine._run_ops_kill_switch(actor="ops", reason="drill")

    recorded = engine.ops_safety.record_kill_switch_result.call_args.kwargs["result"]
    assert recorded["authoritative_flatten_verified"] is False
    assert engine._order_event_stop.is_set() is False
    engine.ops_safety.kill_switch_with_authoritative_positions.assert_not_called()


def test_rithmic_emergency_flatten_stops_current_replacement_thread(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    stale_thread = MagicMock()
    stale_thread.is_alive.return_value = True
    engine.order_event_thread = stale_thread
    engine.ops_safety.record_kill_switch_result = MagicMock()
    _install_rithmic_emergency_flatten_service(engine, adapter)
    current_thread = MagicMock()
    current_thread.is_alive.side_effect = [True, True]
    engine.order_event_thread = current_thread

    with pytest.raises(
        RuntimeError,
        match="rithmic_emergency_flatten_event_stream_stop_timeout",
    ):
        engine._run_ops_kill_switch(actor="ops", reason="drill")

    stale_thread.is_alive.assert_not_called()
    stale_thread.join.assert_not_called()
    current_thread.join.assert_called_once_with(timeout=30.0)
    assert engine._order_event_stop.is_set() is False


def test_rithmic_kill_switch_retries_only_from_fresh_residual_position(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.start_order_event_stream()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        side_effect=[
            {"auto_resume_safe": False},
            {"auto_resume_safe": True},
            {"auto_resume_safe": False},
            {"auto_resume_safe": True},
        ]
    )
    engine.account_service.set_position(
        Position(
            strategy_id="strategy-a",
            product_id="RITHMIC:NQ-202609",
            side=PositionSide.LONG,
            quantity=Decimal("2"),
            entry_price=Decimal("20000"),
            unrealized_pnl=Decimal("0"),
        )
    )

    def flatten(**kwargs):
        assert kwargs["position_loader"]()
        return _kill_switch_result()

    engine.ops_safety.kill_switch_with_authoritative_positions = MagicMock(
        side_effect=flatten
    )
    snapshots = [
        _rithmic_emergency_snapshot(net_quantity="2"),
        _rithmic_emergency_snapshot(net_quantity="1"),
        _rithmic_emergency_snapshot(net_quantity="1"),
        _rithmic_emergency_snapshot(),
    ]
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=snapshots,
    ):
        result = engine._run_ops_kill_switch(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is True
    assert result["flattened_positions"] == 2
    assert engine.ops_safety.kill_switch_with_authoritative_positions.call_count == 2
    assert engine.account_service.get_all_positions() == []


def test_rithmic_kill_switch_waits_for_accepted_exit_working_order(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.start_order_event_stream()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": True}
    )
    engine.account_service.replace_positions_for_products = MagicMock()

    def exit_position(**kwargs):
        assert kwargs["position_loader"]()
        return _kill_switch_result()

    engine.ops_safety.kill_switch_with_authoritative_positions = MagicMock(
        side_effect=exit_position
    )
    working = SimpleNamespace(basket_id="native-exit-1")
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=[
            _rithmic_emergency_snapshot(net_quantity="1"),
            _rithmic_emergency_snapshot(net_quantity="1", orders=[working]),
            _rithmic_emergency_snapshot(),
        ],
    ):
        result = engine._run_ops_kill_switch(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is True
    (engine.ops_safety.kill_switch_with_authoritative_positions.assert_called_once())
    assert engine.execution_engine.reconcile_rithmic_owned_orders.call_count == 3
    engine.account_service.replace_positions_for_products.assert_called_once_with(
        [],
        ("RITHMIC:NQ-202609",),
        timestamp_ms=1704067200000,
    )


def test_rithmic_kill_switch_ignores_terminal_remote_order_rows(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.start_order_event_stream()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": True}
    )
    terminal_order = SimpleNamespace(
        status="complete",
        notification_type="CANCEL",
        quantity="1",
        filled_quantity="0",
    )

    def exit_position(**kwargs):
        assert kwargs["position_loader"]()
        return _kill_switch_result()

    engine.ops_safety.kill_switch_with_authoritative_positions = MagicMock(
        side_effect=exit_position
    )
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=[
            _rithmic_emergency_snapshot(
                net_quantity="1",
                orders=[terminal_order],
            ),
            _rithmic_emergency_snapshot(orders=[terminal_order]),
        ],
    ):
        result = engine._run_ops_kill_switch(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is True
    (engine.ops_safety.kill_switch_with_authoritative_positions.assert_called_once())


def test_rithmic_kill_switch_does_not_retry_unreconciled_residual(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.start_order_event_stream()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": False}
    )

    def flatten(**kwargs):
        assert kwargs["position_loader"]()
        return _kill_switch_result()

    engine.ops_safety.kill_switch_with_authoritative_positions = MagicMock(
        side_effect=flatten
    )
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=[
            _rithmic_emergency_snapshot(net_quantity="1"),
            _rithmic_emergency_snapshot(net_quantity="1"),
        ],
    ):
        result = engine._run_ops_kill_switch(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is False
    (engine.ops_safety.kill_switch_with_authoritative_positions.assert_called_once())


def test_rithmic_kill_switch_blocks_when_remote_working_order_remains(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.start_order_event_stream()
    order_client = adapter._client
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock()
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"auto_resume_safe": False}
    )
    engine.ops_safety._write_event_best_effort = MagicMock()
    working = SimpleNamespace(basket_id="external-1")
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with patch(
        "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
        side_effect=[
            _rithmic_emergency_snapshot(
                net_quantity="1",
                orders=[working],
            ),
            _rithmic_emergency_snapshot(
                net_quantity="1",
                orders=[working],
            ),
        ],
    ):
        result = engine._run_ops_kill_switch(actor="ops", reason="drill")

    assert result["authoritative_flatten_verified"] is False
    assert any(
        "working_orders_remain" in failure["reason"]
        for failure in result["flatten_failures"]
    )
    order_client.submit.assert_not_called()
    order_client.exit_position.assert_not_called()


def test_rithmic_kill_switch_preserves_primary_error_when_restart_also_fails(
    engine,
):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.start_order_event_stream()
    engine.execution_engine.adapter = adapter
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine._start_exchange_order_event_stream = MagicMock(
        side_effect=RuntimeError("restart failed")
    )
    engine.ops_safety.record_kill_switch_result = MagicMock()
    engine.ops_safety.kill_switch_with_authoritative_positions = MagicMock(
        side_effect=lambda **kwargs: kwargs["position_loader"]()
    )
    _install_rithmic_emergency_flatten_service(engine, adapter)

    with (
        patch(
            "src.core.adapters.rithmic_emergency_flatten.load_rithmic_recovery_snapshot",
            side_effect=RuntimeError("ledger unavailable"),
        ),
        pytest.raises(RuntimeError, match="ledger unavailable"),
    ):
        engine._run_ops_kill_switch(actor="ops", reason="drill")

    recorded = engine.ops_safety.record_kill_switch_result.call_args.kwargs["result"]
    assert recorded["authoritative_flatten_verified"] is False
    assert recorded["recovery_failures"][-1]["reason"].endswith(":RuntimeError")


def test_reconnect_triggers_owned_order_reconcile_and_gates(engine):
    """A generation bump closes the old runtime before safe recovery/restart."""
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.connection_generation = MagicMock(return_value=1)
    engine.execution_engine.adapter = adapter
    engine.execution_engine.audit_external_orders = True
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        side_effect=lambda *_: (
            {"recoverable_count": 0, "auto_resume_safe": True}
            if adapter._client is None
            else pytest.fail("ledger recovery started before ORDER runtime closed")
        )
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
    engine.execution_engine.resume_after_reconcile = MagicMock()
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)

    adapter.start_order_event_stream()
    assert reconnect.last_generation == 1

    adapter.connection_generation.return_value = 2
    assert engine._reconcile_owned_orders_on_reconnect() is True

    engine.execution_engine.halt_for_reconcile.assert_called_once_with(timeout=30.0)
    engine.execution_engine.reconcile_rithmic_owned_orders.assert_called_once_with(
        "test", "ACCOUNT"
    )
    engine.execution_engine.resume_after_reconcile.assert_called_once()
    assert adapter._client is not None
    assert reconnect.last_generation == 1
    assert reconnect.pending_generation is None


def test_engine_reconnect_seam_delegates_only_to_venue_owner(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    service = MagicMock()
    service.reconcile_if_needed.return_value = True
    engine.execution_engine.adapter = adapter
    engine._rithmic_runtime.order_reconnect = service
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock()

    assert engine._reconcile_owned_orders_on_reconnect() is True

    service.reconcile_if_needed.assert_called_once_with()
    engine.execution_engine.reconcile_rithmic_owned_orders.assert_not_called()


def test_engine_reconnect_seams_route_through_runtime_facade(engine_factory):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine = engine_factory(adapter=adapter, audit_external_orders=True)
    engine._rithmic_runtime.order_reconnect = None
    engine._rithmic_runtime.reconcile_order_reconnect = MagicMock(
        return_value=(True, True)
    )
    engine._rithmic_runtime.on_order_runtime_started = MagicMock()

    assert engine._reconcile_owned_orders_on_reconnect() is True
    engine._rithmic_runtime.reconcile_order_reconnect.assert_called_once_with()

    engine._rithmic_runtime.order_event_stream._on_runtime_started()
    engine._rithmic_runtime.on_order_runtime_started.assert_called_once_with()


def test_rithmic_stream_start_clears_pending_generation_after_success(
    engine_factory,
):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.connection_generation = MagicMock(return_value=2)
    engine = engine_factory(adapter=adapter, audit_external_orders=True)
    engine.execution_engine.halt_for_reconcile = MagicMock(return_value=False)
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)
    assert reconnect.reconcile_if_needed() is False
    assert reconnect.pending_generation == 2

    with patch(
        "src.core.adapters.rithmic_order_event_stream.threading.Thread"
    ) as thread_type:
        engine._start_exchange_order_event_stream()

    thread_type.return_value.start.assert_called_once_with()
    assert reconnect.last_generation == 1
    assert reconnect.pending_generation is None


def test_rithmic_stream_start_failure_preserves_pending_generation(engine_factory):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.connection_generation = MagicMock(return_value=2)
    engine = engine_factory(adapter=adapter, audit_external_orders=True)
    engine.execution_engine.halt_for_reconcile = MagicMock(return_value=False)
    engine._halt_for_kill_switch = MagicMock()
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)
    assert reconnect.reconcile_if_needed() is False
    adapter.start_order_event_stream = MagicMock(
        side_effect=NetworkError("rithmic_order_start_failed")
    )

    with pytest.raises(NetworkError, match="rithmic_order_start_failed"):
        engine._start_exchange_order_event_stream()

    assert reconnect.last_generation == 1
    assert reconnect.pending_generation == 2
    engine._halt_for_kill_switch.assert_called_once_with()


def test_generic_stream_start_without_rithmic_owner_remains_compatible(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    engine.execution_engine.adapter = adapter
    engine._rithmic_runtime.order_reconnect = None

    with patch("src.core.engine.threading.Thread") as thread_type:
        engine._start_exchange_order_event_stream()

    assert adapter._client is not None
    thread_type.return_value.start.assert_called_once_with()


def test_non_rithmic_stream_helper_does_not_notify_rithmic_owner(engine):
    service = MagicMock()
    engine._rithmic_runtime.order_reconnect = service

    engine._start_exchange_order_event_stream()

    service.on_runtime_started.assert_not_called()


def test_reconnect_reconcile_failure_keeps_gate_and_retries(engine):
    """If the reconnect reconcile fails, submissions stay gated and it retries."""
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.connection_generation = MagicMock(return_value=2)
    engine.execution_engine.adapter = adapter
    engine.execution_engine.audit_external_orders = True
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        side_effect=RuntimeError("boom")
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
    engine.execution_engine.resume_after_reconcile = MagicMock()
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)

    assert engine._reconcile_owned_orders_on_reconnect() is False

    engine.execution_engine.halt_for_reconcile.assert_called_once_with(timeout=30.0)
    # Fail-safe: never resume against an unreconciled book, and do not advance
    # the generation so the next tick retries.
    engine.execution_engine.resume_after_reconcile.assert_not_called()
    assert reconnect.last_generation == 1
    assert reconnect.pending_generation == 2
    assert adapter._client is None

    engine.execution_engine.reconcile_rithmic_owned_orders.side_effect = None
    engine.execution_engine.reconcile_rithmic_owned_orders.return_value = {
        "recoverable_count": 0,
        "auto_resume_safe": True,
    }

    assert engine._reconcile_owned_orders_on_reconnect() is True
    assert adapter._client is not None
    assert reconnect.pending_generation is None
    engine.execution_engine.resume_after_reconcile.assert_called_once_with()


def test_reconnect_lease_loss_after_reconcile_keeps_gate_and_generation(engine):
    """A stale owner must not restart or publish completed reconciliation."""
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.connection_generation = MagicMock(return_value=2)
    adapter.start_order_event_stream = MagicMock()
    adapter.close = MagicMock(wraps=adapter.close)
    engine.execution_engine.adapter = adapter
    engine.execution_engine.audit_external_orders = True
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    leadership_lost = False

    def reconcile(*_args):
        nonlocal leadership_lost
        leadership_lost = True
        return {"recoverable_count": 0, "auto_resume_safe": True}

    def assert_leadership():
        if leadership_lost:
            raise RuntimeError("market_consumer_ownership_lost")

    engine._leadership_guard = assert_leadership
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        side_effect=reconcile
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    engine.execution_engine.halt_for_reconcile = MagicMock(
        wraps=engine.execution_engine.halt_for_reconcile
    )
    engine.execution_engine.resume_after_reconcile = MagicMock()
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)

    with pytest.raises(RuntimeError, match="market_consumer_ownership_lost"):
        engine._reconcile_owned_orders_on_reconnect()

    adapter.start_order_event_stream.assert_not_called()
    engine.execution_engine.resume_after_reconcile.assert_not_called()
    assert reconnect.last_generation == 1
    assert reconnect.pending_generation == 2
    assert engine.execution_engine._reconcile_halt is True
    assert adapter.close.call_count == 2


def test_reconnect_lease_loss_during_stream_restart_keeps_gate_and_generation(
    engine,
):
    """A restarted stream cannot resume submissions after its lease expires."""
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.connection_generation = MagicMock(return_value=2)
    adapter.close = MagicMock(wraps=adapter.close)
    engine.execution_engine.adapter = adapter
    engine.execution_engine.audit_external_orders = True
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    leadership_lost = False

    def restart_then_lose_leadership():
        nonlocal leadership_lost
        leadership_lost = True

    def assert_leadership():
        if leadership_lost:
            raise RuntimeError("market_consumer_ownership_lost")

    adapter.start_order_event_stream = MagicMock(
        side_effect=restart_then_lose_leadership
    )
    engine._leadership_guard = assert_leadership
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"recoverable_count": 0, "auto_resume_safe": True}
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    engine.execution_engine.halt_for_reconcile = MagicMock(
        wraps=engine.execution_engine.halt_for_reconcile
    )
    engine.execution_engine.resume_after_reconcile = MagicMock()
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)

    with pytest.raises(RuntimeError, match="market_consumer_ownership_lost"):
        engine._reconcile_owned_orders_on_reconnect()

    adapter.start_order_event_stream.assert_called_once_with()
    engine.execution_engine.resume_after_reconcile.assert_not_called()
    assert reconnect.last_generation == 1
    assert reconnect.pending_generation == 2
    assert engine.execution_engine._reconcile_halt is True
    assert adapter.close.call_count == 2


def test_reconnect_waits_for_submission_drain_before_closing_runtime(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.connection_generation = MagicMock(return_value=2)
    adapter.close = MagicMock(wraps=adapter.close)
    engine.execution_engine.adapter = adapter
    engine.execution_engine.halt_for_reconcile = MagicMock(return_value=False)
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock()
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)

    assert engine._reconcile_owned_orders_on_reconnect() is False

    adapter.close.assert_not_called()
    engine.execution_engine.reconcile_rithmic_owned_orders.assert_not_called()
    assert reconnect.pending_generation == 2


def test_generation_read_failure_reconciles_fail_closed(engine):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.connection_generation = MagicMock(side_effect=RuntimeError("old binding"))
    engine.execution_engine.adapter = adapter
    engine.execution_engine.audit_external_orders = True
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"recoverable_count": 0, "auto_resume_safe": True}
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    engine.execution_engine.resume_after_reconcile = MagicMock()
    _install_rithmic_order_reconnect_service(engine, adapter)

    assert engine._reconcile_owned_orders_on_reconnect() is True

    engine.execution_engine.reconcile_rithmic_owned_orders.assert_called_once_with(
        "test", "ACCOUNT"
    )
    engine.execution_engine.resume_after_reconcile.assert_called_once_with()


@pytest.mark.parametrize(
    "summary",
    [
        {"recoverable_count": 1, "auto_resume_safe": False},
        {"recoverable_count": 1},
    ],
)
def test_reconnect_unresolved_summary_stays_closed_and_gated(engine, summary):
    adapter = _rithmic_adapter_for_reconnect_test()
    adapter.connection_generation = MagicMock(return_value=2)
    engine.execution_engine.adapter = adapter
    engine.execution_engine.audit_external_orders = True
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.execution_engine.halt_for_reconcile = MagicMock(return_value=True)
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value=summary
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    engine.execution_engine.resume_after_reconcile = MagicMock()
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)

    assert engine._reconcile_owned_orders_on_reconnect() is False

    assert adapter._client is None
    assert reconnect.pending_generation == 2
    engine.execution_engine.resume_after_reconcile.assert_not_called()


def test_rithmic_event_loop_checks_reconnect_without_runtime_reconcile_service(
    engine_factory,
):
    client = MagicMock()
    adapter = _rithmic_adapter_for_reconnect_test(client=client)
    engine = engine_factory(adapter=adapter, audit_external_orders=True)
    client.poll_event.side_effect = lambda: engine._order_event_stop.set()
    engine._reconcile_owned_orders_on_reconnect = MagicMock(return_value=True)
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)

    class ImmediateThread:
        def __init__(self, *, target, name, daemon):
            self.target = target

        def start(self):
            self.target()

    with patch(
        "src.core.adapters.rithmic_order_event_stream.threading.Thread",
        ImmediateThread,
    ):
        engine._start_exchange_order_event_stream()

    engine._reconcile_owned_orders_on_reconnect.assert_called_once_with()
    assert reconnect.last_generation == 1


def test_rithmic_event_loop_reconciles_and_restarts_before_polling(engine_factory):
    """The real event loop closes, reconciles, restarts, then resumes polling."""
    first_client = MagicMock()
    first_client.connection_generation.return_value = 2
    second_client = MagicMock()
    second_client.connection_generation.return_value = 1
    second_client.poll_event.side_effect = lambda: engine._order_event_stop.set()
    factory = MagicMock(side_effect=[first_client, second_client])
    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments={
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        client_factory=factory,
    )
    engine = engine_factory(adapter=adapter, audit_external_orders=True)
    engine.execution_engine.audit_external_orders = True
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        side_effect=lambda *_: (
            {"recoverable_count": 0, "auto_resume_safe": True}
            if adapter._client is None
            else pytest.fail("ledger recovery started before ORDER runtime closed")
        )
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)

    class ImmediateThread:
        def __init__(self, *, target, name, daemon):
            self.target = target

        def start(self):
            self.target()

    with patch(
        "src.core.adapters.rithmic_order_event_stream.threading.Thread",
        ImmediateThread,
    ):
        engine._start_exchange_order_event_stream()

    assert factory.call_count == 2
    engine.execution_engine.reconcile_rithmic_owned_orders.assert_called_once_with(
        "test", "ACCOUNT"
    )
    second_client.poll_event.assert_called_once_with()
    assert engine.execution_engine._reconcile_halt is False
    assert reconnect.pending_generation is None
    assert reconnect.last_generation == 1


def test_reconnect_stream_restart_failure_stays_gated_then_retries(engine):
    """A failed runtime restart remains gated and a later tick may recover."""
    first_client = MagicMock()
    first_client.connection_generation.return_value = 2
    recovered_client = MagicMock()
    factory = MagicMock(
        side_effect=[first_client, RuntimeError("offline"), recovered_client]
    )
    adapter = RithmicExchangeAdapter(
        profile="test",
        account_id="ACCOUNT",
        instruments={
            "RITHMIC:NQ-202609": {
                "exchange": "CME",
                "quantity_step": "1",
                "price_tick": "0.25",
            }
        },
        client_factory=factory,
    )
    engine.execution_engine.adapter = adapter
    engine.execution_engine.audit_external_orders = True
    engine._rithmic_recovery_profile = "test"
    engine._rithmic_recovery_account_id = "ACCOUNT"
    engine.execution_engine.reconcile_rithmic_owned_orders = MagicMock(
        return_value={"recoverable_count": 0, "auto_resume_safe": True}
    )
    engine._apply_rithmic_authoritative_account_summary = MagicMock()
    reconnect = _install_rithmic_order_reconnect_service(engine, adapter)
    adapter.start_order_event_stream()

    assert engine._reconcile_owned_orders_on_reconnect() is False
    assert adapter._client is None
    assert engine.execution_engine._reconcile_halt is True
    assert reconnect.pending_generation == 2

    assert engine._reconcile_owned_orders_on_reconnect() is True
    assert adapter._client is recovered_client
    assert engine.execution_engine._reconcile_halt is False
    assert reconnect.pending_generation is None
    assert reconnect.last_generation == 1
    assert engine.execution_engine.reconcile_rithmic_owned_orders.call_count == 2
