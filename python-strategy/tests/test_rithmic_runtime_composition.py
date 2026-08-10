from dataclasses import replace
from typing import cast
from unittest.mock import MagicMock

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
from src.core.adapters.rithmic_order_event_stream import (
    RithmicOrderEventStreamService,
)
from src.core.adapters.rithmic_order_reconnect import (
    RithmicOrderReconnectService,
)
from src.core.adapters.rithmic_portfolio_exit import RithmicPortfolioExitService
from src.core.adapters.rithmic_runtime_composition import (
    RithmicRuntimeCallbacks,
    build_rithmic_runtime_owners,
)
from src.core.adapters.rithmic_runtime_recovery import (
    RithmicRuntimeRecoveryService,
)
from src.core.adapters.rithmic_strategy_exit import RithmicStrategyExitService
from src.core.execution import ExecutionEngine
from src.core.interfaces import IExchangeAdapter
from src.core.ops_safety import OpsSafetyService
from src.core.risk_manager import AccountService


def _callbacks() -> RithmicRuntimeCallbacks:
    return RithmicRuntimeCallbacks(
        is_running=MagicMock(return_value=True),
        publish_worker=MagicMock(),
        on_runtime_started=MagicMock(),
        reconcile_if_needed=MagicMock(return_value=True),
        process_event=MagicMock(return_value={"action": "applied"}),
        lockdown=MagicMock(),
        assert_runtime_leadership=MagicMock(),
        halt_submissions=MagicMock(),
        clear_local_halt=MagicMock(),
        persist_lockdown_state=MagicMock(),
        persist_redis_lockdown=MagicMock(),
        stop_order_event_stream=MagicMock(return_value=True),
        start_order_event_stream=MagicMock(),
        current_order_event_thread=MagicMock(return_value=None),
        publish_authoritative_summary=MagicMock(),
    )


def _execution_engine() -> MagicMock:
    execution_engine = MagicMock(spec=ExecutionEngine)
    execution_engine.clock = MagicMock()
    execution_engine.clock.now.return_value = 1_700_000_000.0
    execution_engine.audit_external_orders = True
    return execution_engine


def test_rithmic_composition_builds_the_complete_shared_owner_graph() -> None:
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
    execution_engine = _execution_engine()
    account_service = MagicMock(spec=AccountService)
    ops_safety = MagicMock(spec=OpsSafetyService)
    runtime_started = MagicMock()
    callbacks = replace(
        _callbacks(),
        on_runtime_started=runtime_started,
    )

    owners = build_rithmic_runtime_owners(
        adapter=adapter,
        profile="test",
        account_id="ACCOUNT",
        execution_engine=execution_engine,
        account_service=account_service,
        ops_safety=ops_safety,
        stop_event=MagicMock(),
        callbacks=callbacks,
        logger=MagicMock(),
    )

    assert isinstance(owners.ledger_recovery, RithmicLedgerRecoveryService)
    assert isinstance(owners.order_reconnect, RithmicOrderReconnectService)
    assert isinstance(owners.runtime_recovery, RithmicRuntimeRecoveryService)
    assert isinstance(owners.external_order_drift, RithmicExternalOrderDriftService)
    assert isinstance(owners.strategy_exit, RithmicStrategyExitService)
    assert isinstance(owners.order_event_stream, RithmicOrderEventStreamService)
    assert isinstance(
        owners.kill_switch_clear_preparation,
        RithmicKillSwitchClearPreparationService,
    )
    assert isinstance(owners.emergency_flatten, RithmicEmergencyFlattenService)
    assert owners.portfolio_exit_factory is not None
    portfolio_id_for_sleeve = MagicMock(return_value="portfolio")
    portfolio_exit = owners.portfolio_exit_factory(portfolio_id_for_sleeve)
    assert isinstance(portfolio_exit, RithmicPortfolioExitService)
    assert portfolio_exit.operation_gate is owners.order_event_lifecycle
    assert (
        portfolio_exit.schedule_emergency_flatten
        == owners.emergency_flatten.schedule_portfolio_exit_compensation
    )
    assert portfolio_exit.portfolio_id_for_sleeve is portfolio_id_for_sleeve
    assert owners.strategy_exit.operation_gate is owners.order_event_lifecycle
    assert (
        owners.kill_switch_clear_preparation._operation_gate
        is owners.order_event_lifecycle
    )
    assert owners.emergency_flatten.operation_gate is owners.order_event_lifecycle
    for callback in callbacks.__dict__.values():
        callback.assert_not_called()
    execution_engine.reconcile_rithmic_owned_orders.assert_not_called()
    execution_engine.halt_for_reconcile.assert_not_called()
    execution_engine.resume_after_reconcile.assert_not_called()
    account_service.replace_authoritative_balance.assert_not_called()
    ops_safety.persist_kill_switch_state.assert_not_called()

    replacement_halt = MagicMock(return_value=True)
    execution_engine.halt_for_reconcile = replacement_halt
    assert owners.kill_switch_clear_preparation._halt_for_reconcile(timeout=30.0)
    replacement_halt.assert_called_once_with(timeout=30.0)

    replacement_generation = MagicMock(return_value=7)
    owners.external_order_drift.current_generation = replacement_generation
    assert owners.kill_switch_clear_preparation._current_drift_generation() == 7
    replacement_generation.assert_called_once_with()

    owners.order_event_stream._on_runtime_started()
    runtime_started.assert_called_once_with()


def test_non_rithmic_composition_creates_no_venue_runtime_owner() -> None:
    callbacks = _callbacks()

    owners = build_rithmic_runtime_owners(
        adapter=cast(IExchangeAdapter, MagicMock()),
        profile=None,
        account_id=None,
        execution_engine=_execution_engine(),
        account_service=MagicMock(spec=AccountService),
        ops_safety=MagicMock(spec=OpsSafetyService),
        stop_event=MagicMock(),
        callbacks=callbacks,
        logger=MagicMock(),
    )

    assert owners.ledger_recovery is None
    assert owners.order_reconnect is None
    assert owners.runtime_recovery is None
    assert owners.external_order_drift is None
    assert owners.strategy_exit is None
    assert owners.order_event_stream is None
    assert owners.kill_switch_clear_preparation is None
    assert owners.emergency_flatten is None
    assert owners.portfolio_exit_factory is None
    for callback in callbacks.__dict__.values():
        callback.assert_not_called()


def test_configured_ledger_recovery_remains_available_without_rithmic_adapter() -> None:
    callbacks = _callbacks()

    owners = build_rithmic_runtime_owners(
        adapter=cast(IExchangeAdapter, MagicMock()),
        profile="test",
        account_id="ACCOUNT",
        execution_engine=_execution_engine(),
        account_service=MagicMock(spec=AccountService),
        ops_safety=MagicMock(spec=OpsSafetyService),
        stop_event=MagicMock(),
        callbacks=callbacks,
        logger=MagicMock(),
    )

    assert isinstance(owners.ledger_recovery, RithmicLedgerRecoveryService)
    assert owners.order_reconnect is None
    assert owners.runtime_recovery is None
    assert owners.external_order_drift is None
    assert owners.strategy_exit is None
    assert owners.order_event_stream is None
    assert owners.kill_switch_clear_preparation is None
    assert owners.emergency_flatten is None
    assert owners.portfolio_exit_factory is None
    for callback in callbacks.__dict__.values():
        callback.assert_not_called()
