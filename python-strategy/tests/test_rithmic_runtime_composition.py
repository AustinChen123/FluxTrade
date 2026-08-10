from dataclasses import replace
from decimal import Decimal
from typing import cast
from unittest.mock import MagicMock

import pytest

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
    RithmicRuntimeOwners,
    build_rithmic_runtime_owners,
)
from src.core.adapters.rithmic_runtime_recovery import (
    RithmicRuntimeRecoveryService,
)
from src.core.adapters.rithmic_strategy_exit import RithmicStrategyExitService
from src.core.execution import ExecutionEngine, ExitDecision
from src.core.interfaces import IExchangeAdapter
from src.core.models import Signal, SignalType
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


def test_runtime_handle_routes_control_calls_to_current_owners() -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())
    stream = MagicMock()
    drift = MagicMock()
    drift.current_generation.return_value = 7
    clear_preparation = MagicMock()
    clear_preparation.prepare.return_value = (True, 7)
    ledger = MagicMock()
    summary = {"auto_resume_safe": True}
    ledger.reconcile_startup.return_value = summary
    runtime_recovery = MagicMock()
    runtime_recovery.run_once.return_value = True
    owners.order_event_stream = stream
    owners.external_order_drift = drift
    owners.kill_switch_clear_preparation = clear_preparation
    owners.ledger_recovery = ledger
    owners.runtime_recovery = runtime_recovery

    assert owners.start_order_event_stream() is True
    owners.detect_external_order_drift("external order")
    assert owners.prepare_kill_switch_clear() == (True, 7)
    assert owners.current_external_order_drift_generation() == 7
    owners.finalize_external_order_drift_clear(
        prepared_generation=7,
        clear_succeeded=True,
    )
    assert owners.reconcile_startup() == (True, summary)
    owners.publish_authoritative_summary(summary)
    assert owners.runtime_recovery_operation()() is True

    stream.start.assert_called_once_with()
    drift.detect.assert_called_once_with("external order")
    drift.current_generation.assert_called_once_with()
    drift.finalize_clear.assert_called_once_with(
        prepared_generation=7,
        clear_succeeded=True,
    )
    clear_preparation.prepare.assert_called_once_with()
    ledger.reconcile_startup.assert_called_once_with()
    ledger.publish_authoritative_summary.assert_called_once_with(summary)
    runtime_recovery.run_once.assert_called_once_with()


def test_runtime_handle_preserves_absent_owner_defaults_and_errors() -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())

    assert owners.start_order_event_stream() is False
    assert owners.prepare_kill_switch_clear() == (True, None)
    assert owners.current_external_order_drift_generation() == 0
    assert owners.reconcile_startup() == (False, None)

    with pytest.raises(
        RuntimeError,
        match="Rithmic external-order drift owner is unavailable",
    ):
        owners.detect_external_order_drift("external order")
    with pytest.raises(
        RuntimeError,
        match="Rithmic external-order drift owner is unavailable",
    ):
        owners.finalize_external_order_drift_clear(
            prepared_generation=1,
            clear_succeeded=False,
        )
    with pytest.raises(RuntimeError, match="rithmic_ledger_recovery_unavailable"):
        owners.publish_authoritative_summary({})
    with pytest.raises(
        RuntimeError,
        match="rithmic_runtime_reconciliation_unavailable",
    ):
        owners.runtime_recovery_operation()


def test_runtime_handle_does_not_fall_through_when_ledger_owner_returns_none() -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())
    owners.ledger_recovery = MagicMock()
    owners.ledger_recovery.reconcile_startup.return_value = None

    assert owners.reconcile_startup() == (True, None)


def test_runtime_handle_routes_reconnect_lifecycle_to_current_owner() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )

    assert owners.on_order_runtime_started() is None
    assert owners.reconcile_order_reconnect() is None

    first_owner = MagicMock()
    first_owner.reconcile_if_needed.return_value = False
    owners.order_reconnect = first_owner

    assert owners.on_order_runtime_started() is None
    assert owners.reconcile_order_reconnect() is False
    first_owner.on_runtime_started.assert_called_once_with()
    first_owner.reconcile_if_needed.assert_called_once_with()

    second_owner = MagicMock()
    second_owner.reconcile_if_needed.return_value = True
    owners.order_reconnect = second_owner

    assert owners.on_order_runtime_started() is None
    assert owners.reconcile_order_reconnect() is True
    first_owner.on_runtime_started.assert_called_once_with()
    first_owner.reconcile_if_needed.assert_called_once_with()
    second_owner.on_runtime_started.assert_called_once_with()
    second_owner.reconcile_if_needed.assert_called_once_with()


def test_reconnect_policy_uses_facade_authority_before_current_owner() -> None:
    owner = MagicMock()
    owner.reconcile_if_needed.return_value = False
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        order_reconnect=owner,
        is_rithmic_runtime=False,
    )

    assert owners.reconcile_order_reconnect() is True
    owner.reconcile_if_needed.assert_not_called()

    owners.is_rithmic_runtime = True
    assert owners.reconcile_order_reconnect() is False
    owner.reconcile_if_needed.assert_called_once_with()


@pytest.mark.parametrize(
    ("owner_attr", "owner_method", "facade_method", "args", "kwargs"),
    [
        ("order_event_stream", "start", "start_order_event_stream", (), {}),
        (
            "external_order_drift",
            "detect",
            "detect_external_order_drift",
            ("external order",),
            {},
        ),
        (
            "kill_switch_clear_preparation",
            "prepare",
            "prepare_kill_switch_clear",
            (),
            {},
        ),
        (
            "external_order_drift",
            "current_generation",
            "current_external_order_drift_generation",
            (),
            {},
        ),
        (
            "external_order_drift",
            "finalize_clear",
            "finalize_external_order_drift_clear",
            (),
            {"prepared_generation": 1, "clear_succeeded": False},
        ),
        ("ledger_recovery", "reconcile_startup", "reconcile_startup", (), {}),
        (
            "ledger_recovery",
            "publish_authoritative_summary",
            "publish_authoritative_summary",
            ({},),
            {},
        ),
    ],
)
def test_runtime_handle_preserves_owner_exception_identity(
    owner_attr: str,
    owner_method: str,
    facade_method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())
    owner = MagicMock()
    error = RuntimeError(owner_method)
    getattr(owner, owner_method).side_effect = error
    setattr(owners, owner_attr, owner)

    with pytest.raises(RuntimeError) as caught:
        getattr(owners, facade_method)(*args, **kwargs)

    assert caught.value is error


def test_runtime_handle_preserves_runtime_recovery_exception_identity() -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())
    error = RuntimeError("runtime recovery")
    runtime_recovery = MagicMock()
    runtime_recovery.run_once.side_effect = error
    owners.runtime_recovery = runtime_recovery

    operation = owners.runtime_recovery_operation()
    with pytest.raises(RuntimeError) as caught:
        operation()

    assert caught.value is error


def test_runtime_reconciliation_selector_preserves_generic_operation_identity() -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())
    generic_operation = MagicMock(return_value=True)
    run_exclusive = MagicMock()

    venue_owned, selected_operation = owners.select_runtime_reconciliation(
        generic_operation,
        run_exclusive,
    )

    assert venue_owned is False
    assert selected_operation is generic_operation
    run_exclusive.assert_not_called()


def test_runtime_reconciliation_selector_resolves_current_owner_each_time() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    generic_operation = MagicMock(return_value=False)
    events: list[str] = []

    def run_exclusive(operation):
        events.append("exclusive")
        return operation()

    venue_owned, selected_operation = owners.select_runtime_reconciliation(
        generic_operation,
        run_exclusive,
    )

    first_owner = MagicMock()
    first_owner.run_once.side_effect = lambda: events.append("first") or True
    owners.runtime_recovery = first_owner
    assert selected_operation() is True

    second_owner = MagicMock()
    second_owner.run_once.side_effect = lambda: events.append("second") or False
    owners.runtime_recovery = second_owner
    assert selected_operation() is False

    assert venue_owned is True
    assert events == ["exclusive", "first", "exclusive", "second"]
    generic_operation.assert_not_called()
    first_owner.run_once.assert_called_once_with()
    second_owner.run_once.assert_called_once_with()


def test_runtime_reconciliation_selector_fails_closed_before_exclusion() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    generic_operation = MagicMock(return_value=True)
    run_exclusive = MagicMock()

    venue_owned, selected_operation = owners.select_runtime_reconciliation(
        generic_operation,
        run_exclusive,
    )

    assert venue_owned is True
    with pytest.raises(
        RuntimeError,
        match="^rithmic_runtime_reconciliation_unavailable$",
    ):
        selected_operation()
    generic_operation.assert_not_called()
    run_exclusive.assert_not_called()


def test_runtime_reconciliation_selector_preserves_operation_exception() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    generic_operation = MagicMock(return_value=True)
    error = RuntimeError("recovery sentinel")
    owner = MagicMock()
    owner.run_once.side_effect = error
    owners.runtime_recovery = owner
    events: list[str] = []

    def run_exclusive(operation):
        events.extend(("enter:market", "enter:ops"))
        try:
            return operation()
        finally:
            events.extend(("exit:ops", "exit:market"))

    venue_owned, selected_operation = owners.select_runtime_reconciliation(
        generic_operation,
        run_exclusive,
    )

    assert venue_owned is True
    with pytest.raises(RuntimeError) as caught:
        selected_operation()

    assert caught.value is error
    assert events == ["enter:market", "enter:ops", "exit:ops", "exit:market"]
    owner.run_once.assert_called_once_with()
    generic_operation.assert_not_called()


def test_startup_balance_dispatch_preserves_generic_result_and_call_count() -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())
    result = object()
    generic_reconciliation = MagicMock(return_value=result)

    assert owners.run_startup_balance_reconciliation(generic_reconciliation) is result
    generic_reconciliation.assert_called_once_with()


def test_startup_balance_dispatch_defers_to_rithmic_ledger_owner() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    generic_reconciliation = MagicMock()

    assert owners.run_startup_balance_reconciliation(generic_reconciliation) is None
    generic_reconciliation.assert_not_called()


@pytest.mark.parametrize(
    ("is_rithmic_runtime", "summary", "expected"),
    [
        (False, {"auto_resume_safe": True}, (False, False)),
        (True, None, (True, False)),
        (True, {}, (True, False)),
        (True, 0, (True, False)),
        (True, "", (True, False)),
        (True, [], (True, False)),
        (True, {"other": True}, (True, False)),
        (True, {"auto_resume_safe": False}, (True, False)),
        (True, {"auto_resume_safe": 1}, (True, False)),
        (True, {"auto_resume_safe": True}, (True, True)),
    ],
)
def test_startup_reconciliation_classifier_preserves_owner_and_exact_safety(
    is_rithmic_runtime: bool,
    summary: object,
    expected: tuple[bool, bool],
) -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=is_rithmic_runtime,
    )

    assert owners.classify_startup_reconciliation(summary) == expected


@pytest.mark.parametrize("summary", [1, "unsafe", ["unsafe"]])
def test_startup_reconciliation_classifier_preserves_truthy_malformed_failure(
    summary: object,
) -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    classify = owners.classify_startup_reconciliation

    with pytest.raises(AttributeError):
        classify(summary)


@pytest.mark.parametrize(
    ("facade_method", "owner_method"),
    [
        ("on_order_runtime_started", "on_runtime_started"),
        ("reconcile_order_reconnect", "reconcile_if_needed"),
    ],
)
def test_runtime_handle_preserves_reconnect_exception_identity(
    facade_method: str,
    owner_method: str,
) -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    owner = MagicMock()
    error = RuntimeError(owner_method)
    getattr(owner, owner_method).side_effect = error
    owners.order_reconnect = owner

    with pytest.raises(RuntimeError) as caught:
        getattr(owners, facade_method)()

    assert caught.value is error


def test_runtime_handle_routes_execution_dispatch_to_current_owners() -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())
    signal = MagicMock()
    decision = MagicMock()
    first_exit = MagicMock()
    first_exit_result = {"status": "first"}
    first_exit.execute.return_value = first_exit_result
    first_flatten = MagicMock()
    first_flatten_result = {"status": "flattened"}
    first_flatten.execute.return_value = first_flatten_result
    owners.strategy_exit = first_exit
    owners.emergency_flatten = first_flatten

    assert owners.execute_strategy_exit(signal, decision) is first_exit_result
    assert (
        owners.execute_emergency_flatten(
            actor="ops",
            reason="drill",
            operation_id="operation-1",
        )
        is first_flatten_result
    )
    first_exit.execute.assert_called_once_with(signal, decision)
    first_flatten.execute.assert_called_once_with(
        actor="ops",
        reason="drill",
        operation_id="operation-1",
    )

    second_exit = MagicMock()
    second_exit_result = {"status": "second"}
    second_exit.execute.return_value = second_exit_result
    second_flatten = MagicMock()
    second_flatten_result = {"status": "flat-again"}
    second_flatten.execute.return_value = second_flatten_result
    owners.strategy_exit = second_exit
    owners.emergency_flatten = second_flatten

    assert owners.execute_strategy_exit(signal, decision) is second_exit_result
    assert (
        owners.execute_emergency_flatten(actor="ops", reason=None)
        is second_flatten_result
    )
    first_exit.execute.assert_called_once_with(signal, decision)
    first_flatten.execute.assert_called_once()
    second_exit.execute.assert_called_once_with(signal, decision)
    second_flatten.execute.assert_called_once_with(
        actor="ops",
        reason=None,
        operation_id=None,
    )


@pytest.mark.parametrize("operation_id", [None, "operation-1"])
def test_emergency_flatten_dispatcher_uses_generic_fallback_exactly_once(
    operation_id: str | None,
) -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=False,
    )
    result = {"source": "generic", "operation_id": operation_id}
    fallback = MagicMock(return_value=result)
    owners.emergency_flatten = MagicMock()

    actual = owners.run_emergency_flatten(
        fallback,
        actor="ops",
        reason="drill",
        operation_id=operation_id,
    )

    assert actual is result
    fallback.assert_called_once_with()
    owners.emergency_flatten.execute.assert_not_called()
    assert owners.requires_authoritative_flatten_verification() is False


def test_emergency_flatten_dispatcher_uses_current_rithmic_owner() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    fallback = MagicMock()
    first = MagicMock()
    first_result = {"source": "first"}
    first.execute.return_value = first_result
    owners.emergency_flatten = first

    assert (
        owners.run_emergency_flatten(
            fallback,
            actor="ops",
            reason="drill",
            operation_id="operation-1",
        )
        is first_result
    )

    second = MagicMock()
    second_result = {"source": "second"}
    second.execute.return_value = second_result
    owners.emergency_flatten = second
    assert (
        owners.run_emergency_flatten(
            fallback,
            actor="operator",
            reason=None,
        )
        is second_result
    )
    fallback.assert_not_called()
    first.execute.assert_called_once_with(
        actor="ops",
        reason="drill",
        operation_id="operation-1",
    )
    second.execute.assert_called_once_with(
        actor="operator",
        reason=None,
        operation_id=None,
    )
    assert owners.requires_authoritative_flatten_verification() is True


def test_emergency_flatten_dispatcher_rejects_missing_rithmic_owner() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    fallback = MagicMock()

    with pytest.raises(
        RuntimeError,
        match="^rithmic_emergency_flatten_unavailable$",
    ):
        owners.run_emergency_flatten(
            fallback,
            actor="ops",
            reason="drill",
        )

    fallback.assert_not_called()


@pytest.mark.parametrize("owner_configured", [False, True])
def test_emergency_flatten_dispatcher_preserves_selected_exception_identity(
    owner_configured: bool,
) -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=owner_configured,
    )
    fallback = MagicMock()
    error = RuntimeError("selected owner failed")
    if owner_configured:
        owners.emergency_flatten = MagicMock()
        owners.emergency_flatten.execute.side_effect = error
    else:
        fallback.side_effect = error

    with pytest.raises(RuntimeError) as caught:
        owners.run_emergency_flatten(
            fallback,
            actor="ops",
            reason=None,
        )

    assert caught.value is error
    if owner_configured:
        fallback.assert_not_called()
    else:
        assert owners.emergency_flatten is None


@pytest.mark.parametrize(
    ("facade_method", "args", "kwargs", "message"),
    [
        (
            "execute_strategy_exit",
            (MagicMock(), MagicMock()),
            {},
            "rithmic_strategy_exit_unavailable",
        ),
        (
            "execute_emergency_flatten",
            (),
            {"actor": "ops", "reason": None},
            "rithmic_emergency_flatten_unavailable",
        ),
    ],
)
def test_runtime_handle_rejects_missing_execution_dispatch_owner(
    facade_method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
    message: str,
) -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())

    with pytest.raises(RuntimeError, match=f"^{message}$"):
        getattr(owners, facade_method)(*args, **kwargs)


@pytest.mark.parametrize(
    ("owner_attr", "owner_method", "facade_method", "args", "kwargs"),
    [
        (
            "strategy_exit",
            "execute",
            "execute_strategy_exit",
            (MagicMock(), MagicMock()),
            {},
        ),
        (
            "emergency_flatten",
            "execute",
            "execute_emergency_flatten",
            (),
            {"actor": "ops", "reason": "drill"},
        ),
    ],
)
def test_runtime_handle_preserves_execution_dispatch_exception_identity(
    owner_attr: str,
    owner_method: str,
    facade_method: str,
    args: tuple[object, ...],
    kwargs: dict[str, object],
) -> None:
    owners = RithmicRuntimeOwners(order_event_lifecycle=MagicMock())
    owner = MagicMock()
    error = RuntimeError(owner_method)
    getattr(owner, owner_method).side_effect = error
    setattr(owners, owner_attr, owner)

    with pytest.raises(RuntimeError) as caught:
        getattr(owners, facade_method)(*args, **kwargs)

    assert caught.value is error


def test_runtime_handle_routes_portfolio_exit_to_current_factory() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        profile="test",
        account_id="ACCOUNT",
    )
    signal = MagicMock()
    decision = MagicMock()
    candle = MagicMock()
    portfolio_id_for_sleeve = MagicMock(return_value="portfolio")
    owner = MagicMock()
    result = {"status": "verified_reduced"}
    owner.execute.return_value = result
    factory = MagicMock(return_value=owner)
    owners.portfolio_exit_factory = factory

    assert (
        owners.execute_portfolio_exit(
            signal,
            decision,
            candle,
            portfolio_id_for_sleeve,
        )
        is result
    )
    factory.assert_called_once_with(portfolio_id_for_sleeve)
    owner.execute.assert_called_once_with(signal, decision, candle)


def test_runtime_handle_rejects_missing_portfolio_exit_factory() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        profile="test",
        account_id="ACCOUNT",
    )

    with pytest.raises(
        RuntimeError,
        match="^rithmic_portfolio_exit_unavailable$",
    ):
        owners.execute_portfolio_exit(
            MagicMock(),
            MagicMock(),
            None,
            MagicMock(),
        )


@pytest.mark.parametrize("failure_owner", ["factory", "execute"])
def test_runtime_handle_preserves_portfolio_exit_exception_identity(
    failure_owner: str,
) -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        profile="test",
        account_id="ACCOUNT",
    )
    error = RuntimeError(failure_owner)
    owner = MagicMock()
    factory = MagicMock(return_value=owner)
    owners.portfolio_exit_factory = factory
    if failure_owner == "factory":
        factory.side_effect = error
    else:
        owner.execute.side_effect = error

    with pytest.raises(RuntimeError) as caught:
        owners.execute_portfolio_exit(
            MagicMock(),
            MagicMock(),
            None,
            MagicMock(),
        )

    assert caught.value is error


def _exit_signal() -> Signal:
    return Signal(
        strategy_id="portfolio.sleeve",
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_700_000_000_000,
        type=SignalType.EXIT_LONG,
        quantity=Decimal("1"),
    )


def test_runtime_handle_routes_current_strategy_exit_without_identity_guard() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    signal = _exit_signal()
    decision = ExitDecision(
        allowed=True,
        reason="position_matched",
        quantity=signal.quantity,
        position_quantity=signal.quantity,
    )
    strategy_exit = MagicMock()
    result = {"status": "verified_flat"}
    strategy_exit.execute.return_value = result
    owners.strategy_exit = strategy_exit
    resolver = MagicMock(return_value=None)
    authoritative = MagicMock(
        side_effect=lambda routed_signal, candle, execute: (
            execute(routed_signal, decision) is result
        )
    )

    assert owners.route_authoritative_exit(
        signal,
        None,
        resolver,
        authoritative,
    ) == (True, True)

    resolver.assert_called_once_with(signal.strategy_id)
    authoritative.assert_called_once()
    strategy_exit.execute.assert_called_once_with(signal, decision)


def test_runtime_handle_routes_portfolio_with_original_resolver_identity() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
        profile="test",
        account_id="ACCOUNT",
    )
    signal = _exit_signal()
    candle = MagicMock()
    decision = MagicMock()
    resolver = MagicMock(return_value="portfolio")
    owner = MagicMock()
    result = {"status": "verified_reduced"}
    owner.execute.return_value = result
    factory = MagicMock(return_value=owner)
    owners.portfolio_exit_factory = factory
    authoritative = MagicMock(
        side_effect=lambda routed_signal, routed_candle, execute: (
            execute(routed_signal, decision) is result
        )
    )

    assert owners.route_authoritative_exit(
        signal,
        candle,
        resolver,
        authoritative,
    ) == (True, True)

    resolver.assert_called_once_with(signal.strategy_id)
    factory.assert_called_once_with(resolver)
    owner.execute.assert_called_once_with(signal, decision, candle)


@pytest.mark.parametrize("configured", [False, True])
def test_runtime_handle_leaves_unowned_signals_unhandled(configured: bool) -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=configured,
    )
    signal = _exit_signal()
    if configured:
        signal = signal.model_copy(update={"type": SignalType.LONG})
    resolver = MagicMock()
    authoritative = MagicMock()

    assert owners.route_authoritative_exit(
        signal,
        None,
        resolver,
        authoritative,
    ) == (False, False)

    resolver.assert_not_called()
    authoritative.assert_not_called()


def test_runtime_handle_rejects_missing_portfolio_identity_before_factory() -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
    )
    owners.portfolio_exit_factory = MagicMock()
    resolver = MagicMock(return_value="portfolio")
    authoritative = MagicMock(
        side_effect=lambda signal, candle, execute: execute(signal, MagicMock())
    )

    with pytest.raises(
        RuntimeError,
        match="^rithmic_portfolio_exit_account_identity_missing$",
    ):
        owners.route_authoritative_exit(
            _exit_signal(),
            None,
            resolver,
            authoritative,
        )

    owners.portfolio_exit_factory.assert_not_called()


@pytest.mark.parametrize(
    "failure_owner",
    ["resolver", "factory", "owner", "authoritative"],
)
def test_runtime_handle_preserves_routed_exit_exception_identity(
    failure_owner: str,
) -> None:
    owners = RithmicRuntimeOwners(
        order_event_lifecycle=MagicMock(),
        is_rithmic_runtime=True,
        profile="test",
        account_id="ACCOUNT",
    )
    error = RuntimeError(failure_owner)
    resolver = MagicMock(return_value="portfolio")
    factory = MagicMock()
    owner = MagicMock()
    factory.return_value = owner
    owners.portfolio_exit_factory = factory
    authoritative = MagicMock(
        side_effect=lambda signal, candle, execute: execute(signal, MagicMock())
    )
    if failure_owner == "resolver":
        resolver.side_effect = error
    elif failure_owner == "factory":
        factory.side_effect = error
    elif failure_owner == "owner":
        owner.execute.side_effect = error
    else:
        authoritative.side_effect = error

    with pytest.raises(RuntimeError) as caught:
        owners.route_authoritative_exit(
            _exit_signal(),
            None,
            resolver,
            authoritative,
        )

    assert caught.value is error


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
    assert owners.is_rithmic_runtime is True
    assert owners.profile == "test"
    assert owners.account_id == "ACCOUNT"
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
    assert owners.is_rithmic_runtime is False
    assert owners.profile is None
    assert owners.account_id is None
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
