import ast
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from src.core import execution_failure_diagnostics as diagnostics
from src.core.execution import ExecutionEngine
from src.core.interfaces.exchange import ExchangeError


def _order():
    return SimpleNamespace(
        id="order-1",
        strategy_id="strategy-1",
        product_id="BINANCE:BTCUSDT-PERP",
    )


class _SessionContext:
    def __init__(self, session, *, enter_error=None, exit_error=None):
        self.session = session
        self.enter_error = enter_error
        self.exit_error = exit_error

    def __enter__(self):
        if self.enter_error is not None:
            raise self.enter_error
        return self.session

    def __exit__(self, *_args):
        if self.exit_error is not None:
            raise self.exit_error
        return False


def test_order_rejection_reason_uses_exact_literal_matrix():
    cases = [
        ("min_notional_not_met: value", "min_notional_not_met"),
        ("  Network Error  : detail", "network_error"),
        ("A!!B??C: detail", "a__b__c"),
        ("Ünicode 交易: detail", "ünicode_交易"),
        (": detail", "exchange_error"),
        ("!!!: detail", "exchange_error"),
        ("", "exchange_error"),
    ]

    assert [
        diagnostics.order_rejection_reason(ExchangeError(value)) for value, _ in cases
    ] == [expected for _, expected in cases]


def test_pure_rejection_projection_has_exact_shape_and_no_transaction_control(
    monkeypatch,
):
    write_event = MagicMock()
    monkeypatch.setattr(diagnostics, "write_system_event", write_event)
    session = MagicMock()
    error = ExchangeError("min_notional_not_met: value")

    diagnostics.write_order_rejection_event(
        session,
        order=_order(),
        order_type="market",
        reason="min_notional_not_met",
        error=error,
        phase="audited_execution",
    )

    write_event.assert_called_once_with(
        session,
        event_type="system_error",
        event_subtype="order_rejected",
        related_strategy_id="strategy-1",
        related_order_id="order-1",
        payload={
            "order_id": "order-1",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "order_type": "market",
            "phase": "audited_execution",
            "reason": "min_notional_not_met",
            "error": "min_notional_not_met: value",
        },
    )
    session.commit.assert_not_called()
    session.rollback.assert_not_called()


def test_pure_rejection_projection_preserves_write_error_identity(monkeypatch):
    error = RuntimeError("write sentinel")
    monkeypatch.setattr(diagnostics, "write_system_event", MagicMock(side_effect=error))
    session = MagicMock()

    with pytest.raises(RuntimeError) as raised:
        diagnostics.write_order_rejection_event(
            session,
            order=_order(),
            order_type="market",
            reason="network_error",
            error=ExchangeError("Network Error"),
            phase="audited_execution",
        )

    assert raised.value is error
    session.commit.assert_not_called()
    session.rollback.assert_not_called()


def test_record_rejection_metrics_precede_optional_persistence(monkeypatch):
    labels = MagicMock()
    metric = MagicMock()
    metric.labels.return_value = labels
    persist = MagicMock()
    monkeypatch.setattr(diagnostics, "ORDERS_TOTAL", metric)
    monkeypatch.setattr(diagnostics, "try_write_order_rejection_event", persist)
    factory = MagicMock()
    logger = MagicMock()
    error = ExchangeError("Network Error: disconnected")
    order = _order()

    reason = diagnostics.record_order_rejection(
        db_session_factory=factory,
        logger=logger,
        order=order,
        order_type="limit",
        error=error,
        phase="execution",
    )

    assert reason == "network_error"
    metric.labels.assert_called_once_with(
        order_type="limit", status="failed", reason="network_error"
    )
    labels.inc.assert_called_once_with()
    persist.assert_called_once_with(
        db_session_factory=factory,
        logger=logger,
        order=order,
        order_type="limit",
        reason="network_error",
        error=error,
        phase="execution",
    )


@pytest.mark.parametrize("write_event,factory", [(False, MagicMock()), (True, None)])
def test_record_rejection_without_persistence_still_records_metric(
    monkeypatch, write_event, factory
):
    labels = MagicMock()
    metric = MagicMock()
    metric.labels.return_value = labels
    persist = MagicMock()
    monkeypatch.setattr(diagnostics, "ORDERS_TOTAL", metric)
    monkeypatch.setattr(diagnostics, "try_write_order_rejection_event", persist)

    diagnostics.record_order_rejection(
        db_session_factory=factory,
        logger=MagicMock(),
        order=_order(),
        order_type="market",
        error=ExchangeError("rejected"),
        phase="execution",
        write_event=write_event,
    )

    labels.inc.assert_called_once_with()
    if write_event:
        persist.assert_called_once()
        assert persist.call_args.kwargs["db_session_factory"] is None
    else:
        persist.assert_not_called()
        factory.assert_not_called()


@pytest.mark.parametrize("wrapper", ["rejection", "conditional"])
def test_best_effort_persistence_without_factory_is_silent(monkeypatch, wrapper):
    logger = MagicMock()
    projection = MagicMock()
    if wrapper == "rejection":
        monkeypatch.setattr(diagnostics, "write_order_rejection_event", projection)
        diagnostics.try_write_order_rejection_event(
            db_session_factory=None,
            logger=logger,
            order=_order(),
            order_type="market",
            reason="network_error",
            error=ExchangeError("Network Error"),
            phase="execution",
        )
    else:
        monkeypatch.setattr(diagnostics, "write_system_event", projection)
        diagnostics.try_write_conditional_order_event_warning(
            db_session_factory=None,
            logger=logger,
            event_subtype="conditional_order_warning",
            order=_order(),
            failures=[],
        )

    projection.assert_not_called()
    logger.exception.assert_not_called()


def test_record_rejection_persistence_failure_keeps_exact_metric(monkeypatch):
    labels = MagicMock()
    metric = MagicMock()
    metric.labels.return_value = labels
    monkeypatch.setattr(diagnostics, "ORDERS_TOTAL", metric)
    error = RuntimeError("factory sentinel")
    factory = MagicMock(side_effect=error)
    logger = MagicMock()
    contexts = []
    logger.exception.side_effect = lambda *_args: contexts.append(sys.exception())

    reason = diagnostics.record_order_rejection(
        db_session_factory=factory,
        logger=logger,
        order=_order(),
        order_type="market",
        error=ExchangeError("Network Error"),
        phase="execution",
    )

    assert reason == "network_error"
    metric.labels.assert_called_once_with(
        order_type="market", status="failed", reason="network_error"
    )
    labels.inc.assert_called_once_with()
    logger.exception.assert_called_once_with(
        "Failed to write order rejection system event"
    )
    assert contexts == [error]


@pytest.mark.parametrize("failure_stage", ["labels", "inc"])
def test_metric_failure_propagates_before_persistence(monkeypatch, failure_stage):
    error = RuntimeError(f"{failure_stage} sentinel")
    metric = MagicMock()
    labels = MagicMock()
    metric.labels.return_value = labels
    if failure_stage == "labels":
        metric.labels.side_effect = error
    else:
        labels.inc.side_effect = error
    persist = MagicMock()
    monkeypatch.setattr(diagnostics, "ORDERS_TOTAL", metric)
    monkeypatch.setattr(diagnostics, "try_write_order_rejection_event", persist)

    with pytest.raises(RuntimeError) as raised:
        diagnostics.record_order_rejection(
            db_session_factory=MagicMock(),
            logger=MagicMock(),
            order=_order(),
            order_type="market",
            error=ExchangeError("rejected"),
            phase="execution",
        )

    assert raised.value is error
    persist.assert_not_called()


@pytest.mark.parametrize("wrapper", ["rejection", "conditional"])
@pytest.mark.parametrize(
    "failure_stage", ["factory", "enter", "write", "commit", "exit"]
)
def test_best_effort_persistence_suppresses_each_failure_with_original_context(
    monkeypatch, wrapper, failure_stage
):
    error = RuntimeError(f"{wrapper} {failure_stage} sentinel")
    session = MagicMock()
    enter_error = error if failure_stage == "enter" else None
    exit_error = error if failure_stage == "exit" else None
    context = _SessionContext(session, enter_error=enter_error, exit_error=exit_error)
    factory = MagicMock(return_value=context)
    if failure_stage == "factory":
        factory.side_effect = error
    if failure_stage == "commit":
        session.commit.side_effect = error
    logger = MagicMock()
    contexts = []
    logger.exception.side_effect = lambda *_args: contexts.append(sys.exception())

    if wrapper == "rejection":
        projection = MagicMock()
        if failure_stage == "write":
            projection.side_effect = error
        monkeypatch.setattr(diagnostics, "write_order_rejection_event", projection)
        diagnostics.try_write_order_rejection_event(
            db_session_factory=factory,
            logger=logger,
            order=_order(),
            order_type="market",
            reason="network_error",
            error=ExchangeError("Network Error"),
            phase="execution",
        )
        message = "Failed to write order rejection system event"
    else:
        projection = MagicMock()
        if failure_stage == "write":
            projection.side_effect = error
        monkeypatch.setattr(diagnostics, "write_system_event", projection)
        diagnostics.try_write_conditional_order_event_warning(
            db_session_factory=factory,
            logger=logger,
            event_subtype="conditional_order_warning",
            order=_order(),
            failures=[{"reason": "pending"}],
        )
        message = "Failed to write conditional order warning event"

    logger.exception.assert_called_once_with(message)
    assert contexts == [error]
    session.rollback.assert_not_called()
    assert session.commit.call_count == (
        1 if failure_stage in {"commit", "exit"} else 0
    )


def test_successful_rejection_persistence_projects_then_commits_once(monkeypatch):
    projection = MagicMock()
    monkeypatch.setattr(diagnostics, "write_order_rejection_event", projection)
    session = MagicMock()
    factory = MagicMock(return_value=_SessionContext(session))
    order = _order()
    error = ExchangeError("Network Error")

    diagnostics.try_write_order_rejection_event(
        db_session_factory=factory,
        logger=MagicMock(),
        order=order,
        order_type="market",
        reason="network_error",
        error=error,
        phase="execution",
    )

    projection.assert_called_once_with(
        session,
        order=order,
        order_type="market",
        reason="network_error",
        error=error,
        phase="execution",
    )
    session.commit.assert_called_once_with()
    session.rollback.assert_not_called()


def test_successful_conditional_warning_has_exact_shape_and_one_commit(monkeypatch):
    write_event = MagicMock()
    monkeypatch.setattr(diagnostics, "write_system_event", write_event)
    session = MagicMock()
    factory = MagicMock(return_value=_SessionContext(session))
    failures = [{"order_id": "child-1", "reason": "pending"}]

    diagnostics.try_write_conditional_order_event_warning(
        db_session_factory=factory,
        logger=MagicMock(),
        event_subtype="conditional_order_warning",
        order=_order(),
        failures=failures,
    )

    write_event.assert_called_once_with(
        session,
        event_type="system_error",
        event_subtype="conditional_order_warning",
        related_strategy_id="strategy-1",
        related_order_id="order-1",
        payload={
            "order_id": "order-1",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "failures": failures,
        },
    )
    session.commit.assert_called_once_with()
    session.rollback.assert_not_called()


def test_execution_facades_resolve_current_dependencies_and_owner_functions(
    monkeypatch,
):
    engine = ExecutionEngine.__new__(ExecutionEngine)
    first_factory = MagicMock(name="first_factory")
    first_logger = MagicMock(name="first_logger")
    engine._db_session_factory = first_factory
    engine.logger = first_logger
    rejection_owner = MagicMock(return_value="network_error")
    monkeypatch.setattr(diagnostics, "record_order_rejection", rejection_owner)

    assert (
        engine._record_order_rejection(
            order=_order(),
            order_type="market",
            error=ExchangeError("Network Error"),
            phase="execution",
        )
        == "network_error"
    )
    assert rejection_owner.call_args.kwargs["db_session_factory"] is first_factory
    assert rejection_owner.call_args.kwargs["logger"] is first_logger

    second_factory = MagicMock(name="second_factory")
    second_logger = MagicMock(name="second_logger")
    engine._db_session_factory = second_factory
    engine.logger = second_logger
    conditional_owner = MagicMock()
    monkeypatch.setattr(
        diagnostics, "try_write_conditional_order_event_warning", conditional_owner
    )
    failures = [{"reason": "pending"}]

    engine._try_write_conditional_order_event_warning(
        event_subtype="conditional_order_warning",
        order=_order(),
        failures=failures,
    )

    assert conditional_owner.call_args.kwargs["db_session_factory"] is second_factory
    assert conditional_owner.call_args.kwargs["logger"] is second_logger
    assert conditional_owner.call_args.kwargs["failures"] is failures


def test_owner_module_has_narrow_dependency_and_function_boundary():
    source = Path(diagnostics.__file__).read_text(encoding="utf-8")
    tree = ast.parse(source)
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module is not None:
            imports.add(node.module)
    functions = {
        node.name
        for node in tree.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert imports == {
        "logging",
        "typing",
        "sqlalchemy.orm",
        "src.core.audit_service",
        "src.core.interfaces.exchange",
        "src.core.metrics",
    }
    assert functions == {
        "order_rejection_reason",
        "write_order_rejection_event",
        "try_write_order_rejection_event",
        "record_order_rejection",
        "try_write_conditional_order_event_warning",
    }
    compact_source = source.lower().replace("_", "")
    for forbidden in (
        "ordermanager",
        "repository",
        "adapter",
        "adoption",
        "submit",
        "submission",
        "reconcile",
        "protection",
        "position",
        "signal",
        "rithmic",
        "binance",
        "backpack",
        "bybit",
        "okx",
    ):
        assert forbidden not in compact_source
