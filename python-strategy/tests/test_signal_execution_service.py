from __future__ import annotations

from contextlib import contextmanager
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import src.core.signal_execution_service as signal_execution_module
from src.core.models import Candlestick, Signal, SignalType
from src.core.signal_execution_service import SignalExecutionService


def _signal(
    signal_type: SignalType = SignalType.LONG,
    *,
    quantity: Decimal | None = None,
    price: Decimal | None = None,
) -> Signal:
    return Signal(
        strategy_id="strategy-a",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=1_704_067_200_000,
        type=signal_type,
        quantity=quantity,
        price=price,
        value=Decimal("42000"),
    )


def _candle() -> Candlestick:
    return Candlestick(
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=1_704_067_200_000,
        open=Decimal("41990"),
        high=Decimal("42010"),
        low=Decimal("41980"),
        close=Decimal("42000"),
        volume=Decimal("2"),
    )


def _harness(*, audit_external_orders: bool = False) -> SimpleNamespace:
    trace: list[str] = []
    clock = MagicMock()
    clock.now.return_value = 1_704_067_200.0
    session = MagicMock()

    @contextmanager
    def session_factory():
        yield session

    check_risk = MagicMock(return_value=(True, "PASS"))
    route_authoritative_exit = MagicMock(return_value=(False, False))
    execute_signal = MagicMock(return_value="order-123")
    execute_authoritative_exit_signal = MagicMock(return_value=True)
    portfolio_id_for_sleeve = MagicMock(return_value=None)
    logger = MagicMock()
    service = SignalExecutionService(
        clock=clock,
        default_entry_quantity=lambda: Decimal("2"),
        check_risk=check_risk,
        route_authoritative_exit=route_authoritative_exit,
        execute_signal=execute_signal,
        execute_authoritative_exit_signal=lambda: execute_authoritative_exit_signal,
        portfolio_id_for_sleeve=lambda: portfolio_id_for_sleeve,
        audit_external_orders=lambda: audit_external_orders,
        db_session_factory=session_factory,
        event_logger=logger,
    )
    return SimpleNamespace(
        service=service,
        trace=trace,
        clock=clock,
        session=session,
        check_risk=check_risk,
        route_authoritative_exit=route_authoritative_exit,
        execute_signal=execute_signal,
        execute_authoritative_exit_signal=execute_authoritative_exit_signal,
        portfolio_id_for_sleeve=portfolio_id_for_sleeve,
        logger=logger,
    )


def test_no_signal_returns_without_transaction_work() -> None:
    harness = _harness()

    assert harness.service.process(_signal(SignalType.NO_SIGNAL), None) is True

    harness.check_risk.assert_not_called()
    harness.route_authoritative_exit.assert_not_called()
    harness.execute_signal.assert_not_called()
    harness.session.add.assert_not_called()
    harness.session.commit.assert_not_called()


def test_normalized_signal_flows_through_risk_route_execution_and_audit() -> None:
    harness = _harness()
    candle = _candle()

    def check_risk(signal: Signal, *, current_price: Decimal | None):
        harness.trace.append("risk")
        assert signal.quantity == Decimal("2")
        assert current_price == candle.close
        return True, "PASS"

    def route(signal: Signal, *_args):
        harness.trace.append("route")
        assert signal.quantity == Decimal("2")
        return False, False

    def execute(signal: Signal, actual_candle: Candlestick | None):
        harness.trace.append("execute")
        assert signal.quantity == Decimal("2")
        assert actual_candle is candle
        return "order-123"

    harness.check_risk.side_effect = check_risk
    harness.route_authoritative_exit.side_effect = route
    harness.execute_signal.side_effect = execute
    harness.session.add.side_effect = lambda _audit: harness.trace.append("audit")

    assert harness.service.process(_signal(), candle) is True

    assert harness.trace == ["risk", "route", "execute", "audit"]
    audit = harness.session.add.call_args.args[0]
    assert audit.strategy_id == "strategy-a"
    assert audit.risk_status == "PASS"
    assert audit.risk_message == "PASS"
    assert audit.order_id == "order-123"
    harness.session.commit.assert_called_once_with()


def test_invalid_intent_becomes_audited_rejection_before_risk_or_execution() -> None:
    harness = _harness()

    assert (
        harness.service.process(
            _signal(SignalType.EXIT_LONG, quantity=Decimal("1"), price=Decimal("0")),
            None,
        )
        is False
    )

    harness.check_risk.assert_not_called()
    harness.route_authoritative_exit.assert_not_called()
    harness.execute_signal.assert_not_called()
    audit = harness.session.add.call_args.args[0]
    assert audit.risk_status == "REJECT"
    assert audit.risk_message.startswith("REJECT: ")
    harness.session.commit.assert_called_once_with()


def test_risk_rejection_is_audited_without_execution() -> None:
    harness = _harness()
    harness.check_risk.return_value = (False, "REJECT: exposure")

    assert harness.service.process(_signal(quantity=Decimal("1")), _candle()) is False

    harness.route_authoritative_exit.assert_not_called()
    harness.execute_signal.assert_not_called()
    audit = harness.session.add.call_args.args[0]
    assert audit.risk_status == "REJECT"
    assert audit.risk_message == "REJECT: exposure"
    assert audit.order_id is None


@pytest.mark.parametrize(
    ("risk_result", "expected_status"),
    [((True, "PASS"), "PASS"), ((False, "REJECT: exposure"), "REJECT")],
)
def test_signal_metric_keeps_exact_identity_and_risk_status(
    monkeypatch: pytest.MonkeyPatch,
    risk_result: tuple[bool, str],
    expected_status: str,
) -> None:
    harness = _harness()
    harness.check_risk.return_value = risk_result
    metric = MagicMock()
    counter = MagicMock()
    metric.labels.return_value = counter
    monkeypatch.setattr(signal_execution_module, "SIGNALS_TOTAL", metric)

    harness.service.process(_signal(quantity=Decimal("1")), _candle())

    metric.labels.assert_called_once_with(
        strategy_id="strategy-a",
        signal_type="LONG",
        risk_status=expected_status,
    )
    counter.inc.assert_called_once_with()


@pytest.mark.parametrize("execution_succeeded", [False, True])
def test_handled_authoritative_exit_never_falls_through_to_generic_execution(
    execution_succeeded: bool,
) -> None:
    harness = _harness()
    signal = _signal(SignalType.EXIT_LONG, quantity=Decimal("1"))
    harness.route_authoritative_exit.return_value = (True, execution_succeeded)

    assert harness.service.process(signal, None) is execution_succeeded

    harness.route_authoritative_exit.assert_called_once_with(
        signal,
        None,
        harness.portfolio_id_for_sleeve,
        harness.execute_authoritative_exit_signal,
    )
    harness.execute_signal.assert_not_called()
    harness.session.add.assert_called_once()
    harness.session.commit.assert_called_once_with()


@pytest.mark.parametrize("execution_succeeded", [False, True])
def test_external_order_audit_owner_preserves_legacy_audit_early_return(
    execution_succeeded: bool,
) -> None:
    harness = _harness(audit_external_orders=True)
    harness.route_authoritative_exit.return_value = (True, execution_succeeded)

    assert (
        harness.service.process(
            _signal(SignalType.EXIT_SHORT, quantity=Decimal("1")),
            None,
        )
        is execution_succeeded
    )

    harness.execute_signal.assert_not_called()
    harness.session.add.assert_not_called()
    harness.session.commit.assert_not_called()


@pytest.mark.parametrize("failure_owner", ["route", "execute", "audit"])
def test_transaction_failures_propagate_original_exception(
    failure_owner: str,
) -> None:
    harness = _harness()
    failure = RuntimeError(f"{failure_owner}-failure")
    if failure_owner == "route":
        harness.route_authoritative_exit.side_effect = failure
    elif failure_owner == "execute":
        harness.execute_signal.side_effect = failure
    else:
        harness.session.commit.side_effect = failure

    with pytest.raises(RuntimeError) as caught:
        harness.service.process(_signal(quantity=Decimal("1")), _candle())

    assert caught.value is failure
    if failure_owner == "route":
        harness.execute_signal.assert_not_called()
        harness.session.add.assert_not_called()
    elif failure_owner == "execute":
        harness.session.add.assert_not_called()
    else:
        harness.session.rollback.assert_called_once_with()
