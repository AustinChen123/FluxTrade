"""Venue-neutral orchestration for one admitted strategy signal."""

from __future__ import annotations

import logging
import uuid
from decimal import Decimal
from typing import Callable, ContextManager, Protocol

from sqlalchemy.orm import Session

from src.core.audit_service import build_signal_audit, commit_signal_audit
from src.core.clock import Clock
from src.core.execution import ExitDecision
from src.core.metrics import SIGNALS_TOTAL
from src.core.models import Candlestick, Signal, SignalType
from src.core.signal_order_intent import (
    InvalidSignalOrderIntent,
    normalize_signal_quantity,
    resolve_signal_order_intent,
)


class RiskCheck(Protocol):
    def __call__(
        self,
        signal: Signal,
        *,
        current_price: Decimal | None,
    ) -> tuple[bool, str]: ...


AuthoritativeExitExecutor = Callable[
    [Signal, Candlestick | None, Callable[[Signal, ExitDecision], dict[str, object]]],
    bool,
]
PortfolioIdResolver = Callable[[str], str | None]
AuthoritativeExitRouter = Callable[
    [
        Signal,
        Candlestick | None,
        PortfolioIdResolver,
        AuthoritativeExitExecutor,
    ],
    tuple[bool, bool],
]


class SignalExecutionService:
    """Run the existing admitted-signal transaction in its exact order."""

    def __init__(
        self,
        *,
        clock: Clock,
        default_entry_quantity: Callable[[], Decimal],
        check_risk: RiskCheck,
        route_authoritative_exit: AuthoritativeExitRouter,
        execute_signal: Callable[[Signal, Candlestick | None], str | None],
        execute_authoritative_exit_signal: Callable[[], AuthoritativeExitExecutor],
        portfolio_id_for_sleeve: Callable[[], PortfolioIdResolver],
        audit_external_orders: Callable[[], bool],
        db_session_factory: Callable[[], ContextManager[Session]],
        event_logger: logging.Logger,
    ) -> None:
        self._clock = clock
        self._default_entry_quantity = default_entry_quantity
        self._check_risk = check_risk
        self._route_authoritative_exit = route_authoritative_exit
        self._execute_signal = execute_signal
        self._execute_authoritative_exit_signal = execute_authoritative_exit_signal
        self._portfolio_id_for_sleeve = portfolio_id_for_sleeve
        self._audit_external_orders = audit_external_orders
        self._db_session_factory = db_session_factory
        self._logger = event_logger

    def process(
        self,
        signal: Signal,
        candle: Candlestick | None,
    ) -> bool:
        """Process one signal after Engine has completed admission policy."""
        if signal.type == SignalType.NO_SIGNAL:
            return True

        import structlog.contextvars

        structlog.contextvars.bind_contextvars(trace_id=uuid.uuid4().hex[:16])

        current_price = candle.close if candle else None
        try:
            signal = normalize_signal_quantity(
                signal,
                default_entry_quantity=self._default_entry_quantity(),
            )
            resolve_signal_order_intent(signal)
        except InvalidSignalOrderIntent as exc:
            is_passed = False
            risk_msg = f"REJECT: {exc}"
            self._logger.warning("RISK_REJECTED: %s", risk_msg)
        else:
            is_passed, risk_msg = self._check_risk(
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
        execution_succeeded = False
        if is_passed:
            self._logger.info(
                "✅ SIGNAL ACCEPTED: %s. Forwarding to Execution Engine...",
                signal.type,
            )
            handled, execution_succeeded = self._route_authoritative_exit(
                signal,
                candle,
                self._portfolio_id_for_sleeve(),
                self._execute_authoritative_exit_signal(),
            )
            if not handled:
                order_id = self._execute_signal(signal, candle)
                execution_succeeded = order_id is not None
            if self._audit_external_orders():
                return execution_succeeded

        audit = build_signal_audit(
            clock=self._clock,
            signal=signal,
            candle=candle,
            risk_passed=is_passed,
            risk_message=risk_msg,
            order_id=order_id,
        )
        with self._db_session_factory() as db:
            commit_signal_audit(db, audit)
        return execution_succeeded
