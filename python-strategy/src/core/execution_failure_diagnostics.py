import logging
from typing import Any, Callable, ContextManager

from sqlalchemy.orm import Session

from src.core.audit_service import write_system_event
from src.core.interfaces.exchange import ExchangeError
from src.core.metrics import ORDERS_TOTAL


def order_rejection_reason(error: ExchangeError) -> str:
    message = str(error)
    token = message.split(":", 1)[0].strip()
    normalized = "".join(
        char if char.isalnum() else "_" for char in token.lower()
    ).strip("_")
    return normalized or "exchange_error"


def write_order_rejection_event(
    db: Session,
    *,
    order: Any,
    order_type: str,
    reason: str,
    error: ExchangeError,
    phase: str,
) -> None:
    write_system_event(
        db,
        event_type="system_error",
        event_subtype="order_rejected",
        related_strategy_id=order.strategy_id,
        related_order_id=str(order.id),
        payload={
            "order_id": str(order.id),
            "product_id": order.product_id,
            "order_type": order_type,
            "phase": phase,
            "reason": reason,
            "error": str(error),
        },
    )


def try_write_order_rejection_event(
    *,
    db_session_factory: Callable[[], ContextManager[Session]] | None,
    logger: logging.Logger,
    order: Any,
    order_type: str,
    reason: str,
    error: ExchangeError,
    phase: str,
) -> None:
    if db_session_factory is None:
        return
    try:
        with db_session_factory() as db:
            write_order_rejection_event(
                db,
                order=order,
                order_type=order_type,
                reason=reason,
                error=error,
                phase=phase,
            )
            db.commit()
    except Exception:
        logger.exception("Failed to write order rejection system event")


def record_order_rejection(
    *,
    db_session_factory: Callable[[], ContextManager[Session]] | None,
    logger: logging.Logger,
    order: Any,
    order_type: str,
    error: ExchangeError,
    phase: str,
    write_event: bool = True,
) -> str:
    reason = order_rejection_reason(error)
    ORDERS_TOTAL.labels(
        order_type=order_type,
        status="failed",
        reason=reason,
    ).inc()
    if write_event:
        try_write_order_rejection_event(
            db_session_factory=db_session_factory,
            logger=logger,
            order=order,
            order_type=order_type,
            reason=reason,
            error=error,
            phase=phase,
        )
    return reason


def try_write_conditional_order_event_warning(
    *,
    db_session_factory: Callable[[], ContextManager[Session]] | None,
    logger: logging.Logger,
    event_subtype: str,
    order: Any,
    failures: list[dict[str, Any]],
) -> None:
    if db_session_factory is None:
        return
    try:
        with db_session_factory() as db:
            write_system_event(
                db,
                event_type="system_error",
                event_subtype=event_subtype,
                related_strategy_id=order.strategy_id,
                related_order_id=str(order.id),
                payload={
                    "order_id": str(order.id),
                    "product_id": order.product_id,
                    "failures": failures,
                },
            )
            db.commit()
    except Exception:
        logger.exception("Failed to write conditional order warning event")
