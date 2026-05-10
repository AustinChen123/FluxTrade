"""Strict audit helpers for signal and system events."""

from __future__ import annotations

from typing import Any, Optional

from sqlalchemy.orm import Session

from src.core.clock import Clock
from src.core.jsonb_helpers import serialize_payload_with_decimals
from src.core.models import Candlestick, Signal
from src.core.orm_models import SignalAudit, SystemEvent


SYSTEM_EVENT_TYPES = frozenset(
    {
        "reconcile",
        "gene_promote",
        "gene_retire",
        "system_error",
    }
)


def build_signal_audit(
    *,
    clock: Clock,
    signal: Signal,
    candle: Optional[Candlestick],
    risk_passed: bool,
    risk_message: str,
    order_id: Optional[str],
) -> SignalAudit:
    """Build a SignalAudit row with a JSONB-native details payload."""
    details = serialize_payload_with_decimals(
        {
            "candle": candle.model_dump(mode="json") if candle else None,
            "signal_metadata": signal.metadata,
        }
    )
    return SignalAudit(
        timestamp=int(clock.now() * 1000),
        strategy_id=signal.strategy_id,
        product_id=signal.product_id,
        signal_type=signal.type.value,
        risk_status="PASS" if risk_passed else "REJECT",
        risk_message=risk_message,
        order_id=order_id,
        details_json=details,
    )


def commit_signal_audit(session: Session, audit: SignalAudit) -> None:
    """Persist an audit row strictly: rollback and raise on any failure."""
    try:
        session.add(audit)
        session.commit()
    except Exception:
        session.rollback()
        raise


def build_signal_intent_audit(
    *,
    clock: Clock,
    signal: Signal,
    client_order_id: str,
    intent_payload: dict[str, Any],
    signal_batch_id: Optional[str] = None,
) -> SignalAudit:
    """Build a committed-intent audit row for external order execution."""
    return SignalAudit(
        timestamp=int(clock.now() * 1000),
        strategy_id=signal.strategy_id,
        product_id=signal.product_id,
        signal_type=signal.type.value,
        risk_status="PASS",
        risk_message="PASS",
        client_order_id=client_order_id,
        intent_payload=serialize_payload_with_decimals(intent_payload),
        signal_batch_id=signal_batch_id,
    )


def write_signal_audit_intent(session: Session, audit: SignalAudit) -> SignalAudit:
    """Commit an external-execution intent audit row before the side effect."""
    try:
        session.add(audit)
        session.flush()
        session.commit()
    except Exception:
        session.rollback()
        raise
    return audit


def write_signal_audit_outcome(
    session: Session,
    audit: SignalAudit,
    *,
    outcome_payload: dict[str, Any],
    order_id: Optional[str] = None,
    risk_message: Optional[str] = None,
) -> SignalAudit:
    """Commit an external-execution outcome on an existing audit row."""
    audit.outcome_payload = serialize_payload_with_decimals(outcome_payload)
    if order_id is not None:
        audit.order_id = order_id
    if risk_message is not None:
        audit.risk_message = risk_message

    try:
        session.add(audit)
        session.commit()
    except Exception:
        session.rollback()
        raise
    return audit


def write_system_event(
    session: Session,
    *,
    event_type: str,
    payload: dict[str, Any],
    event_subtype: Optional[str] = None,
    related_strategy_id: Optional[str] = None,
    related_order_id: Optional[str] = None,
    related_gene_id: Optional[int] = None,
) -> SystemEvent:
    """Add a system event to the caller-controlled transaction."""
    if event_type not in SYSTEM_EVENT_TYPES:
        raise ValueError(f"unsupported system event type: {event_type}")

    event = SystemEvent(
        event_type=event_type,
        event_subtype=event_subtype,
        related_strategy_id=related_strategy_id,
        related_order_id=related_order_id,
        related_gene_id=related_gene_id,
        payload=serialize_payload_with_decimals(payload),
    )
    session.add(event)
    return event
