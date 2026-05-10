"""Strict audit helpers for signal and system events."""

from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from src.core.clock import Clock
from src.core.jsonb_helpers import serialize_payload_with_decimals
from src.core.models import Candlestick, Signal
from src.core.orm_models import SignalAudit


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
