"""Reject duplicate same-side entries when a position already exists."""

from __future__ import annotations

from typing import Optional

from src.core.models import Position, PositionSide, Signal, SignalType
from src.core.risk_rules import RuleStatus


class ExistingPositionEntryRule:
    """Optional restart-idempotency guard for entry signals."""

    def evaluate(
        self,
        signal: Signal,
        current_position: Optional[Position],
    ) -> tuple[RuleStatus, Optional[str]]:
        if current_position is None or current_position.quantity <= 0:
            return RuleStatus.PASS, None
        expected_side: PositionSide | None = None
        if signal.type == SignalType.LONG:
            expected_side = PositionSide.LONG
        elif signal.type == SignalType.SHORT:
            expected_side = PositionSide.SHORT
        else:
            return RuleStatus.PASS, None

        position_side = getattr(current_position.side, "value", current_position.side)
        if position_side != expected_side.value:
            return RuleStatus.PASS, None
        return (
            RuleStatus.REJECT,
            (
                "existing_position_entry_duplicate: "
                f"side={position_side} quantity={current_position.quantity}"
            ),
        )
