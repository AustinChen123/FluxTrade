"""Maximum projected position notional risk rule."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from src.core.models import Position, PositionSide, Signal, SignalType
from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus


class MaxPositionNotionalRule:
    """Reject entries that would exceed the configured position notional limit."""

    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def evaluate(
        self,
        signal: Signal,
        current_position: Optional[Position],
        mid_price: Decimal,
    ) -> tuple[RuleStatus, Optional[str]]:
        if signal.type in {SignalType.NO_SIGNAL, SignalType.EXIT_LONG, SignalType.EXIT_SHORT}:
            return RuleStatus.PASS, None
        if signal.quantity is None:
            return RuleStatus.REJECT, "max_position_notional_missing_quantity"
        if mid_price <= 0:
            return RuleStatus.REJECT, f"max_position_notional_invalid_mid_price: {mid_price}"

        order_price = signal.price if signal.price is not None else mid_price
        current_notional = _signed_position_notional(current_position, mid_price)
        order_notional = _signed_order_notional(signal, order_price)
        total_notional = abs(current_notional + order_notional)

        if total_notional > self.config.max_position_notional:
            return (
                RuleStatus.REJECT,
                (
                    "max_position_notional_exceeded: "
                    f"{total_notional} > {self.config.max_position_notional}"
                ),
            )

        return RuleStatus.PASS, None


def _signed_position_notional(
    current_position: Optional[Position],
    mid_price: Decimal,
) -> Decimal:
    if current_position is None:
        return Decimal("0")
    sign = Decimal("1") if current_position.side == PositionSide.LONG else Decimal("-1")
    return sign * current_position.quantity * mid_price


def _signed_order_notional(signal: Signal, order_price: Decimal) -> Decimal:
    sign = Decimal("1") if signal.type == SignalType.LONG else Decimal("-1")
    return sign * signal.quantity * order_price
