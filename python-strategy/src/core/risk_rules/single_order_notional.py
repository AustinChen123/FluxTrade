"""Single-order notional risk rule."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from src.core.models import Signal
from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus


class SingleOrderNotionalRule:
    """Reject orders whose price * quantity exceeds the configured NAV share."""

    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def evaluate(self, signal: Signal, nav: Decimal) -> tuple[RuleStatus, Optional[str]]:
        if signal.price is None:
            return RuleStatus.PASS, None
        if signal.quantity is None:
            return RuleStatus.REJECT, "single_order_notional_missing_quantity"
        if nav <= 0:
            return RuleStatus.REJECT, f"single_order_notional_invalid_nav: {nav}"

        notional = signal.price * signal.quantity
        limit_notional = nav * self.config.max_single_order_notional_pct
        if notional > limit_notional:
            return (
                RuleStatus.REJECT,
                f"single_order_notional_exceeded: {notional} > {limit_notional}",
            )

        return RuleStatus.PASS, None
