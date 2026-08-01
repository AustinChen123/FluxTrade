"""Single-order notional risk rule."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from src.core.models import Signal
from src.core.product_registry import InstrumentSpec, calculate_notional_exposure
from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.signal_order_intent import (
    InvalidSignalOrderIntent,
    resolve_signal_order_intent,
)


class SingleOrderNotionalRule:
    """Reject orders whose price * quantity exceeds the configured NAV share."""

    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def evaluate(
        self,
        signal: Signal,
        nav: Decimal,
        instrument_spec: InstrumentSpec | None = None,
    ) -> tuple[RuleStatus, Optional[str]]:
        try:
            resolved_intent = resolve_signal_order_intent(signal)
        except InvalidSignalOrderIntent as exc:
            return RuleStatus.REJECT, str(exc)
        if resolved_intent.limit_price is None:
            return RuleStatus.PASS, None
        if signal.quantity is None:
            return RuleStatus.REJECT, "single_order_notional_missing_quantity"
        if nav <= 0:
            return RuleStatus.REJECT, f"single_order_notional_invalid_nav: {nav}"

        notional = calculate_notional_exposure(
            signal.quantity,
            resolved_intent.limit_price,
            instrument_spec,
        )
        limit_notional = nav * self.config.max_single_order_notional_pct
        if notional > limit_notional:
            return (
                RuleStatus.REJECT,
                f"single_order_notional_exceeded: {notional} > {limit_notional}",
            )

        return RuleStatus.PASS, None
