"""Price sanity risk rule."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from src.core.models import Signal
from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.signal_order_intent import (
    InvalidSignalOrderIntent,
    resolve_signal_order_intent,
)


class PriceSanityCheckRule:
    """Reject limit prices that deviate too far from current mid price."""

    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def evaluate(
        self,
        signal: Signal,
        *,
        best_bid: Optional[Decimal],
        best_ask: Optional[Decimal],
    ) -> tuple[RuleStatus, Optional[str]]:
        try:
            resolved_intent = resolve_signal_order_intent(signal)
        except InvalidSignalOrderIntent as exc:
            return RuleStatus.REJECT, str(exc)
        if resolved_intent.limit_price is None:
            return RuleStatus.PASS, None
        if best_bid is None or best_ask is None:
            return RuleStatus.REJECT, "price_sanity_check_missing_market"
        if best_bid <= 0 or best_ask <= 0 or best_bid > best_ask:
            return RuleStatus.REJECT, "price_sanity_check_invalid_market"

        mid = (best_bid + best_ask) / Decimal("2")
        deviation_pct = abs(resolved_intent.limit_price - mid) / mid
        if deviation_pct > self.config.max_price_deviation_from_mid_pct:
            return (
                RuleStatus.REJECT,
                (
                    "price_sanity_check_failed: "
                    f"deviation={deviation_pct}% > "
                    f"{self.config.max_price_deviation_from_mid_pct}%"
                ),
            )

        return RuleStatus.PASS, None
