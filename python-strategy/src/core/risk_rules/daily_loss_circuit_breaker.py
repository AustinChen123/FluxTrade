"""Daily loss circuit-breaker risk rule."""

from __future__ import annotations

from decimal import Decimal
from typing import Optional

from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus


class DailyLossCircuitBreakerRule:
    """Trigger a circuit breaker when intraday NAV loss exceeds threshold."""

    def __init__(self, config: RiskConfig) -> None:
        self.config = config

    def evaluate(
        self,
        *,
        start_nav: Decimal,
        current_nav: Decimal,
    ) -> tuple[RuleStatus, Optional[str]]:
        if start_nav <= 0:
            return RuleStatus.REJECT, f"daily_loss_invalid_start_nav: {start_nav}"

        loss = start_nav - current_nav
        if loss <= 0:
            return RuleStatus.PASS, None

        loss_pct = loss / start_nav
        if loss_pct > self.config.daily_loss_circuit_breaker_pct:
            return (
                RuleStatus.CIRCUIT_BREAKER_TRIGGERED,
                (
                    "daily_loss_circuit_breaker_triggered: "
                    f"loss={_pct(loss_pct)}% > "
                    f"{_pct(self.config.daily_loss_circuit_breaker_pct)}%"
                ),
            )

        return RuleStatus.PASS, None


def _pct(value: Decimal) -> Decimal:
    return (value * Decimal("100")).quantize(Decimal("0.01"))
