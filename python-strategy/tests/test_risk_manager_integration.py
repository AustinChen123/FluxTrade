"""Integration-style tests for RiskManager rule orchestration."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock

from src.core.models import SignalType
from src.core.risk_config import RiskConfig
from src.core.risk_manager import RiskManager
from src.core.risk_rules import RuleStatus


class _RecordingRateLimitRule:
    def __init__(self, status=RuleStatus.PASS, reason=None):
        self.status = status
        self.reason = reason
        self.calls = []

    def try_record_order(self, strategy_id):
        self.calls.append(strategy_id)
        return self.status, self.reason


class _DailyNavService:
    def __init__(self, nav):
        self.nav = nav
        self.calls = []

    def get_start_nav(self, strategy_id, snapshot_date):
        self.calls.append((strategy_id, snapshot_date))
        return self.nav


def test_risk_manager_short_circuits_before_rate_limit(
    mock_account_service,
    signal_factory,
) -> None:
    mock_account_service.set_balance(Decimal("100000"))
    rate_limit = _RecordingRateLimitRule()
    risk_manager = RiskManager(
        mock_account_service,
        order_rate_limit_rule=rate_limit,
    )
    signal = signal_factory(
        signal_type=SignalType.LONG,
        price=Decimal("60000"),
        quantity=Decimal("0.1"),
    )

    allowed, reason = risk_manager.check_risk(signal)

    assert allowed is False
    assert "single_order_notional_exceeded" in reason
    assert rate_limit.calls == []


def test_risk_manager_circuit_breaker_updates_state_from_snapshot(
    mock_account_service,
    signal_factory,
) -> None:
    mock_account_service.set_balance(Decimal("100000"))
    state_manager = MagicMock()
    daily_nav = _DailyNavService(Decimal("100000"))
    rate_limit = _RecordingRateLimitRule()
    risk_manager = RiskManager(
        mock_account_service,
        daily_nav_service=daily_nav,
        order_rate_limit_rule=rate_limit,
        state_manager=state_manager,
    )
    signal = signal_factory(signal_type=SignalType.LONG)

    allowed, reason = risk_manager.check_risk(
        signal,
        current_nav=Decimal("94990"),
        snapshot_date=date(2026, 5, 18),
    )

    assert allowed is False
    assert "daily_loss_circuit_breaker_triggered" in reason
    assert daily_nav.calls == [("test_strategy", date(2026, 5, 18))]
    assert rate_limit.calls == []
    state_manager.transition_to_error.assert_called_once_with(
        "test_strategy",
        reason.removeprefix("REJECT: "),
        actor="system",
    )


def test_risk_manager_market_order_passes_and_records_rate_limit(
    mock_account_service,
    signal_factory,
) -> None:
    mock_account_service.set_balance(Decimal("100000"))
    rate_limit = _RecordingRateLimitRule()
    risk_manager = RiskManager(
        mock_account_service,
        order_rate_limit_rule=rate_limit,
        risk_config=RiskConfig(max_position_notional=Decimal("100000")),
    )
    signal = signal_factory(
        signal_type=SignalType.LONG,
        price=None,
        quantity=Decimal("0.1"),
    )

    allowed, reason = risk_manager.check_risk(
        signal,
        best_bid=Decimal("99"),
        best_ask=Decimal("101"),
        daily_start_nav=Decimal("100000"),
        current_nav=Decimal("100000"),
    )

    assert allowed is True
    assert reason == "PASS"
    assert rate_limit.calls == ["test_strategy"]
