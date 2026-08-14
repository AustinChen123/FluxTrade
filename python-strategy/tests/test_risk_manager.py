"""
Tests for src/core/risk_manager.py

Covers:
- Balance checks (zero, positive, negative scenarios)
- Position exposure limits
- Entry vs exit signal handling
- Position size calculation
- Edge cases
- AccountService with Redis mock
"""

from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from src.core.models import Position, PositionSide, SignalType
from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.risk_rules.existing_position_entry import ExistingPositionEntryRule
from src.core.risk_manager import RiskManager, AccountService
from src.core.product_registry import InstrumentSpec
from src.core.runtime_environment import RuntimeEnvironment


class _FakeOrderRateLimitRule:
    def __init__(self, status=RuleStatus.PASS, reason=None):
        self.status = status
        self.reason = reason
        self.calls = []

    def try_record_order(self, strategy_id):
        self.calls.append(strategy_id)
        return self.status, self.reason


class _FakeDailyNavService:
    def __init__(self, nav):
        self.nav = nav
        self.calls = []

    def get_start_nav(self, strategy_id, snapshot_date):
        self.calls.append((strategy_id, snapshot_date))
        return self.nav



class _PassExistingPositionEntryRule:
    """Permissive stub so exposure tests exercise exposure semantics, not the
    default-on duplicate-entry rule."""

    def evaluate(self, signal, current_position):
        from src.core.risk_rules import RuleStatus
        return RuleStatus.PASS, None


class TestRiskManagerBalanceChecks:
    """Tests for balance-related risk checks."""

    def test_reject_entry_on_zero_balance(self, mock_account_service, signal_factory):
        """Entry signals should be rejected when balance is zero."""
        mock_account_service.set_balance(Decimal("0"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG, value=None)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False
        assert "balance" in reason.lower()

    def test_reject_short_entry_on_zero_balance(self, mock_account_service, signal_factory):
        """SHORT entry should also be rejected on zero balance."""
        mock_account_service.set_balance(Decimal("0"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.SHORT)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False

    def test_allow_exit_on_zero_balance(self, mock_account_service, signal_factory):
        """Exit signals should be allowed even with zero balance (stop loss)."""
        mock_account_service.set_balance(Decimal("0"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.EXIT_LONG)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True

    def test_allow_exit_short_on_zero_balance(self, mock_account_service, signal_factory):
        """EXIT_SHORT should also be allowed on zero balance."""
        mock_account_service.set_balance(Decimal("0"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.EXIT_SHORT)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True

    @pytest.mark.parametrize(
        "signal_type",
        [SignalType.EXIT_LONG, SignalType.EXIT_SHORT],
    )
    def test_exit_does_not_read_unavailable_balance(
        self,
        mock_account_service,
        signal_factory,
        signal_type,
    ):
        mock_account_service.get_balance = MagicMock(
            side_effect=RuntimeError("authoritative_balance_snapshot_stale")
        )
        risk_manager = RiskManager(mock_account_service)

        allowed, reason = risk_manager.check_risk(
            signal_factory(signal_type=signal_type)
        )

        assert allowed is True
        assert reason == "PASS"
        mock_account_service.get_balance.assert_not_called()

    @pytest.mark.parametrize(
        "signal_type",
        [SignalType.NO_SIGNAL, SignalType.EXIT_LONG, SignalType.EXIT_SHORT],
    )
    def test_risk_reducing_signals_do_not_resolve_instrument_metadata(
        self, mock_account_service, signal_factory, signal_type
    ):
        def fail_if_called(product_id):
            raise RuntimeError(f"metadata unavailable for {product_id}")

        risk_manager = RiskManager(
            mock_account_service,
            instrument_spec_resolver=fail_if_called,
        )

        allowed, reason = risk_manager.check_risk(
            signal_factory(signal_type=signal_type)
        )

        assert allowed is True
        assert reason in {"NO_SIGNAL", "PASS"}

    @pytest.mark.parametrize("signal_type", [SignalType.LONG, SignalType.SHORT])
    def test_entry_signals_fail_closed_when_instrument_metadata_is_unavailable(
        self, mock_account_service, signal_factory, signal_type
    ):
        def fail_if_called(product_id):
            raise RuntimeError(f"metadata unavailable for {product_id}")

        risk_manager = RiskManager(
            mock_account_service,
            instrument_spec_resolver=fail_if_called,
        )

        with pytest.raises(RuntimeError, match="metadata unavailable"):
            risk_manager.check_risk(signal_factory(signal_type=signal_type))

    def test_allow_entry_with_positive_balance(self, mock_account_service, signal_factory):
        """Entry signals should be allowed with positive balance."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG, value=None)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True
        assert reason == "PASS"

    def test_reject_entry_on_negative_balance(self, mock_account_service, signal_factory):
        """Entry signals should be rejected on negative balance."""
        mock_account_service.set_balance(Decimal("-100"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False


class TestRiskManagerExposureChecks:
    """Tests for position exposure limits."""

    def test_reject_entry_when_single_order_notional_exceeds_nav_limit(
        self, mock_account_service, signal_factory
    ):
        """Single-order notional rule should reject oversized limit entries."""
        mock_account_service.set_balance(Decimal("100000"))
        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("60000"),
            quantity=Decimal("0.1"),
        )

        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False
        assert "single_order_notional_exceeded" in reason

    def test_reject_entry_when_price_sanity_context_fails(
        self, mock_account_service, signal_factory
    ):
        """Price sanity rule should reject outlier prices when bid/ask are supplied."""
        mock_account_service.set_balance(Decimal("100000"))
        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("103.01"),
            quantity=Decimal("0.01"),
        )

        is_allowed, reason = risk_manager.check_risk(
            signal,
            best_bid=Decimal("99"),
            best_ask=Decimal("101"),
        )

        assert is_allowed is False
        assert "price_sanity_check_failed" in reason

    def test_price_sanity_is_skipped_without_market_context(
        self, mock_account_service, signal_factory
    ):
        """Existing callers without bid/ask context should remain compatible."""
        mock_account_service.set_balance(Decimal("100000"))
        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("103.01"),
            quantity=Decimal("0.01"),
        )

        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True
        assert reason == "PASS"

    def test_daily_loss_circuit_breaker_rejects_entry(
        self, mock_account_service, signal_factory
    ):
        """Daily-loss circuit breaker should reject entries when NAV loss breaches threshold."""
        mock_account_service.set_balance(Decimal("100000"))
        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        is_allowed, reason = risk_manager.check_risk(
            signal,
            daily_start_nav=Decimal("100000"),
            current_nav=Decimal("94990"),
        )

        assert is_allowed is False
        assert "daily_loss_circuit_breaker_triggered" in reason

    def test_daily_loss_circuit_breaker_transitions_strategy_to_error(
        self, mock_account_service, signal_factory
    ):
        """Circuit breaker should move strategy state to ERROR when manager is injected."""
        mock_account_service.set_balance(Decimal("100000"))
        state_manager = MagicMock()
        risk_manager = RiskManager(
            mock_account_service,
            state_manager=state_manager,
        )
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        is_allowed, reason = risk_manager.check_risk(
            signal,
            daily_start_nav=Decimal("100000"),
            current_nav=Decimal("94990"),
        )

        assert is_allowed is False
        state_manager.transition_to_error.assert_called_once_with(
            "test_strategy",
            reason.removeprefix("REJECT: "),
            actor="system",
        )

    def test_daily_loss_transitions_portfolio_parent_lifecycle(
        self,
        mock_account_service,
        signal_factory,
    ):
        mock_account_service.set_balance(Decimal("100000"))
        state_manager = MagicMock()
        risk_manager = RiskManager(
            mock_account_service,
            state_manager=state_manager,
            lifecycle_id_resolver=lambda _strategy_id: "portfolio_v1",
        )

        is_allowed, reason = risk_manager.check_risk(
            signal_factory(signal_type=SignalType.LONG, value=None),
            daily_start_nav=Decimal("100000"),
            current_nav=Decimal("94990"),
        )

        assert is_allowed is False
        state_manager.transition_to_error.assert_called_once_with(
            "portfolio_v1",
            reason.removeprefix("REJECT: "),
            actor="system",
        )

    def test_daily_loss_rejects_even_if_state_transition_fails(
        self, mock_account_service, signal_factory
    ):
        """Risk rejection should remain fail-closed if ERROR transition fails."""
        mock_account_service.set_balance(Decimal("100000"))
        state_manager = MagicMock()
        state_manager.transition_to_error.side_effect = RuntimeError("db unavailable")
        risk_manager = RiskManager(
            mock_account_service,
            state_manager=state_manager,
        )
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        is_allowed, reason = risk_manager.check_risk(
            signal,
            daily_start_nav=Decimal("100000"),
            current_nav=Decimal("94990"),
        )

        assert is_allowed is False
        assert "daily_loss_circuit_breaker_triggered" in reason

    def test_daily_loss_requires_complete_nav_context(
        self, mock_account_service, signal_factory
    ):
        """Partial NAV context should fail closed instead of silently skipping."""
        mock_account_service.set_balance(Decimal("100000"))
        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        is_allowed, reason = risk_manager.check_risk(
            signal,
            daily_start_nav=Decimal("100000"),
        )

        assert is_allowed is False
        assert reason == "REJECT: daily_loss_missing_nav_context"

    def test_daily_loss_uses_snapshot_service_for_start_nav(
        self, mock_account_service, signal_factory
    ):
        """RiskManager should fetch start NAV when current NAV is supplied alone."""
        mock_account_service.set_balance(Decimal("100000"))
        daily_nav_service = _FakeDailyNavService(Decimal("100000"))
        risk_manager = RiskManager(
            mock_account_service,
            daily_nav_service=daily_nav_service,
        )
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        is_allowed, reason = risk_manager.check_risk(
            signal,
            current_nav=Decimal("94990"),
            snapshot_date=date(2026, 5, 18),
        )

        assert is_allowed is False
        assert "daily_loss_circuit_breaker_triggered" in reason
        assert daily_nav_service.calls == [("test_strategy", date(2026, 5, 18))]

    def test_daily_loss_rejects_when_snapshot_missing(
        self, mock_account_service, signal_factory
    ):
        """Missing daily snapshot should fail closed."""
        mock_account_service.set_balance(Decimal("100000"))
        risk_manager = RiskManager(
            mock_account_service,
            daily_nav_service=_FakeDailyNavService(None),
        )
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        is_allowed, reason = risk_manager.check_risk(
            signal,
            current_nav=Decimal("94990"),
            snapshot_date=date(2026, 5, 18),
        )

        assert is_allowed is False
        assert reason == "REJECT: daily_loss_missing_start_nav_snapshot"

    def test_authoritative_daily_nav_context_is_enforced_automatically(
        self,
        mock_account_service,
        signal_factory,
    ):
        mock_account_service.get_daily_nav_context = MagicMock(
            return_value=(Decimal("100000"), Decimal("94990"))
        )
        mock_account_service.get_balance = MagicMock(
            side_effect=AssertionError("authoritative context already contains current NAV")
        )
        risk_manager = RiskManager(mock_account_service)

        allowed, reason = risk_manager.check_risk(
            signal_factory(signal_type=SignalType.LONG, value=None)
        )

        assert allowed is False
        assert "daily_loss_circuit_breaker_triggered" in reason
        mock_account_service.get_balance.assert_not_called()

    def test_order_rate_limit_rejects_after_prior_checks_pass(
        self, mock_account_service, signal_factory
    ):
        """Rate limit should reject and record attempts only after earlier checks pass."""
        mock_account_service.set_balance(Decimal("100000"))
        rate_limit = _FakeOrderRateLimitRule(
            RuleStatus.REJECT,
            "order_rate_limit_exceeded: 11 > 10",
        )
        risk_manager = RiskManager(
            mock_account_service,
            order_rate_limit_rule=rate_limit,
        )
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False
        assert "order_rate_limit_exceeded" in reason
        assert rate_limit.calls == ["test_strategy"]

    def test_order_rate_limit_not_recorded_when_prior_check_rejects(
        self, mock_account_service, signal_factory
    ):
        """Failed earlier checks should not consume rate-limit slots."""
        mock_account_service.set_balance(Decimal("100000"))
        rate_limit = _FakeOrderRateLimitRule()
        risk_manager = RiskManager(
            mock_account_service,
            order_rate_limit_rule=rate_limit,
        )
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("60000"),
            quantity=Decimal("0.1"),
        )

        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False
        assert "single_order_notional_exceeded" in reason
        assert rate_limit.calls == []

    def test_reject_entry_when_max_exposure_reached(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Entry should be rejected when max exposure is already reached."""
        mock_account_service.set_balance(Decimal("100000"))

        # Set position with high exposure (quantity * current_price > configured max)
        large_position = position_factory(
            quantity=Decimal("3"),
            entry_price=Decimal("40000")
        )
        mock_account_service.set_position(large_position)

        risk_manager = RiskManager(
            mock_account_service,
            existing_position_entry_rule=_PassExistingPositionEntryRule(),
        )
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        # current_price=40000 -> 3 * 40000 = 120000 > default 100000
        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("40000"))

        assert is_allowed is False
        assert "exposure" in reason.lower()

    def test_reject_same_side_entry_when_position_already_exists(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Duplicate same-side entries are rejected for restart idempotency."""
        mock_account_service.set_balance(Decimal("100000"))
        position = position_factory(
            quantity=Decimal("0.5"),
            entry_price=Decimal("40000"),
            side=PositionSide.LONG,
        )
        mock_account_service.set_position(position)
        risk_manager = RiskManager(
            mock_account_service,
            existing_position_entry_rule=ExistingPositionEntryRule(),
        )
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("40000"),
            quantity=Decimal("0.01"),
        )

        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("40000"))

        assert is_allowed is False
        assert "existing_position_entry_duplicate" in reason

    def test_reject_same_side_short_entry_when_position_already_exists(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Duplicate SHORT entries are rejected symmetrically."""
        mock_account_service.set_balance(Decimal("100000"))
        position = position_factory(
            quantity=Decimal("0.5"),
            entry_price=Decimal("40000"),
            side=PositionSide.SHORT,
        )
        mock_account_service.set_position(position)
        risk_manager = RiskManager(
            mock_account_service,
            existing_position_entry_rule=ExistingPositionEntryRule(),
        )
        signal = signal_factory(
            signal_type=SignalType.SHORT,
            price=Decimal("40000"),
            quantity=Decimal("0.01"),
        )

        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("40000"))

        assert is_allowed is False
        assert "existing_position_entry_duplicate" in reason

    def test_same_side_entry_rejected_by_default(
        self, mock_account_service, signal_factory, position_factory
    ):
        """ExistingPositionEntryRule is active by default; same-side entries are rejected."""
        mock_account_service.set_balance(Decimal("100000"))
        position = position_factory(
            quantity=Decimal("0.5"),
            entry_price=Decimal("40000"),
            side=PositionSide.LONG,
        )
        mock_account_service.set_position(position)
        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("40000"),
            quantity=Decimal("0.01"),
        )

        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("40000"))

        assert is_allowed is False
        assert "existing_position_entry_duplicate" in reason

    def test_default_construction_rejects_duplicate_long_entry(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Regression: default RiskManager (no explicit rule) must reject same-side entry.

        This test exercises the DEFAULT construction path — do NOT inject the rule
        manually. It must FAIL if someone reverts the default wiring in risk_manager.py.
        """
        mock_account_service.set_balance(Decimal("100000"))
        position = position_factory(
            quantity=Decimal("1"),
            entry_price=Decimal("50000"),
            side=PositionSide.LONG,
        )
        mock_account_service.set_position(position)
        risk_manager = RiskManager(mock_account_service)  # no existing_position_entry_rule arg
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("50000"),
            quantity=Decimal("0.01"),
        )

        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("50000"))

        assert is_allowed is False
        assert "existing_position_entry_duplicate" in reason

    def test_exposure_uses_current_price_not_entry_price(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Exposure should be calculated with current market price, not entry price."""
        mock_account_service.set_balance(Decimal("100000"))

        # Entry at $100, but current price moved; exposure uses current price.
        position = position_factory(
            quantity=Decimal("1000"),
            entry_price=Decimal("100")
        )
        mock_account_service.set_position(position)

        risk_manager = RiskManager(
            mock_account_service,
            existing_position_entry_rule=_PassExistingPositionEntryRule(),
        )
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        # current 90 -> exposure 90000 < 100000: allowed
        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("90"))
        assert is_allowed is True
        assert reason == "PASS"

        # current 120 -> exposure 120000 > 100000: rejected by exposure
        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("120"))
        assert is_allowed is False
        assert "exposure" in reason.lower()

    def test_exposure_falls_back_to_entry_price_when_no_current_price(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Without current_price, exposure should fall back to entry_price."""
        mock_account_service.set_balance(Decimal("100000"))

        large_position = position_factory(
            quantity=Decimal("3"),
            entry_price=Decimal("40000")  # 3 * 40000 = 120000 > 100000
        )
        mock_account_service.set_position(large_position)

        risk_manager = RiskManager(
            mock_account_service,
            existing_position_entry_rule=_PassExistingPositionEntryRule(),
        )
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False
        assert "exposure" in reason.lower()

    def test_allow_exit_regardless_of_exposure(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Exit signals should be allowed regardless of exposure."""
        mock_account_service.set_balance(Decimal("100000"))

        large_position = position_factory(
            quantity=Decimal("2.0"),
            entry_price=Decimal("40000")
        )
        mock_account_service.set_position(large_position)

        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(signal_type=SignalType.EXIT_LONG)

        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("40000"))

        assert is_allowed is True

    def test_reject_same_side_entry_before_projected_position_check(
        self, mock_account_service, signal_factory, position_factory
    ):
        """The idempotency guard owns duplicate same-side entry rejection."""
        mock_account_service.set_balance(Decimal("100000"))
        position = position_factory(
            quantity=Decimal("1.99"),
            entry_price=Decimal("50000"),
        )
        mock_account_service.set_position(position)
        risk_manager = RiskManager(
            mock_account_service,
            risk_config=RiskConfig(max_position_notional=Decimal("100000")),
            existing_position_entry_rule=ExistingPositionEntryRule(),
        )
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("50000"),
            quantity=Decimal("0.02"),
        )

        is_allowed, reason = risk_manager.check_risk(
            signal,
            current_price=Decimal("50000"),
        )

        assert is_allowed is False
        assert "existing_position_entry_duplicate" in reason

    def test_allow_opposite_entry_that_reduces_exposure(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Opposite-side entries that reduce existing exposure should pass."""
        mock_account_service.set_balance(Decimal("100000"))
        position = position_factory(
            quantity=Decimal("2"),
            entry_price=Decimal("50000"),
        )
        mock_account_service.set_position(position)
        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(
            signal_type=SignalType.SHORT,
            price=Decimal("50000"),
            quantity=Decimal("0.05"),
        )

        is_allowed, reason = risk_manager.check_risk(
            signal,
            current_price=Decimal("50000"),
        )

        assert is_allowed is True
        assert reason == "PASS"

    def test_allow_same_side_reentry_when_flag_is_true(
        self, mock_account_service, signal_factory, position_factory
    ):
        """RiskManager with allow_same_side_reentry=True lets scale-in through to exposure checks."""
        mock_account_service.set_balance(Decimal("100000"))
        position = position_factory(
            quantity=Decimal("0.5"),
            entry_price=Decimal("50000"),
        )
        mock_account_service.set_position(position)
        risk_manager = RiskManager(
            mock_account_service,
            risk_config=RiskConfig(
                max_position_notional=Decimal("100000"),
                allow_same_side_reentry=True,
            ),
            existing_position_entry_rule=ExistingPositionEntryRule(),
        )
        signal = signal_factory(
            signal_type=SignalType.LONG,
            price=Decimal("50000"),
            quantity=Decimal("0.02"),
        )

        is_allowed, reason = risk_manager.check_risk(
            signal,
            current_price=Decimal("50000"),
        )

        assert is_allowed is True
        assert reason == "PASS"


class TestRiskManagerNoSignal:
    """Tests for NO_SIGNAL handling."""

    def test_no_signal_always_passes(self, mock_account_service, signal_factory):
        """NO_SIGNAL should always pass risk check."""
        mock_account_service.set_balance(Decimal("0"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.NO_SIGNAL)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True
        assert reason == "NO_SIGNAL"


class TestPositionSizeCalculation:
    """Tests for position size calculation."""

    def test_calculate_position_size_basic(self, mock_account_service):
        """Position size should be calculated based on risk percentage."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        # Entry at 42000, SL at 41000 (1000 point risk)
        # 2% of 10000 = 200 USDT risk
        # Size = 200 / 1000 = 0.2
        size = risk_manager.calculate_position_size(
            entry_price=Decimal("42000"),
            stop_loss_price=Decimal("41000"),
            risk_percent=Decimal("0.02")
        )

        assert size == Decimal("0.2")

    def test_calculate_position_size_custom_risk(self, mock_account_service):
        """Position size should scale with risk percentage."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        # 1% risk = 100 USDT
        # Size = 100 / 1000 = 0.1
        size = risk_manager.calculate_position_size(
            entry_price=Decimal("42000"),
            stop_loss_price=Decimal("41000"),
            risk_percent=Decimal("0.01")
        )

        assert size == Decimal("0.1")

    def test_calculate_position_size_zero_balance(self, mock_account_service):
        """Position size should be zero when balance is zero."""
        mock_account_service.set_balance(Decimal("0"))
        risk_manager = RiskManager(mock_account_service)

        size = risk_manager.calculate_position_size(
            entry_price=Decimal("42000"),
            stop_loss_price=Decimal("41000")
        )

        assert size == Decimal("0")

    def test_calculate_position_size_zero_stop_distance(self, mock_account_service):
        """Position size should be zero when stop distance is zero."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        size = risk_manager.calculate_position_size(
            entry_price=Decimal("42000"),
            stop_loss_price=Decimal("42000")  # Same as entry
        )

        assert size == Decimal("0")

    def test_calculate_position_size_short_position(self, mock_account_service):
        """Position size calculation should work for short positions."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        # Short entry at 42000, SL at 43000 (above entry)
        # Distance is still 1000
        size = risk_manager.calculate_position_size(
            entry_price=Decimal("42000"),
            stop_loss_price=Decimal("43000"),
            risk_percent=Decimal("0.02")
        )

        assert size == Decimal("0.2")


class TestRiskManagerEdgeCases:
    """Edge case tests for RiskManager."""

    def test_very_small_balance(self, mock_account_service, signal_factory):
        """Risk check should work with very small positive balance."""
        mock_account_service.set_balance(Decimal("0.01"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG, value=None)
        is_allowed, reason = risk_manager.check_risk(signal)

        # Should be allowed (balance > 0)
        assert is_allowed is True

    def test_very_large_balance(self, mock_account_service, signal_factory):
        """Risk check should work with very large balance."""
        mock_account_service.set_balance(Decimal("1000000000"))  # 1 billion
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG, value=None)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True

    def test_no_existing_position(self, mock_account_service, signal_factory):
        """Entry should be allowed when no position exists."""
        mock_account_service.set_balance(Decimal("10000"))
        # No position set
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG, value=None)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True

    def test_position_at_exactly_max_exposure(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Entry at exactly max exposure should be allowed."""
        mock_account_service.set_balance(Decimal("100000"))

        # Position at exactly max exposure (100000)
        position = position_factory(
            quantity=Decimal("2.5"),
            entry_price=Decimal("40000")  # 2.5 * 40000 = 100000
        )
        mock_account_service.set_position(position)

        risk_manager = RiskManager(
            mock_account_service,
            existing_position_entry_rule=_PassExistingPositionEntryRule(),
        )
        signal = signal_factory(signal_type=SignalType.LONG, value=None)

        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("40000"))

        assert is_allowed is True

    def test_negative_balance_rejects_entry(self, mock_account_service, signal_factory):
        """Large negative balance should still reject entry."""
        mock_account_service.set_balance(Decimal("-99999"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.SHORT)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False
        assert "balance" in reason.lower()

    def test_tight_stop_loss_small_size(self, mock_account_service):
        """Tight SL should produce very small position size."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        # 2% risk with 10-point SL on 42000 entry
        size = risk_manager.calculate_position_size(
            entry_price=Decimal("42000"),
            stop_loss_price=Decimal("41990"),
            risk_percent=Decimal("0.02"),
        )

        # 200 / 10 = 20 BTC — very large because SL is tight
        assert size == Decimal("20")

    def test_zero_risk_percent_returns_zero(self, mock_account_service):
        """0% risk should produce zero position size."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        size = risk_manager.calculate_position_size(
            entry_price=Decimal("42000"),
            stop_loss_price=Decimal("41000"),
            risk_percent=Decimal("0"),
        )

        assert size == Decimal("0")

    def test_position_size_rounding(self, mock_account_service):
        """Position size should be rounded to 4 decimal places."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        size = risk_manager.calculate_position_size(
            entry_price=Decimal("42000"),
            stop_loss_price=Decimal("41333"),
            risk_percent=Decimal("0.02"),
        )

        # Verify result has at most 4 decimal places
        assert abs(size - round(size, 4)) == 0


class TestAccountService:
    """Tests for the AccountService Redis integration."""

    def test_init_redis_success(self):
        """AccountService should connect to Redis successfully."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        assert service.redis is not None

    def test_init_redis_failure_sets_none(self):
        """Redis connection failure should set redis to None."""
        with patch("src.core.risk_manager.create_redis_client", side_effect=Exception("conn fail")):
            service = AccountService()

        assert service.redis is None

    def test_get_balance_returns_decimal(self):
        """Should return Decimal from Redis hash."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hget.return_value = "12345.67"

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        result = service.get_balance()
        assert result == Decimal("12345.67")

    def test_get_balance_no_redis_returns_zero(self):
        """Without Redis connection, should return zero."""
        with patch("src.core.risk_manager.create_redis_client", side_effect=Exception("fail")):
            service = AccountService()

        assert service.get_balance() == Decimal("0")

    def test_get_balance_no_value_returns_zero(self):
        """When Redis has no balance value, should return zero."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hget.return_value = None

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        assert service.get_balance() == Decimal("0")

    def test_replace_generic_balance_persists_exact_risk_owner_field(self):
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hget.return_value = "12345.6700"

        with patch(
            "src.core.risk_manager.create_redis_client", return_value=mock_redis
        ):
            service = AccountService()

        service.replace_generic_balance(Decimal("12345.6700"))

        mock_redis.hset.assert_called_once_with(
            "state:balance:main",
            mapping={"free": "12345.6700"},
        )
        assert service.get_balance() == Decimal("12345.6700")
        mock_redis.hget.assert_called_once_with("state:balance:main", "free")

    def test_authoritative_balance_uses_account_scoped_fresh_snapshot(self):
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hgetall.return_value = {
            "venue": "rithmic",
            "account_id": "TEST_ACCOUNT_001",
            "currency": "USD",
            "balance": "50123.45",
            "day_pnl": "-100.55",
            "day_start_nav": "50224.00",
            "observed_at_ms": "1704067200000",
            "source_timestamp_ms": "1704067199000",
        }

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()
        service.configure_authoritative_balance(
            venue="rithmic",
            account_id="TEST_ACCOUNT_001",
            max_age_seconds=30,
            runtime_environment=RuntimeEnvironment("live"),
        )

        with patch("src.core.risk_manager.time.time", return_value=1704067201):
            assert service.get_balance() == Decimal("50123.45")
        mock_redis.hgetall.assert_called_once_with(
            "fluxtrade:live:account:rithmic:TEST_ACCOUNT_001"
        )

    def test_authoritative_daily_nav_context_uses_same_snapshot(self):
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hgetall.return_value = {
            "venue": "rithmic",
            "account_id": "TEST_ACCOUNT_001",
            "currency": "USD",
            "balance": "49900",
            "day_pnl": "-100",
            "day_start_nav": "50000",
            "observed_at_ms": "1704067200000",
            "source_timestamp_ms": "1704067199000",
        }
        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()
        service.configure_authoritative_balance(
            venue="rithmic",
            account_id="TEST_ACCOUNT_001",
            max_age_seconds=30,
            runtime_environment=RuntimeEnvironment("live"),
        )

        with patch("src.core.risk_manager.time.time", return_value=1704067201):
            assert service.get_daily_nav_context() == (
                Decimal("50000"),
                Decimal("49900"),
            )

        mock_redis.hgetall.assert_called_once_with(
            "fluxtrade:live:account:rithmic:TEST_ACCOUNT_001"
        )

    @pytest.mark.parametrize(
        ("snapshot", "error"),
        [
            ({}, "authoritative_balance_snapshot_missing"),
            (
                {
                    "venue": "rithmic",
                    "account_id": "OTHER",
                    "currency": "USD",
                    "balance": "50000",
                    "day_pnl": "0",
                    "day_start_nav": "50000",
                    "observed_at_ms": "1704067200000",
                },
                "authoritative_balance_account_mismatch",
            ),
            (
                {
                    "venue": "rithmic",
                    "account_id": "TEST_ACCOUNT_001",
                    "currency": "USD",
                    "balance": "50000",
                    "day_pnl": "0",
                    "day_start_nav": "50000",
                    "observed_at_ms": "1704067100000",
                },
                "authoritative_balance_snapshot_stale",
            ),
        ],
    )
    def test_authoritative_balance_fails_closed_for_untrusted_snapshot(
        self,
        snapshot,
        error,
    ):
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hgetall.return_value = snapshot

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()
        service.configure_authoritative_balance(
            venue="rithmic",
            account_id="TEST_ACCOUNT_001",
            max_age_seconds=30,
            runtime_environment=RuntimeEnvironment("live"),
        )

        with patch("src.core.risk_manager.time.time", return_value=1704067201):
            with pytest.raises(RuntimeError, match=error):
                service.get_balance()

    def test_replace_authoritative_balance_rejects_wrong_account(self):
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()
        service.configure_authoritative_balance(
            venue="rithmic",
            account_id="TEST_ACCOUNT_001",
            max_age_seconds=30,
            runtime_environment=RuntimeEnvironment("live"),
        )

        with pytest.raises(
            ValueError,
            match="authoritative_balance_account_mismatch",
        ):
            service.replace_authoritative_balance(
                venue="rithmic",
                account_id="OTHER",
                currency="USD",
                balance=Decimal("50000"),
                day_pnl=Decimal("0"),
                observed_at_ms=1704067200000,
                source_timestamp_ms=None,
            )
        mock_redis.hset.assert_not_called()

    def test_replace_authoritative_balance_persists_decimal_as_text(self):
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()
        service.configure_authoritative_balance(
            venue="rithmic",
            account_id="TEST_ACCOUNT_001",
            max_age_seconds=30,
            runtime_environment=RuntimeEnvironment("live"),
        )

        service.replace_authoritative_balance(
            venue="rithmic",
            account_id="TEST_ACCOUNT_001",
            currency="USD",
            balance=Decimal("50123.45"),
            day_pnl=Decimal("-100.55"),
            observed_at_ms=1704067200000,
            source_timestamp_ms=1704067199000,
        )

        mock_redis.hset.assert_called_once_with(
            "fluxtrade:live:account:rithmic:TEST_ACCOUNT_001",
            mapping={
                "venue": "rithmic",
                "account_id": "TEST_ACCOUNT_001",
                "currency": "USD",
                "balance": "50123.45",
                "day_pnl": "-100.55",
                "day_start_nav": "50224.00",
                "observed_at_ms": "1704067200000",
                "source_timestamp_ms": "1704067199000",
            },
        )

    def test_get_position_returns_position(self):
        """Should return Position from Redis hash data."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hgetall.return_value = {
            "quantity": "0.5",
            "entry_price": "42000",
        }

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        pos = service.get_position("strat", "BINANCE:BTCUSDT-PERP")
        assert pos is not None
        assert pos.side == "LONG"
        assert pos.quantity == Decimal("0.5")
        assert pos.entry_price == Decimal("42000")

    def test_get_position_short_side(self):
        """Negative quantity should produce SHORT side."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hgetall.return_value = {
            "quantity": "-0.3",
            "entry_price": "42000",
        }

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        pos = service.get_position("strat", "BINANCE:BTCUSDT-PERP")
        assert pos is not None
        assert pos.side == "SHORT"
        assert pos.quantity == Decimal("0.3")

    def test_get_position_zero_quantity_returns_none(self):
        """Zero quantity should return None (no position)."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hgetall.return_value = {
            "quantity": "0",
            "entry_price": "42000",
        }

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        assert service.get_position("strat", "BINANCE:BTCUSDT-PERP") is None

    def test_get_position_no_data_returns_none(self):
        """Empty hash should return None."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.hgetall.return_value = {}

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        assert service.get_position("strat", "BINANCE:BTCUSDT-PERP") is None

    def test_get_position_no_redis_returns_none(self):
        """Without Redis connection, should return None."""
        with patch("src.core.risk_manager.create_redis_client", side_effect=Exception("fail")):
            service = AccountService()

        assert service.get_position("strat", "BINANCE:BTCUSDT-PERP") is None

    def test_exit_position_lookup_fails_when_redis_is_unavailable(self):
        with patch("src.core.risk_manager.create_redis_client", side_effect=Exception("fail")):
            service = AccountService()

        with pytest.raises(RuntimeError, match="position_state_unavailable"):
            service.get_position_for_exit("strat", "BINANCE:BTCUSDT-PERP")

    def test_get_all_positions_enumerates_redis_position_keys(self):
        """Should enumerate Redis position hashes and return non-flat positions."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.scan_iter.return_value = [
            "state:position:strat_a:BINANCE:BTCUSDT-PERP",
            "state:position:strat_b:BINANCE:ETHUSDT-PERP",
        ]
        mock_redis.hgetall.side_effect = [
            {"quantity": "0.5", "entry_price": "42000"},
            {"quantity": "0", "entry_price": "3000"},
        ]

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        positions = service.get_all_positions()

        assert len(positions) == 1
        assert positions[0].strategy_id == "strat_a"
        assert positions[0].product_id == "BINANCE:BTCUSDT-PERP"
        assert positions[0].quantity == Decimal("0.5")

    def test_get_all_positions_preserves_strategy_ids_with_colons(self):
        """Redis position keys must parse from the product suffix, not the left."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.scan_iter.return_value = [
            "state:position:test.py::StratB:BINANCE:BTCUSDT-PERP",
        ]
        mock_redis.hgetall.return_value = {
            "quantity": "0.25",
            "entry_price": "42000",
        }

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        positions = service.get_all_positions()

        assert len(positions) == 1
        assert positions[0].strategy_id == "test.py::StratB"
        assert positions[0].product_id == "BINANCE:BTCUSDT-PERP"
        assert positions[0].quantity == Decimal("0.25")

    @pytest.mark.parametrize(
        ("side", "expected_quantity"),
        [
            (PositionSide.LONG, "1.5"),
            (PositionSide.SHORT, "-1.5"),
        ],
    )
    def test_replace_positions_for_products_replaces_only_authoritative_scope(
        self,
        side,
        expected_quantity,
    ):
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        mock_redis.scan_iter.return_value = [
            "state:position:strat_a:RITHMIC:NQ-202609",
            "state:position:test.py::StratB:RITHMIC:NQ-202609",
            "state:position:strat_c:BINANCE:BTCUSDT-PERP",
        ]
        pipeline = mock_redis.pipeline.return_value

        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()

        position = Position(
            strategy_id="LIVE",
            product_id="RITHMIC:NQ-202609",
            side=side,
            quantity=Decimal("1.5"),
            entry_price=Decimal("20000.25"),
            unrealized_pnl=Decimal("0"),
        )
        result = service.replace_positions_for_products(
            [position],
            ("RITHMIC:NQ-202609",),
            timestamp_ms=1704067200000,
        )

        assert result == {"removed": 2, "written": 1}
        mock_redis.pipeline.assert_called_once_with(transaction=True)
        pipeline.delete.assert_called_once_with(
            "state:position:strat_a:RITHMIC:NQ-202609",
            "state:position:test.py::StratB:RITHMIC:NQ-202609",
        )
        pipeline.hset.assert_called_once_with(
            "state:position:LIVE:RITHMIC:NQ-202609",
            mapping={
                "quantity": expected_quantity,
                "entry_price": "20000.25",
                "last_update": "1704067200000",
            },
        )
        pipeline.execute.assert_called_once_with()

    def test_replace_positions_for_products_requires_redis_when_scope_nonempty(self):
        with patch(
            "src.core.risk_manager.create_redis_client",
            side_effect=Exception("fail"),
        ):
            service = AccountService()

        with pytest.raises(
            RuntimeError,
            match="authoritative_position_cache_unavailable",
        ):
            service.replace_positions_for_products(
                [],
                ("RITHMIC:NQ-202609",),
                timestamp_ms=1704067200000,
            )

    def test_close_with_redis(self):
        """close() should close Redis connection."""
        mock_redis = MagicMock()
        mock_redis.ping.return_value = True
        with patch("src.core.risk_manager.create_redis_client", return_value=mock_redis):
            service = AccountService()
        service.close()
        mock_redis.close.assert_called_once()

    def test_close_without_redis(self):
        """close() should not raise when redis is None."""
        with patch("src.core.risk_manager.create_redis_client", side_effect=Exception("fail")):
            service = AccountService()
        service.close()  # Should not raise


class TestRiskManagerWithCapitalAllocator:
    """Tests for RiskManager with per-strategy CapitalAllocator."""

    def test_reject_entry_when_no_strategy_capital(
        self, mock_account_service, signal_factory
    ):
        """Entry should be rejected when strategy has no allocated capital."""
        from src.core.capital_allocator import CapitalAllocator

        allocator = CapitalAllocator(Decimal("100000"))
        # No allocation for test_strategy
        risk_manager = RiskManager(mock_account_service, capital_allocator=allocator)

        signal = signal_factory(signal_type=SignalType.LONG)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False
        assert "capital" in reason.lower()

    def test_allow_entry_when_strategy_has_capital(
        self, mock_account_service, signal_factory
    ):
        """Entry should be allowed when strategy has allocated capital."""
        from src.core.capital_allocator import CapitalAllocator

        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("test_strategy", Decimal("50000"))
        risk_manager = RiskManager(mock_account_service, capital_allocator=allocator)

        signal = signal_factory(signal_type=SignalType.LONG, value=None)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True

    def test_reject_entry_when_strategy_capital_exhausted(
        self, mock_account_service, signal_factory
    ):
        """Entry should be rejected when all strategy capital is in use."""
        from src.core.capital_allocator import CapitalAllocator

        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("test_strategy", Decimal("50000"))
        allocator.record_usage("test_strategy", Decimal("50000"))
        risk_manager = RiskManager(mock_account_service, capital_allocator=allocator)

        signal = signal_factory(signal_type=SignalType.LONG)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is False
        assert "capital" in reason.lower()

    def test_allow_exit_even_without_capital(
        self, mock_account_service, signal_factory
    ):
        """Exit signals should always pass even without allocated capital."""
        from src.core.capital_allocator import CapitalAllocator

        allocator = CapitalAllocator(Decimal("100000"))
        risk_manager = RiskManager(mock_account_service, capital_allocator=allocator)

        signal = signal_factory(signal_type=SignalType.EXIT_LONG)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True

    def test_per_strategy_exposure_limit(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Per-strategy exposure limit should reject when exceeded."""
        from src.core.capital_allocator import CapitalAllocator

        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("test_strategy", Decimal("50000"))

        # Set position with exposure below configured max position notional
        # but above per-strategy limit (20000)
        position = position_factory(
            quantity=Decimal("0.5"),
            entry_price=Decimal("45000")
        )
        mock_account_service.set_position(position)

        risk_manager = RiskManager(
            mock_account_service,
            capital_allocator=allocator,
            max_exposure_per_strategy=Decimal("20000"),
            existing_position_entry_rule=_PassExistingPositionEntryRule(),
        )

        signal = signal_factory(signal_type=SignalType.LONG, value=None)
        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("45000"))

        assert is_allowed is False
        assert "strategy" in reason.lower()

    def test_backward_compat_no_allocator(
        self, mock_account_service, signal_factory
    ):
        """Without CapitalAllocator, RiskManager should behave exactly as before."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG, value=None)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True
        assert reason == "PASS"

    def test_per_strategy_exposure_applies_contract_multiplier(
        self, mock_account_service, signal_factory, position_factory
    ):
        from src.core.capital_allocator import CapitalAllocator

        allocator = CapitalAllocator(Decimal("100000"))
        allocator.allocate("test_strategy", Decimal("50000"))
        position = position_factory(quantity=Decimal("0.25"), entry_price=Decimal("40000"))
        mock_account_service.set_position(position)
        spec = InstrumentSpec(
            product_id=position.product_id,
            exchange="test",
            symbol="MNQ",
            base="MNQ",
            quote="USD",
            multiplier=Decimal("2"),
        )
        risk_manager = RiskManager(
            mock_account_service,
            capital_allocator=allocator,
            max_exposure_per_strategy=Decimal("15000"),
            existing_position_entry_rule=_PassExistingPositionEntryRule(),
            instrument_spec_resolver=lambda product_id: spec,
        )

        allowed, reason = risk_manager.check_risk(
            signal_factory(signal_type=SignalType.LONG, value=None),
            current_price=Decimal("40000"),
        )

        assert allowed is False
        assert "20000" in reason
