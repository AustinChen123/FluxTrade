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

from decimal import Decimal
from unittest.mock import MagicMock, patch

from src.core.models import SignalType
from src.core.risk_config import RiskConfig
from src.core.risk_rules import RuleStatus
from src.core.risk_manager import RiskManager, AccountService


class _FakeOrderRateLimitRule:
    def __init__(self, status=RuleStatus.PASS, reason=None):
        self.status = status
        self.reason = reason
        self.calls = []

    def try_record_order(self, strategy_id):
        self.calls.append(strategy_id)
        return self.status, self.reason


class TestRiskManagerBalanceChecks:
    """Tests for balance-related risk checks."""

    def test_reject_entry_on_zero_balance(self, mock_account_service, signal_factory):
        """Entry signals should be rejected when balance is zero."""
        mock_account_service.set_balance(Decimal("0"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG)
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

    def test_allow_entry_with_positive_balance(self, mock_account_service, signal_factory):
        """Entry signals should be allowed with positive balance."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG)
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
        signal = signal_factory(signal_type=SignalType.LONG)

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
        signal = signal_factory(signal_type=SignalType.LONG)

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
        signal = signal_factory(signal_type=SignalType.LONG)

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
        signal = signal_factory(signal_type=SignalType.LONG)

        is_allowed, reason = risk_manager.check_risk(
            signal,
            daily_start_nav=Decimal("100000"),
        )

        assert is_allowed is False
        assert reason == "REJECT: daily_loss_missing_nav_context"

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
        signal = signal_factory(signal_type=SignalType.LONG)

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

        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(signal_type=SignalType.LONG)

        # current_price=40000 -> 3 * 40000 = 120000 > default 100000
        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("40000"))

        assert is_allowed is False
        assert "exposure" in reason.lower()

    def test_allow_entry_when_under_max_exposure(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Entry should be allowed when under max exposure."""
        mock_account_service.set_balance(Decimal("100000"))

        # Set position with low exposure
        small_position = position_factory(
            quantity=Decimal("0.5"),
            entry_price=Decimal("40000")
        )
        mock_account_service.set_position(small_position)

        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(signal_type=SignalType.LONG)

        # current_price=40000 -> 0.5 * 40000 = 20000 < default 100000
        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("40000"))

        assert is_allowed is True

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

        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(signal_type=SignalType.LONG)

        # With current_price=$90, exposure = 1000 * 90 = 90000 < 100000 -> PASS
        is_allowed, _ = risk_manager.check_risk(signal, current_price=Decimal("90"))
        assert is_allowed is True

        # With current_price=$120, exposure = 1000 * 120 = 120000 > 100000 -> REJECT
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

        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(signal_type=SignalType.LONG)

        # No current_price → fallback to entry_price
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

    def test_reject_entry_when_projected_position_exceeds_config(
        self, mock_account_service, signal_factory, position_factory
    ):
        """Configured rule should reject projected same-side exposure."""
        mock_account_service.set_balance(Decimal("100000"))
        position = position_factory(
            quantity=Decimal("1.99"),
            entry_price=Decimal("50000"),
        )
        mock_account_service.set_position(position)
        risk_manager = RiskManager(
            mock_account_service,
            risk_config=RiskConfig(max_position_notional=Decimal("100000")),
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
        assert "max_position_notional_exceeded" in reason

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

        signal = signal_factory(signal_type=SignalType.LONG)
        is_allowed, reason = risk_manager.check_risk(signal)

        # Should be allowed (balance > 0)
        assert is_allowed is True

    def test_very_large_balance(self, mock_account_service, signal_factory):
        """Risk check should work with very large balance."""
        mock_account_service.set_balance(Decimal("1000000000"))  # 1 billion
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True

    def test_no_existing_position(self, mock_account_service, signal_factory):
        """Entry should be allowed when no position exists."""
        mock_account_service.set_balance(Decimal("10000"))
        # No position set
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG)
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

        risk_manager = RiskManager(mock_account_service)
        signal = signal_factory(signal_type=SignalType.LONG)

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

        signal = signal_factory(signal_type=SignalType.LONG)
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
        )

        signal = signal_factory(signal_type=SignalType.LONG)
        # 0.5 * 45000 = 22500 < 100000 (position OK) but >= 20000 (strategy REJECT)
        is_allowed, reason = risk_manager.check_risk(signal, current_price=Decimal("45000"))

        assert is_allowed is False
        assert "strategy" in reason.lower()

    def test_backward_compat_no_allocator(
        self, mock_account_service, signal_factory
    ):
        """Without CapitalAllocator, RiskManager should behave exactly as before."""
        mock_account_service.set_balance(Decimal("10000"))
        risk_manager = RiskManager(mock_account_service)

        signal = signal_factory(signal_type=SignalType.LONG)
        is_allowed, reason = risk_manager.check_risk(signal)

        assert is_allowed is True
        assert reason == "PASS"
