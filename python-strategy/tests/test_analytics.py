"""Tests for analytics.py — basic and advanced metrics."""

import pytest
from decimal import Decimal
from src.core.analytics import calculate_metrics, _build_closed_trades
from src.core.models import Trade


# ── Helpers ──────────────────────────────────────────────────────

def _make_trade(side: str, price: float, qty: float, ts: int) -> Trade:
    return Trade(
        id=f"t-{ts}-{side}",
        product_id="BINANCE:BTCUSDT-PERP",
        price=Decimal(str(price)),
        quantity=Decimal(str(qty)),
        side=side,
        timestamp=ts,
    )


def _round_trip(
    entry_price: float,
    exit_price: float,
    qty: float = 0.1,
    side: str = "long",
    entry_ts: int = 1_000_000,
    exit_ts: int = 2_000_000,
) -> list[Trade]:
    """Create a simple buy→sell or sell→buy round-trip."""
    if side == "long":
        return [
            _make_trade("buy", entry_price, qty, entry_ts),
            _make_trade("sell", exit_price, qty, exit_ts),
        ]
    else:
        return [
            _make_trade("sell", entry_price, qty, entry_ts),
            _make_trade("buy", exit_price, qty, exit_ts),
        ]


# ── Empty / edge cases ──────────────────────────────────────────

class TestEmptyAndEdge:
    def test_empty_trades(self):
        result = calculate_metrics([])
        assert result["total_pnl"] == Decimal("0.00")
        assert result["win_rate"] == 0.0

    def test_single_trade_no_close(self):
        trades = [_make_trade("buy", 100.0, 1.0, 1000)]
        result = calculate_metrics(trades)
        assert result["total_trades"] == 0
        assert result["total_pnl"] == Decimal("0.00")


# ── Basic metrics (backward compatibility) ───────────────────────

class TestBasicMetrics:
    def test_single_winning_trade(self):
        trades = _round_trip(100.0, 110.0)
        result = calculate_metrics(trades)
        assert result["total_pnl"] == Decimal("1.00")  # 10 * 0.1
        assert result["win_rate"] == 1.0
        assert result["total_trades"] == 1
        assert result["profit_factor"] == 999.0

    def test_single_losing_trade(self):
        trades = _round_trip(100.0, 90.0)
        result = calculate_metrics(trades)
        assert result["total_pnl"] == Decimal("-1.00")
        assert result["win_rate"] == 0.0
        assert result["profit_factor"] == 0.0

    def test_short_winning_trade(self):
        trades = _round_trip(100.0, 90.0, side="short")
        result = calculate_metrics(trades)
        assert result["total_pnl"] == Decimal("1.00")
        assert result["win_rate"] == 1.0

    def test_mixed_trades(self):
        trades = (
            _round_trip(100.0, 120.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 95.0, entry_ts=3000, exit_ts=4000)
        )
        result = calculate_metrics(trades)
        assert result["total_trades"] == 2
        assert result["win_rate"] == 0.5
        assert float(result["total_pnl"]) == pytest.approx(1.5, abs=0.01)

    def test_profit_factor(self):
        trades = (
            _round_trip(100.0, 120.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 95.0, entry_ts=3000, exit_ts=4000)
        )
        result = calculate_metrics(trades)
        # gross_profit=2.0, gross_loss=0.5
        assert result["profit_factor"] == pytest.approx(4.0, abs=0.01)

    def test_max_drawdown(self):
        # Win then lose: equity goes 0 → +2 → +1.5
        trades = (
            _round_trip(100.0, 120.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 95.0, entry_ts=3000, exit_ts=4000)
        )
        result = calculate_metrics(trades)
        assert float(result["max_drawdown"]) == pytest.approx(-0.5, abs=0.01)

    def test_trade_sharpe(self):
        trades = (
            _round_trip(100.0, 110.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 110.0, entry_ts=3000, exit_ts=4000)
        )
        result = calculate_metrics(trades)
        # All wins → sharpe is mean/std, but std=0 for identical PnLs
        # std(1.0, 1.0) = 0 → sharpe = 0
        assert result["trade_sharpe"] == 0.0


# ── ClosedTrade pairing ─────────────────────────────────────────

class TestClosedTrades:
    def test_long_round_trip(self):
        trades = _round_trip(100.0, 110.0, entry_ts=1_000_000, exit_ts=2_000_000)
        closed, _, _, _ = _build_closed_trades(trades)
        assert len(closed) == 1
        ct = closed[0]
        assert ct.side == "LONG"
        assert ct.entry_price == Decimal("100.0")
        assert ct.exit_price == Decimal("110.0")
        assert ct.entry_time == 1_000_000
        assert ct.exit_time == 2_000_000
        assert ct.pnl == Decimal("1.00")

    def test_short_round_trip(self):
        trades = _round_trip(100.0, 90.0, side="short", entry_ts=1_000_000, exit_ts=2_000_000)
        closed, _, _, _ = _build_closed_trades(trades)
        assert len(closed) == 1
        assert closed[0].side == "SHORT"
        assert closed[0].pnl == Decimal("1.00")

    def test_multiple_round_trips(self):
        trades = (
            _round_trip(100.0, 110.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(200.0, 190.0, entry_ts=3000, exit_ts=4000)
        )
        closed, _, _, _ = _build_closed_trades(trades)
        assert len(closed) == 2


# ── Advanced metrics ─────────────────────────────────────────────

class TestSortinoRatio:
    def test_all_positive_returns(self):
        trades = (
            _round_trip(100.0, 110.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 120.0, entry_ts=3000, exit_ts=4000)
        )
        result = calculate_metrics(trades)
        # No downside → downside_std=0 → sortino=0
        assert result["sortino_ratio"] == 0.0

    def test_mixed_returns(self):
        trades = (
            _round_trip(100.0, 120.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 80.0, entry_ts=3000, exit_ts=4000)
        )
        result = calculate_metrics(trades)
        # Has both positive and negative → sortino should be nonzero
        assert isinstance(result["sortino_ratio"], Decimal)


class TestCalmarRatio:
    def test_calmar_with_drawdown(self):
        # Create trades with drawdown
        trades = (
            _round_trip(100.0, 120.0, entry_ts=86_400_000, exit_ts=86_400_000 * 2)
            + _round_trip(100.0, 80.0, entry_ts=86_400_000 * 3, exit_ts=86_400_000 * 4)
        )
        result = calculate_metrics(trades, initial_balance=10000.0)
        assert isinstance(result["calmar_ratio"], Decimal)

    def test_calmar_no_drawdown(self):
        trades = _round_trip(100.0, 110.0)
        result = calculate_metrics(trades, initial_balance=10000.0)
        assert result["calmar_ratio"] == 0.0


class TestMonthlyReturns:
    def test_monthly_bucketing(self):
        # Jan trade + Feb trade
        jan_ts = 1704067200000  # 2024-01-01
        feb_ts = 1706745600000  # 2024-02-01
        trades = (
            _round_trip(100.0, 110.0, entry_ts=jan_ts, exit_ts=jan_ts + 60000)
            + _round_trip(100.0, 90.0, entry_ts=feb_ts, exit_ts=feb_ts + 60000)
        )
        result = calculate_metrics(trades)
        monthly = result["monthly_returns"]
        assert "2024-01" in monthly
        assert "2024-02" in monthly
        assert monthly["2024-01"] == Decimal("1.00")
        assert monthly["2024-02"] == Decimal("-1.00")

    def test_empty_monthly(self):
        result = calculate_metrics([])
        assert "monthly_returns" not in result


class TestMaxDrawdownDays:
    def test_has_drawdown_days(self):
        trades = (
            _round_trip(100.0, 120.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 80.0, entry_ts=3000, exit_ts=4000)
        )
        result = calculate_metrics(trades)
        assert isinstance(result["max_drawdown_days"], Decimal)
        assert result["max_drawdown_days"] >= 0


class TestTradeFrequency:
    def test_frequency_calculation(self):
        day_ms = 86_400_000
        trades = (
            _round_trip(100.0, 110.0, entry_ts=day_ms, exit_ts=day_ms + 60000)
            + _round_trip(100.0, 110.0, entry_ts=day_ms * 2, exit_ts=day_ms * 2 + 60000)
            + _round_trip(100.0, 110.0, entry_ts=day_ms * 3, exit_ts=day_ms * 3 + 60000)
        )
        result = calculate_metrics(trades)
        # 3 trades over ~2 days = ~1.5/day
        assert result["trade_frequency_per_day"] > 0


class TestAvgHoldTime:
    def test_hold_time_hours(self):
        hour_ms = 3_600_000
        trades = _round_trip(100.0, 110.0, entry_ts=0, exit_ts=2 * hour_ms)
        result = calculate_metrics(trades)
        assert result["avg_hold_time_hours"] == pytest.approx(2.0, abs=0.1)


class TestConsecutiveStreaks:
    def test_consecutive_wins(self):
        trades = (
            _round_trip(100.0, 110.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 120.0, entry_ts=3000, exit_ts=4000)
            + _round_trip(100.0, 130.0, entry_ts=5000, exit_ts=6000)
            + _round_trip(100.0, 90.0, entry_ts=7000, exit_ts=8000)
        )
        result = calculate_metrics(trades)
        assert result["max_consecutive_wins"] == 3
        assert result["max_consecutive_win_amount"] > 0

    def test_consecutive_losses(self):
        trades = (
            _round_trip(100.0, 90.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 80.0, entry_ts=3000, exit_ts=4000)
            + _round_trip(100.0, 110.0, entry_ts=5000, exit_ts=6000)
        )
        result = calculate_metrics(trades)
        assert result["max_consecutive_losses"] == 2
        assert result["max_consecutive_loss_amount"] > 0

    def test_no_trades_streaks(self):
        result = calculate_metrics([])
        assert "max_consecutive_wins" not in result


class TestGrossProfitLoss:
    def test_gross_values(self):
        trades = (
            _round_trip(100.0, 120.0, entry_ts=1000, exit_ts=2000)
            + _round_trip(100.0, 95.0, entry_ts=3000, exit_ts=4000)
        )
        result = calculate_metrics(trades)
        assert result["gross_profit"] == pytest.approx(2.0, abs=0.01)
        assert result["gross_loss"] == pytest.approx(0.5, abs=0.01)
