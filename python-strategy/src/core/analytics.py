from dataclasses import dataclass
from decimal import Decimal
from typing import List, Dict
import pandas as pd
import numpy as np
from src.core.models import Trade


@dataclass(slots=True)
class ClosedTrade:
    """A completed round-trip trade with entry/exit details."""

    entry_time: int  # unix ms
    exit_time: int  # unix ms
    entry_price: Decimal
    exit_price: Decimal
    side: str  # "LONG" or "SHORT"
    quantity: Decimal
    pnl: Decimal


def _build_closed_trades(trade_history: List[Trade]) -> tuple[
    list[ClosedTrade], list[float], list[float], Decimal
]:
    """Pair raw trades into closed round-trips using FIFO netting.

    Returns (closed_trades, trade_pnls, equity_curve, total_pnl).
    trade_pnls and equity_curve are float lists for numpy/pandas compatibility.
    """
    trades = []
    for t in trade_history:
        trades.append({
            "timestamp": t.timestamp,
            "side": t.side,
            "price": t.price,
            "quantity": t.quantity,
        })

    df = pd.DataFrame(trades)
    df.sort_values("timestamp", inplace=True)

    _ZERO = Decimal("0")
    total_pnl = _ZERO
    net_qty = _ZERO
    avg_entry_price = _ZERO
    entry_time = 0

    equity_curve: list[float] = [0.0]
    trade_pnls: list[float] = []
    closed_trades: list[ClosedTrade] = []

    for _, row in df.iterrows():
        qty: Decimal = row["quantity"]
        price: Decimal = row["price"]
        side = row["side"]
        timestamp = int(row["timestamp"])

        signed_qty = qty if side.lower() == "buy" else -qty

        is_reducing = (net_qty > 0 and signed_qty < 0) or (
            net_qty < 0 and signed_qty > 0
        )

        if is_reducing:
            qty_closing = min(abs(net_qty), abs(signed_qty))

            if net_qty > 0:
                pnl = (price - avg_entry_price) * qty_closing
                trade_side = "LONG"
            else:
                pnl = (avg_entry_price - price) * qty_closing
                trade_side = "SHORT"

            total_pnl += pnl
            trade_pnls.append(float(pnl))

            closed_trades.append(ClosedTrade(
                entry_time=entry_time,
                exit_time=timestamp,
                entry_price=avg_entry_price,
                exit_price=price,
                side=trade_side,
                quantity=qty_closing,
                pnl=pnl,
            ))

            remaining_after_close = abs(signed_qty) - qty_closing

            if net_qty > 0:
                net_qty -= qty_closing
            else:
                net_qty += qty_closing

            if remaining_after_close > 0:
                net_qty = (
                    remaining_after_close if signed_qty > 0 else -remaining_after_close
                )
                avg_entry_price = price
                entry_time = timestamp
        else:
            if net_qty == 0:
                entry_time = timestamp
            total_cost = (abs(net_qty) * avg_entry_price) + (abs(signed_qty) * price)
            new_qty = abs(net_qty) + abs(signed_qty)

            if new_qty > 0:
                avg_entry_price = total_cost / new_qty

            net_qty += signed_qty

        equity_curve.append(float(total_pnl))

    return closed_trades, trade_pnls, equity_curve, total_pnl


def calculate_metrics(
    trade_history: List[Trade],
    *,
    initial_balance: float = 10000.0,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 365,
) -> Dict:
    """Calculate performance metrics from a list of trades.

    Basic metrics (backward-compatible):
        total_pnl, max_drawdown, trade_sharpe, win_rate,
        profit_factor, avg_trade, total_trades

    Advanced metrics:
        sortino_ratio, calmar_ratio, monthly_returns,
        max_drawdown_days, trade_frequency_per_day,
        avg_hold_time_hours, max_consecutive_wins,
        max_consecutive_losses, max_consecutive_win_amount,
        max_consecutive_loss_amount, gross_profit, gross_loss
    """
    if not trade_history:
        return {
            "total_pnl": Decimal("0.00"),
            "max_drawdown": Decimal("0.00"),
            "sharpe_ratio": 0.0,
            "win_rate": 0.0,
        }

    closed_trades, trade_pnls, equity_curve, total_pnl = _build_closed_trades(
        trade_history
    )

    # --- Basic metrics (backward-compatible) ---
    wins = sum(1 for p in trade_pnls if p > 0)
    losses = sum(1 for p in trade_pnls if p < 0)
    total_trades = wins + losses
    win_rate = (wins / total_trades) if total_trades > 0 else 0.0

    gross_profit = sum(p for p in trade_pnls if p > 0)
    gross_loss = abs(sum(p for p in trade_pnls if p < 0))
    profit_factor = (
        (gross_profit / gross_loss)
        if gross_loss > 0
        else (999.0 if gross_profit > 0 else 0.0)
    )
    avg_trade = float(total_pnl / total_trades) if total_trades > 0 else 0.0

    # Max Drawdown
    equity_series = pd.Series(equity_curve)
    rolling_max = equity_series.cummax()
    drawdown = equity_series - rolling_max
    max_drawdown = drawdown.min()

    # Trade-based Sharpe Ratio
    trade_sharpe = 0.0
    if len(trade_pnls) > 1:
        returns = np.array(trade_pnls)
        std_dev = np.std(returns)
        if std_dev != 0:
            trade_sharpe = np.mean(returns) / std_dev

    # --- Advanced metrics ---

    # Sortino Ratio (downside deviation only)
    sortino_ratio = 0.0
    if len(trade_pnls) > 1:
        returns_arr = np.array(trade_pnls)
        downside = returns_arr[returns_arr < 0]
        downside_std = np.std(downside) if len(downside) > 0 else 0.0
        if downside_std != 0:
            sortino_ratio = float(np.mean(returns_arr) / downside_std)

    # Calmar Ratio (annualized return / max drawdown)
    calmar_ratio = 0.0
    if closed_trades and max_drawdown < 0:
        first_ts = closed_trades[0].entry_time
        last_ts = closed_trades[-1].exit_time
        duration_days = max((last_ts - first_ts) / (1000 * 86400), 1.0)
        annualized_return = (float(total_pnl) / initial_balance) * (
            periods_per_year / duration_days
        )
        calmar_ratio = float(annualized_return / abs(float(max_drawdown) / initial_balance))

    # Monthly returns
    monthly_returns: Dict[str, Decimal] = {}
    if closed_trades:
        for ct in closed_trades:
            month_key = pd.Timestamp(ct.exit_time, unit="ms").strftime("%Y-%m")
            monthly_returns[month_key] = monthly_returns.get(month_key, Decimal("0")) + ct.pnl

    # Max drawdown duration (in days)
    max_drawdown_days = 0.0
    if closed_trades:
        eq = np.array(equity_curve)
        running_max = np.maximum.accumulate(eq)
        in_dd = eq < running_max
        dd_start = -1
        longest = 0
        for i, is_dd in enumerate(in_dd):
            if is_dd:
                if dd_start < 0:
                    dd_start = i
            else:
                if dd_start >= 0:
                    longest = max(longest, i - dd_start)
                    dd_start = -1
        if dd_start >= 0:
            longest = max(longest, len(in_dd) - dd_start)
        if len(closed_trades) > 0:
            first_ts = closed_trades[0].entry_time
            last_ts = closed_trades[-1].exit_time
            total_bars = len(equity_curve) - 1
            if total_bars > 0:
                ms_per_bar = (last_ts - first_ts) / total_bars
                max_drawdown_days = float((longest * ms_per_bar) / (1000 * 86400))

    # Trade frequency (per day)
    trade_frequency_per_day = 0.0
    if closed_trades:
        first_ts = closed_trades[0].entry_time
        last_ts = closed_trades[-1].exit_time
        duration_days = (last_ts - first_ts) / (1000 * 86400)
        if duration_days > 0:
            trade_frequency_per_day = float(len(closed_trades) / duration_days)

    # Average hold time (hours)
    avg_hold_time_hours = 0.0
    if closed_trades:
        hold_times = [(ct.exit_time - ct.entry_time) for ct in closed_trades]
        avg_hold_ms = sum(hold_times) / len(hold_times)
        avg_hold_time_hours = float(avg_hold_ms / (1000 * 3600))

    # Max consecutive wins / losses
    max_consecutive_wins = 0
    max_consecutive_losses = 0
    max_consecutive_win_amount = 0.0
    max_consecutive_loss_amount = 0.0

    if trade_pnls:
        cur_wins = 0
        cur_losses = 0
        cur_win_amt = 0.0
        cur_loss_amt = 0.0

        for pnl in trade_pnls:
            if pnl > 0:
                cur_wins += 1
                cur_win_amt += pnl
                if cur_wins > max_consecutive_wins:
                    max_consecutive_wins = cur_wins
                    max_consecutive_win_amount = cur_win_amt
                cur_losses = 0
                cur_loss_amt = 0.0
            elif pnl < 0:
                cur_losses += 1
                cur_loss_amt += abs(pnl)
                if cur_losses > max_consecutive_losses:
                    max_consecutive_losses = cur_losses
                    max_consecutive_loss_amount = cur_loss_amt
                cur_wins = 0
                cur_win_amt = 0.0
            else:
                cur_wins = 0
                cur_losses = 0
                cur_win_amt = 0.0
                cur_loss_amt = 0.0

    return {
        # Basic (backward-compatible) - all numeric values as Decimal for precision
        "total_pnl": Decimal(f"{total_pnl:.2f}"),
        "max_drawdown": Decimal(f"{max_drawdown:.2f}"),
        "trade_sharpe": Decimal(f"{trade_sharpe:.2f}"),
        "win_rate": Decimal(f"{win_rate:.2f}"),
        "profit_factor": Decimal(f"{profit_factor:.2f}"),
        "avg_trade": Decimal(f"{avg_trade:.2f}"),
        "total_trades": total_trades,
        # Advanced
        "sortino_ratio": Decimal(f"{sortino_ratio:.4f}"),
        "calmar_ratio": Decimal(f"{calmar_ratio:.4f}"),
        "monthly_returns": monthly_returns,
        "max_drawdown_days": Decimal(f"{max_drawdown_days:.2f}"),
        "trade_frequency_per_day": Decimal(f"{trade_frequency_per_day:.2f}"),
        "avg_hold_time_hours": Decimal(f"{avg_hold_time_hours:.2f}"),
        "max_consecutive_wins": max_consecutive_wins,
        "max_consecutive_losses": max_consecutive_losses,
        "max_consecutive_win_amount": Decimal(f"{max_consecutive_win_amount:.2f}"),
        "max_consecutive_loss_amount": Decimal(f"{max_consecutive_loss_amount:.2f}"),
        "gross_profit": Decimal(f"{gross_profit:.2f}"),
        "gross_loss": Decimal(f"{gross_loss:.2f}"),
        "closed_trades": closed_trades,
    }
