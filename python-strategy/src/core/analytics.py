from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Dict, Iterable, List, Sequence
import pandas as pd
import numpy as np
from src.core.models import Trade, PositionSide


InitialBalanceInput = Decimal | int | float | str
_INVALID_INITIAL_BALANCE = "initial_balance must be a positive finite decimal value"


def _normalize_initial_balance(value: object) -> Decimal:
    if type(value) is Decimal:
        balance = value
    elif type(value) in (int, float, str):
        try:
            balance = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise ValueError(_INVALID_INITIAL_BALANCE) from None
    else:
        raise ValueError(_INVALID_INITIAL_BALANCE)
    if not balance.is_finite() or balance <= 0:
        raise ValueError(_INVALID_INITIAL_BALANCE)
    return balance


@dataclass(slots=True)
class ClosedTrade:
    """A completed round-trip trade with entry/exit details."""

    entry_time: int  # unix ms
    exit_time: int  # unix ms
    entry_price: Decimal
    exit_price: Decimal
    side: PositionSide  # "LONG" or "SHORT"
    quantity: Decimal
    pnl: Decimal
    fee: Decimal = Decimal("0")


def utc_daily_return_metrics(
    equity_samples: Sequence[tuple[int, Decimal]],
    *,
    initial_balance: Decimal,
    start_time: int,
    end_time: int,
) -> dict[str, object]:
    """Summarize UTC calendar-day mark-to-market equity returns."""
    if initial_balance <= 0:
        raise ValueError("daily Sharpe initial balance must be positive")
    if end_time < start_time:
        raise ValueError("daily Sharpe end_time cannot precede start_time")
    if not equity_samples:
        return {
            **raw_return_moments(()),
            "equity_sample_count": 0,
            "yearly_returns": {},
        }

    daily_closes: dict[date, Decimal] = {}
    for timestamp, equity in equity_samples:
        day = datetime.fromtimestamp(timestamp / 1000, tz=UTC).date()
        daily_closes[day] = equity

    first_day = datetime.fromtimestamp(start_time / 1000, tz=UTC).date()
    last_day = datetime.fromtimestamp(end_time / 1000, tz=UTC).date()
    previous_equity = initial_balance
    returns: list[Decimal] = []
    yearly_growth: dict[str, Decimal] = {}
    day = first_day
    while day <= last_day:
        current_equity = daily_closes.get(day, previous_equity)
        if previous_equity <= 0:
            raise ValueError(
                "daily Sharpe requires positive prior mark-to-market equity"
            )
        daily_return = current_equity / previous_equity - Decimal("1")
        returns.append(daily_return)
        year = str(day.year)
        yearly_growth[year] = yearly_growth.get(year, Decimal("1")) * (
            Decimal("1") + daily_return
        )
        previous_equity = current_equity
        day += timedelta(days=1)
    return {
        **raw_return_moments(returns),
        "equity_sample_count": len(equity_samples),
        "yearly_returns": {
            year: growth - Decimal("1")
            for year, growth in yearly_growth.items()
        },
    }


def raw_return_moments(
    returns: Iterable[Decimal],
) -> dict[str, Decimal | int]:
    count = 0
    sums = [Decimal("0")] * 4
    for value in returns:
        if not value.is_finite():
            raise ValueError("daily returns must be finite")
        count += 1
        power = value
        for index in range(4):
            sums[index] += power
            power *= value
    return {
        "count": count,
        "sum": sums[0],
        "sum_squares": sums[1],
        "sum_cubes": sums[2],
        "sum_fourth": sums[3],
    }


def annualized_sharpe_from_moments(
    moments: dict[str, Decimal | int],
    *,
    periods_per_year: int = 365,
) -> Decimal:
    count = int(moments["count"])
    if count < 2:
        return Decimal("0")
    total = Decimal(moments["sum"])
    sum_squares = Decimal(moments["sum_squares"])
    mean = total / Decimal(count)
    sample_variance = (
        sum_squares - total * total / Decimal(count)
    ) / Decimal(count - 1)
    if sample_variance <= 0:
        return Decimal("0")
    return mean / sample_variance.sqrt() * Decimal(periods_per_year).sqrt()


def _build_closed_trades(
    trade_history: List[Trade],
    *,
    contract_multiplier: Decimal = Decimal("1"),
) -> tuple[
    list[ClosedTrade], list[float], list[float], Decimal
]:
    """Pair raw trades into closed round-trips using FIFO netting.

    Returns (closed_trades, trade_pnls, equity_curve, total_pnl).
    trade_pnls and equity_curve are float lists for numpy/pandas compatibility.
    """
    if contract_multiplier <= 0:
        raise ValueError("contract_multiplier must be positive")
    trades = []
    has_fill_sequence = []
    for t in trade_history:
        fill_sequence = getattr(t, "fill_sequence", None)
        has_fill_sequence.append(fill_sequence is not None)
        trades.append({
            "timestamp": t.timestamp,
            "side": t.side,
            "price": t.price,
            "quantity": t.quantity,
            "fee": getattr(t, "fee", Decimal("0")) or Decimal("0"),
            "fill_sequence": fill_sequence,
        })

    if any(has_fill_sequence) and not all(has_fill_sequence):
        raise ValueError("fill_sequence must be present for every trade or none")
    if all(has_fill_sequence):
        fill_sequences = [trade["fill_sequence"] for trade in trades]
        if any(
            isinstance(sequence, bool)
            or not isinstance(sequence, int)
            or sequence < 0
            for sequence in fill_sequences
        ):
            raise ValueError("fill_sequence must be a non-negative integer")
        if len(set(fill_sequences)) != len(fill_sequences):
            raise ValueError("fill_sequence must be unique")
        trades.sort(
            key=lambda trade: (trade["timestamp"], trade["fill_sequence"])
        )
    else:
        trades.sort(key=lambda trade: trade["timestamp"])

    _ZERO = Decimal("0")
    total_pnl = _ZERO
    net_qty = _ZERO
    avg_entry_price = _ZERO
    open_fee = _ZERO
    entry_time = 0

    equity_curve: list[float] = [0.0]
    trade_pnls: list[float] = []
    closed_trades: list[ClosedTrade] = []

    for row in trades:
        qty: Decimal = row["quantity"]
        price: Decimal = row["price"]
        side = row["side"]
        fee: Decimal = row["fee"]
        timestamp = int(row["timestamp"])

        signed_qty = qty if side.lower() == "buy" else -qty
        if fee:
            total_pnl -= fee

        is_reducing = (net_qty > 0 and signed_qty < 0) or (
            net_qty < 0 and signed_qty > 0
        )

        if is_reducing:
            previous_qty = abs(net_qty)
            qty_closing = min(abs(net_qty), abs(signed_qty))
            entry_fee = (
                open_fee * qty_closing / previous_qty
                if previous_qty > _ZERO
                else _ZERO
            )
            exit_fee = fee * qty_closing / abs(signed_qty)

            if net_qty > 0:
                gross_pnl = (price - avg_entry_price) * qty_closing * contract_multiplier
                trade_side = PositionSide.LONG
            else:
                gross_pnl = (avg_entry_price - price) * qty_closing * contract_multiplier
                trade_side = PositionSide.SHORT

            pnl = gross_pnl - entry_fee - exit_fee
            total_pnl += gross_pnl
            trade_pnls.append(float(pnl))

            closed_trades.append(ClosedTrade(
                entry_time=entry_time,
                exit_time=timestamp,
                entry_price=avg_entry_price,
                exit_price=price,
                side=trade_side,
                quantity=qty_closing,
                pnl=pnl,
                fee=entry_fee + exit_fee,
            ))

            remaining_after_close = abs(signed_qty) - qty_closing
            open_fee -= entry_fee

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
                open_fee = fee - exit_fee
        else:
            if net_qty == 0:
                entry_time = timestamp
                open_fee = _ZERO
            total_cost = (abs(net_qty) * avg_entry_price) + (abs(signed_qty) * price)
            new_qty = abs(net_qty) + abs(signed_qty)

            if new_qty > 0:
                avg_entry_price = total_cost / new_qty

            net_qty += signed_qty
            open_fee += fee

        equity_curve.append(float(total_pnl))

    return closed_trades, trade_pnls, equity_curve, total_pnl


def calculate_metrics(
    trade_history: List[Trade],
    *,
    initial_balance: InitialBalanceInput = Decimal("10000"),
    risk_free_rate: float = 0.0,
    periods_per_year: int = 365,
    contract_multiplier: Decimal = Decimal("1"),
    equity_samples: Sequence[tuple[int, Decimal]] | None = None,
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
    initial_balance_decimal = _normalize_initial_balance(initial_balance)
    if not trade_history:
        result = {
            "closed_trade_count": 0,
            "total_pnl": Decimal("0.00"),
            "max_drawdown": Decimal("0.00"),
            "sharpe_ratio": 0.0,
            "win_rate": 0.0,
        }
        if equity_samples is not None:
            result.update(
                _mark_to_market_drawdown_metrics(
                    equity_samples,
                    initial_balance=initial_balance_decimal,
                    periods_per_year=periods_per_year,
                )
            )
        return result

    closed_trades, trade_pnls, equity_curve, total_pnl = _build_closed_trades(
        trade_history,
        contract_multiplier=contract_multiplier,
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
        duration_days = max(
            Decimal(last_ts - first_ts) / Decimal(86_400_000),
            Decimal("1"),
        )
        annualized_return = (
            total_pnl
            / initial_balance_decimal
            * (Decimal(periods_per_year) / duration_days)
        )
        calmar_ratio = annualized_return / (
            abs(Decimal(str(max_drawdown))) / initial_balance_decimal
        )

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
    max_consecutive_win_amount = Decimal("0")
    max_consecutive_loss_amount = Decimal("0")

    if trade_pnls:
        cur_wins = 0
        cur_losses = 0
        cur_win_amt = Decimal("0")
        cur_loss_amt = Decimal("0")

        for pnl in trade_pnls:
            if pnl > 0:
                cur_wins += 1
                cur_win_amt += Decimal(str(pnl))
                if cur_wins > max_consecutive_wins:
                    max_consecutive_wins = cur_wins
                    max_consecutive_win_amount = cur_win_amt
                cur_losses = 0
                cur_loss_amt = Decimal("0")
            elif pnl < 0:
                cur_losses += 1
                cur_loss_amt += Decimal(str(abs(pnl)))
                if cur_losses > max_consecutive_losses:
                    max_consecutive_losses = cur_losses
                    max_consecutive_loss_amount = cur_loss_amt
                cur_wins = 0
                cur_win_amt = Decimal("0")
            else:
                cur_wins = 0
                cur_losses = 0
                cur_win_amt = Decimal("0")
                cur_loss_amt = Decimal("0")

    if equity_samples is not None:
        mark_to_market_metrics = _mark_to_market_drawdown_metrics(
            equity_samples,
            initial_balance=initial_balance_decimal,
            periods_per_year=periods_per_year,
        )
        max_drawdown = mark_to_market_metrics["max_drawdown"]
        calmar_ratio = mark_to_market_metrics["calmar_ratio"]
        max_drawdown_days = mark_to_market_metrics["max_drawdown_days"]
        mark_to_market_pnl = mark_to_market_metrics["mark_to_market_pnl"]
    else:
        mark_to_market_pnl = total_pnl

    reported_max_drawdown = (
        max_drawdown
        if equity_samples is not None
        else Decimal(f"{max_drawdown:.2f}")
    )
    return {
        # Basic (backward-compatible) - all numeric values as Decimal for precision
        "total_pnl": Decimal(f"{total_pnl:.2f}"),
        "mark_to_market_pnl": mark_to_market_pnl,
        "max_drawdown": reported_max_drawdown,
        "trade_sharpe": Decimal(f"{trade_sharpe:.2f}"),
        "win_rate": Decimal(f"{win_rate:.2f}"),
        "profit_factor": Decimal(f"{profit_factor:.2f}"),
        "avg_trade": Decimal(f"{avg_trade:.2f}"),
        "total_trades": total_trades,
        "closed_trade_count": len(closed_trades),
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


def _mark_to_market_drawdown_metrics(
    equity_samples: Sequence[tuple[int, Decimal]],
    *,
    initial_balance: Decimal,
    periods_per_year: int,
) -> dict[str, Decimal]:
    if initial_balance <= 0:
        raise ValueError("initial_balance must be positive")
    if periods_per_year <= 0:
        raise ValueError("periods_per_year must be positive")
    if not equity_samples:
        return {
            "max_drawdown": Decimal("0.00"),
            "calmar_ratio": Decimal("0.0000"),
            "max_drawdown_days": Decimal("0.00"),
            "mark_to_market_pnl": Decimal("0"),
        }

    previous_timestamp: int | None = None
    peak_equity = initial_balance
    peak_timestamp: int | None = None
    max_drawdown = Decimal("0")
    drawdown_started_at: int | None = None
    longest_drawdown_ms = 0

    for timestamp, equity in equity_samples:
        if previous_timestamp is not None and timestamp <= previous_timestamp:
            raise ValueError("equity sample timestamps must be strictly increasing")
        previous_timestamp = timestamp
        if not equity.is_finite():
            raise ValueError("equity samples must be finite")

        if equity >= peak_equity:
            peak_equity = equity
            if drawdown_started_at is not None:
                longest_drawdown_ms = max(
                    longest_drawdown_ms,
                    timestamp - drawdown_started_at,
                )
                drawdown_started_at = None
            peak_timestamp = timestamp
            continue

        max_drawdown = max(max_drawdown, peak_equity - equity)
        if drawdown_started_at is None:
            drawdown_started_at = (
                peak_timestamp if peak_timestamp is not None else timestamp
            )

    first_timestamp = equity_samples[0][0]
    last_timestamp = equity_samples[-1][0]
    mark_to_market_pnl = equity_samples[-1][1] - initial_balance
    if drawdown_started_at is not None:
        longest_drawdown_ms = max(
            longest_drawdown_ms,
            last_timestamp - drawdown_started_at,
        )

    calmar_ratio = Decimal("0")
    if max_drawdown > 0:
        duration_days = max(
            Decimal(last_timestamp - first_timestamp) / Decimal(86_400_000),
            Decimal("1"),
        )
        annualized_return = (
            mark_to_market_pnl
            / initial_balance
            * Decimal(periods_per_year)
            / duration_days
        )
        calmar_ratio = annualized_return / (max_drawdown / initial_balance)

    return {
        "max_drawdown": max_drawdown,
        "calmar_ratio": calmar_ratio.quantize(Decimal("0.0001")),
        "max_drawdown_days": (
            Decimal(longest_drawdown_ms) / Decimal(86_400_000)
        ).quantize(Decimal("0.01")),
        "mark_to_market_pnl": mark_to_market_pnl,
    }
