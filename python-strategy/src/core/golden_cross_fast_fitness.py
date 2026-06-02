"""Numeric fast fitness evaluator for GoldenCross parameter searches.

This module is intentionally narrower than ResearchBacktestRunner. It skips
Candlestick, Signal, Order, and adapter objects and models only the current
GoldenCrossStrategy market LONG/EXIT_LONG behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import numpy as np
import pandas as pd


@dataclass(frozen=True, slots=True)
class GoldenCrossFastFitnessResult:
    total_pnl: Decimal
    max_drawdown: Decimal
    total_trades: int
    raw_trade_count: int
    win_rate: Decimal
    profit_factor: Decimal
    gross_profit: Decimal
    gross_loss: Decimal


class GoldenCrossFastFitnessEvaluator:
    """Evaluate GoldenCross candidates over cached numeric OHLCV arrays."""

    def __init__(
        self,
        timestamps: np.ndarray,
        opens: np.ndarray,
        closes: np.ndarray,
        *,
        initial_balance: Decimal = Decimal("10000"),
        taker_fee: Decimal = Decimal("0"),
    ) -> None:
        if len(timestamps) != len(opens) or len(opens) != len(closes):
            raise ValueError("timestamps, opens, and closes must have equal length")
        self.timestamps = timestamps.astype(np.int64, copy=False)
        self.opens = opens.astype(np.float64, copy=False)
        self.closes = closes.astype(np.float64, copy=False)
        self.initial_balance = float(initial_balance)
        self.taker_fee = float(taker_fee)

    @classmethod
    def from_dataframe(
        cls,
        df: pd.DataFrame,
        *,
        initial_balance: Decimal = Decimal("10000"),
        taker_fee: Decimal = Decimal("0"),
    ) -> "GoldenCrossFastFitnessEvaluator":
        if "timestamp" in df.columns:
            timestamps = df["timestamp"].to_numpy(dtype=np.int64)
        else:
            timestamps = df.index.to_numpy(dtype=np.int64)
        return cls(
            timestamps=timestamps,
            opens=df["open"].to_numpy(dtype=np.float64),
            closes=df["close"].to_numpy(dtype=np.float64),
            initial_balance=initial_balance,
            taker_fee=taker_fee,
        )

    def evaluate(
        self,
        *,
        short_window: int,
        long_window: int,
        quantity: Decimal = Decimal("0.01"),
    ) -> GoldenCrossFastFitnessResult:
        if short_window <= 0 or long_window <= 0:
            raise ValueError("SMA windows must be positive")
        if short_window >= long_window:
            raise ValueError("short_window must be smaller than long_window")

        n = len(self.closes)
        if n <= long_window:
            return _empty_result()

        short_ma = _rolling_mean(self.closes, short_window)
        long_ma = _rolling_mean(self.closes, long_window)
        bullish = short_ma > long_ma
        previous_bullish = np.empty_like(bullish)
        previous_bullish[0] = False
        previous_bullish[1:] = bullish[:-1]

        valid_signal_index = np.arange(n) >= long_window
        entries = bullish & ~previous_bullish & valid_signal_index
        exits = ~bullish & previous_bullish & valid_signal_index
        signal_indices = np.flatnonzero(entries | exits)

        qty = float(quantity)
        net_qty = 0.0
        entry_price = 0.0
        total_pnl = 0.0
        equity_curve = [0.0]
        trade_pnls: list[float] = []
        raw_trade_count = 0

        for signal_index in signal_indices:
            fill_index = signal_index + 1
            if fill_index >= n:
                break

            is_entry = bool(entries[signal_index])
            is_exit = bool(exits[signal_index])
            fill_price = float(self.opens[fill_index])
            fee = fill_price * qty * self.taker_fee

            if is_entry and net_qty == 0.0:
                net_qty = qty
                entry_price = fill_price
                total_pnl -= fee
                equity_curve.append(total_pnl)
                raw_trade_count += 1
            elif is_exit and net_qty > 0.0:
                entry_fee = entry_price * net_qty * self.taker_fee
                gross_pnl = (fill_price - entry_price) * net_qty
                trade_pnl = gross_pnl - entry_fee - fee
                total_pnl += gross_pnl - fee
                trade_pnls.append(trade_pnl)
                equity_curve.append(total_pnl)
                raw_trade_count += 1
                net_qty = 0.0
                entry_price = 0.0

        return _result_from_pnls(total_pnl, equity_curve, trade_pnls, raw_trade_count)


def _rolling_mean(values: np.ndarray, window: int) -> np.ndarray:
    result = np.full(len(values), np.nan, dtype=np.float64)
    if len(values) < window:
        return result
    cumsum = np.concatenate(([0.0], np.cumsum(values, dtype=np.float64)))
    result[window - 1:] = (cumsum[window:] - cumsum[:-window]) / window
    return result


def _empty_result() -> GoldenCrossFastFitnessResult:
    zero = Decimal("0.00")
    return GoldenCrossFastFitnessResult(
        total_pnl=zero,
        max_drawdown=zero,
        total_trades=0,
        raw_trade_count=0,
        win_rate=zero,
        profit_factor=zero,
        gross_profit=zero,
        gross_loss=zero,
    )


def _result_from_pnls(
    total_pnl: float,
    equity_curve: list[float],
    trade_pnls: list[float],
    raw_trade_count: int,
) -> GoldenCrossFastFitnessResult:
    wins = sum(1 for pnl in trade_pnls if pnl > 0)
    losses = sum(1 for pnl in trade_pnls if pnl < 0)
    total_trades = wins + losses
    win_rate = wins / total_trades if total_trades > 0 else 0.0
    gross_profit = sum(pnl for pnl in trade_pnls if pnl > 0)
    gross_loss = abs(sum(pnl for pnl in trade_pnls if pnl < 0))
    if gross_loss > 0:
        profit_factor = gross_profit / gross_loss
    else:
        profit_factor = 999.0 if gross_profit > 0 else 0.0

    equity = np.array(equity_curve, dtype=np.float64)
    drawdown = equity - np.maximum.accumulate(equity)
    max_drawdown = float(drawdown.min()) if len(drawdown) > 0 else 0.0

    return GoldenCrossFastFitnessResult(
        total_pnl=Decimal(f"{total_pnl:.8f}"),
        max_drawdown=Decimal(f"{max_drawdown:.2f}"),
        total_trades=total_trades,
        raw_trade_count=raw_trade_count,
        win_rate=Decimal(f"{win_rate:.2f}"),
        profit_factor=Decimal(f"{profit_factor:.2f}"),
        gross_profit=Decimal(f"{gross_profit:.2f}"),
        gross_loss=Decimal(f"{gross_loss:.2f}"),
    )
