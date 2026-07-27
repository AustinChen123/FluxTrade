from collections import deque
from decimal import Decimal
from typing import Deque

import pandas as pd

from src.core.models import Candlestick, Signal, SignalType
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.fast_bar import BarView, RollingMean, SignalIntent


class GoldenCrossStrategy(BaseStrategy):
    """Simple moving-average crossover strategy.

    Emits market LONG on golden cross and market EXIT_LONG on death cross.
    Indicator values are carried in metadata so they do not become limit prices.
    """

    def __init__(
        self,
        strategy_id: str,
        product_id: str,
        short_window: int = 50,
        long_window: int = 200,
        *,
        timeframe: str = "1h",
        quantity: Decimal | str = Decimal("0.01"),
    ):
        if short_window <= 0 or long_window <= 0:
            raise ValueError("SMA windows must be positive")
        if short_window >= long_window:
            raise ValueError("short_window must be smaller than long_window")

        super().__init__(strategy_id, product_id)
        self.short_window = short_window
        self.long_window = long_window
        self.timeframe = timeframe
        self.quantity = Decimal(str(quantity))
        if self.quantity <= 0:
            raise ValueError("quantity must be positive")

        self.close_history: Deque[Decimal] = deque(maxlen=long_window + 1)
        self._short_values: Deque[Decimal] = deque()
        self._long_values: Deque[Decimal] = deque()
        self._short_sum = Decimal("0")
        self._long_sum = Decimal("0")
        self._in_position = False

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=self.timeframe,
            lookback_window=self.long_window,
        )

    def fresh_instance_for_replay(self) -> BaseStrategy:
        return type(self)(
            self.strategy_id,
            self.product_id,
            self.short_window,
            self.long_window,
            timeframe=self.timeframe,
            quantity=self.quantity,
        )

    def replay_configuration(self) -> object:
        return (
            self.short_window,
            self.long_window,
            self.timeframe,
            self.quantity,
        )

    def run_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates SMA crossovers using Vectorized Pandas operations.
        Returns DataFrame with 'sma_short', 'sma_long', and 'signal' columns.
        Signal: 1 (Long), -1 (Exit/Short), 0 (No Signal)
        """
        df = df.copy()

        # Calculate SMAs
        df["sma_short"] = df["close"].rolling(window=self.short_window).mean()
        df["sma_long"] = df["close"].rolling(window=self.long_window).mean()

        # Identify Bullish condition
        df["bullish"] = df["sma_short"] > df["sma_long"]

        # Identify Crossovers
        # True if Bullish now AND Not Bullish previously
        df["crossover"] = df["bullish"] & (~df["bullish"].shift(1).fillna(False))

        # Identify Crossunders (Death Cross)
        # True if Not Bullish now AND Bullish previously
        df["crossunder"] = (~df["bullish"]) & (df["bullish"].shift(1).fillna(False))

        # Generate Signals
        df["signal"] = 0
        df.loc[df["crossover"], "signal"] = 1
        df.loc[df["crossunder"], "signal"] = -1

        return df

    def on_candle(self, candle: Candlestick) -> Signal:
        """
        Event-driven execution for Golden Cross.
        """
        had_previous_long_window = len(self._long_values) == self.long_window
        prev_sma_short = (
            self._short_sum / Decimal(self.short_window)
            if had_previous_long_window
            else None
        )
        prev_sma_long = (
            self._long_sum / Decimal(self.long_window)
            if had_previous_long_window
            else None
        )

        self.close_history.append(candle.close)
        self._append_close(candle.close)

        if not had_previous_long_window:
            return self._signal(candle, SignalType.NO_SIGNAL)

        curr_sma_short = self._short_sum / Decimal(self.short_window)
        curr_sma_long = self._long_sum / Decimal(self.long_window)

        curr_bullish = curr_sma_short > curr_sma_long
        prev_bullish = prev_sma_short > prev_sma_long

        signal_type = SignalType.NO_SIGNAL
        if curr_bullish and not prev_bullish and not self._in_position:
            signal_type = SignalType.LONG
            self._in_position = True
        elif not curr_bullish and prev_bullish and self._in_position:
            signal_type = SignalType.EXIT_LONG
            self._in_position = False

        metadata = {
            "sma_short": str(curr_sma_short),
            "sma_long": str(curr_sma_long),
            "prev_sma_short": str(prev_sma_short),
            "prev_sma_long": str(prev_sma_long),
        }
        return self._signal(candle, signal_type, metadata=metadata)

    def prepare_fast(self) -> "_PreparedGoldenCrossStrategy":
        """Prepare a live-compatible fast-bar runtime for this strategy."""
        return _PreparedGoldenCrossStrategy(
            strategy_id=self.strategy_id,
            short_window=self.short_window,
            long_window=self.long_window,
            quantity=self.quantity,
        )

    def sync_position_state(self, position_side: str | None) -> bool:
        if position_side not in (None, "LONG"):
            return False
        self._in_position = position_side == "LONG"
        return True

    def snapshot_walk_forward_trade_state(self) -> object:
        return self._in_position

    def restore_walk_forward_trade_state(self, state: object) -> None:
        if not isinstance(state, bool):
            raise TypeError("golden-cross warm-up state must be bool")
        self._in_position = state

    def _signal(
        self,
        candle: Candlestick,
        signal_type: SignalType,
        *,
        metadata: dict[str, str] | None = None,
    ) -> Signal:
        quantity = self.quantity if signal_type != SignalType.NO_SIGNAL else None
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=self.timeframe,
            timestamp=candle.timestamp,
            type=signal_type,
            quantity=quantity,
            metadata=metadata,
        )

    def _append_close(self, close: Decimal) -> None:
        self._short_values.append(close)
        self._short_sum += close
        if len(self._short_values) > self.short_window:
            self._short_sum -= self._short_values.popleft()

        self._long_values.append(close)
        self._long_sum += close
        if len(self._long_values) > self.long_window:
            self._long_sum -= self._long_values.popleft()


class _PreparedGoldenCrossStrategy:
    """Fast-bar runtime equivalent of GoldenCrossStrategy.on_candle()."""

    def __init__(
        self,
        *,
        strategy_id: str,
        short_window: int,
        long_window: int,
        quantity: Decimal,
    ) -> None:
        self.strategy_id = strategy_id
        self.short_window = short_window
        self.long_window = long_window
        self.quantity = quantity
        self._short_mean = RollingMean(short_window)
        self._long_mean = RollingMean(long_window)
        self._in_position = False

    def on_bar(self, bar: BarView) -> SignalIntent | None:
        had_previous_long_window = self._long_mean.ready
        prev_sma_short = self._short_mean.mean if had_previous_long_window else None
        prev_sma_long = self._long_mean.mean if had_previous_long_window else None

        self._short_mean.append(bar.close_value)
        self._long_mean.append(bar.close_value)

        if not had_previous_long_window:
            return None

        curr_sma_short = self._short_mean.mean
        curr_sma_long = self._long_mean.mean

        curr_bullish = curr_sma_short > curr_sma_long
        prev_bullish = prev_sma_short > prev_sma_long

        if curr_bullish and not prev_bullish and not self._in_position:
            self._in_position = True
            return SignalIntent(SignalType.LONG, quantity=self.quantity)
        if not curr_bullish and prev_bullish and self._in_position:
            self._in_position = False
            return SignalIntent(SignalType.EXIT_LONG, quantity=self.quantity)
        return None
