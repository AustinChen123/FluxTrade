from collections import deque
from decimal import Decimal
from typing import Deque

import pandas as pd

from src.core.models import Candlestick, Signal, SignalType
from src.strategies.base import BaseStrategy, StrategyRequirements


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
        self._in_position = False

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=self.timeframe,
            lookback_window=self.long_window,
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
        self.close_history.append(candle.close)

        if len(self.close_history) <= self.long_window:
            return self._signal(candle, SignalType.NO_SIGNAL)

        history = list(self.close_history)
        curr_sma_short = _sma(history, self.short_window)
        curr_sma_long = _sma(history, self.long_window)

        prev_history_list = list(self.close_history)[:-1]
        prev_sma_short = _sma(prev_history_list, self.short_window)
        prev_sma_long = _sma(prev_history_list, self.long_window)

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


def _sma(values: list[Decimal], window: int) -> Decimal:
    return sum(values[-window:]) / Decimal(window)
