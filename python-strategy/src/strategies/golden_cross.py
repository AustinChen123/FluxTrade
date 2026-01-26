import pandas as pd
from typing import Deque
from collections import deque
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType

class GoldenCrossStrategy(BaseStrategy):
    def __init__(self, strategy_id: str, product_id: str, short_window: int = 50, long_window: int = 200):
        super().__init__(strategy_id, product_id)
        self.short_window = short_window
        self.long_window = long_window
        # History for Event-Driven
        self.close_history: Deque[float] = deque(maxlen=long_window + 1)

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe="1h",
            lookback_window=self.long_window
        )

    def run_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculates SMA crossovers using Vectorized Pandas operations.
        Returns DataFrame with 'sma_short', 'sma_long', and 'signal' columns.
        Signal: 1 (Long), -1 (Exit/Short), 0 (No Signal)
        """
        df = df.copy()
        
        # Calculate SMAs
        df['sma_short'] = df['close'].rolling(window=self.short_window).mean()
        df['sma_long'] = df['close'].rolling(window=self.long_window).mean()
        
        # Identify Bullish condition
        df['bullish'] = df['sma_short'] > df['sma_long']
        
        # Identify Crossovers
        # True if Bullish now AND Not Bullish previously
        df['crossover'] = df['bullish'] & (~df['bullish'].shift(1).fillna(False))
        
        # Identify Crossunders (Death Cross)
        # True if Not Bullish now AND Bullish previously
        df['crossunder'] = (~df['bullish']) & (df['bullish'].shift(1).fillna(False))
        
        # Generate Signals
        df['signal'] = 0
        df.loc[df['crossover'], 'signal'] = 1  # Buy
        df.loc[df['crossunder'], 'signal'] = -1 # Sell/Exit
        
        return df

    def on_candle(self, candle: Candlestick) -> Signal:
        """
        Event-driven execution for Golden Cross.
        """
        self.close_history.append(float(candle.close))
        
        if len(self.close_history) < self.long_window:
            return Signal(
                strategy_id=self.strategy_id, 
                product_id=self.product_id, 
                timeframe=candle.timeframe, 
                timestamp=candle.timestamp, 
                type=SignalType.NO_SIGNAL
            )
            
        # Helper to calculate mean of last N items
        def get_sma(window):
            # history is a deque, slicing not directly supported efficiently, 
            # but list(deque) is O(N). For backtest it's fine.
            # Optimized: Iteration.
            # Since we only need the tail, we can convert to list.
            data = list(self.close_history)
            return sum(data[-window:]) / window

        # We need Current and Previous SMAs to detect cross
        # Current
        curr_sma_short = get_sma(self.short_window)
        curr_sma_long = get_sma(self.long_window)
        
        # Previous
        # We need history state from one step ago.
        # But we just appended the current candle.
        # So "Previous" means calculating SMA excluding the last candle?
        # Yes.
        
        prev_history_list = list(self.close_history)[:-1]
        if len(prev_history_list) < self.long_window:
             return Signal(
                strategy_id=self.strategy_id, 
                product_id=self.product_id, 
                timeframe=candle.timeframe, 
                timestamp=candle.timestamp, 
                type=SignalType.NO_SIGNAL
            )

        prev_sma_short = sum(prev_history_list[-self.short_window:]) / self.short_window
        prev_sma_long = sum(prev_history_list[-self.long_window:]) / self.long_window
        
        curr_bullish = curr_sma_short > curr_sma_long
        prev_bullish = prev_sma_short > prev_sma_long
        
        signal_type = SignalType.NO_SIGNAL
        
        if curr_bullish and not prev_bullish:
            signal_type = SignalType.LONG
        elif not curr_bullish and prev_bullish:
            signal_type = SignalType.EXIT_LONG
            
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            type=signal_type
        )
