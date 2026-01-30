from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, List
import pandas as pd
from src.core.models import Candlestick, Signal, Trade
from src.core.journal import StrategyJournal

@dataclass
class StrategyRequirements:
    product_id: str
    timeframe: str
    lookback_window: int

class BaseStrategy(ABC):
    def __init__(self, strategy_id: str, product_id: str):
        self.strategy_id = strategy_id
        self.product_id = product_id
        self.journal = StrategyJournal(strategy_id)

    @property
    @abstractmethod
    def requirements(self) -> StrategyRequirements:
        """
        Define data requirements for the strategy.
        """
        pass

    @abstractmethod
    def on_candle(self, candle: Candlestick) -> Signal:
        """
        Process a new candlestick and optionally return a trading signal.
        """
        pass

    def on_trade(self, trade: Trade) -> Optional[Signal]:
        """Optional: Strategies can override to react to individual trades."""
        return None

    def run_vectorized(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run strategy in vectorized mode using Pandas.
        Expected to return DataFrame with 'signal' column.
        """
        raise NotImplementedError("Vectorized execution not implemented")