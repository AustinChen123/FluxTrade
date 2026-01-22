from abc import ABC, abstractmethod
from src.core.models import Candlestick, Signal

class BaseStrategy(ABC):
    def __init__(self, strategy_id: str):
        self.strategy_id = strategy_id

    @abstractmethod
    def on_candle(self, candle: Candlestick) -> Signal:
        """
        Process a new candlestick and optionally return a trading signal.
        """
        pass
