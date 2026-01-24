import random
from src.core.models import Candlestick, Signal, SignalType
from src.strategies.base import BaseStrategy

class RandomStrategy(BaseStrategy):
    def __init__(self, strategy_id: str, product_id: str):
        super().__init__(strategy_id, product_id)

    def on_candle(self, candle: Candlestick) -> Signal:
        # Only process relevant product
        if candle.product_id != self.product_id:
            return Signal(
                strategy_id=self.strategy_id,
                product_id=candle.product_id,
                timeframe=candle.timeframe,
                timestamp=candle.timestamp,
                type=SignalType.NO_SIGNAL
            )

        # Simple logic: 10% chance to generate a signal
        roll = random.random()
        signal_type = SignalType.NO_SIGNAL
        
        if roll < 0.05:
            signal_type = SignalType.LONG
        elif roll < 0.10:
            signal_type = SignalType.SHORT
            
        return Signal(
            strategy_id=self.strategy_id,
            product_id=candle.product_id,
            timeframe=candle.timeframe,
            timestamp=candle.timestamp,
            type=signal_type,
            value=candle.close,
            metadata={"roll": roll}
        )