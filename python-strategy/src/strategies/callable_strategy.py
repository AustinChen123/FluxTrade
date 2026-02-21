"""Strategy wrapper for external callable signal sources (e.g., ML models)."""
from typing import Callable, Optional
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.core.models import Candlestick, Signal, SignalType


class CallableStrategy(BaseStrategy):
    """Wrap any fn(Candlestick) -> Signal | None as a backtestable/live strategy.

    The predict_fn receives a Candlestick and should return a Signal or None.
    Returned Signals have their strategy_id overwritten to match this strategy.
    None returns are converted to NO_SIGNAL.

    Usage:
        model = load_model("my_model.pt")
        def predict(candle):
            if model.predict(candle) > 0.7:
                return Signal(type=SignalType.LONG, ...)
            return None

        strategy = CallableStrategy("ml_v1", predict, "BINANCE:BTCUSDT-PERP", "1h")
    """

    def __init__(
        self,
        strategy_id: str,
        predict_fn: Callable[[Candlestick], Optional[Signal]],
        product_id: str,
        timeframe: str = "1h",
        lookback_window: int = 1,
    ):
        super().__init__(strategy_id, product_id)
        self._predict_fn = predict_fn
        self._timeframe = timeframe
        self._lookback_window = lookback_window

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=self._timeframe,
            lookback_window=self._lookback_window,
        )

    def on_candle(self, candle: Candlestick) -> Signal:
        result = self._predict_fn(candle)
        if result is not None:
            result.strategy_id = self.strategy_id
            return result
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe=self._timeframe,
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
            value=candle.close,
        )
