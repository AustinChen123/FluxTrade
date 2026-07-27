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
        *,
        replay_predict_factory: (
            Callable[[], Callable[[Candlestick], Optional[Signal]]] | None
        ) = None,
        replay_config: object | None = None,
    ):
        super().__init__(strategy_id, product_id)
        self._predict_fn = predict_fn
        self._timeframe = timeframe
        self._lookback_window = lookback_window
        self._replay_predict_factory = replay_predict_factory
        self._replay_config = replay_config
        if (replay_predict_factory is None) != (replay_config is None):
            raise ValueError(
                "replay_predict_factory and replay_config must be provided together"
            )

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=self._timeframe,
            lookback_window=self._lookback_window,
        )

    def fresh_instance_for_replay(self) -> BaseStrategy:
        if self._replay_predict_factory is None:
            raise NotImplementedError(
                "callable strategy requires an explicit replay_predict_factory"
            )
        return type(self)(
            self.strategy_id,
            self._replay_predict_factory(),
            self.product_id,
            self._timeframe,
            self._lookback_window,
            replay_predict_factory=self._replay_predict_factory,
            replay_config=self._replay_config,
        )

    def replay_configuration(self) -> object:
        return (
            self._timeframe,
            self._lookback_window,
            self._replay_config,
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
