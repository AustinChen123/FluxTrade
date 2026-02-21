"""Unit tests for CallableStrategy."""
from decimal import Decimal
from src.core.models import Candlestick, Signal, SignalType
from src.strategies.callable_strategy import CallableStrategy


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "15m"


def _make_candle(ts: int = 1_700_000_000_000, close: Decimal = Decimal("50000")) -> Candlestick:
    return Candlestick(
        product_id=PRODUCT_ID, timeframe=TIMEFRAME, timestamp=ts,
        open=close, high=close + Decimal("100"), low=close - Decimal("100"),
        close=close, volume=Decimal("100"),
    )


class TestCallableStrategy:

    def test_predict_fn_returning_signal(self):
        """When predict_fn returns a Signal, on_candle returns it with correct strategy_id."""
        def predict(candle):
            return Signal(
                strategy_id="wrong_id",
                product_id=candle.product_id,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.LONG,
                quantity=Decimal("0.1"),
            )

        strat = CallableStrategy(
            strategy_id="ml_v1",
            predict_fn=predict,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
        )
        candle = _make_candle()
        sig = strat.on_candle(candle)

        assert sig is not None
        assert sig.type == SignalType.LONG
        assert sig.strategy_id == "ml_v1"
        assert sig.quantity == Decimal("0.1")

    def test_predict_fn_returning_none(self):
        """When predict_fn returns None, on_candle returns NO_SIGNAL."""
        strat = CallableStrategy(
            strategy_id="ml_v2",
            predict_fn=lambda c: None,
            product_id=PRODUCT_ID,
            timeframe=TIMEFRAME,
        )
        sig = strat.on_candle(_make_candle())
        assert sig.type == SignalType.NO_SIGNAL

    def test_requirements(self):
        """Requirements should match constructor params."""
        strat = CallableStrategy(
            strategy_id="test",
            predict_fn=lambda c: None,
            product_id=PRODUCT_ID,
            timeframe="1h",
            lookback_window=50,
        )
        req = strat.requirements
        assert req.product_id == PRODUCT_ID
        assert req.timeframe == "1h"
        assert req.lookback_window == 50

    def test_signal_inherits_candle_metadata(self):
        """Signal should preserve optional fields like stop_loss and take_profit."""
        def predict(candle):
            return Signal(
                strategy_id="x",
                product_id=candle.product_id,
                timeframe=TIMEFRAME,
                timestamp=candle.timestamp,
                type=SignalType.SHORT,
                stop_loss=candle.close + Decimal("500"),
                take_profit=candle.close - Decimal("1000"),
            )

        strat = CallableStrategy("test_meta", predict, PRODUCT_ID, TIMEFRAME)
        sig = strat.on_candle(_make_candle())
        assert sig.stop_loss == Decimal("50500")
        assert sig.take_profit == Decimal("49000")
