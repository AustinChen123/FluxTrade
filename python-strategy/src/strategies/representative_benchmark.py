"""Representative event-driven strategy for GA performance benchmarks.

This strategy deliberately combines several common incremental features without
claiming production trading merit. It exercises the normal strategy and matching
paths so benchmark results remain representative of live-compatible code.
"""

from __future__ import annotations

from collections import deque
from decimal import Decimal

from src.core.models import Candlestick, Signal, SignalType
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy, StrategyRequirements


class RepresentativeBenchmarkStrategy(BaseStrategy):
    """Score trend, breakout, momentum, volume, FVG, and confirmed swings."""

    def __init__(
        self,
        strategy_id: str,
        product_id: str,
        *,
        timeframe: str,
        trend_window: int,
        breakout_window: int,
        atr_window: int,
        rsi_window: int,
        volume_window: int,
        swing_window: int,
        entry_score: int,
        hold_bars: int,
        max_atr_expansion: Decimal | str,
        quantity: Decimal | str,
    ) -> None:
        super().__init__(strategy_id, product_id)
        self.timeframe = timeframe
        self.trend_window = _positive_int(trend_window, "trend_window")
        self.breakout_window = _positive_int(breakout_window, "breakout_window")
        self.atr_window = _positive_int(atr_window, "atr_window")
        self.rsi_window = _positive_int(rsi_window, "rsi_window")
        self.volume_window = _positive_int(volume_window, "volume_window")
        self.swing_window = _positive_int(swing_window, "swing_window")
        self.entry_score = _positive_int(entry_score, "entry_score")
        self.hold_bars = _positive_int(hold_bars, "hold_bars")
        if self.entry_score > 6:
            raise ValueError("entry_score must be at most 6")

        self.max_atr_expansion = _positive_decimal(
            max_atr_expansion,
            "max_atr_expansion",
        )
        self.quantity = _positive_decimal(quantity, "quantity")

        self._closes: deque[Decimal] = deque()
        self._close_sum = Decimal("0")
        self._highs: deque[Decimal] = deque(maxlen=self.breakout_window)
        self._lows: deque[Decimal] = deque(maxlen=self.breakout_window)
        self._true_ranges: deque[Decimal] = deque()
        self._true_range_sum = Decimal("0")
        self._gains: deque[Decimal] = deque()
        self._gain_sum = Decimal("0")
        self._losses: deque[Decimal] = deque()
        self._loss_sum = Decimal("0")
        self._volumes: deque[Decimal] = deque()
        self._volume_sum = Decimal("0")
        self._recent_candles: deque[Candlestick] = deque(maxlen=2)
        self._swing_span = 2 * self.swing_window + 1
        self._swing_candles: deque[Candlestick] = deque(maxlen=self._swing_span)
        self._previous_close: Decimal | None = None
        self._latest_true_range = Decimal("0")
        self._fvg_bias = 0
        self._last_swing_high_timestamp: int | None = None
        self._last_swing_low_timestamp: int | None = None
        self.position = 0
        self._bars_held = 0

    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(
            product_id=self.product_id,
            timeframe=self.timeframe,
            lookback_window=max(
                self.trend_window,
                self.breakout_window,
                self.atr_window + 1,
                self.rsi_window + 1,
                self.volume_window,
                2 * self.swing_window + 1,
            ),
        )

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        prior_high = max(self._highs) if len(self._highs) == self.breakout_window else None
        prior_low = min(self._lows) if len(self._lows) == self.breakout_window else None
        self._update_features(candle)

        if self.position:
            self._bars_held += 1
            if self._bars_held >= self.hold_bars:
                signal_type = (
                    SignalType.EXIT_LONG
                    if self.position == 1
                    else SignalType.EXIT_SHORT
                )
                self.position = 0
                self._bars_held = 0
                return self._signal(candle, signal_type)
            return self._signal(candle, SignalType.NO_SIGNAL)

        if context is not None and not context.risk.trading_enabled:
            return self._signal(candle, SignalType.NO_SIGNAL)

        if not self._ready() or prior_high is None or prior_low is None:
            return self._signal(candle, SignalType.NO_SIGNAL)

        trend_mean = self._close_sum / Decimal(self.trend_window)
        average_gain = self._gain_sum / Decimal(self.rsi_window)
        average_loss = self._loss_sum / Decimal(self.rsi_window)
        rsi = _rsi(average_gain, average_loss)
        average_volume = self._volume_sum / Decimal(self.volume_window)
        average_true_range = self._true_range_sum / Decimal(self.atr_window)
        if self._latest_true_range > average_true_range * self.max_atr_expansion:
            return self._signal(candle, SignalType.NO_SIGNAL)
        long_score, short_score = self._scores(
            candle,
            trend_mean=trend_mean,
            rsi=rsi,
            average_volume=average_volume,
            prior_high=prior_high,
            prior_low=prior_low,
        )
        metadata = {
            "long_score": str(long_score),
            "short_score": str(short_score),
            "trend_mean": str(trend_mean),
            "rsi": str(rsi),
        }

        if long_score >= self.entry_score and long_score > short_score:
            self.position = 1
            self._bars_held = 0
            return self._signal(candle, SignalType.LONG, metadata=metadata)
        if short_score >= self.entry_score and short_score > long_score:
            self.position = -1
            self._bars_held = 0
            return self._signal(candle, SignalType.SHORT, metadata=metadata)
        return self._signal(candle, SignalType.NO_SIGNAL, metadata=metadata)

    def sync_position_state(self, position_side: str | None) -> bool:
        normalized = position_side.upper() if position_side else None
        if normalized not in (None, "LONG", "SHORT"):
            return False
        self.position = 1 if normalized == "LONG" else -1 if normalized == "SHORT" else 0
        self._bars_held = 0
        return True

    @property
    def last_confirmed_swing(self) -> tuple[str, int] | None:
        if self._last_swing_high_timestamp is None:
            if self._last_swing_low_timestamp is None:
                return None
            return ("LOW", self._last_swing_low_timestamp)
        if self._last_swing_low_timestamp is None:
            return ("HIGH", self._last_swing_high_timestamp)
        if self._last_swing_high_timestamp > self._last_swing_low_timestamp:
            return ("HIGH", self._last_swing_high_timestamp)
        return ("LOW", self._last_swing_low_timestamp)

    def _update_features(self, candle: Candlestick) -> None:
        if len(self._recent_candles) == 2:
            two_bars_ago = self._recent_candles[0]
            if candle.low > two_bars_ago.high:
                self._fvg_bias = 1
            elif candle.high < two_bars_ago.low:
                self._fvg_bias = -1

        true_range = candle.high - candle.low
        if self._previous_close is not None:
            change = candle.close - self._previous_close
            true_range = max(
                true_range,
                abs(candle.high - self._previous_close),
                abs(candle.low - self._previous_close),
            )
            self._append_rolling(
                self._gains,
                max(change, Decimal("0")),
                self.rsi_window,
                "_gain_sum",
            )
            self._append_rolling(
                self._losses,
                max(-change, Decimal("0")),
                self.rsi_window,
                "_loss_sum",
            )
        self._latest_true_range = true_range

        self._append_rolling(
            self._closes,
            candle.close,
            self.trend_window,
            "_close_sum",
        )
        self._append_rolling(
            self._true_ranges,
            true_range,
            self.atr_window,
            "_true_range_sum",
        )
        self._append_rolling(
            self._volumes,
            candle.volume,
            self.volume_window,
            "_volume_sum",
        )
        self._highs.append(candle.high)
        self._lows.append(candle.low)
        self._recent_candles.append(candle)
        self._swing_candles.append(candle)
        self._confirm_swing()
        self._previous_close = candle.close

    def _append_rolling(
        self,
        values: deque[Decimal],
        value: Decimal,
        window: int,
        sum_attribute: str,
    ) -> None:
        total = getattr(self, sum_attribute)
        values.append(value)
        total += value
        if len(values) > window:
            total -= values.popleft()
        setattr(self, sum_attribute, total)

    def _confirm_swing(self) -> None:
        if len(self._swing_candles) < self._swing_span:
            return
        candles = list(self._swing_candles)
        center = candles[self.swing_window]
        others = candles[: self.swing_window] + candles[self.swing_window + 1 :]
        if all(center.high > candidate.high for candidate in others):
            self._last_swing_high_timestamp = center.timestamp
        if all(center.low < candidate.low for candidate in others):
            self._last_swing_low_timestamp = center.timestamp

    def _ready(self) -> bool:
        return (
            len(self._closes) == self.trend_window
            and len(self._true_ranges) == self.atr_window
            and len(self._gains) == self.rsi_window
            and len(self._volumes) == self.volume_window
            and len(self._swing_candles) == self._swing_span
        )

    def _scores(
        self,
        candle: Candlestick,
        *,
        trend_mean: Decimal,
        rsi: Decimal,
        average_volume: Decimal,
        prior_high: Decimal,
        prior_low: Decimal,
    ) -> tuple[int, int]:
        long_score = int(candle.close > trend_mean)
        short_score = int(candle.close < trend_mean)
        long_score += int(candle.close > prior_high)
        short_score += int(candle.close < prior_low)
        long_score += int(rsi >= Decimal("55"))
        short_score += int(rsi <= Decimal("45"))
        long_score += int(candle.volume >= average_volume)
        short_score += int(candle.volume >= average_volume)
        long_score += int(self._fvg_bias == 1)
        short_score += int(self._fvg_bias == -1)
        latest_swing = self.last_confirmed_swing
        long_score += int(latest_swing is not None and latest_swing[0] == "LOW")
        short_score += int(latest_swing is not None and latest_swing[0] == "HIGH")
        return long_score, short_score

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


def representative_strategy_factory(
    strategy_id: str,
    product_id: str,
    timeframe: str,
    param_pack: dict,
) -> RepresentativeBenchmarkStrategy:
    required = (
        "trend_window",
        "breakout_window",
        "atr_window",
        "rsi_window",
        "volume_window",
        "swing_window",
        "entry_score",
        "hold_bars",
        "max_atr_expansion",
    )
    missing = [name for name in required if name not in param_pack]
    if missing:
        raise ValueError(f"candidate param_pack missing: {', '.join(missing)}")
    return RepresentativeBenchmarkStrategy(
        strategy_id,
        product_id,
        timeframe=timeframe,
        trend_window=int(param_pack["trend_window"]),
        breakout_window=int(param_pack["breakout_window"]),
        atr_window=int(param_pack["atr_window"]),
        rsi_window=int(param_pack["rsi_window"]),
        volume_window=int(param_pack["volume_window"]),
        swing_window=int(param_pack["swing_window"]),
        entry_score=int(param_pack["entry_score"]),
        hold_bars=int(param_pack["hold_bars"]),
        max_atr_expansion=Decimal(str(param_pack["max_atr_expansion"])),
        quantity=Decimal(str(param_pack.get("quantity", "1"))),
    )


def _positive_int(value: int, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")
    return value


def _positive_decimal(value: Decimal | str, field_name: str) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite() or result <= 0:
        raise ValueError(f"{field_name} must be finite and positive")
    return result


def _rsi(average_gain: Decimal, average_loss: Decimal) -> Decimal:
    if average_loss == 0:
        return Decimal("100") if average_gain > 0 else Decimal("50")
    relative_strength = average_gain / average_loss
    return Decimal("100") - Decimal("100") / (Decimal("1") + relative_strength)
