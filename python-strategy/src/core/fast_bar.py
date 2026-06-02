"""Live-compatible fast-bar replay primitives.

The fast-bar path keeps strategy decisions one candle at a time while avoiding
Pydantic candle/signal objects in the hot strategy loop. It is source-agnostic:
CSV, DB, memory data sources, and live append flows can all build a MarketTape.
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from decimal import Decimal
from typing import Deque, Iterable, Protocol

import numpy as np

from src.core.analytics import calculate_metrics
from src.core.interfaces.data_source import IDataSource
from src.core.models import Candlestick, OrderSide, SignalType
from src.core.research_backtest_runner import ResearchTrade
from src.strategies.base import BaseStrategy


class PreparedStrategy(Protocol):
    """Fast strategy runtime prepared once for a specific replay/live context."""

    strategy_id: str

    def on_bar(self, bar: "BarView") -> "SignalIntent | None":
        """Return a lightweight signal intent for the current bar, if any."""


def prepare_fast_strategy(strategy: BaseStrategy) -> PreparedStrategy:
    """Return a prepared fast-bar strategy or fail with an explicit message."""
    prepare_fast = getattr(strategy, "prepare_fast", None)
    if prepare_fast is None or not callable(prepare_fast):
        raise TypeError(
            f"{type(strategy).__name__} does not support fast-bar execution; "
            "implement prepare_fast() or use ResearchBacktestRunner."
        )
    return prepare_fast()


@dataclass(frozen=True, slots=True)
class SignalIntent:
    """Lightweight strategy intent emitted by PreparedStrategy.on_bar()."""

    type: SignalType
    quantity: Decimal | None = None
    price: Decimal | None = None


@dataclass(slots=True)
class RollingMean:
    """Small Python-side helper for fast custom indicator code."""

    window: int
    _values: Deque[float] | None = None
    _sum: float = 0.0

    def __post_init__(self) -> None:
        if self.window <= 0:
            raise ValueError("window must be positive")
        if self._values is None:
            self._values = deque()

    def append(self, value: float) -> None:
        values = self._values
        if values is None:
            raise RuntimeError("RollingMean was not initialized")
        values.append(value)
        self._sum += value
        if len(values) > self.window:
            self._sum -= values.popleft()

    @property
    def ready(self) -> bool:
        values = self._values
        return values is not None and len(values) == self.window

    @property
    def mean(self) -> float:
        if not self.ready:
            raise ValueError("rolling mean is not ready")
        return self._sum / self.window


@dataclass(slots=True)
class MarketTape:
    """Array-backed OHLCV tape shared by live-compatible fast replay."""

    product_id: str
    timeframe: str
    timestamps: np.ndarray
    open: np.ndarray
    high: np.ndarray
    low: np.ndarray
    close: np.ndarray
    volume: np.ndarray
    _length: int | None = None

    def __post_init__(self) -> None:
        array_lengths = [
            len(self.timestamps),
            len(self.open),
            len(self.high),
            len(self.low),
            len(self.close),
            len(self.volume),
        ]
        lengths = set(array_lengths)
        if len(lengths) != 1:
            raise ValueError("all MarketTape arrays must have equal length")
        capacity = array_lengths[0]
        if self._length is None:
            self._length = capacity
        if self._length < 0 or self._length > capacity:
            raise ValueError("MarketTape length must be between zero and array capacity")
        self.timestamps = self.timestamps.astype(np.int64, copy=False)
        self.open = self.open.astype(np.float64, copy=False)
        self.high = self.high.astype(np.float64, copy=False)
        self.low = self.low.astype(np.float64, copy=False)
        self.close = self.close.astype(np.float64, copy=False)
        self.volume = self.volume.astype(np.float64, copy=False)

    def __len__(self) -> int:
        return int(self._length or 0)

    @property
    def capacity(self) -> int:
        return len(self.timestamps)

    @classmethod
    def from_candles(
        cls,
        candles: Iterable[Candlestick],
        *,
        product_id: str,
        timeframe: str,
    ) -> "MarketTape":
        rows = [
            candle
            for candle in candles
            if candle.product_id == product_id and candle.timeframe == timeframe
        ]
        rows.sort(key=lambda candle: candle.timestamp)
        return cls(
            product_id=product_id,
            timeframe=timeframe,
            timestamps=np.array([c.timestamp for c in rows], dtype=np.int64),
            open=np.array([float(c.open) for c in rows], dtype=np.float64),
            high=np.array([float(c.high) for c in rows], dtype=np.float64),
            low=np.array([float(c.low) for c in rows], dtype=np.float64),
            close=np.array([float(c.close) for c in rows], dtype=np.float64),
            volume=np.array([float(c.volume) for c in rows], dtype=np.float64),
        )

    @classmethod
    def from_data_source(
        cls,
        data_source: IDataSource,
        *,
        product_id: str,
        timeframe: str,
        start_time: int,
        end_time: int,
    ) -> "MarketTape":
        frame = data_source.get_candles_df(product_id, timeframe, start_time, end_time)
        if not frame.empty:
            timestamps = (
                frame["timestamp"].to_numpy(dtype=np.int64)
                if "timestamp" in frame.columns
                else frame.index.to_numpy(dtype=np.int64)
            )
            return cls(
                product_id=product_id,
                timeframe=timeframe,
                timestamps=timestamps,
                open=frame["open"].to_numpy(dtype=np.float64),
                high=frame["high"].to_numpy(dtype=np.float64),
                low=frame["low"].to_numpy(dtype=np.float64),
                close=frame["close"].to_numpy(dtype=np.float64),
                volume=frame["volume"].to_numpy(dtype=np.float64),
            )
        return cls.from_candles(
            data_source.get_candles(product_id, timeframe, start_time, end_time),
            product_id=product_id,
            timeframe=timeframe,
        )

    @classmethod
    def empty(
        cls,
        *,
        product_id: str,
        timeframe: str,
        capacity: int = 0,
    ) -> "MarketTape":
        if capacity < 0:
            raise ValueError("capacity must be non-negative")
        return cls(
            product_id=product_id,
            timeframe=timeframe,
            timestamps=np.empty(capacity, dtype=np.int64),
            open=np.empty(capacity, dtype=np.float64),
            high=np.empty(capacity, dtype=np.float64),
            low=np.empty(capacity, dtype=np.float64),
            close=np.empty(capacity, dtype=np.float64),
            volume=np.empty(capacity, dtype=np.float64),
            _length=0,
        )

    def append_candle(self, candle: Candlestick) -> None:
        if candle.product_id != self.product_id or candle.timeframe != self.timeframe:
            return
        index = len(self)
        if index >= self.capacity:
            self._grow(max(1, self.capacity * 2))
        self.timestamps[index] = np.int64(candle.timestamp)
        self.open[index] = float(candle.open)
        self.high[index] = float(candle.high)
        self.low[index] = float(candle.low)
        self.close[index] = float(candle.close)
        self.volume[index] = float(candle.volume)
        self._length = index + 1

    def compact(self) -> "MarketTape":
        """Return a view-sized tape without unused live-append capacity."""
        length = len(self)
        return MarketTape(
            product_id=self.product_id,
            timeframe=self.timeframe,
            timestamps=self.timestamps[:length].copy(),
            open=self.open[:length].copy(),
            high=self.high[:length].copy(),
            low=self.low[:length].copy(),
            close=self.close[:length].copy(),
            volume=self.volume[:length].copy(),
        )

    def _grow(self, capacity: int) -> None:
        self.timestamps = _resize_array(self.timestamps, capacity, np.int64)
        self.open = _resize_array(self.open, capacity, np.float64)
        self.high = _resize_array(self.high, capacity, np.float64)
        self.low = _resize_array(self.low, capacity, np.float64)
        self.close = _resize_array(self.close, capacity, np.float64)
        self.volume = _resize_array(self.volume, capacity, np.float64)


@dataclass(slots=True)
class BarView:
    """Reusable current-bar view over a MarketTape."""

    tape: MarketTape
    index: int = 0
    timestamp_value: int = 0
    open_value: float = 0.0
    high_value: float = 0.0
    low_value: float = 0.0
    close_value: float = 0.0
    volume_value: float = 0.0

    def move_to(self, index: int) -> None:
        self.index = index
        self.timestamp_value = int(self.tape.timestamps[index])
        self.open_value = float(self.tape.open[index])
        self.high_value = float(self.tape.high[index])
        self.low_value = float(self.tape.low[index])
        self.close_value = float(self.tape.close[index])
        self.volume_value = float(self.tape.volume[index])

    @property
    def timestamp(self) -> int:
        return self.timestamp_value

    @property
    def open(self) -> float:
        return self.open_value

    @property
    def high(self) -> float:
        return self.high_value

    @property
    def low(self) -> float:
        return self.low_value

    @property
    def close(self) -> float:
        return self.close_value

    @property
    def volume(self) -> float:
        return self.volume_value

    def close_window(self, window: int) -> np.ndarray:
        start = max(0, self.index + 1 - window)
        return self.tape.close[start : self.index + 1]


class FastBarReplayRunner:
    """Replay PreparedStrategy over MarketTape while matching through Rust."""

    def __init__(
        self,
        *,
        tape: MarketTape,
        strategy: PreparedStrategy,
        initial_balance: Decimal = Decimal("10000"),
        maker_fee: Decimal = Decimal("0"),
        taker_fee: Decimal = Decimal("0"),
    ) -> None:
        self.tape = tape
        self.strategy = strategy
        self.initial_balance = Decimal(str(initial_balance))
        self.maker_fee = Decimal(str(maker_fee))
        self.taker_fee = Decimal(str(taker_fee))

    def run(self) -> dict:
        from fluxtrade_core import Candlestick as RustCandlestick
        from fluxtrade_core import Order as RustOrder
        from fluxtrade_core import PyMatchingEngine

        engine = PyMatchingEngine(
            str(self.initial_balance),
            maker_fee=str(self.maker_fee),
            taker_fee=str(self.taker_fee),
        )
        bar = BarView(self.tape)
        trades: list[ResearchTrade] = []
        order_strategy: dict[str, str] = {}
        order_side: dict[str, OrderSide] = {}
        order_counter = 0

        product_id = self.tape.product_id
        timeframe = self.tape.timeframe
        strategy_id = self.strategy.strategy_id
        timestamps = self.tape.timestamps
        opens = self.tape.open
        highs = self.tape.high
        lows = self.tape.low
        closes = self.tape.close
        volumes = self.tape.volume
        for index in range(len(self.tape)):
            bar.move_to(index)
            rust_candle = RustCandlestick(
                product_id=product_id,
                timeframe=timeframe,
                timestamp=int(timestamps[index]),
                open=str(opens[index]),
                high=str(highs[index]),
                low=str(lows[index]),
                close=str(closes[index]),
                volume=str(volumes[index]),
            )
            fills = engine.on_candle(rust_candle)
            for fill in fills:
                trades.append(
                    ResearchTrade(
                        id=f"fast_fill_{len(trades) + 1}",
                        order_id=fill.order_id,
                        product_id=fill.product_id,
                        strategy_id=order_strategy.get(fill.order_id, strategy_id),
                        side=order_side.get(fill.order_id, OrderSide.BUY),
                        price=Decimal(fill.price),
                        quantity=Decimal(fill.quantity),
                        fee=Decimal(fill.fee),
                        timestamp=fill.timestamp,
                    )
                )

            intent = self.strategy.on_bar(bar)
            if intent is None or intent.type == SignalType.NO_SIGNAL:
                continue
            side = _rust_side(intent.type)
            if side is None:
                continue
            trade_side = _trade_side(intent.type)
            if trade_side is None:
                continue
            quantity = intent.quantity if intent.quantity and intent.quantity > 0 else Decimal("0.01")
            order_counter += 1
            order_id = f"fast_{order_counter}"
            order_strategy[order_id] = strategy_id
            order_side[order_id] = trade_side
            engine.submit_order(
                RustOrder(
                    id=order_id,
                    product_id=product_id,
                    side=side,
                    order_type="MARKET",
                    price=str(intent.price) if intent.price else "0",
                    quantity=str(quantity),
                    timestamp=bar.timestamp,
                    strategy_id=strategy_id,
                )
            )

        final_balance = Decimal(engine.balance)
        metrics = calculate_metrics(trades, initial_balance=float(self.initial_balance))
        return {
            "total_pnl": final_balance - self.initial_balance,
            "max_drawdown": metrics.get("max_drawdown", Decimal("0")),
            "win_rate": metrics.get("win_rate", Decimal("0")),
            "profit_factor": metrics.get("profit_factor", Decimal("0")),
            "total_trades": int(metrics.get("total_trades", 0)),
            "raw_trade_count": len(trades),
            "raw_trades": trades,
            "closed_trades": metrics.get("closed_trades", []),
            "candle_count": len(self.tape),
        }

def _resize_array(array: np.ndarray, capacity: int, dtype: np.dtype) -> np.ndarray:
    resized = np.empty(capacity, dtype=dtype)
    resized[: len(array)] = array
    return resized


def _rust_side(signal_type: SignalType) -> str | None:
    if signal_type == SignalType.LONG:
        return "LONG"
    if signal_type == SignalType.SHORT:
        return "SHORT"
    if signal_type == SignalType.EXIT_LONG:
        return "SHORT"
    if signal_type == SignalType.EXIT_SHORT:
        return "LONG"
    return None


def _trade_side(signal_type: SignalType) -> OrderSide | None:
    if signal_type == SignalType.LONG:
        return OrderSide.BUY
    if signal_type == SignalType.SHORT:
        return OrderSide.SELL
    if signal_type == SignalType.EXIT_LONG:
        return OrderSide.SELL
    if signal_type == SignalType.EXIT_SHORT:
        return OrderSide.BUY
    return None
