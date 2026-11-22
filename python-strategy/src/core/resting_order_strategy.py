from abc import ABC, abstractmethod
from dataclasses import dataclass
import math
from typing import Literal


Side = Literal["buy", "sell"]


def _validate_positive_finite(name: str, value: float) -> None:
    if type(value) not in (int, float) or not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive")


def _validate_side(side: str) -> None:
    if side not in ("buy", "sell"):
        raise ValueError("side must be 'buy' or 'sell'")


def _validate_ts(ts_ms: int) -> None:
    if type(ts_ms) is not int or ts_ms % 60_000 != 0:
        raise ValueError("ts_ms must be an integer minute timestamp")


@dataclass(frozen=True)
class MinuteBar:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float

    def __post_init__(self) -> None:
        _validate_ts(self.ts_ms)
        for name in ("open", "high", "low", "close"):
            _validate_positive_finite(name, getattr(self, name))
        if (
            not self.low
            <= min(self.open, self.close)
            <= max(self.open, self.close)
            <= self.high
        ):
            raise ValueError("bar prices violate OHLC ordering")


@dataclass(frozen=True)
class DesiredOrder:
    side: Side
    price: float
    quantity: float

    def __post_init__(self) -> None:
        _validate_side(self.side)
        _validate_positive_finite("price", self.price)
        _validate_positive_finite("quantity", self.quantity)


@dataclass(frozen=True)
class RestingIntent:
    orders: tuple[DesiredOrder, ...]
    flatten: bool = False

    def __post_init__(self) -> None:
        if type(self.orders) is not tuple or any(
            type(order) is not DesiredOrder for order in self.orders
        ):
            raise ValueError("orders must be a tuple of DesiredOrder")
        seen: set[tuple[str, float]] = set()
        buys: list[float] = []
        sells: list[float] = []
        for order in self.orders:
            key = (order.side, order.price)
            if key in seen:
                raise ValueError("duplicate side and price")
            seen.add(key)
            (buys if order.side == "buy" else sells).append(order.price)
        if buys and sells and max(buys) >= min(sells):
            raise ValueError("buy prices must be below sell prices")
        if type(self.flatten) is not bool:
            raise ValueError("flatten must be bool")


@dataclass(frozen=True)
class RestingFill:
    ts_ms: int
    side: Side
    price: float
    quantity: float

    def __post_init__(self) -> None:
        _validate_ts(self.ts_ms)
        _validate_side(self.side)
        _validate_positive_finite("price", self.price)
        _validate_positive_finite("quantity", self.quantity)


class RestingOrderStrategy(ABC):
    @abstractmethod
    def on_bar(self, bar: MinuteBar) -> RestingIntent:
        raise NotImplementedError

    @abstractmethod
    def on_fill(self, fill: RestingFill) -> RestingIntent:
        raise NotImplementedError
