from typing import Generator, Optional

import pandas as pd

from src.core.interfaces.data_source import IDataSource
from src.core.models import Candlestick


class MemoryDataSource(IDataSource):
    """In-memory data source for testing and synthetic data.

    Accepts a pre-built list of Candlestick objects and serves them
    through the standard IDataSource interface.
    """

    def __init__(self, candles: list[Candlestick] | None = None):
        self._candles = sorted(candles or [], key=lambda c: c.timestamp)

    def add_candles(self, candles: list[Candlestick]) -> None:
        """Append candles and re-sort by timestamp."""
        self._candles.extend(candles)
        self._candles.sort(key=lambda c: c.timestamp)

    def get_candles(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> Generator[Candlestick, None, None]:
        for c in self._candles:
            if c.product_id != product_id or c.timeframe != timeframe:
                continue
            if c.timestamp < start or c.timestamp > end:
                continue
            yield c

    def get_candles_df(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> pd.DataFrame:
        rows = []
        for c in self.get_candles(product_id, timeframe, start, end):
            rows.append(
                {
                    "timestamp": c.timestamp,
                    "open": float(c.open),
                    "high": float(c.high),
                    "low": float(c.low),
                    "close": float(c.close),
                    "volume": float(c.volume),
                }
            )
        df = pd.DataFrame(rows)
        if not df.empty:
            df.set_index("timestamp", inplace=True)
        return df

    def get_available_range(
        self, product_id: str, timeframe: str
    ) -> Optional[tuple[int, int]]:
        timestamps = [
            c.timestamp
            for c in self._candles
            if c.product_id == product_id and c.timeframe == timeframe
        ]
        if not timestamps:
            return None
        return (min(timestamps), max(timestamps))

    def validate(self) -> bool:
        return len(self._candles) > 0
