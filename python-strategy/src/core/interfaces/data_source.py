from abc import ABC, abstractmethod
from typing import Generator, Optional

import pandas as pd

from src.core.models import Candlestick


class IDataSource(ABC):
    """Abstract data source for candlestick data.

    Implementations provide candle data from various backends
    (PostgreSQL, CSV, in-memory, etc.) through a unified interface.
    """

    @abstractmethod
    def get_candles(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> Generator[Candlestick, None, None]:
        """Yield Candlestick objects ordered by timestamp ascending.

        Args:
            product_id: Product identifier (e.g. BINANCE:BTCUSDT-PERP).
            timeframe: Candle timeframe (e.g. 1m, 5m, 15m).
            start: Start timestamp in milliseconds (inclusive).
            end: End timestamp in milliseconds (inclusive).
        """

    @abstractmethod
    def get_candles_df(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> pd.DataFrame:
        """Return DataFrame with OHLCV columns indexed by timestamp.

        Columns: open, high, low, close, volume (float).
        Index: timestamp (int, milliseconds).

        Args:
            product_id: Product identifier.
            timeframe: Candle timeframe.
            start: Start timestamp in milliseconds (inclusive).
            end: End timestamp in milliseconds (inclusive).
        """

    @abstractmethod
    def get_available_range(
        self, product_id: str, timeframe: str
    ) -> Optional[tuple[int, int]]:
        """Return (min_timestamp, max_timestamp) or None if no data.

        Args:
            product_id: Product identifier.
            timeframe: Candle timeframe.
        """

    def validate(self) -> bool:
        """Check if data source is accessible and contains valid data."""
        return True
