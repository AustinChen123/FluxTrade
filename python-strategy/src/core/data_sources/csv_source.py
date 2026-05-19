import os
from decimal import Decimal
from typing import Generator, Optional

import pandas as pd

from src.core.interfaces.data_source import IDataSource
from src.core.models import Candlestick

# Column name aliases for auto-detection
_COLUMN_ALIASES = {
    "timestamp": ["timestamp", "time", "ts", "date", "datetime"],
    "open": ["open", "Open", "o"],
    "high": ["high", "High", "h"],
    "low": ["low", "Low", "l"],
    "close": ["close", "Close", "c", "adj close", "Adj Close"],
    "volume": ["volume", "Volume", "vol", "Vol", "v"],
}


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Map various CSV column names to standard OHLCV names."""
    col_map = {}
    lower_cols = {c.lower().strip(): c for c in df.columns}

    for standard, aliases in _COLUMN_ALIASES.items():
        for alias in aliases:
            if alias.lower() in lower_cols:
                col_map[lower_cols[alias.lower()]] = standard
                break

    return df.rename(columns=col_map)


def _parse_timestamp(df: pd.DataFrame) -> pd.DataFrame:
    """Convert timestamp column to millisecond int if needed."""
    if "timestamp" not in df.columns:
        raise ValueError("CSV must have a timestamp/time/date column")

    sample = df["timestamp"].iloc[0]

    # Already numeric (epoch ms or seconds)
    if pd.api.types.is_numeric_dtype(df["timestamp"]):
        # If value looks like seconds (< 1e12), convert to ms
        if float(sample) < 1e12:
            df["timestamp"] = (df["timestamp"] * 1000).astype(int)
        else:
            df["timestamp"] = df["timestamp"].astype(int)
    else:
        # Parse date string → epoch ms
        df["timestamp"] = (
            pd.to_datetime(df["timestamp"]).astype("int64") // 10**6
        )

    return df


class CsvDataSource(IDataSource):
    """Data source that reads candlestick data from a CSV file.

    Auto-detects column names from common formats:
      - Standard: timestamp,open,high,low,close,volume
      - TradingView: time,open,high,low,close,Volume
      - Yahoo Finance: Date,Open,High,Low,Close,Adj Close,Volume
    """

    def __init__(
        self,
        file_path: str,
        product_id: str = "CSV:DATA-PERP",
        timeframe: str = "1m",
    ):
        self._file_path = file_path
        self._product_id = product_id
        self._timeframe = timeframe
        self._df: pd.DataFrame | None = None

    def _load(self) -> pd.DataFrame:
        """Lazy-load and normalize the CSV on first access."""
        if self._df is not None:
            return self._df

        df = pd.read_csv(self._file_path)
        df = _normalize_columns(df)
        df = _parse_timestamp(df)

        required = {"timestamp", "open", "high", "low", "close", "volume"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"CSV missing required columns: {missing}")

        df = df.sort_values("timestamp").reset_index(drop=True)
        self._df = df
        return df

    def get_candles(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> Generator[Candlestick, None, None]:
        if product_id != self._product_id or timeframe != self._timeframe:
            return

        df = self._load()
        mask = (df["timestamp"] >= start) & (df["timestamp"] <= end)

        for _, row in df[mask].iterrows():
            yield Candlestick(
                product_id=self._product_id,
                timeframe=self._timeframe,
                timestamp=int(row["timestamp"]),
                open=Decimal(str(row["open"])),
                high=Decimal(str(row["high"])),
                low=Decimal(str(row["low"])),
                close=Decimal(str(row["close"])),
                volume=Decimal(str(row["volume"])),
            )

    def get_candles_df(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> pd.DataFrame:
        if product_id != self._product_id or timeframe != self._timeframe:
            return pd.DataFrame(
                columns=["open", "high", "low", "close", "volume"],
                index=pd.Index([], name="timestamp"),
            )

        df = self._load()
        mask = (df["timestamp"] >= start) & (df["timestamp"] <= end)
        result = df.loc[mask, ["timestamp", "open", "high", "low", "close", "volume"]].copy()

        for col in ("open", "high", "low", "close", "volume"):
            result[col] = result[col].astype(float)

        if not result.empty:
            result.set_index("timestamp", inplace=True)
        return result

    def get_available_range(
        self, product_id: str, timeframe: str
    ) -> Optional[tuple[int, int]]:
        if product_id != self._product_id or timeframe != self._timeframe:
            return None

        df = self._load()
        if df.empty:
            return None
        return (int(df["timestamp"].iloc[0]), int(df["timestamp"].iloc[-1]))

    def validate(self) -> bool:
        if not os.path.isfile(self._file_path):
            return False
        try:
            self._load()
            return True
        except Exception:
            return False
