"""Data source backed by Yahoo Finance via yfinance.

Requires: pip install yfinance  (optional dependency)

Usage:
    ds = YahooFinanceDataSource(ticker="BTC-USD")
    for candle in ds.get_candles("BINANCE:BTCUSDT-PERP", "1d", start_ms, end_ms):
        ...
"""

from datetime import datetime, timezone
from decimal import Decimal
from importlib import import_module
from typing import Generator, Optional, Protocol, runtime_checkable

import pandas as pd

from src.core.interfaces.data_source import IDataSource
from src.core.models import Candlestick


# yfinance interval strings that map to our timeframe convention.
_TF_MAP = {
    "1m": "1m",
    "2m": "2m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "1d": "1d",
    "1w": "1wk",
    "1M": "1mo",
}

_YFINANCE_IMPORT_ERROR = (
    "yfinance is required for YahooFinanceDataSource. "
    "Install with: pip install yfinance"
)


class _YFinanceTicker(Protocol):
    @property
    def info(self) -> dict[str, object]: ...


@runtime_checkable
class _YFinanceModule(Protocol):
    def download(
        self,
        tickers: str,
        *,
        start: datetime,
        end: datetime,
        interval: str,
        auto_adjust: bool,
        progress: bool,
    ) -> pd.DataFrame | None: ...

    def Ticker(self, ticker: str) -> _YFinanceTicker: ...


def _load_yfinance() -> _YFinanceModule:
    try:
        module = import_module("yfinance")
    except ImportError:
        raise ImportError(_YFINANCE_IMPORT_ERROR)

    if not isinstance(module, _YFinanceModule):
        raise ImportError(_YFINANCE_IMPORT_ERROR)
    return module


class YahooFinanceDataSource(IDataSource):
    """Data source that downloads OHLCV from Yahoo Finance.

    Useful for quick backtests on daily/hourly crypto or equity data
    without needing a local database.

    Limitations:
        - Intraday data (1m-1h) limited to last 7-60 days by Yahoo.
        - Daily data available for full history.
        - Timestamps are normalized to UTC milliseconds.
        - Volume is in base asset units (same as exchange data).
    """

    def __init__(
        self,
        ticker: str = "BTC-USD",
        product_id: str = "YAHOO:BTCUSD-PERP",
        timeframe: str = "1d",
    ):
        self._ticker = ticker
        self._product_id = product_id
        self._timeframe = timeframe
        self._df: pd.DataFrame | None = None

    def _download(self, timeframe: str, start_ms: int, end_ms: int) -> pd.DataFrame:
        """Download data from Yahoo Finance, with lazy import."""
        yf = _load_yfinance()

        interval = _TF_MAP.get(timeframe)
        if interval is None:
            raise ValueError(
                f"Unsupported timeframe '{timeframe}' for Yahoo Finance. "
                f"Supported: {list(_TF_MAP.keys())}"
            )

        start_dt = datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end_ms / 1000, tz=timezone.utc)

        df = yf.download(
            self._ticker,
            start=start_dt,
            end=end_dt,
            interval=interval,
            auto_adjust=True,
            progress=False,
        )

        if df is None or df.empty:
            return pd.DataFrame(
                columns=["timestamp", "open", "high", "low", "close", "volume"]
            )

        # Flatten MultiIndex columns if present (yfinance >= 0.2.31)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
            }
        )

        if not isinstance(df.index, pd.DatetimeIndex):
            raise TypeError("Yahoo Finance index must be a DatetimeIndex")

        # Convert index to UTC ms timestamp
        datetime_index = df.index
        utc_index = (
            datetime_index.tz_localize("UTC")
            if datetime_index.tz is None
            else datetime_index.tz_convert("UTC")
        )
        df["timestamp"] = utc_index.as_unit("ms").astype("int64")

        required_columns = [
            "timestamp",
            "open",
            "high",
            "low",
            "close",
            "volume",
        ]
        df = pd.DataFrame(df.loc[:, required_columns])
        df = df.reset_index(drop=True)

        return df

    def get_candles(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> Generator[Candlestick, None, None]:
        df = self._download(timeframe, start, end)

        for timestamp, open_, high, low, close, volume in df.itertuples(
            index=False,
            name=None,
        ):
            yield Candlestick(
                product_id=self._product_id,
                timeframe=timeframe,
                timestamp=int(timestamp),
                open=Decimal(str(open_)),
                high=Decimal(str(high)),
                low=Decimal(str(low)),
                close=Decimal(str(close)),
                volume=Decimal(str(volume)),
            )

    def get_candles_df(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> pd.DataFrame:
        df = self._download(timeframe, start, end)

        for col in ("open", "high", "low", "close", "volume"):
            df[col] = df[col].astype(float)

        if not df.empty:
            df.set_index("timestamp", inplace=True)
        return df

    def get_available_range(
        self, product_id: str, timeframe: str
    ) -> Optional[tuple[int, int]]:
        # Yahoo Finance doesn't expose range metadata without downloading.
        # Return None to signal that the caller should specify explicit bounds.
        return None

    def validate(self) -> bool:
        try:
            yf = _load_yfinance()
            info = yf.Ticker(self._ticker).info
            return info is not None and "regularMarketPrice" in info
        except Exception:
            return False
