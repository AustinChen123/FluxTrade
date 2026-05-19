"""DEPRECATED: Use src.core.data_sources.DatabaseDataSource instead.

This module is kept for backward compatibility only.
All new code should use IDataSource implementations directly.
"""

import warnings
from typing import Generator

import pandas as pd
from sqlalchemy.orm import Session

from src.core.data_sources.database import DatabaseDataSource
from src.core.models import Candlestick


def get_candles_df(
    product_id: str, start: int, end: int, timeframe: str = "1m"
) -> pd.DataFrame:
    """Deprecated. Use DatabaseDataSource.get_candles_df() instead."""
    warnings.warn(
        "get_candles_df() is deprecated. Use DatabaseDataSource.get_candles_df().",
        DeprecationWarning,
        stacklevel=2,
    )
    ds = DatabaseDataSource()
    return ds.get_candles_df(product_id, timeframe, start, end)


def get_candles_generator(
    session: Session,
    product_id: str,
    timeframe: str,
    start: int,
    end: int,
) -> Generator[Candlestick, None, None]:
    """Deprecated. Use DatabaseDataSource.get_candles() instead."""
    warnings.warn(
        "get_candles_generator() is deprecated. Use DatabaseDataSource.get_candles().",
        DeprecationWarning,
        stacklevel=2,
    )
    ds = DatabaseDataSource(session_factory=lambda: session)
    yield from ds.get_candles(product_id, timeframe, start, end)
