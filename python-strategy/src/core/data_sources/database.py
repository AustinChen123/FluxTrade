from typing import Generator, Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from src.core.db import SessionLocal
from src.core.interfaces.data_source import IDataSource
from src.core.models import Candlestick
from src.core.orm_models import Candlestick as CandlestickORM


class DatabaseDataSource(IDataSource):
    """Data source backed by PostgreSQL via SQLAlchemy.

    Extracts the data-loading logic previously in backtest/loader.py
    into the IDataSource interface.
    """

    def __init__(self, session_factory=None):
        self._session_factory = session_factory or SessionLocal

    def get_candles(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> Generator[Candlestick, None, None]:
        session: Session = self._session_factory()
        try:
            query = session.query(CandlestickORM).filter(
                CandlestickORM.product_id == product_id,
                CandlestickORM.timeframe == timeframe,
                CandlestickORM.timestamp >= start,
                CandlestickORM.timestamp <= end,
            ).order_by(CandlestickORM.timestamp.asc())

            for row in query.yield_per(100):
                yield Candlestick(
                    product_id=row.product_id,
                    timeframe=row.timeframe,
                    timestamp=row.timestamp,
                    open=row.open,
                    high=row.high,
                    low=row.low,
                    close=row.close,
                    volume=row.volume,
                )
        finally:
            session.close()

    def get_candles_df(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> pd.DataFrame:
        session: Session = self._session_factory()
        try:
            query = session.query(
                CandlestickORM.timestamp,
                CandlestickORM.open,
                CandlestickORM.high,
                CandlestickORM.low,
                CandlestickORM.close,
                CandlestickORM.volume,
            ).filter(
                CandlestickORM.product_id == product_id,
                CandlestickORM.timeframe == timeframe,
                CandlestickORM.timestamp >= start,
                CandlestickORM.timestamp <= end,
            ).order_by(CandlestickORM.timestamp.asc())

            data = [
                {
                    "timestamp": row.timestamp,
                    "open": float(row.open),
                    "high": float(row.high),
                    "low": float(row.low),
                    "close": float(row.close),
                    "volume": float(row.volume),
                }
                for row in query.all()
            ]

            df = pd.DataFrame(data)
            if not df.empty:
                df.set_index("timestamp", inplace=True)
            return df
        finally:
            session.close()

    def get_available_range(
        self, product_id: str, timeframe: str
    ) -> Optional[tuple[int, int]]:
        session: Session = self._session_factory()
        try:
            min_ts = session.query(func.min(CandlestickORM.timestamp)).filter(
                CandlestickORM.product_id == product_id,
                CandlestickORM.timeframe == timeframe,
            ).scalar()
            max_ts = session.query(func.max(CandlestickORM.timestamp)).filter(
                CandlestickORM.product_id == product_id,
                CandlestickORM.timeframe == timeframe,
            ).scalar()

            if min_ts is None or max_ts is None:
                return None
            return (min_ts, max_ts)
        finally:
            session.close()

    def validate(self) -> bool:
        try:
            session: Session = self._session_factory()
            session.execute(func.now())
            session.close()
            return True
        except Exception:
            return False
