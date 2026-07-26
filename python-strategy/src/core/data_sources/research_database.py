from __future__ import annotations

from typing import Generator, Optional

import pandas as pd
from sqlalchemy import func
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.core.db import SessionLocal
from src.core.interfaces.data_source import IDataSource
from src.core.models import Candlestick
from src.core.orm_models import ResearchCandlestick, ResearchDataset


def _empty_candle_frame() -> pd.DataFrame:
    return pd.DataFrame(
        columns=["open", "high", "low", "close", "volume"],
        index=pd.Index([], name="timestamp"),
    )


class ResearchDatabaseDataSource(IDataSource):
    """Read candles from one immutable, versioned research dataset."""

    def __init__(self, dataset_id: str, session_factory=None):
        if not dataset_id.strip():
            raise ValueError("dataset_id must be non-empty")
        self._dataset_id = dataset_id
        self._session_factory = session_factory or SessionLocal

    def get_candles(
        self, product_id: str, timeframe: str, start: int, end: int
    ) -> Generator[Candlestick, None, None]:
        session: Session = self._session_factory()
        try:
            dataset = session.get(ResearchDataset, self._dataset_id)
            if (
                dataset is None
                or dataset.product_id != product_id
                or dataset.timeframe != timeframe
            ):
                return
            query = (
                session.query(ResearchCandlestick)
                .filter(
                    ResearchCandlestick.dataset_id == self._dataset_id,
                    ResearchCandlestick.timestamp >= start,
                    ResearchCandlestick.timestamp <= end,
                )
                .order_by(ResearchCandlestick.timestamp.asc())
            )
            for row in query.yield_per(1_000):
                yield Candlestick(
                    product_id=dataset.product_id,
                    timeframe=dataset.timeframe,
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
            dataset = session.get(ResearchDataset, self._dataset_id)
            if (
                dataset is None
                or dataset.product_id != product_id
                or dataset.timeframe != timeframe
            ):
                return _empty_candle_frame()
            rows = (
                session.query(
                    ResearchCandlestick.timestamp,
                    ResearchCandlestick.open,
                    ResearchCandlestick.high,
                    ResearchCandlestick.low,
                    ResearchCandlestick.close,
                    ResearchCandlestick.volume,
                )
                .filter(
                    ResearchCandlestick.dataset_id == self._dataset_id,
                    ResearchCandlestick.timestamp >= start,
                    ResearchCandlestick.timestamp <= end,
                )
                .order_by(ResearchCandlestick.timestamp.asc())
                .all()
            )
            frame = pd.DataFrame(
                [
                    {
                        "timestamp": row.timestamp,
                        "open": row.open,
                        "high": row.high,
                        "low": row.low,
                        "close": row.close,
                        "volume": row.volume,
                    }
                    for row in rows
                ]
            )
            if frame.empty:
                return _empty_candle_frame()
            frame.set_index("timestamp", inplace=True)
            return frame
        finally:
            session.close()

    def get_available_range(
        self, product_id: str, timeframe: str
    ) -> Optional[tuple[int, int]]:
        session: Session = self._session_factory()
        try:
            dataset = session.get(ResearchDataset, self._dataset_id)
            if (
                dataset is None
                or dataset.product_id != product_id
                or dataset.timeframe != timeframe
            ):
                return None
            return (dataset.start_time, dataset.end_time)
        finally:
            session.close()

    def validate(self) -> bool:
        session: Session | None = None
        try:
            session = self._session_factory()
            dataset = session.get(ResearchDataset, self._dataset_id)
            if dataset is None or dataset.quality_status != "validated":
                return False
            actual_count = (
                session.query(func.count(ResearchCandlestick.timestamp))
                .filter(ResearchCandlestick.dataset_id == self._dataset_id)
                .scalar()
            )
            return actual_count == dataset.row_count
        except SQLAlchemyError:
            return False
        finally:
            if session is not None:
                session.close()
