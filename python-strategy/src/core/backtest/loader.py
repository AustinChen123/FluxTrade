import pandas as pd
from typing import Generator
from src.core.db import SessionLocal
from src.core.orm_models import Candlestick as CandlestickORM
from src.core.models import Candlestick

from sqlalchemy.orm import Session

def get_candles_df(product_id: str, start: int, end: int, timeframe: str = "1m") -> pd.DataFrame:
    """
    Fetch candles as a Pandas DataFrame for Vectorized Backtesting.
    """
    session = SessionLocal()
    try:
        query = session.query(
            CandlestickORM.timestamp,
            CandlestickORM.open,
            CandlestickORM.high,
            CandlestickORM.low,
            CandlestickORM.close,
            CandlestickORM.volume
        ).filter(
            CandlestickORM.product_id == product_id,
            CandlestickORM.timeframe == timeframe,
            CandlestickORM.timestamp >= start,
            CandlestickORM.timestamp <= end
        ).order_by(CandlestickORM.timestamp.asc())

        # Use pandas read_sql logic or manual list construction
        # Manual construction is often safer with ORM sessions if we don't want to expose the engine directly
        data = [
            {
                "timestamp": row.timestamp,
                "open": float(row.open),
                "high": float(row.high),
                "low": float(row.low),
                "close": float(row.close),
                "volume": float(row.volume)
            }
            for row in query.all()
        ]
        
        df = pd.DataFrame(data)
        if not df.empty:
             df.set_index('timestamp', inplace=True)
        return df
    finally:
        session.close()

def get_candles_generator(session: Session, product_id: str, timeframe: str, start: int, end: int) -> Generator[Candlestick, None, None]:
    """
    Generator that yields Pydantic Candlestick objects for Event-Driven Backtesting.
    Uses an external session to support concurrent streaming.
    """
    query = session.query(CandlestickORM).filter(
        CandlestickORM.product_id == product_id,
        CandlestickORM.timeframe == timeframe,
        CandlestickORM.timestamp >= start,
        CandlestickORM.timestamp <= end
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
            volume=row.volume
        )
