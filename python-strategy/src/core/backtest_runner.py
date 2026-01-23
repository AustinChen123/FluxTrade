import time
from typing import Generator
from sqlalchemy.orm import Session
from src.core.db import SessionLocal
from src.core.orm_models import Candlestick as CandlestickORM
from src.core.models import Candlestick
from src.core.engine import StrategyEngine
from src.core.clock import BacktestClock
from src.strategies.base import BaseStrategy

class BacktestRunner:
    def __init__(self, start_time: int, end_time: int, product_id: str, timeframe: str):
        self.start_time = start_time
        self.end_time = end_time
        self.product_id = product_id
        self.timeframe = timeframe
        self.db_session = SessionLocal()
        self.clock = BacktestClock(start_time=start_time / 1000) # Clock uses seconds
        self.engine = StrategyEngine(self.db_session, self.clock)

    def add_strategy(self, strategy: BaseStrategy):
        self.engine.add_strategy(strategy)

    def fetch_candles(self) -> Generator[Candlestick, None, None]:
        """Generator that yields Pydantic Candlestick objects"""
        query = self.db_session.query(CandlestickORM).filter(
            CandlestickORM.product_id == self.product_id,
            CandlestickORM.timeframe == self.timeframe,
            CandlestickORM.timestamp >= self.start_time,
            CandlestickORM.timestamp <= self.end_time
        ).order_by(CandlestickORM.timestamp.asc())

        # Yield one by one to simulate stream
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

    def run(self):
        print(f"🚀 Starting Backtest for {self.product_id} [{self.start_time} - {self.end_time}]")
        count = 0
        for candle in self.fetch_candles():
            # 1. Update Clock
            self.clock.set_time(candle.timestamp / 1000)
            
            # 2. Process Candle
            self.engine.on_market_data(candle)
            
            # 3. Process Pending Orders (Placeholder)
            # In a full system, we would check open limit orders here against the candle.
            
            count += 1
            if count % 1000 == 0:
                print(f"Processed {count} candles... Current Time: {candle.timestamp}")

        print(f"✅ Backtest Complete. Processed {count} candles.")
        self.db_session.close()

if __name__ == "__main__":
    # Example usage
    # Note: Ensure DB has data before running
    runner = BacktestRunner(
        start_time=1704067200000, # 2024-01-01
        end_time=1704153600000,   # 2024-01-02
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m"
    )
    # Import a strategy to test
    from src.strategies.example import RandomStrategy
    strategy = RandomStrategy("backtest_strat", "BINANCE:BTCUSDT-PERP")
    runner.add_strategy(strategy)
    runner.run()
