import time
from typing import Generator, List
from sqlalchemy.orm import Session
from src.core.db import SessionLocal
from src.core.orm_models import Candlestick as CandlestickORM, Strategy as StrategyORM, BacktestResultSummary
from src.core.models import Candlestick
from src.core.engine import StrategyEngine
from src.core.clock import BacktestClock
from src.strategies.base import BaseStrategy
from src.core.repositories import BacktestOrderRepository
from src.core.backtest.loader import get_candles_generator

class BacktestRunner:
    def __init__(self, start_time: int, end_time: int, product_id: str, timeframe: str):
        self.start_time = start_time
        self.end_time = end_time
        self.product_id = product_id
        self.timeframe = timeframe
        
        # Dual sessions: one for data stream, one for execution writing
        self.data_session = SessionLocal()
        self.db_session = SessionLocal()
        
        self.clock = BacktestClock(start_time=start_time / 1000) # Clock uses seconds
        self._strategies_buffer: List[BaseStrategy] = []
        self.engine = None

    def add_strategy(self, strategy: BaseStrategy):
        self._strategies_buffer.append(strategy)

    def _ensure_strategies_registered(self):
        """Register all added strategies in the DB to avoid FK constraints"""
        for strat in self._strategies_buffer:
            exists = self.db_session.query(StrategyORM).filter_by(id=strat.strategy_id).first()
            if not exists:
                print(f"Registering missing strategy in DB: {strat.strategy_id}")
                new_strat = StrategyORM(
                    id=strat.strategy_id,
                    name=f"Backtest: {strat.strategy_id}",
                    configuration_json="{}"
                )
                self.db_session.add(new_strat)
        self.db_session.commit()

    def run(self):
        # 0. Registration Check
        self._ensure_strategies_registered()

        if not self._strategies_buffer:
            print("⚠️ No strategies added. Exiting.")
            return

        # 1. Setup Backtest Session (Isolation)
        primary_strategy_id = self._strategies_buffer[0].strategy_id
        summary = BacktestResultSummary(
            strategy_id=primary_strategy_id,
            start_time=self.start_time,
            end_time=self.end_time,
            total_pnl=0,
            metrics_json="{}"
        )
        self.db_session.add(summary)
        self.db_session.commit()
        print(f"📝 Backtest Session Created: ID {summary.id}")

        # 2. Setup Engine with Backtest Repo (Repository Pattern)
        repo = BacktestOrderRepository(self.db_session, summary.id)
        self.engine = StrategyEngine(self.db_session, self.clock, order_repository=repo)
        
        for strat in self._strategies_buffer:
            self.engine.add_strategy(strat)

        print(f"🚀 Starting Backtest for {self.product_id} [{self.start_time} - {self.end_time}]")
        count = 0
        
        # Use Junior's loader if available, otherwise fallback logic could go here
        # But we assume Junior did his job.
        candle_gen = get_candles_generator(
            self.data_session, 
            self.product_id, 
            self.timeframe, 
            self.start_time, 
            self.end_time
        )

        try:
            for candle in candle_gen:
                # 3. Update Clock
                self.clock.set_time(candle.timestamp / 1000)
                
                # 4. Process Candle
                self.engine.on_market_data(candle)
                
                count += 1
                if count % 1000 == 0:
                    print(f"Processed {count} candles... Current Time: {candle.timestamp}")
        finally:
            print(f"✅ Backtest Complete. Processed {count} candles.")
            self.db_session.close()
            self.data_session.close()

if __name__ == "__main__":
    # Example usage
    runner = BacktestRunner(
        start_time=1704067200000, 
        end_time=1704153600000,
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m"
    )
    from src.strategies.golden_cross import GoldenCrossStrategy
    strategy = GoldenCrossStrategy("backtest_strat", "BINANCE:BTCUSDT-PERP")
    runner.add_strategy(strategy)
    runner.run()
