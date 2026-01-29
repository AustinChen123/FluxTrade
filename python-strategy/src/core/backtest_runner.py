import time
import json
from typing import Generator, List, Optional
from decimal import Decimal
from sqlalchemy.orm import Session
from src.core.db import SessionLocal
from src.core.orm_models import Candlestick as CandlestickORM, Strategy as StrategyORM, BacktestResultSummary, BacktestTradeLog
from src.core.models import Candlestick
from src.core.engine import StrategyEngine
from src.core.clock import BacktestClock
from src.strategies.base import BaseStrategy
from src.core.repositories import BacktestOrderRepository
from src.core.backtest.loader import get_candles_generator
from src.core.analytics import calculate_metrics
from src.core.interfaces.data_source import IDataSource

from src.core.mocks.account_service import BacktestAccountService

class BacktestRunner:
    def __init__(
        self,
        start_time: int,
        end_time: int,
        product_id: str,
        timeframe: str,
        initial_balance: float = 10000.0,
        max_drawdown_limit: float = 0.20,
        data_source: Optional[IDataSource] = None,
    ):
        self.start_time = start_time
        self.end_time = end_time
        self.product_id = product_id
        self.timeframe = timeframe
        self.initial_balance = initial_balance
        self.max_drawdown_limit = max_drawdown_limit
        self.data_source = data_source

        # Data session only needed when no external data_source
        self.data_session = None if data_source else SessionLocal()
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

        # 2. Setup Engine with Backtest Repo and Mock Account
        repo = BacktestOrderRepository(self.db_session, summary.id, initial_balance=Decimal(str(self.initial_balance)))
        mock_account = BacktestAccountService(repo=repo, initial_balance=Decimal(str(self.initial_balance)))
        
        self.engine = StrategyEngine(
            self.db_session, 
            self.clock, 
            order_repository=repo, 
            account_service=mock_account,
            execution_mock_only=True
        )
        
        # Inject the SAME RiskManager (with Mock Account) into the strategies
        for strat in self._strategies_buffer:
            if hasattr(strat, 'risk_manager'):
                strat.risk_manager.account_service = mock_account
            self.engine.add_strategy(strat)

        print(f"🚀 Starting Backtest for {self.product_id} [{self.start_time} - {self.end_time}]")
        count = 0
        
        if self.data_source:
            candle_gen = self.data_source.get_candles(
                self.product_id,
                self.timeframe,
                self.start_time,
                self.end_time,
            )
        else:
            candle_gen = get_candles_generator(
                self.data_session,
                self.product_id,
                self.timeframe,
                self.start_time,
                self.end_time,
            )
        
        stop_threshold = Decimal(str(self.initial_balance)) * Decimal(str(1 - self.max_drawdown_limit))

        try:
            for candle in candle_gen:
                # 3. Update Clock
                self.clock.set_time(candle.timestamp / 1000)
                
                # 4. Process Candle
                self.engine.on_market_data(candle)
                
                # 5. Check Circuit Breaker
                current_balance = mock_account.get_balance()
                if current_balance < stop_threshold:
                    print(f"🛑 STOPPING BACKTEST: Max Drawdown Reached! Balance: {current_balance} < {stop_threshold}")
                    break

                count += 1
                if count % 1000 == 0:
                    print(f"Processed {count} candles... Current Time: {candle.timestamp} | Bal: {current_balance:.2f}")
        finally:
            # Calculate Final PnL
            final_balance = mock_account.get_balance()
            total_pnl = final_balance - Decimal(str(self.initial_balance))
            
            summary.total_pnl = total_pnl
            
            # Metrics
            trades = self.db_session.query(BacktestTradeLog).filter_by(session_id=summary.id).all()
            metrics = calculate_metrics(trades)
            summary.metrics_json = json.dumps(metrics, default=str)
            
            self.db_session.commit()
            
            print(f"✅ Backtest Complete. Processed {count} candles. Final PnL: {total_pnl}")
            print(f"📊 Metrics: {metrics}")
            
            self.db_session.close()
            if self.data_session:
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
