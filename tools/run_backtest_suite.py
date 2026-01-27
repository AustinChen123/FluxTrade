import time
from datetime import datetime, timedelta, timezone
from src.core.backtest_runner import BacktestRunner
from src.strategies.rsi_scalper import RSIScalperStrategy
from src.strategies.bb_reversion import BBReversionStrategy
from src.strategies.macd_momentum import MACDMomentumStrategy
from src.core.db import SessionLocal
from src.core.orm_models import Candlestick
from sqlalchemy import func

import os

def get_data_range(product_id):
    session = SessionLocal()
    try:
        min_ts = session.query(func.min(Candlestick.timestamp)).filter(Candlestick.product_id == product_id).scalar()
        max_ts = session.query(func.max(Candlestick.timestamp)).filter(Candlestick.product_id == product_id).scalar()
        return min_ts, max_ts
    finally:
        session.close()

def run_suite():
    # Force Mock Execution
    if "EXCHANGE_API_KEY" in os.environ:
        del os.environ["EXCHANGE_API_KEY"]
    if "EXCHANGE_SECRET" in os.environ:
        del os.environ["EXCHANGE_SECRET"]
        
    product_id = "BINANCE:BTCUSDT-PERP"
    timeframe = "1m"
    
    start_ts, end_ts = get_data_range(product_id)
    if not start_ts or not end_ts:
        print("❌ No data found in DB. Run fetch_real_data.py first.")
        return

    print(f"📊 Running Backtest Suite on {product_id}")
    print(f"📅 Data Range: {datetime.fromtimestamp(start_ts/1000, tz=timezone.utc)} - {datetime.fromtimestamp(end_ts/1000, tz=timezone.utc)}")
    print("-" * 50)

    strategies = [
        (RSIScalperStrategy, "RSI_Scalper_v1"),
        (BBReversionStrategy, "BB_Reversion_v1"),
        (MACDMomentumStrategy, "MACD_Momentum_v1")
    ]

    for StratClass, name in strategies:
        print(f"\n🚀 Testing Strategy: {name}")
        
        # Instantiate Strategy
        strategy = StratClass(name, product_id)
        
        # Instantiate Runner
        # Note: BacktestRunner typically handles ONE engine run. 
        # We create a new runner for each to ensure clean state.
        runner = BacktestRunner(
            start_time=start_ts,
            end_time=end_ts,
            product_id=product_id,
            timeframe=timeframe
        )
        
        runner.add_strategy(strategy)
        runner.run()
        
        # Ideally we would query BacktestResultSummary here to print stats
        # but the runner prints logs.
        
    print("\n✅ Suite Complete.")

if __name__ == "__main__":
    run_suite()
