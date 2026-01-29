import numpy as np
import pandas as pd
from decimal import Decimal
from src.core.db import SessionLocal
from src.core.orm_models import Candlestick
from datetime import datetime, timedelta
from sqlalchemy import text

def generate_structured_data(days=365):
    print(f"Generating {days} days of STRUCTURED synthetic data...")
    
    # Timeframe 15m
    n_candles = days * 96
    
    # 1. Create a Trend Component (Sine Wave + Linear Trend)
    t = np.linspace(0, 4 * np.pi, n_candles) # 2 cycles per year? No, let's do more cycles.
    # Let's say 1 trend cycle every 10 days = 960 candles
    cycles = days / 10 
    t = np.linspace(0, cycles * 2 * np.pi, n_candles)
    
    # Trend: Long term Up
    trend = np.linspace(30000, 60000, n_candles)
    
    # Waves: Amplitude 2000
    waves = np.sin(t) * 2000
    
    # Fractal Noise (Random Walk)
    noise = np.cumsum(np.random.normal(0, 50, n_candles))
    
    price_path = trend + waves + noise
    
    session = SessionLocal()
    
    # Clear existing data? No, let's append or overwrite.
    # Ideally we should clear to avoid mix.
    # TRUNCATE table
    session.execute(text("TRUNCATE TABLE candlestick CASCADE"))
    session.commit()
    print("🗑️ Cleared existing data.")
    
    start_time = datetime.now() - timedelta(days=days)
    base_ts = int(start_time.timestamp() * 1000)
    
    batch = []
    
    for i in range(n_candles):
        close = price_path[i]
        
        # Volatility for High/Low
        vol = np.random.uniform(0.001, 0.005) * close
        
        if i > 0:
            prev_close = price_path[i-1]
            open_ = prev_close # No gaps usually
        else:
            open_ = close * 0.99
            
        high = max(open_, close) + vol
        low = min(open_, close) - vol
        
        # Ensure High/Low envelop Open/Close
        high = max(high, open_, close)
        low = min(low, open_, close)
        
        volume = np.random.uniform(100, 1000) + (abs(close-open_) * 10) # Vol matches volatility
        
        timestamp = base_ts + (i * 15 * 60 * 1000)
        
        c = Candlestick(
            product_id="BINANCE:BTCUSDT-PERP",
            timeframe="15m",
            timestamp=timestamp,
            open=Decimal(f"{open_:.2f}"),
            high=Decimal(f"{high:.2f}"),
            low=Decimal(f"{low:.2f}"),
            close=Decimal(f"{close:.2f}"),
            volume=Decimal(f"{volume:.2f}")
        )
        batch.append(c)
        
        if len(batch) >= 10000:
            session.bulk_save_objects(batch)
            session.commit()
            batch = []
            print(f"Generated {i}/{n_candles} candles...")
            
    if batch:
        session.bulk_save_objects(batch)
        session.commit()
        
    session.close()
    print("✅ Structured Synthetic Data Generation Complete.")

if __name__ == "__main__":
    generate_structured_data(days=60) # 2 months
