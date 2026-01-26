import time
import datetime
from typing import Tuple
from sqlalchemy.orm import Session
from sqlalchemy import text

def timeframe_to_ms(timeframe: str) -> int:
    """Converts timeframe string (e.g. '1m', '1h') to milliseconds."""
    unit = timeframe[-1]
    try:
        value = int(timeframe[:-1])
    except ValueError:
        raise ValueError(f"Invalid timeframe format: {timeframe}")

    if unit == 'm':
        return value * 60 * 1000
    elif unit == 'h':
        return value * 60 * 60 * 1000
    elif unit == 'd':
        return value * 24 * 60 * 60 * 1000
    else:
        raise ValueError(f"Unknown timeframe unit: {unit}")

def check_data_availability(db: Session, product: str, timeframe: str, lookback: int) -> Tuple[bool, str]:
    """
    Checks if enough candlestick data exists in the database.
    Returns (True, "") if OK, or (False, backfill_command) if insufficient.
    """
    now_ms = int(time.time() * 1000)
    tf_ms = timeframe_to_ms(timeframe)
    required_start_time = now_ms - (lookback * tf_ms)
    
    query = text("""
        SELECT COUNT(*) FROM candlestick 
        WHERE product_id = :product_id 
        AND timeframe = :timeframe
        AND timestamp >= :start_time
    """)
    
    result = db.execute(query, {
        "product_id": product,
        "timeframe": timeframe,
        "start_time": required_start_time
    }).scalar()
    
    # 10% gap tolerance
    if result >= lookback * 0.9:
        return True, ""
    
    # Generate backfill command
    # product format: EXCHANGE:SYMBOL-PERP
    try:
        parts = product.split(':')
        exchange = parts[0]
        symbol = parts[1].replace("-PERP", "")
    except (IndexError, AttributeError):
        exchange = "unknown"
        symbol = product

    start_dt = datetime.datetime.fromtimestamp(required_start_time / 1000.0)
    end_dt = datetime.datetime.now()
    
    command = (
        f"docker exec fluxtrade-rust rust-data-service backfill "
        f"--exchange {exchange.lower()} --symbol {symbol} "
        f"--start {start_dt.strftime('%Y-%m-%d')} --end {end_dt.strftime('%Y-%m-%d')}"
    )
    
    return False, command
