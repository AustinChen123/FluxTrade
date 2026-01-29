"""
Fetch real OHLCV data from exchange and store in PostgreSQL.

Usage:
    python tools/fetch_real_data.py
    python tools/fetch_real_data.py --product BINANCE:ETHUSDT-PERP --days 30 --timeframe 1m
    python tools/fetch_real_data.py --resume  # Continue from last stored candle
    python tools/fetch_real_data.py --no-truncate  # Append without clearing
"""

import argparse
import asyncio
import sys
from decimal import Decimal
from datetime import datetime, timedelta, timezone

import ccxt.async_support as ccxt
from sqlalchemy import text, func
from src.core.db import SessionLocal
from src.core.orm_models import Candlestick


# Product ID -> CCXT symbol mapping
PRODUCT_TO_CCXT = {
    "BINANCE:BTCUSDT-PERP": ("binance", "BTC/USDT:USDT"),
    "BINANCE:ETHUSDT-PERP": ("binance", "ETH/USDT:USDT"),
    "BYBIT:BTCUSDT-PERP": ("bybit", "BTC/USDT:USDT"),
    "BYBIT:ETHUSDT-PERP": ("bybit", "ETH/USDT:USDT"),
}


def parse_args():
    parser = argparse.ArgumentParser(description="Fetch real OHLCV data from exchange")
    parser.add_argument("--product", default="BINANCE:BTCUSDT-PERP",
                        help="Product ID (default: BINANCE:BTCUSDT-PERP)")
    parser.add_argument("--timeframe", default="15m",
                        help="Candlestick timeframe (default: 15m)")
    parser.add_argument("--days", type=int, default=90,
                        help="Days of history to fetch (default: 90)")
    parser.add_argument("--start", default=None,
                        help="Start date (YYYY-MM-DD), overrides --days")
    parser.add_argument("--end", default=None,
                        help="End date (YYYY-MM-DD), default: now")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last stored candle timestamp")
    parser.add_argument("--no-truncate", action="store_true",
                        help="Append data without clearing existing candles")
    return parser.parse_args()


def get_last_timestamp(session, product_id: str, timeframe: str) -> int | None:
    """Get the latest stored candle timestamp for resume."""
    result = session.query(func.max(Candlestick.timestamp)).filter(
        Candlestick.product_id == product_id,
        Candlestick.timeframe == timeframe,
    ).scalar()
    return result


def resolve_exchange(product_id: str):
    """Resolve product ID to CCXT exchange and symbol."""
    if product_id in PRODUCT_TO_CCXT:
        return PRODUCT_TO_CCXT[product_id]

    # Generic parsing: EXCHANGE:BASEQUOTE-PERP
    parts = product_id.split(":")
    if len(parts) != 2:
        raise ValueError(f"Invalid product_id format: {product_id}")

    exchange_name = parts[0].lower()
    symbol_part = parts[1].replace("-PERP", "")

    # Try to split into base/quote (assume USDT quote)
    if "USDT" in symbol_part:
        base = symbol_part.replace("USDT", "")
        return exchange_name, f"{base}/USDT:USDT"

    raise ValueError(f"Cannot resolve CCXT symbol for: {product_id}")


async def fetch_and_store(args):
    product_id = args.product
    timeframe = args.timeframe

    exchange_name, ccxt_symbol = resolve_exchange(product_id)

    # Calculate time range
    end_time = datetime.now(timezone.utc)
    if args.end:
        end_time = datetime.strptime(args.end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    if args.start:
        start_time = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        start_time = end_time - timedelta(days=args.days)

    print(f"Fetching {ccxt_symbol} ({timeframe}) from {exchange_name}")
    print(f"Range: {start_time.date()} -> {end_time.date()}")

    # Initialize exchange
    exchange_cls = getattr(ccxt, exchange_name)
    exchange = exchange_cls({
        'enableRateLimit': True,
        'options': {'defaultType': 'future'}
    })

    session = SessionLocal()

    try:
        # Resume mode: start from last stored timestamp
        if args.resume:
            last_ts = get_last_timestamp(session, product_id, timeframe)
            if last_ts:
                since = last_ts + 1
                print(f"Resuming from {datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)}")
            else:
                since = int(start_time.timestamp() * 1000)
                print("No existing data found, starting fresh")
        else:
            since = int(start_time.timestamp() * 1000)

            if not args.no_truncate:
                print("Clearing existing candlestick data...")
                session.execute(text(
                    "DELETE FROM candlestick WHERE product_id = :pid AND timeframe = :tf"
                ), {"pid": product_id, "tf": timeframe})
                session.commit()

        end_ms = int(end_time.timestamp() * 1000)
        total_candles = 0
        consecutive_empty = 0

        while since < end_ms:
            ohlcv = await exchange.fetch_ohlcv(ccxt_symbol, timeframe, since=since, limit=1000)

            if not ohlcv:
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    print("No more data after 3 empty responses")
                    break
                await asyncio.sleep(1)
                continue

            consecutive_empty = 0

            batch = []
            for ts, open_, high, low, close, volume in ohlcv:
                if ts >= end_ms:
                    break
                batch.append(Candlestick(
                    product_id=product_id,
                    timeframe=timeframe,
                    timestamp=ts,
                    open=Decimal(str(open_)),
                    high=Decimal(str(high)),
                    low=Decimal(str(low)),
                    close=Decimal(str(close)),
                    volume=Decimal(str(volume)),
                ))

            if batch:
                session.bulk_save_objects(batch)
                session.commit()

            count = len(batch)
            total_candles += count
            last_ts = ohlcv[-1][0]
            last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
            print(f"  +{count} candles (total: {total_candles}) | last: {last_dt}")

            since = last_ts + 1
            await asyncio.sleep(exchange.rateLimit / 1000)

        print(f"Done. Total candles stored: {total_candles}")

    except KeyboardInterrupt:
        print(f"\nInterrupted. Stored {total_candles} candles so far.")
        session.commit()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        session.close()
        await exchange.close()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(fetch_and_store(args))
