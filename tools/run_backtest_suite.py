"""
Run backtest suite across available strategies.

Usage:
    python tools/run_backtest_suite.py
    python tools/run_backtest_suite.py --strategy golden_cross
    python tools/run_backtest_suite.py --product BINANCE:ETHUSDT-PERP --timeframe 1m
"""

import argparse
import importlib
import inspect
import os
import sys
from datetime import datetime, timezone

from src.strategies.base import BaseStrategy


def parse_args():
    parser = argparse.ArgumentParser(description="Run backtest suite")
    parser.add_argument("--strategy", default=None,
                        help="Run specific strategy module name (e.g., golden_cross)")
    parser.add_argument("--product", default="BINANCE:BTCUSDT-PERP",
                        help="Product ID (default: BINANCE:BTCUSDT-PERP)")
    parser.add_argument("--timeframe", default="1m",
                        help="Timeframe (default: 1m)")
    return parser.parse_args()


def discover_strategies(filter_name: str | None = None) -> list[tuple[type, str]]:
    """Auto-discover strategy classes from src/strategies/."""
    strategies_dir = os.path.join("src", "strategies")
    results = []

    for filename in sorted(os.listdir(strategies_dir)):
        if not filename.endswith(".py"):
            continue
        if filename in ("__init__.py", "base.py"):
            continue

        module_name = filename[:-3]  # strip .py

        if filter_name and module_name != filter_name:
            continue

        try:
            module = importlib.import_module(f"src.strategies.{module_name}")
        except ImportError as e:
            print(f"  Skip {module_name}: import error ({e})")
            continue

        for name, obj in inspect.getmembers(module, inspect.isclass):
            if issubclass(obj, BaseStrategy) and obj is not BaseStrategy:
                results.append((obj, f"{module_name}::{name}"))

    return results


def get_data_range(product_id: str):
    """Get available data range from DB."""
    from sqlalchemy import func
    from src.core.db import SessionLocal
    from src.core.orm_models import Candlestick

    session = SessionLocal()
    try:
        min_ts = session.query(func.min(Candlestick.timestamp)).filter(
            Candlestick.product_id == product_id
        ).scalar()
        max_ts = session.query(func.max(Candlestick.timestamp)).filter(
            Candlestick.product_id == product_id
        ).scalar()
        return min_ts, max_ts
    finally:
        session.close()


def run_suite(args):
    # Force simulated mode
    os.environ.pop("EXCHANGE_API_KEY", None)
    os.environ.pop("EXCHANGE_SECRET", None)

    product_id = args.product

    # Check DB connectivity
    try:
        start_ts, end_ts = get_data_range(product_id)
    except Exception as e:
        print(f"Cannot connect to database: {e}")
        print("Ensure PostgreSQL is running and .env is configured.")
        sys.exit(1)

    if not start_ts or not end_ts:
        print(f"No data found for {product_id}. Run fetch_real_data.py first.")
        sys.exit(1)

    start_dt = datetime.fromtimestamp(start_ts / 1000, tz=timezone.utc)
    end_dt = datetime.fromtimestamp(end_ts / 1000, tz=timezone.utc)

    print(f"Backtest Suite: {product_id}")
    print(f"Data range: {start_dt} -> {end_dt}")
    print("-" * 60)

    # Discover strategies
    strategies = discover_strategies(args.strategy)

    if not strategies:
        if args.strategy:
            print(f"Strategy '{args.strategy}' not found.")
        else:
            print("No strategies found in src/strategies/")
        sys.exit(1)

    print(f"Found {len(strategies)} strategy(ies):\n")

    from src.core.backtest_runner import BacktestRunner

    results = []
    for strat_class, strat_label in strategies:
        print(f"  Running: {strat_label}")

        try:
            strategy = strat_class(strat_label, product_id)
            runner = BacktestRunner(
                start_time=start_ts,
                end_time=end_ts,
                product_id=product_id,
                timeframe=args.timeframe,
            )
            runner.add_strategy(strategy)
            runner.run()
            results.append((strat_label, "OK"))
            print(f"  -> OK\n")
        except Exception as e:
            results.append((strat_label, f"FAIL: {e}"))
            print(f"  -> FAIL: {e}\n")

    # Summary
    print("=" * 60)
    print("Summary:")
    for label, status in results:
        mark = "OK" if status == "OK" else "FAIL"
        print(f"  [{mark}] {label}")

    failed = sum(1 for _, s in results if s != "OK")
    if failed:
        print(f"\n{failed}/{len(results)} strategies failed.")
        sys.exit(1)
    else:
        print(f"\nAll {len(results)} strategies passed.")


if __name__ == "__main__":
    args = parse_args()
    run_suite(args)
