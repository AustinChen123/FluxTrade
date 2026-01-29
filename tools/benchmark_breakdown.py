#!/usr/bin/env python3
"""
Micro-benchmark: isolate where time is actually spent.

Breaks down:
  1. Object construction (Python → Rust FFI overhead)
  2. Pure engine processing (matching only)
  3. Pure NumPy vectorized SMA crossover (no framework overhead)
  4. vectorbt full pipeline
  5. backtesting.py full pipeline

This reveals whether the Rust engine is truly faster than vectorized
approaches, or if the Python loop is the bottleneck.
"""

import sys
import time
import random
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "python-strategy" / "src"))

# Reuse data generation from main benchmark
sys.path.insert(0, str(Path(__file__).parent))
from benchmark_matching_engine import (
    generate_candles,
    generate_orders,
    PythonMatchingEngine,
    PyCandle,
    PyOrder,
)


def benchmark_breakdown(num_candles: int = 100_000):
    print(f"\n{'=' * 80}")
    print(f"BREAKDOWN: {num_candles:,} candles")
    print(f"{'=' * 80}")

    candles = generate_candles(num_candles)
    orders = generate_orders(candles)
    print(f"Orders from SMA(10/30): {len(orders):,}")

    # ---------------------------------------------------------------
    # 1. Pure NumPy vectorized (no framework, raw numpy)
    # ---------------------------------------------------------------
    import numpy as np

    closes_np = np.array([c["close"] for c in candles])

    t0 = time.perf_counter()
    for _ in range(3):
        # SMA calculation
        fast_ma = np.convolve(closes_np, np.ones(10) / 10, mode='valid')
        slow_ma = np.convolve(closes_np, np.ones(30) / 30, mode='valid')
        # Align arrays
        offset = 30 - 10
        fast_aligned = fast_ma[offset:]
        slow_aligned = slow_ma
        min_len = min(len(fast_aligned), len(slow_aligned))
        fast_aligned = fast_aligned[:min_len]
        slow_aligned = slow_aligned[:min_len]
        # Crossover detection
        bullish = fast_aligned > slow_aligned
        entries = bullish[1:] & ~bullish[:-1]
        exits = ~bullish[1:] & bullish[:-1]
        n_signals = int(entries.sum() + exits.sum())
    numpy_time = (time.perf_counter() - t0) / 3

    print(f"\n--- Pure NumPy (SMA + crossover detection only) ---")
    print(f"  Time:         {numpy_time * 1000:.2f} ms")
    print(f"  Candles/sec:  {num_candles / numpy_time:,.0f}")
    print(f"  Signals:      {n_signals}")

    # ---------------------------------------------------------------
    # 2. vectorbt full pipeline
    # ---------------------------------------------------------------
    try:
        import vectorbt as vbt
        import pandas as pd

        df = pd.DataFrame(candles)
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df.set_index('timestamp', inplace=True)

        t0 = time.perf_counter()
        for _ in range(3):
            fast_ma_vbt = df['close'].rolling(10).mean()
            slow_ma_vbt = df['close'].rolling(30).mean()
            entries_vbt = (fast_ma_vbt > slow_ma_vbt) & (fast_ma_vbt.shift(1) <= slow_ma_vbt.shift(1))
            exits_vbt = (fast_ma_vbt < slow_ma_vbt) & (fast_ma_vbt.shift(1) >= slow_ma_vbt.shift(1))
            pf = vbt.Portfolio.from_signals(
                df['close'], entries_vbt, exits_vbt,
                init_cash=100000.0, fees=0.0,
            )
            _ = pf.total_return()
        vbt_time = (time.perf_counter() - t0) / 3

        print(f"\n--- vectorbt (SMA + portfolio simulation) ---")
        print(f"  Time:         {vbt_time * 1000:.2f} ms")
        print(f"  Candles/sec:  {num_candles / vbt_time:,.0f}")
    except ImportError:
        vbt_time = None
        print(f"\n--- vectorbt: not installed ---")

    # ---------------------------------------------------------------
    # 3. backtesting.py full pipeline
    # ---------------------------------------------------------------
    try:
        from backtesting import Backtest, Strategy
        from backtesting.lib import crossover
        import pandas as pd
        import warnings
        warnings.filterwarnings("ignore")

        df_bt = pd.DataFrame(candles)
        df_bt['timestamp'] = pd.to_datetime(df_bt['timestamp'], unit='ms')
        df_bt.set_index('timestamp', inplace=True)
        df_bt.columns = ['product_id', 'Open', 'High', 'Low', 'Close', 'Volume']
        df_bt = df_bt[['Open', 'High', 'Low', 'Close', 'Volume']]

        class SmaCross(Strategy):
            def init(self):
                self.sma1 = self.I(lambda x: pd.Series(x).rolling(10).mean(), self.data.Close)
                self.sma2 = self.I(lambda x: pd.Series(x).rolling(30).mean(), self.data.Close)
            def next(self):
                if crossover(self.sma1, self.sma2):
                    if not self.position:
                        self.buy()
                elif crossover(self.sma2, self.sma1):
                    if self.position:
                        self.position.close()

        t0 = time.perf_counter()
        for _ in range(3):
            bt = Backtest(df_bt, SmaCross, cash=100000, commission=0.0,
                         trade_on_close=True, exclusive_orders=True)
            stats = bt.run()
        btpy_time = (time.perf_counter() - t0) / 3
        btpy_trades = stats['# Trades']

        print(f"\n--- backtesting.py (SMA + event-driven simulation) ---")
        print(f"  Time:         {btpy_time * 1000:.2f} ms")
        print(f"  Candles/sec:  {num_candles / btpy_time:,.0f}")
        print(f"  Trades:       {btpy_trades}")
    except ImportError:
        btpy_time = None
        print(f"\n--- backtesting.py: not installed ---")

    # ---------------------------------------------------------------
    # 4. Rust engine — with breakdown
    # ---------------------------------------------------------------
    try:
        import fluxtrade_core

        # Phase A: Pre-construct all Rust objects
        t0 = time.perf_counter()
        rust_candles = []
        for c in candles:
            rust_candles.append(fluxtrade_core.Candlestick(
                product_id=c["product_id"], timeframe="1m",
                timestamp=c["timestamp"],
                open=c["open"], high=c["high"],
                low=c["low"], close=c["close"],
                volume=c["volume"],
            ))
        rust_orders_by_idx = {}
        for o in orders:
            idx = o["candle_idx"]
            rust_order = fluxtrade_core.Order(
                id=o["id"], product_id=o["product_id"],
                side=o["side"], order_type=o["order_type"],
                price=o["price"], quantity=o["quantity"],
                timestamp=candles[idx]["timestamp"],
            )
            rust_orders_by_idx.setdefault(idx, []).append(rust_order)
        construct_time = time.perf_counter() - t0

        # Phase B: Pure engine processing (objects already built)
        times_engine = []
        for _ in range(3):
            engine = fluxtrade_core.PyMatchingEngine(100000.0)
            fills = 0
            t0 = time.perf_counter()
            for i, rc in enumerate(rust_candles):
                for ro in rust_orders_by_idx.get(i, []):
                    engine.submit_order(ro)
                fill_events = engine.on_candle(rc)
                fills += len(fill_events)
            times_engine.append(time.perf_counter() - t0)
        engine_time = sum(times_engine) / len(times_engine)

        # Phase C: Total (construct + engine)
        total_rust = construct_time + engine_time

        print(f"\n--- Rust fluxtrade_core (breakdown) ---")
        print(f"  Object construction:  {construct_time * 1000:.2f} ms")
        print(f"  Engine processing:    {engine_time * 1000:.2f} ms")
        print(f"  Total:                {total_rust * 1000:.2f} ms")
        print(f"  Candles/sec (engine): {num_candles / engine_time:,.0f}")
        print(f"  Candles/sec (total):  {num_candles / total_rust:,.0f}")
        print(f"  Fills:                {fills}")
    except ImportError:
        engine_time = None
        total_rust = None
        print(f"\n--- Rust engine: not available ---")

    # ---------------------------------------------------------------
    # 5. Pure Python engine — with breakdown
    # ---------------------------------------------------------------
    # Phase A: Pre-construct objects
    t0 = time.perf_counter()
    py_candles = [
        PyCandle(
            product_id=c["product_id"], timestamp=c["timestamp"],
            open=c["open"], high=c["high"], low=c["low"],
            close=c["close"], volume=c["volume"],
        )
        for c in candles
    ]
    py_orders_by_idx = {}
    for o in orders:
        idx = o["candle_idx"]
        py_order = PyOrder(
            id=o["id"], product_id=o["product_id"],
            side=o["side"], order_type=o["order_type"],
            price=o["price"], quantity=o["quantity"],
        )
        py_orders_by_idx.setdefault(idx, []).append(py_order)
    py_construct_time = time.perf_counter() - t0

    # Phase B: Engine processing
    times_py = []
    for _ in range(3):
        engine = PythonMatchingEngine(100000.0)
        fills = 0
        t0 = time.perf_counter()
        for i, pc in enumerate(py_candles):
            for po in py_orders_by_idx.get(i, []):
                engine.submit_order(po)
            fill_events = engine.on_candle(pc)
            fills += len(fill_events)
        times_py.append(time.perf_counter() - t0)
    py_engine_time = sum(times_py) / len(times_py)
    total_py = py_construct_time + py_engine_time

    print(f"\n--- Pure Python (breakdown) ---")
    print(f"  Object construction:  {py_construct_time * 1000:.2f} ms")
    print(f"  Engine processing:    {py_engine_time * 1000:.2f} ms")
    print(f"  Total:                {total_py * 1000:.2f} ms")
    print(f"  Candles/sec (engine): {num_candles / py_engine_time:,.0f}")
    print(f"  Candles/sec (total):  {num_candles / total_py:,.0f}")
    print(f"  Fills:                {fills}")

    # ---------------------------------------------------------------
    # Summary comparison
    # ---------------------------------------------------------------
    print(f"\n{'=' * 80}")
    print("COMPARISON SUMMARY")
    print(f"{'=' * 80}")
    print(f"{'Approach':<35} {'Time (ms)':>10} {'Candles/sec':>15} {'What it does'}")
    print("-" * 80)
    print(f"{'Pure NumPy (SMA only)':<35} {numpy_time*1000:>10.2f} {num_candles/numpy_time:>15,.0f} SMA + crossover detect")
    if vbt_time:
        print(f"{'vectorbt (full pipeline)':<35} {vbt_time*1000:>10.2f} {num_candles/vbt_time:>15,.0f} SMA + portfolio sim")
    if engine_time:
        print(f"{'Rust engine (engine only)':<35} {engine_time*1000:>10.2f} {num_candles/engine_time:>15,.0f} order match + position")
        print(f"{'Rust engine (+ obj construct)':<35} {total_rust*1000:>10.2f} {num_candles/total_rust:>15,.0f} ^ + FFI object creation")
    print(f"{'Python engine (engine only)':<35} {py_engine_time*1000:>10.2f} {num_candles/py_engine_time:>15,.0f} order match + position")
    print(f"{'Python engine (+ obj construct)':<35} {total_py*1000:>10.2f} {num_candles/total_py:>15,.0f} ^ + dataclass creation")
    if btpy_time:
        print(f"{'backtesting.py (full pipeline)':<35} {btpy_time*1000:>10.2f} {num_candles/btpy_time:>15,.0f} SMA + event-driven sim")
    print(f"{'=' * 80}")

    if engine_time and vbt_time:
        print(f"\nKey ratios:")
        print(f"  Rust engine vs Python engine:  {py_engine_time/engine_time:.2f}x")
        print(f"  Rust engine vs NumPy:          {'slower' if engine_time > numpy_time else 'faster'} ({engine_time/numpy_time:.1f}x vs {numpy_time/engine_time:.1f}x)")
        print(f"  Rust engine vs vectorbt:       {'slower' if engine_time > vbt_time else 'faster'} ({engine_time/vbt_time:.1f}x vs {vbt_time/engine_time:.1f}x)")
        print(f"  Rust FFI overhead:             {construct_time / total_rust * 100:.0f}% of total Rust time")
        print(f"  Python loop overhead:          {(py_engine_time - engine_time) / py_engine_time * 100:.0f}% slower than Rust (engine-only)")


if __name__ == "__main__":
    for n in [100_000, 500_000]:
        benchmark_breakdown(n)
