#!/usr/bin/env python3
"""
Benchmark: FluxTrade Rust Matching Engine vs Python Backtesting Frameworks

Compares performance of:
1. fluxtrade_core (Rust/PyO3) - Our matching engine
2. Pure Python implementation - Baseline
3. vectorbt (optional) - Popular vectorized backtester
4. backtesting.py (optional) - Event-driven backtester

Usage:
    cd python-strategy
    uv run python ../tools/benchmark_matching_engine.py

    # With optional frameworks:
    uv add vectorbt backtesting
    uv run python ../tools/benchmark_matching_engine.py
"""

import sys
import time
import random
from dataclasses import dataclass
from typing import Optional
from pathlib import Path

# Add src to path for fluxtrade_core
sys.path.insert(0, str(Path(__file__).parent.parent / "python-strategy" / "src"))

# ============================================================================
# Pure Python Matching Engine (Baseline)
# ============================================================================

@dataclass
class PyCandle:
    product_id: str
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float

@dataclass
class PyOrder:
    id: str
    product_id: str
    side: str  # "LONG" or "SHORT"
    order_type: str  # "MARKET" or "LIMIT"
    price: float
    quantity: float
    status: str = "PENDING"

@dataclass
class PyFill:
    order_id: str
    product_id: str
    price: float
    quantity: float
    fee: float
    timestamp: int

@dataclass
class PyPosition:
    product_id: str
    side: str
    quantity: float
    entry_price: float
    unrealized_pnl: float = 0.0


class PythonMatchingEngine:
    """Pure Python matching engine for benchmark comparison."""

    def __init__(self, initial_balance: float):
        self.balance = initial_balance
        self.positions: dict[str, PyPosition] = {}
        self.open_orders: list[PyOrder] = []

    def submit_order(self, order: PyOrder) -> str:
        self.open_orders.append(order)
        return order.id

    def on_candle(self, candle: PyCandle) -> list[PyFill]:
        fills = []
        remaining = []

        # Separate market and limit orders
        market_orders = [o for o in self.open_orders if o.order_type == "MARKET"]
        limit_orders = [o for o in self.open_orders if o.order_type == "LIMIT"]

        # Process market orders
        for order in market_orders:
            if order.product_id == candle.product_id:
                fill = PyFill(
                    order_id=order.id,
                    product_id=order.product_id,
                    price=candle.open,
                    quantity=order.quantity,
                    fee=0.0,
                    timestamp=candle.timestamp
                )
                self._update_position(order, candle.open)
                fills.append(fill)
            else:
                remaining.append(order)

        # Process limit orders
        for order in limit_orders:
            matched = False
            fill_price = 0.0

            if order.product_id == candle.product_id:
                if order.side == "LONG" and candle.low <= order.price:
                    matched = True
                    fill_price = order.price
                elif order.side == "SHORT" and candle.high >= order.price:
                    matched = True
                    fill_price = order.price

            if matched:
                fill = PyFill(
                    order_id=order.id,
                    product_id=order.product_id,
                    price=fill_price,
                    quantity=order.quantity,
                    fee=0.0,
                    timestamp=candle.timestamp
                )
                self._update_position(order, fill_price)
                fills.append(fill)
            else:
                remaining.append(order)

        self.open_orders = remaining
        return fills

    def _update_position(self, order: PyOrder, fill_price: float):
        if order.product_id not in self.positions:
            self.positions[order.product_id] = PyPosition(
                product_id=order.product_id,
                side="FLAT",
                quantity=0.0,
                entry_price=0.0
            )

        pos = self.positions[order.product_id]

        if pos.quantity == 0.0 or pos.side == "FLAT":
            pos.side = order.side
            pos.quantity = order.quantity
            pos.entry_price = fill_price
        elif pos.side == order.side:
            total_cost = pos.quantity * pos.entry_price + order.quantity * fill_price
            new_qty = pos.quantity + order.quantity
            pos.entry_price = total_cost / new_qty
            pos.quantity = new_qty
        else:
            close_qty = min(order.quantity, pos.quantity)
            price_diff = (fill_price - pos.entry_price) if pos.side == "LONG" else (pos.entry_price - fill_price)
            realized_pnl = price_diff * close_qty
            self.balance += realized_pnl

            remaining_qty = pos.quantity - close_qty
            excess_qty = order.quantity - close_qty

            if remaining_qty > 1e-9:
                pos.quantity = remaining_qty
            elif excess_qty > 1e-9:
                pos.side = order.side
                pos.quantity = excess_qty
                pos.entry_price = fill_price
            else:
                pos.side = "FLAT"
                pos.quantity = 0.0
                pos.entry_price = 0.0


# ============================================================================
# Benchmark Data Generation
# ============================================================================

def generate_candles(n: int, product_id: str = "BTCUSDT", seed: int = 42) -> list[dict]:
    """Generate synthetic OHLCV candles with fixed seed for reproducibility.

    Uses Ornstein-Uhlenbeck mean-reversion so price stays in a
    realistic range regardless of candle count.
    """
    rng = random.Random(seed)
    candles = []
    mean_price = 50000.0
    price = mean_price
    reversion = 0.001  # mean-reversion strength
    volatility = 100.0
    timestamp = 1704067200000  # 2024-01-01 00:00:00 UTC

    for i in range(n):
        # Ornstein-Uhlenbeck: pull toward mean + random noise
        change = reversion * (mean_price - price) + rng.gauss(0, volatility)
        open_price = price
        close_price = price + change
        high = max(open_price, close_price) + abs(rng.gauss(0, 50))
        low = min(open_price, close_price) - abs(rng.gauss(0, 50))

        candles.append({
            "product_id": product_id,
            "timestamp": timestamp + i * 60000,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close_price,
            "volume": rng.uniform(100, 1000)
        })

        price = close_price

    return candles


def generate_orders(
    candles: list[dict],
    fast_window: int = 10,
    slow_window: int = 30,
    quantity: float = 0.1,
) -> list[dict]:
    """Generate orders from SMA crossover signals.

    Uses the same SMA(fast/slow) crossover logic as the vectorbt and
    backtesting.py benchmarks so that all engines trade the same
    strategy and results are comparable.
    """
    orders = []
    order_id = 0

    if len(candles) < slow_window + 1:
        return orders

    closes = [c["close"] for c in candles]
    in_position = False

    for i in range(slow_window, len(candles)):
        fast_ma = sum(closes[i - fast_window + 1 : i + 1]) / fast_window
        slow_ma = sum(closes[i - slow_window + 1 : i + 1]) / slow_window
        prev_fast = sum(closes[i - fast_window : i]) / fast_window
        prev_slow = sum(closes[i - slow_window : i]) / slow_window

        bullish_now = fast_ma > slow_ma
        bullish_prev = prev_fast > prev_slow

        if bullish_now and not bullish_prev and not in_position:
            # Golden cross → buy
            order_id += 1
            orders.append({
                "id": f"order_{order_id}",
                "candle_idx": i,
                "product_id": candles[i]["product_id"],
                "side": "LONG",
                "order_type": "MARKET",
                "price": candles[i]["close"],
                "quantity": quantity,
            })
            in_position = True
        elif not bullish_now and bullish_prev and in_position:
            # Death cross → sell / exit
            order_id += 1
            orders.append({
                "id": f"order_{order_id}",
                "candle_idx": i,
                "product_id": candles[i]["product_id"],
                "side": "SHORT",
                "order_type": "MARKET",
                "price": candles[i]["close"],
                "quantity": quantity,
            })
            in_position = False

    return orders


# ============================================================================
# Benchmark Functions
# ============================================================================

def benchmark_rust_engine(candles: list[dict], orders: list[dict], iterations: int = 3) -> dict:
    """Benchmark the Rust matching engine."""
    try:
        import fluxtrade_core
    except ImportError:
        return {"error": "fluxtrade_core not available"}

    times = []
    total_fills = 0

    for _ in range(iterations):
        engine = fluxtrade_core.PyMatchingEngine(100000.0)
        order_idx = 0
        fills = 0

        start = time.perf_counter()

        for i, c in enumerate(candles):
            # Submit orders for this candle
            while order_idx < len(orders) and orders[order_idx]["candle_idx"] == i:
                o = orders[order_idx]
                rust_order = fluxtrade_core.Order(
                    id=o["id"],
                    product_id=o["product_id"],
                    side=o["side"],
                    order_type=o["order_type"],
                    price=o["price"],
                    quantity=o["quantity"],
                    timestamp=candles[o["candle_idx"]]["timestamp"]
                )
                engine.submit_order(rust_order)
                order_idx += 1

            # Process candle
            rust_candle = fluxtrade_core.Candlestick(
                product_id=c["product_id"],
                timeframe="1m",
                timestamp=c["timestamp"],
                open=c["open"],
                high=c["high"],
                low=c["low"],
                close=c["close"],
                volume=c["volume"]
            )
            fill_events = engine.on_candle(rust_candle)
            fills += len(fill_events)

        elapsed = time.perf_counter() - start
        times.append(elapsed)
        total_fills = fills

    return {
        "name": "Rust (fluxtrade_core)",
        "mean_time": sum(times) / len(times),
        "min_time": min(times),
        "max_time": max(times),
        "fills": total_fills,
        "candles_per_sec": len(candles) / (sum(times) / len(times))
    }


def benchmark_python_engine(candles: list[dict], orders: list[dict], iterations: int = 3) -> dict:
    """Benchmark the pure Python matching engine."""
    times = []
    total_fills = 0

    for _ in range(iterations):
        engine = PythonMatchingEngine(100000.0)
        order_idx = 0
        fills = 0

        start = time.perf_counter()

        for i, c in enumerate(candles):
            # Submit orders for this candle
            while order_idx < len(orders) and orders[order_idx]["candle_idx"] == i:
                o = orders[order_idx]
                py_order = PyOrder(
                    id=o["id"],
                    product_id=o["product_id"],
                    side=o["side"],
                    order_type=o["order_type"],
                    price=o["price"],
                    quantity=o["quantity"]
                )
                engine.submit_order(py_order)
                order_idx += 1

            # Process candle
            py_candle = PyCandle(
                product_id=c["product_id"],
                timestamp=c["timestamp"],
                open=c["open"],
                high=c["high"],
                low=c["low"],
                close=c["close"],
                volume=c["volume"]
            )
            fill_events = engine.on_candle(py_candle)
            fills += len(fill_events)

        elapsed = time.perf_counter() - start
        times.append(elapsed)
        total_fills = fills

    return {
        "name": "Pure Python",
        "mean_time": sum(times) / len(times),
        "min_time": min(times),
        "max_time": max(times),
        "fills": total_fills,
        "candles_per_sec": len(candles) / (sum(times) / len(times))
    }


def benchmark_vectorbt(
    candles: list[dict],
    fast_window: int = 10,
    slow_window: int = 30,
    iterations: int = 3,
) -> Optional[dict]:
    """Benchmark vectorbt with SMA crossover (same windows as engine benchmarks)."""
    try:
        import vectorbt as vbt
        import pandas as pd
        import numpy as np
    except ImportError:
        return None

    df = pd.DataFrame(candles)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)

    times = []

    for _ in range(iterations):
        start = time.perf_counter()

        fast_ma = df['close'].rolling(fast_window).mean()
        slow_ma = df['close'].rolling(slow_window).mean()

        entries = (fast_ma > slow_ma) & (fast_ma.shift(1) <= slow_ma.shift(1))
        exits = (fast_ma < slow_ma) & (fast_ma.shift(1) >= slow_ma.shift(1))

        pf = vbt.Portfolio.from_signals(
            df['close'],
            entries,
            exits,
            init_cash=100000.0,
            fees=0.0,
        )

        _ = pf.total_return()

        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "name": "vectorbt",
        "mean_time": sum(times) / len(times),
        "min_time": min(times),
        "max_time": max(times),
        "fills": "N/A (vectorized)",
        "candles_per_sec": len(candles) / (sum(times) / len(times))
    }


def benchmark_backtestingpy(
    candles: list[dict],
    fast_window: int = 10,
    slow_window: int = 30,
    iterations: int = 3,
) -> Optional[dict]:
    """Benchmark backtesting.py with SMA crossover (same windows as engine benchmarks)."""
    try:
        from backtesting import Backtest, Strategy
        from backtesting.lib import crossover
        import pandas as pd
    except ImportError:
        return None

    df = pd.DataFrame(candles)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    df.set_index('timestamp', inplace=True)
    df.columns = ['product_id', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = df[['Open', 'High', 'Low', 'Close', 'Volume']]

    class SmaCross(Strategy):
        n1 = fast_window
        n2 = slow_window

        def init(self):
            self.sma1 = self.I(lambda x: pd.Series(x).rolling(self.n1).mean(), self.data.Close)
            self.sma2 = self.I(lambda x: pd.Series(x).rolling(self.n2).mean(), self.data.Close)

        def next(self):
            if crossover(self.sma1, self.sma2):
                if not self.position:
                    self.buy()
            elif crossover(self.sma2, self.sma1):
                if self.position:
                    self.position.close()

    times = []

    for _ in range(iterations):
        start = time.perf_counter()

        bt = Backtest(
            df, SmaCross, cash=100000, commission=0.0,
            trade_on_close=True, exclusive_orders=True,
        )
        stats = bt.run()

        elapsed = time.perf_counter() - start
        times.append(elapsed)

    return {
        "name": "backtesting.py",
        "mean_time": sum(times) / len(times),
        "min_time": min(times),
        "max_time": max(times),
        "fills": stats['# Trades'],
        "candles_per_sec": len(candles) / (sum(times) / len(times))
    }


# ============================================================================
# Main
# ============================================================================

def print_results(results: list[dict], num_candles: int, num_orders: int):
    """Print benchmark results in a formatted table."""
    print("\n" + "=" * 80)
    print(f"BENCHMARK RESULTS: {num_candles:,} candles, {num_orders:,} orders")
    print("=" * 80)
    print(f"{'Framework':<25} {'Mean Time':<12} {'Min':<10} {'Max':<10} {'Candles/sec':<15} {'Fills'}")
    print("-" * 80)

    for r in results:
        if "error" in r:
            print(f"{r.get('name', 'Unknown'):<25} ERROR: {r['error']}")
        else:
            fills = r['fills'] if isinstance(r['fills'], int) else r['fills']
            print(f"{r['name']:<25} {r['mean_time']:.4f}s      {r['min_time']:.4f}s    {r['max_time']:.4f}s    {r['candles_per_sec']:>12,.0f}   {fills}")

    print("=" * 80)

    # Calculate speedup vs Python
    python_result = next((r for r in results if r.get("name") == "Pure Python"), None)
    if python_result and "mean_time" in python_result:
        print("\nSpeedup vs Pure Python:")
        for r in results:
            if "mean_time" in r and r["name"] != "Pure Python":
                speedup = python_result["mean_time"] / r["mean_time"]
                print(f"  {r['name']}: {speedup:.2f}x faster")


def main():
    print("FluxTrade Matching Engine Benchmark")
    print("=" * 80)
    print("Strategy: SMA(10/30) crossover  |  Qty: 0.1  |  Fees: 0  |  Cash: 100K")
    print("=" * 80)

    configs = [
        10_000,
        100_000,
        500_000,
    ]

    for num_candles in configs:
        print(f"\nGenerating {num_candles:,} candles (seed=42)...")
        candles = generate_candles(num_candles)
        orders = generate_orders(candles)
        print(f"SMA crossover generated {len(orders):,} orders")

        results = []

        # Rust engine
        print("Benchmarking Rust engine...")
        results.append(benchmark_rust_engine(candles, orders))

        # Python engine
        print("Benchmarking Python engine...")
        results.append(benchmark_python_engine(candles, orders))

        # vectorbt (optional)
        print("Benchmarking vectorbt (if available)...")
        vbt_result = benchmark_vectorbt(candles)
        if vbt_result:
            results.append(vbt_result)
        else:
            print("  vectorbt not installed, skipping")

        # backtesting.py (optional)
        print("Benchmarking backtesting.py (if available)...")
        bt_result = benchmark_backtestingpy(candles)
        if bt_result:
            results.append(bt_result)
        else:
            print("  backtesting.py not installed, skipping")

        print_results(results, num_candles, len(orders))


if __name__ == "__main__":
    main()
