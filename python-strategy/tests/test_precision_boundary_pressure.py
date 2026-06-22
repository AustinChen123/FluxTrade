"""Diagnostics for Decimal/string pressure at the PyO3 boundary.

These tests are intentionally bounded. The memory-pressure diagnostic is opt-in
and runs in a subprocess so a bad experiment cannot leak into the pytest worker.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import textwrap
from decimal import Decimal

import pytest

from src.core.precision import PrecisionCodec, PrecisionSpec

try:
    from fluxtrade_core import Candlestick as RustCandlestick

    HAS_RUST = True
except ImportError:
    HAS_RUST = False

try:
    from fluxtrade_core import ScaledCandlestick as RustScaledCandlestick  # noqa: F401

    HAS_SCALED_RUST = True
except ImportError:
    HAS_SCALED_RUST = False


PRODUCT_ID = "BINANCE:BTCUSDT-PERP"
TIMEFRAME = "5m"


def test_precision_codec_quantizes_payload_that_current_rust_decimal_boundary_changes():
    """A boundary codec can safely reduce over-precise input to product ticks.

    This does not make the current Rust/string path faster. It proves why the
    codec belongs before the PyO3 boundary: pathological precision can be
    handled explicitly instead of leaking into Rust Decimal parsing. The current
    Rust Decimal boundary accepts this payload, but it cannot preserve arbitrary
    Python Decimal precision.
    """
    over_precise_price = "104523." + ("1234567890" * 8)
    codec = PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.01"),
            quantity_step=Decimal("0.001"),
        )
    )

    units = codec.encode_price(over_precise_price)

    assert codec.decode_price(units) == Decimal("104523.12")
    if HAS_RUST:
        candle = RustCandlestick(
            PRODUCT_ID,
            TIMEFRAME,
            1_700_000_000_000,
            over_precise_price,
            "104524.00",
            "104522.00",
            "104523.50",
            "12.5",
        )
        assert Decimal(candle.open) != Decimal(over_precise_price)


@pytest.mark.memory
@pytest.mark.rust
@pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled")
@pytest.mark.skipif(
    os.getenv("FLUXTRADE_BOUNDARY_MEMORY_TEST") != "1",
    reason="set FLUXTRADE_BOUNDARY_MEMORY_TEST=1 to run bounded memory diagnostic",
)
def test_current_rust_decimal_string_boundary_memory_pressure_is_bounded():
    """Measure current string/Decimal boundary pressure in an isolated child.

    This is a diagnostic baseline for the experiment gates, not a proof that the
    scaled-int path is better. A future scaled binding must beat this baseline on
    the same machine and workload before it can be promoted.
    """
    child_code = textwrap.dedent(
        """
        import gc
        import json
        import tracemalloc

        from fluxtrade_core import Candlestick

        product_id = "BINANCE:BTCUSDT-PERP"
        timeframe = "5m"
        rounds = 5
        count = 2_000
        price = "104523.123456789012345678"

        tracemalloc.start()
        after_gc_values = []
        peak_values = []
        for round_index in range(rounds):
            candles = [
                Candlestick(
                    product_id,
                    timeframe,
                    1_700_000_000_000 + ((round_index * count) + index) * 300_000,
                    price,
                    "104524.123456789012345678",
                    "104522.123456789012345678",
                    "104523.623456789012345678",
                    "12.500000000000000001",
                )
                for index in range(count)
            ]
            current, peak = tracemalloc.get_traced_memory()
            peak_values.append(peak)
            del candles
            gc.collect()
            current_after_gc, _ = tracemalloc.get_traced_memory()
            after_gc_values.append(current_after_gc)
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        print(json.dumps({
            "rounds": rounds,
            "count": count,
            "current": current,
            "peak": peak,
            "after_gc_values": after_gc_values,
            "max_after_gc": max(after_gc_values),
            "peak_values": peak_values,
        }))
        """
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=os.getcwd(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    metrics = json.loads(result.stdout)
    print(json.dumps(metrics, sort_keys=True))

    assert metrics["rounds"] == 5
    assert metrics["count"] == 2_000
    assert metrics["peak"] > 100_000
    assert metrics["max_after_gc"] < metrics["peak"] * 0.05
    assert metrics["after_gc_values"][-1] < metrics["peak"] * 0.05


@pytest.mark.memory
@pytest.mark.rust
@pytest.mark.skipif(not HAS_SCALED_RUST, reason="ScaledCandlestick extension not compiled")
@pytest.mark.skipif(
    os.getenv("FLUXTRADE_BOUNDARY_MEMORY_TEST") != "1",
    reason="set FLUXTRADE_BOUNDARY_MEMORY_TEST=1 to run bounded memory diagnostic",
)
def test_scaled_candle_boundary_memory_pressure_against_string_boundary():
    """Compare current string candles with scaled-int candles in one child process."""
    child_code = textwrap.dedent(
        """
        import gc
        import json
        import tracemalloc

        from fluxtrade_core import Candlestick, ScaledCandlestick

        product_id = "BINANCE:BTCUSDT-PERP"
        timeframe = "5m"
        count = 5_000
        price = "104523.123456789012345678"

        def measure(factory):
            gc.collect()
            tracemalloc.start()
            candles = [factory(index) for index in range(count)]
            _, peak = tracemalloc.get_traced_memory()
            del candles
            gc.collect()
            current_after_gc, _ = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            return {
                "peak": peak,
                "current_after_gc": current_after_gc,
            }

        string_metrics = measure(lambda index: Candlestick(
            product_id,
            timeframe,
            1_700_000_000_000 + index * 300_000,
            price,
            "104524.123456789012345678",
            "104522.123456789012345678",
            "104523.623456789012345678",
            "12.500000000000000001",
        ))
        scaled_metrics = measure(lambda index: ScaledCandlestick(
            product_id,
            timeframe,
            1_700_000_000_000 + index * 300_000,
            10_452_312,
            10_452_412,
            10_452_212,
            10_452_362,
            12_500_000,
        ))

        print(json.dumps({
            "count": count,
            "string": string_metrics,
            "scaled": scaled_metrics,
        }))
        """
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=os.getcwd(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    metrics = json.loads(result.stdout)
    print(json.dumps(metrics, sort_keys=True))

    assert metrics["count"] == 5_000
    assert metrics["scaled"]["peak"] < metrics["string"]["peak"]


@pytest.mark.performance
@pytest.mark.rust
@pytest.mark.skipif(not HAS_SCALED_RUST, reason="ScaledCandlestick extension not compiled")
@pytest.mark.skipif(
    os.getenv("FLUXTRADE_BOUNDARY_BENCHMARK") != "1",
    reason="set FLUXTRADE_BOUNDARY_BENCHMARK=1 to run boundary speed diagnostic",
)
def test_scaled_candle_boundary_construction_speed_against_string_boundary():
    """Compare object construction speed for string/Decimal vs scaled-int candles."""
    child_code = textwrap.dedent(
        """
        import gc
        import json
        import time

        from fluxtrade_core import Candlestick, ScaledCandlestick

        product_id = "BINANCE:BTCUSDT-PERP"
        timeframe = "5m"
        count = 50_000
        price = "104523.123456789012345678"

        def bench(factory):
            gc.collect()
            start = time.perf_counter()
            candles = [factory(index) for index in range(count)]
            elapsed = time.perf_counter() - start
            del candles
            gc.collect()
            return elapsed

        # Warm both code paths before timing to reduce first-use noise.
        for index in range(100):
            Candlestick(
                product_id,
                timeframe,
                1_700_000_000_000 + index * 300_000,
                price,
                "104524.123456789012345678",
                "104522.123456789012345678",
                "104523.623456789012345678",
                "12.500000000000000001",
            )
            ScaledCandlestick(
                product_id,
                timeframe,
                1_700_000_000_000 + index * 300_000,
                10_452_312,
                10_452_412,
                10_452_212,
                10_452_362,
                12_500_000,
            )

        string_seconds = bench(lambda index: Candlestick(
            product_id,
            timeframe,
            1_700_000_000_000 + index * 300_000,
            price,
            "104524.123456789012345678",
            "104522.123456789012345678",
            "104523.623456789012345678",
            "12.500000000000000001",
        ))
        scaled_seconds = bench(lambda index: ScaledCandlestick(
            product_id,
            timeframe,
            1_700_000_000_000 + index * 300_000,
            10_452_312,
            10_452_412,
            10_452_212,
            10_452_362,
            12_500_000,
        ))

        print(json.dumps({
            "count": count,
            "string_seconds": string_seconds,
            "scaled_seconds": scaled_seconds,
            "string_per_second": count / string_seconds,
            "scaled_per_second": count / scaled_seconds,
            "speedup": string_seconds / scaled_seconds,
        }))
        """
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=os.getcwd(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=20,
    )
    metrics = json.loads(result.stdout)
    print(json.dumps(metrics, sort_keys=True))

    assert metrics["count"] == 50_000
    assert metrics["scaled_seconds"] < metrics["string_seconds"]


@pytest.mark.performance
@pytest.mark.rust
@pytest.mark.skipif(not HAS_SCALED_RUST, reason="ScaledCandlestick extension not compiled")
@pytest.mark.skipif(
    os.getenv("FLUXTRADE_ADAPTER_BENCHMARK") != "1",
    reason="set FLUXTRADE_ADAPTER_BENCHMARK=1 to run adapter speed diagnostic",
)
def test_scaled_adapter_market_data_speed_against_decimal_boundary():
    """Compare SimulatedAdapter market-data hot path with and without scaled candles."""
    child_code = textwrap.dedent(
        """
        import gc
        import json
        import time
        from decimal import Decimal

        from src.core.adapters.simulated import SimulatedAdapter
        from src.core.models import Candlestick
        from src.core.precision import PrecisionCodec, PrecisionSpec

        product_id = "BINANCE:BTCUSDT-PERP"
        timeframe = "5m"
        count = 50_000
        rounds = 5
        candles = [
            Candlestick(
                product_id=product_id,
                timeframe=timeframe,
                timestamp=1_700_000_000_000 + index * 300_000,
                open=Decimal("104523.12"),
                high=Decimal("104524.12"),
                low=Decimal("104522.12"),
                close=Decimal("104523.62"),
                volume=Decimal("12.500"),
            )
            for index in range(count)
        ]
        codec = PrecisionCodec(
            PrecisionSpec(
                price_tick=Decimal("0.01"),
                quantity_step=Decimal("0.001"),
            )
        )

        def bench(adapter):
            start = time.perf_counter()
            for candle in candles:
                adapter.on_market_data(candle)
            return time.perf_counter() - start

        decimal_times = []
        scaled_times = []
        for _ in range(rounds):
            gc.collect()
            decimal_times.append(bench(SimulatedAdapter(Decimal("10000"))))
            gc.collect()
            scaled_times.append(bench(SimulatedAdapter(Decimal("10000"), precision_codec=codec)))

        decimal_avg = sum(decimal_times) / len(decimal_times)
        scaled_avg = sum(scaled_times) / len(scaled_times)
        print(json.dumps({
            "count": count,
            "rounds": rounds,
            "decimal_times": decimal_times,
            "scaled_times": scaled_times,
            "decimal_avg": decimal_avg,
            "scaled_avg": scaled_avg,
            "decimal_per_second": count / decimal_avg,
            "scaled_per_second": count / scaled_avg,
            "speedup": decimal_avg / scaled_avg,
        }))
        """
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=os.getcwd(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    metrics = json.loads(result.stdout)
    print(json.dumps(metrics, sort_keys=True))

    assert metrics["count"] == 50_000
    assert metrics["rounds"] == 5
    # This diagnostic intentionally does not assert scaled is faster. Inline
    # Decimal->units encoding can be slower than the current string boundary;
    # the promoted path should pre-encode units during data preparation.
    assert metrics["decimal_avg"] > 0
    assert metrics["scaled_avg"] > 0


@pytest.mark.performance
@pytest.mark.rust
@pytest.mark.skipif(not HAS_SCALED_RUST, reason="ScaledCandlestick extension not compiled")
@pytest.mark.skipif(
    os.getenv("FLUXTRADE_PREENCODED_BOUNDARY_BENCHMARK") != "1",
    reason="set FLUXTRADE_PREENCODED_BOUNDARY_BENCHMARK=1 to run pre-encoded speed diagnostic",
)
def test_preencoded_scaled_engine_speed_against_decimal_boundary():
    """Compare engine calls when scaled units are prepared before the hot loop."""
    child_code = textwrap.dedent(
        """
        import gc
        import json
        import time

        from fluxtrade_core import Candlestick, PyMatchingEngine, ScaledCandlestick

        product_id = "BINANCE:BTCUSDT-PERP"
        timeframe = "5m"
        count = 50_000
        rounds = 5
        raw_candles = [
            (
                1_700_000_000_000 + index * 300_000,
                "104523.12",
                "104524.12",
                "104522.12",
                "104523.62",
                "12.500",
            )
            for index in range(count)
        ]
        scaled_candles = [
            ScaledCandlestick(
                product_id,
                timeframe,
                1_700_000_000_000 + index * 300_000,
                10_452_312,
                10_452_412,
                10_452_212,
                10_452_362,
                12_500,
            )
            for index in range(count)
        ]

        def bench_decimal():
            engine = PyMatchingEngine("10000")
            start = time.perf_counter()
            for timestamp, open_, high, low, close, volume in raw_candles:
                candle = Candlestick(
                    product_id,
                    timeframe,
                    timestamp,
                    open_,
                    high,
                    low,
                    close,
                    volume,
                )
                engine.on_candle(candle)
            return time.perf_counter() - start

        def bench_scaled():
            engine = PyMatchingEngine("10000")
            engine.set_scaled_precision("0.01", "0.001")
            start = time.perf_counter()
            for candle in scaled_candles:
                engine.on_scaled_candle(candle)
            return time.perf_counter() - start

        decimal_times = []
        scaled_times = []
        for _ in range(rounds):
            gc.collect()
            decimal_times.append(bench_decimal())
            gc.collect()
            scaled_times.append(bench_scaled())

        decimal_avg = sum(decimal_times) / len(decimal_times)
        scaled_avg = sum(scaled_times) / len(scaled_times)
        print(json.dumps({
            "count": count,
            "rounds": rounds,
            "decimal_times": decimal_times,
            "scaled_times": scaled_times,
            "decimal_avg": decimal_avg,
            "scaled_avg": scaled_avg,
            "decimal_per_second": count / decimal_avg,
            "scaled_per_second": count / scaled_avg,
            "speedup": decimal_avg / scaled_avg,
        }))
        """
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = "src"
    result = subprocess.run(
        [sys.executable, "-c", child_code],
        cwd=os.getcwd(),
        env=env,
        check=True,
        capture_output=True,
        text=True,
        timeout=30,
    )
    metrics = json.loads(result.stdout)
    print(json.dumps(metrics, sort_keys=True))

    assert metrics["count"] == 50_000
    assert metrics["rounds"] == 5
    assert metrics["scaled_avg"] < metrics["decimal_avg"]
