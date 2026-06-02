from __future__ import annotations

import argparse
import sys
import time
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from src.core.data_sources.csv_source import CsvDataSource  # noqa: E402
from src.core.golden_cross_fast_fitness import GoldenCrossFastFitnessEvaluator  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run numeric GoldenCross fast fitness evaluations against a BTC OHLCV CSV.",
    )
    parser.add_argument("--csv", default="data/smc/BTCUSDT_5m.csv")
    parser.add_argument("--product-id", default="BINANCE:BTCUSDT-PERP")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--initial-balance", default="10000")
    parser.add_argument("--taker-fee", default="0.0006")
    parser.add_argument("--quantity", default="0.01")
    parser.add_argument(
        "--candidates",
        default="20:80,10:70,11:72,12:74,13:76,14:78,15:80,16:82",
        help="Comma-separated short:long SMA windows.",
    )
    return parser


def _parse_candidates(value: str) -> list[tuple[int, int]]:
    candidates = []
    for item in value.split(","):
        short, long = item.split(":", 1)
        candidates.append((int(short), int(long)))
    return candidates


def main() -> None:
    args = _build_parser().parse_args()
    candidates = _parse_candidates(args.candidates)

    data_source = CsvDataSource(
        args.csv,
        product_id=args.product_id,
        timeframe=args.timeframe,
    )
    available_range = data_source.get_available_range(args.product_id, args.timeframe)
    if available_range is None:
        raise RuntimeError(f"No candles found in {args.csv}")
    start_time, end_time = available_range

    load_started = time.perf_counter()
    frame = data_source.get_candles_df(
        args.product_id,
        args.timeframe,
        start_time,
        end_time,
    )
    load_elapsed = time.perf_counter() - load_started

    evaluator = GoldenCrossFastFitnessEvaluator.from_dataframe(
        frame,
        initial_balance=Decimal(args.initial_balance),
        taker_fee=Decimal(args.taker_fee),
    )

    run_started = time.perf_counter()
    results = []
    for short_window, long_window in candidates:
        result = evaluator.evaluate(
            short_window=short_window,
            long_window=long_window,
            quantity=Decimal(args.quantity),
        )
        results.append((short_window, long_window, result))
    run_elapsed = time.perf_counter() - run_started

    best = max(results, key=lambda item: item[2].total_pnl)
    print("GoldenCross fast fitness")
    print(f"  csv={args.csv}")
    print(f"  candles={len(frame)}")
    print(f"  candidates={len(candidates)}")
    print(f"  load_seconds={load_elapsed:.6f}")
    print(f"  eval_seconds={run_elapsed:.6f}")
    print(f"  seconds_per_candidate={run_elapsed / len(candidates):.6f}")
    print(
        "  best="
        f"short:{best[0]} long:{best[1]} "
        f"pnl:{best[2].total_pnl} "
        f"trades:{best[2].total_trades} "
        f"max_drawdown:{best[2].max_drawdown}"
    )


if __name__ == "__main__":
    main()
