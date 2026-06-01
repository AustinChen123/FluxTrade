from __future__ import annotations

import argparse
import sys
import time
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from src.core.backtest_runner import BacktestRunner  # noqa: E402
from src.core.data_sources.csv_source import CsvDataSource  # noqa: E402
from src.core.orm_models import (  # noqa: E402
    BacktestResultSummary,
    BacktestTradeLog,
    Exchange,
    Product,
    SignalAudit,
    Strategy,
)
from src.strategies.golden_cross import GoldenCrossStrategy  # noqa: E402


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a GoldenCrossStrategy backtest against a BTC OHLCV CSV.",
    )
    parser.add_argument("--csv", default="data/smc/BTCUSDT_5m.csv")
    parser.add_argument("--product-id", default="BINANCE:BTCUSDT-PERP")
    parser.add_argument("--timeframe", default="5m")
    parser.add_argument("--short-window", type=int, default=20)
    parser.add_argument("--long-window", type=int, default=80)
    parser.add_argument("--quantity", default="0.01")
    parser.add_argument("--initial-balance", type=float, default=10_000.0)
    parser.add_argument("--maker-fee", type=float, default=0.0002)
    parser.add_argument("--taker-fee", type=float, default=0.0006)
    parser.add_argument(
        "--output-dir",
        default="backtest_output/golden_cross_btc",
        help="Directory for report.md, trades.csv, equity_curve.csv, and journal.jsonl.",
    )
    parser.add_argument(
        "--db-path",
        default="/private/tmp/fluxtrade_golden_cross_demo.db",
        help="SQLite database path used by the demo run.",
    )
    return parser


def _create_session_factory(db_path: str | Path, product_id: str):
    db_path = Path(db_path)
    if db_path.exists():
        db_path.unlink()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    engine = create_engine(
        f"sqlite:///{db_path}",
        connect_args={"check_same_thread": False, "timeout": 30},
    )
    for table in [
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        SignalAudit.__table__,
        BacktestResultSummary.__table__,
        BacktestTradeLog.__table__,
    ]:
        table.create(engine, checkfirst=True)

    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add(Exchange(id="BINANCE", name="Binance"))
        session.add(
            Product(
                id=product_id,
                exchange_id="BINANCE",
                base_asset="BTC",
                quote_asset="USDT",
            )
        )
        session.commit()
    return session_factory


def main() -> None:
    args = _build_parser().parse_args()
    started = time.perf_counter()

    data_source = CsvDataSource(
        args.csv,
        product_id=args.product_id,
        timeframe=args.timeframe,
    )
    available_range = data_source.get_available_range(args.product_id, args.timeframe)
    if available_range is None:
        raise RuntimeError(f"No candles found in {args.csv}")
    start_time, end_time = available_range
    candle_count = sum(
        1
        for _ in data_source.get_candles(
            args.product_id,
            args.timeframe,
            start_time,
            end_time,
        )
    )

    session_factory = _create_session_factory(args.db_path, args.product_id)
    runner = BacktestRunner(
        start_time=start_time,
        end_time=end_time,
        product_id=args.product_id,
        timeframe=args.timeframe,
        initial_balance=args.initial_balance,
        data_source=data_source,
        fee_config={"maker": args.maker_fee, "taker": args.taker_fee},
        report_config={
            "csv_trades": True,
            "markdown_report": True,
            "equity_curve": True,
            "journal_export": True,
            "output_dir": args.output_dir,
        },
        db_session_factory=session_factory,
    )
    runner.add_strategy(
        GoldenCrossStrategy(
            "golden_cross_btc_demo",
            args.product_id,
            short_window=args.short_window,
            long_window=args.long_window,
            timeframe=args.timeframe,
            quantity=Decimal(args.quantity),
        )
    )

    result = runner.run()
    elapsed = time.perf_counter() - started

    print("GoldenCrossStrategy BTC backtest")
    print(f"  csv={args.csv}")
    print(f"  candles={candle_count}")
    print(f"  timeframe={args.timeframe}")
    print(f"  short_window={args.short_window}")
    print(f"  long_window={args.long_window}")
    print(f"  total_pnl={result['total_pnl']}")
    print(f"  total_trades={result['total_trades']}")
    print(f"  win_rate={result['win_rate']}")
    print(f"  profit_factor={result['profit_factor']}")
    print(f"  max_drawdown={result['max_drawdown']}")
    print(f"  journal_count={result['journal_count']}")
    print(f"  elapsed_seconds={elapsed:.3f}")
    print(f"  candles_per_second={candle_count / elapsed:.0f}")
    print(f"  report_dir={result['report_dir']}")


if __name__ == "__main__":
    main()
