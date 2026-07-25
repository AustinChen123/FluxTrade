"""Profile isolated stages of the real-data MNQ research replay pipeline."""

from __future__ import annotations

import argparse
import csv
import gc
import hashlib
import importlib
import os
import resource
import statistics
import sys
import time
import tracemalloc
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from src.core.adapters.simulated import SimulatedAdapter  # noqa: E402
from src.core.data_sources.csv_source import CsvDataSource  # noqa: E402
from src.core.data_sources.memory import MemoryDataSource  # noqa: E402
from src.core.precision import PrecisionCodec, PrecisionSpec  # noqa: E402
from src.core.product_registry import CapitalModel, FeeModel, InstrumentSpec  # noqa: E402
from src.core.research_backtest_runner import ResearchBacktestRunner  # noqa: E402
from src.strategies.representative_benchmark import (  # noqa: E402
    representative_strategy_factory,
)

CandleAggregator = getattr(importlib.import_module("fluxtrade_core"), "CandleAggregator")
RustCandlestick = getattr(importlib.import_module("fluxtrade_core"), "Candlestick")

_FIVE_MINUTES_MS = 5 * 60 * 1000
_PRODUCT_ID = "RITHMIC:MNQ_ROLL-PERP"
_STRATEGY_ID = "representative_profile"
_INITIAL_BALANCE = Decimal("100000")
_PARAMETERS = {
    "trend_window": 8,
    "breakout_window": 10,
    "atr_window": 21,
    "rsi_window": 21,
    "volume_window": 15,
    "swing_window": 3,
    "entry_score": 3,
    "hold_bars": 18,
    "max_atr_expansion": "1.5",
    "quantity": "1",
}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate real MNQ 1m data with the Rust candle aggregator, then "
            "profile one isolated replay stage. Results are printed only."
        )
    )
    parser.add_argument("--csv", required=True)
    parser.add_argument("--start", required=True, help="Inclusive ISO-8601 5m boundary")
    parser.add_argument(
        "--end",
        required=True,
        help="Inclusive ISO-8601 5m boundary used to close the preceding bucket",
    )
    parser.add_argument(
        "--mode",
        choices=(
            "bare",
            "minimal-context",
            "full-context",
            "representative",
            "parity",
            "ga-memory",
        ),
        required=True,
    )
    parser.add_argument("--repeats", type=int, default=5)
    return parser


def _timestamp_ms(value: str) -> int:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timestamps must include a timezone")
    return int(parsed.astimezone(UTC).timestamp() * 1000)


def _csv_timestamp_ms(value: str) -> int:
    if value.isdigit():
        numeric = int(value)
        return numeric * 1000 if numeric < 1_000_000_000_000 else numeric
    return _timestamp_ms(value)


def _aggregate_5m(
    source_path: Path,
    output_path: Path,
    *,
    start_time: int,
    end_time: int,
) -> tuple[int, int]:
    if start_time % _FIVE_MINUTES_MS or end_time % _FIVE_MINUTES_MS:
        raise ValueError("start and end must align to UTC 5m boundaries")
    if end_time <= start_time:
        raise ValueError("end must be after start")

    aggregator = CandleAggregator()
    source_count = 0
    output_count = 0
    with source_path.open(newline="") as source, output_path.open(
        "w",
        newline="",
    ) as output:
        reader = csv.DictReader(source)
        writer = csv.writer(output)
        writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        for row in reader:
            timestamp = _csv_timestamp_ms(row["timestamp"])
            if timestamp < start_time:
                continue
            if timestamp > end_time:
                break
            source_count += 1
            completed = aggregator.add_candle(
                RustCandlestick(
                    product_id=_PRODUCT_ID,
                    timeframe="1m",
                    timestamp=timestamp,
                    open=row["open"],
                    high=row["high"],
                    low=row["low"],
                    close=row["close"],
                    volume=row["volume"],
                ),
                "5m",
            )
            if completed is None:
                continue
            writer.writerow(
                [
                    completed.timestamp,
                    completed.open,
                    completed.high,
                    completed.low,
                    completed.close,
                    completed.volume,
                ]
            )
            output_count += 1
    if source_count == 0 or output_count == 0:
        raise ValueError("selected range produced no closed 5m candles")
    return source_count, output_count


def _instrument() -> InstrumentSpec:
    return InstrumentSpec(
        product_id=_PRODUCT_ID,
        exchange="RITHMIC",
        symbol="MNQ_ROLL",
        base="MNQ",
        quote="USD",
        quantity_step=Decimal("1"),
        price_tick=Decimal("0.25"),
        multiplier=Decimal("2"),
        fee_model=FeeModel.PER_CONTRACT,
        capital_model=CapitalModel.PER_CONTRACT,
        capital_per_contract=Decimal("1000"),
    )


def _codec() -> PrecisionCodec:
    return PrecisionCodec(
        PrecisionSpec(
            price_tick=Decimal("0.25"),
            quantity_step=Decimal("1"),
        )
    )


def _fresh_adapter(codec: PrecisionCodec, instrument: InstrumentSpec) -> SimulatedAdapter:
    return SimulatedAdapter(
        initial_balance=_INITIAL_BALANCE,
        precision_codec=codec,
        instrument_spec=instrument,
    )


def _run_bare(prepared, codec, instrument) -> dict:
    adapter = _fresh_adapter(codec, instrument)
    for prepared_candle in prepared:
        adapter.on_prepared_market_data(prepared_candle)
    return {}


def _run_minimal_context(candles, prepared, codec, instrument) -> dict:
    adapter = _fresh_adapter(codec, instrument)
    peak_equity = _INITIAL_BALANCE
    max_drawdown = Decimal("0")
    for candle, prepared_candle in zip(candles, prepared):
        adapter.on_prepared_market_data(prepared_candle)
        cash = adapter.get_balance()
        position = adapter.get_position(_PRODUCT_ID, strategy_id=_STRATEGY_ID)
        unrealized = Decimal("0")
        if position is not None:
            direction = Decimal("1") if position.side.value == "LONG" else Decimal("-1")
            unrealized = (
                (candle.close - position.entry_price)
                * position.quantity
                * instrument.multiplier
                * direction
            )
        equity = cash + unrealized
        peak_equity = max(peak_equity, equity)
        max_drawdown = max(max_drawdown, peak_equity - equity)
    return {"max_drawdown": max_drawdown}


def _run_full_context(candles, prepared, codec, instrument) -> dict:
    adapter = _fresh_adapter(codec, instrument)
    peak_equity = _INITIAL_BALANCE
    max_drawdown = Decimal("0")
    for candle, prepared_candle in zip(candles, prepared):
        fills = adapter.on_prepared_market_data(prepared_candle)
        context = adapter.get_strategy_context(
            strategy_id=_STRATEGY_ID,
            product_id=_PRODUCT_ID,
            timestamp=candle.timestamp,
            initial_balance=_INITIAL_BALANCE,
            mark_price=candle.close,
            peak_equity=peak_equity,
            max_drawdown=max_drawdown,
            latest_fills=fills,
        )
        peak_equity = max(peak_equity, context.total_equity)
        max_drawdown = max(max_drawdown, peak_equity - context.total_equity)
    return {"max_drawdown": max_drawdown}


def _run_representative(candles, prepared, codec, instrument) -> dict:
    runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=_PRODUCT_ID,
        timeframe="5m",
        initial_balance=int(_INITIAL_BALANCE),
        data_source=MemoryDataSource(candles),
        precision_codec=codec,
        prepared_scaled_candles=prepared,
        instrument_spec=instrument,
    )
    runner.add_strategy(
        representative_strategy_factory(
            _STRATEGY_ID,
            _PRODUCT_ID,
            "5m",
            _PARAMETERS,
        )
    )
    return runner.run()


def _trade_digest(trades) -> str:
    def decimal_text(value) -> str:
        normalized = Decimal(value).normalize()
        return "0" if normalized == 0 else format(normalized, "f")

    rows = []
    for trade in trades:
        side = getattr(trade.side, "value", trade.side)
        rows.append(
            (
                int(trade.timestamp),
                str(side).lower(),
                decimal_text(trade.price),
                decimal_text(trade.quantity),
                decimal_text(trade.fee),
            )
        )
    rows.sort()
    return hashlib.sha256(repr(rows).encode()).hexdigest()


def _closed_trade_digest(trades) -> str:
    rows = [
        (
            int(trade.entry_time),
            int(trade.exit_time),
            str(getattr(trade.side, "value", trade.side)),
            str(trade.entry_price.normalize()),
            str(trade.exit_price.normalize()),
            str(trade.quantity.normalize()),
            str(trade.pnl.normalize()),
            str(trade.fee.normalize()),
        )
        for trade in trades
    ]
    return hashlib.sha256(repr(rows).encode()).hexdigest()


def _net_quantity(trades) -> Decimal:
    quantity = Decimal("0")
    for trade in trades:
        side = getattr(trade.side, "value", trade.side)
        quantity += trade.quantity if str(side).lower() == "buy" else -trade.quantity
    return Decimal("0") if quantity == 0 else quantity


def _rss_bytes() -> int:
    rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(rss if sys.platform == "darwin" else rss * 1024)


def _run_ga_memory(
    aggregated_path: Path,
    *,
    start_time: int,
    end_time: int,
    codec: PrecisionCodec,
    database_path: Path,
) -> dict:
    from sqlalchemy import create_engine
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.orm import sessionmaker

    from src.control_plane.models import ParameterSearchJobRequest
    from src.control_plane.parameter_search import (
        ParameterSearchJobExecutor,
        ResearchBacktestParameterEvaluator,
    )
    from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy

    @compiles(JSONB, "sqlite")
    def _compile_jsonb_for_sqlite(type_, compiler, **kw):
        return "JSON"

    engine = create_engine(f"sqlite:///{database_path}")
    for table in (
        Strategy.__table__,
        EvolutionEpoch.__table__,
        GeneRecord.__table__,
    ):
        table.create(engine, checkfirst=True)
    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add(Strategy(id=_STRATEGY_ID, name="Representative Profile"))
        session.commit()

    request = ParameterSearchJobRequest.model_validate(
        {
            "strategy_id": _STRATEGY_ID,
            "product_id": _PRODUCT_ID,
            "timeframe": "5m",
            "start_time": start_time,
            "end_time": end_time - _FIVE_MINUTES_MS,
            "objective": "maximize_return",
            "seed": 20260724,
            "backtest": {
                "candles_csv_path": str(aggregated_path),
                "initial_balance": str(_INITIAL_BALANCE),
                "maker_fee": "0",
                "taker_fee": "0",
                "write_reports": False,
                "instrument": {
                    "multiplier": "2",
                    "quantity_step": "1",
                    "price_tick": "0.25",
                    "fee_model": "per_contract",
                    "capital_model": "per_contract",
                    "capital_per_contract": "1000",
                },
            },
            "search_space": {
                "parameters": {
                    "trend_window": {
                        "type": "integer",
                        "min": 8,
                        "max": 20,
                        "step": 4,
                    },
                    "breakout_window": {
                        "type": "integer",
                        "min": 5,
                        "max": 15,
                        "step": 5,
                    },
                    "atr_window": {
                        "type": "integer",
                        "min": 7,
                        "max": 21,
                        "step": 7,
                    },
                    "rsi_window": {
                        "type": "integer",
                        "min": 7,
                        "max": 21,
                        "step": 7,
                    },
                    "volume_window": {
                        "type": "integer",
                        "min": 5,
                        "max": 15,
                        "step": 5,
                    },
                    "swing_window": {
                        "type": "integer",
                        "min": 1,
                        "max": 3,
                        "step": 1,
                    },
                    "entry_score": {
                        "type": "integer",
                        "min": 2,
                        "max": 4,
                        "step": 1,
                    },
                    "hold_bars": {
                        "type": "integer",
                        "min": 6,
                        "max": 18,
                        "step": 6,
                    },
                    "max_atr_expansion": {
                        "type": "decimal",
                        "min": "1",
                        "max": "2",
                        "step": "0.5",
                    },
                    "quantity": {
                        "type": "decimal",
                        "min": "1",
                        "max": "1",
                        "step": "1",
                    },
                }
            },
            "evolution": {
                "population_size": 4,
                "max_generations": 1,
                "tournament_size": 2,
                "elite_count": 1,
                "crossover_probability": "0.9",
                "mutation_probability": "0.3",
                "mutation_sigma_steps": "1.5",
                "epoch_id": "representative_profile_memory",
            },
        }
    )
    inner = ResearchBacktestParameterEvaluator(
        representative_strategy_factory,
        precision_codec=codec,
    )

    class MemoryTrackingEvaluator:
        def __init__(self) -> None:
            self.python_current_bytes: list[int] = []
            self.process_peak_rss_bytes: list[int] = []

        def evaluate(self, request, candidate):
            result = inner.evaluate(request, candidate)
            gc.collect()
            self.python_current_bytes.append(tracemalloc.get_traced_memory()[0])
            self.process_peak_rss_bytes.append(_rss_bytes())
            return result

    tracking = MemoryTrackingEvaluator()
    job = ParameterSearchJobExecutor(
        tracking,
        run_inline=True,
        db_session_factory=session_factory,
    ).submit_search(request)
    if len(tracking.python_current_bytes) != 4:
        raise RuntimeError(
            "ga-memory profile requires four unique candidate evaluations"
        )
    if job.result is None:
        raise RuntimeError("ga-memory profile returned no result")
    return {
        "unique_candidate_evaluations": len(tracking.python_current_bytes),
        "candidate_python_current_bytes": tracking.python_current_bytes,
        "candidate_python_current_growth_bytes": (
            tracking.python_current_bytes[-1]
            - tracking.python_current_bytes[0]
        ),
        "candidate_process_peak_rss_bytes": tracking.process_peak_rss_bytes,
        "evaluator_decimal_cache_entries": len(inner._candle_cache),
        "evaluator_prepared_cache_entries": len(inner._prepared_scaled_cache),
    }


def _run_parity(candles, prepared, codec, instrument, database_path: Path) -> dict:
    from sqlalchemy import create_engine, select
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.ext.compiler import compiles
    from sqlalchemy.orm import sessionmaker

    from src.core.analytics import calculate_metrics
    from src.core.backtest_runner import BacktestRunner
    from src.core.orm_models import (
        BacktestResultSummary,
        BacktestTradeLog,
        Exchange,
        Product,
        SignalAudit,
        Strategy,
    )

    @compiles(JSONB, "sqlite")
    def _compile_jsonb_for_sqlite(type_, compiler, **kw):
        return "JSON"

    engine = create_engine(f"sqlite:///{database_path}")
    for table in (
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        SignalAudit.__table__,
        BacktestResultSummary.__table__,
        BacktestTradeLog.__table__,
    ):
        table.create(engine, checkfirst=True)
    session_factory = sessionmaker(bind=engine)
    with session_factory() as session:
        session.add(Exchange(id="RITHMIC", name="Rithmic"))
        session.add(
            Product(
                id=_PRODUCT_ID,
                exchange_id="RITHMIC",
                base_asset="MNQ",
                quote_asset="USD",
            )
        )
        session.commit()

    def strategy():
        return representative_strategy_factory(
            _STRATEGY_ID,
            _PRODUCT_ID,
            "5m",
            _PARAMETERS,
        )

    full_runner = BacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=_PRODUCT_ID,
        timeframe="5m",
        initial_balance=int(_INITIAL_BALANCE),
        data_source=MemoryDataSource(candles),
        max_drawdown_limit=None,
        report_config={
            "csv_trades": False,
            "markdown_report": False,
            "equity_curve": False,
            "journal_export": False,
        },
        db_session_factory=session_factory,
        instrument_spec=instrument,
    )
    full_runner.add_strategy(strategy())
    decimal_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=_PRODUCT_ID,
        timeframe="5m",
        initial_balance=int(_INITIAL_BALANCE),
        data_source=MemoryDataSource(candles),
        instrument_spec=instrument,
    )
    decimal_runner.add_strategy(strategy())
    prepared_runner = ResearchBacktestRunner(
        start_time=candles[0].timestamp,
        end_time=candles[-1].timestamp,
        product_id=_PRODUCT_ID,
        timeframe="5m",
        initial_balance=int(_INITIAL_BALANCE),
        data_source=MemoryDataSource(candles),
        precision_codec=codec,
        prepared_scaled_candles=prepared,
        instrument_spec=instrument,
    )
    prepared_runner.add_strategy(strategy())

    previous_environment = os.environ.get("FLUXTRADE_ENVIRONMENT")
    os.environ["FLUXTRADE_ENVIRONMENT"] = "test"
    try:
        full_result = full_runner.run()
    finally:
        if previous_environment is None:
            os.environ.pop("FLUXTRADE_ENVIRONMENT", None)
        else:
            os.environ["FLUXTRADE_ENVIRONMENT"] = previous_environment
    if full_result is None:
        raise RuntimeError("full backtest returned no result")
    decimal_result = decimal_runner.run()
    prepared_result = prepared_runner.run()
    with session_factory() as session:
        full_trades = session.scalars(select(BacktestTradeLog)).all()
    full_closed = calculate_metrics(
        list(full_trades),
        initial_balance=int(_INITIAL_BALANCE),
        contract_multiplier=instrument.multiplier,
    )["closed_trades"]

    raw_digests = {
        "full": _trade_digest(full_trades),
        "research": _trade_digest(decimal_result["raw_trades"]),
        "prepared": _trade_digest(prepared_result["raw_trades"]),
    }
    closed_digests = {
        "full": _closed_trade_digest(full_closed),
        "research": _closed_trade_digest(decimal_result["closed_trades"]),
        "prepared": _closed_trade_digest(prepared_result["closed_trades"]),
    }
    pnl = {
        "full": full_result["total_pnl"],
        "research": decimal_result["total_pnl"],
        "prepared": prepared_result["total_pnl"],
    }
    mark_to_market_pnl = {
        "full": full_result["mark_to_market_pnl"],
        "research": decimal_result["mark_to_market_pnl"],
        "prepared": prepared_result["mark_to_market_pnl"],
    }
    drawdown = {
        "full": full_result["max_drawdown"],
        "research": decimal_result["max_drawdown"],
        "prepared": prepared_result["max_drawdown"],
    }
    calmar_ratio = {
        "full": full_result["calmar_ratio"],
        "research": decimal_result["calmar_ratio"],
        "prepared": prepared_result["calmar_ratio"],
    }
    max_drawdown_days = {
        "full": full_result["max_drawdown_days"],
        "research": decimal_result["max_drawdown_days"],
        "prepared": prepared_result["max_drawdown_days"],
    }
    final_position = {
        "full": _net_quantity(full_trades),
        "research": _net_quantity(decimal_result["raw_trades"]),
        "prepared": _net_quantity(prepared_result["raw_trades"]),
    }
    for values in (
        raw_digests,
        closed_digests,
        pnl,
        mark_to_market_pnl,
        drawdown,
        calmar_ratio,
        max_drawdown_days,
        final_position,
    ):
        if len(set(values.values())) != 1:
            raise RuntimeError(f"full/research/prepared parity mismatch: {values}")
    return {
        "raw_trade_digest_sha256": raw_digests["full"],
        "closed_trade_digest_sha256": closed_digests["full"],
        "raw_trade_count": len(full_trades),
        "closed_trade_count": len(full_closed),
        "total_pnl": pnl["full"],
        "mark_to_market_pnl": mark_to_market_pnl["full"],
        "max_drawdown": drawdown["full"],
        "calmar_ratio": calmar_ratio["full"],
        "max_drawdown_days": max_drawdown_days["full"],
        "final_net_quantity": final_position["full"],
    }


def main() -> None:
    args = _build_parser().parse_args()
    if args.repeats <= 0:
        raise ValueError("repeats must be positive")
    source_path = Path(args.csv)
    start_time = _timestamp_ms(args.start)
    end_time = _timestamp_ms(args.end)

    tracemalloc.start()
    with TemporaryDirectory(prefix="fluxtrade-mnq-profile-") as temp_dir:
        aggregated_path = Path(temp_dir) / "mnq_5m.csv"
        started = time.perf_counter()
        source_count, closed_count = _aggregate_5m(
            source_path,
            aggregated_path,
            start_time=start_time,
            end_time=end_time,
        )
        aggregation_seconds = time.perf_counter() - started

        load_started = time.perf_counter()
        data_source = CsvDataSource(
            str(aggregated_path),
            product_id=_PRODUCT_ID,
            timeframe="5m",
        )
        candles = list(
            data_source.get_candles(
                _PRODUCT_ID,
                "5m",
                start_time,
                end_time - _FIVE_MINUTES_MS,
            )
        )
        load_seconds = time.perf_counter() - load_started
        decimal_cache_current_bytes, decimal_cache_peak_bytes = (
            tracemalloc.get_traced_memory()
        )
        codec = _codec()
        instrument = _instrument()
        prepare_started = time.perf_counter()
        prepared = ResearchBacktestRunner.prepare_scaled_candles(candles, codec)
        prepare_seconds = time.perf_counter() - prepare_started
        cache_current_bytes, cache_peak_bytes = tracemalloc.get_traced_memory()
        prepared_cache_increment_bytes = max(
            cache_current_bytes - decimal_cache_current_bytes,
            0,
        )
        if args.mode == "ga-memory":
            del prepared
            del candles
            del data_source
            gc.collect()
            ga_memory = _run_ga_memory(
                aggregated_path,
                start_time=start_time,
                end_time=end_time,
                codec=codec,
                database_path=Path(temp_dir) / "ga-memory.db",
            )
            print(f"source_csv={source_path}")
            print(f"range_start_ms={start_time}")
            print(f"range_end_ms={end_time}")
            print(f"source_1m_candles={source_count}")
            print(f"closed_5m_candles={closed_count}")
            print(f"decimal_cache_python_current_bytes={decimal_cache_current_bytes}")
            print(f"decimal_cache_python_peak_bytes={decimal_cache_peak_bytes}")
            print(f"prepared_cache_python_current_bytes={cache_current_bytes}")
            print(f"prepared_cache_python_peak_bytes={cache_peak_bytes}")
            print(f"prepared_cache_python_increment_bytes={prepared_cache_increment_bytes}")
            for key, value in ga_memory.items():
                print(f"{key}={value}")
            return
        if args.mode == "parity":
            parity = _run_parity(
                candles,
                prepared,
                codec,
                instrument,
                Path(temp_dir) / "parity.db",
            )
            print(f"source_csv={source_path}")
            print(f"range_start_ms={start_time}")
            print(f"range_end_ms={end_time}")
            print(f"source_1m_candles={source_count}")
            print(f"closed_5m_candles={closed_count}")
            print(f"loaded_5m_candles={len(candles)}")
            print("paths=full,research,prepared")
            for key, value in parity.items():
                print(f"{key}={value}")
            return

        runner = {
            "bare": lambda: _run_bare(prepared, codec, instrument),
            "minimal-context": lambda: _run_minimal_context(
                candles,
                prepared,
                codec,
                instrument,
            ),
            "full-context": lambda: _run_full_context(
                candles,
                prepared,
                codec,
                instrument,
            ),
            "representative": lambda: _run_representative(
                candles,
                prepared,
                codec,
                instrument,
            ),
        }[args.mode]

        tracemalloc.stop()
        durations = []
        digests = []
        final_summary = {}
        for _ in range(args.repeats):
            replay_started = time.perf_counter()
            result = runner()
            durations.append(time.perf_counter() - replay_started)
            if result.get("raw_trades") is not None:
                digests.append(_trade_digest(result["raw_trades"]))
                final_summary = {
                    "raw_trade_count": result["raw_trade_count"],
                    "closed_trade_count": len(result["closed_trades"]),
                    "nonzero_pnl_trade_count": result["total_trades"],
                    "total_pnl": result["total_pnl"],
                    "max_drawdown": result["max_drawdown"],
                    "final_net_quantity": _net_quantity(result["raw_trades"]),
                }
            del result
            gc.collect()

        tracemalloc.start()
        current_after_repeat = []
        replay_base_current, _ = tracemalloc.get_traced_memory()
        for _ in range(args.repeats):
            result = runner()
            del result
            gc.collect()
            current_after_repeat.append(tracemalloc.get_traced_memory()[0])
        replay_current, replay_peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

    if digests and len(set(digests)) != 1:
        raise RuntimeError("representative replay produced non-deterministic trade digests")

    median_seconds = statistics.median(durations)
    print(f"source_csv={source_path}")
    print(f"range_start_ms={start_time}")
    print(f"range_end_ms={end_time}")
    print(f"source_1m_candles={source_count}")
    print(f"closed_5m_candles={closed_count}")
    print(f"loaded_5m_candles={len(candles)}")
    print(f"mode={args.mode}")
    print(f"repeats={args.repeats}")
    print(f"aggregation_seconds={aggregation_seconds:.6f}")
    print(f"load_seconds={load_seconds:.6f}")
    print(f"prepare_seconds={prepare_seconds:.6f}")
    print(f"replay_seconds_median={median_seconds:.6f}")
    print(f"replay_seconds_max={max(durations):.6f}")
    print(f"replay_candles_per_second_median={len(candles) / median_seconds:.0f}")
    print(f"cache_python_current_bytes={cache_current_bytes}")
    print(f"cache_python_peak_bytes={cache_peak_bytes}")
    print(f"decimal_cache_python_current_bytes={decimal_cache_current_bytes}")
    print(f"decimal_cache_python_peak_bytes={decimal_cache_peak_bytes}")
    print(f"prepared_cache_python_increment_bytes={prepared_cache_increment_bytes}")
    print(f"replay_python_peak_increment_bytes={max(replay_peak - replay_base_current, 0)}")
    print(f"replay_python_retained_bytes={max(replay_current - replay_base_current, 0)}")
    print(
        "repeat_python_current_growth_bytes="
        f"{current_after_repeat[-1] - current_after_repeat[0]}"
    )
    print(f"process_peak_rss_bytes={_rss_bytes()}")
    if digests:
        print(f"trade_digest_sha256={digests[-1]}")
        print(f"raw_trade_count={final_summary['raw_trade_count']}")
        print(f"closed_trade_count={final_summary['closed_trade_count']}")
        print(
            "nonzero_pnl_trade_count="
            f"{final_summary['nonzero_pnl_trade_count']}"
        )
        print(f"total_pnl={final_summary['total_pnl']}")
        print(f"max_drawdown={final_summary['max_drawdown']}")
        print(f"final_net_quantity={final_summary['final_net_quantity']}")


if __name__ == "__main__":
    main()
