from __future__ import annotations

import argparse
import csv
import sys
import time
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

from fluxtrade_core import CandleAggregator, Candlestick  # noqa: E402
from src.control_plane.models import ParameterSearchJobRequest  # noqa: E402
from src.control_plane.parameter_search import (  # noqa: E402
    GoldenCrossResearchParameterEvaluator,
    ParameterSearchJobExecutor,
)
from src.core.orm_models import EvolutionEpoch, GeneRecord, Strategy  # noqa: E402

_FIVE_MINUTES_MS = 5 * 60 * 1000
_PRODUCT_ID = "RITHMIC:MNQ_ROLL-PERP"


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


class _TimedEvaluator:
    def __init__(self) -> None:
        self.inner = GoldenCrossResearchParameterEvaluator()
        self.durations: list[float] = []

    def evaluate(self, request, candidate):
        started = time.perf_counter()
        result = self.inner.evaluate(request, candidate)
        self.durations.append(time.perf_counter() - started)
        return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate real MNQ 1m CSV data to closed 5m candles in Rust, "
            "then run a resumable GoldenCross GA through ResearchBacktestRunner."
        )
    )
    parser.add_argument("--csv", required=True)
    parser.add_argument("--start", required=True, help="Inclusive ISO-8601 5m boundary")
    parser.add_argument(
        "--end",
        required=True,
        help="Inclusive ISO-8601 5m boundary used to close the preceding bucket",
    )
    parser.add_argument("--population", type=int, default=8)
    parser.add_argument("--generations", type=int, default=5)
    parser.add_argument("--seed", type=int, default=20260724)
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
                Candlestick(
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


def _session_factory(path: Path):
    engine = create_engine(f"sqlite:///{path}")
    for table in [
        Strategy.__table__,
        EvolutionEpoch.__table__,
        GeneRecord.__table__,
    ]:
        table.create(engine, checkfirst=True)
    factory = sessionmaker(bind=engine)
    with factory() as session:
        session.add(Strategy(id="golden_cross_mnq_5m", name="Golden Cross MNQ 5m"))
        session.commit()
    return factory


def main() -> None:
    args = _build_parser().parse_args()
    source_path = Path(args.csv)
    start_time = _timestamp_ms(args.start)
    end_time = _timestamp_ms(args.end)

    with TemporaryDirectory(prefix="fluxtrade-mnq-5m-ga-") as temp_dir:
        temp = Path(temp_dir)
        aggregated_path = temp / "mnq_5m.csv"

        aggregation_started = time.perf_counter()
        source_count, candle_count = _aggregate_5m(
            source_path,
            aggregated_path,
            start_time=start_time,
            end_time=end_time,
        )
        aggregation_elapsed = time.perf_counter() - aggregation_started

        request = ParameterSearchJobRequest.model_validate(
            {
                "strategy_id": "golden_cross_mnq_5m",
                "product_id": _PRODUCT_ID,
                "timeframe": "5m",
                "start_time": start_time,
                "end_time": end_time - _FIVE_MINUTES_MS,
                "objective": "maximize_return",
                "seed": args.seed,
                "backtest": {
                    "candles_csv_path": str(aggregated_path),
                    "initial_balance": "100000",
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
                        "short_window": {
                            "type": "integer",
                            "min": 2,
                            "max": 10,
                            "step": 1,
                        },
                        "long_window": {
                            "type": "integer",
                            "min": 12,
                            "max": 40,
                            "step": 2,
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
                    "population_size": args.population,
                    "max_generations": args.generations,
                    "tournament_size": min(3, args.population),
                    "elite_count": 1,
                    "crossover_probability": "0.9",
                    "mutation_probability": "0.3",
                    "mutation_sigma_steps": "1.5",
                    "epoch_id": "golden_cross_mnq_5m_demo",
                },
            }
        )
        evaluator = _TimedEvaluator()
        ga_started = time.perf_counter()
        job = ParameterSearchJobExecutor(
            evaluator,
            run_inline=True,
            db_session_factory=_session_factory(temp / "checkpoint.db"),
        ).submit_search(request)
        ga_elapsed = time.perf_counter() - ga_started

        by_generation = defaultdict(list)
        with _session_factory_for_existing(temp / "checkpoint.db")() as session:
            records = (
                session.query(GeneRecord)
                .order_by(GeneRecord.generation_index, GeneRecord.candidate_id)
                .all()
            )
            epoch = session.get(EvolutionEpoch, "golden_cross_mnq_5m_demo")
            for record in records:
                by_generation[record.generation_index].append(record)

        print("Rust 1m -> 5m + GoldenCross GA")
        print(f"  source_csv={source_path}")
        print(f"  source_1m_candles={source_count}")
        print(f"  closed_5m_candles={candle_count}")
        print("  maker_fee=0 taker_fee=0")
        print(f"  aggregation_seconds={aggregation_elapsed:.3f}")
        print(f"  aggregation_1m_candles_per_second={source_count / aggregation_elapsed:.0f}")
        for generation, generation_records in by_generation.items():
            best = max(generation_records, key=lambda record: record.score_total)
            print(
                f"  generation={generation} score={best.score_total} "
                f"drawdown={best.max_drawdown} params={best.param_pack}"
            )
        durations = evaluator.durations
        print(f"  checkpoint={epoch.status} generations={epoch.generations_run}")
        print(f"  gene_records={len(records)} unique_evaluations={len(durations)}")
        print(f"  ga_seconds={ga_elapsed:.3f}")
        print(f"  seconds_per_unique_candidate={sum(durations) / len(durations):.3f}")
        print(f"  best_params={job.result['best_candidate_param_pack']}")
        print(f"  best_metrics={job.result['best_candidate']['metrics']}")


def _session_factory_for_existing(path: Path):
    return sessionmaker(bind=create_engine(f"sqlite:///{path}"))


if __name__ == "__main__":
    main()
