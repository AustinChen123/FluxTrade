"""Opt-in full MNQ replay across audited futures roll boundaries."""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import sessionmaker

from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.models import (
    BacktestInstrumentConfig,
    BacktestJobRequest,
    JobStatus,
)
from src.core.orm_models import (
    BacktestResultSummary,
    BacktestTradeLog,
    Exchange,
    Product,
    SignalAudit,
    Strategy,
)

try:
    import fluxtrade_core  # noqa: F401

    HAS_RUST = True
except ImportError:
    HAS_RUST = False

pytestmark = [
    pytest.mark.rust,
    pytest.mark.skipif(not HAS_RUST, reason="fluxtrade_core.so not compiled"),
]

REPO_ROOT = Path(__file__).parents[3]
FULL_CSV = REPO_ROOT / "mnq_intraday_research/data/MNQ_1m_massive.csv"
ROLL_LOG = REPO_ROOT / "mnq_intraday_research/data/MNQ_roll_log.csv"
CONTRACT_PRODUCTS = {
    "MNQU4": "RITHMIC:MNQ-202409",
    "MNQZ4": "RITHMIC:MNQ-202412",
    "MNQH5": "RITHMIC:MNQ-202503",
    "MNQM5": "RITHMIC:MNQ-202506",
    "MNQU5": "RITHMIC:MNQ-202509",
    "MNQZ5": "RITHMIC:MNQ-202512",
    "MNQH6": "RITHMIC:MNQ-202603",
    "MNQM6": "RITHMIC:MNQ-202606",
    "MNQU6": "RITHMIC:MNQ-202609",
}


@dataclass(frozen=True)
class RollBoundary:
    timestamp: int
    from_contract: str
    to_contract: str
    old_close: Decimal
    new_open: Decimal


@dataclass
class Segment:
    contract: str
    csv_path: Path
    count: int = 0
    first_timestamp: int | None = None
    last_timestamp: int | None = None
    head_timestamps: list[int] = field(default_factory=list)
    tail_timestamps: list[int] = field(default_factory=list)

    @property
    def product_id(self) -> str:
        return CONTRACT_PRODUCTS[self.contract]


@compiles(JSONB, "sqlite")
def _compile_jsonb_for_sqlite(type_, compiler, **kw):
    return "JSON"


def _timestamp_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


def _load_rolls(path: Path) -> list[RollBoundary]:
    with path.open(newline="") as handle:
        return [
            RollBoundary(
                timestamp=_timestamp_ms(row["roll_time_utc"]),
                from_contract=row["from_contract"],
                to_contract=row["to_contract"],
                old_close=Decimal(row["old_close"]),
                new_open=Decimal(row["new_open"]),
            )
            for row in csv.DictReader(handle)
        ]


def _split_segments(
    source: Path, rolls: list[RollBoundary], output: Path
) -> list[Segment]:
    contracts = [rolls[0].from_contract, *(roll.to_contract for roll in rolls)]
    segments = [Segment(contract, output / f"{contract}.csv") for contract in contracts]
    handles = [segment.csv_path.open("w", newline="") for segment in segments]
    try:
        writers = [csv.writer(handle) for handle in handles]
        with source.open(newline="") as source_handle:
            reader = csv.DictReader(source_handle)
            headers = reader.fieldnames
            assert headers is not None
            for writer in writers:
                writer.writerow(headers)

            segment_index = 0
            previous_close: Decimal | None = None
            for row in reader:
                timestamp = _timestamp_ms(row["timestamp"])
                if (
                    segment_index < len(rolls)
                    and timestamp >= rolls[segment_index].timestamp
                ):
                    roll = rolls[segment_index]
                    assert segments[segment_index].contract == roll.from_contract
                    assert previous_close == roll.old_close
                    assert Decimal(row["open"]) == roll.new_open
                    segment_index += 1

                segment = segments[segment_index]
                writers[segment_index].writerow(row.values())
                segment.count += 1
                segment.first_timestamp = segment.first_timestamp or timestamp
                segment.last_timestamp = timestamp
                if len(segment.head_timestamps) < 5:
                    segment.head_timestamps.append(timestamp)
                segment.tail_timestamps.append(timestamp)
                if len(segment.tail_timestamps) > 5:
                    segment.tail_timestamps.pop(0)
                previous_close = Decimal(row["close"])
    finally:
        for handle in handles:
            handle.close()
    return segments


def _write_flat_at_roll_signals(segment: Segment, output: Path) -> Path:
    assert len(segment.head_timestamps) == 5
    assert len(segment.tail_timestamps) == 5
    path = output / f"{segment.contract}_signals.csv"
    with path.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "type", "quantity"])
        writer.writerow([segment.head_timestamps[1], "LONG", "1"])
        writer.writerow([segment.head_timestamps[3], "EXIT_LONG", "1"])
        writer.writerow([segment.tail_timestamps[0], "LONG", "1"])
        writer.writerow([segment.tail_timestamps[2], "EXIT_LONG", "1"])
    return path


def _session_factory(path: Path, product_ids: list[str]):
    engine = create_engine(f"sqlite:///{path}")
    for table in [
        Exchange.__table__,
        Product.__table__,
        Strategy.__table__,
        SignalAudit.__table__,
        BacktestResultSummary.__table__,
        BacktestTradeLog.__table__,
    ]:
        table.create(engine, checkfirst=True)

    factory = sessionmaker(bind=engine)
    with factory() as session:
        session.add(Exchange(id="RITHMIC", name="Rithmic"))
        session.add_all(
            Product(
                id=product_id,
                exchange_id="RITHMIC",
                base_asset="MNQ",
                quote_asset="USD",
            )
            for product_id in product_ids
        )
        session.commit()
    return factory


def test_split_segments_assigns_roll_timestamp_to_new_contract(tmp_path):
    source = tmp_path / "candles.csv"
    source.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2024-09-12T20:58:00Z,19412,19414,19411,19413,1\n"
        "2024-09-12T20:59:00Z,19413,19414,19410.75,19413.25,1\n"
        "2024-09-12T22:00:00Z,19647,19655,19646,19652.25,1\n"
        "2024-09-12T22:01:00Z,19653.5,19658,19652.75,19658,1\n"
    )
    roll = RollBoundary(
        timestamp=_timestamp_ms("2024-09-12T22:00:00Z"),
        from_contract="MNQU4",
        to_contract="MNQZ4",
        old_close=Decimal("19413.25"),
        new_open=Decimal("19647"),
    )

    segments = _split_segments(source, [roll], tmp_path)

    assert [segment.count for segment in segments] == [2, 2]
    assert [segment.product_id for segment in segments] == [
        "RITHMIC:MNQ-202409",
        "RITHMIC:MNQ-202412",
    ]
    assert segments[0].last_timestamp is not None
    assert segments[0].last_timestamp < roll.timestamp
    assert segments[1].first_timestamp == roll.timestamp


@pytest.mark.parametrize(
    ("old_close", "new_open"),
    [("0", "19647"), ("19413.25", "0")],
)
def test_split_segments_rejects_roll_price_mismatch(tmp_path, old_close, new_open):
    source = tmp_path / "candles.csv"
    source.write_text(
        "timestamp,open,high,low,close,volume\n"
        "2024-09-12T20:59:00Z,19413,19414,19410.75,19413.25,1\n"
        "2024-09-12T22:00:00Z,19647,19655,19646,19652.25,1\n"
    )
    roll = RollBoundary(
        timestamp=_timestamp_ms("2024-09-12T22:00:00Z"),
        from_contract="MNQU4",
        to_contract="MNQZ4",
        old_close=Decimal(old_close),
        new_open=Decimal(new_open),
    )

    with pytest.raises(AssertionError):
        _split_segments(source, [roll], tmp_path)


@pytest.mark.skipif(
    os.getenv("RUN_MNQ_FULL_REPLAY") != "1",
    reason="set RUN_MNQ_FULL_REPLAY=1 to use local research data",
)
def test_full_mnq_replay_is_flat_at_every_roll_boundary(tmp_path):
    assert FULL_CSV.is_file()
    assert ROLL_LOG.is_file()
    rolls = _load_rolls(ROLL_LOG)
    assert len(rolls) == 8

    segments = _split_segments(FULL_CSV, rolls, tmp_path)
    assert [segment.contract for segment in segments] == list(CONTRACT_PRODUCTS)
    assert sum(segment.count for segment in segments) == 705_493

    session_factory = _session_factory(
        tmp_path / "mnq_full_replay.db",
        [segment.product_id for segment in segments],
    )
    executor = BacktestJobExecutor(db_session_factory=session_factory, run_inline=True)
    running_balance = Decimal("100000")
    for index, segment in enumerate(segments):
        assert segment.first_timestamp is not None
        assert segment.last_timestamp is not None
        job = executor.submit_backtest(
            BacktestJobRequest(
                strategy_id=f"mnq_full_{index}",
                product_id=segment.product_id,
                timeframe="1m",
                candles_csv_path=str(segment.csv_path),
                signals_csv_path=str(_write_flat_at_roll_signals(segment, tmp_path)),
                start_time=segment.first_timestamp,
                end_time=segment.last_timestamp,
                initial_balance=running_balance,
                maker_fee=Decimal("0.50"),
                taker_fee=Decimal("0.50"),
                instrument=BacktestInstrumentConfig.model_validate(
                    {
                        "multiplier": "2",
                        "quantity_step": "1",
                        "price_tick": "0.25",
                        "fee_model": "per_contract",
                        "capital_model": "per_contract",
                        "capital_per_contract": "2500",
                    }
                ),
            )
        )
        assert job.status == JobStatus.SUCCEEDED, job.error
        assert job.result is not None
        assert job.result["candle_count"] == segment.count
        assert job.result["total_trades"] == 2
        assert [entry["tag"] for entry in job.result["journal"]].count("entry") == 4
        assert [entry["tag"] for entry in job.result["journal"]].count("fill") == 4
        running_balance += Decimal(job.result["total_pnl"])

    with session_factory() as session:
        trades = list(session.scalars(select(BacktestTradeLog)))
        summaries = list(session.scalars(select(BacktestResultSummary)))

    assert len(trades) == 36
    assert len(summaries) == 9
    assert {trade.product_id for trade in trades} == set(CONTRACT_PRODUCTS.values())
    assert sum(Decimal(summary.total_pnl) for summary in summaries) == (
        running_balance - Decimal("100000")
    )
    for index, segment in enumerate(segments):
        assert segment.last_timestamp is not None
        segment_trades = [
            trade for trade in trades if trade.strategy_id == f"mnq_full_{index}"
        ]
        assert len(segment_trades) == 4
        assert sum(
            Decimal(trade.quantity)
            if trade.side.lower() == "buy"
            else -Decimal(trade.quantity)
            for trade in segment_trades
        ) == Decimal("0")
        assert max(trade.timestamp for trade in segment_trades) < (
            rolls[index].timestamp if index < len(rolls) else segment.last_timestamp + 1
        )
