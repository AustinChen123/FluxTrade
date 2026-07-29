from __future__ import annotations

import csv
import io
import json
from decimal import Decimal
from pathlib import Path

import pytest

from src.core.models import Candlestick, Signal, SignalType
from src.core.portfolio_runtime import PortfolioDefinition, PortfolioSleeve
from src.core.strategy_context import StrategyContext
from src.strategies.base import BaseStrategy, StrategyRequirements
from src.validation.strategy_evidence import (
    run_portfolio_shadow_evidence,
    run_shadow_evidence,
    verify_portfolio_historical_stream_parity,
    verify_portfolio_shadow_evidence_bundle,
    verify_shadow_evidence_bundle,
    verify_historical_stream_parity,
)

PRODUCT_ID = "RITHMIC:MNQ-202609"
SOURCE_STREAM_KEY = "stream:market:rithmic:mnq-202609:1m"
DECISION_STREAM_KEY = "stream:market:rithmic:mnq-202609:5m"


class CloseSignalStrategy(BaseStrategy):
    @property
    def requirements(self) -> StrategyRequirements:
        return StrategyRequirements(self.product_id, "5m", 0)

    def fresh_instance_for_replay(self) -> BaseStrategy:
        return type(self)(self.strategy_id, self.product_id)

    def replay_configuration(self) -> object:
        return ()

    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="5m",
            timestamp=candle.timestamp,
            type=SignalType.LONG,
            quantity=Decimal("1"),
            value=candle.close,
        )


setattr(CloseSignalStrategy, "__fluxtrade_display_name__", "Evidence Fixture")
setattr(CloseSignalStrategy, "__fluxtrade_artifact_version__", "1.0.0")
setattr(CloseSignalStrategy, "__fluxtrade_readiness__", "RESEARCH_VALIDATED")
setattr(CloseSignalStrategy, "__fluxtrade_catalog_sha256__", "0" * 64)


def _portfolio(
    *,
    max_gross_quantity: Decimal = Decimal("2"),
) -> PortfolioDefinition:
    return PortfolioDefinition(
        portfolio_id="portfolio",
        product_id=PRODUCT_ID,
        sleeves=(
            PortfolioSleeve(
                CloseSignalStrategy("portfolio.sleeve_a", PRODUCT_ID)
            ),
            PortfolioSleeve(
                CloseSignalStrategy("portfolio.sleeve_b", PRODUCT_ID)
            ),
        ),
        max_gross_quantity=max_gross_quantity,
        artifact_version="1.0.0",
        display_name="Evidence Portfolio",
        readiness="RESEARCH_FROZEN",
        catalog_sha256="1" * 64,
    )


def _write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "open", "high", "low", "close", "volume"])
        writer.writerows(rows)


def test_historical_stream_parity_compares_closed_candles_and_signals(
    tmp_path,
    monkeypatch,
):
    source = tmp_path / "source.csv"
    reference = tmp_path / "reference.csv"
    source_rows = [
        (
            f"2026-07-27T12:{minute:02d}:00Z",
            str(100 + minute),
            str(101 + minute),
            str(99 + minute),
            str(100 + minute),
            "1",
        )
        for minute in range(11)
    ]
    _write_csv(source, source_rows)
    _write_csv(
        reference,
        [
            ("1785153600000", "100", "105", "99", "104", "5"),
            ("1785153900000", "105", "110", "104", "109", "5"),
        ],
    )
    original_open = Path.open
    open_counts = {source: 0, reference: 0}

    def counting_open(path, *args, **kwargs):
        if path in open_counts:
            open_counts[path] += 1
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", counting_open)

    report = verify_historical_stream_parity(
        source,
        reference,
        product_id=PRODUCT_ID,
        strategy_factory=lambda: CloseSignalStrategy("strategy", PRODUCT_ID),
    )

    assert report.matched_candles == 2
    assert report.product_id == PRODUCT_ID
    assert report.source_timeframe == "1m"
    assert report.target_timeframe == "5m"
    assert report.extra_source_candles == 0
    assert report.source_decision_count == 2
    assert report.source_actionable_signal_count == 2
    assert report.source_signal_digest == report.reference_signal_digest
    assert len(report.source_sha256) == 64
    assert len(report.reference_sha256) == 64
    assert report.strategy.strategy_id == "strategy"
    assert len(report.strategy.replay_configuration_sha256) == 64
    assert open_counts == {source: 1, reference: 1}


def test_historical_stream_parity_rejects_one_value_difference(tmp_path):
    source = tmp_path / "source.csv"
    reference = tmp_path / "reference.csv"
    _write_csv(
        source,
        [
            (
                f"2026-07-27T12:{minute:02d}:00Z",
                "100",
                "101",
                "99",
                "100",
                "1",
            )
            for minute in range(6)
        ],
    )
    _write_csv(
        reference,
        [("1785153600000", "100", "101", "98.75", "100", "5")],
    )

    with pytest.raises(AssertionError, match="candle value mismatch"):
        verify_historical_stream_parity(
            source,
            reference,
            product_id=PRODUCT_ID,
            strategy_factory=lambda: CloseSignalStrategy("strategy", PRODUCT_ID),
        )


def test_historical_stream_parity_reports_only_partial_reference_prefix(tmp_path):
    source = tmp_path / "source.csv"
    reference = tmp_path / "reference.csv"
    _write_csv(
        source,
        [
            (
                f"2026-07-27T12:{minute:02d}:00Z",
                str(100 + minute),
                str(101 + minute),
                str(99 + minute),
                str(100 + minute),
                "1",
            )
            for minute in range(2, 11)
        ],
    )
    _write_csv(
        reference,
        [
            ("1785153600000", "102", "105", "101", "104", "3"),
            ("1785153900000", "105", "110", "104", "109", "5"),
        ],
    )

    report = verify_historical_stream_parity(
        source,
        reference,
        product_id=PRODUCT_ID,
        strategy_factory=lambda: CloseSignalStrategy("strategy", PRODUCT_ID),
    )

    assert report.skipped_reference_prefix_candles == 1
    assert report.matched_candles == 1
    assert report.source_actionable_signal_count == 1


def test_portfolio_historical_stream_parity_uses_complete_sleeve_batch(
    tmp_path,
):
    source = tmp_path / "source.csv"
    reference = tmp_path / "reference.csv"
    _write_csv(
        source,
        [
            (
                f"2026-07-27T12:{minute:02d}:00Z",
                str(100 + minute),
                str(101 + minute),
                str(99 + minute),
                str(100 + minute),
                "1",
            )
            for minute in range(11)
        ],
    )
    _write_csv(
        reference,
        [
            ("1785153600000", "100", "105", "99", "104", "5"),
            ("1785153900000", "105", "110", "104", "109", "5"),
        ],
    )

    report = verify_portfolio_historical_stream_parity(
        source,
        reference,
        product_id=PRODUCT_ID,
        portfolio_factory=_portfolio,
    )

    assert report.source_decision_count == 4
    assert report.source_actionable_signal_count == 4
    assert report.source_signal_digest == report.reference_signal_digest
    assert report.strategy.strategy_id == "portfolio"
    assert report.strategy.display_name == "Evidence Portfolio"


def test_portfolio_historical_stream_parity_enforces_batch_gross_limit(
    tmp_path,
):
    source = tmp_path / "source.csv"
    reference = tmp_path / "reference.csv"
    _write_csv(
        source,
        [
            (
                f"2026-07-27T12:{minute:02d}:00Z",
                "100",
                "101",
                "99",
                "100",
                "1",
            )
            for minute in range(6)
        ],
    )
    _write_csv(
        reference,
        [("1785153600000", "100", "101", "99", "100", "5")],
    )

    with pytest.raises(
        RuntimeError,
        match="portfolio_gross_limit_exceeded",
    ):
        verify_portfolio_historical_stream_parity(
            source,
            reference,
            product_id=PRODUCT_ID,
            portfolio_factory=lambda: _portfolio(
                max_gross_quantity=Decimal("1")
            ),
        )


class _FakeRedis:
    def __init__(self, events):
        self.events = list(events)
        self.calls = []

    def xread(self, streams, *, count, block):
        self.calls.append((streams, count, block))
        if not self.events:
            return []
        stream_key, entry = self.events.pop(0)
        return [(stream_key, [entry])]


class _Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        self.now += 0.05
        return self.now


def _source_entry(minute):
    candle = Candlestick(
        product_id=PRODUCT_ID,
        timeframe="1m",
        timestamp=1_800_000_000_000 + minute * 60_000,
        open=Decimal(100 + minute),
        high=Decimal(101 + minute),
        low=Decimal(99 + minute),
        close=Decimal(100 + minute),
        volume=Decimal("1"),
    )
    return (
        f"{1_800_000_000_000 + minute * 60_000}-0",
        {"json": candle.model_dump_json()},
    )


def _decision_entry(bucket_minute):
    candle = Candlestick(
        product_id=PRODUCT_ID,
        timeframe="5m",
        timestamp=1_800_000_000_000 + bucket_minute * 60_000,
        open=Decimal(100 + bucket_minute),
        high=Decimal(105 + bucket_minute),
        low=Decimal(99 + bucket_minute),
        close=Decimal(104 + bucket_minute),
        volume=Decimal("5"),
    )
    return (
        f"{1_800_000_000_000 + (bucket_minute + 5) * 60_000}-0",
        {"json": candle.model_dump_json()},
    )


def _source_events(minutes):
    return [(SOURCE_STREAM_KEY, _source_entry(minute)) for minute in minutes]


def _run_shadow(redis, strategy, output, duration_seconds=1):
    return run_shadow_evidence(
        redis,
        source_stream_key=SOURCE_STREAM_KEY,
        decision_stream_key=DECISION_STREAM_KEY,
        strategy=strategy,
        output=output,
        duration_seconds=duration_seconds,
        monotonic=_Clock(),
    )


def test_shadow_records_replayable_source_candles_and_complete_decisions():
    redis = _FakeRedis(
        _source_events(range(6))
        + [(DECISION_STREAM_KEY, _decision_entry(0))]
    )
    output = io.StringIO()

    report = _run_shadow(
        redis,
        CloseSignalStrategy("strategy", PRODUCT_ID),
        output,
    )

    assert report.source_candles == 6
    assert report.source_stream_key == SOURCE_STREAM_KEY
    assert report.decision_stream_key == DECISION_STREAM_KEY
    assert report.product_id == PRODUCT_ID
    assert report.target_timeframe == "5m"
    assert report.completed_candles == 1
    assert report.actionable_signal_count == 1
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    assert records[0]["type"] == "session"
    assert [record["type"] for record in records].count("source_candle") == 6
    decisions = [record for record in records if record["type"] == "decision"]
    assert decisions[0]["signals"][0]["type"] == "LONG"
    assert decisions[0]["signals"][0]["quantity"] == "1"
    assert records[-1]["type"] == "summary"
    output.seek(0)
    replay = verify_shadow_evidence_bundle(
        output,
        strategy_factory=lambda: CloseSignalStrategy("strategy", PRODUCT_ID),
    )
    assert replay == report
    assert all(call[0] for call in redis.calls)
    assert not hasattr(redis, "xreadgroup")


def test_portfolio_shadow_records_and_replays_coordinated_decisions():
    redis = _FakeRedis(
        _source_events(range(6))
        + [(DECISION_STREAM_KEY, _decision_entry(0))]
    )
    output = io.StringIO()

    report = run_portfolio_shadow_evidence(
        redis,
        source_stream_key=SOURCE_STREAM_KEY,
        decision_stream_key=DECISION_STREAM_KEY,
        portfolio=_portfolio(),
        output=output,
        duration_seconds=1,
        monotonic=_Clock(),
    )

    assert report.actionable_signal_count == 2
    assert report.strategy.strategy_id == "portfolio"
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    decision = next(record for record in records if record["type"] == "decision")
    assert [signal["strategy_id"] for signal in decision["signals"]] == [
        "portfolio.sleeve_a",
        "portfolio.sleeve_b",
    ]
    output.seek(0)
    assert (
        verify_portfolio_shadow_evidence_bundle(
            output,
            portfolio_factory=_portfolio,
        )
        == report
    )


def test_shadow_discards_partial_startup_bucket_then_recovers():
    redis = _FakeRedis(
        _source_events(range(2, 11))
        + [(DECISION_STREAM_KEY, _decision_entry(5))]
    )
    output = io.StringIO()

    report = _run_shadow(
        redis,
        CloseSignalStrategy("strategy", PRODUCT_ID),
        output,
        duration_seconds=2,
    )

    assert report.source_candles == 9
    assert report.completed_candles == 1
    assert report.actionable_signal_count == 1


class NoSignalStrategy(CloseSignalStrategy):
    def on_candle(
        self,
        candle: Candlestick,
        context: StrategyContext | None = None,
    ) -> Signal:
        return Signal(
            strategy_id=self.strategy_id,
            product_id=self.product_id,
            timeframe="5m",
            timestamp=candle.timestamp,
            type=SignalType.NO_SIGNAL,
        )


def test_shadow_records_no_signal_decisions():
    output = io.StringIO()
    report = _run_shadow(
        _FakeRedis(
            _source_events(range(6))
            + [(DECISION_STREAM_KEY, _decision_entry(0))]
        ),
        NoSignalStrategy("strategy", PRODUCT_ID),
        output,
    )

    decisions = [
        json.loads(line)
        for line in output.getvalue().splitlines()
        if json.loads(line)["type"] == "decision"
    ]
    assert report.actionable_signal_count == 0
    assert decisions[0]["signals"][0]["type"] == "NO_SIGNAL"


def test_shadow_replay_rejects_changed_decision():
    output = io.StringIO()
    _run_shadow(
        _FakeRedis(
            _source_events(range(6))
            + [(DECISION_STREAM_KEY, _decision_entry(0))]
        ),
        CloseSignalStrategy("strategy", PRODUCT_ID),
        output,
    )
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    decision = next(record for record in records if record["type"] == "decision")
    decision["signals"][0]["quantity"] = "2"
    tampered = io.StringIO(
        "".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            for record in records
        )
    )

    with pytest.raises(AssertionError, match="strategy decision mismatch"):
        verify_shadow_evidence_bundle(
            tampered,
            strategy_factory=lambda: CloseSignalStrategy(
                "strategy",
                PRODUCT_ID,
            ),
        )


def test_shadow_fails_closed_on_wrong_product():
    wrong = Candlestick(
        product_id="RITHMIC:NQ-202609",
        timeframe="1m",
        timestamp=1_800_000_000_000,
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100"),
        volume=Decimal("1"),
    )
    redis = _FakeRedis(
        [
            (
                SOURCE_STREAM_KEY,
                ("1800000000000-0", {"json": wrong.model_dump_json()}),
            )
        ]
    )

    with pytest.raises(ValueError, match="shadow product mismatch"):
        _run_shadow(
            redis,
            CloseSignalStrategy("strategy", PRODUCT_ID),
            io.StringIO(),
        )


@pytest.mark.parametrize("minutes", [[], [2]])
def test_shadow_rejects_empty_or_partial_only_evidence(minutes):
    output = io.StringIO()

    with pytest.raises(ValueError, match="shadow evidence is insufficient"):
        _run_shadow(
            _FakeRedis(_source_events(minutes)),
            CloseSignalStrategy("strategy", PRODUCT_ID),
            output,
        )

    output.seek(0)
    with pytest.raises(ValueError, match="incomplete|boundaries"):
        verify_shadow_evidence_bundle(
            output,
            strategy_factory=lambda: CloseSignalStrategy(
                "strategy",
                PRODUCT_ID,
            ),
        )


def test_shadow_rejects_strategy_without_catalog_provenance():
    class LegacyStrategy(CloseSignalStrategy):
        __fluxtrade_display_name__ = None
        __fluxtrade_artifact_version__ = None
        __fluxtrade_readiness__ = None
        __fluxtrade_catalog_sha256__ = None

    with pytest.raises(ValueError, match="catalog-loaded strategy"):
        _run_shadow(
            _FakeRedis(_source_events(range(6))),
            LegacyStrategy("strategy", PRODUCT_ID),
            io.StringIO(),
        )


def test_shadow_uses_official_decision_stream_after_reconnect_reset():
    output = io.StringIO()
    redis = _FakeRedis(
        _source_events((2, 3, 5, 6, 7, 8, 9, 10))
        + [(DECISION_STREAM_KEY, _decision_entry(5))]
    )

    report = _run_shadow(
        redis,
        CloseSignalStrategy("strategy", PRODUCT_ID),
        output,
        duration_seconds=2,
    )

    assert report.source_candles == 8
    assert report.completed_candles == 1
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    decisions = [record for record in records if record["type"] == "decision"]
    assert [record["candle"]["timestamp"] for record in decisions] == [
        1_800_000_300_000
    ]
    output.seek(0)
    assert (
        verify_shadow_evidence_bundle(
            output,
            strategy_factory=lambda: CloseSignalStrategy(
                "strategy",
                PRODUCT_ID,
            ),
        )
        == report
    )


def test_shadow_waits_for_source_watermark_before_committing_decision():
    output = io.StringIO()
    redis = _FakeRedis(
        _source_events((0,))
        + [(DECISION_STREAM_KEY, _decision_entry(0))]
        + _source_events(range(1, 6))
    )

    report = _run_shadow(
        redis,
        CloseSignalStrategy("strategy", PRODUCT_ID),
        output,
        duration_seconds=2,
    )

    assert report.completed_candles == 1
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    record_types = [record["type"] for record in records]
    assert record_types.index("decision") > max(
        index
        for index, record_type in enumerate(record_types)
        if record_type == "source_candle"
    )

    decision = next(record for record in records if record["type"] == "decision")
    records.remove(decision)
    records.insert(2, decision)
    reordered = io.StringIO(
        "".join(
            json.dumps(record, sort_keys=True, separators=(",", ":")) + "\n"
            for record in records
        )
    )
    with pytest.raises(AssertionError, match="source watermark"):
        verify_shadow_evidence_bundle(
            reordered,
            strategy_factory=lambda: CloseSignalStrategy(
                "strategy",
                PRODUCT_ID,
            ),
        )


def test_shadow_rejects_decision_when_source_watermark_never_catches_up():
    redis = _FakeRedis(
        _source_events((0,))
        + [(DECISION_STREAM_KEY, _decision_entry(0))]
        + _source_events((1,))
    )

    with pytest.raises(ValueError, match="source watermark"):
        _run_shadow(
            redis,
            CloseSignalStrategy("strategy", PRODUCT_ID),
            io.StringIO(),
            duration_seconds=1,
        )


def test_shadow_skips_official_bucket_not_fully_covered_by_capture():
    output = io.StringIO()
    redis = _FakeRedis(
        _source_events(range(2, 11))
        + [
            (DECISION_STREAM_KEY, _decision_entry(0)),
            (DECISION_STREAM_KEY, _decision_entry(5)),
        ]
    )

    report = _run_shadow(
        redis,
        CloseSignalStrategy("strategy", PRODUCT_ID),
        output,
        duration_seconds=2,
    )

    assert report.skipped_decision_prefix_candles == 1
    assert report.completed_candles == 1
    output.seek(0)
    assert (
        verify_shadow_evidence_bundle(
            output,
            strategy_factory=lambda: CloseSignalStrategy(
                "strategy",
                PRODUCT_ID,
            ),
        )
        == report
    )
