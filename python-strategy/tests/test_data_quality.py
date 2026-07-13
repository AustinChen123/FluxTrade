"""Tests for data_quality.py — gap detection, OHLC validation, outlier detection."""

from datetime import UTC, datetime
from decimal import Decimal

import pytest
from src.core.data_quality import (
    check_gaps,
    check_ohlc,
    check_outliers,
    validate,
    QualityReport,
)
from src.core.models import Candlestick
from src.core.session_calendar import CmeEquityIndexCalendar, SessionClosure


# ── Helpers ──────────────────────────────────────────────────────

def _timestamp_ms(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=UTC).timestamp() * 1000)


def _candle(
    ts: int,
    o: float = 100.0,
    h: float = 105.0,
    lo: float = 95.0,
    c: float = 102.0,
    v: float = 1000.0,
) -> Candlestick:
    return Candlestick(
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        timestamp=ts,
        open=Decimal(str(o)),
        high=Decimal(str(h)),
        low=Decimal(str(lo)),
        close=Decimal(str(c)),
        volume=Decimal(str(v)),
    )


def _series(count: int = 10, start: int = 0, interval: int = 60_000) -> list[Candlestick]:
    """Create a clean series of candles."""
    return [_candle(start + i * interval) for i in range(count)]


# ── Gap detection ────────────────────────────────────────────────

class TestCheckGaps:
    def test_no_gaps(self):
        candles = _series(5)
        issues = check_gaps(candles, "1m")
        assert len(issues) == 0

    def test_single_gap(self):
        candles = [
            _candle(0),
            _candle(60_000),
            _candle(240_000),  # 3-minute gap
        ]
        issues = check_gaps(candles, "1m")
        assert len(issues) == 1
        assert issues[0].category == "gap"
        assert "~2 missing candles" in issues[0].message

    def test_non_aligned_gap_counts_each_expected_timestamp(self):
        candles = [_candle(0), _candle(100_000)]

        issues = check_gaps(candles, "1m")

        assert len(issues) == 1
        assert "~1 missing candles" in issues[0].message

    def test_multiple_gaps(self):
        candles = [
            _candle(0),
            _candle(180_000),  # gap
            _candle(360_000),  # gap
        ]
        issues = check_gaps(candles, "1m")
        assert len(issues) == 2

    def test_tolerance(self):
        # 1.5x tolerance means 90_000ms is still OK for 1m candles
        candles = [_candle(0), _candle(80_000)]
        issues = check_gaps(candles, "1m")
        assert len(issues) == 0

    @pytest.mark.parametrize(
        "session_calendar",
        [None, CmeEquityIndexCalendar()],
    )
    def test_low_tolerance_reports_sub_interval_violation(self, session_calendar):
        candles = [_candle(0), _candle(45_000)]

        issues = check_gaps(
            candles,
            "1m",
            tolerance=0.5,
            session_calendar=session_calendar,
        )

        assert len(issues) == 1
        assert issues[0].category == "gap"

    def test_unknown_timeframe(self):
        candles = _series(3)
        issues = check_gaps(candles, "7m")
        assert len(issues) == 1
        assert issues[0].category == "general"

    def test_empty_candles(self):
        assert check_gaps([], "1m") == []

    def test_single_candle(self):
        assert check_gaps([_candle(0)], "1m") == []

    def test_5m_timeframe(self):
        candles = [
            _candle(0),
            _candle(300_000),
            _candle(600_000),
        ]
        issues = check_gaps(candles, "5m")
        assert len(issues) == 0

    @pytest.mark.parametrize(
        ("before_close", "after_reopen"),
        [
            ("2026-01-05T21:59:00", "2026-01-05T23:00:00"),
            ("2026-07-06T20:59:00", "2026-07-06T22:00:00"),
            ("2026-07-10T20:59:00", "2026-07-12T22:00:00"),
        ],
    )
    def test_cme_scheduled_closures_are_not_gaps(self, before_close, after_reopen):
        candles = [_candle(_timestamp_ms(before_close)), _candle(_timestamp_ms(after_reopen))]

        issues = check_gaps(
            candles,
            "1m",
            session_calendar=CmeEquityIndexCalendar(),
        )

        assert issues == []

    def test_cme_missing_open_minute_remains_a_gap(self):
        candles = [
            _candle(_timestamp_ms("2026-07-06T22:00:00")),
            _candle(_timestamp_ms("2026-07-06T22:02:00")),
        ]

        issues = check_gaps(
            candles,
            "1m",
            session_calendar=CmeEquityIndexCalendar(),
        )

        assert len(issues) == 1
        assert "~1 missing candles" in issues[0].message

    def test_cme_gap_spanning_maintenance_and_open_time_is_reported(self):
        candles = [
            _candle(_timestamp_ms("2026-07-06T20:59:00")),
            _candle(_timestamp_ms("2026-07-06T22:02:00")),
        ]

        issues = check_gaps(
            candles,
            "1m",
            session_calendar=CmeEquityIndexCalendar(),
        )

        assert len(issues) == 1
        assert "~2 missing candles" in issues[0].message

    @pytest.mark.parametrize(
        ("timeframe", "before_close", "after_reopen"),
        [
            ("1m", "2026-07-05T20:59:20", "2026-07-05T21:59:40"),
            ("2h", "2026-03-06T21:00:00", "2026-03-08T22:00:00"),
        ],
    )
    def test_cme_candidate_interval_stops_at_next_observed_candle(
        self,
        timeframe,
        before_close,
        after_reopen,
    ):
        candles = [
            _candle(_timestamp_ms(before_close)),
            _candle(_timestamp_ms(after_reopen)),
        ]

        issues = check_gaps(
            candles,
            timeframe,
            session_calendar=CmeEquityIndexCalendar(),
        )

        assert issues == []

    @pytest.mark.parametrize(
        ("timeframe", "before_gap", "after_gap", "missing"),
        [
            ("2h", "2026-01-05T20:00:00", "2026-01-06T00:00:00", 1),
            ("2h", "2026-01-09T18:00:00", "2026-01-09T22:00:00", 1),
            ("1d", "2026-01-10T00:00:00", "2026-01-12T00:00:00", 1),
        ],
    )
    def test_cme_high_timeframe_bucket_with_open_time_is_a_gap(
        self,
        timeframe,
        before_gap,
        after_gap,
        missing,
    ):
        candles = [
            _candle(_timestamp_ms(before_gap)),
            _candle(_timestamp_ms(after_gap)),
        ]

        issues = check_gaps(
            candles,
            timeframe,
            session_calendar=CmeEquityIndexCalendar(),
        )

        assert len(issues) == 1
        assert f"~{missing} missing candles" in issues[0].message

    def test_cme_high_timeframe_bucket_fully_closed_is_not_a_gap(self):
        candles = [
            _candle(_timestamp_ms("2026-01-09T20:00:00")),
            _candle(_timestamp_ms("2026-01-10T02:00:00")),
        ]

        issues = check_gaps(
            candles,
            "2h",
            session_calendar=CmeEquityIndexCalendar(),
        )

        assert issues == []

    def test_explicit_holiday_closure_is_not_a_gap(self):
        closure = SessionClosure(
            _timestamp_ms("2026-07-03T17:00:00"),
            _timestamp_ms("2026-07-05T22:00:00"),
        )
        candles = [
            _candle(_timestamp_ms("2026-07-03T16:59:00")),
            _candle(_timestamp_ms("2026-07-05T22:00:00")),
        ]

        issues = check_gaps(
            candles,
            "1m",
            session_calendar=CmeEquityIndexCalendar(closures=(closure,)),
        )

        assert issues == []


# ── OHLC validation ─────────────────────────────────────────────

class TestCheckOhlc:
    def test_valid_ohlc(self):
        candles = _series(5)
        issues = check_ohlc(candles)
        assert len(issues) == 0

    def test_high_below_open(self):
        c = _candle(0, o=100, h=99, lo=95, c=98)
        issues = check_ohlc([c])
        assert len(issues) >= 1
        assert any("High" in i.message for i in issues)

    def test_high_below_close(self):
        c = _candle(0, o=95, h=99, lo=90, c=100)
        issues = check_ohlc([c])
        assert len(issues) >= 1

    def test_low_above_open(self):
        c = _candle(0, o=100, h=110, lo=101, c=105)
        issues = check_ohlc([c])
        assert len(issues) >= 1
        assert any("Low" in i.message for i in issues)

    def test_low_above_close(self):
        c = _candle(0, o=105, h=110, lo=104, c=103)
        issues = check_ohlc([c])
        assert len(issues) >= 1

    def test_high_below_low(self):
        c = _candle(0, o=100, h=90, lo=95, c=92)
        issues = check_ohlc([c])
        assert any("High" in i.message and "Low" in i.message for i in issues)

    def test_negative_volume(self):
        c = _candle(0, v=-100)
        issues = check_ohlc([c])
        assert len(issues) == 1
        assert "volume" in issues[0].message.lower()

    def test_empty_candles(self):
        assert check_ohlc([]) == []


# ── Outlier detection ────────────────────────────────────────────

class TestCheckOutliers:
    def test_no_outliers(self):
        candles = _series(20)
        issues = check_outliers(candles)
        assert len(issues) == 0

    def test_spike_detected(self):
        candles = _series(20)
        # Inject extreme spike
        candles[10] = _candle(
            candles[10].timestamp,
            o=100, h=500, lo=95, c=500,
        )
        issues = check_outliers(candles, z_threshold=3.0)
        assert len(issues) >= 1
        assert issues[0].category == "outlier"

    def test_custom_threshold(self):
        candles = _series(20)
        candles[10] = _candle(candles[10].timestamp, o=100, h=200, lo=95, c=200)
        strict = check_outliers(candles, z_threshold=2.0)
        loose = check_outliers(candles, z_threshold=10.0)
        assert len(strict) >= len(loose)

    def test_too_few_candles(self):
        assert check_outliers([_candle(0), _candle(60_000)]) == []

    def test_constant_price_no_outlier(self):
        candles = [_candle(i * 60_000, o=100, h=100, lo=100, c=100) for i in range(20)]
        issues = check_outliers(candles)
        assert len(issues) == 0


# ── Aggregate validate() ────────────────────────────────────────

class TestValidate:
    def test_clean_data(self):
        candles = _series(10)
        report = validate(candles, "1m")
        assert report.is_clean
        assert report.total_candles == 10
        assert report.error_count == 0
        assert report.warning_count == 0

    def test_mixed_issues(self):
        candles = [
            _candle(0),
            _candle(180_000),  # gap
            _candle(240_000, o=100, h=90, lo=95, c=92),  # OHLC violation
        ]
        report = validate(candles, "1m")
        assert not report.is_clean
        assert report.gap_count >= 1
        assert report.ohlc_violation_count >= 1

    def test_empty_data(self):
        report = validate([], "1m")
        assert report.is_clean
        assert report.total_candles == 0

    def test_summary_output(self):
        candles = _series(5)
        report = validate(candles, "1m")
        summary = report.summary()
        assert "Candles: 5" in summary
        assert "Issues: 0" in summary

    def test_custom_thresholds(self):
        candles = _series(10)
        report = validate(candles, "1m", z_threshold=2.0, gap_tolerance=1.0)
        assert isinstance(report, QualityReport)

    def test_session_calendar_is_applied_to_gap_validation(self):
        candles = [
            _candle(_timestamp_ms("2026-07-06T20:59:00")),
            _candle(_timestamp_ms("2026-07-06T22:00:00")),
        ]

        report = validate(
            candles,
            "1m",
            session_calendar=CmeEquityIndexCalendar(),
        )

        assert report.gap_count == 0
