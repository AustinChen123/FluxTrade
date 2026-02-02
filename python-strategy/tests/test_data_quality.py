"""Tests for data_quality.py — gap detection, OHLC validation, outlier detection."""

from decimal import Decimal
from src.core.data_quality import (
    check_gaps,
    check_ohlc,
    check_outliers,
    validate,
    QualityReport,
)
from src.core.models import Candlestick


# ── Helpers ──────────────────────────────────────────────────────

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
        assert "missing" in issues[0].message.lower()

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
