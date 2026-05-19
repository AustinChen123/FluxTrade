"""Data quality validation for candlestick data.

Checks for gaps, outliers, and OHLC relationship violations before
feeding data into backtesting or analytics pipelines.
"""

from dataclasses import dataclass, field
from typing import List
import numpy as np
from src.core.models import Candlestick


# Timeframe durations in milliseconds
_TF_MS = {
    "1m": 60_000,
    "3m": 180_000,
    "5m": 300_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "6h": 21_600_000,
    "8h": 28_800_000,
    "12h": 43_200_000,
    "1d": 86_400_000,
    "1w": 604_800_000,
}


@dataclass
class QualityIssue:
    """A single data quality issue."""

    severity: str  # "error" | "warning"
    category: str  # "gap" | "ohlc" | "outlier" | "general"
    index: int  # position in candle list (-1 for general)
    timestamp: int  # unix ms
    message: str


@dataclass
class QualityReport:
    """Aggregated data quality report."""

    total_candles: int = 0
    issues: List[QualityIssue] = field(default_factory=list)
    gap_count: int = 0
    ohlc_violation_count: int = 0
    outlier_count: int = 0

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")

    @property
    def is_clean(self) -> bool:
        return len(self.issues) == 0

    def summary(self) -> str:
        lines = [
            f"Candles: {self.total_candles}",
            f"Issues: {len(self.issues)} ({self.error_count} errors, {self.warning_count} warnings)",
            f"  Gaps: {self.gap_count}",
            f"  OHLC violations: {self.ohlc_violation_count}",
            f"  Outliers: {self.outlier_count}",
        ]
        return "\n".join(lines)


def check_gaps(
    candles: List[Candlestick],
    timeframe: str,
    tolerance: float = 1.5,
) -> List[QualityIssue]:
    """Detect gaps in candle timestamps.

    A gap is flagged when the interval between consecutive candles
    exceeds ``expected_interval * tolerance``.
    """
    if len(candles) < 2:
        return []

    expected_ms = _TF_MS.get(timeframe)
    if expected_ms is None:
        return [QualityIssue(
            severity="warning",
            category="general",
            index=-1,
            timestamp=0,
            message=f"Unknown timeframe '{timeframe}', cannot check gaps",
        )]

    threshold = expected_ms * tolerance
    issues: List[QualityIssue] = []

    for i in range(1, len(candles)):
        delta = candles[i].timestamp - candles[i - 1].timestamp
        if delta > threshold:
            missing = int(delta / expected_ms) - 1
            issues.append(QualityIssue(
                severity="warning",
                category="gap",
                index=i,
                timestamp=candles[i].timestamp,
                message=f"Gap of {delta}ms (~{missing} missing candles) between index {i-1} and {i}",
            ))

    return issues


def check_ohlc(candles: List[Candlestick]) -> List[QualityIssue]:
    """Validate OHLC relationships: high >= max(open, close), low <= min(open, close)."""
    issues: List[QualityIssue] = []

    for i, c in enumerate(candles):
        h = float(c.high)
        lo = float(c.low)
        o = float(c.open)
        cl = float(c.close)

        if h < o or h < cl:
            issues.append(QualityIssue(
                severity="error",
                category="ohlc",
                index=i,
                timestamp=c.timestamp,
                message=f"High ({h}) < Open ({o}) or Close ({cl})",
            ))
        if lo > o or lo > cl:
            issues.append(QualityIssue(
                severity="error",
                category="ohlc",
                index=i,
                timestamp=c.timestamp,
                message=f"Low ({lo}) > Open ({o}) or Close ({cl})",
            ))
        if h < lo:
            issues.append(QualityIssue(
                severity="error",
                category="ohlc",
                index=i,
                timestamp=c.timestamp,
                message=f"High ({h}) < Low ({lo})",
            ))
        if float(c.volume) < 0:
            issues.append(QualityIssue(
                severity="error",
                category="ohlc",
                index=i,
                timestamp=c.timestamp,
                message=f"Negative volume ({c.volume})",
            ))

    return issues


def check_outliers(
    candles: List[Candlestick],
    z_threshold: float = 4.0,
) -> List[QualityIssue]:
    """Detect price outliers using z-score on close-to-close returns."""
    if len(candles) < 3:
        return []

    closes = np.array([float(c.close) for c in candles])
    returns = np.diff(closes) / closes[:-1]

    mean = np.mean(returns)
    std = np.std(returns)
    if std == 0:
        return []

    z_scores = np.abs((returns - mean) / std)
    issues: List[QualityIssue] = []

    for i, z in enumerate(z_scores):
        if z > z_threshold:
            issues.append(QualityIssue(
                severity="warning",
                category="outlier",
                index=i + 1,
                timestamp=candles[i + 1].timestamp,
                message=f"Outlier: return={returns[i]:.4f}, z-score={z:.2f} (threshold={z_threshold})",
            ))

    return issues


def validate(
    candles: List[Candlestick],
    timeframe: str = "1m",
    *,
    z_threshold: float = 4.0,
    gap_tolerance: float = 1.5,
) -> QualityReport:
    """Run all quality checks and return an aggregated report."""
    report = QualityReport(total_candles=len(candles))

    if not candles:
        return report

    gap_issues = check_gaps(candles, timeframe, tolerance=gap_tolerance)
    ohlc_issues = check_ohlc(candles)
    outlier_issues = check_outliers(candles, z_threshold=z_threshold)

    report.gap_count = len(gap_issues)
    report.ohlc_violation_count = len(ohlc_issues)
    report.outlier_count = len(outlier_issues)
    report.issues = gap_issues + ohlc_issues + outlier_issues

    return report
