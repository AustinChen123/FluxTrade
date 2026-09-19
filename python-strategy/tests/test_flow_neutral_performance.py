from decimal import Decimal

import pytest

from src.core.backtest.external_funding import (
    ExternalFundingApplication,
    ExternalFundingEvent,
)
from src.core.backtest.flow_neutral_performance import (
    ANNUALIZATION_SECONDS,
    FlowNeutralPerformanceTracker,
)


DAY_MS = 86_400_000
YEAR_MS = ANNUALIZATION_SECONDS * 1000


def _application(
    *,
    event_id: str = "funding-1",
    amount: str = "100",
    timestamp: int = DAY_MS,
    pre_equity: str = "100",
    post_equity: str = "200",
) -> ExternalFundingApplication:
    event = ExternalFundingEvent(
        event_id=event_id,
        account_id="research-account",
        asset="USDT",
        amount=Decimal(amount),
        available_at=timestamp,
        source="test-schedule",
    )
    return ExternalFundingApplication(
        event=event,
        applied_at=timestamp,
        mark_price=Decimal("50000"),
        pre_equity=Decimal(pre_equity),
        post_equity=Decimal(post_equity),
        quote_total_before=Decimal(pre_equity),
        quote_total_after=Decimal(post_equity),
    )


def test_cash_only_contribution_changes_units_not_return():
    tracker = FlowNeutralPerformanceTracker(Decimal("100"))
    tracker.observe(timestamp=0, equity=Decimal("100"))
    tracker.record_funding(_application())
    tracker.observe(timestamp=DAY_MS, equity=Decimal("200"))

    report = tracker.report()

    assert report.initial_capital == Decimal("100")
    assert report.external_contributions == Decimal("100")
    assert report.total_contributed_capital == Decimal("200")
    assert report.final_equity == Decimal("200")
    assert report.net_pnl == Decimal("0")
    assert report.ending_units == Decimal("200")
    assert report.ending_nav == Decimal("1")
    assert report.time_weighted_return == Decimal("0")
    assert report.annualized_time_weighted_return == Decimal("0")
    assert report.annualization_seconds == ANNUALIZATION_SECONDS
    assert report.start_timestamp == 0
    assert report.end_timestamp == DAY_MS
    assert report.duration_milliseconds == DAY_MS
    assert report.unitized_max_drawdown == Decimal("0")
    assert report.raw_max_drawdown == Decimal("0")
    assert [sample.phase for sample in report.nav_samples] == [
        "valuation",
        "pre_flow",
        "post_flow",
    ]


def test_contribution_at_drawdown_preserves_nav_and_drawdown():
    tracker = FlowNeutralPerformanceTracker(Decimal("100"))
    tracker.observe(timestamp=0, equity=Decimal("100"))
    tracker.observe(timestamp=DAY_MS, equity=Decimal("80"))
    tracker.record_funding(
        _application(
            timestamp=DAY_MS,
            pre_equity="80",
            post_equity="180",
        )
    )
    tracker.observe(timestamp=2 * DAY_MS, equity=Decimal("180"))

    report = tracker.report()

    assert report.ending_units == Decimal("225")
    assert report.ending_nav == Decimal("0.8")
    assert report.time_weighted_return == Decimal("-0.2")
    assert report.unitized_max_drawdown == Decimal("0.2")
    assert report.raw_max_drawdown == Decimal("20")
    assert report.final_equity == Decimal("180")
    assert report.net_pnl == Decimal("-20")
    pre_flow, post_flow = report.nav_samples[2:4]
    assert pre_flow.nav == post_flow.nav == Decimal("0.8")
    assert pre_flow.units == Decimal("100")
    assert post_flow.units == Decimal("225")


def test_final_value_after_contribution_is_flow_neutral():
    tracker = FlowNeutralPerformanceTracker(Decimal("100"))
    tracker.observe(timestamp=0, equity=Decimal("100"))
    tracker.record_funding(_application())
    tracker.observe(timestamp=DAY_MS * 2, equity=Decimal("220"))

    report = tracker.report()

    assert report.final_equity == Decimal("220")
    assert report.net_pnl == Decimal("20")
    assert report.ending_nav == Decimal("1.1")
    assert report.time_weighted_return == Decimal("0.1")
    assert report.annualized_time_weighted_return is not None


def test_one_year_doubling_uses_explicit_365_25_day_annualization():
    tracker = FlowNeutralPerformanceTracker(Decimal("100"))
    tracker.observe(timestamp=0, equity=Decimal("100"))
    tracker.observe(timestamp=YEAR_MS, equity=Decimal("200"))

    report = tracker.report()

    assert report.time_weighted_return == Decimal("1")
    assert report.annualized_time_weighted_return == Decimal("1")
    assert report.duration_milliseconds == YEAR_MS


def test_zero_duration_does_not_fabricate_annualized_return():
    tracker = FlowNeutralPerformanceTracker(Decimal("100"))
    tracker.observe(timestamp=0, equity=Decimal("100"))

    report = tracker.report()

    assert report.annualized_time_weighted_return is None
    assert report.duration_milliseconds == 0


def test_missing_valuation_does_not_fabricate_endpoint_metrics():
    report = FlowNeutralPerformanceTracker(Decimal("100")).report()

    assert report.final_equity is None
    assert report.net_pnl is None
    assert report.ending_nav is None
    assert report.time_weighted_return is None
    assert report.annualized_time_weighted_return is None


@pytest.mark.parametrize("equity", [Decimal("0"), Decimal("-1"), Decimal("NaN")])
def test_non_positive_or_non_finite_equity_fails_closed(equity):
    tracker = FlowNeutralPerformanceTracker(Decimal("100"))

    with pytest.raises(ValueError, match="equity must be finite and positive"):
        tracker.observe(timestamp=0, equity=equity)


def test_duplicate_or_non_conserved_funding_fails_closed():
    tracker = FlowNeutralPerformanceTracker(Decimal("100"))
    application = _application()
    tracker.record_funding(application)

    with pytest.raises(ValueError, match="duplicate funding event"):
        tracker.record_funding(application)

    inconsistent = _application(
        event_id="funding-2",
        amount="100",
        timestamp=2 * DAY_MS,
        pre_equity="200",
        post_equity="299",
    )
    with pytest.raises(ValueError, match="application is not conserved"):
        tracker.record_funding(inconsistent)


def test_timestamp_regression_fails_closed():
    tracker = FlowNeutralPerformanceTracker(Decimal("100"))
    tracker.observe(timestamp=DAY_MS, equity=Decimal("100"))

    with pytest.raises(ValueError, match="timestamps must be non-decreasing"):
        tracker.observe(timestamp=DAY_MS - 1, equity=Decimal("100"))
