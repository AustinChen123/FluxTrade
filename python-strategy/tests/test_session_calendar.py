"""Session-calendar boundary tests."""

from datetime import UTC, datetime

import pytest

from src.core.session_calendar import CmeEquityIndexCalendar, SessionClosure


def _timestamp_ms(value: str) -> int:
    return int(datetime.fromisoformat(value).replace(tzinfo=UTC).timestamp() * 1000)


@pytest.mark.parametrize(
    ("utc_timestamp", "expected"),
    [
        ("2026-01-04T22:59:00", False),  # Sunday before 17:00 CT
        ("2026-01-04T23:00:00", True),
        ("2026-01-05T21:59:00", True),
        ("2026-01-05T22:00:00", False),
        ("2026-01-05T22:59:00", False),
        ("2026-01-05T23:00:00", True),
        ("2026-01-09T21:59:00", True),
        ("2026-01-09T22:00:00", False),
        ("2026-01-10T18:00:00", False),
        ("2026-01-11T23:00:00", True),
    ],
)
def test_cme_equity_index_weekly_session_matrix(utc_timestamp, expected):
    calendar = CmeEquityIndexCalendar()

    assert calendar.is_open(_timestamp_ms(utc_timestamp)) is expected


@pytest.mark.parametrize(
    ("closed_timestamp", "reopen_timestamp"),
    [
        ("2026-01-05T22:00:00", "2026-01-05T23:00:00"),
        ("2026-07-06T21:00:00", "2026-07-06T22:00:00"),
    ],
)
def test_cme_maintenance_tracks_chicago_dst(closed_timestamp, reopen_timestamp):
    calendar = CmeEquityIndexCalendar()

    assert not calendar.is_open(_timestamp_ms(closed_timestamp))
    assert calendar.is_open(_timestamp_ms(reopen_timestamp))


def test_explicit_closure_overrides_regular_session():
    closure = SessionClosure(
        _timestamp_ms("2026-07-03T17:00:00"),
        _timestamp_ms("2026-07-05T22:00:00"),
    )
    calendar = CmeEquityIndexCalendar(closures=(closure,))

    assert not calendar.is_open(_timestamp_ms("2026-07-03T18:00:00"))


def test_invalid_closure_is_rejected():
    with pytest.raises(ValueError, match="end must be after start"):
        SessionClosure(1000, 1000)


@pytest.mark.parametrize(
    ("start", "end", "expected"),
    [
        ("2026-01-05T22:00:00", "2026-01-06T00:00:00", True),
        ("2026-01-05T22:00:00", "2026-01-05T23:00:00", False),
        ("2026-01-05T21:00:00", "2026-01-05T23:00:00", True),
        ("2026-01-10T00:00:00", "2026-01-11T00:00:00", False),
        ("2026-01-11T00:00:00", "2026-01-12T00:00:00", True),
    ],
)
def test_cme_interval_open_time_matrix(start, end, expected):
    calendar = CmeEquityIndexCalendar()

    assert calendar.has_open_time(_timestamp_ms(start), _timestamp_ms(end)) is expected


def test_explicit_closure_can_cover_entire_open_interval():
    start = _timestamp_ms("2026-01-05T23:00:00")
    end = _timestamp_ms("2026-01-06T01:00:00")
    calendar = CmeEquityIndexCalendar(closures=(SessionClosure(start, end),))

    assert not calendar.has_open_time(start, end)


@pytest.mark.parametrize(
    ("closure_offsets", "expected"),
    [
        (((0, 20_000), (40_000, 60_000)), True),
        (((0, 30_000), (30_000, 60_000)), False),
        (((0, 40_000), (20_000, 60_000)), False),
        (((0, 50_000),), False),
    ],
)
def test_subminute_closure_interval_matrix(closure_offsets, expected):
    base = _timestamp_ms("2026-01-06T00:00:00")
    closures = tuple(
        SessionClosure(base + start_offset, base + end_offset)
        for start_offset, end_offset in closure_offsets
    )
    calendar = CmeEquityIndexCalendar(closures=closures)

    assert calendar.has_open_time(base + 10_000, base + 50_000) is expected


def test_invalid_session_interval_is_rejected():
    calendar = CmeEquityIndexCalendar()

    with pytest.raises(ValueError, match="end must be after start"):
        calendar.has_open_time(1000, 1000)
