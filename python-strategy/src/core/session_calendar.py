"""Trading-session calendars used to classify expected market-data gaps."""

from __future__ import annotations

import calendar
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from functools import lru_cache
from typing import Protocol, cast

import pandas_market_calendars as market_calendars


_CME_EQUITY = market_calendars.get_calendar("CME_Equity")
# https://www.cmegroup.com/notices/electronic-trading/2021/06/20210621.html
_CME_DAILY_PAUSE_REMOVED = date(2021, 6, 28)


class SessionCalendar(Protocol):
    def is_open(self, timestamp_ms: int) -> bool:
        """Return whether continuous matching is scheduled at this UTC instant."""

        ...

    def has_open_time(self, start_ms: int, end_ms: int) -> bool:
        """Return whether a half-open UTC interval contains matching time."""

        ...


@dataclass(frozen=True)
class SessionClosure:
    """One explicit half-open UTC closure interval."""

    start_ms: int
    end_ms: int

    def __post_init__(self) -> None:
        if self.end_ms <= self.start_ms:
            raise ValueError("session closure end must be after start")

    def contains(self, timestamp_ms: int) -> bool:
        return self.start_ms <= timestamp_ms < self.end_ms


@dataclass(frozen=True)
class CmeEquityIndexCalendar:
    """CME equity-index Globex matching hours plus caller-supplied closures.

    The packaged market calendar supplies historical holidays and early closes.
    CME's 15:15-15:30 CT pause was removed from daily sessions for trade date
    2021-06-28, but remains on the final trading day of each month.
    """

    closures: tuple[SessionClosure, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "closures",
            tuple(sorted(self.closures, key=lambda closure: closure.start_ms)),
        )

    def is_open(self, timestamp_ms: int) -> bool:
        if any(closure.contains(timestamp_ms) for closure in self.closures):
            return False

        utc_date = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).date()
        return any(
            start_ms <= timestamp_ms < end_ms
            for start_ms, end_ms in _cme_open_intervals(utc_date)
        )

    def has_open_time(self, start_ms: int, end_ms: int) -> bool:
        if end_ms <= start_ms:
            raise ValueError("session interval end must be after start")

        utc_date = datetime.fromtimestamp(start_ms / 1000, tz=UTC).date()
        last_date = datetime.fromtimestamp((end_ms - 1) / 1000, tz=UTC).date()
        while utc_date <= last_date:
            for open_ms, close_ms in _cme_open_intervals(utc_date):
                overlap_start = max(start_ms, open_ms)
                overlap_end = min(end_ms, close_ms)
                if overlap_start < overlap_end and _has_unclosed_time(
                    overlap_start, overlap_end, self.closures
                ):
                    return True
            utc_date += timedelta(days=1)
        return False


def _has_unclosed_time(
    start_ms: int, end_ms: int, closures: tuple[SessionClosure, ...]
) -> bool:
    cursor = start_ms
    for closure in closures:
        if closure.end_ms <= cursor:
            continue
        if closure.start_ms > cursor:
            return True
        cursor = max(cursor, closure.end_ms)
        if cursor >= end_ms:
            return False
    return cursor < end_ms


@lru_cache(maxsize=4096)
def _cme_open_intervals(utc_date: date) -> tuple[tuple[int, int], ...]:
    day_start = datetime.combine(utc_date, time(), tzinfo=UTC)
    day_end = day_start + timedelta(days=1)
    day_start_ms = _timestamp_ms(day_start)
    day_end_ms = _timestamp_ms(day_end)
    schedule = _CME_EQUITY.schedule(
        start_date=utc_date - timedelta(days=1),
        end_date=utc_date + timedelta(days=1),
    )

    intervals: list[tuple[int, int]] = []
    for session_date, session in schedule.iterrows():
        market_open = _timestamp_ms(cast(datetime, session["market_open"]))
        market_close = _timestamp_ms(cast(datetime, session["market_close"]))
        session_day = cast(datetime, session_date).date()
        ranges = [(market_open, market_close)]
        if session_day < _CME_DAILY_PAUSE_REMOVED or _is_month_end_session(
            session_day
        ):
            break_start = _timestamp_ms(cast(datetime, session["break_start"]))
            break_end = _timestamp_ms(cast(datetime, session["break_end"]))
            if market_open < break_start < break_end < market_close:
                ranges = [
                    (market_open, break_start),
                    (break_end, market_close),
                ]

        for start_ms, end_ms in ranges:
            clipped_start = max(start_ms, day_start_ms)
            clipped_end = min(end_ms, day_end_ms)
            if clipped_start < clipped_end:
                intervals.append((clipped_start, clipped_end))

    return tuple(sorted(set(intervals)))


@lru_cache(maxsize=512)
def _is_month_end_session(session_date: date) -> bool:
    month_end = session_date.replace(
        day=calendar.monthrange(session_date.year, session_date.month)[1]
    )
    if session_date == month_end:
        return True
    return _CME_EQUITY.schedule(
        start_date=session_date + timedelta(days=1),
        end_date=month_end,
    ).empty


def _timestamp_ms(value: datetime) -> int:
    return int(value.timestamp() * 1000)
