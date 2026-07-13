"""Trading-session calendars used to classify expected market-data gaps."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, time
from typing import Protocol
from zoneinfo import ZoneInfo


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
    """CME equity-index Globex hours with caller-supplied holiday closures.

    Regular hours are Sunday 17:00 through Friday 16:00 America/Chicago,
    with a daily maintenance break from 16:00 to 17:00.
    """

    closures: tuple[SessionClosure, ...] = ()

    def is_open(self, timestamp_ms: int) -> bool:
        if any(closure.contains(timestamp_ms) for closure in self.closures):
            return False

        local = datetime.fromtimestamp(timestamp_ms / 1000, tz=UTC).astimezone(
            ZoneInfo("America/Chicago")
        )
        weekday = local.weekday()
        local_time = local.time().replace(tzinfo=None)
        session_open = time(17)
        session_close = time(16)

        if weekday == 5:
            return False
        if weekday == 6:
            return local_time >= session_open
        if weekday == 4:
            return local_time < session_close
        return local_time < session_close or local_time >= session_open

    def has_open_time(self, start_ms: int, end_ms: int) -> bool:
        if end_ms <= start_ms:
            raise ValueError("session interval end must be after start")

        if any(
            start_ms <= closure.end_ms < end_ms
            and self.is_open(closure.end_ms)
            for closure in self.closures
        ):
            return True

        timestamp = start_ms
        while timestamp < end_ms:
            if self.is_open(timestamp):
                return True
            timestamp = ((timestamp // 60_000) + 1) * 60_000
        return self.is_open(end_ms - 1)
