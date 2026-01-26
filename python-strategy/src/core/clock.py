from typing import Protocol
import time

class Clock(Protocol):
    def now(self) -> float:
        """Return the current epoch timestamp in seconds (float)."""
        ...

class RealtimeClock:
    def now(self) -> float:
        return time.time()

class BacktestClock:
    def __init__(self, start_time: float = 0.0):
        self._current_time = start_time

    def set_time(self, timestamp: float):
        self._current_time = timestamp

    def now(self) -> float:
        return self._current_time
