"""Serialize Rithmic operations that replace the order-event worker."""

from __future__ import annotations

from collections.abc import Callable
from threading import RLock
from typing import ParamSpec, TypeVar

_P = ParamSpec("_P")
_R = TypeVar("_R")


class RithmicOrderEventLifecycleGate:
    """Own mutual exclusion across complete venue worker lifecycles."""

    def __init__(self) -> None:
        self._lock = RLock()

    def run(
        self, operation: Callable[_P, _R], *args: _P.args, **kwargs: _P.kwargs
    ) -> _R:
        with self._lock:
            return operation(*args, **kwargs)
