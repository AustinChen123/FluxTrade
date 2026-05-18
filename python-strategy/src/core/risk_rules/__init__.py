"""Independent risk rule primitives."""

from __future__ import annotations

from enum import Enum


class RuleStatus(str, Enum):
    """Result status returned by an individual risk rule."""

    PASS = "PASS"
    REJECT = "REJECT"
    CIRCUIT_BREAKER_TRIGGERED = "CIRCUIT_BREAKER_TRIGGERED"


__all__ = ["RuleStatus"]
