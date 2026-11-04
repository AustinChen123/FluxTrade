"""Pure error identities and classification for historical spider execution."""

from copy import deepcopy
from dataclasses import dataclass
from typing import Literal

_OMITTED_REASON = object()


class SpiderRunStoreError(Exception):
    def __init__(self, reason: str, requested_failure_reason: str | None = None, primary_failure: dict | None = None):
        super().__init__(reason)
        self.reason = reason
        self.requested_failure_reason = requested_failure_reason
        self.primary_failure = deepcopy(primary_failure)


SpiderRunStoreError.__module__ = "src.core.backtest.spider_run_store"


class ReconciliationProjectionError(ValueError):
    """Required evidence cannot be represented by the frozen eleven shapes."""

    reason = "ENDPOINT_RECONCILIATION_FAILED"

    def __init__(self, reason: object = _OMITTED_REASON) -> None:
        super().__init__(self.reason if reason is _OMITTED_REASON else reason)


ReconciliationProjectionError.__module__ = "src.core.backtest.spider_run_evidence"


@dataclass(frozen=True)
class HistoricalFailure:
    """Pure callback or outer-invocation classification; no publication authority."""

    classification: str
    reason: str
    primary_failure: dict | None
    closed: bool
    handled: bool
    propagate_original: bool


def classify_historical_failure(
    error: Exception,
    phase: Literal["CALLBACK_PERSIST", "OUTER_INVOCATION", "INPUT_REJECTION"],
    native_failure: dict | None,
    closed: bool,
) -> HistoricalFailure:
    """Map established historical failure cases without I/O or durable-state reads."""
    if phase == "CALLBACK_PERSIST":
        primary = (
            error.primary_failure if isinstance(error, SpiderRunStoreError)
            else dict(kind="PERSISTENCE", reason="EVIDENCE_CAPTURE_FAILED")
        )
        if isinstance(error, SpiderRunStoreError) and error.reason == "PUBLICATION_DURABILITY_UNKNOWN":
            return HistoricalFailure("PERSISTENCE_FAILED", error.reason, primary, True, True, True)
        return HistoricalFailure("PERSISTENCE_FAILED", "PERSISTENCE_FAILED", primary, True, True, False)
    if phase == "OUTER_INVOCATION":
        if native_failure is not None:
            return HistoricalFailure("NATIVE_FAULT", "NATIVE_FAULT", dict(kind="NATIVE", reason=native_failure["reason"]), closed, True, False)
        if isinstance(error, SpiderRunStoreError):
            classification = "DURABILITY_UNKNOWN" if error.reason == "PUBLICATION_DURABILITY_UNKNOWN" else "PERSISTENCE_FAILED"
            return HistoricalFailure(classification, error.reason, error.primary_failure, closed, True, False)
        if isinstance(error, ReconciliationProjectionError):
            return HistoricalFailure("ENDPOINT_RECONCILIATION_FAILED", "ENDPOINT_RECONCILIATION_FAILED", None, closed, True, False)
        return HistoricalFailure("UNEXPECTED_EXCEPTION", "UNEXPECTED_EXCEPTION", None, closed, False, False)
    if phase == "INPUT_REJECTION":
        if isinstance(error, (OSError, ValueError, TypeError, KeyError)):
            return HistoricalFailure("UNSUPPORTED_CONFIGURATION", "UNSUPPORTED_CONFIGURATION", None, False, True, False)
        return HistoricalFailure("UNEXPECTED_EXCEPTION", "UNEXPECTED_EXCEPTION", None, closed, False, False)
    raise ValueError("INVALID_HISTORICAL_FAILURE_PHASE")
