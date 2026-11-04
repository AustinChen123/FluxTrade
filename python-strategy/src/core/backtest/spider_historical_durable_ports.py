"""Durable implementation of historical checkpoint and evidence ports."""

from typing import Any, Protocol

from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_evidence import build_endpoint_artifacts
from src.core.backtest.spider_run_store import SpiderRunStore


class ExecutionCheckpointPort(Protocol):
    def checkpoint(self, boundary: dict[str, Any], kind: str) -> None: ...


class EvidenceSink(Protocol):
    def persist(self, row: dict[str, Any]) -> None: ...
    def retain(self, row: dict[str, Any]) -> None: ...
    def fail(self, classification: str, reason: str, primary: dict[str, Any] | None,
             last_processed_boundary: dict[str, Any] | None,
             last_fully_recorded_row_ordinal: int | None) -> None: ...
    def finish(self, initial_owner_evidence: dict[str, Any], final_owner_evidence: dict[str, Any],
               final_scheduler_observation: dict[str, Any], terminal: str,
               normalized_context: dict[str, Any]) -> str | None: ...


class DurableHistoricalOutputAdapter:
    """Preserve the existing store calls at the shared loop's durable ports."""

    def __init__(self, store: SpiderRunStore, attempt: dict[str, Any], run: Any, journal: list[dict[str, Any]]):
        self._store = store
        self._attempt = attempt
        self._run = run
        self._journal = journal

    def checkpoint(self, boundary: dict[str, Any], kind: str) -> None:
        self._store.mark_processed(boundary, record_kind=kind)

    def persist(self, row: dict[str, Any]) -> None:
        self._store.append_journal(row)

    def retain(self, row: dict[str, Any]) -> None:
        self._journal.append(row)

    def fail(self, classification: str, reason: str, primary: dict[str, Any] | None,
             last_processed_boundary: dict[str, Any] | None,
             last_fully_recorded_row_ordinal: int | None) -> None:
        self._store.publish_failure(reason, primary)

    def finish(self, initial_owner_evidence: dict[str, Any], final_owner_evidence: dict[str, Any],
               final_scheduler_observation: dict[str, Any], terminal: str,
               normalized_context: dict[str, Any]) -> str | None:
        attempt = dict(self._attempt)
        attempt["terminal_policy"] = terminal
        attempt["configuration_context"] = normalized_context["configuration_context"]
        attempt["historical_context"] = normalized_context["historical_context"]
        artifacts = build_endpoint_artifacts(
            self._run.run_id, attempt, self._store._status("RUNNING"), self._journal,
            initial_owner_evidence, final_owner_evidence, final_scheduler_observation,
        )
        if self._store.finalize(**artifacts) != "COMPLETE_PUBLISHED":
            raise RuntimeError("INVALID_STORE_ACKNOWLEDGEMENT")
        admitted = admit_spider_run(self._store._path)
        return None if admitted["decision"] == "ACCEPT" else admitted["reason"]
