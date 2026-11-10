"""Store-free ports shared by durable and research historical execution."""

from typing import Any, Protocol


class ExecutionCheckpointPort(Protocol):
    def checkpoint(self, boundary: dict[str, Any], kind: str) -> None: ...


class EvidenceSink(ExecutionCheckpointPort, Protocol):
    failure_result: Any

    def note_boundary(self, boundary: dict[str, Any], kind: str) -> None: ...
    def persist(self, row: dict[str, Any]) -> None: ...
    def retain(self, row: dict[str, Any]) -> None: ...
    def fail(self, classification: str, reason: str, primary: dict[str, Any] | None,
             last_processed_boundary: dict[str, Any] | None,
             last_fully_recorded_row_ordinal: int | None) -> Any: ...
    def finish(self, initial_owner_evidence: dict[str, Any], final_owner_evidence: dict[str, Any],
               final_scheduler_observation: dict[str, Any], terminal: str,
               normalized_context: dict[str, Any]) -> Any: ...
