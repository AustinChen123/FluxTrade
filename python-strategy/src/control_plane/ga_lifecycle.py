"""Pure versioned lifecycle rules for profile-based GA jobs."""

from __future__ import annotations

import copy
import json
from dataclasses import dataclass, replace
from enum import Enum
from typing import Any, Mapping


class GaJobStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class GaCheckpointReference:
    epoch_id: str
    completed_generation: int


@dataclass(frozen=True)
class GaJobRecord:
    id: str
    status: GaJobStatus
    version: int
    request: dict[str, Any]
    epoch_id: str
    completed_generation: int = -1
    checkpoint: GaCheckpointReference | None = None
    retry_of_job_id: str | None = None
    error: str | None = None

    def detached_copy(self) -> GaJobRecord:
        return replace(self, request=copy.deepcopy(self.request))


class GaJobStoreError(ValueError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def new_ga_job_record(
    job_id: str,
    request: Mapping[str, Any],
    epoch_id: str,
    retry_of_job_id: str | None = None,
) -> GaJobRecord:
    if not isinstance(job_id, str) or not job_id:
        raise GaJobStoreError("validation_error")
    if not isinstance(epoch_id, str) or not epoch_id.strip():
        raise GaJobStoreError("validation_error")
    if retry_of_job_id is not None and (
        not isinstance(retry_of_job_id, str) or not retry_of_job_id.strip()
    ):
        raise GaJobStoreError("validation_error")
    try:
        preserved_request = json.loads(
            json.dumps(request, allow_nan=False, separators=(",", ":"))
        )
    except (TypeError, ValueError):
        raise GaJobStoreError("validation_error") from None
    if not isinstance(preserved_request, dict):
        raise GaJobStoreError("validation_error")
    evolution = preserved_request.get("evolution")
    if not isinstance(evolution, dict):
        raise GaJobStoreError("validation_error")
    max_generations = evolution.get("max_generations")
    if type(max_generations) is not int or max_generations < 1:
        raise GaJobStoreError("validation_error")
    request_epoch = evolution.get("epoch_id")
    if request_epoch is not None and request_epoch != epoch_id:
        raise GaJobStoreError("validation_error")
    return GaJobRecord(
        id=job_id,
        status=GaJobStatus.QUEUED,
        version=1,
        request=preserved_request,
        epoch_id=epoch_id,
        retry_of_job_id=retry_of_job_id,
    )


def transition_ga_job(
    current: GaJobRecord,
    action: str,
    expected_version: object,
    payload: Mapping[str, object] | None = None,
) -> GaJobRecord:
    """Validate then apply exactly one GA transition."""
    actions = {
        "claim",
        "pause",
        "resume",
        "cancel",
        "ack_generation",
        "finalize",
        "fail",
        "interrupt",
    }
    if not isinstance(action, str) or action not in actions:
        raise GaJobStoreError("validation_error")
    values = _validated_payload(action, expected_version, payload)
    if current.version != expected_version:
        raise GaJobStoreError("job_version_conflict")
    if action == "interrupt":
        if current.status not in {
            GaJobStatus.RUNNING,
            GaJobStatus.PAUSING,
            GaJobStatus.CANCELLING,
        }:
            raise GaJobStoreError("job_transition_invalid")
        return _advance(current, status=GaJobStatus.FAILED, error=values["error"])

    status = current.status
    if action == "claim" and status == GaJobStatus.QUEUED:
        return _advance(current, status=GaJobStatus.RUNNING, error=None)
    if action == "pause":
        if status == GaJobStatus.QUEUED:
            return _advance(current, status=GaJobStatus.PAUSED)
        if status == GaJobStatus.RUNNING:
            return _advance(current, status=GaJobStatus.PAUSING)
    if action == "resume" and status == GaJobStatus.PAUSED:
        return _advance(current, status=GaJobStatus.QUEUED, error=None)
    if action == "cancel":
        if status in {GaJobStatus.QUEUED, GaJobStatus.PAUSED}:
            return _advance(current, status=GaJobStatus.CANCELLED)
        if status in {GaJobStatus.RUNNING, GaJobStatus.PAUSING}:
            return _advance(current, status=GaJobStatus.CANCELLING)
    if action == "fail" and status in {
        GaJobStatus.RUNNING,
        GaJobStatus.PAUSING,
        GaJobStatus.CANCELLING,
    }:
        return _advance(current, status=GaJobStatus.FAILED, error=values["error"])
    if action == "ack_generation" and status in {
        GaJobStatus.RUNNING,
        GaJobStatus.PAUSING,
        GaJobStatus.CANCELLING,
    }:
        generation = values["completed_generation"]
        if generation != current.completed_generation + 1:
            raise GaJobStoreError("validation_error")
        if values["checkpoint_epoch_id"] != current.epoch_id:
            raise GaJobStoreError("validation_error")
        max_generations = current.request["evolution"]["max_generations"]
        if generation >= max_generations:
            raise GaJobStoreError("validation_error")
        checkpoint = GaCheckpointReference(current.epoch_id, generation)
        next_status = _after_generation(status, generation, max_generations)
        return _advance(
            current,
            status=next_status,
            completed_generation=generation,
            checkpoint=checkpoint,
            error=None if next_status != GaJobStatus.FAILED else current.error,
        )
    if action == "finalize" and status in {
        GaJobStatus.RUNNING,
        GaJobStatus.PAUSING,
        GaJobStatus.CANCELLING,
    }:
        max_generations = current.request["evolution"]["max_generations"]
        if current.completed_generation != max_generations - 1:
            raise GaJobStoreError("job_transition_invalid")
        return _advance(
            current,
            status=_after_generation(status, max_generations - 1, max_generations),
        )
    raise GaJobStoreError("job_transition_invalid")


def _validated_payload(
    action: str,
    expected_version: object,
    payload: Mapping[str, object] | None,
) -> dict[str, Any]:
    if type(expected_version) is not int or expected_version < 1:
        raise GaJobStoreError("validation_error")
    if payload is None:
        values: dict[str, object] = {}
    elif isinstance(payload, Mapping):
        values = dict(payload)
    else:
        raise GaJobStoreError("validation_error")
    expected_fields = {
        "ack_generation": {"completed_generation", "checkpoint_epoch_id"},
        "fail": {"error"},
        "interrupt": {"error"},
    }.get(action, set())
    if set(values) != expected_fields:
        raise GaJobStoreError("validation_error")
    if action == "ack_generation":
        generation = values["completed_generation"]
        checkpoint_epoch_id = values["checkpoint_epoch_id"]
        if type(generation) is not int or generation < 0:
            raise GaJobStoreError("validation_error")
        if not isinstance(checkpoint_epoch_id, str) or not checkpoint_epoch_id.strip():
            raise GaJobStoreError("validation_error")
    if action in {"fail", "interrupt"}:
        error = values["error"]
        if not isinstance(error, str) or not error.strip():
            raise GaJobStoreError("validation_error")
    return values


def _after_generation(
    status: GaJobStatus,
    generation: int,
    max_generations: int,
) -> GaJobStatus:
    if status == GaJobStatus.PAUSING:
        return GaJobStatus.PAUSED
    if status == GaJobStatus.CANCELLING:
        return GaJobStatus.CANCELLED
    if generation == max_generations - 1:
        return GaJobStatus.SUCCEEDED
    return GaJobStatus.RUNNING


def _advance(current: GaJobRecord, **changes: Any) -> GaJobRecord:
    return replace(
        current,
        version=current.version + 1,
        request=copy.deepcopy(current.request),
        **changes,
    )
