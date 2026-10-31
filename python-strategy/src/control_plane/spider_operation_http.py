"""Raw HTTP adapter for immutable Spider operation proposal/confirmation."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC
from typing import Any

from sqlalchemy.orm import Session

from src.core.backtest.spider_run_artifacts import decode_canonical
from src.core.runtime_environment import validate_environment_identity
from src.core.spider_management.operation_request import (
    MAX_OPERATION_REQUEST_BYTES,
    SpiderOperationRequest,
    SpiderOperationRequestConflict,
    SpiderOperationRequestError,
    SpiderOperationRequestIntegrityError,
    SpiderOperationRequestStale,
)
from src.core.spider_management.operation_request_store import (
    SpiderOperationReadbackStatus,
    SpiderOperationRequestStore,
)

SPIDER_OPERATION_REQUEST_PATH = "/api/v1/spider/operation-requests"
SPIDER_OPERATION_READBACK_PATH = "/api/v1/spider/operation-requests/readback"


class SpiderOperationHttpService:
    def __init__(
        self,
        sessions: Callable[[], AbstractContextManager[Session]],
        *,
        environment: str,
        execution_scope_id: str,
    ) -> None:
        self._environment = validate_environment_identity(environment)
        self._execution_scope_id = execution_scope_id
        self._sessions = sessions
        self._store = SpiderOperationRequestStore(sessions)

    def record(
        self,
        *,
        raw_query: str,
        body: str | bytes | None,
        actor: str,
        step_up_confirmed: bool = False,
    ) -> tuple[int, dict[str, Any]]:
        if raw_query:
            return self._error(422, "validation_error")
        request = self._parse(body)
        if isinstance(request, tuple):
            return request
        scope_error = self._scope_error(request)
        if scope_error is not None:
            return scope_error
        if request.actor != actor:
            return self._error(403, "ACTOR_MISMATCH")
        if request.request_kind == "CONFIRMATION" and not step_up_confirmed:
            return self._error(403, "step_up_required")
        try:
            with self._sessions() as session:
                with session.begin():
                    self._store.record_in_transaction(session, request)
        except SpiderOperationRequestConflict as error:
            return self._error(409, str(error))
        except SpiderOperationRequestStale as error:
            return self._error(409, str(error))
        except SpiderOperationRequestIntegrityError as error:
            return self._error(503, str(error))
        except SpiderOperationRequestError as error:
            return self._error(422, str(error))
        except Exception:
            return self._readback_response(request)
        return self._readback_response(request)

    def readback(
        self, *, raw_query: str, body: str | bytes | None
    ) -> tuple[int, dict[str, Any]]:
        if raw_query:
            return self._error(422, "validation_error")
        request = self._parse(body)
        if isinstance(request, tuple):
            return request
        scope_error = self._scope_error(request)
        if scope_error is not None:
            return scope_error
        return self._readback_response(request)

    @staticmethod
    def _parse(body: str | bytes | None) -> SpiderOperationRequest | tuple[int, dict[str, Any]]:
        if type(body) is str:
            try:
                raw = body.encode("utf-8", errors="strict")
            except UnicodeEncodeError:
                return SpiderOperationHttpService._error(422, "validation_error")
        elif type(body) is bytes:
            raw = body
        else:
            return SpiderOperationHttpService._error(422, "validation_error")
        if not 1 <= len(raw) <= MAX_OPERATION_REQUEST_BYTES:
            return SpiderOperationHttpService._error(422, "validation_error")
        try:
            return SpiderOperationRequest.from_canonical_bytes(raw)
        except SpiderOperationRequestError:
            return SpiderOperationHttpService._error(422, "validation_error")

    def _scope_error(self, request: SpiderOperationRequest) -> tuple[int, dict[str, Any]] | None:
        if (
            request.environment != self._environment
            or request.execution_scope_id != self._execution_scope_id
        ):
            return self._error(403, "SCOPE_MISMATCH")
        return None

    def _readback_response(self, request: SpiderOperationRequest) -> tuple[int, dict[str, Any]]:
        result = self._store.readback(request)
        status = result.status
        payload = {
            "schema_version": "spider_operation_http_v1",
            "request_id": request.request_id,
            "operation_id": request.operation_id,
            "operation_core_sha256": request.operation_core_sha256,
            "readback_status": status.value,
            "application_status": "COMMAND_SELECTION_REQUIRED",
            "record": None,
            "runtime": None,
        }
        if status is SpiderOperationReadbackStatus.CONFIRMED and result.record is not None:
            recorded_at = result.record.recorded_at.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
            payload["record"] = {
                "request": decode_canonical(result.record.request.canonical_request_bytes),
                "authorization_confirmed": result.record.authorization_confirmed,
                "recorded_at": recorded_at,
            }
            return 200, payload
        return {
            SpiderOperationReadbackStatus.NOT_FOUND: (404, payload),
            SpiderOperationReadbackStatus.UNKNOWN: (503, payload),
            SpiderOperationReadbackStatus.INVALID_EVIDENCE: (503, payload),
            SpiderOperationReadbackStatus.CONFIRMED: (503, payload),
        }[status]

    @staticmethod
    def _error(status: int, code: str) -> tuple[int, dict[str, Any]]:
        return status, {"error": code}


__all__ = [
    "SPIDER_OPERATION_READBACK_PATH", "SPIDER_OPERATION_REQUEST_PATH",
    "SpiderOperationHttpService",
]
