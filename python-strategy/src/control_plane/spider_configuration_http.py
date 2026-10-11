"""HTTP boundary for immutable Spider configuration request evidence."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import AbstractContextManager
from datetime import UTC
from typing import Any

from sqlalchemy.orm import Session

from src.core.runtime_environment import validate_environment_identity
from src.core.spider_management.configuration_request import (
    MAX_REQUEST_BYTES,
    SpiderConfigurationReadbackStatus,
    SpiderConfigurationRequest,
    SpiderConfigurationRequestConflict,
    SpiderConfigurationRequestError,
    SpiderConfigurationRequestIntegrityError,
    SpiderConfigurationRequestStale,
    _identifier,
)
from src.core.spider_management.configuration_request_store import (
    SpiderConfigurationRequestStore,
)


SPIDER_CONFIGURATION_REQUEST_PATH = "/api/v1/spider/configuration-requests"
SPIDER_CONFIGURATION_READBACK_PATH = (
    "/api/v1/spider/configuration-requests/readback"
)
SPIDER_CONFIGURATION_SCOPE_EVIDENCE = "OPERATOR_CONFIGURED_NOT_PROVIDER_VERIFIED"


class SpiderConfigurationHttpService:
    """Translate the frozen HTTP contract into the accepted metadata store."""

    def __init__(
        self,
        sessions: Callable[[], AbstractContextManager[Session]],
        *,
        environment: str,
        execution_scope_id: str,
    ) -> None:
        try:
            self._environment = validate_environment_identity(environment)
            self._execution_scope_id = _identifier(execution_scope_id)
        except (TypeError, ValueError):
            raise ValueError("spider_configuration_scope_invalid") from None
        self._sessions = sessions
        self._store = SpiderConfigurationRequestStore(sessions)

    def record(
        self,
        *,
        raw_query: str,
        body: str | bytes | None,
        actor: str,
    ) -> tuple[int, dict[str, Any]]:
        if raw_query:
            return self._error(422, "validation_error")
        request = self._parse_request(body)
        if isinstance(request, tuple):
            return request
        scope_error = self._validate_scope(request)
        if scope_error is not None:
            return scope_error
        if request.actor != actor:
            return self._error(403, "ACTOR_MISMATCH")

        try:
            with self._sessions() as session:
                with session.begin():
                    self._store.record_in_transaction(session, request)
        except SpiderConfigurationRequestConflict as error:
            return self._error(409, str(error))
        except SpiderConfigurationRequestStale as error:
            return self._error(409, str(error))
        except SpiderConfigurationRequestIntegrityError as error:
            return self._error(503, str(error))
        except SpiderConfigurationRequestError as error:
            return self._error(422, str(error))
        except Exception:
            # A failed COMMIT may have succeeded. Resolve once using the exact
            # same immutable request; never retry the write or mint a new key.
            return self._readback_response(request)
        return self._readback_response(request)

    def readback(
        self,
        *,
        raw_query: str,
        body: str | bytes | None,
    ) -> tuple[int, dict[str, Any]]:
        if raw_query:
            return self._error(422, "validation_error")
        request = self._parse_request(body)
        if isinstance(request, tuple):
            return request
        scope_error = self._validate_scope(request)
        if scope_error is not None:
            return scope_error
        return self._readback_response(request)

    @staticmethod
    def _parse_request(
        body: str | bytes | None,
    ) -> SpiderConfigurationRequest | tuple[int, dict[str, Any]]:
        if type(body) is str:
            try:
                raw = body.encode("utf-8", errors="strict")
            except UnicodeEncodeError:
                return SpiderConfigurationHttpService._error(422, "validation_error")
        elif type(body) is bytes:
            raw = body
        else:
            return SpiderConfigurationHttpService._error(422, "validation_error")
        if not 1 <= len(raw) <= MAX_REQUEST_BYTES:
            return SpiderConfigurationHttpService._error(422, "validation_error")
        try:
            return SpiderConfigurationRequest.from_canonical_bytes(raw)
        except SpiderConfigurationRequestError:
            return SpiderConfigurationHttpService._error(422, "validation_error")

    def _validate_scope(
        self, request: SpiderConfigurationRequest
    ) -> tuple[int, dict[str, Any]] | None:
        if (
            request.environment != self._environment
            or request.execution_scope_id != self._execution_scope_id
        ):
            return self._error(403, "SCOPE_MISMATCH")
        return None

    def _readback_response(
        self, request: SpiderConfigurationRequest
    ) -> tuple[int, dict[str, Any]]:
        try:
            result = self._store.readback(request)
        except Exception:
            result = None
        if result is None:
            status = SpiderConfigurationReadbackStatus.UNKNOWN
            record_value = None
        else:
            status = result.status
            record_value = result.record
        status_code = {
            SpiderConfigurationReadbackStatus.CONFIRMED: 200,
            SpiderConfigurationReadbackStatus.NOT_FOUND: 404,
            SpiderConfigurationReadbackStatus.UNKNOWN: 503,
            SpiderConfigurationReadbackStatus.INVALID_EVIDENCE: 503,
        }[status]
        record: dict[str, Any] | None = None
        if record_value is not None:
            recorded_at = record_value.recorded_at.astimezone(UTC).isoformat(
                timespec="milliseconds"
            )
            record = {
                "request": json.loads(record_value.request.canonical_bytes),
                "configuration_sha256": record_value.request.configuration.sha256,
                "recorded_at": recorded_at.removesuffix("+00:00") + "Z",
            }
        return status_code, {
            "schema_version": "spider_configuration_http_v1",
            "request_id": request.request_id,
            "readback_status": status.value,
            "application_status": "NOT_APPLIED",
            "scope_evidence": SPIDER_CONFIGURATION_SCOPE_EVIDENCE,
            "effective_status": "UNAVAILABLE",
            "effective_configuration": None,
            "record": record,
        }

    @staticmethod
    def _error(status_code: int, code: str) -> tuple[int, dict[str, Any]]:
        return status_code, {"error": code}
