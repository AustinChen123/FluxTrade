"""Pure service mapping tests for immutable Spider configuration evidence."""

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime

import pytest
from sqlalchemy.orm import Session

from src.control_plane.spider_configuration_http import (
    SPIDER_CONFIGURATION_SCOPE_EVIDENCE,
    SpiderConfigurationHttpService,
)
from src.core.spider_management.configuration_request import (
    SpiderConfigurationReadbackStatus,
    SpiderConfigurationRequestConflict,
    SpiderConfigurationRequestError,
    SpiderConfigurationRequestIntegrityError,
    SpiderConfigurationRequestRecord,
    SpiderConfigurationRequestStale,
)
from src.core.spider_management.configuration_request_store import (
    SpiderConfigurationReadback,
)
from test_spider_configuration_request import request


@contextmanager
def _session_context() -> Iterator[Session]:
    with Session() as session:
        yield session


def test_http_readback_status_and_write_error_mappings(monkeypatch):
    service = SpiderConfigurationHttpService(
        _session_context,
        environment="live",
        execution_scope_id="account-main",
    )
    original = request()
    record = SpiderConfigurationRequestRecord(
        original,
        datetime(2026, 10, 11, tzinfo=UTC),
    )
    expected = {
        SpiderConfigurationReadbackStatus.CONFIRMED: 200,
        SpiderConfigurationReadbackStatus.NOT_FOUND: 404,
        SpiderConfigurationReadbackStatus.UNKNOWN: 503,
        SpiderConfigurationReadbackStatus.INVALID_EVIDENCE: 503,
    }
    for state, http_status in expected.items():
        result = SpiderConfigurationReadback(
            state,
            record if state is SpiderConfigurationReadbackStatus.CONFIRMED else None,
        )
        monkeypatch.setattr(service._store, "readback", lambda _request, r=result: r)
        status, body = service.readback(
            raw_query="", body=original.canonical_bytes
        )
        assert status == http_status
        assert body["readback_status"] == state.value
        assert body["application_status"] == "NOT_APPLIED"
        assert body["scope_evidence"] == SPIDER_CONFIGURATION_SCOPE_EVIDENCE
        assert body["effective_status"] == "UNAVAILABLE"
        assert body["effective_configuration"] is None
        assert (body["record"] is not None) is (
            state is SpiderConfigurationReadbackStatus.CONFIRMED
        )
        if body["record"] is not None:
            assert body["record"]["recorded_at"] == "2026-10-11T00:00:00.000Z"

    errors = (
        (SpiderConfigurationRequestConflict(), 409),
        (SpiderConfigurationRequestStale(), 409),
        (SpiderConfigurationRequestError(), 422),
        (SpiderConfigurationRequestIntegrityError(), 503),
    )
    for error, expected_status in errors:
        def fail(_session, _request, error=error):
            raise error

        monkeypatch.setattr(service._store, "record_in_transaction", fail)
        status, body = service.record(
            raw_query="", body=original.canonical_bytes, actor=original.actor
        )
        assert status == expected_status
        assert body == {"error": str(error)}

    monkeypatch.setattr(
        service._store,
        "readback",
        lambda _request: (_ for _ in ()).throw(RuntimeError("readback unavailable")),
    )
    status, body = service.readback(raw_query="", body=original.canonical_bytes)
    assert status == 503
    assert body["readback_status"] == "UNKNOWN"
    assert body["record"] is None


def test_http_service_validates_configured_scope():
    with pytest.raises(ValueError, match="spider_configuration_scope_invalid"):
        SpiderConfigurationHttpService(
            _session_context,
            environment="Research",
            execution_scope_id="account-main",
        )
    with pytest.raises(ValueError, match="spider_configuration_scope_invalid"):
        SpiderConfigurationHttpService(
            _session_context,
            environment="live",
            execution_scope_id="bad scope",
        )
