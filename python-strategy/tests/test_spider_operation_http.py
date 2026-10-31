from datetime import UTC, datetime

from src.control_plane.app import ControlPlaneApp
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.spider_operation_http import (
    SPIDER_OPERATION_READBACK_PATH,
    SPIDER_OPERATION_REQUEST_PATH,
    SpiderOperationHttpService,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.spider_management.operation_request import SpiderOperationRequest
from src.core.spider_management.operation_request_store import (
    SpiderOperationReadback,
    SpiderOperationReadbackStatus,
    SpiderOperationRecord,
)
from test_spider_operation_request import _body

API_KEY = "spider-operation-http-test-key"


def _request(kind="PROPOSAL", proposal_request_id=None):
    value = _body(kind, proposal_request_id)
    value["actor"] = "api_key"
    value["idempotency_key"] = f"http-{kind.lower()}"
    return SpiderOperationRequest.from_canonical_bytes(canonical_bytes(value))


def _app(service=None):
    return ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key=API_KEY,
        spider_operation_http_service=service,
    )


def test_operation_http_routes_are_closed_no_store_and_step_up_confirmation(monkeypatch):
    service = SpiderOperationHttpService(
        lambda: (_ for _ in ()).throw(RuntimeError("unexpected database access")),
        environment="test",
        execution_scope_id="spider-test",
    )
    app = _app(service)
    headers = {"Authorization": f"Bearer {API_KEY}"}
    malformed = app.handle("POST", SPIDER_OPERATION_REQUEST_PATH, b"{}", headers=headers)
    assert malformed.status_code == 422
    assert dict(malformed.headers) == {"Cache-Control": "no-store"}

    proposal = _request()
    record = SpiderOperationRecord(proposal, False, datetime(2026, 10, 11, tzinfo=UTC))
    monkeypatch.setattr(
        service._store,
        "record_in_transaction",
        lambda _session, _request: record,
    )
    monkeypatch.setattr(
        service._store,
        "readback",
        lambda _request: SpiderOperationReadback(
            SpiderOperationReadbackStatus.CONFIRMED, record
        ),
    )
    response = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH,
        proposal.canonical_request_bytes, headers=headers,
    )
    assert response.status_code == 200
    assert response.body["application_status"] == "COMMAND_SELECTION_REQUIRED"
    assert response.body["record"]["authorization_confirmed"] is False
    assert dict(response.headers) == {"Cache-Control": "no-store"}

    confirmation = _request("CONFIRMATION", proposal.request_id)
    confirmed_record = SpiderOperationRecord(
        confirmation, True, datetime(2026, 10, 11, tzinfo=UTC)
    )
    monkeypatch.setattr(
        service._store,
        "record_in_transaction",
        lambda _session, _request: confirmed_record,
    )
    monkeypatch.setattr(
        service._store,
        "readback",
        lambda _request: SpiderOperationReadback(
            SpiderOperationReadbackStatus.CONFIRMED, confirmed_record
        ),
    )
    response = app.handle(
        "POST", SPIDER_OPERATION_REQUEST_PATH,
        confirmation.canonical_request_bytes, headers=headers,
    )
    assert response.status_code == 200
    assert response.body["record"]["authorization_confirmed"] is True


def test_operation_http_readback_does_not_require_actor_match(monkeypatch):
    service = SpiderOperationHttpService(
        lambda: (_ for _ in ()).throw(RuntimeError("unused")),
        environment="test",
        execution_scope_id="spider-test",
    )
    request = _request()
    record = SpiderOperationRecord(request, False, datetime(2026, 10, 11, tzinfo=UTC))
    monkeypatch.setattr(
        service._store,
        "readback",
        lambda _request: SpiderOperationReadback(
            SpiderOperationReadbackStatus.CONFIRMED, record
        ),
    )
    status, body = service.readback(raw_query="", body=request.canonical_request_bytes)
    assert status == 200
    assert body["readback_status"] == "CONFIRMED"
    assert body["runtime"] is None

    app = _app(service)
    response = app.handle(
        "POST", SPIDER_OPERATION_READBACK_PATH,
        request.canonical_request_bytes,
        headers={"Authorization": f"Bearer {API_KEY}"},
    )
    assert response.status_code == 200
    assert dict(response.headers) == {"Cache-Control": "no-store"}
