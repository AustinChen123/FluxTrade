from contextlib import nullcontext
from datetime import UTC, datetime
import json

from src.control_plane.app import ControlPlaneApp
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.spider_operation_http import (
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


ORIGIN = "https://fluxtrade.example.ts.net"
OPERATOR = "operator@example.com"
OPERATOR_CAP = "example.com/cap/fluxtrade-operator"
STEP_UP_CAP = "example.com/cap/fluxtrade-step-up"


def _request(kind, proposal_id=None):
    body = _body(kind, proposal_id)
    body["actor"] = OPERATOR
    body["idempotency_key"] = f"browser-{kind.lower()}"
    return SpiderOperationRequest.from_canonical_bytes(canonical_bytes(body))


def _browser_headers(app, *, step_up):
    capabilities = {OPERATOR_CAP: [{}]}
    if step_up:
        capabilities[STEP_UP_CAP] = [{}]
    identity = {
        "Origin": ORIGIN,
        "Tailscale-User-Login": OPERATOR,
        "Tailscale-App-Capabilities": json.dumps(capabilities),
    }
    issued = app.handle("POST", "/api/v1/auth/session", headers=identity)
    assert issued.status_code == 201
    return {
        **identity,
        "Cookie": dict(issued.headers)["Set-Cookie"].split(";", 1)[0],
        "X-CSRF-Token": issued.body["csrf_token"],
    }


def test_operation_browser_proposal_does_not_require_step_up_but_confirmation_does(
    monkeypatch,
):
    class Session:
        def begin(self):
            return nullcontext()

    service = SpiderOperationHttpService(
        lambda: _session_context(Session()),
        environment="test",
        execution_scope_id="spider-test",
    )
    auth = BrowserSessionAuth(
        allowed_origin=ORIGIN,
        operator_capability=OPERATOR_CAP,
        step_up_capability=STEP_UP_CAP,
        clock=lambda: 1_000.0,
    )
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        browser_auth=auth,
        spider_operation_http_service=service,
    )
    recorded = []

    def record(_session, request):
        recorded.append(request)

    def readback(request):
        return SpiderOperationReadback(
            SpiderOperationReadbackStatus.CONFIRMED,
            SpiderOperationRecord(request, request.request_kind == "CONFIRMATION", datetime(2026, 10, 11, tzinfo=UTC)),
        )

    monkeypatch.setattr(service._store, "record_in_transaction", record)
    monkeypatch.setattr(service._store, "readback", readback)
    proposal = _request("PROPOSAL")

    for step_up in (False, True):
        response = app.handle(
            "POST",
            SPIDER_OPERATION_REQUEST_PATH,
            proposal.canonical_request_bytes,
            headers=_browser_headers(app, step_up=step_up),
        )
        assert response.status_code == 200
        assert response.body["record"]["authorization_confirmed"] is False

    confirmation = _request("CONFIRMATION", proposal.request_id)
    no_step_up = app.handle(
        "POST",
        SPIDER_OPERATION_REQUEST_PATH,
        confirmation.canonical_request_bytes,
        headers=_browser_headers(app, step_up=False),
    )
    assert no_step_up.status_code == 403
    assert no_step_up.body == {"error": "step_up_required"}
    assert len(recorded) == 2

    with_step_up = app.handle(
        "POST",
        SPIDER_OPERATION_REQUEST_PATH,
        confirmation.canonical_request_bytes,
        headers=_browser_headers(app, step_up=True),
    )
    assert with_step_up.status_code == 200
    assert with_step_up.body["record"]["authorization_confirmed"] is True
    assert len(recorded) == 3


def _session_context(session):
    class Context:
        def __enter__(self):
            return session

        def __exit__(self, *_args):
            return False

    return Context()
