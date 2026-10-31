"""Browser/API authentication and actor contract for Spider metadata routes."""

from datetime import UTC, datetime

from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.spider_configuration_http import (
    SPIDER_CONFIGURATION_READBACK_PATH,
    SPIDER_CONFIGURATION_REQUEST_PATH,
    SpiderConfigurationHttpService,
)
from src.core.spider_management.configuration_request import (
    SpiderConfigurationReadbackStatus,
    SpiderConfigurationRequestRecord,
)
from src.core.spider_management.configuration_request_store import (
    SpiderConfigurationReadback,
)
from test_spider_configuration_http import _app, _request


def test_browser_route_reuses_operator_csrf_and_trusted_actor(monkeypatch):
    auth = BrowserSessionAuth(
        allowed_origin="https://fluxtrade.example.ts.net",
        operator_capability="example.com/cap/fluxtrade-operator",
        step_up_capability="example.com/cap/fluxtrade-step-up",
        clock=lambda: 1_000.0,
    )

    database_calls = 0

    def unavailable_sessions():
        nonlocal database_calls
        database_calls += 1
        raise RuntimeError("unexpected database access")

    service = SpiderConfigurationHttpService(
        unavailable_sessions,
        environment="research",
        execution_scope_id="account-main",
    )
    app = _app(service, browser_auth=auth)
    identity_headers = {
        "Origin": "https://fluxtrade.example.ts.net",
        "Tailscale-User-Login": "operator@example.com",
        "Tailscale-App-Capabilities": '{"example.com/cap/fluxtrade-operator":[{}]}',
    }
    issued = app.handle("POST", "/api/v1/auth/session", headers=identity_headers)
    cookie = dict(issued.headers)["Set-Cookie"].split(";", 1)[0]
    csrf = issued.body["csrf_token"]
    headers = {
        **identity_headers,
        "Cookie": cookie,
        "X-CSRF-Token": csrf,
    }

    request = _request(actor="api_key")
    record = SpiderConfigurationRequestRecord(
        request,
        datetime(2026, 10, 11, tzinfo=UTC),
    )
    monkeypatch.setattr(
        service._store,
        "readback",
        lambda _request: SpiderConfigurationReadback(
            SpiderConfigurationReadbackStatus.CONFIRMED, record
        ),
    )
    inspected = app.handle(
        "POST", SPIDER_CONFIGURATION_READBACK_PATH,
        request.canonical_bytes, headers=headers
    )
    assert inspected.status_code == 200
    assert inspected.body["record"]["request"]["actor"] == "api_key"

    response = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
        request.canonical_bytes, headers=headers
    )
    assert response.status_code == 403
    assert response.body == {"error": "ACTOR_MISMATCH"}
    assert dict(response.headers) == {"Cache-Control": "no-store"}

    missing_csrf = dict(headers)
    missing_csrf.pop("X-CSRF-Token")
    response = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
        request.canonical_bytes, headers=missing_csrf
    )
    assert response.status_code == 403
    assert response.body == {"error": "csrf_rejected"}

    wrong_origin = dict(headers)
    wrong_origin["Origin"] = "https://untrusted.example"
    response = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
        request.canonical_bytes, headers=wrong_origin
    )
    assert response.status_code == 403
    assert response.body == {"error": "origin_rejected"}

    read_only_identity = {
        **identity_headers,
        "Tailscale-App-Capabilities": '{"example.com/cap/read-only":[{}]}',
    }
    read_only_session = app.handle(
        "POST", "/api/v1/auth/session", headers=read_only_identity
    )
    read_only_headers = {
        **read_only_identity,
        "Cookie": dict(read_only_session.headers)["Set-Cookie"].split(";", 1)[0],
        "X-CSRF-Token": read_only_session.body["csrf_token"],
    }
    response = app.handle(
        "POST", SPIDER_CONFIGURATION_READBACK_PATH,
        request.canonical_bytes, headers=read_only_headers
    )
    assert response.status_code == 403
    assert response.body == {"error": "operator_capability_required"}

    legal_actor_request = _request(actor="operator@example.com", key="browser-write")
    record_calls = []

    def record_for_browser(*, raw_query, body, actor):
        record_calls.append((raw_query, body, actor))
        return 200, {"application_status": "NOT_APPLIED"}

    monkeypatch.setattr(service, "record", record_for_browser)
    response = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
        legal_actor_request.canonical_bytes, headers=headers
    )
    assert response.status_code == 200
    assert record_calls == [("", legal_actor_request.canonical_bytes, "operator@example.com")]
    assert database_calls == 0


def test_http_auth_exact_paths_and_unconfigured_service_are_fail_closed():
    database_calls = 0

    def unavailable_sessions():
        nonlocal database_calls
        database_calls += 1
        raise RuntimeError("unexpected database access")

    service = SpiderConfigurationHttpService(
        unavailable_sessions,
        environment="research",
        execution_scope_id="account-main",
    )
    body = _request().canonical_bytes

    anonymous = _app(service, api_key=None)
    response = anonymous.handle("POST", SPIDER_CONFIGURATION_REQUEST_PATH, body)
    assert response.status_code == 403
    assert response.body == {"error": "forbidden"}
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert database_calls == 0

    configured = _app(service)
    for headers in (None, {"Authorization": "Bearer wrong-key"}):
        response = configured.handle(
            "POST", SPIDER_CONFIGURATION_REQUEST_PATH, body, headers=headers
        )
        assert response.status_code == 401
        assert dict(response.headers) == {"Cache-Control": "no-store"}
    valid_header_key = configured.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
        b"{}", headers={"X-API-Key": "spider-http-test-key"}
    )
    assert valid_header_key.status_code == 422
    assert database_calls == 0

    unavailable = _app(None)
    response = unavailable.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH, body,
        headers={"Authorization": "Bearer spider-http-test-key"}
    )
    assert response.status_code == 503
    assert response.body == {"error": "spider_configuration_unavailable"}
    assert dict(response.headers) == {"Cache-Control": "no-store"}
