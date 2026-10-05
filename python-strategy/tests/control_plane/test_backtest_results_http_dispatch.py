from __future__ import annotations

import json
from unittest.mock import Mock

import pytest

from src.control_plane.app import ControlPlaneApp
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.backtest_result_http_contract import (
    BacktestResultQuery,
    InvalidBacktestResultHttpRequest,
)
from src.control_plane.backtest_results import (
    BacktestResultsNotFound,
    BacktestResultsReadUnavailable,
    BacktestResultsUnavailable,
)
from src.control_plane.browser_auth import BrowserSessionAuth

ORIGIN = "https://fluxtrade.example.ts.net"
OPERATOR = "example.com/cap/fluxtrade-operator"


def _session(
    app: ControlPlaneApp, *, operator: bool = True, owner: bool = False
) -> dict[str, str]:
    caps = {OPERATOR: [{}]} if operator else {}
    identity = {
        "Origin": ORIGIN,
        "Tailscale-User-Login": (
            "owner@example.com" if owner else "operator@example.com"
        ),
        "Tailscale-App-Capabilities": json.dumps(caps),
    }
    issued = app.handle("POST", "/api/v1/auth/session", headers=identity)
    assert issued.status_code == 201
    headers = {
        **identity,
        "Cookie": dict(issued.headers)["Set-Cookie"].split(";", 1)[0],
        "X-CSRF-Token": issued.body["csrf_token"],
    }
    return headers


@pytest.fixture
def harness():
    executor = Mock(spec=BacktestJobExecutor)
    service = Mock()
    payloads = {
        "index": {"items": [], "next_cursor": None, "revision": 1},
        "detail": {"job_id": "job+1", "revision": 1},
        "trades": {"items": [], "next_cursor": None, "revision": 1},
        "candles": {"items": [], "next_cursor": None, "revision": 1},
    }
    service.list_index.return_value = payloads["index"]
    service.get_detail.return_value = payloads["detail"]
    service.list_trades.return_value = payloads["trades"]
    service.list_candles.return_value = payloads["candles"]
    auth = BrowserSessionAuth(
        allowed_origin=ORIGIN,
        operator_capability=OPERATOR,
        step_up_capability="example.com/cap/step-up",
        owner_login="owner@example.com",
    )
    app = ControlPlaneApp(
        executor,
        browser_auth=auth,
        api_key="operator-key",
        backtest_results_query_service=service,
    )
    return app, service, payloads


@pytest.mark.parametrize(
    ("path", "method_name", "args"),
    [
        (
            "/api/v1/backtest-results?limit=2",
            "list_index",
            (BacktestResultQuery(limit=2),),
        ),
        ("/api/v1/backtest-results/job%2B1", "get_detail", ("job+1",)),
        ("/api/v1/backtest-results/job+1", "get_detail", ("job+1",)),
        (
            "/api/v1/backtest-results/job%2B1/trades?limit=3",
            "list_trades",
            ("job+1", BacktestResultQuery(limit=3)),
        ),
        (
            "/api/v1/backtest-results/job%2B1/candles?start=0&end=60000",
            "list_candles",
            ("job+1", BacktestResultQuery(limit=100, start_ms=0, end_ms=60000)),
        ),
    ],
)
def test_operator_session_dispatches_exact_query_and_plain_response(
    harness, path, method_name, args
):
    app, service, payloads = harness
    response = app.handle("GET", path, headers=_session(app))

    assert response.status_code == 200
    assert (
        response.body
        == payloads[
            method_name.removeprefix("list_")
            if method_name != "get_detail"
            else "detail"
        ]
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    getattr(service, method_name).assert_called_once_with(*args)


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/backtest-results?limit=1&limit=2",
        "/api/v1/backtest-results?unknown=x",
        "/api/v1/backtest-results/job%2Fchild",
        "/api/v1/backtest-results//trades",
        "/api/v1/backtest-results/job/trades?cursor=",
        "/api/v1/backtest-results/job/candles?start=2&end=1",
    ],
)
def test_operator_session_rejects_raw_contract_inputs_before_service(harness, path):
    app, service, _ = harness
    response = app.handle("GET", path, headers=_session(app))
    assert response.status_code == 422
    assert response.body == {"error": "validation_error"}
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    for name in ("list_index", "get_detail", "list_trades", "list_candles"):
        getattr(service, name).assert_not_called()


@pytest.mark.parametrize("body", ["not empty", b"x"])
def test_result_get_rejects_nonempty_body(harness, body):
    app, service, _ = harness
    response = app.handle(
        "GET", "/api/v1/backtest-results", body=body, headers=_session(app)
    )
    assert (response.status_code, response.body) == (422, {"error": "validation_error"})
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    service.list_index.assert_not_called()


def test_empty_get_body_is_accepted(harness):
    app, service, payloads = harness
    response = app.handle(
        "GET", "/api/v1/backtest-results", body=b"", headers=_session(app)
    )
    assert (response.status_code, response.body) == (200, payloads["index"])
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    service.list_index.assert_called_once_with(BacktestResultQuery(limit=100))


def test_existing_trusted_owner_grant_authorizes_result_projection(harness):
    app, service, payloads = harness
    response = app.handle(
        "GET",
        "/api/v1/backtest-results",
        headers=_session(app, operator=False, owner=True),
    )
    assert (response.status_code, response.body) == (200, payloads["index"])
    service.list_index.assert_called_once_with(BacktestResultQuery(limit=100))


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/backtest-results?unknown=x",
        "/api/v1/backtest-results/%2F?unknown=x",
        "/api/v1/backtest-results/job/trades?cursor=",
        "/api/v1/backtest-results/job/candles?start=2&end=1",
    ],
)
@pytest.mark.parametrize(
    "identity_kind", ["unauthenticated", "api_key", "local", "nonoperator"]
)
def test_every_projection_route_authorizes_before_parsing_or_service(
    harness, path, identity_kind
):
    app, service, _ = harness
    if identity_kind == "unauthenticated":
        headers = None
        expected = (401, {"error": "unauthorized"})
    elif identity_kind == "api_key":
        headers = {"X-API-Key": "operator-key"}
        expected = (403, {"error": "forbidden"})
    elif identity_kind == "local":
        app = ControlPlaneApp(
            Mock(spec=BacktestJobExecutor), backtest_results_query_service=service
        )
        headers = {}
        expected = (403, {"error": "forbidden"})
    else:
        headers = _session(app, operator=False)
        expected = (403, {"error": "forbidden"})

    response = app.handle("GET", path, headers=headers)
    assert (response.status_code, response.body) == expected
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    for name in ("list_index", "get_detail", "list_trades", "list_candles"):
        getattr(service, name).assert_not_called()


def test_result_errors_are_fixed_and_no_store(harness):
    app, service, _ = harness
    cases = [
        (InvalidBacktestResultHttpRequest(), 422, {"error": "validation_error"}),
        (BacktestResultsUnavailable(), 409, {"error": "result_unavailable"}),
        (BacktestResultsNotFound(), 404, {"error": "result_not_found"}),
        (
            BacktestResultsReadUnavailable(),
            503,
            {"error": "browser_result_backend_unavailable"},
        ),
    ]
    for error, status, body in cases:
        service.get_detail.side_effect = error
        response = app.handle(
            "GET", "/api/v1/backtest-results/job", headers=_session(app)
        )
        assert (response.status_code, response.body) == (status, body)
        assert dict(response.headers) == {"Cache-Control": "no-store"}
    service.get_detail.side_effect = RuntimeError("sensitive backend detail")
    response = app.handle("GET", "/api/v1/backtest-results/job", headers=_session(app))
    assert (response.status_code, response.body) == (
        503,
        {"error": "browser_result_backend_unavailable"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert "sensitive backend detail" not in response.json()


def test_unknown_result_suffix_is_authenticated_no_store_404(harness):
    app, service, _ = harness
    response = app.handle(
        "GET", "/api/v1/backtest-results/job/unknown?limit=bad", headers=_session(app)
    )
    assert (response.status_code, response.body) == (404, {"error": "not_found"})
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    service.list_index.assert_not_called()
    service.get_detail.assert_not_called()


@pytest.mark.parametrize(
    "path",
    [
        "/api/v1/backtest-results//",
        "/api/v1/backtest-results/job//",
        "/api/v1/backtest-results/job/trades/",
        "/api/v1/backtest-results/job/candles/",
    ],
)
def test_trailing_empty_result_path_segments_are_unknown(harness, path):
    app, service, _ = harness
    response = app.handle("GET", path, headers=_session(app))
    assert (response.status_code, response.body) == (404, {"error": "not_found"})
    assert dict(response.headers) == {"Cache-Control": "no-store"}
    for name in ("list_index", "get_detail", "list_trades", "list_candles"):
        getattr(service, name).assert_not_called()


def test_missing_query_service_is_sanitized_and_no_store():
    app, _, _ = _new_harness_without_service()
    response = app.handle("GET", "/api/v1/backtest-results", headers=_session(app))
    assert (response.status_code, response.body) == (
        503,
        {"error": "browser_result_backend_unavailable"},
    )
    assert dict(response.headers) == {"Cache-Control": "no-store"}


def _new_harness_without_service():
    executor = Mock(spec=BacktestJobExecutor)
    auth = BrowserSessionAuth(
        allowed_origin=ORIGIN,
        operator_capability=OPERATOR,
        step_up_capability="example.com/cap/step-up",
    )
    return (
        ControlPlaneApp(executor, browser_auth=auth, api_key="operator-key"),
        None,
        None,
    )
