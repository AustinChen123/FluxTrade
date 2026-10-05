from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from src.control_plane.app import ControlPlaneApp
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.browser_auth import BrowserSessionAuth
from src.control_plane.jobs import InMemoryJobStore
from src.control_plane.models import (
    BacktestJobRequest,
    ParameterSearchJobRequest,
)
from src.control_plane.parameter_search import ParameterSearchJobExecutor

ORIGIN = "https://fluxtrade.example.ts.net"
OPERATOR_CAPABILITY = "example.com/cap/fluxtrade-operator"


def _app_and_store():
    store = InMemoryJobStore()
    backtests = MagicMock(spec=BacktestJobExecutor)
    backtests.store = store
    searches = MagicMock(spec=ParameterSearchJobExecutor)
    return (
        ControlPlaneApp(
            backtests,
            parameter_search_executor=searches,
            browser_auth=BrowserSessionAuth(
                allowed_origin=ORIGIN,
                operator_capability=OPERATOR_CAPABILITY,
                step_up_capability="example.com/cap/step-up",
            ),
            api_key="operator-key",
        ),
        store,
        backtests,
        searches,
    )


def _session(app: ControlPlaneApp, *, operator: bool = True):
    capabilities = {OPERATOR_CAPABILITY: [{}]} if operator else {}
    identity = {
        "Origin": ORIGIN,
        "Tailscale-User-Login": "operator@example.com",
        "Tailscale-App-Capabilities": json.dumps(capabilities),
    }
    issued = app.handle("POST", "/api/v1/auth/session", headers=identity)
    assert issued.status_code == 201
    cookie = dict(issued.headers)["Set-Cookie"].split(";", 1)[0]
    headers = {**identity, "Cookie": cookie}
    headers["X-CSRF-Token"] = issued.body["csrf_token"]
    return headers


def _search_request() -> ParameterSearchJobRequest:
    return ParameterSearchJobRequest.model_validate(
        {
            "strategy_type": "golden_cross",
            "strategy_id": "golden_cross",
            "product_id": "BINANCE:BTCUSDT-PERP",
            "timeframe": "1m",
            "start_time": 1_700_000_000_000,
            "end_time": 1_700_000_060_000,
            "backtest": {"candles_csv_path": "legacy.csv"},
            "candidates": [
                {
                    "candidate_id": "candidate-1",
                    "param_pack": {"short_window": 2, "long_window": 3},
                }
            ],
        }
    )


@pytest.mark.parametrize(
    ("path", "body"),
    [
        ("/jobs/parameter-searches", "{}"),
        ("/jobs/parameter-search-presets/golden-cross", "{}"),
    ],
)
def test_browser_parameter_search_creation_is_denied_after_auth_checks(path, body):
    app, _, _, searches = _app_and_store()
    headers = _session(app)

    denied = app.handle("POST", path, body=body, headers=headers)
    csrf_denied = app.handle(
        "POST",
        path,
        body=body,
        headers={key: value for key, value in headers.items() if key != "X-CSRF-Token"},
    )
    origin_denied = app.handle(
        "POST",
        path,
        body=body,
        headers={**headers, "Origin": "https://attacker.example"},
    )
    unauthenticated = app.handle("POST", path, body=body)
    app.shutdown(timeout=1)

    assert denied.status_code == 403
    assert denied.body == {"error": "browser_ga_controls_unavailable"}
    assert csrf_denied.body == {"error": "csrf_rejected"}
    assert origin_denied.body == {"error": "origin_rejected"}
    assert unauthenticated.status_code == 401
    assert unauthenticated.body == {"error": "unauthorized"}
    searches.submit_search.assert_not_called()


def test_browser_parameter_search_requires_operator_capability_before_ga_gate():
    app, _, _, searches = _app_and_store()
    headers = _session(app, operator=False)

    response = app.handle(
        "POST", "/jobs/parameter-searches", body="{}", headers=headers
    )
    app.shutdown(timeout=1)

    assert response.status_code == 403
    assert response.body == {"error": "operator_capability_required"}
    searches.submit_search.assert_not_called()


@pytest.mark.parametrize("action", ["cancel", "retry"])
def test_browser_known_parameter_search_actions_are_denied_but_unknown_is_404(action):
    app, store, _, searches = _app_and_store()
    headers = _session(app)
    job = store.create(kind="parameter_search", request=_search_request())
    if action == "retry":
        store.mark_failed(job.id, "previously failed")

    denied = app.handle("POST", f"/jobs/{job.id}/{action}", body="{}", headers=headers)
    missing = app.handle(
        "POST", f"/jobs/not-a-job/{action}", body="{}", headers=headers
    )
    app.shutdown(timeout=1)

    assert denied.status_code == 403
    assert denied.body == {"error": "browser_ga_controls_unavailable"}
    assert missing.status_code == 404
    assert missing.body == {"error": "job_not_found"}
    searches.cancel_search.assert_not_called()
    searches.retry_search.assert_not_called()


def test_browser_backtest_job_action_is_not_blocked_by_parameter_search_gate():
    app, store, backtests, _ = _app_and_store()
    headers = _session(app)
    request = BacktestJobRequest(
        strategy_id="csv-signal",
        product_id="BINANCE:BTCUSDT-PERP",
        timeframe="1m",
        candles_csv_path="candles.csv",
        signals_csv_path="signals.csv",
        start_time=1_700_000_000_000,
        end_time=1_700_000_060_000,
    )
    job = store.create(kind="csv_signal_backtest", request=request)
    backtests.cancel_backtest.side_effect = lambda job_id, reason: store.mark_cancelled(
        job_id, reason
    )

    response = app.handle("POST", f"/jobs/{job.id}/cancel", body="{}", headers=headers)
    app.shutdown(timeout=1)

    assert response.status_code == 200
    assert response.body["job"]["status"] == "CANCELLED"
    backtests.cancel_backtest.assert_called_once_with(job.id, None)


def test_api_key_can_still_submit_legacy_csv_parameter_search():
    app, store, _, searches = _app_and_store()
    request = _search_request()
    searches.submit_search.side_effect = lambda submitted: store.create(
        kind=submitted.kind, request=submitted
    )

    response = app.handle(
        "POST",
        "/jobs/parameter-searches",
        body=request.model_dump_json(exclude_none=True),
        headers={"Authorization": "Bearer operator-key"},
    )
    app.shutdown(timeout=1)

    assert response.status_code == 202
    assert (
        response.body["job"]["request"]["backtest"]["candles_csv_path"] == "legacy.csv"
    )
    searches.submit_search.assert_called_once()
