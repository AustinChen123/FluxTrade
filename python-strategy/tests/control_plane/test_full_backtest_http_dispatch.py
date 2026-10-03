from __future__ import annotations

import json

import pytest

from src.control_plane.app import ControlPlaneApp
from src.control_plane.full_backtest_request import FullStrategyBacktestRequest
from src.control_plane.models import BacktestJobRequest
from test_full_backtest_resolution import _request
from test_parameter_search_browser_gate import _app_and_store, _session


def _wire_request() -> dict[str, object]:
    payload = _request().model_dump(mode="json", exclude_none=True)
    payload["drawdown_limit"] = None
    return payload


def _csv_payload(*, kind: str | None = None) -> dict[str, object]:
    payload: dict[str, object] = {
        "strategy_id": "csv-signal",
        "product_id": "BINANCE:BTCUSDT-PERP",
        "timeframe": "1m",
        "candles_csv_path": "candles.csv",
        "signals_csv_path": "signals.csv",
        "start_time": 1_700_000_000_000,
        "end_time": 1_700_000_060_000,
    }
    if kind is not None:
        payload["kind"] = kind
    return payload


def test_full_request_dispatches_for_api_key_and_local_operator():
    for local in (False, True):
        app, store, backtests, _ = _app_and_store()
        if local:
            app = ControlPlaneApp(backtests)
        backtests.submit_backtest.side_effect = lambda request: store.create(
            kind=request.kind, request=request
        )

        response = app.handle(
            "POST",
            "/jobs/backtests",
            body=json.dumps(_wire_request()),
            headers=None if local else {"Authorization": "Bearer operator-key"},
        )
        app.shutdown(timeout=1)

        assert response.status_code == 202
        assert response.body["job"]["kind"] == "full_strategy_backtest"
        dispatched = backtests.submit_backtest.call_args.args[0]
        assert isinstance(dispatched, FullStrategyBacktestRequest)
        assert response.body["job"]["request"]["dataset_id"] == dispatched.dataset_id


def test_full_validation_error_is_sanitized_but_malformed_json_stays_400():
    app, _, backtests, _ = _app_and_store()
    invalid = app.handle(
        "POST",
        "/jobs/backtests",
        body='{"kind":"full_strategy_backtest"}',
        headers={"Authorization": "Bearer operator-key"},
    )
    malformed = app.handle(
        "POST",
        "/jobs/backtests",
        body="{",
        headers={"Authorization": "Bearer operator-key"},
    )
    app.shutdown(timeout=1)

    assert invalid.status_code == 422
    assert invalid.body == {"error": "validation_error"}
    assert malformed.status_code == 400
    assert malformed.body["error"] == "invalid_json"
    backtests.submit_backtest.assert_not_called()


@pytest.mark.parametrize("kind", [None, "csv_signal_backtest"])
def test_csv_requests_keep_legacy_dispatch(kind):
    app, store, backtests, _ = _app_and_store()
    backtests.submit_backtest.side_effect = lambda request: store.create(
        kind=request.kind, request=request
    )
    response = app.handle(
        "POST",
        "/jobs/backtests",
        body=json.dumps(_csv_payload(kind=kind)),
        headers={"Authorization": "Bearer operator-key"},
    )
    app.shutdown(timeout=1)

    assert response.status_code == 202
    assert response.body["job"]["kind"] == "csv_signal_backtest"
    assert isinstance(backtests.submit_backtest.call_args.args[0], BacktestJobRequest)


def test_unknown_kind_keeps_legacy_validation_response():
    app, _, backtests, _ = _app_and_store()
    response = app.handle(
        "POST",
        "/jobs/backtests",
        body=json.dumps({**_csv_payload(), "kind": "unknown"}),
        headers={"Authorization": "Bearer operator-key"},
    )
    app.shutdown(timeout=1)

    assert response.status_code == 422
    assert response.body["error"] == "validation_error"
    assert "detail" in response.body
    backtests.submit_backtest.assert_not_called()


@pytest.mark.parametrize(
    "payload", [_wire_request(), {"kind": "full_strategy_backtest"}]
)
def test_browser_full_request_is_denied_before_validation_or_creation(payload):
    app, store, backtests, _ = _app_and_store()
    headers = _session(app)
    response = app.handle(
        "POST", "/jobs/backtests", body=json.dumps(payload), headers=headers
    )
    no_auth = app.handle("POST", "/jobs/backtests", body=json.dumps(payload))
    wrong_origin = app.handle(
        "POST",
        "/jobs/backtests",
        body=json.dumps(payload),
        headers={**headers, "Origin": "https://attacker.example"},
    )
    missing_csrf = app.handle(
        "POST",
        "/jobs/backtests",
        body=json.dumps(payload),
        headers={key: value for key, value in headers.items() if key != "X-CSRF-Token"},
    )
    non_operator = app.handle(
        "POST",
        "/jobs/backtests",
        body=json.dumps(payload),
        headers=_session(app, operator=False),
    )
    app.shutdown(timeout=1)

    assert response.status_code == 403
    assert response.body == {"error": "browser_backtest_controls_unavailable"}
    assert no_auth.body == {"error": "unauthorized"}
    assert wrong_origin.body == {"error": "origin_rejected"}
    assert missing_csrf.body == {"error": "csrf_rejected"}
    assert non_operator.body == {"error": "operator_capability_required"}
    assert store.list() == []
    backtests.submit_backtest.assert_not_called()


@pytest.mark.parametrize("action", ["cancel", "retry"])
def test_browser_full_job_actions_are_denied_after_lookup(action):
    app, store, backtests, _ = _app_and_store()
    headers = _session(app)
    request = FullStrategyBacktestRequest.model_validate(_wire_request())
    job = store.create(kind=request.kind, request=request)
    if action == "retry":
        store.mark_failed(job.id, "failed")

    denied = app.handle("POST", f"/jobs/{job.id}/{action}", body="{", headers=headers)
    missing = app.handle("POST", f"/jobs/missing/{action}", body="{", headers=headers)
    app.shutdown(timeout=1)

    assert denied.status_code == 403
    assert denied.body == {"error": "browser_backtest_controls_unavailable"}
    assert missing.status_code == 404
    assert missing.body == {"error": "job_not_found"}
    backtests.cancel_backtest.assert_not_called()
    backtests.retry_backtest.assert_not_called()
