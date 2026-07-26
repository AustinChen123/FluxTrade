from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer
from threading import Thread
from urllib.request import Request, urlopen
from unittest.mock import MagicMock

import pytest

from src.control_plane.app import ControlPlaneApp
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.browser_auth import (
    BrowserPrincipal,
    BrowserSessionAuth,
    InMemoryBrowserSessionStore,
)
from src.control_plane.server import make_handler


ORIGIN = "https://fluxtrade.example.ts.net"
OPERATOR_CAPABILITY = "example.com/cap/fluxtrade-operator"
STEP_UP_CAPABILITY = "example.com/cap/fluxtrade-step-up"


def _browser_auth(*, clock=lambda: 1_000.0) -> BrowserSessionAuth:
    return BrowserSessionAuth(
        allowed_origin=ORIGIN,
        operator_capability=OPERATOR_CAPABILITY,
        step_up_capability=STEP_UP_CAPABILITY,
        clock=clock,
    )


def _session_headers(
    *capabilities: str,
    actor: str = "operator@example.com",
) -> dict[str, str]:
    payload = {capability: [{}] for capability in capabilities}
    return {
        "Origin": ORIGIN,
        "Tailscale-User-Login": actor,
        "Tailscale-App-Capabilities": json.dumps(payload),
    }


def _create_session(
    app: ControlPlaneApp,
    *capabilities: str,
) -> tuple[str, str]:
    response = app.handle(
        "POST",
        "/api/v1/auth/session",
        headers=_session_headers(*capabilities),
    )
    assert response.status_code == 201
    assert response.body["expires_at"].endswith("+00:00")
    cookie = dict(response.headers)["Set-Cookie"].split(";", 1)[0]
    return cookie, response.body["csrf_token"]


def _browser_request_headers(
    cookie: str,
    csrf_token: str | None = None,
    *capabilities: str,
) -> dict[str, str]:
    headers = {
        **_session_headers(*capabilities),
        "Cookie": cookie,
    }
    if csrf_token is not None:
        headers["X-CSRF-Token"] = csrf_token
    return headers


def test_browser_session_matrix_keeps_identity_only_session_read_only():
    redis = MagicMock()
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        redis_client=redis,
        browser_auth=_browser_auth(),
    )
    cookie, csrf_token = _create_session(app)

    read_response = app.handle(
        "GET",
        "/jobs",
        headers=_browser_request_headers(cookie),
    )
    write_response = app.handle(
        "POST",
        "/ops/kill-switch",
        json.dumps({"confirm": True}),
        headers=_browser_request_headers(cookie, csrf_token),
    )

    assert read_response.status_code == 200
    assert write_response.status_code == 403
    assert write_response.body == {"error": "operator_capability_required"}
    redis.publish.assert_not_called()


def test_browser_capability_revocation_takes_effect_on_next_request():
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        browser_auth=_browser_auth(),
    )
    cookie, csrf_token = _create_session(app, OPERATOR_CAPABILITY)

    response = app.handle(
        "POST",
        "/ops/kill-switch",
        json.dumps({"confirm": True}),
        headers=_browser_request_headers(cookie, csrf_token),
    )

    assert response.status_code == 403
    assert response.body == {"error": "operator_capability_required"}


def test_browser_session_rejects_changed_trusted_identity():
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        browser_auth=_browser_auth(),
    )
    cookie, _ = _create_session(app)
    headers = _browser_request_headers(cookie)
    headers["Tailscale-User-Login"] = "attacker@example.com"

    response = app.handle("GET", "/jobs", headers=headers)

    assert response.status_code == 401
    assert response.body == {"error": "trusted_identity_mismatch"}


def test_browser_session_expires_fail_closed():
    now = [1_000.0]
    auth = BrowserSessionAuth(
        allowed_origin=ORIGIN,
        operator_capability=OPERATOR_CAPABILITY,
        step_up_capability=STEP_UP_CAPABILITY,
        session_ttl_seconds=10,
        clock=lambda: now[0],
    )
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        browser_auth=auth,
    )
    cookie, _ = _create_session(app)
    now[0] += 11

    response = app.handle(
        "GET",
        "/jobs",
        headers=_browser_request_headers(cookie),
    )

    assert response.status_code == 401
    assert response.body == {"error": "unauthorized"}

    session_response = app.handle(
        "GET",
        "/api/v1/auth/session",
        headers=_browser_request_headers(cookie),
    )

    assert session_response.status_code == 401
    assert dict(session_response.headers)["Cache-Control"] == "no-store"


def test_browser_session_decodes_tailscale_rfc2047_identity():
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        browser_auth=_browser_auth(),
    )
    headers = _session_headers()
    headers["Tailscale-User-Login"] = (
        "=?utf-8?q?b=C3=BCller=40example=2Ecom?="
    )

    response = app.handle("POST", "/api/v1/auth/session", headers=headers)
    cookie = dict(response.headers)["Set-Cookie"].split(";", 1)[0]
    authenticated = app.handle(
        "GET",
        "/jobs",
        headers={
            **headers,
            "Cookie": cookie,
        },
    )

    assert response.status_code == 201
    assert response.body["actor"] == "büller@example.com"
    assert authenticated.status_code == 200


@pytest.mark.parametrize(
    ("headers", "expected_error"),
    [
        ({"Cookie": "{cookie}", "X-CSRF-Token": "{csrf}"}, "origin_rejected"),
        (
            {"Cookie": "{cookie}", "Origin": ORIGIN},
            "csrf_rejected",
        ),
    ],
)
def test_browser_write_requires_same_origin_and_csrf(headers, expected_error):
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        browser_auth=_browser_auth(),
    )
    cookie, csrf_token = _create_session(app, OPERATOR_CAPABILITY)
    request_headers = _session_headers(OPERATOR_CAPABILITY)
    request_headers.pop("Origin")
    request_headers.update(
        {
            key: value.format(cookie=cookie, csrf=csrf_token)
            for key, value in headers.items()
        }
    )

    response = app.handle(
        "POST",
        "/ops/kill-switch",
        json.dumps({"confirm": True}),
        headers=request_headers,
    )

    assert response.status_code == 403
    assert response.body == {"error": expected_error}


def test_browser_lockdown_requires_confirmation_and_uses_trusted_actor():
    redis = MagicMock()
    redis.publish.return_value = 1
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        redis_client=redis,
        browser_auth=_browser_auth(),
    )
    cookie, csrf_token = _create_session(app, OPERATOR_CAPABILITY)
    headers = _browser_request_headers(
        cookie,
        csrf_token,
        OPERATOR_CAPABILITY,
    )
    headers["Idempotency-Key"] = "lockdown-mobile-1"

    rejected = app.handle("POST", "/ops/kill-switch", "{}", headers=headers)
    accepted = app.handle(
        "POST",
        "/ops/kill-switch",
        json.dumps({"confirm": True, "reason": "mobile safety action"}),
        headers=headers,
    )

    assert rejected.status_code == 403
    assert rejected.body == {"error": "confirmation_required"}
    assert accepted.status_code == 202
    assert accepted.body == {
        "status": "accepted",
        "operation_id": "lockdown-mobile-1",
    }
    _, raw_payload = redis.publish.call_args.args
    assert json.loads(raw_payload)["params"] == {
        "actor": "operator@example.com",
        "idempotency_key": "lockdown-mobile-1",
        "reason": "mobile safety action",
    }


@pytest.mark.parametrize(
    ("idempotency_key", "expected_error"),
    [
        (None, "idempotency_key_required"),
        ("", "idempotency_key_invalid"),
        (" leading-space", "idempotency_key_invalid"),
        ("embedded space", "idempotency_key_invalid"),
        ("重複鍵", "idempotency_key_invalid"),
        ("x" * 129, "idempotency_key_invalid"),
    ],
)
def test_browser_lockdown_requires_valid_idempotency_key(
    idempotency_key,
    expected_error,
):
    redis = MagicMock()
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        redis_client=redis,
        browser_auth=_browser_auth(),
    )
    cookie, csrf_token = _create_session(app, OPERATOR_CAPABILITY)
    headers = _browser_request_headers(
        cookie,
        csrf_token,
        OPERATOR_CAPABILITY,
    )
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key

    response = app.handle(
        "POST",
        "/ops/kill-switch",
        json.dumps({"confirm": True}),
        headers=headers,
    )

    assert response.status_code == 400
    assert response.body == {"error": expected_error}
    redis.publish.assert_not_called()


def test_browser_clear_requires_live_step_up_and_uses_trusted_actor():
    now = [1_000.0]
    redis = MagicMock()
    redis.publish.return_value = 1
    auth = _browser_auth(clock=lambda: now[0])
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        redis_client=redis,
        browser_auth=auth,
    )
    operator_cookie, operator_csrf = _create_session(app, OPERATOR_CAPABILITY)
    rejected = app.handle(
        "POST",
        "/ops/kill-switch/clear",
        headers=_browser_request_headers(
            operator_cookie,
            operator_csrf,
            OPERATOR_CAPABILITY,
        ),
    )
    step_cookie, step_csrf = _create_session(
        app,
        OPERATOR_CAPABILITY,
        STEP_UP_CAPABILITY,
    )
    revoked = app.handle(
        "POST",
        "/ops/kill-switch/clear",
        headers=_browser_request_headers(
            step_cookie,
            step_csrf,
            OPERATOR_CAPABILITY,
        ),
    )
    accepted = app.handle(
        "POST",
        "/ops/kill-switch/clear",
        headers=_browser_request_headers(
            step_cookie,
            step_csrf,
            OPERATOR_CAPABILITY,
            STEP_UP_CAPABILITY,
        ),
    )
    now[0] += 301
    expired = app.handle(
        "POST",
        "/ops/kill-switch/clear",
        headers=_browser_request_headers(
            step_cookie,
            step_csrf,
            OPERATOR_CAPABILITY,
            STEP_UP_CAPABILITY,
        ),
    )

    assert rejected.body == {"error": "step_up_required"}
    assert revoked.body == {"error": "step_up_required"}
    assert accepted.status_code == 202
    _, raw_payload = redis.publish.call_args.args
    assert json.loads(raw_payload)["params"]["actor"] == "operator@example.com"
    assert expired.body == {"error": "step_up_required"}


def test_browser_gene_promotion_ignores_body_actor():
    gene_control = MagicMock()
    gene_control.promote_gene.return_value = {"gene_id": 7}
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        gene_control=gene_control,
        browser_auth=_browser_auth(),
    )
    cookie, csrf_token = _create_session(
        app,
        OPERATOR_CAPABILITY,
        STEP_UP_CAPABILITY,
    )

    response = app.handle(
        "POST",
        "/genes/7/promote",
        json.dumps({"reason": "approved", "actor": "spoofed"}),
        headers=_browser_request_headers(
            cookie,
            csrf_token,
            OPERATOR_CAPABILITY,
            STEP_UP_CAPABILITY,
        ),
    )

    assert response.status_code == 200
    gene_control.promote_gene.assert_called_once_with(
        7,
        reason="approved",
        actor="operator@example.com",
    )


@pytest.mark.parametrize(
    "command",
    ["START", "RESUME", "FORCE_RECOVER", "RELOAD"],
)
def test_browser_risk_increasing_strategy_commands_require_step_up(command):
    strategy_control = MagicMock()
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        strategy_control=strategy_control,
        browser_auth=_browser_auth(),
    )
    cookie, csrf_token = _create_session(app, OPERATOR_CAPABILITY)

    response = app.handle(
        "POST",
        "/strategies/s1/commands",
        json.dumps({"command": command}),
        headers=_browser_request_headers(
            cookie,
            csrf_token,
            OPERATOR_CAPABILITY,
        ),
    )

    assert response.status_code == 403
    assert response.body == {"error": "step_up_required"}
    strategy_control.submit_command.assert_not_called()


def test_browser_stop_command_needs_operator_but_not_step_up():
    strategy_control = MagicMock()
    strategy_control.submit_command.return_value = {
        "success": True,
        "message": "accepted",
        "data": {},
        "accepted": True,
    }
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        strategy_control=strategy_control,
        browser_auth=_browser_auth(),
    )
    cookie, csrf_token = _create_session(app, OPERATOR_CAPABILITY)

    response = app.handle(
        "POST",
        "/strategies/s1/commands",
        json.dumps({"command": "STOP"}),
        headers=_browser_request_headers(
            cookie,
            csrf_token,
            OPERATOR_CAPABILITY,
        ),
    )

    assert response.status_code == 202
    strategy_control.submit_command.assert_called_once()
    assert strategy_control.submit_command.call_args.kwargs["actor"] == (
        "operator@example.com"
    )


def test_logout_revokes_session_and_expires_cookie():
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        browser_auth=_browser_auth(),
    )
    cookie, csrf_token = _create_session(app)

    response = app.handle(
        "POST",
        "/api/v1/auth/logout",
        headers=_browser_request_headers(cookie, csrf_token),
    )
    after_logout = app.handle(
        "GET",
        "/jobs",
        headers=_browser_request_headers(cookie),
    )

    assert response.status_code == 200
    assert "Max-Age=0" in dict(response.headers)["Set-Cookie"]
    assert after_logout.status_code == 401


def test_api_key_client_remains_compatible_without_browser_proofs():
    redis = MagicMock()
    redis.publish.return_value = 1
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        redis_client=redis,
        browser_auth=_browser_auth(),
    )

    response = app.handle(
        "POST",
        "/ops/kill-switch",
        headers={"Authorization": "Bearer service-secret"},
    )

    assert response.status_code == 202
    _, raw_payload = redis.publish.call_args.args
    assert json.loads(raw_payload)["params"]["actor"] == "api_key"


@pytest.mark.parametrize(
    ("headers", "expected_status", "expected_error"),
    [
        (
            {
                "Origin": ORIGIN,
                "Tailscale-App-Capabilities": "{}",
            },
            401,
            "trusted_identity_missing",
        ),
        (
            {
                "Origin": ORIGIN,
                "Tailscale-User-Login": "operator@example.com",
                "Tailscale-App-Capabilities": "not-json",
            },
            401,
            "capabilities_invalid",
        ),
        (
            {
                "Origin": "https://attacker.example",
                "Tailscale-User-Login": "operator@example.com",
                "Tailscale-App-Capabilities": "{}",
            },
            403,
            "origin_rejected",
        ),
    ],
)
def test_session_creation_fails_closed(
    headers,
    expected_status,
    expected_error,
):
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        browser_auth=_browser_auth(),
    )

    response = app.handle("POST", "/api/v1/auth/session", headers=headers)

    assert response.status_code == expected_status
    assert response.body == {"error": expected_error}
    assert dict(response.headers)["Cache-Control"] == "no-store"


def test_browser_auth_rejects_oversized_actor():
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        browser_auth=_browser_auth(),
    )

    response = app.handle(
        "POST",
        "/api/v1/auth/session",
        headers=_session_headers(actor=f"{'a' * 53}@example.com"),
    )

    assert response.status_code == 401
    assert response.body == {"error": "trusted_identity_invalid"}


def test_browser_auth_rejects_identical_operator_and_step_up_capability():
    with pytest.raises(
        ValueError,
        match="operator and step-up capabilities must be distinct",
    ):
        BrowserSessionAuth(
            allowed_origin=ORIGIN,
            operator_capability=OPERATOR_CAPABILITY,
            step_up_capability=OPERATOR_CAPABILITY,
        )


def test_browser_auth_preserves_falsey_session_store():
    class FalseyStore:
        def __init__(self):
            self.principal: BrowserPrincipal | None = None

        def __bool__(self):
            return False

        def put(self, principal, *, now):
            self.principal = principal

        def get(self, token, *, now):
            return self.principal

        def delete(self, token):
            self.principal = None

    store = FalseyStore()
    auth = BrowserSessionAuth(
        allowed_origin=ORIGIN,
        operator_capability=OPERATOR_CAPABILITY,
        step_up_capability=STEP_UP_CAPABILITY,
        session_store=store,
    )

    principal = auth.issue(_session_headers())

    assert store.principal is principal


def test_in_memory_session_store_bounds_sessions_per_actor():
    store = InMemoryBrowserSessionStore(
        max_sessions=2,
        max_sessions_per_actor=1,
    )
    auth = BrowserSessionAuth(
        allowed_origin=ORIGIN,
        operator_capability=OPERATOR_CAPABILITY,
        step_up_capability=STEP_UP_CAPABILITY,
        session_store=store,
    )
    first = auth.issue(_session_headers())
    second = auth.issue(_session_headers())

    evicted = auth.authenticate(
        {
            **_session_headers(),
            "Cookie": f"__Host-fluxtrade_session={first.session_token}",
        }
    )
    current = auth.authenticate(
        {
            **_session_headers(),
            "Cookie": f"__Host-fluxtrade_session={second.session_token}",
        }
    )

    assert evicted is None
    assert current is not None


def test_in_memory_session_store_bounds_concurrent_global_issuance():
    store = InMemoryBrowserSessionStore(
        max_sessions=16,
        max_sessions_per_actor=4,
    )
    auth = BrowserSessionAuth(
        allowed_origin=ORIGIN,
        operator_capability=OPERATOR_CAPABILITY,
        step_up_capability=STEP_UP_CAPABILITY,
        session_store=store,
    )

    with ThreadPoolExecutor(max_workers=8) as executor:
        principals = list(
            executor.map(
                lambda index: auth.issue(
                    _session_headers(actor=f"operator-{index}@example.com")
                ),
                range(64),
            )
        )

    retained = [
        auth.authenticate(
            {
                **_session_headers(actor=principal.actor),
                "Cookie": (
                    f"__Host-fluxtrade_session={principal.session_token}"
                ),
            }
        )
        for principal in principals
    ]

    assert sum(principal is not None for principal in retained) == 16


def test_stdlib_http_server_emits_secure_session_cookie():
    app = ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key="service-secret",
        browser_auth=_browser_auth(),
    )
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(app))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    request = Request(
        f"http://127.0.0.1:{server.server_port}/api/v1/auth/session",
        method="POST",
        headers=_session_headers(OPERATOR_CAPABILITY),
    )
    try:
        with urlopen(request, timeout=2) as response:
            payload = json.load(response)
            cookie = response.headers["Set-Cookie"]
            cache_control = response.headers["Cache-Control"]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)

    assert payload["actor"] == "operator@example.com"
    assert "Secure" in cookie
    assert "HttpOnly" in cookie
    assert "SameSite=Strict" in cookie
    assert cache_control == "no-store"


def test_browser_auth_environment_requires_explicit_complete_trust_config(
    monkeypatch,
):
    from src.control_plane import main as control_plane_main

    monkeypatch.setenv("CONTROL_PLANE_BROWSER_ORIGIN", ORIGIN)
    with pytest.raises(
        ValueError,
        match="browser auth settings require",
    ):
        control_plane_main.build_browser_session_auth_from_env()

    monkeypatch.setenv("CONTROL_PLANE_TRUSTED_PROXY_AUTH", "true")
    with pytest.raises(
        ValueError,
        match="requires browser origin and capability names",
    ):
        control_plane_main.build_browser_session_auth_from_env()

    monkeypatch.setenv(
        "CONTROL_PLANE_OPERATOR_CAPABILITY",
        OPERATOR_CAPABILITY,
    )
    monkeypatch.setenv(
        "CONTROL_PLANE_STEP_UP_CAPABILITY",
        STEP_UP_CAPABILITY,
    )
    auth = control_plane_main.build_browser_session_auth_from_env()

    assert auth is not None
    assert auth.allowed_origin == ORIGIN

    monkeypatch.setenv(
        "CONTROL_PLANE_STEP_UP_CAPABILITY",
        OPERATOR_CAPABILITY,
    )
    with pytest.raises(
        ValueError,
        match="operator and step-up capabilities must be distinct",
    ):
        control_plane_main.build_browser_session_auth_from_env()
