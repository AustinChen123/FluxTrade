"""HTTP consumption tests for immutable Spider configuration evidence."""

from __future__ import annotations

from src.control_plane.app import ControlPlaneApp
from src.control_plane.backtest_jobs import BacktestJobExecutor
from src.control_plane.spider_configuration_http import (
    SPIDER_CONFIGURATION_REQUEST_PATH,
    SpiderConfigurationHttpService,
)
from src.core.spider_management.configuration_request import (
    SpiderConfigurationRequest,
    SpiderConfigurationRequestKind,
)
from test_spider_configuration_request import configuration

API_KEY = "spider-http-test-key"


def _request(
    *,
    actor: str = "api_key",
    key: str = "request-c2",
    kind: SpiderConfigurationRequestKind = SpiderConfigurationRequestKind.PROPOSAL,
    config: dict | None = None,
    proposal_id: str | None = None,
    version: int = 7,
    environment: str = "research",
) -> SpiderConfigurationRequest:
    return SpiderConfigurationRequest.from_fields(
        environment=environment,
        execution_scope_id="account-main",
        strategy_id="spider-main",
        request_kind=kind,
        actor=actor,
        idempotency_key=key,
        expected_state_version=version,
        configuration=configuration() if config is None else config,
        proposal_request_id=proposal_id,
    )


def _app(
    service: SpiderConfigurationHttpService | None,
    *,
    api_key: str | None = API_KEY,
    browser_auth=None,
) -> ControlPlaneApp:
    return ControlPlaneApp(
        BacktestJobExecutor(run_inline=True),
        api_key=api_key,
        browser_auth=browser_auth,
        spider_configuration_http_service=service,
    )


def _api_headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {API_KEY}"}


def test_http_input_precedence_and_scope_actor_reject_before_database():
    session_factory_calls = 0

    def unavailable_sessions():
        nonlocal session_factory_calls
        session_factory_calls += 1
        raise RuntimeError("database must not be reached")

    service = SpiderConfigurationHttpService(
        unavailable_sessions,
        environment="research",
        execution_scope_id="account-main",
    )
    app = _app(service)

    malformed = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH, b"{}", headers=_api_headers()
    )
    assert malformed.status_code == 422
    assert malformed.body == {"error": "validation_error"}
    assert dict(malformed.headers) == {"Cache-Control": "no-store"}

    invalid_utf8 = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH, b"\xff",
        headers=_api_headers(),
    )
    assert invalid_utf8.status_code == 422
    duplicate_key = _request().canonical_bytes.replace(
        b'"actor":"api_key"', b'"actor":"forged","actor":"api_key"'
    )
    duplicate_json = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH, duplicate_key,
        headers=_api_headers(),
    )
    assert duplicate_json.status_code == 422

    with_query = app.handle(
        "POST",
        f"{SPIDER_CONFIGURATION_REQUEST_PATH}?unexpected=1",
        _request().canonical_bytes,
        headers=_api_headers(),
    )
    assert with_query.status_code == 422

    scope_mismatch = _request()
    scope_mismatch = SpiderConfigurationRequest.from_fields(
        environment="research",
        execution_scope_id="other-account",
        strategy_id="spider-main",
        request_kind=scope_mismatch.request_kind,
        actor=scope_mismatch.actor,
        idempotency_key=scope_mismatch.idempotency_key,
        expected_state_version=scope_mismatch.expected_state_version,
        configuration=scope_mismatch.configuration.value,
    )
    response = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
        scope_mismatch.canonical_bytes, headers=_api_headers()
    )
    assert response.status_code == 403
    assert response.body == {"error": "SCOPE_MISMATCH"}

    environment_mismatch = _request(environment="test")
    response = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
        environment_mismatch.canonical_bytes, headers=_api_headers()
    )
    assert response.status_code == 403
    assert response.body == {"error": "SCOPE_MISMATCH"}

    actor_mismatch = _request(actor="operator")
    response = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
        actor_mismatch.canonical_bytes, headers=_api_headers()
    )
    assert response.status_code == 403
    assert response.body == {"error": "ACTOR_MISMATCH"}

    oversized = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH, b"x" * 65537,
        headers=_api_headers(),
    )
    assert oversized.status_code == 422
    assert oversized.body == {"error": "validation_error"}

    noncanonical = _request().canonical_bytes + b" "
    response = app.handle(
        "POST", SPIDER_CONFIGURATION_REQUEST_PATH,
        noncanonical, headers=_api_headers(),
    )
    assert response.status_code == 422
    assert response.body == {"error": "validation_error"}
    assert session_factory_calls == 0


def test_spider_raw_target_aliases_never_reach_service():
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
    app = _app(service)
    body = _request().canonical_bytes
    aliases = (
        f"{SPIDER_CONFIGURATION_REQUEST_PATH}#fragment",
        f"{SPIDER_CONFIGURATION_REQUEST_PATH}\n",
        f"http://control.example{SPIDER_CONFIGURATION_REQUEST_PATH}",
        f"{SPIDER_CONFIGURATION_REQUEST_PATH}?",
        f"{SPIDER_CONFIGURATION_REQUEST_PATH}%2freadback",
        f"{SPIDER_CONFIGURATION_REQUEST_PATH}/",
    )
    for target in aliases:
        response = app.handle(
            "POST", target, body, headers=_api_headers()
        )
        assert response.status_code == 404
        assert dict(response.headers) == {"Cache-Control": "no-store"}
    assert database_calls == 0

    explicit_query = app.handle(
        "POST", f"{SPIDER_CONFIGURATION_REQUEST_PATH}?field=1",
        body, headers=_api_headers()
    )
    assert explicit_query.status_code == 422
    assert explicit_query.body == {"error": "validation_error"}
    assert database_calls == 0
