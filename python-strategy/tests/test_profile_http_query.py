from unittest.mock import Mock

import pytest

from src.control_plane import profile_http_query as service
from src.control_plane.app import ControlPlaneApp, HttpResponse
from src.core.market_data.profiles.live_validation import ValidatedLiveProfileQuery
from test_profile_http_contract import RAW
from test_profile_live_validation import fixture

PATH = "/api/v1/market-data/volume-profiles"
HEADERS = {"X-API-Key": "test-key"}


def test_service_order_and_single_calls(monkeypatch: pytest.MonkeyPatch) -> None:
    request, provider, query, _, start = fixture(monkeypatch)
    evidence = ValidatedLiveProfileQuery(query, start, start + 1, 1)
    events = []

    def clock():
        events.append("served")
        return start + 2

    parse = Mock(side_effect=lambda raw: events.append("parse") or request)
    validate = Mock(side_effect=lambda *a, **k: events.append("validate") or evidence)
    encode = Mock(side_effect=lambda *a, **k: events.append("encode") or b"{}")
    monkeypatch.setattr(service, "parse_profile_query", parse)
    monkeypatch.setattr(service, "validate_live_profile", validate)
    monkeypatch.setattr(service, "encode_profile_success", encode)
    monotonic = Mock()
    owner = service.ProfileQueryService(provider, utc_ms=clock, monotonic_ms=monotonic)
    assert owner.query(RAW) == b"{}"
    assert events == ["parse", "validate", "served", "encode"]
    parse.assert_called_once_with(RAW)
    validate.assert_called_once_with(
        provider, request, utc_ms=clock, monotonic_ms=monotonic
    )
    encode.assert_called_once_with(evidence, served_at_ms=start + 2)


@pytest.mark.parametrize(
    "reason,status",
    [
        ("NOT_READY", 404),
        ("PROFILE_EXPIRED", 404),
        ("SNAPSHOT_REVOKED", 409),
        ("QUERY_TOO_LARGE", 400),
        ("BACKEND_UNAVAILABLE", 503),
        ("INVALID_PROFILE", 503),
        ("CLOCK_UNCERTAIN", 503),
        ("VALIDATION_EXPIRED", 503),
    ],
)
def test_unavailable_no_served_clock(
    monkeypatch: pytest.MonkeyPatch,
    reason: str,
    status: int,
    caplog: pytest.LogCaptureFixture,
) -> None:
    cls = (
        service.LiveProfileValidationUnavailable
        if reason in ("CLOCK_UNCERTAIN", "VALIDATION_EXPIRED")
        else service.LiveProfileQueryUnavailable
    )
    monkeypatch.setattr(
        service, "validate_live_profile", Mock(return_value=cls(reason))
    )
    clock = Mock(side_effect=AssertionError("served clock forbidden"))
    result = service.ProfileQueryService(
        Mock(), utc_ms=clock, monotonic_ms=clock
    ).query(RAW)
    assert isinstance(result, service.ProfileHttpError) and result.status == status
    clock.assert_not_called()
    assert len(caplog.records) == (1 if status >= 500 else 0)
    if status >= 500:
        assert caplog.records[0].getMessage() == (
            f"profile_backend_failure phase=validate reason={reason}"
        )


def test_service_sanitizes_exception_not_baseexception(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    validate = Mock(side_effect=RuntimeError("SECRET"))
    monkeypatch.setattr(service, "validate_live_profile", validate)
    owner = service.ProfileQueryService(
        Mock(), utc_ms=lambda: 0, monotonic_ms=lambda: 0
    )
    assert owner.query(RAW) == service.ProfileHttpError(503, "BACKEND_UNAVAILABLE")
    validate.assert_called_once()
    assert [record.getMessage() for record in caplog.records] == [
        "profile_backend_failure phase=validate exception_type=RuntimeError"
    ]
    assert "SECRET" not in caplog.text
    assert caplog.records[0].exc_info is None
    caplog.clear()
    validate.side_effect = KeyboardInterrupt("SECRET")
    with pytest.raises(KeyboardInterrupt):
        owner.query(RAW)
    assert not caplog.records


@pytest.mark.parametrize(
    "status,code", [(503, "BACKEND_UNAVAILABLE"), (400, "QUERY_TOO_LARGE")]
)
def test_encoder_failure_logging(monkeypatch, caplog, status, code):
    _, provider, query, _, start = fixture(monkeypatch)
    evidence = ValidatedLiveProfileQuery(query, start, start, 0)
    monkeypatch.setattr(service, "validate_live_profile", Mock(return_value=evidence))
    error = service.ProfileHttpError(status, code)
    monkeypatch.setattr(service, "encode_profile_success", Mock(return_value=error))
    owner = service.ProfileQueryService(
        provider, utc_ms=lambda: start, monotonic_ms=lambda: 0
    )
    assert owner.query(RAW) is error
    assert [record.getMessage() for record in caplog.records] == (
        ["profile_backend_failure phase=encode code=BACKEND_UNAVAILABLE"]
        if status == 503
        else []
    )


def test_invalid_query_has_no_backend_log(caplog):
    owner = service.ProfileQueryService(
        Mock(), utc_ms=lambda: 0, monotonic_ms=lambda: 0
    )
    assert owner.query(b"SECRET=%") == service.ProfileHttpError(400, "INVALID_REQUEST")
    assert not caplog.records


def app(owner=None):
    return ControlPlaneApp(Mock(), api_key="test-key", profile_query_service=owner)


def test_auth_before_profile_handling_and_browser_policy(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = Mock()
    router = app(owner)
    assert router.handle("GET", PATH + "?bad=%", body="SECRET").status_code == 401
    owner.query.assert_not_called()
    policy = Mock(return_value=HttpResponse(403, {"error": "browser-policy"}))
    monkeypatch.setattr(router, "_authorize_browser_request", policy)
    assert router.handle("GET", PATH, headers=HEADERS).status_code == 403
    policy.assert_called_once()
    owner.query.assert_not_called()


@pytest.mark.parametrize("body", [b"x", "x", b"{}", "{}"])
def test_body_rejected_without_service(body: bytes | str) -> None:
    owner = Mock()
    assert app(owner).handle("GET", PATH, body=body, headers=HEADERS) == HttpResponse(
        400, {"error": "INVALID_REQUEST"}
    )
    owner.query.assert_not_called()


def test_unicode_missing_and_other_routes() -> None:
    owner = Mock()
    assert app(owner).handle("GET", PATH + "?x=é", headers=HEADERS).status_code == 400
    owner.query.assert_not_called()
    router = app()
    assert router.handle("GET", PATH, headers=HEADERS) == HttpResponse(
        503, {"error": "BACKEND_UNAVAILABLE"}
    )
    assert router.handle("POST", PATH, headers=HEADERS).status_code == 404
    assert router.handle("GET", "/health").status_code == 200


@pytest.mark.parametrize("body", [None, b"", ""])
def test_success_route(body: bytes | str | None) -> None:
    owner = Mock()
    owner.query.return_value = b'{"schema_version":1}'
    response = app(owner).handle(
        "GET", PATH + "?" + RAW.decode(), body=body, headers=HEADERS
    )
    assert response == HttpResponse(200, {"schema_version": 1})
    owner.query.assert_called_once_with(RAW)


@pytest.mark.parametrize(
    "result", [b"SECRET", b"[]", service.ProfileHttpError(400, "QUERY_TOO_LARGE")]
)
def test_route_error_projection(result: object) -> None:
    owner = Mock()
    owner.query.return_value = result
    response = app(owner).handle("GET", PATH, headers=HEADERS)
    expected = (
        "QUERY_TOO_LARGE"
        if type(result) is service.ProfileHttpError
        else "BACKEND_UNAVAILABLE"
    )
    assert response.body == {"error": expected}
    assert response.status_code == (
        400 if type(result) is service.ProfileHttpError else 503
    )


def test_real_validation_uses_third_utc_for_served(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import json

    _, provider, _, query, start = fixture(monkeypatch)
    events = []
    values = iter((start, start + 1, start + 2))

    def utc():
        events.append("utc")
        return next(values)

    def monotonic():
        events.append("mono")
        return 10

    owner = service.ProfileQueryService(provider, utc_ms=utc, monotonic_ms=monotonic)
    result = owner.query(RAW.replace(b"end_ms=86400000", b"end_ms=172800000"))
    assert type(result) is bytes and json.loads(result)["served_at_ms"] == start + 2
    assert events == ["utc", "mono", "mono", "utc", "utc"]
    query.assert_called_once()
