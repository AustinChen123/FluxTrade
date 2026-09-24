from dataclasses import fields, replace
import json
from typing import Any, cast
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles import snapshot_client as owner
from src.core.market_data.profiles.live_query import LiveProfileQueryUnavailable
from test_profile_wire import sample


def setup(status=200, body=None):
    request, evidence, raw = sample()
    transport = Mock()
    transport.get.return_value = owner.ProfileSnapshotResponse(
        status, raw if body is None else body
    )
    client = owner.ProfileSnapshotClient(
        transport, auth_header="X-API-Key", auth_token="SECRET"
    )
    return request, evidence, transport, client


@pytest.mark.parametrize("revision", [None, 1])
def test_exact_get_identity_timeout_and_auth(revision):
    request, evidence, transport, client = setup()
    request = replace(request, revision=revision)
    result = client.fetch(request)
    assert isinstance(result, owner.ProfileWireEvidence)
    assert result.validated.query.request is request
    assert result.validated.query.profile == evidence.query.profile
    expected = (
        b"/api/v1/market-data/volume-profiles?product_id=BINANCE%3ABTCUSDT-SPOT"
        b"&base_grid_id=btc_spot_usdt_10_v1&output_grid_id=btc_spot_usdt_10_v1"
        b"&algorithm_version=vp-v1&start_ms=0&end_ms=86400000&purpose=LIVE_QUERY"
        b"&freshness_policy_id=utc_complete_strict_v1"
    )
    transport.get.assert_called_once_with(
        expected + (b"&revision=1" if revision else b""),
        headers=(("X-API-Key", "SECRET"),),
        timeout_seconds=3.0,
        max_response_bytes=2 * 1024 * 1024,
    )
    assert "SECRET" not in repr(client) + repr(result) + repr(
        transport.get.return_value
    )


CASES = [
    (404, "PROFILE_NOT_READY", "NOT_READY"),
    (404, "PROFILE_EXPIRED", "PROFILE_EXPIRED"),
    (409, "SNAPSHOT_REVOKED", "SNAPSHOT_REVOKED"),
    (400, "QUERY_TOO_LARGE", "QUERY_TOO_LARGE"),
    (503, "BACKEND_UNAVAILABLE", "BACKEND_UNAVAILABLE"),
    (400, "INVALID_REQUEST", None),
]


@pytest.mark.parametrize("status", [200, 201, 301, 400, 401, 404, 409, 500, 503])
@pytest.mark.parametrize("expected_status,code,reason", CASES)
def test_complete_status_code_matrix(status, expected_status, code, reason):
    request, _, transport, client = setup(status, json.dumps({"error": code}).encode())
    if status == expected_status and reason is None:
        with pytest.raises(
            owner.ProfileSnapshotClientError, match="^PROFILE_SNAPSHOT_INVALID_REQUEST$"
        ):
            client.fetch(request)
    else:
        expected = (
            "INVALID_PROFILE"
            if status == 200
            else reason
            if status == expected_status
            else "BACKEND_UNAVAILABLE"
        )
        result = client.fetch(request)
        assert result == LiveProfileQueryUnavailable(request, expected)
        assert isinstance(result, LiveProfileQueryUnavailable)
        assert result.request is request
    assert transport.get.call_count == 1


@pytest.mark.parametrize(
    "body",
    [
        b"\xff",
        b"[]",
        b"null",
        b"{}",
        b'{"error":"SECRET"}',
        b'{"error":"BACKEND_UNAVAILABLE","error":"BACKEND_UNAVAILABLE"}',
        b'{"error":"BACKEND_UNAVAILABLE","extra":0}',
        b'{"error":true}',
        b'{"error":1.0}',
        b'{"error":NaN}',
        b'{"error":{"error":"BACKEND_UNAVAILABLE"}}',
    ],
)
def test_bad_error_envelope(body):
    request, _, _, client = setup(503, body)
    result = client.fetch(request)
    assert result == LiveProfileQueryUnavailable(request, "BACKEND_UNAVAILABLE")
    assert "SECRET" not in repr(result)


@pytest.mark.parametrize("error", [RuntimeError("SECRET"), TimeoutError("SECRET")])
def test_transport_failure_sanitized_without_retry(error):
    request, _, transport, client = setup()
    transport.get.side_effect = error
    result = client.fetch(request)
    assert result == LiveProfileQueryUnavailable(request, "BACKEND_UNAVAILABLE")
    assert "SECRET" not in repr(result)
    transport.get.assert_called_once()
    transport.get.side_effect = KeyboardInterrupt("SECRET")
    with pytest.raises(KeyboardInterrupt):
        client.fetch(request)


def test_body_admission_before_decoder_and_malformed_success(monkeypatch):
    request, _, transport, client = setup(200, b"x" * (2 * 1024 * 1024 + 1))
    decode = Mock(side_effect=AssertionError("decoder forbidden"))
    with monkeypatch.context() as patch:
        patch.setattr(owner, "decode_live_profile_response", decode)
        assert client.fetch(request) == LiveProfileQueryUnavailable(
            request, "QUERY_TOO_LARGE"
        )
        decode.assert_not_called()
    transport.get.return_value = owner.ProfileSnapshotResponse(200, b"SECRET")
    assert client.fetch(request) == LiveProfileQueryUnavailable(
        request, "INVALID_PROFILE"
    )
    transport.get.return_value = None
    assert client.fetch(request) == LiveProfileQueryUnavailable(
        request, "BACKEND_UNAVAILABLE"
    )


def test_invalid_request_and_credentials_never_io():
    request, _, transport, client = setup()
    child = type("Child", (type(request),), {})(
        **{f.name: getattr(request, f.name) for f in fields(request)}
    )
    for bad in (
        None,
        child,
        replace(request, freshness_policy_id="other"),
        replace(
            request,
            purpose="MODELED_RESEARCH",
            freshness_policy_id=None,
            availability_policy_id="modeled",
            as_of_ms=request.end_ms,
        ),
    ):
        with pytest.raises(owner.ProfileSnapshotClientError):
            client.fetch(cast(Any, bad))
    for header, token in (
        ("X\nSECRET", "SECRET"),
        ("X", "SECRET\r"),
        ("X", ""),
        (True, "SECRET"),
    ):
        with pytest.raises(owner.ProfileSnapshotClientError) as error:
            owner.ProfileSnapshotClient(
                transport, auth_header=cast(Any, header), auth_token=token
            )
        assert "SECRET" not in str(error.value)
    transport.get.assert_not_called()


def test_exact_success_size_limit():
    request, _, transport, client = setup()
    raw = transport.get.return_value.body
    transport.get.return_value = owner.ProfileSnapshotResponse(
        200, raw + b" " * (2 * 1024 * 1024 - len(raw))
    )
    assert isinstance(client.fetch(request), owner.ProfileWireEvidence)


@pytest.mark.parametrize("status", [200, 400, 404, 503])
@pytest.mark.parametrize("size", [2 * 1024 * 1024, 2 * 1024 * 1024 + 1])
def test_crossed_status_body_limits(status, size):
    request, _, _, client = setup(status, b"x" * size)
    reason = (
        ("QUERY_TOO_LARGE" if size > 2 * 1024 * 1024 else "INVALID_PROFILE")
        if status == 200
        else "BACKEND_UNAVAILABLE"
    )
    assert client.fetch(request) == LiveProfileQueryUnavailable(request, reason)


@pytest.mark.parametrize(
    "size,reason", [(1024, "NOT_READY"), (1025, "BACKEND_UNAVAILABLE")]
)
def test_error_envelope_limit_before_parser(monkeypatch, size, reason):
    body = b'{"error":"PROFILE_NOT_READY"}'
    request, _, _, client = setup(404, body + b" " * (size - len(body)))
    parser = Mock(wraps=owner.json.loads)
    monkeypatch.setattr(owner.json, "loads", parser)
    assert client.fetch(request) == LiveProfileQueryUnavailable(request, reason)
    assert parser.call_count == (1 if size == 1024 else 0)


def test_auth_token_limit_without_io():
    transport = Mock()
    token = "S" * 4096
    client = owner.ProfileSnapshotClient(
        transport, auth_header="X-API-Key", auth_token=token
    )
    assert owner._MAX_AUTH_TOKEN_BYTES == 4096
    assert token not in repr(client)
    with pytest.raises(owner.ProfileSnapshotClientError) as error:
        owner.ProfileSnapshotClient(
            transport, auth_header="X-API-Key", auth_token=token + "S"
        )
    assert str(error.value) == "PROFILE_SNAPSHOT_INVALID_REQUEST"
    assert token not in str(error.value)
    transport.get.assert_not_called()
