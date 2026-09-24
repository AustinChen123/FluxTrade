from typing import Any, cast
from unittest.mock import Mock

import pytest

from src.core.market_data.profiles import snapshot_http_transport as owner


def invoke(transport, **changes):
    args = dict(
        headers=(("X-API-Key", "SECRET"),), timeout_seconds=3.0, max_response_bytes=4
    )
    args.update(changes)
    return transport.get(args.pop("target", b"/api?q=1"), **args)


@pytest.fixture
def connections(monkeypatch):
    factories = [Mock(), Mock()]
    monkeypatch.setattr(owner.http.client, "HTTPConnection", factories[0])
    monkeypatch.setattr(owner.http.client, "HTTPSConnection", factories[1])
    for factory in factories:
        response = factory.return_value.getresponse.return_value
        response.status = 200
        response.read.return_value = b"data"
    return factories


@pytest.mark.parametrize("scheme,index,port", [("http", 0, None), ("https", 1, 8443)])
@pytest.mark.parametrize(
    "status,body", [(200, b"data"), (200, b"extra"), (302, b"data")]
)
def test_single_bounded_request(connections, scheme, index, port, status, body):
    origin = f"{scheme}://internal" + (f":{port}" if port else "")
    transport = owner.ProfileSnapshotHttpTransport(origin)
    factory = connections[index]
    response = factory.return_value.getresponse.return_value
    response.status, response.read.return_value = status, body
    result = invoke(transport)
    assert (result.status, result.body) == (status, body)
    factory.assert_called_once_with("internal", port=port, timeout=3.0)
    connections[1 - index].assert_not_called()
    factory.return_value.request.assert_called_once_with(
        "GET", "/api?q=1", headers={"X-API-Key": "SECRET"}
    )
    response.read.assert_called_once_with(5)
    response.close.assert_called_once()
    factory.return_value.close.assert_called_once()
    assert "SECRET" not in repr(transport) + repr(result)


@pytest.mark.parametrize(
    "phase", ["request", "getresponse", "read", "response_close", "close"]
)
def test_cleanup_and_error_identity(connections, phase):
    connection = connections[0].return_value
    response = connection.getresponse.return_value
    error = RuntimeError("SECRET")
    action = (
        response.close
        if phase == "response_close"
        else (response.read if phase == "read" else getattr(connection, phase))
    )
    action.side_effect = error
    with pytest.raises(RuntimeError) as caught:
        invoke(owner.ProfileSnapshotHttpTransport("http://internal"))
    assert caught.value is error
    connection.close.assert_called_once()
    assert response.close.call_count == (
        0 if phase in ("request", "getresponse") else 1
    )


@pytest.mark.parametrize(
    "origin",
    [
        None,
        True,
        "",
        "ftp://internal",
        "http:///",
        "http://u:SECRET@host",
        "http://host/path",
        "http://host?",
        "http://host#",
        "http://host:",
        "http://host:0",
        "http://host:65536",
        "http://host:bad",
        "http://ho st",
        "http://host\n",
        "http://[broken",
    ],
)
def test_invalid_origin_no_io(connections, origin):
    with pytest.raises(ValueError, match="^PROFILE_HTTP_TRANSPORT_INVALID$"):
        owner.ProfileSnapshotHttpTransport(cast(Any, origin))
    for factory in connections:
        factory.assert_not_called()


@pytest.mark.parametrize(
    "changes",
    [
        {"target": " /"},
        {"target": b"//host"},
        {"target": b"http://host"},
        {"target": b"/\r\nSECRET"},
        {"target": b"/\x00"},
        {"target": b"/\xff"},
        {"timeout_seconds": True},
        {"timeout_seconds": 0},
        {"timeout_seconds": 31},
        {"timeout_seconds": float("nan")},
        {"timeout_seconds": float("inf")},
        {"max_response_bytes": True},
        {"max_response_bytes": 0},
        {"max_response_bytes": 2097153},
        {"headers": []},
        {"headers": (("X",),)},
        {"headers": (("X", "a"), ("x", "b"))},
        {"headers": (("X\r", "SECRET"),)},
        {"headers": (("X", "SECRET\n"),)},
        {"headers": (("Host", "other"),)},
        {"headers": (("Content-Length", "1"),)},
        {"headers": (("Transfer-Encoding", "chunked"),)},
    ],
)
def test_invalid_call_no_io(connections, changes):
    transport = owner.ProfileSnapshotHttpTransport("https://internal")
    with pytest.raises(ValueError) as caught:
        invoke(transport, **changes)
    assert str(caught.value) == "PROFILE_HTTP_TRANSPORT_INVALID"
    for factory in connections:
        factory.assert_not_called()


def test_each_call_fresh_connection(connections):
    transport = owner.ProfileSnapshotHttpTransport("http://internal")
    invoke(transport)
    invoke(transport)
    assert connections[0].call_count == 2


def test_invalid_response_status_still_closes(connections):
    connection = connections[0].return_value
    response = connection.getresponse.return_value
    response.status = True
    with pytest.raises(ValueError):
        invoke(owner.ProfileSnapshotHttpTransport("http://internal"))
    response.close.assert_called_once()
    connection.close.assert_called_once()
