from http.client import HTTPMessage
from io import BytesIO
from unittest.mock import Mock

import pytest

from src.control_plane.app import HttpResponse
from src.control_plane.server import _invalid_profile_body, make_handler

PATH = "/api/v1/market-data/volume-profiles"


def headers(*pairs: tuple[str, str]) -> HTTPMessage:
    result = HTTPMessage()
    for key, value in pairs:
        result[key] = value
    return result


@pytest.mark.parametrize(
    "value",
    ["-1", "", " ", " 0", "0 ", "00", "+0", "SECRET", "1", "9999999999999999999999"],
)
def test_noncanonical_length_rejected(value: str) -> None:
    assert _invalid_profile_body("GET", PATH, headers(("Content-Length", value)))


@pytest.mark.parametrize("value", ["", "chunked", "identity", "SECRET"])
def test_transfer_encoding_always_rejected(value: str) -> None:
    assert _invalid_profile_body(
        "GET", PATH, headers(("transfer-encoding", value), ("content-length", "0"))
    )


def test_duplicate_lengths_rejected() -> None:
    assert _invalid_profile_body(
        "GET", PATH, headers(("Content-Length", "0"), ("Content-Length", "0"))
    )


@pytest.mark.parametrize("path", [PATH, PATH + "?x=1", PATH + "/?x=1"])
def test_exact_normalized_endpoint(path: str) -> None:
    assert _invalid_profile_body("GET", path, headers(("Content-Length", "1")))
    assert not _invalid_profile_body("GET", path, headers())
    assert not _invalid_profile_body("GET", path, headers(("Content-Length", "0")))


@pytest.mark.parametrize(
    "method,path",
    [
        ("POST", PATH),
        ("GET", PATH + "-extra"),
        ("GET", PATH + "/child"),
        ("GET", "/other"),
    ],
)
def test_other_routes_unchanged(method: str, path: str) -> None:
    assert not _invalid_profile_body(
        method, path, headers(("Transfer-Encoding", "chunked"))
    )


class UnreadBody(BytesIO):
    def read(self, size: int | None = -1) -> bytes:
        raise AssertionError("body must remain unread")


def handler(app: Mock, message: HTTPMessage, *, path: str = PATH):
    cls = make_handler(app)
    instance = cls.__new__(cls)
    instance.command, instance.path, instance.headers = "GET", path, message
    instance.request_version = "HTTP/1.1"
    instance.requestline = "GET " + path + " HTTP/1.1"
    instance.rfile, instance.wfile = UnreadBody(b"SECRET"), BytesIO()
    return instance


@pytest.mark.parametrize(
    "message",
    [
        headers(("Content-Length", "1")),
        headers(("Content-Length", "SECRET")),
        headers(("Transfer-Encoding", "chunked")),
    ],
)
def test_rejection_before_read_or_app(message: HTTPMessage) -> None:
    app = Mock()
    instance = handler(app, message)
    getattr(instance, "_handle")()
    app.handle.assert_not_called()
    assert instance.rfile.tell() == 0 and instance.close_connection
    assert isinstance(instance.wfile, BytesIO)
    wire = instance.wfile.getvalue()
    assert wire.startswith(b"HTTP/1.0 400")
    assert wire.split(b"\r\n\r\n", 1)[1] == b'{"error":"INVALID_REQUEST"}'
    assert b"SECRET" not in wire


@pytest.mark.parametrize("message", [headers(), headers(("Content-Length", "0"))])
def test_empty_request_reaches_app_without_read(message: HTTPMessage) -> None:
    app = Mock()
    app.handle.return_value = HttpResponse(200, {"ok": True})
    instance = handler(app, message, path=PATH + "?x=1")
    getattr(instance, "_handle")()
    app.handle.assert_called_once_with(
        "GET", PATH + "?x=1", None, dict(message.items())
    )
    assert instance.rfile.tell() == 0
