"""Wire-level compatibility checks for the unchanged stdlib HTTP transport."""

from http.server import ThreadingHTTPServer
from threading import Thread
from unittest.mock import MagicMock
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from src.control_plane.server import make_handler
from src.control_plane.spider_configuration_http import (
    SPIDER_CONFIGURATION_REQUEST_PATH,
    SpiderConfigurationHttpService,
)
from test_spider_configuration_http import _api_headers, _app


def test_stdlib_http_dispatch_keeps_auth_validation_no_store_and_put_501():
    database_calls = MagicMock()
    service = SpiderConfigurationHttpService(
        lambda: database_calls(),
        environment="research",
        execution_scope_id="account-main",
    )
    app = _app(service)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(app))
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    root = f"http://127.0.0.1:{server.server_port}"
    try:
        get_request = Request(
            f"{root}{SPIDER_CONFIGURATION_REQUEST_PATH}", headers=_api_headers()
        )
        with pytest.raises(HTTPError) as exc_info:
            urlopen(get_request, timeout=2)
        assert exc_info.value.code == 404
        assert exc_info.value.headers["Cache-Control"] == "no-store"
        exc_info.value.close()

        unauthorized = Request(
            f"{root}{SPIDER_CONFIGURATION_REQUEST_PATH}",
            data=b"{}",
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(HTTPError) as exc_info:
            urlopen(unauthorized, timeout=2)
        assert exc_info.value.code == 401
        assert exc_info.value.headers["Cache-Control"] == "no-store"
        exc_info.value.close()

        request = Request(
            f"{root}{SPIDER_CONFIGURATION_REQUEST_PATH}",
            data=b"{}",
            headers={**_api_headers(), "Content-Type": "application/json"},
            method="POST",
        )
        with pytest.raises(HTTPError) as exc_info:
            urlopen(request, timeout=2)
        assert exc_info.value.code == 422
        assert exc_info.value.headers["Cache-Control"] == "no-store"
        exc_info.value.close()

        unsupported = Request(
            f"{root}{SPIDER_CONFIGURATION_REQUEST_PATH}",
            headers=_api_headers(),
            method="PUT",
        )
        with pytest.raises(HTTPError) as exc_info:
            urlopen(unsupported, timeout=2)
        assert exc_info.value.code == 501
        assert exc_info.value.headers.get("Cache-Control") is None
        exc_info.value.close()
        database_calls.assert_not_called()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)
        app.shutdown(timeout=0)
