from __future__ import annotations

from typing import Any, Protocol, cast
from urllib.parse import urlsplit

import requests
from requests import PreparedRequest, Response

from src.core.adapters.okx_spider_demo_trading import OkxSpiderDemoRoutingError
from src.core.adapters.okx_spider_profile_capture import (
    _DEMO_HEADER,
    _REST,
    create_okx_spider_demo_profile_client,
)


_ALLOWED_PATHS = frozenset(
    {
        "/api/v5/account/positions",
        "/api/v5/trade/orders-pending",
        "/api/v5/trade/fills",
        "/api/v5/public/instruments",
    }
)


class _ReadClient(Protocol):
    urls: object
    headers: object
    session: requests.Session

    def privateGetAccountPositions(self, params: dict[str, str], /) -> object: ...

    def privateGetTradeOrdersPending(self, params: dict[str, str], /) -> object: ...

    def privateGetTradeFills(self, params: dict[str, str], /) -> object: ...

    def publicGetPublicInstruments(self, params: dict[str, str], /) -> object: ...


class _DemoReadRoutingSession(requests.Session):
    def send(self, request: PreparedRequest, **kwargs: Any) -> Response:
        headers = [
            value
            for key, value in request.headers.items()
            if type(key) is str and key.lower() == _DEMO_HEADER
        ]
        try:
            parsed = urlsplit(request.url or "")
        except ValueError:
            parsed = None
        if (
            request.method != "GET"
            or any(request.hooks.values())
            or parsed is None
            or parsed.scheme != "https"
            or parsed.netloc != "www.okx.com"
            or parsed.fragment != ""
            or parsed.path not in _ALLOWED_PATHS
            or any(type(key) is not str or key.lower() == "host" for key in request.headers)
            or len(headers) != 1
            or type(headers[0]) is not str
            or headers[0] != "1"
        ):
            raise OkxSpiderDemoRoutingError("request is outside OKX Demo read routing")
        return super().send(request, **kwargs)


def _has_demo_read_routing(client: object) -> bool:
    try:
        if type(getattr(client, "session")) is not _DemoReadRoutingSession:
            return False
        urls = getattr(client, "urls")
        headers = getattr(client, "headers")
        if type(urls) is not dict or type(headers) is not dict:
            return False
        api = urls["api"]
        if type(api) is not dict or api["rest"] != _REST:
            return False
        if any(type(key) is not str or key.lower() == "host" for key in headers):
            return False
        matches = [
            value
            for key, value in headers.items()
            if type(key) is str and key.lower() == _DEMO_HEADER
        ]
        return len(matches) == 1 and type(matches[0]) is str and matches[0] == "1"
    except Exception:
        return False


class OkxRestingDemoReadClient:
    def __init__(self, client: object) -> None:
        self._client = cast(_ReadClient, client)

    def _call(self, method: str, params: dict[str, str]) -> object:
        if not _has_demo_read_routing(self._client):
            raise OkxSpiderDemoRoutingError("invalid OKX Spider Demo read client")
        return cast(Any, self._client).__getattribute__(method)(params)

    def privateGetAccountPositions(self, params: dict[str, str], /) -> object:
        return self._call("privateGetAccountPositions", params)

    def privateGetTradeOrdersPending(self, params: dict[str, str], /) -> object:
        return self._call("privateGetTradeOrdersPending", params)

    def privateGetTradeFills(self, params: dict[str, str], /) -> object:
        return self._call("privateGetTradeFills", params)

    def publicGetPublicInstruments(self, params: dict[str, str], /) -> object:
        return self._call("publicGetPublicInstruments", params)


def create_okx_resting_demo_read_client(
    config: dict[str, object],
) -> OkxRestingDemoReadClient:
    client = create_okx_spider_demo_profile_client(config)
    requests_trust_env = getattr(client, "requests_trust_env")
    session = _DemoReadRoutingSession()
    session.trust_env = requests_trust_env
    setattr(client, "session", session)
    if not _has_demo_read_routing(client):
        raise ValueError("invalid OKX Spider Demo read client")
    return OkxRestingDemoReadClient(client)
