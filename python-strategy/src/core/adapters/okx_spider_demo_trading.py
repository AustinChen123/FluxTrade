"""OKX Demo routed Spider batch transport."""

from __future__ import annotations

from typing import Any, Protocol, cast

import requests
from requests import PreparedRequest, Response

from src.core.adapters.okx_spider_profile_capture import (
    _DEMO_HEADER,
    _REST,
    create_okx_spider_demo_profile_client,
)
from src.core.adapters.okx_spider_transport import (
    OkxBatchCallResult,
    OkxSpiderBatchTransport,
)


class OkxSpiderDemoRoutingError(RuntimeError):
    """The client no longer has the required OKX Demo routing identity."""


class _DemoClient(Protocol):
    urls: object
    headers: object
    session: requests.Session
    requests_trust_env: bool

    def privatePostTradeBatchOrders(
        self, orders: list[dict[str, object]]
    ) -> object: ...

    def privatePostTradeCancelBatchOrders(
        self, orders: list[dict[str, object]]
    ) -> object: ...


class _DemoRoutingSession(requests.Session):
    def send(self, request: PreparedRequest, **kwargs: Any) -> Response:
        matches = [
            value
            for key, value in request.headers.items()
            if type(key) is str and key.lower() == _DEMO_HEADER
        ]
        if (
            request.method != "POST"
            or any(request.hooks.values())
            or request.url
            not in {
                f"{_REST}/api/v5/trade/batch-orders",
                f"{_REST}/api/v5/trade/cancel-batch-orders",
            }
            or len(matches) != 1
            or type(matches[0]) is not str
            or matches[0] != "1"
        ):
            raise OkxSpiderDemoRoutingError("request is outside OKX Demo batch routing")
        return super().send(request, **kwargs)


def _install_guard_session(client: object) -> None:
    requests_trust_env = getattr(client, "requests_trust_env")
    session = _DemoRoutingSession()
    session.trust_env = requests_trust_env
    setattr(client, "session", session)


def _has_demo_routing(client: object) -> bool:
    try:
        if type(getattr(client, "session")) is not _DemoRoutingSession:
            return False
        urls = getattr(client, "urls")
        headers = getattr(client, "headers")
        if type(urls) is not dict or type(headers) is not dict:
            return False
        api = urls["api"]
        if type(api) is not dict or api["rest"] != _REST:
            return False
        matches = [
            value
            for key, value in headers.items()
            if type(key) is str and key.lower() == _DEMO_HEADER
        ]
        return len(matches) == 1 and type(matches[0]) is str and matches[0] == "1"
    except Exception:
        return False


def create_okx_spider_demo_batch_transport(
    config: dict[str, object],
) -> OkxSpiderDemoBatchTransport:
    client = create_okx_spider_demo_profile_client(config)
    _install_guard_session(client)
    if not _has_demo_routing(client):
        raise ValueError("invalid OKX Spider Demo trading client")
    return OkxSpiderDemoBatchTransport(client)


class OkxSpiderDemoBatchTransport:
    def __init__(self, client: object) -> None:
        self._client = client
        self._transport = OkxSpiderBatchTransport(cast(_DemoClient, client))

    def submit(self, request_payload: bytes) -> OkxBatchCallResult:
        if not _has_demo_routing(self._client):
            raise OkxSpiderDemoRoutingError("invalid OKX Spider Demo trading client")
        return self._transport.submit(request_payload)
