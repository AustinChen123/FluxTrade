from typing import Any, cast

import ccxt
import pytest
import requests
from requests.adapters import BaseAdapter
from requests.auth import HTTPDigestAuth
from requests import PreparedRequest, Response

import src.core.adapters.okx_spider_demo_trading as demo_trading
from src.core.adapters.okx_spider_demo_trading import (
    OkxSpiderDemoRoutingError,
    create_okx_spider_demo_batch_transport,
)
from src.core.adapters.okx_spider_profile_capture import _REST
from src.core.adapters.okx_spider_transport import (
    OkxSpiderBatchTransport,
    create_okx_spider_batch_client,
)
from tests.test_okx_spider_transport import (
    _batch,
    _http_response,
    _two_member_cancel_batch,
)


_CONFIG: dict[str, object] = {
    "apiKey": "dummy-api-key",
    "secret": "dummy-secret",
    "password": "dummy-password",
}


class _RecordingAdapter(BaseAdapter):
    def __init__(self, response: Response | None = None) -> None:
        self.requests: list[PreparedRequest] = []
        self.response = response or _http_response(
            200, b'{"code":"0","data":[],"msg":""}'
        )

    def send(
        self,
        request: PreparedRequest,
        stream: bool = False,
        timeout: object = None,
        verify: object = True,
        cert: object = None,
        proxies: object = None,
    ) -> Response:
        self.requests.append(request)
        response = self.response
        response.request = request
        response.url = request.url or ""
        return response

    def close(self) -> None:
        return None


def _mount_adapter(
    client: ccxt.okx, response: Response | None = None
) -> _RecordingAdapter:
    adapter = _RecordingAdapter(response)
    session = cast(requests.Session, client.session)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return adapter


def test_factory_rechecks_returned_client_routing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = ccxt.okx({})
    monkeypatch.setattr(
        demo_trading, "create_okx_spider_demo_profile_client", lambda _config: client
    )
    with pytest.raises(ValueError, match="invalid OKX Spider Demo trading client"):
        create_okx_spider_demo_batch_transport(dict(_CONFIG))


def test_factory_replaces_supplied_session_and_preserves_trust_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = ccxt.okx({})
    old_session = cast(requests.Session, client.session)
    old_adapter = _RecordingAdapter()
    old_session.mount("https://", old_adapter)
    client.headers["x-simulated-trading"] = "1"
    cast(dict[str, object], cast(dict[str, object], client.urls)["api"])["rest"] = _REST
    client.requests_trust_env = False
    monkeypatch.setattr(
        demo_trading, "create_okx_spider_demo_profile_client", lambda _config: client
    )
    create_okx_spider_demo_batch_transport(dict(_CONFIG))
    assert type(client.session) is demo_trading._DemoRoutingSession
    assert client.session is not old_session
    assert client.session.trust_env is False
    assert client.session.get_adapter("https://www.okx.com") is not old_adapter


def test_factory_locks_client_and_place_cancel_each_make_one_demo_http_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = create_okx_spider_demo_batch_transport(dict(_CONFIG))
    client = cast(ccxt.okx, transport._client)
    assert client.headers["x-simulated-trading"] == "1"
    api_urls = cast(dict[str, object], cast(dict[str, object], client.urls)["api"])
    assert api_urls["rest"] == _REST
    adapter = _mount_adapter(client)
    transport.submit(_batch())
    transport.submit(_two_member_cancel_batch())
    assert len(adapter.requests) == 2
    assert adapter.requests[0].url == f"{_REST}/api/v5/trade/batch-orders"
    assert adapter.requests[1].url == f"{_REST}/api/v5/trade/cancel-batch-orders"
    for request in adapter.requests:
        assert request.headers["x-simulated-trading"] == "1"


@pytest.mark.parametrize(
    ("config", "accepted"),
    [
        ({"headers": {"x-simulated-trading": "0"}}, False),
        ({"headers": {"X-Simulated-Trading": "0"}}, False),
        ({"urls": {"api": {"rest": "https://aws.okx.com"}}}, False),
        ({"headers": {"x-simulated-trading": "1"}}, True),
        ({"urls": {"api": {"rest": _REST}}}, True),
    ],
)
def test_factory_rejections_have_legal_twins(
    config: dict[str, object], accepted: bool
) -> None:
    merged = dict(_CONFIG)
    merged.update(config)
    if accepted:
        create_okx_spider_demo_batch_transport(merged)
    else:
        with pytest.raises(ValueError, match="invalid OKX Spider Demo profile client"):
            create_okx_spider_demo_batch_transport(merged)


@pytest.mark.parametrize("operation", ["place", "cancel"])
@pytest.mark.parametrize(
    "mutation",
    ["removed", "zero", "integer", "duplicate", "near_match", "rest", "missing_api"],
)
def test_submit_rechecks_routing_before_any_http_call(
    monkeypatch: pytest.MonkeyPatch, mutation: str, operation: str
) -> None:
    transport = create_okx_spider_demo_batch_transport(dict(_CONFIG))
    client = cast(ccxt.okx, transport._client)
    adapter = _mount_adapter(client)
    client_calls: list[str] = []
    for method_name in (
        "privatePostTradeBatchOrders",
        "privatePostTradeCancelBatchOrders",
    ):
        original = getattr(client, method_name)

        def record_call(
            *args: object, _original: Any = original, _name: str = method_name
        ) -> object:
            client_calls.append(_name)
            return _original(*args)

        monkeypatch.setattr(client, method_name, record_call)
    if mutation == "removed":
        del client.headers["x-simulated-trading"]
    elif mutation == "zero":
        client.headers["x-simulated-trading"] = "0"
    elif mutation == "integer":
        client.headers["x-simulated-trading"] = 1
    elif mutation == "duplicate":
        client.headers["X-SIMULATED-TRADING"] = "1"
    elif mutation == "near_match":
        del client.headers["x-simulated-trading"]
        client.headers["x-simulated-trading-old"] = "1"
    elif mutation == "rest":
        api_urls = cast(dict[str, object], cast(dict[str, object], client.urls)["api"])
        api_urls["rest"] = "https://aws.okx.com"
    else:
        cast(dict[str, object], client.urls).pop("api")
    payload = _batch() if operation == "place" else _two_member_cancel_batch()
    with pytest.raises(OkxSpiderDemoRoutingError):
        transport.submit(payload)
    assert client_calls == []
    assert adapter.requests == []


def test_unmutated_submit_is_legal_twin(monkeypatch: pytest.MonkeyPatch) -> None:
    transport = create_okx_spider_demo_batch_transport(dict(_CONFIG))
    client = cast(ccxt.okx, transport._client)
    adapter = _mount_adapter(client)
    transport.submit(_batch())
    assert len(adapter.requests) == 1


def test_submit_accepts_single_case_variant_header_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = create_okx_spider_demo_batch_transport(dict(_CONFIG))
    client = cast(ccxt.okx, transport._client)
    client.headers.pop("x-simulated-trading")
    client.headers["X-SIMULATED-TRADING"] = "1"
    adapter = _mount_adapter(client)
    transport.submit(_batch())
    assert len(adapter.requests) == 1


def test_submit_returns_underlying_transport_result_field_by_field(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    wrapped = create_okx_spider_demo_batch_transport(dict(_CONFIG))
    direct_client = create_okx_spider_batch_client(
        {
            **_CONFIG,
            "headers": {"x-simulated-trading": "1"},
            "urls": {"api": {"rest": _REST}},
        }
    )
    body = b'{"code":"0","data":[],"msg":""}'

    _mount_adapter(cast(ccxt.okx, wrapped._client), _http_response(200, body))
    _mount_adapter(direct_client, _http_response(200, body))
    wrapped_result = wrapped.submit(_batch())
    direct_result = OkxSpiderBatchTransport(cast(Any, direct_client)).submit(_batch())
    assert type(wrapped_result) is type(direct_result)
    assert type(wrapped_result.response_payload) is type(direct_result.response_payload)
    assert wrapped_result.response_payload == direct_result.response_payload
    assert type(wrapped_result.response_failure) is type(direct_result.response_failure)
    assert wrapped_result.response_failure == direct_result.response_failure
    assert type(wrapped_result.member_outcomes) is type(direct_result.member_outcomes)
    assert wrapped_result.member_outcomes == direct_result.member_outcomes


@pytest.mark.parametrize("operation", ["place", "cancel"])
@pytest.mark.parametrize("stage", ["factory", "mutation"])
@pytest.mark.parametrize(
    "bypass",
    [
        {"userAgent": {"User-Agent": "offline-review", "x-simulated-trading": "0"}},
        {"proxyUrl": "https://offline.invalid/"},
    ],
)
def test_ccxt_header_and_url_rewrites_are_blocked(
    operation: str, stage: str, bypass: dict[str, object]
) -> None:
    config = {**_CONFIG, **(bypass if stage == "factory" else {})}
    transport = create_okx_spider_demo_batch_transport(config)
    client = cast(ccxt.okx, transport._client)
    adapter = _mount_adapter(client)
    if stage == "mutation":
        for key, value in bypass.items():
            setattr(client, key, value)
    payload = _batch() if operation == "place" else _two_member_cancel_batch()
    with pytest.raises(OkxSpiderDemoRoutingError):
        transport.submit(payload)
    assert adapter.requests == []


@pytest.mark.parametrize(
    "user_agent", ["offline-review", {"User-Agent": "offline-review"}]
)
@pytest.mark.parametrize("operation", ["place", "cancel"])
def test_legal_user_agent_variants_send_demo_request(
    user_agent: object, operation: str
) -> None:
    transport = create_okx_spider_demo_batch_transport(
        {**_CONFIG, "userAgent": user_agent}
    )
    client = cast(ccxt.okx, transport._client)
    adapter = _mount_adapter(client)
    transport.submit(_batch() if operation == "place" else _two_member_cancel_batch())
    assert len(adapter.requests) == 1
    assert adapter.requests[0].headers["x-simulated-trading"] == "1"


@pytest.mark.parametrize(
    ("method", "url", "headers"),
    [
        pytest.param(
            "GET",
            f"{_REST}/api/v5/trade/batch-orders",
            {"x-simulated-trading": "1"},
            id="wrong-method",
        ),
        pytest.param(
            "POST",
            f"{_REST}/api/v5/trade/batch-orders?x=1",
            {"x-simulated-trading": "1"},
            id="query-url",
        ),
        pytest.param(
            "POST",
            f"{_REST}/api/v5/account/balance",
            {"x-simulated-trading": "1"},
            id="other-path",
        ),
        pytest.param(
            "POST",
            "http://www.okx.com/api/v5/trade/batch-orders",
            {"x-simulated-trading": "1"},
            id="http-scheme",
        ),
        pytest.param(
            "POST",
            "https://www.okx.com.evil.test/api/v5/trade/batch-orders",
            {"x-simulated-trading": "1"},
            id="host-suffix",
        ),
        pytest.param(
            "POST",
            f"{_REST}/api/v5/trade/batch-orders",
            {"x-simulated-trading-old": "1"},
            id="near-match-header-key",
        ),
        pytest.param(
            "POST", f"{_REST}/api/v5/trade/batch-orders", {}, id="missing-header"
        ),
        pytest.param(
            "POST",
            f"{_REST}/api/v5/trade/batch-orders",
            {"x-simulated-trading": "0"},
            id="header-zero",
        ),
        pytest.param(
            "POST",
            f"{_REST}/api/v5/trade/batch-orders",
            {"x-simulated-trading": " 1"},
            id="header-whitespace",
        ),
        pytest.param(
            "POST",
            f"{_REST}/api/v5/trade/batch-orders",
            {"x-simulated-trading": 1},
            id="header-integer",
        ),
    ],
)
def test_wire_guard_rejects_near_match_requests(
    method: str, url: str, headers: dict[str, object]
) -> None:
    session = demo_trading._DemoRoutingSession()
    adapter = _RecordingAdapter()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    invalid_space_value = headers.get("x-simulated-trading") == " 1"
    invalid_type_value = headers.get("x-simulated-trading") == 1
    prepared_headers = {} if invalid_space_value or invalid_type_value else headers
    request = requests.Request(
        method, url, headers=cast(Any, prepared_headers)
    ).prepare()
    if invalid_space_value:
        request.headers["x-simulated-trading"] = " 1"
    if invalid_type_value:
        cast(Any, request.headers)["x-simulated-trading"] = 1
    with pytest.raises(OkxSpiderDemoRoutingError):
        session.send(request)
    assert adapter.requests == []
    legal = requests.Request(
        "POST",
        f"{_REST}/api/v5/trade/batch-orders",
        headers={"x-simulated-trading": "1"},
    ).prepare()
    session.send(legal)
    assert len(adapter.requests) == 1


def test_redirect_is_rejected_after_first_demo_request() -> None:
    redirect = _http_response(307, b"")
    redirect.headers["Location"] = "https://offline.invalid/x"
    transport = create_okx_spider_demo_batch_transport(dict(_CONFIG))
    client = cast(ccxt.okx, transport._client)
    adapter = _mount_adapter(client, redirect)
    with pytest.raises(OkxSpiderDemoRoutingError):
        transport.submit(_batch())
    assert len(adapter.requests) == 1


def test_replaced_session_is_rejected_before_client_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    transport = create_okx_spider_demo_batch_transport(dict(_CONFIG))
    client = cast(ccxt.okx, transport._client)
    guarded_adapter = _mount_adapter(client)
    replacement = requests.Session()
    replacement_adapter = _RecordingAdapter()
    replacement.mount("https://", replacement_adapter)
    client.session = replacement
    for method_name in (
        "privatePostTradeBatchOrders",
        "privatePostTradeCancelBatchOrders",
    ):
        monkeypatch.setattr(
            client,
            method_name,
            lambda *_args, _name=method_name: pytest.fail(
                f"client method called: {_name}"
            ),
        )
    with pytest.raises(OkxSpiderDemoRoutingError):
        transport.submit(_batch())
    assert guarded_adapter.requests == []
    assert replacement_adapter.requests == []


@pytest.mark.parametrize("operation", ["place", "cancel"])
@pytest.mark.parametrize("hook", ["benign", "resend_without_header", "digest_auth"])
def test_request_hooks_are_rejected_before_any_http_call(operation: str, hook: str) -> None:
    transport = create_okx_spider_demo_batch_transport(dict(_CONFIG))
    client = cast(ccxt.okx, transport._client)
    adapter = _mount_adapter(client)
    session = cast(requests.Session, client.session)

    def resend(response: Response, **kwargs: Any) -> Response:
        retry = cast(PreparedRequest, response.request).copy()
        del retry.headers["x-simulated-trading"]
        return adapter.send(retry)

    if hook == "benign":
        session.hooks["response"].append(lambda response, **kwargs: response)
    elif hook == "resend_without_header":
        session.hooks["response"].append(resend)
    else:
        session.auth = HTTPDigestAuth("user", "password")
    payload = _batch() if operation == "place" else _two_member_cancel_batch()
    with pytest.raises(OkxSpiderDemoRoutingError):
        transport.submit(payload)
    assert adapter.requests == []
