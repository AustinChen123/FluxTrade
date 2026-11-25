from typing import cast

import ccxt
import pytest
import requests
from requests import PreparedRequest, Response
from requests.adapters import BaseAdapter

from src.core.adapters.okx_resting_order_demo_reads import (
    OkxRestingDemoReadClient,
    _DemoReadRoutingSession,
    create_okx_resting_demo_read_client,
)
from src.core.adapters.okx_resting_order_reads import (
    read_okx_net_position,
    read_okx_swap_instrument,
)
from src.core.adapters.okx_spider_demo_trading import OkxSpiderDemoRoutingError
from src.core.adapters.okx_spider_profile_capture import _REST
from tests.test_okx_spider_demo_trading import _http_response


_CONFIG: dict[str, object] = {
    "apiKey": "dummy-api-key",
    "secret": "dummy-secret",
    "password": "dummy-password",
}


class _RecordingAdapter(BaseAdapter):
    def __init__(self, responses: list[bytes] | None = None) -> None:
        self.requests: list[PreparedRequest] = []
        self.responses = responses or [b'{"code":"0","data":[],"msg":""}']

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
        response = _http_response(200, self.responses[min(len(self.requests) - 1, len(self.responses) - 1)])
        response.request = request
        response.url = request.url or ""
        return response

    def close(self) -> None:
        return None


def _make(response_bodies: list[bytes] | None = None) -> tuple[OkxRestingDemoReadClient, ccxt.okx, _RecordingAdapter]:
    read_client = create_okx_resting_demo_read_client(dict(_CONFIG))
    raw_client = cast(ccxt.okx, read_client._client)
    adapter = _RecordingAdapter(response_bodies)
    cast(requests.Session, raw_client.session).mount("https://", adapter)
    cast(requests.Session, raw_client.session).mount("http://", adapter)
    return read_client, raw_client, adapter


@pytest.mark.parametrize(
    ("method", "params", "path"),
    [
        ("privateGetAccountPositions", {"instType": "SWAP", "instId": "BTC-USDT-SWAP"}, "/api/v5/account/positions"),
        ("privateGetTradeOrdersPending", {"instType": "SWAP", "instId": "BTC-USDT-SWAP", "limit": "7"}, "/api/v5/trade/orders-pending"),
        ("privateGetTradeFills", {"instType": "SWAP", "instId": "BTC-USDT-SWAP", "before": "12"}, "/api/v5/trade/fills"),
        ("publicGetPublicInstruments", {"instType": "SWAP", "instId": "BTC-USDT-SWAP"}, "/api/v5/public/instruments"),
    ],
)
def test_each_read_method_sends_one_exact_demo_get(
    method: str, params: dict[str, str], path: str
) -> None:
    read_client, _raw, adapter = _make()
    getattr(read_client, method)(params)
    assert len(adapter.requests) == 1
    request = adapter.requests[0]
    assert request.method == "GET"
    assert request.url is not None
    from urllib.parse import parse_qs, urlsplit

    parsed = urlsplit(request.url)
    assert parsed.scheme == "https" and parsed.netloc == "www.okx.com"
    assert parsed.path == path
    assert {key: values[0] for key, values in parse_qs(parsed.query).items()} == params
    assert [value for key, value in request.headers.items() if key.lower() == "x-simulated-trading"] == ["1"]


@pytest.mark.parametrize(
    "mutation",
    [
        "post", "balance", "withdrawal", "trailing", "host", "http", "header_removed",
        "header_zero", "header_duplicate", "session", "rest", "hook",
    ],
)
def test_forbidden_routes_are_rejected_before_adapter(mutation: str) -> None:
    read_client, raw, adapter = _make()
    session = cast(requests.Session, raw.session)
    request = requests.Request("GET", f"{_REST}/api/v5/account/positions").prepare()
    request.headers["x-simulated-trading"] = "1"
    if mutation == "post":
        request.method = "POST"
    elif mutation == "balance":
        request.url = f"{_REST}/api/v5/account/balance"
    elif mutation == "withdrawal":
        request.url = f"{_REST}/api/v5/asset/withdrawal-history"
    elif mutation == "trailing":
        request.url = f"{_REST}/api/v5/account/positions/"
    elif mutation == "host":
        request.url = "https://aws.okx.com/api/v5/account/positions"
    elif mutation == "http":
        request.url = "http://www.okx.com/api/v5/account/positions"
    elif mutation == "header_removed":
        del raw.headers["x-simulated-trading"]
    elif mutation == "header_zero":
        raw.headers["x-simulated-trading"] = "0"
    elif mutation == "header_duplicate":
        raw.headers["X-SIMULATED-TRADING"] = "1"
    elif mutation == "session":
        raw.session = requests.Session()
    elif mutation == "rest":
        cast(dict[str, object], cast(dict[str, object], raw.urls)["api"])["rest"] = "https://aws.okx.com"
    else:
        session.hooks["response"].append(lambda response, *args, **kwargs: response)
    with pytest.raises(OkxSpiderDemoRoutingError):
        if mutation in {"post", "balance", "withdrawal", "trailing", "host", "http"}:
            cast(_DemoReadRoutingSession, session).send(request)
        else:
            read_client.privateGetAccountPositions({"instType": "SWAP"})
    assert adapter.requests == []


@pytest.mark.parametrize(
    ("override", "accepted"),
    [
        ({"headers": {"x-simulated-trading": "0"}}, False),
        ({"headers": {"X-Simulated-Trading": "0"}}, False),
        ({"urls": {"api": {"rest": "https://aws.okx.com"}}}, False),
        ({"headers": {"x-simulated-trading": "1"}}, True),
        ({"urls": {"api": {"rest": _REST}}}, True),
    ],
)
def test_factory_config_routing_rejections_have_legal_twins(
    override: dict[str, object], accepted: bool
) -> None:
    config = {**_CONFIG, **override}
    if accepted:
        create_okx_resting_demo_read_client(config)
    else:
        with pytest.raises(ValueError):
            create_okx_resting_demo_read_client(config)


def test_real_resting_read_consumers_use_demo_client() -> None:
    bodies = [
        b'{"code":"0","data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","mgnMode":"cross","posSide":"net","pos":"3"}],"msg":""}',
        b'{"code":"0","data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","ctType":"linear","ctMult":"1","state":"live","ctVal":"1","lotSz":"1","minSz":"1","tickSz":"1"}],"msg":""}',
    ]
    read_client, _raw, adapter = _make(bodies)
    assert read_okx_net_position(read_client, "BTC-USDT-SWAP").to_eng_string() == "3"
    instrument = read_okx_swap_instrument(read_client, "BTC-USDT-SWAP")
    assert instrument.inst_id == "BTC-USDT-SWAP"
    assert len(adapter.requests) == 2


@pytest.mark.parametrize("mutation", ["header_removed", "header_zero", "header_duplicate", "session", "rest"])
def test_client_routing_drift_is_caught_by_the_per_call_check(mutation: str) -> None:
    read_client, raw, adapter = _make()
    if mutation == "header_removed":
        del raw.headers["x-simulated-trading"]
    elif mutation == "header_zero":
        raw.headers["x-simulated-trading"] = "0"
    elif mutation == "header_duplicate":
        raw.headers["X-SIMULATED-TRADING"] = "1"
    elif mutation == "session":
        raw.session = requests.Session()
    else:
        cast(dict[str, object], cast(dict[str, object], raw.urls)["api"])["rest"] = "https://www.okx.com/other"
    with pytest.raises(OkxSpiderDemoRoutingError, match="invalid OKX Spider Demo read client"):
        read_client.privateGetAccountPositions({"instType": "SWAP"})
    assert adapter.requests == []


@pytest.mark.parametrize("mutation", ["legal", "header_missing", "header_zero", "fragment"])
def test_session_checks_each_prepared_request_itself(mutation: str) -> None:
    _client, raw, adapter = _make()
    session = cast(_DemoReadRoutingSession, raw.session)
    request = requests.Request("GET", f"{_REST}/api/v5/trade/fills?instType=SWAP").prepare()
    if mutation != "header_missing":
        request.headers["x-simulated-trading"] = "0" if mutation == "header_zero" else "1"
    if mutation == "fragment":
        request.url = f"{_REST}/api/v5/trade/fills?instType=SWAP#x"
    if mutation == "legal":
        session.send(request)
        assert len(adapter.requests) == 1
        return
    with pytest.raises(OkxSpiderDemoRoutingError, match="outside OKX Demo read routing"):
        session.send(request)
    assert adapter.requests == []


def test_factory_refuses_a_client_whose_routing_check_fails(monkeypatch: pytest.MonkeyPatch) -> None:
    import src.core.adapters.okx_resting_order_demo_reads as demo_reads

    real = demo_reads.create_okx_spider_demo_profile_client

    def drifted(config: dict[str, object]) -> object:
        client = real(config)
        cast(dict[str, object], getattr(client, "headers"))["x-simulated-trading"] = "0"
        return client

    monkeypatch.setattr(demo_reads, "create_okx_spider_demo_profile_client", drifted)
    with pytest.raises(ValueError, match="invalid OKX Spider Demo read client"):
        create_okx_resting_demo_read_client(dict(_CONFIG))


def test_guard_session_keeps_the_client_proxy_environment_setting() -> None:
    _client, raw, _adapter = _make()
    assert raw.requests_trust_env is False
    assert cast(requests.Session, raw.session).trust_env is False


@pytest.mark.parametrize("name", ["Host", "host", "HOST"])
def test_custom_host_header_is_refused_everywhere(name: str) -> None:
    with pytest.raises(ValueError, match="invalid OKX Spider Demo read client"):
        create_okx_resting_demo_read_client({**_CONFIG, "headers": {name: "other.invalid"}})
    read_client, raw, adapter = _make()
    raw.headers[name] = "www.okx.com"
    with pytest.raises(OkxSpiderDemoRoutingError, match="invalid OKX Spider Demo read client"):
        read_client.privateGetAccountPositions({"instType": "SWAP"})
    del raw.headers[name]
    session = cast(_DemoReadRoutingSession, raw.session)
    session.headers[name] = "other.invalid"
    with pytest.raises(OkxSpiderDemoRoutingError, match="outside OKX Demo read routing"):
        read_client.privateGetAccountPositions({"instType": "SWAP"})
    del session.headers[name]
    request = requests.Request("GET", f"{_REST}/api/v5/account/positions").prepare()
    request.headers["x-simulated-trading"] = "1"
    request.headers[name] = "other.invalid"
    with pytest.raises(OkxSpiderDemoRoutingError, match="outside OKX Demo read routing"):
        session.send(request)
    assert adapter.requests == []
    read_client.privateGetAccountPositions({"instType": "SWAP"})
    assert len(adapter.requests) == 1
    assert not any(key.lower() == "host" for key in adapter.requests[0].headers)
