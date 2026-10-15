"""Bounded pure tests for OKX Demo Spider profile capture."""

from copy import deepcopy
from decimal import Decimal
from logging import Logger
from typing import Any, cast

import pytest
import ccxt
import requests

from src.core.adapters import okx_spider_profile_capture as capture
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from tests.test_okx_spider_profile import ACCOUNT, INSTRUMENTS, NOW, SELECTION


class _ReadOnlyClient:
    def __init__(self, *, failed: str | None = None, pages=None, error_at=None, account_uids=None):
        self.urls = {"api": {"rest": "https://www.okx.com"}}
        self.headers = {"x-simulated-trading": "1"}
        self.timeout = 5000
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.failed = failed
        self.error_at = error_at
        self.account_uids = account_uids or ["uid-demo-1", "uid-demo-1"]
        self.counts: dict[str, int] = {}
        self.pages = pages or [{"code": "0", "data": []}]

    def _response(self, method: str, params: dict[str, object], response: object):
        self.calls.append((method, params))
        count = self.counts.get(method, 0) + 1
        self.counts[method] = count
        if self.failed == method:
            raise RuntimeError("private details must not escape")
        if self.error_at == (method, count):
            return {"code": "500", "data": []}
        return response

    def privateGetAccountConfig(self, params):
        account = deepcopy(ACCOUNT)
        index = min(self.counts.get("privateGetAccountConfig", 0), len(self.account_uids) - 1)
        account["data"][0]["uid"] = self.account_uids[index]
        return self._response("privateGetAccountConfig", params, account)

    def publicGetPublicInstruments(self, params):
        instruments = deepcopy(INSTRUMENTS)
        instruments["data"][0].update(instFamily="BTC-USDT", groupId="4")
        return self._response("publicGetPublicInstruments", params, instruments)

    def privateGetAccountTradeFee(self, params):
        response = {"code": "0", "data": [{
            "instType": "SWAP", "ts": str(NOW),
            "feeGroup": [{"groupId": "4", "maker": "-0.0001", "taker": "0.0005"}],
        }]}
        return self._response("privateGetAccountTradeFee", params, response)

    def privateGetAccountPositions(self, params):
        return self._response("privateGetAccountPositions", params, {"code": "0", "data": []})

    def privateGetTradeOrdersPending(self, params):
        return self._response("privateGetTradeOrdersPending", params, self.pages.pop(0))

    def privateGetAccountBalance(self, params):
        return self._response("privateGetAccountBalance", params, {"code": "0", "data": [{
            "adjEq": "1000", "imr": "100", "mmr": "50", "ordFroz": "0", "uTime": str(NOW),
            "details": [{"ccy": "USDT", "eq": "800", "isoEq": "100", "availEq": "600", "liab": "0"}],
        }]})


def _selection() -> bytes:
    return canonical_bytes(SELECTION)


def _capture(client: _ReadOnlyClient, *, clock=lambda: 0.0, wall_time=lambda: NOW):
    return capture.capture_okx_spider_financial_profile(
        client, _selection(), Decimal("25"), monotonic=clock, wall_time_ms=wall_time
    )


def test_capture_is_ordered_bounded_read_only_and_projects_full_profile():
    client = _ReadOnlyClient()
    profile = _capture(client)
    assert profile.financial_unavailable is None
    assert profile.projection(NOW).source_total == Decimal("675")
    assert client.calls == [
        ("privateGetAccountConfig", {}),
        ("publicGetPublicInstruments", {"instType": "SWAP"}),
        ("privateGetAccountTradeFee", {"instType": "SWAP", "instFamily": "BTC-USDT"}),
        ("privateGetAccountPositions", {}),
        ("privateGetTradeOrdersPending", {"limit": "100"}),
        ("privateGetAccountBalance", {}),
        ("privateGetAccountConfig", {}),
        ("publicGetPublicInstruments", {"instType": "SWAP"}),
    ]


def test_financial_read_failure_keeps_cancel_prerequisite_but_disables_projection():
    from tests.test_okx_spider_profile import _request

    profile = _capture(_ReadOnlyClient(failed="privateGetAccountPositions"))
    assert profile.financial_unavailable == "CAPTURE_FAILED"
    assert profile(_request("CANCEL"), NOW)
    with pytest.raises(Exception, match="OKX_SPIDER_FINANCIAL_PROFILE_UNAVAILABLE"):
        profile.projection(NOW)


def test_initial_instrument_failure_keeps_valid_identity_for_cancel_only():
    client = _ReadOnlyClient(failed="publicGetPublicInstruments")
    profile = _capture(client)
    assert profile.financial_unavailable == "CAPTURE_FAILED"
    assert profile(_request_for_cancel(), NOW)
    assert client.counts["publicGetPublicInstruments"] == 1


def test_initial_instrument_error_envelope_is_not_retried_for_refresh():
    client = _ReadOnlyClient(error_at=("publicGetPublicInstruments", 1))
    profile = _capture(client)
    assert profile.financial_unavailable == "CAPTURE_FAILED"
    assert profile(_request_for_cancel(), NOW)
    assert client.counts["publicGetPublicInstruments"] == 1


def test_initial_account_failure_cannot_create_a_financial_composite():
    with pytest.raises(capture.OkxSpiderProfileCaptureUnavailable):
        _capture(_ReadOnlyClient(failed="privateGetAccountConfig"))


@pytest.mark.parametrize("refresh_kind", ["instrument_error", "instrument_transport", "account_error", "account_transport"])
def test_failed_refresh_preserves_valid_initial_identity_and_disables_finance(refresh_kind):
    method = "publicGetPublicInstruments" if refresh_kind.startswith("instrument") else "privateGetAccountConfig"
    error_at = (method, 2) if refresh_kind.endswith("error") else None
    failed = method if refresh_kind.endswith("transport") else None
    client = _ReadOnlyClient(failed=failed, error_at=error_at)
    # Fail only the second request for the transport-error cases.
    if failed is not None:
        original = client._response

        def fail_refresh(method: str, params: dict[str, object], response: object) -> object:
            if method == failed and client.counts.get(method, 0) == 1:
                client.calls.append((method, params))
                client.counts[method] = 2
                raise RuntimeError("refresh transport failure")
            return original(method, params, response)

        client._response = fail_refresh
        client.failed = None
    profile = _capture(client, wall_time=lambda: NOW + len(client.calls))
    assert profile.financial_unavailable == "CAPTURE_FAILED"
    assert profile(_request_for_cancel(), NOW + 20)
    assert client.counts[method] == 2
    identity = profile._identity_prerequisite
    evidence = cast(dict[str, object], decode_canonical(identity.evidence_bytes))
    expected_account_time = NOW + (1 if method == "privateGetAccountConfig" else 7)
    expected_instrument_time = NOW + (2 if method == "publicGetPublicInstruments" else 8)
    assert evidence["account_received_at_ms"] == expected_account_time
    assert evidence["instruments_received_at_ms"] == expected_instrument_time


def test_successful_refresh_identity_mismatch_is_not_hidden_by_old_evidence():
    client = _ReadOnlyClient(account_uids=["uid-demo-1", "uid-other"])
    with pytest.raises(capture.OkxSpiderProfileCaptureUnavailable):
        _capture(client)


def test_malformed_selection_is_rejected_before_any_provider_call():
    client = _ReadOnlyClient()
    with pytest.raises(capture.OkxSpiderProfileCaptureUnavailable):
        capture.capture_okx_spider_financial_profile(
            client, canonical_bytes({"scope": {}}), Decimal("25"),
            monotonic=lambda: 0.0, wall_time_ms=lambda: NOW,
        )
    assert client.calls == []


def test_nonterminating_ten_page_pending_capture_is_unavailable():
    full_page = {"code": "0", "data": [
        {"ordId": str(index), "instId": "BTC-USDT-SWAP"} for index in range(100)
    ]}
    pages = []
    for page_index in range(10):
        page = deepcopy(full_page)
        page["data"] = [
            {"ordId": f"{page_index}-{index}", "instId": "BTC-USDT-SWAP"}
            for index in range(100)
        ]
        pages.append(page)
    client = _ReadOnlyClient(pages=pages)
    profile = _capture(client)
    assert profile.financial_unavailable == "BUDGET_EXHAUSTED"
    assert not any(method == "privateGetAccountBalance" for method, _ in client.calls)


def test_pending_orders_use_last_ordid_cursor_and_complete_short_page():
    first = {"code": "0", "data": [
        {"ordId": f"order-{index}", "instId": "BTC-USDT-SWAP"}
        for index in range(100)
    ]}
    second = {"code": "0", "data": [
        {"ordId": "order-final-1", "instId": "BTC-USDT-SWAP"},
    ]}
    client = _ReadOnlyClient(pages=[first, second])
    profile = _capture(client)
    assert profile.financial_unavailable is None
    assert [params for method, params in client.calls if method == "privateGetTradeOrdersPending"] == [
        {"limit": "100"}, {"limit": "100", "after": "order-99"},
    ]


def test_duplicate_pending_order_across_pages_disables_finance_without_balance_read():
    first = {"code": "0", "data": [
        {"ordId": f"order-{index}", "instId": "BTC-USDT-SWAP"}
        for index in range(100)
    ]}
    second = {"code": "0", "data": [{"ordId": "order-0", "instId": "BTC-USDT-SWAP"}]}
    client = _ReadOnlyClient(pages=[first, second])
    profile = _capture(client)
    assert profile.financial_unavailable == "CAPTURE_FAILED"
    assert not any(method == "privateGetAccountBalance" for method, _ in client.calls)


def test_budget_exhaustion_still_refreshes_prerequisite_when_time_remains():
    ticks = iter([0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 59.0, 59.0, 59.0, 59.0])
    client = _ReadOnlyClient()
    profile = _capture(client, clock=lambda: next(ticks, 59.0))
    assert profile.financial_unavailable == "BUDGET_EXHAUSTED"
    assert profile(_request_for_cancel(), NOW)


def _request_for_cancel() -> bytes:
    from tests.test_okx_spider_profile import _request

    return _request("CANCEL")


def test_demo_client_forces_no_retries_nonverbose_and_demo_routing(monkeypatch):
    captured = {}

    def factory(config):
        captured.update(config)
        return type("Client", (), {})()

    monkeypatch.setattr(capture, "create_okx_spider_batch_client", factory)
    capture.create_okx_spider_demo_profile_client({
        "verbose": True,
        "urls": {"api": {"rest": "https://www.okx.com"}},
        "headers": {"x-simulated-trading": "1"},
        "options": {
            "maxRetriesOnFailure": 7,
            "account/balance": {"maxRetriesOnFailure": 4},
        },
    })
    assert captured["verbose"] is False
    assert captured["timeout"] == 5000
    assert captured["headers"]["x-simulated-trading"] == "1"
    assert captured["options"]["maxRetriesOnFailure"] == 0
    assert captured["options"]["account/balance"]["maxRetriesOnFailure"] == 0
    assert captured["options"]["account/config"]["maxRetriesOnFailure"] == 0
    with pytest.raises(ValueError):
        capture.create_okx_spider_demo_profile_client({
            "headers": {"x-simulated-trading": "0"},
        })


def test_locked_ccxt_client_makes_one_demo_http_attempt_per_read_endpoint():
    client = cast(ccxt.okx, capture.create_okx_spider_demo_profile_client({
        "apiKey": "test-key",
        "secret": "test-secret",
        "password": "test-passphrase",
        "verbose": True,
        "options": {
            "maxRetriesOnFailure": 3,
            "account/config": {"maxRetriesOnFailure": 3},
        },
    }))
    assert client.verbose is False
    logger = cast(Logger, client.logger)
    assert logger.level >= 30
    assert logger.propagate is False
    observed_http: list[tuple[str, str, dict[str, str]]] = []

    def unavailable(method: str, url: str, **kwargs: object) -> None:
        observed_http.append((url, method, cast(dict[str, str], kwargs.get("headers", {}))))
        raise requests.exceptions.ConnectionError("synthetic local transport failure")

    cast(Any, client.session).request = unavailable
    reads = (
        ("privateGetAccountConfig", {}),
        ("publicGetPublicInstruments", {"instType": "SWAP"}),
        ("privateGetAccountTradeFee", {"instType": "SWAP", "instFamily": "BTC-USDT"}),
        ("privateGetAccountPositions", {}),
        ("privateGetTradeOrdersPending", {"limit": "100"}),
        ("privateGetAccountBalance", {}),
    )
    for method, params in reads:
        with pytest.raises(ccxt.NetworkError):
            getattr(client, method)(params)
    assert len(observed_http) == len(reads)
    assert all("www.okx.com" in request[0] for request in observed_http)
    assert all(request[2].get("x-simulated-trading") == "1" for request in observed_http)
