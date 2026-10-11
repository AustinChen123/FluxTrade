"""Offline tests of the concrete CCXT OKX low-level batch path."""

from copy import deepcopy
from hashlib import sha256
from typing import Any, cast

import ccxt
import pytest
import requests

from src.core.adapters.okx_spider_transport import (
    OkxSpiderBatchTransport,
    create_okx_spider_batch_client,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical


def _batch(operation: str = "PLACE") -> bytes:
    if operation == "PLACE":
        members: list[dict[str, object]] = [
            {
                "action_id": "a" * 64,
                "product_id": "OKX:BTC-USDT-SWAP",
                "local_order_id": "00000000-0000-0000-0000-000000000001",
                "binding_sha256": "b" * 64,
                "wire_clOrdId": "sp" + "c" * 30,
                "ordId": None,
                "request_payload": {
                    "instId": "BTC-USDT-SWAP",
                    "tdMode": "cross",
                    "side": "buy",
                    "posSide": "net",
                    "ordType": "limit",
                    "sz": "1",
                    "px": "100",
                    "clOrdId": "sp" + "c" * 30,
                    "reduceOnly": False,
                },
            }
        ]
    else:
        members = [
            {
                "cancel_action_id": "d" * 64,
                "product_id": "OKX:BTC-USDT-SWAP",
                "target_place_action_id": "a" * 64,
                "target_local_order_id": "00000000-0000-0000-0000-000000000001",
                "target_binding_sha256": "b" * 64,
                "target_wire_clOrdId": "sp" + "c" * 30,
                "target_ordId": "provider-order-1",
                "request_payload": {
                    "instId": "BTC-USDT-SWAP",
                    "ordId": "provider-order-1",
                },
            }
        ]
    body: dict[str, object] = {
        "schema_version": "SPIDER_TRANSPORT_BATCH_V1",
        "scope": {
            "environment": "live",
            "execution_scope_id": "account-main",
            "strategy_id": "spider-main",
        },
        "executing_account_ref": "synthetic-test-account",
        "transmission_group_id": "e" * 64,
        "operation": operation,
        "members": members,
    }
    body["payload_sha256"] = sha256(canonical_bytes(body)).hexdigest()
    return canonical_bytes(body)


@pytest.mark.parametrize(
    ("operation", "endpoint"),
    [
        ("PLACE", "trade/batch-orders"),
        ("CANCEL", "trade/cancel-batch-orders"),
    ],
)
def test_ccxt_raw_okx_endpoint_receives_exact_ordered_member_payloads_offline(
    monkeypatch: pytest.MonkeyPatch, operation: str, endpoint: str
) -> None:
    client = ccxt.okx({})
    observed: list[tuple[object, ...]] = []
    envelope = {
        "code": "0",
        "data": [{"ordId": "provider-order-1", "sCode": "0", "unknown": "preserved"}],
        "msg": "",
    }

    def offline_request(*args: object, **kwargs: object) -> object:
        observed.append((*args, kwargs))
        return envelope

    monkeypatch.setattr(client, "request", offline_request)
    result = OkxSpiderBatchTransport(cast(Any, client)).submit(_batch(operation))

    assert result.response_failure is None
    assert result.response_payload == canonical_bytes(envelope)
    assert len(observed) == 1
    assert endpoint in cast(str, observed[0][0])
    sent_rows = cast(list[dict[str, object]], observed[0][3])
    payload = cast(dict[str, object], decode_canonical(_batch(operation)))
    expected_rows = [
        cast(dict[str, object], member["request_payload"])
        for member in cast(list[dict[str, object]], payload["members"])
    ]
    assert sent_rows == expected_rows


@pytest.mark.parametrize("failure", ["wrong_digest", "extra_root", "duplicate_member", "duplicate_cancel_target"])
def test_invalid_canonical_batch_never_calls_ccxt(
    monkeypatch: pytest.MonkeyPatch, failure: str
) -> None:
    client = ccxt.okx({})
    calls: list[object] = []

    def offline_request(*args: object, **kwargs: object) -> object:
        calls.append(args)
        return {"code": "0", "data": [], "msg": ""}

    monkeypatch.setattr(client, "request", offline_request)
    operation = "CANCEL" if failure == "duplicate_cancel_target" else "PLACE"
    decoded = cast(dict[str, object], decode_canonical(_batch(operation)))
    if failure == "wrong_digest":
        decoded["payload_sha256"] = "f" * 64
    elif failure == "extra_root":
        decoded["unexpected"] = True
        body = dict(decoded)
        body.pop("payload_sha256")
        decoded["payload_sha256"] = sha256(canonical_bytes(body)).hexdigest()
    else:
        members = cast(list[dict[str, object]], decoded["members"])
        duplicate = deepcopy(members[0])
        if failure == "duplicate_member":
            decoded["members"] = [*members, duplicate]
        else:
            duplicate["cancel_action_id"] = "f" * 64
            members.append(duplicate)
        body = dict(decoded)
        body.pop("payload_sha256")
        decoded["payload_sha256"] = sha256(canonical_bytes(body)).hexdigest()

    result = OkxSpiderBatchTransport(cast(Any, client)).submit(canonical_bytes(decoded))
    assert result.response_payload is None
    assert result.response_failure == "RESPONSE_INVALID"
    assert calls == []


def test_response_encoding_rejects_float_and_bounds_large_envelopes_offline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = ccxt.okx({})
    responses = iter(({"code": "0", "data": [], "number": 1.25},
                      {"code": "0", "data": [], "opaque": "x" * 1_048_577}))

    monkeypatch.setattr(client, "request", lambda *_args, **_kwargs: next(responses))
    transport = OkxSpiderBatchTransport(cast(Any, client))
    assert transport.submit(_batch()).response_failure == "RESPONSE_INVALID"
    assert transport.submit(_batch()).response_failure == "RESPONSE_TOO_LARGE"


@pytest.mark.parametrize(
    ("member_code", "order_id"),
    [([], ""), ({"unexpected": "shape"}, ""), ("51008", []), ("51000", {})],
)
def test_json_member_identity_fields_with_wrong_types_remain_raw_and_unknown(
    monkeypatch: pytest.MonkeyPatch,
    member_code: object,
    order_id: object,
) -> None:
    envelope = {
        "code": "1",
        "data": [{
            "clOrdId": "sp" + "c" * 30,
            "ordId": order_id,
            "sCode": member_code,
        }],
        "msg": "bounded malformed member fields",
    }
    client = ccxt.okx({})
    monkeypatch.setattr(client, "request", lambda *_args, **_kwargs: envelope)

    result = OkxSpiderBatchTransport(cast(Any, client)).submit(_batch())

    assert result.response_failure is None
    assert result.response_payload == canonical_bytes(envelope)
    assert result.member_outcomes == (("a" * 64, ("SEND_UNKNOWN", None, None)),)


def _http_response(status: int, body: bytes) -> requests.Response:
    response = requests.Response()
    response.status_code = status
    response.reason = "OK" if status < 400 else "Bad Request"
    response.headers["Content-Type"] = "application/json"
    response.url = "https://offline.invalid/api/v5/trade/batch-orders"
    response._content = body
    return response


@pytest.mark.parametrize(
    ("code", "member_code"),
    [("0", "0"), ("2", "51008"), ("1", "51008")],
)
def test_actual_ccxt_fetch_path_preserves_raw_application_result_once(
    monkeypatch: pytest.MonkeyPatch, code: str, member_code: str
) -> None:
    client = create_okx_spider_batch_client(
        {
            "apiKey": "synthetic-key",
            "secret": "synthetic-secret",
            "password": "synthetic-passphrase",
            "options": {"trade/batch-orders": {"maxRetriesOnFailure": 2}},
        }
    )
    body = canonical_bytes(
        {
            "code": code,
            "data": [
                {
                    "clOrdId": "sp" + "c" * 30,
                    "ordId": "provider-order-1" if member_code == "0" else "",
                    "sCode": member_code,
                    "sMsg": "insufficient funds" if member_code != "0" else "",
                }
            ],
            "msg": "",
        }
    )
    calls: list[object] = []

    def offline_http(*_args: object, **_kwargs: object) -> requests.Response:
        calls.append(None)
        return _http_response(200, body)

    monkeypatch.setattr(client.session, "request", offline_http)
    result = OkxSpiderBatchTransport(cast(Any, client)).submit(_batch())

    assert result.response_failure is None
    assert result.response_payload == body
    assert result.member_outcomes == (
        (
            "a" * 64,
            ("ACCEPTED", None, "provider-order-1")
            if member_code == "0"
            else ("REJECTED", member_code, None),
        ),
    )
    assert len(calls) == 1
    assert cast(dict[str, Any], client.options)["maxRetriesOnFailure"] == 0


def _two_member_batch() -> bytes:
    request = cast(dict[str, object], decode_canonical(_batch()))
    first = cast(dict[str, object], cast(list[object], request["members"])[0])
    second = deepcopy(first)
    second["action_id"] = "b" * 64
    second["local_order_id"] = "00000000-0000-0000-0000-000000000002"
    second["binding_sha256"] = "d" * 64
    second["wire_clOrdId"] = "sp" + "y" * 30
    second_request = cast(dict[str, object], second["request_payload"])
    second_request["clOrdId"] = second["wire_clOrdId"]
    request["members"] = [first, second]
    request.pop("payload_sha256")
    request["payload_sha256"] = sha256(canonical_bytes(request)).hexdigest()
    return canonical_bytes(request)


@pytest.mark.parametrize(
    "case",
    [
        "shuffled_accepted_and_rejected",
        "missing_member",
        "accepted_without_provider_id",
        "unrecognized_rejection",
        "conflicting_duplicate",
        "identical_duplicate",
        "malformed_code_type",
        "malformed_provider_id_type",
    ],
)
def test_okx_batch_outcomes_are_exactly_once_per_original_member_and_raw_is_retained(
    monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    wire_a = "sp" + "c" * 30
    wire_b = "sp" + "y" * 30
    accepted_a = {"clOrdId": wire_a, "ordId": "provider-a", "sCode": "0"}
    accepted_b = {"clOrdId": wire_b, "ordId": "provider-b", "sCode": "0"}
    rejected_b = {"clOrdId": wire_b, "ordId": "", "sCode": "51008"}
    expected_a: tuple[str, str | None, str | None] = ("SEND_UNKNOWN", None, None)
    expected_b: tuple[str, str | None, str | None] = ("ACCEPTED", None, "provider-b")
    if case == "shuffled_accepted_and_rejected":
        rows = [rejected_b, accepted_a]
        expected_a = ("ACCEPTED", None, "provider-a")
        expected_b = ("REJECTED", "51008", None)
    elif case == "missing_member":
        rows = [accepted_b]
    elif case == "accepted_without_provider_id":
        rows = [{"clOrdId": wire_a, "ordId": "", "sCode": "0"}, accepted_b]
    elif case == "unrecognized_rejection":
        rows = [{"clOrdId": wire_a, "ordId": "", "sCode": "51149"}, accepted_b]
    elif case == "conflicting_duplicate":
        rows = [accepted_a, {**accepted_a, "ordId": "different-provider-a"}, accepted_b]
    elif case == "identical_duplicate":
        rows = [accepted_a, deepcopy(accepted_a), accepted_b]
        expected_a = ("ACCEPTED", None, "provider-a")
    elif case == "malformed_code_type":
        rows = [{"clOrdId": wire_a, "ordId": "", "sCode": []}, accepted_b]
    else:
        rows = [{"clOrdId": wire_a, "ordId": {}, "sCode": "51008"}, accepted_b]
    envelope = {"code": "1", "data": rows, "msg": f"synthetic {case}"}
    client = ccxt.okx({})
    monkeypatch.setattr(client, "request", lambda *_args, **_kwargs: envelope)

    result = OkxSpiderBatchTransport(cast(Any, client)).submit(_two_member_batch())

    assert result.response_failure is None
    assert result.response_payload == canonical_bytes(envelope)
    assert result.member_outcomes == (
        ("a" * 64, expected_a),
        ("b" * 64, expected_b),
    )


def test_valid_unclassifiable_okx_envelope_is_retained_with_unknown_member(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = ccxt.okx({})
    envelope = {"code": "1", "data": "provider-shape-changed", "msg": "opaque"}
    monkeypatch.setattr(client, "request", lambda *_args, **_kwargs: envelope)

    result = OkxSpiderBatchTransport(cast(Any, client)).submit(_batch())

    assert result.response_payload == canonical_bytes(envelope)
    assert result.response_failure is None
    assert result.member_outcomes == (("a" * 64, ("SEND_UNKNOWN", None, None)),)


def test_actual_ccxt_fetch_path_keeps_http_failure_as_transport_exception_once(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = create_okx_spider_batch_client(
        {
            "apiKey": "synthetic-key",
            "secret": "synthetic-secret",
            "password": "synthetic-passphrase",
            "options": {"trade/batch-orders": {"maxRetriesOnFailure": 2}},
        }
    )
    calls: list[object] = []

    def offline_http(*_args: object, **_kwargs: object) -> requests.Response:
        calls.append(None)
        return _http_response(400, b'{"code":"1","msg":"bad request","data":[]}')

    monkeypatch.setattr(client.session, "request", offline_http)
    with pytest.raises(ccxt.BaseError):
        OkxSpiderBatchTransport(cast(Any, client)).submit(_batch())

    assert len(calls) == 1


@pytest.mark.parametrize(
    ("operation", "endpoint"),
    [("PLACE", "trade/batch-orders"), ("CANCEL", "trade/cancel-batch-orders")],
)
def test_actual_ccxt_fetch_path_timeout_has_no_envelope_and_never_retries(
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
    endpoint: str,
) -> None:
    client = create_okx_spider_batch_client(
        {
            "apiKey": "synthetic-key",
            "secret": "synthetic-secret",
            "password": "synthetic-passphrase",
            "options": {endpoint: {"maxRetriesOnFailure": 2}},
        }
    )
    calls: list[object] = []

    def offline_timeout(*_args: object, **_kwargs: object) -> requests.Response:
        calls.append(None)
        raise requests.Timeout("offline synthetic timeout")

    monkeypatch.setattr(client.session, "request", offline_timeout)
    with pytest.raises(ccxt.RequestTimeout):
        OkxSpiderBatchTransport(cast(Any, client)).submit(_batch(operation))

    assert len(calls) == 1
