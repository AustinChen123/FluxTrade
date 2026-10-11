"""Concrete low-level OKX batch I/O for an already-bound Spider group.

This module performs no profile selection, precision conversion, order
classification, persistence, or automatic retry. The caller supplies a closed
canonical request that has already been checked against durable bindings.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Protocol, cast

import ccxt

from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

_MAX_PAYLOAD_BYTES = 1_048_576
_MAX_MEMBERS = 20
OkxMemberOutcome = tuple[str, str | None, str | None]


class _SpiderOkxClient(ccxt.okx):
    """Keep successful-HTTP OKX envelopes intact for durable member parsing."""

    def __init__(self, config: dict[str, object]) -> None:
        safe_config = dict(config)
        options_value = safe_config.get("options")
        options = dict(options_value) if isinstance(options_value, dict) else {}
        options["maxRetriesOnFailure"] = 0
        for endpoint in ("trade/batch-orders", "trade/cancel-batch-orders"):
            endpoint_value = options.get(endpoint)
            endpoint_options = (
                dict(endpoint_value) if isinstance(endpoint_value, dict) else {}
            )
            endpoint_options["maxRetriesOnFailure"] = 0
            options[endpoint] = endpoint_options
        safe_config["options"] = options
        super().__init__(cast(Any, safe_config))

    def handle_errors(
        self,
        httpCode: int,
        reason: str,
        url: str,
        method: str,
        headers: dict[str, object],
        body: str,
        response: object,
        requestHeaders: object,
        requestBody: object,
    ) -> None:
        # CCXT's OKX override throws for envelope code!=0 before returning the
        # low-level endpoint result. This client is Spider-only: preserve that
        # exact body so the durable owner can classify every batch member.
        # HTTP status/network/timeout errors are still raised by CCXT's fetch.
        del httpCode, reason, url, method, headers, body, response
        del requestHeaders, requestBody


def create_okx_spider_batch_client(config: dict[str, object]) -> ccxt.okx:
    """Build an isolated client with no SDK resend and raw envelope handling."""
    return _SpiderOkxClient(config)


class _OkxBatchClient(Protocol):
    def privatePostTradeBatchOrders(self, orders: list[dict[str, object]]) -> object: ...

    def privatePostTradeCancelBatchOrders(
        self, orders: list[dict[str, object]]
    ) -> object: ...


@dataclass(frozen=True, slots=True)
class OkxBatchCallResult:
    response_payload: bytes | None
    response_failure: str | None
    member_outcomes: tuple[tuple[str, OkxMemberOutcome], ...] = ()


class OkxSpiderBatchTransport:
    """Invoke exactly one low-level OKX REST batch endpoint for saved bytes."""

    def __init__(self, client: _OkxBatchClient) -> None:
        self._client = client

    def submit(self, request_payload: bytes) -> OkxBatchCallResult:
        request = _decode_batch_request(request_payload)
        if request is None:
            return OkxBatchCallResult(None, "RESPONSE_INVALID")

        rows = [
            cast(dict[str, object], member["request_payload"])
            for member in cast(list[dict[str, object]], request["members"])
        ]
        operation = request["operation"]
        if operation == "PLACE":
            response = self._client.privatePostTradeBatchOrders(rows)
        else:
            response = self._client.privatePostTradeCancelBatchOrders(rows)
        if type(response) is not dict:
            return OkxBatchCallResult(None, "RESPONSE_INVALID")
        try:
            response_payload = canonical_bytes(response)
        except (TypeError, ValueError):
            return OkxBatchCallResult(None, "RESPONSE_INVALID")
        if not 1 <= len(response_payload) <= _MAX_PAYLOAD_BYTES:
            return OkxBatchCallResult(None, "RESPONSE_TOO_LARGE")
        if operation == "PLACE":
            member_outcomes = classify_okx_batch_response(request_payload, response_payload)
            if member_outcomes is None:
                return OkxBatchCallResult(None, "RESPONSE_INVALID")
        else:
            # Keep the bounded raw cancel envelope, but do not claim a cancel
            # classification before its durable action producer is implemented.
            member_outcomes = {}
        return OkxBatchCallResult(
            response_payload,
            None,
            tuple(sorted(member_outcomes.items())),
        )


def _decode_batch_request(payload: bytes) -> dict[str, object] | None:
    if type(payload) is not bytes or not 1 <= len(payload) <= _MAX_PAYLOAD_BYTES:
        return None
    try:
        value = decode_canonical(payload)
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    if type(value) is not dict:
        return None
    request = cast(dict[str, object], value)
    expected_keys = {
        "schema_version", "scope", "executing_account_ref", "transmission_group_id",
        "operation", "members", "payload_sha256",
    }
    if set(request) != expected_keys:
        return None
    scope = request["scope"]
    if (
        request["schema_version"] != "SPIDER_TRANSPORT_BATCH_V1"
        or type(scope) is not dict
        or set(cast(dict[str, object], scope))
        != {"environment", "execution_scope_id", "strategy_id"}
        or any(
            type(cast(dict[str, object], scope).get(field)) is not str
            or not cast(dict[str, object], scope)[field]
            for field in ("environment", "execution_scope_id", "strategy_id")
        )
        or type(request["executing_account_ref"]) is not str
        or not cast(str, request["executing_account_ref"])
        or request["operation"] not in {"PLACE", "CANCEL"}
        or type(request["transmission_group_id"]) is not str
        or len(cast(str, request["transmission_group_id"])) != 64
        or any(
            character not in "0123456789abcdef"
            for character in cast(str, request["transmission_group_id"])
        )
    ):
        return None
    members = request["members"]
    if type(members) is not list or not 1 <= len(cast(list[object], members)) <= _MAX_MEMBERS:
        return None
    operation = cast(str, request["operation"])
    member_keys = (
        {
            "action_id", "product_id", "local_order_id", "binding_sha256",
            "wire_clOrdId", "ordId", "request_payload",
        }
        if operation == "PLACE"
        else {
            "cancel_action_id", "product_id", "target_place_action_id",
            "target_local_order_id", "target_binding_sha256", "target_wire_clOrdId",
            "target_ordId", "request_payload",
        }
    )
    identities: list[str] = []
    cancel_targets: set[str] = set()
    for member_value in cast(list[object], members):
        if type(member_value) is not dict:
            return None
        member = cast(dict[str, object], member_value)
        if set(member) != member_keys or type(member["request_payload"]) is not dict:
            return None
        member_id_key = "action_id" if operation == "PLACE" else "cancel_action_id"
        member_id = member[member_id_key]
        product_id = member["product_id"]
        if type(member_id) is not str or len(member_id) != 64:
            return None
        if any(character not in "0123456789abcdef" for character in member_id):
            return None
        if type(product_id) is not str or not product_id:
            return None
        identities.append(member_id)
        if operation == "CANCEL":
            target_id = member["target_local_order_id"]
            target_order_id = member["target_ordId"]
            if (
                type(target_id) is not str
                or not target_id
                or target_id in cancel_targets
                or type(target_order_id) is not str
                or not target_order_id
                or member["request_payload"].get("ordId") != target_order_id
                or "clOrdId" in cast(dict[str, object], member["request_payload"])
            ):
                return None
            cancel_targets.add(target_id)
        elif cast(dict[str, object], member["request_payload"]).get("clOrdId") != member[
            "wire_clOrdId"
        ]:
            return None
    if len(set(identities)) != len(identities):
        return None

    body = dict(request)
    supplied_digest = body.pop("payload_sha256")
    body_digest = sha256(canonical_bytes(body)).hexdigest()
    if type(supplied_digest) is not str or supplied_digest != body_digest:
        return None
    return request


def classify_okx_batch_response(
    request_payload: bytes, response_payload: bytes
) -> dict[str, OkxMemberOutcome] | None:
    """Classify bounded OKX member evidence without parsing SDK exceptions."""
    request = _decode_batch_request(request_payload)
    if request is None or type(response_payload) is not bytes:
        return None
    try:
        response_value = decode_canonical(response_payload)
        if canonical_bytes(response_value) != response_payload:
            return None
    except (TypeError, ValueError, UnicodeDecodeError):
        return None
    if type(response_value) is not dict:
        return None
    response = cast(dict[str, object], response_value)
    members = cast(list[dict[str, object]], request["members"])
    operation = cast(str, request["operation"])
    if operation != "PLACE":
        # CANCEL's target-identity classification is implemented with its
        # durable action producer; never apply PLACE identity rules to it.
        return None
    outcomes: dict[str, OkxMemberOutcome] = {
        cast(str, member["action_id"]): ("SEND_UNKNOWN", None, None)
        for member in members
    }
    data_value = response.get("data")
    if type(response.get("code")) is not str or type(data_value) is not list:
        return outcomes
    by_wire_id = {
        cast(str, member["wire_clOrdId"]): cast(str, member["action_id"])
        for member in members
    }
    matched: dict[str, list[dict[str, object]]] = {wire_id: [] for wire_id in by_wire_id}
    for row in cast(list[object], data_value):
        if type(row) is not dict:
            continue
        provider_row = cast(dict[str, object], row)
        wire_id = provider_row.get("clOrdId")
        if type(wire_id) is str and wire_id in matched:
            matched[wire_id].append(provider_row)
    for wire_id, rows in matched.items():
        if not rows:
            continue
        first = canonical_bytes(rows[0])
        if any(canonical_bytes(row) != first for row in rows[1:]):
            continue
        action_id = by_wire_id[wire_id]
        row = rows[0]
        member_code = row.get("sCode")
        ord_id = row.get("ordId")
        if type(member_code) is not str:
            continue
        if member_code == "0":
            if type(ord_id) is str and ord_id:
                outcomes[action_id] = ("ACCEPTED", None, ord_id)
        elif member_code in {"51000", "51008"} and (
            ord_id is None or (type(ord_id) is str and ord_id == "")
        ):
            outcomes[action_id] = ("REJECTED", cast(str, member_code), None)
    return outcomes
