"""Pure matrix for the OKX Spider pre-marker profile prerequisite."""

from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
from typing import Callable, cast

import pytest

from src.core.adapters.okx_spider_profile import (
    OkxSpiderProfilePrerequisite,
    create_okx_spider_profile_prerequisite,
)
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

NOW = 1_800_000_000_000
PRODUCT = {
    "product_id": "OKX:BTC-USDT-SWAP",
    "instId": "BTC-USDT-SWAP",
    "base_ccy": "BTC",
    "ctVal": "0.01",
    "ctMult": "1",
    "lotSz": "1",
    "minSz": "1",
    "tickSz": "0.1",
}
SELECTION = {
    "scope": {
        "environment": "demo",
        "execution_scope_id": "demo-account-ref",
        "strategy_id": "spider-demo",
    },
    "executing_account_ref": "demo-account-ref",
    "expected_uid": "uid-demo-1",
    "products": [PRODUCT],
}
ACCOUNT = {
    "code": "0",
    "data": [{
        "uid": "uid-demo-1",
        "mainUid": "not-the-expected-uid",
        "acctLv": "3",
        "posMode": "net_mode",
        "autoLoan": False,
        "unexamined": {"ignored": True},
    }],
}
INSTRUMENTS = {
    "code": "0",
    "data": [{
        "instId": "BTC-USDT-SWAP",
        "instType": "SWAP",
        "ctType": "linear",
        "settleCcy": "USDT",
        "ctValCcy": "BTC",
        "ctVal": "0.0100",
        "ctMult": "1.0",
        "lotSz": "1",
        "minSz": "1.00",
        "tickSz": "0.10",
        "state": "live",
        "provider_extra": "allowed",
    }],
}


def _evidence() -> dict[str, object]:
    return {
        "environment": "demo",
        "rest_host": "www.okx.com",
        "simulated_trading": True,
        "account_received_at_ms": NOW - 10_000,
        "instruments_received_at_ms": NOW - 10_000,
        "account": deepcopy(ACCOUNT),
        "instruments": deepcopy(INSTRUMENTS),
    }


def _object(value: object) -> dict[str, object]:
    assert type(value) is dict
    return cast(dict[str, object], value)


def _object_rows(value: object) -> list[dict[str, object]]:
    assert type(value) is list
    return cast(list[dict[str, object]], value)


def _prerequisite(
    selection: object = SELECTION, evidence: object | None = None
):
    selected = deepcopy(selection)
    observed = deepcopy(_evidence() if evidence is None else evidence)
    selection_bytes = canonical_bytes(selected)
    evidence_bytes = canonical_bytes(observed)
    digest = sha256(canonical_bytes({
        "domain": "OKX_SPIDER_PROFILE_PREREQUISITE_V1",
        "selection": selected,
        "evidence": observed,
    })).hexdigest()
    return create_okx_spider_profile_prerequisite(
        selection_bytes, evidence_bytes, digest
    )


def _request(
    operation: str = "PLACE", *, wire: dict[str, object] | None = None
) -> bytes:
    if operation == "PLACE":
        member = {
            "action_id": "a" * 64,
            "product_id": PRODUCT["product_id"],
            "local_order_id": "local-place-1",
            "binding_sha256": "b" * 64,
            "wire_clOrdId": "wireplace1",
            "ordId": None,
            "request_payload": wire or {
                "instId": PRODUCT["instId"],
                "clOrdId": "wireplace1",
                "tdMode": "cross",
                "posSide": "net",
                "ordType": "limit",
                "side": "buy",
                "sz": "2",
                "px": "10.2",
            },
        }
    else:
        member = {
            "cancel_action_id": "c" * 64,
            "product_id": PRODUCT["product_id"],
            "target_place_action_id": "a" * 64,
            "target_local_order_id": "local-place-1",
            "target_binding_sha256": "b" * 64,
            "target_wire_clOrdId": "wireplace1",
            "target_ordId": "venue-order-1",
            "request_payload": wire or {
                "instId": PRODUCT["instId"],
                "ordId": "venue-order-1",
            },
        }
    body = {
        "schema_version": "SPIDER_TRANSPORT_BATCH_V1",
        "scope": deepcopy(SELECTION["scope"]),
        "executing_account_ref": SELECTION["executing_account_ref"],
        "transmission_group_id": "d" * 64,
        "operation": operation,
        "members": [member],
    }
    return canonical_bytes({
        **body,
        "payload_sha256": sha256(canonical_bytes(body)).hexdigest(),
    })


def _mutated_request(
    operation: str, mutate: Callable[[dict[str, object]], None]
) -> bytes:
    request = _object(decode_canonical(_request(operation)))
    body = deepcopy(request)
    body.pop("payload_sha256")
    mutate(body)
    return canonical_bytes({
        **body,
        "payload_sha256": sha256(canonical_bytes(body)).hexdigest(),
    })


def test_exact_demo_profile_and_inclusive_freshness_admit_place() -> None:
    check = _prerequisite()
    assert check(_request(), NOW)
    assert check(_request(), NOW - 0)  # receipt is exactly 10 seconds old
    assert not check(_request(), NOW + 1)  # one millisecond beyond the budget
    assert not check(_request(), NOW - 10_001)  # future receipt


@pytest.mark.parametrize(
    "mutation",
    [
        lambda evidence: evidence["account"]["data"][0].update(acctLv="2"),
        lambda evidence: evidence["account"]["data"][0].update(posMode="long_short_mode"),
        lambda evidence: evidence["account"]["data"][0].update(autoLoan=True),
        lambda evidence: _object_rows(_object(evidence["instruments"])["data"])[0].update(ctType="inverse"),
        lambda evidence: _object_rows(_object(evidence["instruments"])["data"])[0].update(settleCcy="BTC"),
        lambda evidence: _object_rows(_object(evidence["instruments"])["data"])[0].update(ctValCcy="USDT"),
        lambda evidence: _object_rows(_object(evidence["instruments"])["data"])[0].update(state="suspend"),
        lambda evidence: _object_rows(_object(evidence["instruments"])["data"])[0].update(ctVal="0.02"),
        lambda evidence: cast(list[object], _object(evidence["instruments"])["data"]).clear(),
        lambda evidence: cast(list[object], _object(evidence["instruments"])["data"]).append(deepcopy(INSTRUMENTS["data"][0])),
        lambda evidence: evidence.update(instruments_received_at_ms=NOW - 10_001),
    ],
)
def test_place_rejects_profile_evidence_matrix(
    mutation: Callable[[dict[str, object]], None]
) -> None:
    evidence = _evidence()
    mutation(evidence)
    check = _prerequisite(evidence=evidence)
    assert not check(_request(), NOW)


@pytest.mark.parametrize(
    "wire",
    [
        {"instId": "ETH-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": "net", "ordType": "limit", "sz": "2", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "isolated", "posSide": "net", "ordType": "limit", "sz": "2", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": None, "ordType": "limit", "sz": "2", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": "", "ordType": "limit", "sz": "2", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": "long", "ordType": "limit", "sz": "2", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": "short", "ordType": "limit", "sz": "2", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": "net", "ordType": "conditional", "sz": "2", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": "net", "ordType": "limit", "sz": "0", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": "net", "ordType": "limit", "sz": "0.5", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": "net", "ordType": "limit", "sz": "1.5", "px": "10.2"},
        {"instId": "BTC-USDT-SWAP", "clOrdId": "wireplace1", "tdMode": "cross", "posSide": "net", "ordType": "limit", "sz": "2", "px": "10.25"},
    ],
)
def test_place_rejects_wire_identity_mode_size_and_price_matrix(
    wire: dict[str, object]
) -> None:
    assert not _prerequisite()(_request(wire=wire), NOW)


def test_market_and_cancel_ignore_irrelevant_fields_but_keep_identity() -> None:
    market: dict[str, object] = {
        "instId": PRODUCT["instId"], "clOrdId": "wireplace1", "tdMode": "cross",
        "posSide": "net", "ordType": "market", "side": "buy", "sz": "3",
    }
    assert _prerequisite()(_request(wire=market), NOW)

    evidence = _evidence()
    _object_rows(_object(evidence["account"])["data"])[0].update(
        acctLv="0", posMode="unknown", autoLoan=True
    )
    evidence["instruments"] = None
    evidence["instruments_received_at_ms"] = None
    evidence["account_received_at_ms"] = NOW
    assert _prerequisite(evidence=evidence)(_request("CANCEL"), NOW)
    wrong_wire: dict[str, object] = {
        "instId": "ETH-USDT-SWAP", "ordId": "venue-order-1"
    }
    assert not _prerequisite(evidence=evidence)(_request("CANCEL", wire=wrong_wire), NOW)


def test_place_accepts_omitted_pos_side_as_net_default() -> None:
    wire: dict[str, object] = {
        "instId": PRODUCT["instId"], "clOrdId": "wireplace1", "tdMode": "cross",
        "ordType": "limit", "side": "buy", "sz": "2", "px": "10.2",
    }
    assert _prerequisite()(_request(wire=wire), NOW)


@pytest.mark.parametrize("operation", ["PLACE", "CANCEL"])
@pytest.mark.parametrize(
    "mutation",
    [
        lambda request: request["scope"].update(environment="live"),
        lambda request: request["scope"].update(execution_scope_id="other-account"),
        lambda request: request["scope"].update(strategy_id="other-strategy"),
        lambda request: request.update(executing_account_ref="other-account"),
        lambda request: request["members"][0].update(product_id="OTHER:BTC-USDT-SWAP"),
    ],
)
def test_rehashed_request_identity_mutations_reject(operation, mutation) -> None:
    assert not _prerequisite()(
        _mutated_request(operation, mutation), NOW
    )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda evidence: evidence.update(account_received_at_ms=NOW - 10_001),
        lambda evidence: evidence.update(account_received_at_ms=NOW + 1),
        lambda evidence: evidence.update(instruments_received_at_ms=NOW - 10_001),
        lambda evidence: evidence.update(instruments_received_at_ms=NOW + 1),
    ],
)
def test_place_rejects_independently_stale_future_or_invalid_timestamps(
    mutation: Callable[[dict[str, object]], None]
) -> None:
    evidence = _evidence()
    mutation(evidence)
    assert not _prerequisite(evidence=evidence)(_request(), NOW)


def test_account_uid_must_match_uid_not_main_uid() -> None:
    evidence = _evidence()
    account_row = _object_rows(_object(evidence["account"])["data"])[0]
    account_row.update(uid="another-subaccount")
    account_row["mainUid"] = "uid-demo-1"
    with pytest.raises(ValueError, match="invalid OKX Spider profile prerequisite"):
        _prerequisite(evidence=evidence)


@pytest.mark.parametrize(
    "field", ["account_received_at_ms"]
)
def test_constructor_rejects_bool_as_timestamp(field) -> None:
    evidence = _evidence()
    evidence[field] = True
    with pytest.raises(ValueError, match="invalid OKX Spider profile prerequisite"):
        _prerequisite(evidence=evidence)


def test_place_rejects_bool_instrument_timestamp() -> None:
    evidence = _evidence()
    evidence["instruments_received_at_ms"] = True
    assert not _prerequisite(evidence=evidence)(_request(), NOW)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda selection: selection.pop("expected_uid"),
        lambda selection: selection.update(unapproved=True),
        lambda selection: selection.update(products=[]),
        lambda selection: _object_rows(selection["products"])[0].update(product_id=""),
        lambda selection: _object_rows(selection["products"])[0].update(instId=""),
        lambda selection: _object_rows(selection["products"])[0].update(ctVal="NaN"),
        lambda selection: _object_rows(selection["products"])[0].update(ctMult="Infinity"),
        lambda selection: _object_rows(selection["products"])[0].update(lotSz="0"),
        lambda selection: _object_rows(selection["products"])[0].update(minSz="-1"),
        lambda selection: _object_rows(selection["products"])[0].update(tickSz="NaN"),
        lambda selection: _object_rows(selection["products"]).append({**PRODUCT, "base_ccy": "ETH", "instId": "ETH-USDT-SWAP"}),
        lambda selection: _object_rows(selection["products"]).append({**PRODUCT, "product_id": "other"}),
    ],
)
def test_selection_rejects_missing_extra_empty_nonfinite_and_duplicate_identity(
    mutation: Callable[[dict[str, object]], None]
) -> None:
    selection = deepcopy(SELECTION)
    mutation(selection)
    with pytest.raises(ValueError, match="invalid OKX Spider profile prerequisite"):
        _prerequisite(selection)


@pytest.mark.parametrize(
    "field,value",
    [("instType", "FUTURES"), ("ctVal", "NaN"), ("ctMult", "Infinity"),
     ("lotSz", "0"), ("minSz", "-1"), ("tickSz", "NaN")],
)
def test_place_rejects_invalid_instrument_type_and_spec_values(field, value) -> None:
    evidence = _evidence()
    _object_rows(_object(evidence["instruments"])["data"])[0][field] = value
    assert not _prerequisite(evidence=evidence)(_request(), NOW)


def test_contract_multiplier_does_not_change_contract_lot_admission() -> None:
    selection = deepcopy(SELECTION)
    _object_rows(selection["products"])[0]["ctMult"] = "0.01"
    evidence = _evidence()
    _object_rows(_object(evidence["instruments"])["data"])[0]["ctMult"] = "0.010"
    assert _prerequisite(selection, evidence)(_request(), NOW)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda selection: _object(selection["scope"]).update(environment="live"),
        lambda selection: _object_rows(selection["products"])[0].update(instId="BTC-USDC-SWAP"),
        lambda selection: _object_rows(selection["products"])[0].update(ctMult="NaN"),
        lambda selection: _object_rows(selection["products"]).append(
            cast(dict[str, object], deepcopy(PRODUCT))
        ),
        lambda selection: selection.update(expected_uid=""),
    ],
)
def test_constructor_rejects_malformed_selection_without_exposing_values(
    mutation: Callable[[dict[str, object]], None]
) -> None:
    selection = deepcopy(SELECTION)
    mutation(selection)
    with pytest.raises(ValueError, match="invalid OKX Spider profile prerequisite"):
        _prerequisite(selection)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda evidence: evidence.update(environment="live"),
        lambda evidence: evidence.update(rest_host="www.okx.com.eu"),
        lambda evidence: evidence.update(simulated_trading=False),
        lambda evidence: _object(evidence["account"]).update(code="1"),
        lambda evidence: _object_rows(_object(evidence["account"])["data"])[0].update(uid="other-uid"),
        lambda evidence: evidence.update(account_received_at_ms=True),
    ],
)
def test_constructor_rejects_malformed_common_evidence(
    mutation: Callable[[dict[str, object]], None]
) -> None:
    evidence = _evidence()
    mutation(evidence)
    with pytest.raises(ValueError, match="invalid OKX Spider profile prerequisite"):
        _prerequisite(evidence=evidence)


@pytest.mark.parametrize(
    "field", ["ctVal", "ctMult", "lotSz", "minSz", "tickSz"]
)
def test_place_rejects_missing_instrument_spec_field(field: str) -> None:
    evidence = _evidence()
    del _object_rows(_object(evidence["instruments"])["data"])[0][field]
    assert not _prerequisite(evidence=evidence)(_request(), NOW)


def test_request_requires_canonical_bytes_and_matching_payload_hash() -> None:
    check = _prerequisite()
    request = _request()
    assert not check(request + b" ", NOW)
    corrupted = _object(decode_canonical(request))
    corrupted["payload_sha256"] = "0" * 64
    assert not check(canonical_bytes(corrupted), NOW)


def test_hash_binds_canonical_selection_and_evidence_bytes() -> None:
    check = _prerequisite()
    assert check.content_sha256 == sha256(canonical_bytes({
        "domain": "OKX_SPIDER_PROFILE_PREREQUISITE_V1",
        "selection": SELECTION,
        "evidence": _evidence(),
    })).hexdigest()
    with pytest.raises(ValueError, match="invalid OKX Spider profile prerequisite"):
        create_okx_spider_profile_prerequisite(
            canonical_bytes(SELECTION), canonical_bytes(_evidence()), "0" * 64
        )
    changed_evidence = _evidence()
    selection_bytes = canonical_bytes(deepcopy(SELECTION))
    evidence_bytes = canonical_bytes(changed_evidence)
    digest = sha256(canonical_bytes({
        "domain": "OKX_SPIDER_PROFILE_PREREQUISITE_V1",
        "selection": SELECTION,
        "evidence": changed_evidence,
    })).hexdigest()
    immutable = create_okx_spider_profile_prerequisite(
        selection_bytes, evidence_bytes, digest
    )
    _object_rows(_object(changed_evidence["account"])["data"])[0]["acctLv"] = "2"
    assert immutable(_request(), NOW)
    assert check(_request(), NOW)
    with pytest.raises(ValueError, match="invalid OKX Spider profile prerequisite"):
        replace(immutable, evidence_bytes=canonical_bytes(changed_evidence))
    with pytest.raises(ValueError, match="invalid OKX Spider profile prerequisite"):
        OkxSpiderProfilePrerequisite(
            immutable.selection_bytes,
            canonical_bytes(changed_evidence),
            immutable.content_sha256,
        )
