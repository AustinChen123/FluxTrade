"""Pure tests for the immutable OKX Spider financial profile."""

from copy import deepcopy
from decimal import Decimal
from hashlib import sha256
from typing import cast

import pytest

from src.core.adapters.okx_spider_financial_profile import (
    OkxSpiderFinancialProfileUnavailable,
    create_okx_spider_financial_profile,
)
from src.core.adapters.okx_spider_profile import create_okx_spider_profile_prerequisite
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from tests.test_okx_spider_profile import ACCOUNT, INSTRUMENTS, NOW, PRODUCT, SELECTION


def _prerequisite():
    observed_instruments = deepcopy(INSTRUMENTS)
    observed_instruments["data"][0].update(instFamily="BTC-USDT", groupId="4")
    evidence = {
        "environment": "demo",
        "rest_host": "www.okx.com",
        "simulated_trading": True,
        "account_received_at_ms": NOW,
        "instruments_received_at_ms": NOW,
        "account": deepcopy(ACCOUNT),
        "instruments": observed_instruments,
    }
    selection = deepcopy(SELECTION)
    selection_bytes = canonical_bytes(selection)
    evidence_bytes = canonical_bytes(evidence)
    digest = sha256(canonical_bytes({
        "domain": "OKX_SPIDER_PROFILE_PREREQUISITE_V1",
        "selection": selection,
        "evidence": evidence,
    })).hexdigest()
    return create_okx_spider_profile_prerequisite(selection_bytes, evidence_bytes, digest)


def _profile(
    *, unavailable: str | None = None, mutate_balance=None, mutate_fee=None,
    fixed_usdt: Decimal = Decimal("25"), extra_records=(),
    position_rows=None, pending_pages=None,
    balance_params=None, positions_params=None,
):
    balance_row = {
        "adjEq": "1000", "imr": "100", "mmr": "50", "ordFroz": "0",
        "uTime": str(NOW),
        "details": [
            {"ccy": "USDT", "eq": "800", "isoEq": "100", "availEq": "600", "liab": "0"},
            {"ccy": "BTC", "eq": "0.01", "isoEq": "0", "availEq": "0.01", "liab": "0"},
        ],
    }
    if mutate_balance is not None:
        mutate_balance(balance_row)
    fee_row = {
        "instType": "SWAP", "ts": str(NOW),
        "feeGroup": [{"groupId": "4", "maker": "-0.0001", "taker": "0.0005"}],
    }
    if mutate_fee is not None:
        mutate_fee(fee_row)
    pending_pages = [[]] if pending_pages is None else pending_pages
    pending_records = []
    for index, page in enumerate(pending_pages):
        params = {"limit": "100"}
        if index:
            params["after"] = pending_pages[index - 1][-1]["ordId"]
        pending_records.append({
            "endpoint": "trade/orders-pending", "params": params,
            "received_at_ms": NOW, "response": {"code": "0", "data": page},
        })
    records = (
        {"endpoint": "account/trade-fee", "params": {"instType": "SWAP", "instFamily": "BTC-USDT"}, "received_at_ms": NOW, "response": {"code": "0", "data": [fee_row]}},
        {"endpoint": "account/positions", "params": {} if positions_params is None else positions_params, "received_at_ms": NOW, "response": {"code": "0", "data": [{"instId": "BTC-USDT-SWAP", "pos": "1", "uTime": str(NOW)}] if position_rows is None else position_rows}},
        *pending_records,
        {"endpoint": "account/balance", "params": {} if balance_params is None else balance_params, "received_at_ms": NOW, "response": {"code": "0", "data": [balance_row]}},
    ) + tuple(extra_records)
    return create_okx_spider_financial_profile(
        _prerequisite(), fixed_usdt, records, unavailable
    )


def _rebuild(profile, mutate):
    value = decode_canonical(profile.profile_bytes)
    mutate(value)
    raw = canonical_bytes(value)
    return type(profile)(raw, sha256(raw).hexdigest())


def _order(index: int, *, inst_id: str = "BTC-USDT-SWAP"):
    return {
        "ordId": f"order-{index}", "instId": inst_id,
        "sz": "2", "accFillSz": "0", "uTime": str(NOW),
    }


def _assert_invalid_finance_bypasses(profile) -> None:
    from tests.test_okx_spider_profile import _mutated_request, _request

    assert profile(_request("CANCEL"), NOW)
    reduce_only = _mutated_request("PLACE", _set_first_reduce_only)
    assert profile(reduce_only, NOW)
    assert not profile(_request(), NOW)


def _first_member_wire(body: dict[str, object]) -> dict[str, object]:
    members = cast(list[dict[str, object]], body["members"])
    return cast(dict[str, object], members[0]["request_payload"])


def _set_first_reduce_only(body: dict[str, object]) -> None:
    _first_member_wire(body)["reduceOnly"] = True


def test_projection_keeps_currency_rows_signed_fees_and_explicit_earn_zero():
    projection = _profile().projection(NOW)
    assert projection.cross_usdt_equity == Decimal("700")
    assert projection.usdt_available_equity == Decimal("600")
    assert projection.source_total == Decimal("675")
    assert projection.earn_exclusion == Decimal(0)
    assert projection.currency_equity_liabilities == (
        ("USDT", Decimal("800"), Decimal("0")),
        ("BTC", Decimal("0.01"), Decimal("0")),
    )
    assert projection.product_contract_values == ((PRODUCT["product_id"], Decimal("0.01")),)
    assert projection.product_fee_rates == (
        (PRODUCT["product_id"], "4", "BTC-USDT", Decimal("-0.0001"), Decimal("0.0005")),
    )


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-1"])
def test_invalid_balance_or_fee_evidence_fails_projection_closed(bad: str):
    profile = _profile(mutate_balance=lambda row: row.update(ordFroz=bad))
    with pytest.raises(OkxSpiderFinancialProfileUnavailable):
        profile.projection(NOW)
    if bad != "-1":
        profile = _profile(mutate_fee=lambda row: row["feeGroup"][0].update(taker=bad))
        with pytest.raises(OkxSpiderFinancialProfileUnavailable):
            profile.projection(NOW)


def test_stale_financial_or_fee_capture_fails_projection():
    profile = _profile()
    value = cast(dict[str, object], decode_canonical(profile.profile_bytes))
    records = cast(list[dict[str, object]], value["records"])
    records[0]["received_at_ms"] = NOW - 60_001
    raw = canonical_bytes(value)
    stale_fee = type(profile)(raw, sha256(raw).hexdigest())
    with pytest.raises(OkxSpiderFinancialProfileUnavailable):
        stale_fee.projection(NOW)
    value = cast(dict[str, object], decode_canonical(profile.profile_bytes))
    records = cast(list[dict[str, object]], value["records"])
    records[-1]["received_at_ms"] = NOW - 10_001
    raw = canonical_bytes(value)
    stale_balance = type(profile)(raw, sha256(raw).hexdigest())
    with pytest.raises(OkxSpiderFinancialProfileUnavailable):
        stale_balance.projection(NOW)
    with pytest.raises(OkxSpiderFinancialProfileUnavailable):
        profile.projection(True)


@pytest.mark.parametrize(
    "now_offset", [0, 10_000, 10_001, -1],
)
def test_financial_capture_freshness_boundaries(now_offset: int):
    from tests.test_okx_spider_profile import _request

    profile = _profile()
    admitted = profile(_request(), NOW + now_offset)
    assert admitted is (now_offset in (0, 10_000))


@pytest.mark.parametrize("fee_age", [0, 60_000, 60_001, -1])
def test_fee_freshness_boundaries(fee_age: int):
    profile = _profile()
    value = cast(dict[str, object], decode_canonical(profile.profile_bytes))
    records = cast(list[dict[str, object]], value["records"])
    records[0]["received_at_ms"] = NOW - fee_age
    raw = canonical_bytes(value)
    changed = type(profile)(raw, sha256(raw).hexdigest())
    try:
        changed.projection(NOW)
        valid = True
    except OkxSpiderFinancialProfileUnavailable:
        valid = False
    assert valid is (0 <= fee_age <= 60_000)


@pytest.mark.parametrize(
    "mutate,fixed,expected",
    [
        (lambda row: row["details"][0].update(eq="100", isoEq="100"), Decimal("25"), False),
        (lambda row: row["details"][0].update(eq="30", isoEq="0"), Decimal("30"), False),
        (lambda row: row["details"][0].update(availEq="0"), Decimal("25"), False),
        (lambda row: row.update(adjEq="100", imr="100"), Decimal("25"), False),
        (lambda row: row.update(adjEq="99", imr="100"), Decimal("25"), False),
        (lambda row: row.update(adjEq="50", mmr="50"), Decimal("25"), False),
        (lambda row: row.update(adjEq="49", mmr="50"), Decimal("25"), False),
    ],
)
def test_place_admission_margin_and_capital_boundaries(mutate, fixed, expected):
    from tests.test_okx_spider_profile import _request

    profile = _profile(mutate_balance=mutate, fixed_usdt=fixed)
    assert profile(_request(), NOW) is expected


def test_non_usdt_equity_is_retained_but_does_not_increase_usdt_capital():
    from tests.test_okx_spider_profile import _request

    profile = _profile()
    baseline = profile.projection(NOW)
    value = cast(dict[str, object], decode_canonical(profile.profile_bytes))
    records = cast(list[dict[str, object]], value["records"])
    response = cast(dict[str, object], records[-1]["response"])
    data = cast(list[dict[str, object]], response["data"])
    details = cast(list[dict[str, object]], data[0]["details"])
    details[1]["eq"] = "999999999"
    details[1]["liab"] = "4.25"
    raw = canonical_bytes(value)
    changed = type(profile)(raw, sha256(raw).hexdigest()).projection(NOW)
    assert changed.cross_usdt_equity == baseline.cross_usdt_equity
    assert changed.currency_equity_liabilities[1] == (
        "BTC", Decimal("999999999"), Decimal("4.25")
    )
    assert profile(_request(), NOW)


def test_balance_and_positions_require_unfiltered_empty_params():
    from tests.test_okx_spider_profile import _request

    with pytest.raises(OkxSpiderFinancialProfileUnavailable):
        _profile(balance_params={"ccy": "USDT"}).projection(NOW)
    with pytest.raises(OkxSpiderFinancialProfileUnavailable):
        _profile(positions_params={"instType": "SWAP"}).projection(NOW)
    assert _profile()(_request(), NOW)


@pytest.mark.parametrize("timestamp", ["123", str(NOW), "not-an-integer", True])
def test_currency_provider_timestamps_are_format_checked_but_not_freshness_checked(timestamp):
    profile = _profile()
    changed = _rebuild(
        profile,
        lambda value: value["records"][-1]["response"]["data"][0]["details"][0].update(ts=timestamp),
    )
    if type(timestamp) is str and timestamp.isdigit():
        assert changed.projection(NOW).cross_usdt_equity == Decimal("700")
    else:
        with pytest.raises(OkxSpiderFinancialProfileUnavailable):
            changed.projection(NOW)


def test_short_and_hundred_plus_short_pending_pages_retain_unselected_obligations():
    from tests.test_okx_spider_profile import _request

    short = [_order(1), _order(2, inst_id="ETH-USDT-SWAP")]
    profile = _profile(
        position_rows=[{"instId": "ETH-USDT-SWAP", "pos": "3", "ts": "1"}],
        pending_pages=[short],
    )
    projection = profile.projection(NOW)
    assert projection.provider_positions == (("ETH-USDT-SWAP", Decimal("3")),)
    assert projection.provider_pending_orders == (
        ("order-1", "BTC-USDT-SWAP", Decimal("2"), Decimal("0")),
        ("order-2", "ETH-USDT-SWAP", Decimal("2"), Decimal("0")),
    )
    assert profile(_request(), NOW)

    full_page = [_order(index) for index in range(100)]
    final_page = [_order(100, inst_id="ETH-USDT-SWAP")]
    paged = _profile(
        position_rows=[{"instId": "ETH-USDT-SWAP", "pos": "3"}],
        pending_pages=[full_page, final_page],
    )
    projection = paged.projection(NOW)
    assert len(projection.provider_pending_orders) == 101
    assert projection.provider_pending_orders[-1][1] == "ETH-USDT-SWAP"
    assert paged(_request(), NOW)


@pytest.mark.parametrize(
    "corruption",
    [
        "missing_tail", "duplicate_order_id", "wrong_cursor", "invalid_sz",
        "invalid_acc_fill", "stale_positions", "future_positions",
        "stale_pending", "future_pending",
    ],
)
def test_incomplete_or_invalid_positions_and_pending_block_place_but_not_cancel_ro(corruption):
    full = [_order(index) for index in range(100)]
    tail = [_order(100)]
    position_receipt = NOW
    pending_receipt = NOW
    pages = [full, tail]
    positions = [{"instId": "ETH-USDT-SWAP", "pos": "3", "uTime": str(NOW)}]
    if corruption == "missing_tail":
        pages = [full]
    elif corruption == "duplicate_order_id":
        pages = [full, [_order(0)]]
    elif corruption == "wrong_cursor":
        pages = [full, tail]
    elif corruption == "invalid_sz":
        pages = [[{**_order(1), "sz": "NaN"}]]
    elif corruption == "invalid_acc_fill":
        pages = [[{**_order(1), "accFillSz": "Infinity"}]]
    elif corruption == "stale_positions":
        position_receipt = NOW - 10_001
    elif corruption == "future_positions":
        position_receipt = NOW + 1
    elif corruption == "stale_pending":
        pending_receipt = NOW - 10_001
    elif corruption == "future_pending":
        pending_receipt = NOW + 1
    profile = _profile(
        position_rows=positions, pending_pages=pages,
    )
    if corruption in {"stale_positions", "future_positions", "stale_pending", "future_pending"}:
        target_endpoint = "account/positions" if "positions" in corruption else "trade/orders-pending"
        target_receipt = position_receipt if target_endpoint == "account/positions" else pending_receipt
        profile = _rebuild(
            profile,
            lambda value: next(
                row for row in value["records"] if row["endpoint"] == target_endpoint
            ).update(received_at_ms=target_receipt),
        )
    if corruption == "wrong_cursor":
        profile = _rebuild(
            profile,
            lambda value: next(
                row for row in value["records"]
                if row["endpoint"] == "trade/orders-pending" and row["params"].get("after")
            )["params"].update(after="not-last-ordid"),
        )
    with pytest.raises(OkxSpiderFinancialProfileUnavailable):
        profile.projection(NOW)
    _assert_invalid_finance_bypasses(profile)


@pytest.mark.parametrize(
    "change",
    ["duplicate_usdt", "missing_usdt", "duplicate_fee", "wrong_fee_type", "wrong_fee_family", "wrong_group"],
)
def test_ambiguous_or_mismatched_provider_fee_and_currency_records_fail(change: str):
    profile = _profile()
    value = cast(dict[str, object], decode_canonical(profile.profile_bytes))
    records = cast(list[dict[str, object]], value["records"])
    if change == "duplicate_usdt":
        response = cast(dict[str, object], records[-1]["response"])
        data = cast(list[dict[str, object]], response["data"])
        details = cast(list[dict[str, object]], data[0]["details"])
        details.append(deepcopy(details[0]))
    elif change == "missing_usdt":
        response = cast(dict[str, object], records[-1]["response"])
        data = cast(list[dict[str, object]], response["data"])
        details = cast(list[dict[str, object]], data[0]["details"])
        data[0]["details"] = [
            row for row in details
            if row["ccy"] != "USDT"
        ]
    else:
        response = cast(dict[str, object], records[0]["response"])
        data = cast(list[dict[str, object]], response["data"])
        fee_row = data[0]
        if change == "wrong_fee_type":
            fee_row["instType"] = "FUTURES"
        elif change == "wrong_fee_family":
            cast(dict[str, object], records[0]["params"])["instFamily"] = "OTHER"
        elif change == "wrong_group":
            cast(list[dict[str, object]], fee_row["feeGroup"])[0]["groupId"] = "other"
        else:
            records.append(deepcopy(records[0]))
    raw = canonical_bytes(value)
    changed = type(profile)(raw, sha256(raw).hexdigest())
    with pytest.raises(OkxSpiderFinancialProfileUnavailable):
        changed.projection(NOW)


def test_financial_unavailable_preserves_cancel_and_reduce_only_bypass():
    from tests.test_okx_spider_profile import _request

    profile = _profile(unavailable="CAPTURE_FAILED")
    assert profile(_request("CANCEL"), NOW)
    place = _request()
    assert not profile(place, NOW)
    from tests.test_okx_spider_profile import _mutated_request

    reduce_only = _mutated_request("PLACE", _set_first_reduce_only)
    assert profile(reduce_only, NOW)
    def set_string_reduce_only(body: dict[str, object]) -> None:
        _first_member_wire(body)["reduceOnly"] = "true"

    string_true = _mutated_request("PLACE", set_string_reduce_only)
    assert not profile(string_true, NOW)
    def append_non_reduce_only_member(body: dict[str, object]) -> None:
        members = cast(list[dict[str, object]], body["members"])
        members.append({
            **deepcopy(members[0]),
            "action_id": "e" * 64,
            "local_order_id": "local-place-2",
            "wire_clOrdId": "wireplace2",
            "request_payload": {
                "instId": PRODUCT["instId"], "clOrdId": "wireplace2", "tdMode": "cross",
                "posSide": "net", "ordType": "limit", "side": "buy", "sz": "2", "px": "10.2",
            },
        })

    mixed = _mutated_request("PLACE", append_non_reduce_only_member)
    assert not profile(mixed, NOW)


def test_profile_rejects_mutated_canonical_hash_and_unavailable_value():
    profile = _profile()
    with pytest.raises(ValueError):
        type(profile)(profile.profile_bytes, "0" * 64)
    with pytest.raises(ValueError):
        create_okx_spider_financial_profile(
            _prerequisite(), Decimal("25"), (), "unexpected"
        )
