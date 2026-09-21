from dataclasses import FrozenInstanceError
from typing import Callable, cast
from urllib.parse import urlencode

import pytest

from src.control_plane import profile_http_contract as contract
from src.core.market_data.profiles.live_query import (
    LiveProfileQueryUnavailable,
)

FIELDS = dict(
    product_id="BINANCE:BTCUSDT-SPOT",
    base_grid_id="btc_spot_usdt_10_v1",
    output_grid_id="btc_spot_usdt_10_v1",
    algorithm_version="vp-v1",
    start_ms="0",
    end_ms="86400000",
    purpose="LIVE_QUERY",
    freshness_policy_id="utc_complete_strict_v1",
)
RAW = urlencode(FIELDS).encode()


def test_valid_query_and_optional_revision() -> None:
    request = contract.parse_profile_query(RAW)
    assert (request.start_ms, request.end_ms, request.revision) == (0, 86400000, None)
    assert (
        request.product_id == FIELDS["product_id"] and request.purpose == "LIVE_QUERY"
    )
    assert contract.parse_profile_query(RAW + b"&revision=1").revision == 1
    assert (
        contract.parse_profile_query(RAW.replace(b"start_ms", b"%73tart_ms")) == request
    )
    with pytest.raises(FrozenInstanceError):
        setattr(request, "revision", 2)


@pytest.mark.parametrize(
    "raw",
    [
        b"",
        b"&",
        RAW + b"&",
        RAW + b"&start_ms=0",
        RAW + b"&%73tart_ms=0",
        RAW + b"&SECRET=x",
        RAW + b"&revision",
        RAW + b"&revision=",
        RAW + b"&revision=%",
        RAW + b"&revision=%0",
        RAW + b"&revision=%GG",
        RAW + b"&revision=%ff",
        RAW + b"&revision=\xff",
        RAW + b"&revision=%00",
        RAW + b"&revision=+",
        b"x" * 4097,
        RAW.decode(),
        bytearray(RAW),
        type("BytesSubclass", (bytes,), {})(RAW),
    ],
)
def test_raw_adversarial(raw: object) -> None:
    with pytest.raises(contract.InvalidProfileHttpRequest) as caught:
        cast(Callable[..., object], contract.parse_profile_query)(raw)
    assert str(caught.value) == "INVALID_REQUEST"
    assert caught.value.__cause__ is None and caught.value.__suppress_context__
    assert contract.profile_http_error(caught.value) == contract.ProfileHttpError(
        400, "INVALID_REQUEST"
    )


@pytest.mark.parametrize("field", list(FIELDS))
def test_missing_and_blank_fields(field: str) -> None:
    values = FIELDS.copy()
    del values[field]
    with pytest.raises(contract.InvalidProfileHttpRequest):
        contract.parse_profile_query(urlencode(values).encode())
    values[field] = ""
    with pytest.raises(contract.InvalidProfileHttpRequest):
        contract.parse_profile_query(urlencode(values).encode())


@pytest.mark.parametrize(
    "value",
    [
        "00",
        "01",
        "+1",
        "-1",
        "1.0",
        "1e3",
        "١",
        " 1",
        "1 ",
        "9223372036854775808",
        "9" * 4301,
    ],
)
@pytest.mark.parametrize("field", ["start_ms", "end_ms", "revision"])
def test_canonical_integer_domain(field: str, value: str) -> None:
    with pytest.raises(contract.InvalidProfileHttpRequest):
        contract.parse_profile_query(urlencode(FIELDS | {field: value}).encode())


@pytest.mark.parametrize(
    "field",
    [
        "decision_time",
        "decision_time_ms",
        "as_of",
        "as_of_ms",
        "pinned_manifest",
        "cursor",
        "page_size",
    ],
)
def test_forbidden_inputs(field: str) -> None:
    with pytest.raises(contract.InvalidProfileHttpRequest):
        contract.parse_profile_query(RAW + b"&" + field.encode() + b"=SECRET")


@pytest.mark.parametrize(
    "field,value",
    [
        ("purpose", "RECORDED_REPLAY"),
        ("purpose", "MODELED_RESEARCH"),
        ("freshness_policy_id", "SECRET"),
        ("product_id", "SECRET"),
        ("end_ms", "1"),
        ("revision", "0"),
    ],
)
def test_domain_rejections(field: str, value: str) -> None:
    with pytest.raises(contract.InvalidProfileHttpRequest):
        contract.parse_profile_query(urlencode(FIELDS | {field: value}).encode())


@pytest.mark.parametrize(
    "reason,status,code",
    [
        ("NOT_READY", 404, "PROFILE_NOT_READY"),
        ("PROFILE_EXPIRED", 404, "PROFILE_EXPIRED"),
        ("SNAPSHOT_REVOKED", 409, "SNAPSHOT_REVOKED"),
        ("QUERY_TOO_LARGE", 400, "QUERY_TOO_LARGE"),
        ("BACKEND_UNAVAILABLE", 503, "BACKEND_UNAVAILABLE"),
        ("INVALID_PROFILE", 503, "BACKEND_UNAVAILABLE"),
    ],
)
def test_unavailable_mapping(reason: str, status: int, code: str) -> None:
    assert contract.profile_http_error(
        LiveProfileQueryUnavailable(reason)
    ) == contract.ProfileHttpError(status, code)
