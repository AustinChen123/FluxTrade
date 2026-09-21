from dataclasses import FrozenInstanceError, asdict
from typing import Callable, cast
from urllib.parse import urlencode

import pytest

from src.control_plane import profile_http_contract as contract
from src.core.market_data.profiles.live_query import (
    LiveProfileQueryUnavailable,
    ProfileQueryError,
)

FIELDS = dict(
    product_id="BINANCE:BTCUSDT-SPOT",
    base_grid_id="btc_spot_usdt_10_v1",
    output_grid_id="btc_spot_usdt_100_v1",
    algorithm_version="vp-v1",
    start_ms="0",
    end_ms="86400000",
    purpose="LIVE_QUERY",
    freshness_policy_id="utc_complete_strict_v1",
)
RAW = urlencode(FIELDS).encode()


def test_valid_query_and_optional_revision() -> None:
    request = contract.parse_profile_query(RAW)
    assert (
        request.product_id,
        request.base_grid_id,
        request.output_grid_id,
        request.algorithm_version,
        request.start_ms,
        request.end_ms,
        request.purpose,
        request.freshness_policy_id,
        request.availability_policy_id,
        request.as_of_ms,
        request.revision,
        request.pinned_manifest,
    ) == (
        "BINANCE:BTCUSDT-SPOT",
        "btc_spot_usdt_10_v1",
        "btc_spot_usdt_100_v1",
        "vp-v1",
        0,
        86400000,
        "LIVE_QUERY",
        "utc_complete_strict_v1",
        None,
        None,
        None,
        None,
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


def test_raw_size_limit_precedes_decode(monkeypatch: pytest.MonkeyPatch) -> None:
    original = contract._decode
    calls: list[bytes] = []

    def observe(value: bytes) -> str:
        calls.append(value)
        return original(value)

    monkeypatch.setattr(contract, "_decode", observe)
    with pytest.raises(contract.InvalidProfileHttpRequest):
        contract.parse_profile_query(b"a=" + b"x" * 4095)
    assert calls == []
    with pytest.raises(contract.InvalidProfileHttpRequest):
        contract.parse_profile_query(b"a=" + b"x" * 4094)
    assert calls


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


def test_integer_and_window_boundary_handoff() -> None:
    assert (
        contract.parse_profile_query(
            urlencode(FIELDS | {"revision": str((1 << 63) - 1)}).encode()
        ).revision
        == (1 << 63) - 1
    )
    assert (
        contract.parse_profile_query(
            urlencode(FIELDS | {"end_ms": str(90 * 86400000)}).encode()
        ).end_ms
        == 90 * 86400000
    )
    for values in (
        FIELDS | {"end_ms": str(91 * 86400000)},
        FIELDS | {"end_ms": str(2 * 86400000), "revision": "1"},
    ):
        with pytest.raises(contract.InvalidProfileHttpRequest):
            contract.parse_profile_query(urlencode(values).encode())


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


@pytest.mark.parametrize(
    "error,status,code",
    [
        (ProfileQueryError("INVALID"), 400, "INVALID_REQUEST"),
        (ProfileQueryError("INTEGRITY"), 503, "BACKEND_UNAVAILABLE"),
        (RuntimeError("SECRET SQL/path/url"), 503, "BACKEND_UNAVAILABLE"),
        (ValueError("SECRET"), 503, "BACKEND_UNAVAILABLE"),
    ],
)
def test_exception_projection(error: Exception, status: int, code: str) -> None:
    response = contract.profile_http_error(error)
    assert asdict(response) == {"status": status, "code": code}
    assert "SECRET" not in repr(response) and not hasattr(response, "__dict__")


def test_exact_error_dto() -> None:
    constructor = cast(Callable[..., object], contract.ProfileHttpError)
    for status, code in (
        (True, "INVALID_REQUEST"),
        (400, "SECRET"),
        (503, "INVALID_REQUEST"),
        (400, type("Text", (str,), {})("INVALID_REQUEST")),
    ):
        with pytest.raises(ValueError):
            constructor(status, code)
    with pytest.raises(FrozenInstanceError):
        setattr(contract.ProfileHttpError(400, "INVALID_REQUEST"), "code", "SECRET")
    with pytest.raises(TypeError):
        cast(Callable[..., object], contract.profile_http_error)(
            KeyboardInterrupt("SECRET")
        )
