from datetime import UTC, datetime
from decimal import Decimal, localcontext
import base64
import hashlib
import hmac
from typing import Callable, cast
from urllib.parse import quote

import pytest

from src.control_plane.backtest_result_http_contract import (
    CandlesCursorBinding,
    CandlesCursor,
    IndexCursor,
    IndexCursorBinding,
    InvalidBacktestResultHttpRequest,
    TradesCursorBinding,
    TradesCursor,
    encode_result_cursor,
    format_utc_milliseconds,
    parse_job_id,
    parse_result_query,
    verify_result_cursor,
    wire_decimal,
)


@pytest.mark.parametrize(
    ("endpoint", "raw", "expected"),
    [
        ("index", b"", (100, None, None, None)),
        ("index", b"limit=000500&cursor=next%2Bpage", (500, "next+page", None, None)),
        ("index", b"cursor=a+b", (100, "a b", None, None)),
        ("index", b"limit=" + b"0" * 100 + b"1", (1, None, None, None)),
        ("index", b"limit=" + b"0" * 4100 + b"1", (1, None, None, None)),
        ("trades", b"limit=1", (1, None, None, None)),
        ("candles", b"start=0000&end=1&limit=2&cursor=c", (2, "c", 0, 1)),
        ("detail", b"", (None, None, None, None)),
    ],
)
def test_parse_route_queries(endpoint, raw, expected):
    query = parse_result_query(endpoint, raw)
    assert (query.limit, query.cursor, query.start_ms, query.end_ms) == expected


@pytest.mark.parametrize(
    ("endpoint", "raw"),
    [
        ("index", b"limit=0"),
        ("index", b"limit=501"),
        ("index", b"limit=+1"),
        ("index", b"limit=%201"),
        ("index", b"limit=1.0"),
        ("index", b"limit=1e2"),
        ("index", b"limit=1&limit=2"),
        ("index", b"cursor="),
        ("index", b"cursor=%GG"),
        ("index", b"unknown=x"),
        ("index", b"limit"),
        ("index", b"=x"),
        ("index", b"limit=1&"),
        ("index", b"limit=%ff"),
        ("detail", b"cursor=x"),
        ("candles", b"start=0&end=0"),
        ("candles", b"start=2&end=1"),
        ("candles", b"start=253402300800000&end=253402300800001"),
        ("candles", b"start=0&end=1&cursor=x&extra=1"),
    ],
)
def test_invalid_route_queries_have_fixed_error(endpoint, raw):
    with pytest.raises(InvalidBacktestResultHttpRequest) as caught:
        parse_result_query(endpoint, raw)
    assert str(caught.value) == "validation_error"
    assert caught.value.__cause__ is None


@pytest.mark.parametrize(
    "raw",
    [b"", b"%2e", b".", b"..", b"a%2fb", b"a%5cb", b"a%00b", b"a%0ab", b"%ff"],
)
def test_job_id_decodes_then_rejects_blank_path_or_control_text(raw):
    with pytest.raises(InvalidBacktestResultHttpRequest):
        parse_job_id(raw)


def test_job_id_is_decoded_utf8_path_free_text():
    assert parse_job_id(b"job%3Aone") == "job:one"
    assert parse_job_id(quote("資料", safe="").encode()) == "資料"
    assert parse_job_id(b"a+b") == "a+b"
    assert parse_job_id(b"a%2Bb") == "a+b"
    assert parse_job_id(b"a%20b") == "a b"
    assert parse_job_id(b"a+b") != parse_job_id(b"a%20b")
    assert parse_job_id(b"x" * 4100) == "x" * 4100


def test_utc_milliseconds_use_exact_utc_epoch_conversion():
    assert format_utc_milliseconds(0) == "1970-01-01T00:00:00.000Z"
    assert format_utc_milliseconds(1) == "1970-01-01T00:00:00.001Z"
    assert format_utc_milliseconds(253402300799999) == "9999-12-31T23:59:59.999Z"
    assert datetime.fromtimestamp(0, UTC).isoformat() == "1970-01-01T00:00:00+00:00"
    for invalid in (-1, 253402300800000, True, 1.0):
        with pytest.raises(ValueError):
            cast(Callable[..., str], format_utc_milliseconds)(invalid)


def test_wire_decimal_reuses_exact_canonical_owner_across_contexts():
    for precision in (2, 50):
        with localcontext() as context:
            context.prec = precision
            assert wire_decimal(Decimal("-0.000")) == "0"
            assert wire_decimal(Decimal("12345678901234567890.1200")) == (
                "12345678901234567890.12"
            )
    for invalid in (1.25, True, Decimal("NaN"), Decimal("Infinity")):
        with pytest.raises(ValueError):
            cast(Callable[..., str], wire_decimal)(invalid)


def test_signed_cursor_canonical_route_payload_and_exact_signature():
    key = bytes(range(32))
    cases = (
        (
            IndexCursor(1000, "job:1"),
            b'{"completed_at":1000,"job_id":"job:1","route":"index"}',
        ),
        (
            TradesCursor("資料", "a" * 64, 4),
            f'{{"job_id":"資料","result_digest":"{"a" * 64}","route":"trades","sequence":4}}'.encode(),
        ),
        (
            CandlesCursor("job:1", "b" * 64, 100, 200, 150),
            b'{"end":200,"job_id":"job:1","result_digest":"'
            + b"b" * 64
            + b'","route":"candles","start":100,"timestamp":150}',
        ),
    )
    for payload, expected_json in cases:
        token = encode_result_cursor(key, payload)
        encoded, signature = token.split(".")
        assert token == encode_result_cursor(key, payload)
        assert (
            base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
            == expected_json
        )
        assert signature == hmac.new(key, encoded.encode(), hashlib.sha256).hexdigest()


@pytest.mark.parametrize(
    ("key", "payload"),
    [
        (b"short", IndexCursor(1, "job")),
        (bytes(32), IndexCursor(True, "job")),
        (bytes(32), cast(Callable[..., IndexCursor], IndexCursor)(1.0, "job")),
        (bytes(32), IndexCursor(-1, "job")),
        (bytes(32), IndexCursor(1, "../job")),
        (bytes(32), TradesCursor("job", "A" * 64, 0)),
        (bytes(32), TradesCursor("job", "a" * 64, True)),
        (bytes(32), CandlesCursor("job", "a" * 64, 1, 2, 2)),
    ],
)
def test_signed_cursor_rejects_invalid_key_or_payload(key, payload):
    with pytest.raises(InvalidBacktestResultHttpRequest):
        encode_result_cursor(key, payload)


def test_signed_cursor_roundtrips_and_binds_route_result_and_filter():
    key = bytes(range(32))
    cases = (
        (IndexCursor(1000, "job:1"), IndexCursorBinding()),
        (TradesCursor("%2F", "a" * 64, 4), TradesCursorBinding("%2F", "a" * 64)),
        (
            CandlesCursor("job:1", "b" * 64, 100, 200, 150),
            CandlesCursorBinding("job:1", "b" * 64, 100, 200),
        ),
    )
    for payload, binding in cases:
        token = encode_result_cursor(key, payload)
        assert verify_result_cursor(key, token, binding) == payload
        if isinstance(payload, TradesCursor):
            assert (
                parse_result_query(
                    "trades", ("limit=7&cursor=" + token).encode()
                ).cursor
                == token
            )
    failures = (
        (
            bytes(reversed(range(32))),
            encode_result_cursor(key, cases[1][0]),
            cases[1][1],
        ),
        (key, encode_result_cursor(key, cases[1][0])[:-1] + "0", cases[1][1]),
        (
            key,
            encode_result_cursor(key, cases[1][0]),
            TradesCursorBinding("other", "a" * 64),
        ),
        (
            key,
            encode_result_cursor(key, cases[1][0]),
            TradesCursorBinding("%2F", "b" * 64),
        ),
        (
            key,
            encode_result_cursor(key, cases[2][0]),
            CandlesCursorBinding("job:1", "b" * 64, 0, 200),
        ),
        (
            key,
            encode_result_cursor(key, cases[2][0]),
            CandlesCursorBinding("job:1", "b" * 64, 100, 201),
        ),
        (
            key,
            encode_result_cursor(key, cases[2][0]),
            cast(Callable[..., CandlesCursorBinding], CandlesCursorBinding)(
                "job:1", "b" * 64, True, 200
            ),
        ),
        (key, encode_result_cursor(key, cases[1][0]), IndexCursorBinding()),
    )
    for bad_key, token, binding in failures:
        with pytest.raises(InvalidBacktestResultHttpRequest) as caught:
            verify_result_cursor(bad_key, token, binding)
        assert (
            str(caught.value) == "validation_error" and caught.value.__cause__ is None
        )


def test_signed_cursor_rejects_authenticated_noncanonical_or_invalid_json():
    key = bytes(32)
    raw_values = (
        b'{"completed_at":1,"job_id":"job","route":"index","x":1}',
        b'{ "completed_at":1,"job_id":"job","route":"index"}',
        b'{"completed_at":1,"job_id":"job","route":"index","route":"index"}',
        b'{"completed_at":true,"job_id":"job","route":"index"}',
        b'{"completed_at":1.0,"job_id":"job","route":"index"}',
        b'{"completed_at":NaN,"job_id":"job","route":"index"}',
    )
    for raw in raw_values:
        encoded = base64.urlsafe_b64encode(raw).rstrip(b"=").decode()
        signature = hmac.new(key, encoded.encode(), hashlib.sha256).hexdigest()
        with pytest.raises(InvalidBacktestResultHttpRequest):
            verify_result_cursor(key, encoded + "." + signature, IndexCursorBinding())
    signature = hmac.new(key, b"a", hashlib.sha256).hexdigest()
    with pytest.raises(InvalidBacktestResultHttpRequest):
        verify_result_cursor(key, "a." + signature, IndexCursorBinding())
