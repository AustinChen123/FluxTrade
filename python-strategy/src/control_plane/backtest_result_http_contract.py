"""Pure request and wire rules for the backtest-results HTTP projections."""

from dataclasses import dataclass
import base64
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import hashlib
import hmac
import json
import re
import unicodedata
from urllib.parse import unquote_to_bytes

from src.core.decimal_math import canonical_decimal_text

_MAX_UTC_MS = 253402300799999
_ALLOWED_KEYS = {
    "index": frozenset(("limit", "cursor")),
    "detail": frozenset(),
    "trades": frozenset(("limit", "cursor")),
    "candles": frozenset(("start", "end", "limit", "cursor")),
}


class InvalidBacktestResultHttpRequest(ValueError):
    """Fixed public validation failure; request detail is never retained."""

    def __init__(self) -> None:
        super().__init__("validation_error")


@dataclass(frozen=True, slots=True)
class BacktestResultQuery:
    limit: int | None = None
    cursor: str | None = None
    start_ms: int | None = None
    end_ms: int | None = None


@dataclass(frozen=True, slots=True)
class IndexCursor:
    completed_at: int
    job_id: str


@dataclass(frozen=True, slots=True)
class TradesCursor:
    job_id: str
    result_digest: str
    sequence: int


@dataclass(frozen=True, slots=True)
class CandlesCursor:
    job_id: str
    result_digest: str
    start: int
    end: int
    timestamp: int


ResultCursor = IndexCursor | TradesCursor | CandlesCursor


@dataclass(frozen=True, slots=True)
class IndexCursorBinding:
    pass


@dataclass(frozen=True, slots=True)
class TradesCursorBinding:
    job_id: str
    result_digest: str


@dataclass(frozen=True, slots=True)
class CandlesCursorBinding:
    job_id: str
    result_digest: str
    start: int
    end: int


ResultCursorBinding = IndexCursorBinding | TradesCursorBinding | CandlesCursorBinding


def _decode_component(raw: bytes, *, form_encoded: bool = False) -> str:
    if re.search(rb"%(?![0-9A-Fa-f]{2})", raw):
        raise ValueError
    if form_encoded:
        raw = raw.replace(b"+", b" ")
    value = unquote_to_bytes(raw).decode("utf-8", "strict")
    if (
        not value
        or not value.strip()
        or any(unicodedata.category(char) == "Cc" for char in value)
    ):
        raise ValueError
    return value


def _integer(value: str, *, maximum: int) -> int:
    if re.fullmatch(r"[0-9]+", value, flags=re.ASCII) is None:
        raise ValueError
    significant = value.lstrip("0") or "0"
    if len(significant) > len(str(maximum)):
        raise ValueError
    parsed = int(significant)
    if parsed > maximum:
        raise ValueError
    return parsed


def parse_result_query(endpoint: str, raw_query: bytes) -> BacktestResultQuery:
    """Parse exact raw query bytes under one endpoint's closed key set."""
    try:
        if type(endpoint) is not str or endpoint not in _ALLOWED_KEYS:
            raise ValueError
        if type(raw_query) is not bytes:
            raise ValueError
        values: dict[str, str] = {}
        if raw_query:
            for pair in raw_query.split(b"&"):
                key_raw, separator, value_raw = pair.partition(b"=")
                if not separator:
                    raise ValueError
                key = _decode_component(key_raw, form_encoded=True)
                value = _decode_component(value_raw, form_encoded=True)
                if key not in _ALLOWED_KEYS[endpoint] or key in values:
                    raise ValueError
                values[key] = value

        limit = _integer(values["limit"], maximum=500) if "limit" in values else None
        if limit is not None and limit < 1:
            raise ValueError
        cursor = values.get("cursor")
        start_ms = end_ms = None
        if endpoint == "candles":
            if "start" not in values or "end" not in values:
                raise ValueError
            start_ms = _integer(values["start"], maximum=_MAX_UTC_MS)
            end_ms = _integer(values["end"], maximum=_MAX_UTC_MS)
            if start_ms >= end_ms:
                raise ValueError
        return BacktestResultQuery(
            limit=100
            if endpoint in ("index", "trades", "candles") and limit is None
            else limit,
            cursor=cursor,
            start_ms=start_ms,
            end_ms=end_ms,
        )
    except (UnicodeDecodeError, ValueError):
        raise InvalidBacktestResultHttpRequest() from None


def parse_job_id(raw_segment: bytes) -> str:
    """Decode one URL path segment and reject empty, path, and control text."""
    try:
        if type(raw_segment) is not bytes or not raw_segment:
            raise ValueError
        value = _decode_component(raw_segment)
        if not _valid_job_id(value):
            raise ValueError
        return value
    except (UnicodeDecodeError, ValueError):
        raise InvalidBacktestResultHttpRequest() from None


def format_utc_milliseconds(value: int) -> str:
    """Format an in-range exact UTC epoch-millisecond integer as RFC 3339."""
    if type(value) is not int or not 0 <= value <= _MAX_UTC_MS:
        raise ValueError("timestamp must be UTC milliseconds") from None
    instant = datetime(1970, 1, 1, tzinfo=UTC) + timedelta(milliseconds=value)
    return (
        instant.strftime("%Y-%m-%dT%H:%M:%S.") + f"{instant.microsecond // 1000:03d}Z"
    )


def wire_decimal(value: Decimal) -> str:
    """Render only a finite exact Decimal through the canonical money-text owner."""
    if type(value) is not Decimal or not value.is_finite():
        raise ValueError("value must be a finite exact Decimal") from None
    return canonical_decimal_text(value)


def _valid_job_id(value: object) -> bool:
    return (
        type(value) is str
        and bool(value)
        and bool(value.strip())
        and value not in (".", "..")
        and "/" not in value
        and "\\" not in value
        and not any(unicodedata.category(char) == "Cc" for char in value)
    )


def _valid_digest(value: object) -> bool:
    return type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value) is not None


def _valid_utc_ms(value: object) -> bool:
    return type(value) is int and 0 <= value <= _MAX_UTC_MS


def _cursor_fields(payload: ResultCursor) -> dict[str, str | int]:
    if type(payload) is IndexCursor:
        if not _valid_utc_ms(payload.completed_at) or not _valid_job_id(payload.job_id):
            raise ValueError
        return {
            "route": "index",
            "completed_at": payload.completed_at,
            "job_id": payload.job_id,
        }
    if type(payload) is TradesCursor:
        if (
            not _valid_job_id(payload.job_id)
            or not _valid_digest(payload.result_digest)
            or type(payload.sequence) is not int
            or payload.sequence < 0
        ):
            raise ValueError
        return {
            "route": "trades",
            "job_id": payload.job_id,
            "result_digest": payload.result_digest,
            "sequence": payload.sequence,
        }
    if type(payload) is CandlesCursor:
        if (
            not _valid_job_id(payload.job_id)
            or not _valid_digest(payload.result_digest)
            or not _valid_utc_ms(payload.start)
            or not _valid_utc_ms(payload.end)
            or payload.start >= payload.end
            or not _valid_utc_ms(payload.timestamp)
            or not payload.start <= payload.timestamp < payload.end
        ):
            raise ValueError
        return {
            "route": "candles",
            "job_id": payload.job_id,
            "result_digest": payload.result_digest,
            "start": payload.start,
            "end": payload.end,
            "timestamp": payload.timestamp,
        }
    raise ValueError


def _canonical_cursor_json(fields: dict[str, str | int]) -> bytes:
    return json.dumps(
        fields,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def encode_result_cursor(key: bytes, payload: ResultCursor) -> str:
    """Sign one closed route-specific cursor payload with an explicit 32-byte key."""
    try:
        if type(key) is not bytes or len(key) != 32:
            raise ValueError
        canonical = _canonical_cursor_json(_cursor_fields(payload))
        encoded = base64.urlsafe_b64encode(canonical).rstrip(b"=").decode("ascii")
        signature = hmac.new(key, encoded.encode("ascii"), hashlib.sha256).hexdigest()
        return f"{encoded}.{signature}"
    except (TypeError, ValueError, UnicodeError):
        raise InvalidBacktestResultHttpRequest() from None


def _cursor_from_fields(fields: object) -> ResultCursor:
    if type(fields) is not dict or type(fields.get("route")) is not str:
        raise ValueError
    route = fields["route"]
    if route == "index" and fields.keys() == {"route", "completed_at", "job_id"}:
        payload: ResultCursor = IndexCursor(fields["completed_at"], fields["job_id"])
    elif route == "trades" and fields.keys() == {
        "route",
        "job_id",
        "result_digest",
        "sequence",
    }:
        payload = TradesCursor(
            fields["job_id"], fields["result_digest"], fields["sequence"]
        )
    elif route == "candles" and fields.keys() == {
        "route",
        "job_id",
        "result_digest",
        "start",
        "end",
        "timestamp",
    }:
        payload = CandlesCursor(
            fields["job_id"],
            fields["result_digest"],
            fields["start"],
            fields["end"],
            fields["timestamp"],
        )
    else:
        raise ValueError
    _cursor_fields(payload)
    return payload


def _cursor_matches_binding(
    payload: ResultCursor, binding: ResultCursorBinding
) -> bool:
    if type(payload) is IndexCursor:
        return type(binding) is IndexCursorBinding
    if type(payload) is TradesCursor:
        return (
            type(binding) is TradesCursorBinding
            and _valid_job_id(binding.job_id)
            and _valid_digest(binding.result_digest)
            and (
                binding.job_id,
                binding.result_digest,
            )
            == (payload.job_id, payload.result_digest)
        )
    return (
        type(payload) is CandlesCursor
        and type(binding) is CandlesCursorBinding
        and _valid_job_id(binding.job_id)
        and _valid_digest(binding.result_digest)
        and _valid_utc_ms(binding.start)
        and _valid_utc_ms(binding.end)
        and binding.start < binding.end
        and (binding.job_id, binding.result_digest, binding.start, binding.end)
        == (payload.job_id, payload.result_digest, payload.start, payload.end)
    )


def verify_result_cursor(
    key: bytes, token: str, binding: ResultCursorBinding
) -> ResultCursor:
    """Authenticate canonical cursor JSON and enforce its route/filter binding."""
    try:
        if type(key) is not bytes or len(key) != 32 or type(token) is not str:
            raise ValueError
        if type(binding) not in (
            IndexCursorBinding,
            TradesCursorBinding,
            CandlesCursorBinding,
        ):
            raise ValueError
        encoded, separator, supplied_signature = token.partition(".")
        if (
            not separator
            or "." in supplied_signature
            or re.fullmatch(r"[A-Za-z0-9_-]+", encoded) is None
            or re.fullmatch(r"[0-9a-f]{64}", supplied_signature) is None
        ):
            raise ValueError
        expected_signature = hmac.new(
            key, encoded.encode("ascii"), hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(expected_signature, supplied_signature):
            raise ValueError
        padded = encoded + "=" * (-len(encoded) % 4)
        canonical = base64.urlsafe_b64decode(padded.encode("ascii"))
        if base64.urlsafe_b64encode(canonical).rstrip(b"=").decode("ascii") != encoded:
            raise ValueError
        fields = json.loads(canonical.decode("utf-8", "strict"))
        payload = _cursor_from_fields(fields)
        if _canonical_cursor_json(_cursor_fields(payload)) != canonical:
            raise ValueError
        if not _cursor_matches_binding(payload, binding):
            raise ValueError
        return payload
    except (TypeError, ValueError, UnicodeError):
        raise InvalidBacktestResultHttpRequest() from None
