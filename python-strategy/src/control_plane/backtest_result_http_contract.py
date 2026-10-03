"""Pure request and wire rules for the backtest-results HTTP projections."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
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
        if value in (".", "..") or "/" in value or "\\" in value:
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
