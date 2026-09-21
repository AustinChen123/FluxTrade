"""Unwired LIVE query/error contract; no success encoding, route, auth or I/O."""

from dataclasses import dataclass
import re
from urllib.parse import unquote_to_bytes

from src.core.market_data.profiles.live_query import (
    LiveProfileQueryUnavailable,
    ProfileQueryError,
)
from src.core.market_data.profiles.read_types import ProfileQueryRequest

_REQUIRED = frozenset(
    (
        "product_id",
        "base_grid_id",
        "output_grid_id",
        "algorithm_version",
        "start_ms",
        "end_ms",
        "purpose",
        "freshness_policy_id",
    )
)
_ERRORS = {
    "INVALID_REQUEST": 400,
    "PROFILE_NOT_READY": 404,
    "PROFILE_EXPIRED": 404,
    "SNAPSHOT_REVOKED": 409,
    "QUERY_TOO_LARGE": 400,
    "BACKEND_UNAVAILABLE": 503,
}


class InvalidProfileHttpRequest(ValueError):
    def __init__(self) -> None:
        super().__init__("INVALID_REQUEST")


@dataclass(frozen=True, slots=True)
class ProfileHttpError:
    status: int
    code: str

    def __post_init__(self) -> None:
        if (
            type(self.status) is not int
            or type(self.code) is not str
            or _ERRORS.get(self.code) != self.status
        ):
            raise ValueError("invalid profile HTTP error") from None


def _decode(value: bytes) -> str:
    if re.search(rb"%(?![0-9A-Fa-f]{2})", value):
        raise ValueError
    decoded = unquote_to_bytes(value.replace(b"+", b" ")).decode(
        "utf-8", errors="strict"
    )
    if not decoded or not decoded.strip() or "\x00" in decoded:
        raise ValueError
    return decoded


def _integer(value: str) -> int:
    if len(value) > 19 or re.fullmatch(r"0|[1-9][0-9]*", value) is None:
        raise ValueError
    integer = int(value)
    if integer > (1 << 63) - 1:
        raise ValueError
    return integer


def parse_profile_query(raw_query: bytes) -> ProfileQueryRequest:
    """Parse exact raw bytes; request size limit is not a server body/deadline gate."""
    try:
        if type(raw_query) is not bytes or not raw_query or len(raw_query) > 4096:
            raise ValueError
        values: dict[str, str] = {}
        for pair in raw_query.split(b"&"):
            key_bytes, separator, value_bytes = pair.partition(b"=")
            if not separator:
                raise ValueError
            key, value = _decode(key_bytes), _decode(value_bytes)
            if key not in _REQUIRED | {"revision"} or key in values:
                raise ValueError
            values[key] = value
        if (
            not _REQUIRED <= values.keys()
            or values["purpose"] != "LIVE_QUERY"
            or values["freshness_policy_id"] != "utc_complete_strict_v1"
        ):
            raise ValueError
        return ProfileQueryRequest(
            product_id=values["product_id"],
            base_grid_id=values["base_grid_id"],
            output_grid_id=values["output_grid_id"],
            algorithm_version=values["algorithm_version"],
            start_ms=_integer(values["start_ms"]),
            end_ms=_integer(values["end_ms"]),
            purpose="LIVE_QUERY",
            freshness_policy_id=values["freshness_policy_id"],
            revision=_integer(values["revision"]) if "revision" in values else None,
        )
    except ValueError:
        raise InvalidProfileHttpRequest() from None


def profile_http_error(
    value: LiveProfileQueryUnavailable | Exception,
) -> ProfileHttpError:
    """Closed public projection: never serialize exception messages or private details."""
    if (
        not isinstance(value, Exception)
        and type(value) is not LiveProfileQueryUnavailable
    ):
        raise TypeError("invalid profile HTTP mapping input") from None
    code = "BACKEND_UNAVAILABLE"
    if type(value) is InvalidProfileHttpRequest or (
        type(value) is ProfileQueryError and value.reason == "INVALID"
    ):
        code = "INVALID_REQUEST"
    elif type(value) is LiveProfileQueryUnavailable:
        code = {
            "NOT_READY": "PROFILE_NOT_READY",
            "PROFILE_EXPIRED": "PROFILE_EXPIRED",
            "SNAPSHOT_REVOKED": "SNAPSHOT_REVOKED",
            "QUERY_TOO_LARGE": "QUERY_TOO_LARGE",
        }.get(value.reason, code)
    return ProfileHttpError(_ERRORS[code], code)
