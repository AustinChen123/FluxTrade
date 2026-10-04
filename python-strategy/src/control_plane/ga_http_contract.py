"""Closed HTTP grammar and detached projections for GA read endpoints."""

from __future__ import annotations

from dataclasses import dataclass
import re
from typing import Any
import unicodedata
from urllib.parse import unquote_to_bytes

from src.control_plane.backtest_result_http_contract import (
    InvalidBacktestResultHttpRequest,
    parse_job_id,
)
from src.control_plane.ga_lifecycle import GaJobRecord

_JOBS_PATH = "/api/v1/ga-jobs"
_PROFILES_PATH = "/api/v1/ga-profiles"
GA_PROFILE_ID = "golden_cross_research_v1"
_MAX_OFFSET = 2_147_483_647


class GaHttpRequestError(ValueError):
    """Fixed public validation error without request details."""


@dataclass(frozen=True, slots=True)
class GaReadRequest:
    route: str
    identity: str | None = None
    limit: int = 50
    offset: int = 0


def is_ga_read_path(path: str) -> bool:
    return any(
        path == base or path.startswith(base + "/")
        for base in (_JOBS_PATH, _PROFILES_PATH)
    )


def parse_ga_read_request(
    method: str,
    path: str,
    raw_query: str,
    body: str | bytes | None,
    *,
    raw_target: str,
) -> GaReadRequest | None:
    """Return a recognized GET request; ``None`` means an unsupported route."""
    if any(character in raw_target for character in "\t\r\n"):
        raise GaHttpRequestError from None
    if method != "GET":
        return None
    if path == _JOBS_PATH:
        route = "index"
        identity = None
    elif path.startswith(_JOBS_PATH + "/"):
        raw_identity = path[len(_JOBS_PATH) + 1 :]
        if not raw_identity or "/" in raw_identity:
            return None
        route = "detail"
        identity = _identity(raw_identity)
    elif path.startswith(_PROFILES_PATH + "/"):
        raw_identity = path[len(_PROFILES_PATH) + 1 :]
        if not raw_identity or "/" in raw_identity:
            return None
        route = "profile"
        identity = _identity(raw_identity)
    else:
        return None

    if body is not None and not (
        type(body) is str and body == "" or type(body) is bytes and body == b""
    ):
        raise GaHttpRequestError from None
    try:
        query_bytes = raw_query.encode("ascii", errors="strict")
    except UnicodeEncodeError:
        raise GaHttpRequestError from None

    if route != "index":
        if query_bytes:
            raise GaHttpRequestError from None
        return GaReadRequest(route=route, identity=identity)
    limit, offset = _parse_index_query(query_bytes)
    return GaReadRequest(route=route, limit=limit, offset=offset)


def project_ga_job(record: GaJobRecord) -> dict[str, Any]:
    checkpoint = record.checkpoint
    error = record.error
    return {
        "id": record.id,
        "kind": "ga",
        "status": record.status.value,
        "version": record.version,
        "epoch_id": record.epoch_id,
        "completed_generation": record.completed_generation,
        "checkpoint": (
            None
            if checkpoint is None
            else {
                "epoch_id": checkpoint.epoch_id,
                "completed_generation": checkpoint.completed_generation,
            }
        ),
        "retry_of_job_id": record.retry_of_job_id,
        "request": record.request,
        "error": (
            None
            if error is None
            else "control_plane_interrupted"
            if error == "control_plane_interrupted"
            else "ga_execution_failed"
        ),
    }


def _identity(raw: str) -> str:
    try:
        return parse_job_id(raw.encode("ascii", errors="strict"))
    except (InvalidBacktestResultHttpRequest, UnicodeEncodeError):
        raise GaHttpRequestError from None


def _decode_query_component(raw: bytes) -> str:
    if re.search(rb"%(?![0-9A-Fa-f]{2})", raw):
        raise ValueError
    raw = raw.replace(b"+", b" ")
    value = unquote_to_bytes(raw).decode("utf-8", "strict")
    if (
        not value
        or not value.strip()
        or any(unicodedata.category(char) == "Cc" for char in value)
    ):
        raise ValueError
    return value


def _parse_index_query(raw: bytes) -> tuple[int, int]:
    values: dict[str, str] = {}
    try:
        if raw:
            for pair in raw.split(b"&"):
                key_raw, separator, value_raw = pair.partition(b"=")
                if not separator:
                    raise ValueError
                key = _decode_query_component(key_raw)
                value = _decode_query_component(value_raw)
                if key not in {"limit", "offset"} or key in values:
                    raise ValueError
                values[key] = value
        limit = _query_integer(values.get("limit", "50"), maximum=100)
        offset = _query_integer(values.get("offset", "0"), maximum=_MAX_OFFSET)
        if limit < 1:
            raise ValueError
        return limit, offset
    except (UnicodeDecodeError, ValueError):
        raise GaHttpRequestError from None


def _query_integer(value: str, *, maximum: int) -> int:
    if re.fullmatch(r"[0-9]+", value, flags=re.ASCII) is None:
        raise ValueError
    significant = value.lstrip("0") or "0"
    if len(significant) > len(str(maximum)):
        raise ValueError
    parsed = int(significant)
    if parsed > maximum:
        raise ValueError
    return parsed
