"""Bounded single-descriptor capture for a P3-v2 journal."""

import hashlib
import os
from dataclasses import dataclass
from collections.abc import Mapping
from typing import NoReturn

from src.core.backtest import spider_artifact_limits as limits

_V2_SCHEMA = "spider_journal_record_v2"


@dataclass(frozen=True, slots=True)
class OwnedP3V2Buffer:
    data: bytearray
    wire_metadata: dict[str, int | str]


def _fail(code: str) -> NoReturn:
    raise ValueError(code)


def capture_owned_p3_v2_buffer(
    path: str | os.PathLike[str],
    *,
    expected_metadata: Mapping[str, object],
    journal_cap: int,
) -> OwnedP3V2Buffer:
    """Read at most cap+1 bytes from one opened file and verify its manifest."""
    if (
        type(journal_cap) is not int
        or journal_cap < 0
        or journal_cap > limits.P3_V2_JOURNAL_CAP
    ):
        _fail("INVALID_ARTIFACT")
    if expected_metadata.get("schema_version") != _V2_SCHEMA:
        _fail("ARTIFACT_MISMATCH")
    fd = os.open(path, os.O_RDONLY)
    try:
        if os.fstat(fd).st_size > journal_cap:
            _fail("ARTIFACT_LIMIT_EXCEEDED")
        data = bytearray()
        while True:
            remaining = journal_cap + 1 - len(data)
            if remaining <= 0:
                _fail("ARTIFACT_LIMIT_EXCEEDED")
            chunk = os.read(fd, min(limits.P3_V2_ROW_MAX, remaining))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > journal_cap:
                _fail("ARTIFACT_LIMIT_EXCEEDED")
    finally:
        os.close(fd)
    metadata: dict[str, int | str] = {
        "schema_version": _V2_SCHEMA,
        "sha256": hashlib.sha256(data).hexdigest(),
        "byte_count": len(data),
        "row_count": data.count(b"\n"),
    }
    if any(expected_metadata.get(key) != value for key, value in metadata.items()):
        _fail("ARTIFACT_MISMATCH")
    return OwnedP3V2Buffer(data, metadata)
