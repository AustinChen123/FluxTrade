"""Bounded single-descriptor capture for a P3-v2 journal."""

import hashlib
import os
import stat
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


def _check_capture_inputs(
    expected_metadata: Mapping[str, object], journal_cap: int,
) -> None:
    if (
        type(journal_cap) is not int
        or journal_cap < 0
        or journal_cap > limits.P3_V2_JOURNAL_CAP
    ):
        _fail("INVALID_ARTIFACT")
    if expected_metadata.get("schema_version") != _V2_SCHEMA:
        _fail("ARTIFACT_MISMATCH")


def _capture_from_fd(
    fd: int, *, expected_metadata: Mapping[str, object], journal_cap: int,
    require_regular: bool,
) -> OwnedP3V2Buffer:
    try:
        info = os.fstat(fd)
        if require_regular and not stat.S_ISREG(info.st_mode):
            _fail("ARTIFACT_MISMATCH")
        if info.st_size > journal_cap:
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


def capture_owned_p3_v2_buffer(
    path: str | os.PathLike[str],
    *,
    expected_metadata: Mapping[str, object],
    journal_cap: int,
) -> OwnedP3V2Buffer:
    """Read at most cap+1 bytes from one opened file and verify its manifest."""
    _check_capture_inputs(expected_metadata, journal_cap)
    fd = os.open(path, os.O_RDONLY)
    return _capture_from_fd(
        fd, expected_metadata=expected_metadata, journal_cap=journal_cap, require_regular=False,
    )


def capture_owned_p3_v2_buffer_at(
    directory_fd: int,
    name: str,
    *,
    expected_metadata: Mapping[str, object],
    journal_cap: int,
) -> OwnedP3V2Buffer:
    """Capture a regular journal leaf relative to an already-open directory."""
    _check_capture_inputs(expected_metadata, journal_cap)
    fd = os.open(
        name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=directory_fd,
    )
    return _capture_from_fd(
        fd, expected_metadata=expected_metadata, journal_cap=journal_cap, require_regular=True,
    )
