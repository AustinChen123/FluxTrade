"""Owned-buffer lifetime and manifest tests independent of journal parsing."""

import hashlib
import os

import pytest

from src.core.backtest import spider_artifact_limits as limits
from src.core.backtest import spider_journal_buffer_v2 as buffer_reader


def _manifest(data: bytes) -> dict[str, object]:
    return {
        "schema_version": "spider_journal_record_v2",
        "sha256": hashlib.sha256(data).hexdigest(),
        "byte_count": len(data),
        "row_count": data.count(b"\n"),
    }


def test_capture_returns_owned_bytes_and_exact_wire_metadata(tmp_path):
    data = b"{}\n"
    path = tmp_path / "journal.jsonl"
    path.write_bytes(data)
    captured = buffer_reader.capture_owned_p3_v2_buffer(
        path, expected_metadata=_manifest(data), journal_cap=len(data)
    )
    assert captured.data == bytearray(data)
    assert captured.wire_metadata == _manifest(data)


@pytest.mark.parametrize(
    "field", ["schema_version", "sha256", "byte_count", "row_count"]
)
def test_capture_compares_each_manifest_field(tmp_path, field):
    data = b"{}\n"
    path = tmp_path / "journal.jsonl"
    path.write_bytes(data)
    manifest = _manifest(data)
    manifest[field] = "wrong" if field in {"schema_version", "sha256"} else 4
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        buffer_reader.capture_owned_p3_v2_buffer(
            path, expected_metadata=manifest, journal_cap=len(data)
        )


def test_capture_refuses_fstat_size_above_inclusive_cap_before_read(
    monkeypatch, tmp_path
):
    data = b"{}\n"
    path = tmp_path / "journal.jsonl"
    path.write_bytes(data)
    reads = []
    original = os.read

    def counted(fd, size):
        reads.append(size)
        return original(fd, size)

    monkeypatch.setattr(buffer_reader.os, "read", counted)
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        buffer_reader.capture_owned_p3_v2_buffer(
            path, expected_metadata=_manifest(data), journal_cap=len(data) - 1
        )
    assert reads == []


def test_capture_rejects_a_cap_above_the_policy_ceiling(tmp_path):
    data = b""
    path = tmp_path / "journal.jsonl"
    path.write_bytes(data)
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        buffer_reader.capture_owned_p3_v2_buffer(
            path,
            expected_metadata=_manifest(data),
            journal_cap=limits.P3_V2_JOURNAL_CAP + 1,
        )


def test_capture_detects_growth_after_fstat_and_closes_descriptor(
    monkeypatch, tmp_path
):
    data = b"{}\n"
    path = tmp_path / "journal.jsonl"
    path.write_bytes(data)
    original = os.fstat
    changed = False

    def grow(fd):
        nonlocal changed
        value = original(fd)
        if not changed:
            changed = True
            with path.open("ab") as output:
                output.write(b"x" * 16)
        return value

    monkeypatch.setattr(buffer_reader.os, "fstat", grow)
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        buffer_reader.capture_owned_p3_v2_buffer(
            path, expected_metadata=_manifest(data), journal_cap=len(data)
        )
