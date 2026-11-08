"""Bound and file-race matrix for the J3 owned-buffer reader."""

import hashlib
import os

import pytest

from src.core.backtest import spider_artifact_limits as limits
from src.core.backtest import spider_journal_reader_v2 as reader
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_jsonl

CONTEXTS = ({}, {})


def _wire(*, payload: object | None = None, kind: str = "CALLBACK_RESULT", defs=None):
    row: dict[str, object] = {
        "schema_version": "spider_journal_record_v2",
        "record_kind": kind,
        "payload": {"result": "ok"} if payload is None else payload,
    }
    if defs is not None:
        row["order_defs"] = list(defs)
    return canonical_bytes(row) + b"\n"


def _metadata(raw: bytes) -> dict[str, object]:
    return {
        "schema_version": "spider_journal_record_v2",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
        "row_count": raw.count(b"\n"),
    }


def _budget(raw: bytes, **overrides) -> dict[str, object]:
    result: dict[str, object] = {
        "J2": len(raw),
        "kind_budgets": {
            kind: limits.P3_V2_JOURNAL_CAP for kind in limits.P3_V2_RECORD_KINDS
        },
        "row_counts": {kind: 100 for kind in limits.P3_V2_RECORD_KINDS},
        "B_SG": limits.P3_V2_JOURNAL_CAP,
        "table_max": 100,
        "table_bytes_max": 100_000,
    }
    result.update(overrides)
    return result


def _read(path, raw: bytes, budget=None, expected=None):
    return reader.read_p3_v2_journal(
        path,
        expected_metadata=_metadata(raw) if expected is None else expected,
        contexts=CONTEXTS,
        budget=_budget(raw) if budget is None else budget,
    )


def test_partial_last_row_at_each_byte_is_rejected(tmp_path):
    first, last = _wire(), _wire(payload={"result": "last"})
    raw = first + last
    path = tmp_path / "journal.jsonl"
    for offset in range(1, len(last)):
        partial = raw[: len(first) + offset]
        path.write_bytes(partial)
        with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
            _read(path, partial)


def test_j2_byte_cap_is_inclusive_and_plus_one_refuses_before_read(
    monkeypatch, tmp_path
):
    raw = _wire()
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    assert len(_read(path, raw, _budget(raw, J2=len(raw))).rows) == 1
    reads = []
    original = os.read

    def counted(fd, size):
        reads.append(size)
        return original(fd, size)

    monkeypatch.setattr(reader.os, "read", counted)
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        _read(path, raw, _budget(raw, J2=len(raw) - 1))
    assert reads == []


def test_formal_global_j2_cap_is_inclusive(tmp_path):
    raw = b""
    path = tmp_path / "empty.jsonl"
    path.write_bytes(raw)
    budget = _budget(raw, J2=limits.P3_V2_JOURNAL_CAP)
    result = _read(path, raw, budget)
    assert result.wire_metadata == _metadata(raw)
    assert result.counters["rows"] == 0


def test_file_growth_after_fstat_is_bounded_at_cap_plus_one(monkeypatch, tmp_path):
    raw = _wire()
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    original_fstat = os.fstat
    original_read = os.read
    changed = False
    read_bytes = 0

    def grow(fd):
        nonlocal changed
        result = original_fstat(fd)
        if not changed:
            changed = True
            with path.open("ab") as stream:
                stream.write(b"x" * 64)
        return result

    def counted_read(fd, size):
        nonlocal read_bytes
        chunk = original_read(fd, size)
        read_bytes += len(chunk)
        return chunk

    monkeypatch.setattr(reader.os, "fstat", grow)
    monkeypatch.setattr(reader.os, "read", counted_read)
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        _read(path, raw, _budget(raw, J2=len(raw)))
    assert read_bytes == len(raw) + 1


def test_file_shrink_after_fstat_fails_manifest_and_replacement_reads_open_inode(
    monkeypatch, tmp_path
):
    raw = _wire()
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    original_fstat = os.fstat
    changed = False

    def shrink(fd):
        nonlocal changed
        result = original_fstat(fd)
        if not changed:
            changed = True
            path.write_bytes(b"")
        return result

    monkeypatch.setattr(reader.os, "fstat", shrink)
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        _read(path, raw)

    monkeypatch.undo()
    path.write_bytes(raw)
    replacement = tmp_path / "replacement.jsonl"
    original_fstat = os.fstat
    changed = False

    def replace(fd):
        nonlocal changed
        result = original_fstat(fd)
        if not changed:
            changed = True
            path.rename(replacement)
            path.write_bytes(_wire(payload={"result": "new inode"}))
        return result

    monkeypatch.setattr(reader.os, "fstat", replace)
    assert len(_read(path, raw).rows) == 1


def test_growth_after_eof_does_not_change_owned_buffer(monkeypatch, tmp_path):
    raw = _wire()
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    original_read = os.read
    appended = False

    def append_after_eof(fd, size):
        nonlocal appended
        chunk = original_read(fd, size)
        if not chunk and not appended:
            appended = True
            with path.open("ab") as stream:
                stream.write(b"later")
        return chunk

    monkeypatch.setattr(reader.os, "read", append_after_eof)
    result = _read(path, raw)
    assert result.wire_metadata == _metadata(raw)
    assert path.read_bytes() == raw + b"later"
