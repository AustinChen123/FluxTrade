"""Bound and file-race matrix for the J3 owned-buffer reader."""

import hashlib
import os
from typing import cast

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


def test_wire_line_cap_is_inclusive_and_plus_one_refuses():
    prefix = b'{"payload":{"pad":"'
    suffix = b'"},"record_kind":"CALLBACK_RESULT","schema_version":"spider_journal_record_v2"}\n'
    exact = prefix + b"x" * (limits.P3_V2_ROW_MAX - len(prefix) - len(suffix)) + suffix
    over = (
        prefix + b"x" * (limits.P3_V2_ROW_MAX + 1 - len(prefix) - len(suffix)) + suffix
    )
    assert len(exact) == limits.P3_V2_ROW_MAX
    assert len(over) == limits.P3_V2_ROW_MAX + 1
    assert list(reader._iter_owned_lines(bytearray(exact))) == [exact[:-1]]
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        list(reader._iter_owned_lines(bytearray(over)))


def test_v1_decoder_keeps_accepting_rows_over_v2_row_cap():
    row = {"payload": {"pad": "x" * limits.P3_V2_ROW_MAX}}
    raw = canonical_bytes(row) + b"\n"
    assert len(raw) > limits.P3_V2_ROW_MAX
    assert decode_jsonl(raw) == [row]


def test_per_kind_row_and_wire_byte_budget_boundaries_are_exact(tmp_path):
    raw = _wire()
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    projected = (
        len(
            canonical_bytes(
                {
                    "schema_version": "spider_journal_record_v1",
                    "record_kind": "CALLBACK_RESULT",
                    "payload": {"result": "ok"},
                    "configuration_context": CONTEXTS[0],
                    "historical_context": CONTEXTS[1],
                }
            )
        )
        + 1
    )
    wire_bytes = len(raw)
    assert projected > wire_bytes
    budget = _budget(raw)
    caps = cast(dict[str, int], dict(cast(dict[str, object], budget["kind_budgets"])))
    caps["CALLBACK_RESULT"] = wire_bytes
    row_caps = cast(dict[str, int], dict(cast(dict[str, object], budget["row_counts"])))
    row_caps["CALLBACK_RESULT"] = 1
    budget.update(kind_budgets=caps, row_counts=row_caps)
    result = _read(path, raw, budget)
    assert len(result.rows) == 1
    assert result.counters["kind_bytes"]["CALLBACK_RESULT"] == wire_bytes
    caps["CALLBACK_RESULT"] -= 1
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        _read(path, raw, budget)
    caps["CALLBACK_RESULT"] = wire_bytes
    row_caps["CALLBACK_RESULT"] = 0
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        _read(path, raw, budget)


def test_definition_table_count_bytes_and_definition_size_boundaries(tmp_path):
    definition = {"order_id": "a"}
    payload = {"request": {"working_orders": [0]}, "working_orders_snapshot": [0]}
    raw = _wire(payload=payload, kind="HISTORICAL_MARKET_RESULT", defs=[definition])
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    budget = _budget(raw, table_max=1, table_bytes_max=len(canonical_bytes(definition)))
    assert len(_read(path, raw, budget).rows) == 1
    budget["table_max"] = 0
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        _read(path, raw, budget)
    budget["table_max"] = 1
    budget["table_bytes_max"] = cast(int, budget["table_bytes_max"]) - 1
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        _read(path, raw, budget)


def test_per_definition_limit_is_inclusive_and_plus_one_refuses(tmp_path):
    def row_with_definition(size: int) -> bytes:
        base = {"x": ""}
        padding = size - len(canonical_bytes(base))
        definition = {"x": "a" * padding}
        assert len(canonical_bytes(definition)) == size
        return _wire(
            payload={
                "request": {"working_orders": [0]},
                "working_orders_snapshot": [0],
            },
            kind="HISTORICAL_MARKET_RESULT",
            defs=[definition],
        )

    exact = row_with_definition(limits.P3_V2_MAX_DEFINITION_BYTES)
    path = tmp_path / "journal.jsonl"
    path.write_bytes(exact)
    assert len(_read(path, exact).rows) == 1
    over = row_with_definition(limits.P3_V2_MAX_DEFINITION_BYTES + 1)
    path.write_bytes(over)
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        _read(path, over)


def test_source_group_remainder_budget_is_enforced_inclusive(tmp_path):
    empty_evidence = {"open_orders_fact": {"immutable_payload": {"rows": []}}}
    raw = _wire(
        payload={
            "owner_evidence_before": empty_evidence,
            "owner_evidence_after": empty_evidence,
        },
        kind="SOURCE_GROUP_RESULT",
    )
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    projection = {
        "schema_version": "spider_journal_record_v1",
        "record_kind": "SOURCE_GROUP_RESULT",
        "payload": {
            "owner_evidence_before": empty_evidence,
            "owner_evidence_after": empty_evidence,
        },
        "configuration_context": CONTEXTS[0],
        "historical_context": CONTEXTS[1],
    }
    wire_bytes = len(raw)
    projected_bytes = len(canonical_bytes(projection)) + 1
    assert projected_bytes > wire_bytes
    budget = _budget(raw, B_SG=wire_bytes)
    result = _read(path, raw, budget)
    assert len(result.rows) == 1
    assert result.counters["kind_bytes"]["SOURCE_GROUP_RESULT"] == wire_bytes
    budget["B_SG"] = cast(int, budget["B_SG"]) - 1
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        _read(path, raw, budget)
