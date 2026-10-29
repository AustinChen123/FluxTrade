"""Offline tests for the P3 provenance-aware comparator."""

from __future__ import annotations

import json
from pathlib import Path

from tools.spider_run_compare import compare


def _write_run(directory: Path, *, attempt: dict | None = None, completion: dict | None = None,
               journal: list[dict] | None = None) -> None:
    directory.mkdir(parents=True)
    (directory / "attempt.json").write_text(json.dumps(attempt or {
        "program_sha256": "a" * 64,
        "native_artifact_sha256": "b" * 64,
        "input_contract_hashes": [
            {"name": "SCENARIO_PLAN", "sha256": "c" * 64},
            {"name": "PROGRAM", "sha256": "a" * 64},
            {"name": "NATIVE_ARTIFACT", "sha256": "b" * 64},
            {"name": "POLICY_SOURCE_MANIFEST", "sha256": "d" * 64},
        ],
        "account_key": {"account": "x"},
    }, sort_keys=True) + "\n")
    (directory / "completion.json").write_text(json.dumps(completion or {
        "input_contract_hashes": [
            {"name": "SCENARIO_PLAN", "sha256": "c" * 64},
            {"name": "PROGRAM", "sha256": "a" * 64},
            {"name": "NATIVE_ARTIFACT", "sha256": "b" * 64},
            {"name": "POLICY_SOURCE_MANIFEST", "sha256": "d" * 64},
        ],
        "artifacts": [{"path": "attempt.json", "sha256": "e" * 64, "byte_count": 10}],
        "endpoint_state_digest": "f" * 64,
    }, sort_keys=True) + "\n")
    for name in ("status.json", "endpoint.json", "reconciliation.json", "historical_input.json"):
        (directory / name).write_text("{}\n")
    (directory / "report.jsonl").write_text("")
    (directory / "journal.jsonl").write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in (journal or []))
    )


def test_comparator_accepts_identical_runs(tmp_path: Path) -> None:
    _write_run(tmp_path / "left", journal=[{"journal_seq": 1, "payload": {"value": "same"}}])
    _write_run(tmp_path / "right", journal=[{"journal_seq": 1, "payload": {"value": "same"}}])
    assert compare(tmp_path / "left", tmp_path / "right")["equal"] is True


def test_comparator_accepts_only_allowlisted_provenance_differences(tmp_path: Path) -> None:
    left_attempt = {
        "program_sha256": "a" * 64, "native_artifact_sha256": "b" * 64,
        "input_contract_hashes": [
            {"name": "plan", "sha256": "c" * 64},
            {"name": "program", "sha256": "a" * 64},
            {"name": "native", "sha256": "b" * 64},
        ],
    }
    right_attempt = {
        "program_sha256": "1" * 64, "native_artifact_sha256": "2" * 64,
        "input_contract_hashes": [
            {"name": "plan", "sha256": "c" * 64},
            {"name": "program", "sha256": "1" * 64},
            {"name": "native", "sha256": "2" * 64},
        ],
    }
    left_completion = {
        "input_contract_hashes": [
            {"name": "plan", "sha256": "c" * 64},
            {"name": "program", "sha256": "a" * 64},
            {"name": "native", "sha256": "b" * 64},
        ],
        "artifacts": [{"path": "attempt.json", "sha256": "e" * 64, "byte_count": 10}],
    }
    right_completion = {
        "input_contract_hashes": [
            {"name": "plan", "sha256": "c" * 64},
            {"name": "program", "sha256": "1" * 64},
            {"name": "native", "sha256": "2" * 64},
        ],
        "artifacts": [{"path": "attempt.json", "sha256": "3" * 64, "byte_count": 10}],
    }
    _write_run(tmp_path / "left", attempt=left_attempt, completion=left_completion)
    _write_run(tmp_path / "right", attempt=right_attempt, completion=right_completion)
    assert compare(tmp_path / "left", tmp_path / "right")["equal"] is True


def test_comparator_rejects_changed_payload(tmp_path: Path) -> None:
    _write_run(tmp_path / "left", journal=[{"journal_seq": 1, "payload": {"value": "before"}}])
    _write_run(tmp_path / "right", journal=[{"journal_seq": 1, "payload": {"value": "after"}}])
    result = compare(tmp_path / "left", tmp_path / "right")
    assert not result["equal"] and "payload/value" in result["difference"]


def test_comparator_rejects_reordered_journal_rows(tmp_path: Path) -> None:
    _write_run(tmp_path / "left", journal=[{"journal_seq": 1}, {"journal_seq": 2}])
    _write_run(tmp_path / "right", journal=[{"journal_seq": 2}, {"journal_seq": 1}])
    result = compare(tmp_path / "left", tmp_path / "right")
    assert not result["equal"] and "journal.jsonl$/0/journal_seq" in result["difference"]


def test_comparator_rejects_missing_journal_row(tmp_path: Path) -> None:
    _write_run(tmp_path / "left", journal=[{"journal_seq": 1}, {"journal_seq": 2}])
    _write_run(tmp_path / "right", journal=[{"journal_seq": 1}])
    result = compare(tmp_path / "left", tmp_path / "right")
    assert not result["equal"] and "list length 2 != 1" in result["difference"]


def test_comparator_rejects_unknown_field_difference(tmp_path: Path) -> None:
    _write_run(tmp_path / "left", journal=[{"journal_seq": 1, "payload": {"value": "same"}}])
    _write_run(tmp_path / "right", journal=[{"journal_seq": 1, "payload": {"value": "same", "new": 1}}])
    result = compare(tmp_path / "left", tmp_path / "right")
    assert not result["equal"] and "keys differ" in result["difference"]


def test_comparator_rejects_extra_artifact_file(tmp_path: Path) -> None:
    _write_run(tmp_path / "left")
    _write_run(tmp_path / "right")
    (tmp_path / "right" / "unknown.bin").write_bytes(b"unexpected")
    result = compare(tmp_path / "left", tmp_path / "right")
    assert not result["equal"] and "artifact set differs" in result["difference"]
