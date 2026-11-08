"""Minimal end-to-end acceptance test for the P3-v2 reader entry point."""

import hashlib

from src.core.backtest import spider_artifact_limits as limits
from src.core.backtest import spider_journal_reader_v2 as reader
from src.core.backtest.spider_run_artifacts import canonical_bytes


def test_reader_entry_materializes_one_callback_row(tmp_path):
    row = {
        "schema_version": "spider_journal_record_v2",
        "record_kind": "CALLBACK_RESULT",
        "payload": {"result": "ok"},
    }
    raw = canonical_bytes(row) + b"\n"
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    result = reader.read_p3_v2_journal(
        path,
        expected_metadata={
            "schema_version": "spider_journal_record_v2",
            "sha256": hashlib.sha256(raw).hexdigest(),
            "byte_count": len(raw),
            "row_count": 1,
        },
        contexts=({}, {}),
        budget=limits.p3_v2_scope_budget({"HISTORICAL_MARKET_STEP": 1}),
    )
    assert result.rows[0]["schema_version"] == "spider_journal_record_v1"
    assert result.counters["kind_bytes"]["CALLBACK_RESULT"] == len(raw)
