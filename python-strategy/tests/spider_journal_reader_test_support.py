# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""Fixtures shared by J5 P3-v2 context-reader regressions."""

import hashlib

from src.core.backtest import spider_artifact_limits as limits
from src.core.backtest import spider_journal_codec_v2 as codec
from src.core.backtest.spider_run_artifacts import canonical_bytes, encode_artifact
from src.core.backtest.spider_run_envelope_schema import journal_record
from src.core.backtest.spider_run_evidence import _context_chain
from test_spider_run_artifacts import historical_attempt
from test_spider_run_envelope_schema import record


def valid_inputs():
    attempt = historical_attempt()
    attempt_bytes = encode_artifact(attempt)
    row = record("CALLBACK_RESULT")
    row["configuration_context"] = attempt["configuration_context"]
    row["historical_context"] = attempt["historical_context"]
    encoded, _ = codec.encode_p3_v2_row_with_context_gate(
        row,
        validate_v1=journal_record,
        check_context_chain=lambda candidate: _context_chain(attempt, candidate),
    )
    raw = canonical_bytes(encoded) + b"\n"
    metadata = {
        "schema_version": "spider_journal_record_v2",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
        "row_count": 1,
    }
    budget = {
        "J2": len(raw),
        "kind_budgets": {
            kind: limits.P3_V2_JOURNAL_CAP for kind in limits.P3_V2_RECORD_KINDS
        },
        "row_counts": {kind: 100 for kind in limits.P3_V2_RECORD_KINDS},
        "B_SG": limits.P3_V2_JOURNAL_CAP,
        "table_max": 100,
        "table_bytes_max": 100_000,
    }
    return attempt, attempt_bytes, row, encoded, raw, metadata, budget
