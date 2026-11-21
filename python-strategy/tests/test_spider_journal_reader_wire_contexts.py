# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""D5.1 rejects attempt contexts from v2 wire rows."""

import hashlib

import pytest

from src.core.backtest import spider_journal_reader_v2 as reader
from src.core.backtest.spider_run_artifacts import canonical_bytes
from spider_journal_reader_test_support import valid_inputs


@pytest.mark.parametrize(
    "keys",
    [
        ("configuration_context",),
        ("historical_context",),
        ("configuration_context", "historical_context"),
    ],
)
def test_reader_rejects_context_members_on_v2_wire_rows(tmp_path, monkeypatch, keys):
    attempt, _, _, encoded, _, metadata, budget = valid_inputs()
    wire_row = dict(encoded)
    for key in keys:
        wire_row[key] = None
    raw = canonical_bytes(wire_row) + b"\n"
    path = tmp_path / "noncanonical-context.jsonl"
    path.write_bytes(raw)
    metadata = {
        **metadata,
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
    }
    projection_calls = []

    def projection_must_not_run(*args, **kwargs):
        projection_calls.append((args, kwargs))
        raise AssertionError("non-canonical row reached projection")

    monkeypatch.setattr(reader, "_projected_len", projection_must_not_run)

    with pytest.raises(ValueError, match="^ARTIFACT_MISMATCH$"):
        reader.read_p3_v2_journal(
            path,
            expected_metadata=metadata,
            contexts=(
                attempt["configuration_context"],
                attempt["historical_context"],
            ),
            budget={**budget, "J2": len(raw)},
        )
    assert projection_calls == []
