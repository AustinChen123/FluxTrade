"""T3.3 tests for the owned-buffer P3-v2 journal reader."""

import hashlib

import pytest

from src.core.backtest import spider_artifact_limits as limits
from src.core.backtest import spider_journal_reader_v2 as reader
from src.core.backtest.spider_run_envelope_schema import journal_record
from src.core.backtest.spider_run_artifacts import canonical_bytes
from test_spider_run_envelope_schema import record as valid_v1_record

CONTEXTS = (
    {"schema_version": "spider_configuration_context_v1", "config_id": "cfg"},
    {
        "schema_version": "spider_historical_context_v1",
        "historical_input_sha256": "0" * 64,
    },
)


def _wire_row(
    payload: object | None = None, kind: str = "CALLBACK_RESULT"
) -> dict[str, object]:
    return {
        "schema_version": "spider_journal_record_v2",
        "record_kind": kind,
        "payload": {"result": "ok"} if payload is None else payload,
    }


def _raw(*rows: dict[str, object]) -> bytes:
    return b"".join(canonical_bytes(row) + b"\n" for row in rows)


def _metadata(raw: bytes) -> dict[str, object]:
    return {
        "schema_version": "spider_journal_record_v2",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
        "row_count": raw.count(b"\n"),
    }


def _budget(raw: bytes, *, cap: int | None = None) -> dict[str, object]:
    return {
        "J2": len(raw) if cap is None else cap,
        "kind_budgets": {
            kind: limits.P3_V2_JOURNAL_CAP for kind in limits.P3_V2_RECORD_KINDS
        },
        "row_counts": {kind: 100 for kind in limits.P3_V2_RECORD_KINDS},
        "B_SG": limits.P3_V2_JOURNAL_CAP,
        "table_max": 100,
        "table_bytes_max": 100_000,
    }


def _read(
    path,
    raw: bytes,
    *,
    cap: int | None = None,
    budget: dict[str, object] | None = None,
):
    return reader.read_p3_v2_journal(
        path,
        expected_metadata=_metadata(raw),
        contexts=CONTEXTS,
        budget=_budget(raw, cap=cap) if budget is None else budget,
    )


def test_reader_returns_projection_metadata_and_budget_counters(tmp_path):
    raw = _raw(_wire_row())
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)

    result = _read(path, raw)

    assert result.wire_metadata == {
        "schema_version": "spider_journal_record_v2",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
        "row_count": 1,
    }
    assert result.counters == {
        "rows": 1,
        "projected_bytes": len(
            canonical_bytes(
                {
                    **_wire_row(),
                    "schema_version": "spider_journal_record_v1",
                    "configuration_context": CONTEXTS[0],
                    "historical_context": CONTEXTS[1],
                }
            )
        )
        + 1,
        "kind_bytes": {
            **{kind: 0 for kind in limits.P3_V2_RECORD_KINDS},
            "CALLBACK_RESULT": len(raw),
        },
        "kind_rows": {
            **{kind: 0 for kind in limits.P3_V2_RECORD_KINDS},
            "CALLBACK_RESULT": 1,
        },
        "definition_count": 0,
        "definition_bytes": 0,
    }
    assert result.rows[0]["schema_version"] == "spider_journal_record_v1"
    assert result.rows[0]["configuration_context"] == CONTEXTS[0]


def test_all_kind_counter_maps_are_pinned_by_exact_equality(tmp_path):
    callback = _wire_row()
    snapshot = _wire_row({"fact": {"snapshot_kind": "POSITIONS"}}, "SNAPSHOT_FACT")
    raw = _raw(callback, snapshot)
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    callback_v1 = {
        **callback,
        "schema_version": "spider_journal_record_v1",
        "configuration_context": CONTEXTS[0],
        "historical_context": CONTEXTS[1],
    }
    snapshot_v1 = {
        **snapshot,
        "schema_version": "spider_journal_record_v1",
        "configuration_context": CONTEXTS[0],
        "historical_context": CONTEXTS[1],
    }
    callback_size = len(canonical_bytes(callback_v1)) + 1
    snapshot_size = len(canonical_bytes(snapshot_v1)) + 1
    result = _read(path, raw)
    assert result.counters == {
        "rows": 2,
        "projected_bytes": callback_size + snapshot_size,
        "kind_bytes": {
            "HISTORICAL_MARKET_RESULT": 0,
            "SNAPSHOT_FACT": len(canonical_bytes(snapshot)) + 1,
            "CALLBACK_RESULT": len(canonical_bytes(callback)) + 1,
            "SOURCE_GROUP_RESULT": 0,
        },
        "kind_rows": {
            "HISTORICAL_MARKET_RESULT": 0,
            "SNAPSHOT_FACT": 1,
            "CALLBACK_RESULT": 1,
            "SOURCE_GROUP_RESULT": 0,
        },
        "definition_count": 0,
        "definition_bytes": 0,
    }


@pytest.mark.parametrize(
    "field", ["schema_version", "sha256", "byte_count", "row_count"]
)
def test_reader_checks_each_manifest_wire_field_before_parsing(tmp_path, field):
    raw = _raw(_wire_row())
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    expected = _metadata(raw)
    expected[field] = (
        "bad" if field in {"schema_version", "sha256"} else expected[field] + 1
    )

    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        reader.read_p3_v2_journal(
            path, expected_metadata=expected, contexts=CONTEXTS, budget=_budget(raw)
        )


def test_metadata_mismatch_rejects_before_any_json_parse(monkeypatch, tmp_path):
    raw = _raw(_wire_row())
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    called = []

    def forbidden_parse(line):
        called.append(line)
        raise AssertionError("manifest mismatch must precede parsing")

    monkeypatch.setattr(reader, "decode_canonical", forbidden_parse)
    expected = _metadata(raw)
    expected["sha256"] = "f" * 64
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        reader.read_p3_v2_journal(
            path, expected_metadata=expected, contexts=CONTEXTS, budget=_budget(raw)
        )
    assert called == []
