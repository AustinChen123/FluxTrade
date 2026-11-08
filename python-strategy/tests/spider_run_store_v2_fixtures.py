"""Shared P3 v2 store fixture helpers for read, write, and finalize tests."""

from copy import deepcopy
from hashlib import sha256
from typing import Any, cast


from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_journal_codec_v2 import encode_p3_v2_row_with_context_gate
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_jsonl
from src.core.backtest.spider_run_envelope_schema import journal as validate_journal


def build_real_p3_artifacts(tmp_path_factory):
    from test_spider_scenario_run import historical_input as make_historical_input
    from src.core.backtest.spider_scenario_run import run_spider_scenario

    root = tmp_path_factory.mktemp("j6-real-p3-finalize")
    run, raw = make_historical_input("j6-real-p3-finalize")
    assert run_spider_scenario(str(root), run.run_id, "SPIDER_HISTORICAL_RESEARCH_RUN_V1", raw) == {
        "run_id": run.run_id, "outcome": "ADMITTED", "reason": None,
    }
    return {
        name: (root / run.run_id / name).read_bytes()
        for name in ("attempt.json", "status.json", "journal.jsonl", "endpoint.json",
                     "reconciliation.json", "report.jsonl", "historical_input.json")
    }


def p3_v2_finalize_fixture(tmp_path, real_artifacts):
    values: list[Any] = [
        decode_jsonl(real_artifacts["attempt.json"])[0],
        decode_jsonl(real_artifacts["status.json"])[0],
        decode_jsonl(real_artifacts["journal.jsonl"]),
        decode_jsonl(real_artifacts["endpoint.json"])[0],
        decode_jsonl(real_artifacts["report.jsonl"]),
    ]
    attempt = deepcopy(values[0])
    store = module.SpiderRunStore.create(str(tmp_path), cast(str, attempt["run_id"]))
    attempt["artifact_encoding"] = "artifact_encoding_v2"
    state = ()
    wire_rows = []
    for row in values[2]:
        encoded, state = encode_p3_v2_row_with_context_gate(
            row, state,
            validate_v1=module.journal_record,
            check_context_chain=lambda candidate: module._context_chain(values[0], candidate),
        )
        wire_rows.append(encoded)
    validate_journal(values[2])
    journal_raw = b"".join(canonical_bytes(row) + b"\n" for row in wire_rows)
    store._attempt = attempt
    store._processed = deepcopy(cast(dict[str, Any], values[1]["processed_boundary"]))
    store._persisted = deepcopy(cast(dict[str, Any], values[1]["persisted_boundary"]))
    store._state = "RUNNING"
    store._publish("historical_input.json", real_artifacts["historical_input.json"])
    store._publish("attempt.json", canonical_bytes(attempt) + b"\n")
    store._publish("status.json", canonical_bytes(store._status("RUNNING")) + b"\n")
    store._publish("journal.jsonl", journal_raw)
    store._p3_v2_journal_metadata = dict(
        schema_version="spider_journal_record_v2",
        sha256=sha256(journal_raw).hexdigest(),
        byte_count=len(journal_raw),
        row_count=len(wire_rows),
    )
    recon = decode_jsonl(real_artifacts["reconciliation.json"])[0]
    return store, values, recon, journal_raw


def empty_v2_writer(store):
    path = store._path / "journal.jsonl"
    path.write_bytes(b"")
    store._p3_v2_journal_metadata = dict(
        schema_version="spider_journal_record_v2", sha256=sha256(b"").hexdigest(),
        byte_count=0, row_count=0,
    )
    store._p3_v2_journal_hasher = sha256()
    store._p3_v2_wire_bytes = 0
    store._p3_v2_kind_bytes = {}
    store._p3_v2_kind_rows = {}
    store._p3_v2_codec_state = ()
    store._p3_v2_projected_bytes = 0
    store._p3_v2_definition_bytes = 0
    store._processed = store._persisted = None
    store._processed_kind = None
    store._historical_barrier_ids.clear()
    store._p3_v2_write_enabled = True
    return path


def callback_row(store, values, target_wire_bytes):
    row = deepcopy(next(item for item in values[2] if item["record_kind"] == "CALLBACK_RESULT"))
    row["journal_seq"] = 1
    row["barrier_id"] = "writer-boundary"
    expected_wire = deepcopy(row)
    expected_wire["schema_version"] = "spider_journal_record_v2"
    del expected_wire["configuration_context"]
    del expected_wire["historical_context"]
    base_size = len(canonical_bytes(expected_wire)) + 1
    row["barrier_id"] += "x" * (target_wire_bytes - base_size)
    expected_wire["barrier_id"] = row["barrier_id"]
    assert len(canonical_bytes(expected_wire)) + 1 == target_wire_bytes
    return row


def source_group_row(store, values):
    row = deepcopy(next(item for item in values[2] if item["record_kind"] == "SOURCE_GROUP_RESULT"))
    row["journal_seq"] = 1
    row["barrier_id"] = "sg-boundary"
    return row


def independent_source_group_wire(row):
    """Apply D5's two open-order reference slots without calling the codec."""
    wire = deepcopy(row)
    wire["schema_version"] = "spider_journal_record_v2"
    del wire["configuration_context"]
    del wire["historical_context"]
    definitions = []
    indexes = {}
    for phase in ("before", "after"):
        rows = wire["payload"][f"owner_evidence_{phase}"]["open_orders_fact"]["immutable_payload"]["rows"]
        references = []
        for item in rows:
            encoded = canonical_bytes(item)
            if encoded not in indexes:
                indexes[encoded] = len(definitions)
                definitions.append(item)
            references.append(indexes[encoded])
        wire["payload"][f"owner_evidence_{phase}"]["open_orders_fact"]["immutable_payload"]["rows"] = references
    if definitions:
        wire["order_defs"] = definitions
    return canonical_bytes(wire) + b"\n"
