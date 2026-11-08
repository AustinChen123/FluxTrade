"""P3 v2 read-side/finalize tests over real durable files."""

from copy import deepcopy
from hashlib import sha256
from typing import Any, cast

import pytest

from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_journal_codec_v2 import encode_p3_v2_row_with_context_gate
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_jsonl, validate_artifact
from src.core.backtest.spider_run_envelope_schema import journal as validate_journal


@pytest.fixture(scope="module")
def real_p3_artifacts(tmp_path_factory):
    from test_spider_scenario_run import historical_input as make_historical_input
    from src.core.backtest.spider_scenario_run import run_spider_scenario

    root = tmp_path_factory.mktemp("j6-real-p3")
    run, raw = make_historical_input("j6-real-p3")
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


def test_v2_reread_dispatch_returns_projection_and_wire_identity(tmp_path, real_p3_artifacts):
    store, values, _, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    attempt, attempt_metadata = store._reread_artifact("attempt.json", validate_artifact)
    assert attempt == store._attempt
    rows, metadata = store._reread_artifact("journal.jsonl", validate_journal, attempt=attempt)
    assert rows == values[2]
    expected_metadata = cast(dict[str, object], store._p3_v2_journal_metadata)
    assert metadata == dict(path="journal.jsonl", **expected_metadata)
    assert metadata["sha256"] == sha256(raw).hexdigest()
    assert metadata["byte_count"] == len(raw)
    assert metadata["row_count"] == len(values[2])
    assert attempt_metadata["schema_version"] == "spider_attempt_v1"


@pytest.mark.parametrize("field", ["schema_version", "sha256", "byte_count", "row_count"])
def test_v2_reread_rejects_wire_metadata_mismatch_from_real_file(tmp_path, real_p3_artifacts, field):
    store, _, _, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    attempt = store._reread_artifact("attempt.json", validate_artifact)[0]
    metadata = dict(cast(dict[str, object], store._p3_v2_journal_metadata))
    if field == "schema_version":
        metadata[field] = "spider_journal_record_v1"
    else:
        metadata[field] = "f" * 64 if field == "sha256" else cast(int, metadata[field]) + 1
    store._p3_v2_journal_metadata = metadata
    before = (store._path / "journal.jsonl").read_bytes()
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("journal.jsonl", validate_journal, attempt=attempt)
    assert caught.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert (store._path / "journal.jsonl").read_bytes() == before == raw


def test_v2_reread_rejects_mixed_version_rows_without_fallback(tmp_path, real_p3_artifacts):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    attempt = store._reread_artifact("attempt.json", validate_artifact)[0]
    rows = decode_jsonl((store._path / "journal.jsonl").read_bytes())
    rows[0]["schema_version"] = "spider_journal_record_v1"
    mixed = b"".join(canonical_bytes(row) + b"\n" for row in rows)
    (store._path / "journal.jsonl").write_bytes(mixed)
    store._p3_v2_journal_metadata = dict(
        schema_version="spider_journal_record_v2", sha256=sha256(mixed).hexdigest(),
        byte_count=len(mixed), row_count=len(rows),
    )
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("journal.jsonl", validate_journal, attempt=attempt)
    assert caught.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert (store._path / "journal.jsonl").read_bytes() == mixed
    store._attempt["artifact_encoding"] = "artifact_encoding_v1"
    with pytest.raises(module.SpiderRunStoreError) as reverse:
        store._reread_artifact("journal.jsonl", validate_journal)
    assert reverse.value.reason == "ENDPOINT_RECONCILIATION_FAILED"


def test_v2_reread_rejects_partial_tail_with_metadata_for_the_partial_bytes(tmp_path, real_p3_artifacts):
    store, _, _, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    attempt = store._reread_artifact("attempt.json", validate_artifact)[0]
    partial = raw[:-1]
    (store._path / "journal.jsonl").write_bytes(partial)
    store._p3_v2_journal_metadata = dict(
        schema_version="spider_journal_record_v2", sha256=sha256(partial).hexdigest(),
        byte_count=len(partial), row_count=partial.count(b"\n"),
    )
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("journal.jsonl", validate_journal, attempt=attempt)
    assert caught.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert (store._path / "journal.jsonl").read_bytes() == partial


def test_v2_reread_rejects_symlink_journal_target(tmp_path, real_p3_artifacts):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = store._path / "journal.jsonl"
    target = store._path / "journal-target.jsonl"
    journal.rename(target)
    journal.symlink_to(target.name)
    attempt = store._reread_artifact("attempt.json", validate_artifact)[0]
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("journal.jsonl", validate_journal, attempt=attempt)
    assert caught.value.reason == "PUBLICATION_FAILED"


def test_v2_reread_rejects_attempt_bytes_digest_mismatch(tmp_path, real_p3_artifacts):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    disk_attempt = deepcopy(store._attempt)
    disk_attempt["planned_coverage"][0]["barrier_id"] = "different-valid-coverage-id"
    (store._path / "attempt.json").write_bytes(canonical_bytes(disk_attempt) + b"\n")
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("attempt.json", validate_artifact)
    assert caught.value.reason == "ENDPOINT_RECONCILIATION_FAILED"


def test_v2_reread_rejects_foreign_run_id_without_trusting_valid_wire_metadata(tmp_path,
                                                                                real_p3_artifacts):
    store, _, _, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    attempt = store._reread_artifact("attempt.json", validate_artifact)[0]
    rows = decode_jsonl(raw)
    rows[0]["run_id"] = "foreign-run"
    foreign = b"".join(canonical_bytes(row) + b"\n" for row in rows)
    (store._path / "journal.jsonl").write_bytes(foreign)
    store._p3_v2_journal_metadata = dict(
        schema_version="spider_journal_record_v2", sha256=sha256(foreign).hexdigest(),
        byte_count=len(foreign), row_count=len(rows),
    )
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("journal.jsonl", validate_journal, attempt=attempt)
    assert caught.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
    assert (store._path / "journal.jsonl").read_bytes() == foreign


def test_public_register_does_not_select_p3_v2(tmp_path, real_p3_artifacts):
    attempt = decode_jsonl(real_p3_artifacts["attempt.json"])[0]
    attempt["artifact_encoding"] = "artifact_encoding_v2"
    store = module.SpiderRunStore.create(str(tmp_path), cast(str, attempt["run_id"]))
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        store.register(attempt, historical_input=real_p3_artifacts["historical_input.json"])
    assert store._state == "CREATED"
    assert list(store._path.iterdir()) == []
