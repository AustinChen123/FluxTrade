# pyright: reportGeneralTypeIssues=false, reportOptionalSubscript=false
"""P3 v2 finalize lifecycle tests over real durable files."""

import inspect
from copy import deepcopy
from hashlib import sha256
from pathlib import Path
from typing import Any, cast

import pytest

from src.core.backtest import spider_projection_size as projection_size
from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_jsonl
from spider_run_store_v2_fixtures import empty_v2_writer, p3_v2_finalize_fixture


def test_v2_finalize_uses_projection_but_completion_records_wire_bytes(tmp_path, real_p3_artifacts):
    store, values, recon, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    assert store.finalize(values[3], recon, values[4]) == "COMPLETE_PUBLISHED"
    complete = decode_jsonl((store._path / "completion.json").read_bytes())[0]
    entries = cast(list[dict[str, Any]], complete["artifacts"])
    journal_entry = next(item for item in entries if item["path"] == "journal.jsonl")
    assert journal_entry == dict(
        path="journal.jsonl", schema_version="spider_journal_record_v2",
        sha256=sha256(raw).hexdigest(), byte_count=len(raw), row_count=len(values[2]),
    )
    for entry in entries:
        artifact_raw = (store._path / entry["path"]).read_bytes()
        assert entry["sha256"] == sha256(artifact_raw).hexdigest()
        assert entry["byte_count"] == len(artifact_raw)
        assert entry["row_count"] == (artifact_raw.count(b"\n") if entry["path"].endswith(".jsonl") else 1)
    assert (store._path / "journal.jsonl").read_bytes() == raw
    for name in ("endpoint.json", "reconciliation.json", "report.jsonl"):
        assert (store._path / name).read_bytes() == real_p3_artifacts[name]
    assert decode_jsonl((store._path / "endpoint.json").read_bytes()) == [values[3]]
    assert decode_jsonl((store._path / "report.jsonl").read_bytes()) == values[4]
    assert store._state == "FROZEN"


def test_v2_write_path_appends_then_finalizes_from_real_wire_file(tmp_path, real_p3_artifacts):
    store, values, recon, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal_path = store._path / "journal.jsonl"
    journal_path.write_bytes(b"")
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
    store._processed = None
    store._persisted = None
    store._processed_kind = None
    store._historical_barrier_ids.clear()
    store._p3_v2_write_enabled = True

    for row in values[2]:
        boundary = dict(
            ordinal=row["journal_seq"], journal_seq=row["journal_seq"],
            barrier_id=row["barrier_id"],
        )
        store.mark_processed(boundary, record_kind=row["record_kind"])
        store.append_journal(row)

    wire = journal_path.read_bytes()
    assert wire.endswith(b"\n")
    assert wire.count(b"\n") == len(values[2])
    assert store._p3_v2_journal_metadata == dict(
        schema_version="spider_journal_record_v2", sha256=sha256(wire).hexdigest(),
        byte_count=len(wire), row_count=len(values[2]),
    )
    wire_rows = decode_jsonl(wire)
    expected_kind_bytes = {}
    expected_kind_rows = {}
    expected_definition_bytes = 0
    for item in wire_rows:
        kind = item["record_kind"]
        expected_kind_bytes[kind] = expected_kind_bytes.get(kind, 0) + len(canonical_bytes(item)) + 1
        expected_kind_rows[kind] = expected_kind_rows.get(kind, 0) + 1
        expected_definition_bytes += sum(len(canonical_bytes(definition)) for definition in item.get("order_defs", []))
    assert store._p3_v2_kind_bytes == expected_kind_bytes
    assert store._p3_v2_kind_rows == expected_kind_rows
    assert store._p3_v2_definition_bytes == expected_definition_bytes
    assert store._p3_v2_projected_bytes == sum(projection_size.canonical_jsonl_row_len(row) for row in values[2])
    assert store._p3_v2_codec_state == tuple(
        canonical_bytes(definition)
        for item in wire_rows for definition in item.get("order_defs", [])
    )
    assert store.finalize(values[3], recon, values[4]) == "COMPLETE_PUBLISHED"
    manifest = decode_jsonl((store._path / "completion.json").read_bytes())[0]
    entry = next(item for item in manifest["artifacts"] if item["path"] == "journal.jsonl")
    assert entry["sha256"] == sha256(wire).hexdigest()
    assert entry["byte_count"] == len(wire)
    assert entry["row_count"] == len(values[2])


def test_v2_write_precheck_rejects_before_file_or_counter_commit(tmp_path, real_p3_artifacts, monkeypatch):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    empty_v2_writer(store)
    row = values[2][0]
    boundary = dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"])
    store.mark_processed(boundary, record_kind=row["record_kind"])
    original_budget = module.artifact_limits.p3_v2_scope_budget
    monkeypatch.setattr(module.artifact_limits, "p3_v2_scope_budget", lambda coverage: original_budget(coverage) | {"J2": 0})
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.append_journal(row)
    assert caught.value.reason == "PERSISTENCE_FAILED"
    assert caught.value.primary_failure["reason"] == "ARTIFACT_LIMIT_EXCEEDED"
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
    assert store._p3_v2_wire_bytes == 0 and store._p3_v2_codec_state == ()
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind=row["record_kind"])


def test_v2_write_failure_keeps_partial_tail_and_does_not_commit_candidate(tmp_path, real_p3_artifacts, monkeypatch):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = values[2][0]
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind=row["record_kind"])
    original_write = module._write

    def partial_write(path, raw, append=False):
        if Path(path).name == "journal.jsonl":
            with Path(path).open("ab") as stream:
                stream.write(raw[: max(1, len(raw) // 2)])
            raise module.SpiderRunStoreError("ARTIFACT_WRITE_FAILED")
        return original_write(path, raw, append)

    monkeypatch.setattr(module, "_write", partial_write)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.append_journal(row)
    assert caught.value.reason == "PERSISTENCE_FAILED"
    assert 0 < journal.stat().st_size < len(canonical_bytes(row)) + 1
    assert store._p3_v2_wire_bytes == 0 and store._p3_v2_codec_state == ()
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.append_journal(row)


def test_v2_durable_row_then_status_failure_commits_wire_state_and_is_terminal(tmp_path, real_p3_artifacts, monkeypatch):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = values[2][0]
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind=row["record_kind"])
    old_status = (store._path / "status.json").read_bytes()
    original_publish = store._publish

    def fail_running_status(name, raw):
        if name == "status.json" and b'"state":"RUNNING"' in raw:
            raise module.SpiderRunStoreError("ARTIFACT_WRITE_FAILED")
        return original_publish(name, raw)

    monkeypatch.setattr(store, "_publish", fail_running_status)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.append_journal(row)
    wire = journal.read_bytes()
    assert caught.value.reason == "PERSISTENCE_FAILED"
    assert caught.value.primary_failure["reason"] == "ARTIFACT_WRITE_FAILED"
    assert wire.endswith(b"\n") and wire.count(b"\n") == 1
    assert (store._path / "status.json").read_bytes() == old_status
    assert store._p3_v2_wire_bytes == len(wire)
    assert store._p3_v2_journal_metadata["sha256"] == sha256(wire).hexdigest()
    assert store._persisted == store._processed
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.mark_processed(dict(ordinal=2, journal_seq=2, barrier_id="after-status-error"), record_kind=row["record_kind"])


def test_v2_delivery_attempt_rejected_at_mark_before_status_or_journal_write(tmp_path, real_p3_artifacts):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id="delivery"), record_kind="DELIVERY_ATTEMPT")
    assert caught.value.reason == "CODEC_UNSUPPORTED_ROW"
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
    assert journal.read_bytes() == b"" and store._p3_v2_wire_bytes == 0
    store._processed = dict(ordinal=1, journal_seq=1, barrier_id="delivery")
    store._processed_kind = "DELIVERY_ATTEMPT"
    with pytest.raises(module.SpiderRunStoreError) as append_error:
        store.append_journal({"record_kind": "DELIVERY_ATTEMPT"})
    assert append_error.value.reason == "CODEC_UNSUPPORTED_ROW"
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
    assert journal.read_bytes() == b"" and store._p3_v2_wire_bytes == 0


def test_v2_finalize_drops_each_journal_projection_and_preserves_partial_publication(tmp_path,
                                                                                     real_p3_artifacts,
                                                                                     monkeypatch):
    store, values, recon, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    original_publish, original_write = module.SpiderRunStore._publish, module._write
    drop_checks = []

    def observe_publish(self, name, content):
        frame = inspect.currentframe()
        assert frame is not None and frame.f_back is not None
        frame = frame.f_back
        local_values = frame.f_locals
        if name == "endpoint.json":
            drop_checks.append("current_journal" not in local_values)
        if name == "status.json" and b'"COMPLETE"' in content:
            drop_checks.append("journal" not in local_values and local_values["reread"][2][0] is None)
        return original_publish(self, name, content)

    def fail_completion_write(path, content, append=False):
        if Path(path).name == ".completion.json.tmp":
            raise module.SpiderRunStoreError("ARTIFACT_WRITE_FAILED")
        return original_write(path, content, append)

    monkeypatch.setattr(module.SpiderRunStore, "_publish", observe_publish)
    monkeypatch.setattr(module, "_write", fail_completion_write)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.finalize(values[3], recon, values[4])
    assert caught.value.reason == "ARTIFACT_WRITE_FAILED"
    assert drop_checks == [True, True]
    assert (store._path / "journal.jsonl").read_bytes() == raw
    assert b'"state":"COMPLETE"' in (store._path / "status.json").read_bytes()
    assert all((store._path / name).exists() for name in (
        "endpoint.json", "reconciliation.json", "report.jsonl",
    ))
    assert not (store._path / "completion.json").exists()
    assert not (store._path / ".completion.json.tmp").exists()
    assert store._state == "RUNNING"
    before = {p.name: p.read_bytes() for p in store._path.iterdir() if p.is_file()}
    with pytest.raises(module.SpiderRunStoreError) as retry:
        store.finalize(values[3], recon, values[4])
    assert retry.value.reason == "PUBLICATION_FAILED"
    assert not (store._path / "completion.json").exists()
    assert store._state == "RUNNING"
    assert (store._path / "journal.jsonl").read_bytes() == raw
    assert (store._path / "status.json").read_bytes() == before["status.json"]
    after = {p.name: p.read_bytes() for p in store._path.iterdir() if p.is_file()}
    assert after[".endpoint.json.tmp"]
    assert {name: after[name] for name in before} == before


def test_v2_unknown_projected_field_is_named_and_rejected_before_any_finalize_write(tmp_path,
                                                                                   real_p3_artifacts):
    store, values, recon, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    rows = decode_jsonl(raw)
    rows[0]["unapproved_field"] = "x"
    bad = b"".join(canonical_bytes(row) + b"\n" for row in rows)
    (store._path / "journal.jsonl").write_bytes(bad)
    store._p3_v2_journal_metadata = dict(
        schema_version="spider_journal_record_v2", sha256=sha256(bad).hexdigest(),
        byte_count=len(bad), row_count=len(rows),
    )
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    for _ in range(2):
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.finalize(values[3], recon, values[4])
        assert caught.value.reason == "ARTIFACT_MISMATCH"
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
        assert store._state == "RUNNING"


@pytest.mark.parametrize("limited_artifact", [
    "endpoint.json", "reconciliation.json", "report.jsonl", "status.json", "completion.json",
])
def test_v2_finalize_prechecks_generated_artifact_caps_before_first_write(tmp_path, real_p3_artifacts,
                                                                          monkeypatch, limited_artifact):
    store, values, recon, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    original_limit = store._p3_v2_artifact_limit
    status_raw = (store._path / "status.json").read_bytes()
    cap = len(status_raw) if limited_artifact == "status.json" else 1
    monkeypatch.setattr(
        store, "_p3_v2_artifact_limit",
        lambda name: cap if name == limited_artifact else original_limit(name),
    )
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.finalize(values[3], recon, values[4])
    assert caught.value.reason == "ARTIFACT_LIMIT_EXCEEDED"
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
    assert store._state == "RUNNING"
    with pytest.raises(module.SpiderRunStoreError) as retry:
        store.finalize(values[3], recon, values[4])
    assert retry.value.reason == "ARTIFACT_LIMIT_EXCEEDED"
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
    assert store._state == "RUNNING"


@pytest.mark.parametrize("failure,reason", [
    ("link", "PUBLICATION_FAILED"),
    ("sync", "PUBLICATION_DURABILITY_UNKNOWN"),
])
def test_v2_completion_link_and_directory_sync_failure_states(tmp_path, real_p3_artifacts,
                                                               monkeypatch, failure, reason):
    store, values, recon, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    original_link, original_sync = module.os.link, module._sync

    def fail_completion_link(source, destination):
        if Path(destination).name == "completion.json":
            raise OSError("injected completion link failure")
        return original_link(source, destination)

    def fail_completion_sync(directory):
        if (Path(directory) / "completion.json").exists():
            raise module.SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN")
        return original_sync(directory)

    if failure == "link":
        monkeypatch.setattr(module.os, "link", fail_completion_link)
    else:
        monkeypatch.setattr(module, "_sync", fail_completion_sync)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.finalize(values[3], recon, values[4])
    assert caught.value.reason == reason
    assert (store._path / "journal.jsonl").read_bytes() == raw
    assert b'"state":"COMPLETE"' in (store._path / "status.json").read_bytes()
    if failure == "link":
        assert store._state == "RUNNING"
        assert not (store._path / "completion.json").exists()
        assert (store._path / ".completion.json.tmp").is_file()
        expected_retry = "PUBLICATION_FAILED"
    else:
        assert store._state == "FROZEN"
        assert (store._path / "completion.json").is_file()
        expected_retry = "INVALID_STORE_OPERATION"
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    with pytest.raises((module.SpiderRunStoreError, RuntimeError)) as retry:
        store.finalize(values[3], recon, values[4])
    retry_reason = getattr(retry.value, "reason", str(retry.value))
    assert retry_reason == expected_retry
    after = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    assert all(after[name] == content for name, content in before.items())
    if failure == "link":
        assert ".endpoint.json.tmp" in after
    else:
        assert after == before


@pytest.fixture(scope="module")
def shared_partial_tail_fixture(tmp_path_factory, real_p3_artifacts):
    root = tmp_path_factory.mktemp("j6-partial-tail")
    return p3_v2_finalize_fixture(root, real_p3_artifacts)


@pytest.mark.parametrize("bytes_removed_from_tail", range(1, 4377))
def test_v2_partial_tail_at_every_final_row_byte_rejects_repeatedly(bytes_removed_from_tail,
                                                                    shared_partial_tail_fixture):
    store, values, recon, raw = shared_partial_tail_fixture
    start = raw.rfind(b"\n", 0, -1) + 1
    assert len(raw) - start == 4377
    cut = len(raw) - bytes_removed_from_tail
    assert start < cut < len(raw)
    partial = raw[:cut]
    (store._path / "journal.jsonl").write_bytes(partial)
    store._p3_v2_journal_metadata = dict(
        schema_version="spider_journal_record_v2", sha256=sha256(partial).hexdigest(),
        byte_count=len(partial), row_count=partial.count(b"\n"),
    )
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    for _ in range(2):
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.finalize(values[3], recon, values[4])
        assert caught.value.reason == "ENDPOINT_RECONCILIATION_FAILED"
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
        assert store._state == "RUNNING"


@pytest.mark.parametrize("failure", ["metadata", "missing_metadata", "mixed", "partial", "projection",
                                     "attempt", "oversized_attempt", "foreign_run_id"])
def test_v2_finalize_read_failures_leave_real_files_and_running_status(tmp_path, real_p3_artifacts,
                                                                       monkeypatch, failure):
    store, values, recon, raw = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal_path = store._path / "journal.jsonl"
    attempt_path = store._path / "attempt.json"
    if failure == "metadata":
        cast(dict[str, object], store._p3_v2_journal_metadata)["sha256"] = "f" * 64
    elif failure == "missing_metadata":
        store._p3_v2_journal_metadata = None
    elif failure == "foreign_run_id":
        rows = decode_jsonl(raw)
        rows[0]["run_id"] = "foreign-run"
        bad = b"".join(canonical_bytes(row) + b"\n" for row in rows)
        journal_path.write_bytes(bad)
        store._p3_v2_journal_metadata = dict(
            schema_version="spider_journal_record_v2", sha256=sha256(bad).hexdigest(),
            byte_count=len(bad), row_count=len(rows),
        )
    elif failure == "oversized_attempt":
        attempt = deepcopy(store._attempt)
        attempt["planned_coverage"][0]["barrier_id"] = "x" * module.artifact_limits.DEFAULT_LIMIT
        attempt_path.write_bytes(canonical_bytes(attempt) + b"\n")
    elif failure == "mixed":
        rows = decode_jsonl(raw)
        rows[0]["schema_version"] = "spider_journal_record_v1"
        bad = b"".join(canonical_bytes(row) + b"\n" for row in rows)
        journal_path.write_bytes(bad)
        store._p3_v2_journal_metadata = dict(
            schema_version="spider_journal_record_v2", sha256=sha256(bad).hexdigest(),
            byte_count=len(bad), row_count=len(rows),
        )
    elif failure == "partial":
        bad = raw[:-1]
        journal_path.write_bytes(bad)
        store._p3_v2_journal_metadata = dict(
            schema_version="spider_journal_record_v2", sha256=sha256(bad).hexdigest(),
            byte_count=len(bad), row_count=bad.count(b"\n"),
        )
    elif failure == "projection":
        monkeypatch.setattr(projection_size, "P3_V2_MATERIAL_CEILING", 1)
    else:
        attempt = deepcopy(store._attempt)
        attempt["planned_coverage"][0]["barrier_id"] = "different-valid-coverage-id"
        attempt_path.write_bytes(canonical_bytes(attempt) + b"\n")
    journal_before = journal_path.read_bytes()
    status_before = (store._path / "status.json").read_bytes()
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    expected_reason = (
        "ARTIFACT_LIMIT_EXCEEDED" if failure in ("projection", "oversized_attempt")
        else "ARTIFACT_MISMATCH" if failure == "foreign_run_id"
        else "ENDPOINT_RECONCILIATION_FAILED"
    )
    for _ in range(2):
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store.finalize(values[3], recon, values[4])
        assert caught.value.reason == expected_reason
        assert journal_path.read_bytes() == journal_before
        assert (store._path / "status.json").read_bytes() == status_before
        assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
        assert store._state == "RUNNING"
