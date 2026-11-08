"""P3 v2 read-side/finalize tests over real durable files."""

from copy import deepcopy
from hashlib import sha256
import signal
import os
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


def test_v2_read_caps_follow_the_policy_table_and_scope_budget(tmp_path, real_p3_artifacts):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    coverage = {entry["record_kind"]: 1 for entry in store._attempt["planned_coverage"]}
    scope = module.artifact_limits.p3_v2_scope_budget(coverage)
    default_names = (
        "attempt.json", "status.json", "completion.json", "historical_input.json", "report.jsonl",
    )
    for name in default_names:
        assert store._p3_v2_artifact_limit(name) == module.artifact_limits.DEFAULT_LIMIT
    assert store._p3_v2_artifact_limit("endpoint.json") == scope["E2"]
    assert store._p3_v2_artifact_limit("reconciliation.json") == scope["X2"]


def test_v2_attempt_reader_accepts_exact_cap_and_refuses_cap_plus_one(tmp_path, real_p3_artifacts):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    cap = module.artifact_limits.DEFAULT_LIMIT

    def attempt_at_size(size):
        attempt = deepcopy(store._attempt)
        planned = attempt["planned_coverage"][0]
        original = planned["barrier_id"]
        original_size = len(canonical_bytes(attempt)) + 1
        planned["barrier_id"] = "x" * (size - original_size + len(original))
        raw = canonical_bytes(attempt) + b"\n"
        assert len(raw) == size
        store._attempt = attempt
        (store._path / "attempt.json").write_bytes(raw)

    attempt_at_size(cap)
    _, metadata = store._reread_artifact("attempt.json", validate_artifact)
    assert metadata["byte_count"] == cap

    attempt_at_size(cap + 1)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("attempt.json", validate_artifact)
    assert caught.value.reason == "ARTIFACT_LIMIT_EXCEEDED"


@pytest.mark.parametrize("name", [
    "attempt.json", "status.json", "completion.json", "historical_input.json", "report.jsonl",
    "endpoint.json", "reconciliation.json",
])
def test_v2_nonjournal_paths_read_at_most_policy_cap_plus_one(tmp_path, real_p3_artifacts, monkeypatch, name):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    monkeypatch.setattr(store, "_p3_v2_artifact_limit", lambda _: 31)
    path = store._path / name
    path.write_bytes(b"x" * 32)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        if name == "historical_input.json":
            store._historical_input_metadata(store._attempt)
        else:
            store._reread_artifact(name, validate_artifact)
    assert caught.value.reason == "ARTIFACT_LIMIT_EXCEEDED"
    assert path.stat().st_size == 32


def test_v1_reread_still_delegates_without_v2_cap(tmp_path, real_p3_artifacts, monkeypatch):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    store._attempt["artifact_encoding"] = "artifact_encoding_v1"
    calls = []
    expected = ({"v1": True}, {"byte_count": 16_777_217})

    def original_reread(name, validator):
        calls.append((name, validator))
        return expected

    monkeypatch.setattr(store, "_reread", original_reread)
    assert store._reread_artifact("attempt.json", validate_artifact) == expected
    assert calls == [("attempt.json", validate_artifact)]


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


def test_journal_real_read_oserror_named_and_fd_closed(tmp_path, real_p3_artifacts, monkeypatch):
    import os

    store, *_ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    before = {path.name: path.read_bytes() for path in store._path.iterdir()}
    original_read = os.read
    journal_inode = (store._path / "journal.jsonl").stat().st_ino
    failed_descriptors = []

    def fail_journal_read(fd, size):
        if os.fstat(fd).st_ino == journal_inode:
            failed_descriptors.append(fd)
            raise OSError("injected journal read error")
        return original_read(fd, size)

    monkeypatch.setattr(os, "read", fail_journal_read)
    for _ in range(2):
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store._reread_artifact(
                "journal.jsonl", validate_journal, attempt=store._attempt,
            )
        assert caught.value.reason == "PUBLICATION_FAILED"
        assert {path.name: path.read_bytes() for path in store._path.iterdir()} == before
        with pytest.raises(OSError):
            os.fstat(failed_descriptors[-1])


def test_v2_attempt_is_normalized_only_for_the_legacy_schema_validator(tmp_path, real_p3_artifacts,
                                                                       monkeypatch):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    original_validate = module.validate_artifact
    observed = []

    def capture_normalized(value):
        if type(value) is dict and "artifact_encoding" in value:
            observed.append(value["artifact_encoding"])
        return original_validate(value)

    monkeypatch.setattr(module, "validate_artifact", capture_normalized)
    attempt, _ = store._reread_artifact("attempt.json", validate_artifact)
    assert attempt["artifact_encoding"] == "artifact_encoding_v2"
    assert observed == ["artifact_encoding_v1"]


@pytest.mark.parametrize("encoding", ["artifact_encoding_v1", "unknown_encoding", None])
def test_v2_attempt_rejects_changed_disk_wire_encoding(tmp_path, real_p3_artifacts, encoding):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    attempt = deepcopy(store._attempt)
    if encoding is None:
        attempt.pop("artifact_encoding")
    else:
        attempt["artifact_encoding"] = encoding
    (store._path / "attempt.json").write_bytes(canonical_bytes(attempt) + b"\n")
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("attempt.json", validate_artifact)
    assert caught.value.reason == "ENDPOINT_RECONCILIATION_FAILED"


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


def test_v2_reread_rejects_symlink_before_applying_target_size_cap(tmp_path, real_p3_artifacts):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    attempt = store._path / "attempt.json"
    target = store._path / "oversized-target.json"
    target.write_bytes(b"x" * (module.artifact_limits.DEFAULT_LIMIT + 1))
    attempt.unlink()
    attempt.symlink_to(target.name)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("attempt.json", validate_artifact)
    assert caught.value.reason == "PUBLICATION_FAILED"


def test_v2_reread_requires_attempt_before_journal(tmp_path, real_p3_artifacts):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("journal.jsonl", validate_journal)
    assert caught.value.reason == "ENDPOINT_RECONCILIATION_FAILED"


def test_v2_journal_rejects_unregistered_attempt_identity(tmp_path, real_p3_artifacts):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    foreign_attempt = deepcopy(store._attempt)
    foreign_attempt["artifact_encoding"] = "artifact_encoding_v1"
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store._reread_artifact("journal.jsonl", validate_journal, attempt=foreign_attempt)
    assert caught.value.reason == "ENDPOINT_RECONCILIATION_FAILED"


def test_v2_reread_rejects_fifo_before_open(tmp_path, real_p3_artifacts):
    store, _, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = store._path / "journal.jsonl"
    journal.rename(store._path / "journal.saved")
    os.mkfifo(journal)

    class OpenBlocked(Exception):
        pass

    def deadline(*_):
        raise OpenBlocked("reader blocked opening FIFO")

    previous = signal.signal(signal.SIGALRM, deadline)
    signal.setitimer(signal.ITIMER_REAL, 0.5)
    try:
        with pytest.raises(module.SpiderRunStoreError) as caught:
            store._reread_artifact("journal.jsonl", validate_journal, attempt=store._attempt)
        assert caught.value.reason == "PUBLICATION_FAILED"
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def test_v2_reread_rejects_attempt_identity_mismatch(tmp_path, real_p3_artifacts):
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
    assert caught.value.reason == "ARTIFACT_MISMATCH"
    assert (store._path / "journal.jsonl").read_bytes() == foreign


def test_public_register_does_not_select_p3_v2(tmp_path, real_p3_artifacts):
    attempt = decode_jsonl(real_p3_artifacts["attempt.json"])[0]
    attempt["artifact_encoding"] = "artifact_encoding_v2"
    store = module.SpiderRunStore.create(str(tmp_path), cast(str, attempt["run_id"]))
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        store.register(attempt, historical_input=real_p3_artifacts["historical_input.json"])
    assert store._state == "CREATED"
    assert list(store._path.iterdir()) == []
