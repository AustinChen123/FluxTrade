# pyright: reportArgumentType=false, reportCallIssue=false, reportOptionalSubscript=false
"""Store writer state-machine and operating-system fault tests."""

from hashlib import sha256
from copy import deepcopy
import os

import pytest

from src.core.backtest import spider_run_store as module
from src.core.backtest.spider_run_artifacts import canonical_bytes
from spider_run_store_v2_fixtures import (
    callback_row, empty_v2_writer, p3_v2_finalize_fixture, source_group_row,
)


def test_v2_codec_refusal_is_named_terminal_and_precedes_file_write(
    tmp_path, real_p3_artifacts, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = callback_row(store, values, 4_096)
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind="CALLBACK_RESULT")
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    monkeypatch.setattr(
        module, "encode_p3_v2_row_with_context_gate",
        lambda *args, **kwargs: (_ for _ in ()).throw(ValueError("CODEC_UNSUPPORTED_ROW")),
    )
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.append_journal(row)
    assert caught.value.primary_failure["reason"] == "CODEC_UNSUPPORTED_ROW"
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.append_journal(row)
    assert journal.read_bytes() == b""


def test_v2_invalid_order_slot_refusal_is_named_terminal_before_file_write(
    tmp_path, real_p3_artifacts,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = source_group_row(store, values)
    for phase in ("before", "after"):
        slots = row["payload"][f"owner_evidence_{phase}"]["open_orders_fact"]["immutable_payload"]["rows"]
        if slots:
            slots[0]["unsupported_field"] = "reject"
            break
    else:
        raise AssertionError("fixture must contain an order slot")
    boundary = dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"])
    store.mark_processed(boundary, record_kind="SOURCE_GROUP_RESULT")
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}

    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.append_journal(row)
    assert caught.value.reason == "PERSISTENCE_FAILED"
    assert caught.value.primary_failure["reason"] == "CODEC_UNSUPPORTED_ROW"
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
    assert store._p3_v2_wire_bytes == 0 and store._p3_v2_codec_state == ()
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.append_journal(source_group_row(store, values))
    assert journal.read_bytes() == b""


def test_v1_invalid_order_slot_refusal_keeps_legacy_retry_and_bytes(
    tmp_path, real_p3_artifacts,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    store._attempt["artifact_encoding"] = "artifact_encoding_v1"
    row = source_group_row(store, values)
    invalid = deepcopy(row)
    for phase in ("before", "after"):
        slots = invalid["payload"][f"owner_evidence_{phase}"]["open_orders_fact"]["immutable_payload"]["rows"]
        if slots:
            slots[0]["unsupported_field"] = "reject"
            break
    else:
        raise AssertionError("fixture must contain an order slot")
    store.mark_processed(
        dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]),
        record_kind="SOURCE_GROUP_RESULT",
    )
    before = {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()}
    with pytest.raises(ValueError):
        store.append_journal(invalid)
    assert {path.name: path.read_bytes() for path in store._path.iterdir() if path.is_file()} == before
    assert not store._barrier_failed
    store.append_journal(row)
    assert journal.read_bytes() == canonical_bytes(row) + b"\n"


def test_v1_encoding_never_uses_v2_writer_even_if_flag_enabled(tmp_path, real_p3_artifacts):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    store._attempt["artifact_encoding"] = "artifact_encoding_v1"
    row = deepcopy(values[2][0])
    boundary = dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"], barrier_id=row["barrier_id"])
    store.mark_processed(boundary, record_kind=row["record_kind"])
    store.append_journal(row)
    assert journal.read_bytes() == canonical_bytes(row) + b"\n"
    assert store._p3_v2_wire_bytes == 0 and store._p3_v2_codec_state == ()


def test_v2_write_flag_off_keeps_legacy_row_bytes(tmp_path, real_p3_artifacts):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = deepcopy(values[2][0])
    store.mark_processed(
        dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"], barrier_id=row["barrier_id"]),
        record_kind=row["record_kind"],
    )
    store._p3_v2_write_enabled = False
    store.append_journal(row)
    assert journal.read_bytes() == canonical_bytes(row) + b"\n"
    assert store._p3_v2_wire_bytes == 0 and store._p3_v2_codec_state == ()


def test_v2_processed_status_failure_is_terminal_before_journal_append(
    tmp_path, real_p3_artifacts, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = values[2][0]
    before_status = (store._path / "status.json").read_bytes()

    def fail_status(name, raw):
        if name == "status.json":
            raise module.SpiderRunStoreError("ARTIFACT_WRITE_FAILED")
        raise AssertionError(name)

    monkeypatch.setattr(store, "_publish", fail_status)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.mark_processed(
            dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]),
            record_kind=row["record_kind"],
        )
    assert caught.value.reason == "PERSISTENCE_FAILED"
    assert caught.value.primary_failure["reason"] == "ARTIFACT_WRITE_FAILED"
    assert (store._path / "status.json").read_bytes() == before_status
    assert store._processed == dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"])
    assert store._persisted is None
    assert journal.read_bytes() == b"" and store._p3_v2_wire_bytes == 0
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.mark_processed(
            dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]),
            record_kind=row["record_kind"],
        )
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.append_journal(row)
    assert journal.read_bytes() == b""
    assert store._p3_v2_kind_bytes == {} and store._p3_v2_kind_rows == {}
    assert store._p3_v2_projected_bytes == store._p3_v2_definition_bytes == 0


@pytest.mark.parametrize("fault", ["write", "short_write", "flush", "fsync"])
def test_v2_e2_write_flush_and_fsync_faults_close_fd_without_counter_commit(
    tmp_path, real_p3_artifacts, fault, monkeypatch,
):
    store, values, _, _ = p3_v2_finalize_fixture(tmp_path, real_p3_artifacts)
    journal = empty_v2_writer(store)
    row = values[2][0]
    store.mark_processed(dict(ordinal=1, journal_seq=1, barrier_id=row["barrier_id"]), record_kind=row["record_kind"])
    inode = journal.stat().st_ino
    descriptors = []
    original_open = module._open
    original_fsync = os.fsync

    class StreamFault:
        def __init__(self, stream):
            self.stream = stream

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def __enter__(self):
            self.stream.__enter__()
            return self

        def __exit__(self, *args):
            return self.stream.__exit__(*args)

        def write(self, raw):
            if fault == "write":
                self.stream.write(raw[:1])
                raise OSError("injected write fault")
            if fault == "short_write":
                self.stream.write(raw[:-1])
                return len(raw) - 1
            return self.stream.write(raw)

        def flush(self):
            self.stream.flush()
            if fault == "flush":
                raise OSError("injected flush fault")

    def open_stream(fd, mode, closefd=False):
        stream = original_open(fd, mode, closefd=closefd)
        if os.fstat(fd).st_ino == inode:
            descriptors.append(fd)
            return StreamFault(stream)
        return stream

    def fsync(fd):
        if fault == "fsync" and os.fstat(fd).st_ino == inode:
            descriptors.append(fd)
            raise OSError("injected fsync fault")
        return original_fsync(fd)

    monkeypatch.setattr(module, "_open", open_stream)
    monkeypatch.setattr(os, "fsync", fsync)
    with pytest.raises(module.SpiderRunStoreError) as caught:
        store.append_journal(row)
    assert caught.value.reason == "PERSISTENCE_FAILED"
    assert caught.value.primary_failure["reason"] == "ARTIFACT_WRITE_FAILED"
    assert store._p3_v2_wire_bytes == 0 and store._p3_v2_codec_state == ()
    assert store._p3_v2_kind_bytes == {} and store._p3_v2_kind_rows == {}
    assert store._p3_v2_projected_bytes == 0 and store._p3_v2_definition_bytes == 0
    assert store._p3_v2_journal_hasher.hexdigest() == sha256(b"").hexdigest()
    assert descriptors
    with pytest.raises(OSError):
        os.fstat(descriptors[-1])
    if fault in {"flush", "fsync"}:
        assert journal.read_bytes().endswith(b"\n")
    else:
        assert journal.read_bytes() and not journal.read_bytes().endswith(b"\n")
    with pytest.raises(RuntimeError, match="INVALID_STORE_OPERATION"):
        store.append_journal(row)
