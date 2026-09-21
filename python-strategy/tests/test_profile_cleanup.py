"""Offline cleanup orchestration; recording DB, real bounded child-process tests."""
import hashlib
import json
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import DBAPIError

from src.core.market_data.profiles import cleanup, ingest
from src.core.decimal_math import canonical_decimal_text
from src.core.market_data.profiles.handoff import MAX_FRAMED_HANDOFF_BYTES, encode_handoff, parse_handoff
from src.core.market_data.profiles.jobs import RawRetentionResult
from src.core.market_data.profiles.publication import MAX_JSON_BYTES, CanonicalJsonObject
from src.core.market_data.profiles.types import ProfileBin
from test_profile_handoff import RAW
from test_profile_ingest import PARSED, RECOVERED

ENCODED = RAW + b"\n"
DIGEST = "f6630bf75baedaa34b750b167eb0fdda296b14b4c945ec00f2e0f36aa2576ca7"
DELETED = replace(RECOVERED, profile=replace(RECOVERED.profile,
                  publication=replace(RECOVERED.profile.publication, raw_retention_state="DELETED")))
REPORT = dict(reason="COMPLETE", hours=24, referenced_pages=24,
              content_sha256=PARSED.publication.content_sha256, handoff_sha256=DIGEST)


def setup(monkeypatch: pytest.MonkeyPatch):
    store = MagicMock()
    store.recover_completed_snapshot.return_value = RECOVERED
    store.mark_raw_deleted.return_value = RawRetentionResult(DELETED, False)
    execute = MagicMock(side_effect=[ENCODED, json.dumps(REPORT).encode()])
    monkeypatch.setattr(cleanup, "_assemble", execute)
    return cleanup.ProfileRawCleanupProcess(store, "/trusted/assemble", "/trusted/cleanup"), store, execute


def test_encoder_is_exact_rust_fixture_stdout_and_digest() -> None:
    encoded = encode_handoff(PARSED.spec, PARSED.publication)
    assert encoded == ENCODED and hashlib.sha256(encoded).hexdigest() == DIGEST
    assert encoded.endswith(b"\n") and encoded.count(b"\n") == 1
    assert parse_handoff(encoded) == PARSED
    with pytest.raises(ValueError):
        encode_handoff(PARSED.spec, DELETED.profile.publication)
    latest = replace(PARSED.publication, source_available_at=datetime(9999, 12, 31, 23, 59, 59, 999000,
                                                                     tzinfo=timezone.utc))
    assert json.loads(encode_handoff(PARSED.spec, latest))["source_available_at_ms"] == 253402300799999
    assert parse_handoff(encode_handoff(PARSED.spec, latest)).publication == latest


def test_maximum_typed_json_plus_lf_roundtrips_without_reducing_rust_domain() -> None:
    spec = replace(PARSED.spec, id="j" * 64)
    bins = tuple(ProfileBin(10**18 + i, Decimal("1E-28"), Decimal("1E-28"), 1) for i in range(398))
    content = replace(PARSED.publication.content, bins=bins)
    manifest = PARSED.publication.source_manifest.thaw()
    manifest["job_id"] = spec.id
    hours = cast(list[dict[str, Any]], manifest["hours"])
    for hour in hours:
        hour.update(aggregate_count=0, first_aggregate_id=None, last_aggregate_id=None)
    hours[0].update(aggregate_count=398, first_aggregate_id=10**11, last_aggregate_id=10**11 + 397)
    recon = PARSED.publication.reconciliation.thaw()
    recon.update(actual_aggregate_trade_count=398, official_constituent_trade_count=398)
    for name in ("base_volume", "quote_volume"):
        for prefix in ("actual_", "expected_"):
            recon[prefix + name] = canonical_decimal_text(getattr(content, name))
    publication = replace(PARSED.publication, content=content, source_manifest=CanonicalJsonObject(manifest),
                          reconciliation=CanonicalJsonObject(recon))
    framed = encode_handoff(spec, publication)
    assert len(framed[:-1]) == MAX_JSON_BYTES == 65536
    assert len(framed) == MAX_FRAMED_HANDOFF_BYTES == 65537
    parsed = parse_handoff(framed)
    assert parsed.spec == spec and parsed.publication == publication
    assert encode_handoff(parsed.spec, parsed.publication) == framed
    hours[0]["page_count"] = 10  # Exactly one more canonical JSON byte.
    with pytest.raises(ValueError):
        encode_handoff(spec, replace(publication, source_manifest=CanonicalJsonObject(manifest)))
    with pytest.raises(ValueError):
        parse_handoff(framed.replace(b'"page_count":1', b'"page_count":10', 1))


def test_exact_order_argv_and_only_retention_mutation(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, store, execute = setup(monkeypatch)
    events = MagicMock()
    events.attach_mock(store, "store")
    events.attach_mock(execute, "execute")
    assert owner.run(PARSED.spec, "/trusted/SECRET root") == cleanup.CleanupResult("COMPLETE", "DONE")
    args = ["--staging-root", "/trusted/SECRET root", "--job-id", PARSED.spec.id, "--start-ms", "0"]
    assert execute.call_args_list[0].args[0] == ["/trusted/assemble", *args]
    assert execute.call_args_list[1].args[0] == ["/trusted/cleanup", *args, "--expected-content-sha256",
                                               PARSED.publication.content_sha256, "--expected-handoff-sha256", DIGEST]
    assert [call[0] for call in events.mock_calls] == ["store.recover_completed_snapshot", "execute", "execute",
                                                    "store.mark_raw_deleted"]
    store.mark_raw_deleted.assert_called_once_with(PARSED.spec, PARSED.publication.content_sha256)


@pytest.mark.parametrize("present,status", [(False, "UNAVAILABLE"), (True, "RECOVERED_DONE")])
def test_missing_or_deleted_never_starts_process(monkeypatch: pytest.MonkeyPatch, present: bool, status: str) -> None:
    owner, store, execute = setup(monkeypatch)
    store.recover_completed_snapshot.return_value = DELETED if present else None
    assert owner.run(PARSED.spec, "/root").status == status
    execute.assert_not_called()
    store.mark_raw_deleted.assert_not_called()


@pytest.mark.parametrize("output", [b"SECRET", ENCODED + b"\n", RAW, b"{}"])
def test_successful_assembler_mismatch_never_falls_back(monkeypatch: pytest.MonkeyPatch, output: bytes) -> None:
    owner, store, execute = setup(monkeypatch)
    execute.side_effect = [output]
    with pytest.raises(cleanup.CleanupIntegrityError, match="^cleanup evidence verification failed$"):
        owner.run(PARSED.spec, "/SECRET")
    assert execute.call_count == 1
    store.mark_raw_deleted.assert_not_called()


@pytest.mark.parametrize("already", [False, True])
def test_execution_failure_permits_hash_authorized_first_or_resume(monkeypatch: pytest.MonkeyPatch, already: bool) -> None:
    owner, store, execute = setup(monkeypatch)
    execute.side_effect = [ingest.AssemblyFailure("bounded"), json.dumps(REPORT).encode()]
    store.mark_raw_deleted.return_value = RawRetentionResult(DELETED, already)
    assert owner.run(PARSED.spec, "/root").status == ("RECOVERED_DONE" if already else "COMPLETE")
    assert execute.call_count == 2 and execute.call_args.args[0][-1] == DIGEST


@pytest.mark.parametrize("report", [b"SECRET", b"[]", b"{}",
    ('{"reason":"COMPLETE",' + json.dumps(REPORT)[1:]).encode(),
    *[json.dumps({**REPORT, key: value}).encode() for key, value in
      [("reason", "FAILED"), ("hours", True), ("hours", 24.0), ("hours", float("inf")),
       ("referenced_pages", 25), ("referenced_pages", True), ("content_sha256", "a" * 64),
       ("handoff_sha256", "a" * 64), ("extra", 1)]],
])
def test_report_integrity_failure_never_marks_db(monkeypatch: pytest.MonkeyPatch, report: bytes) -> None:
    owner, store, execute = setup(monkeypatch)
    execute.side_effect = [ENCODED, report]
    with pytest.raises(cleanup.CleanupIntegrityError, match="^cleanup report verification failed$"):
        owner.run(PARSED.spec, "/SECRET")
    store.mark_raw_deleted.assert_not_called()


def test_cleanup_failure_and_database_ack_unknown_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, store, execute = setup(monkeypatch)
    execute.side_effect = [ENCODED, ingest.AssemblyFailure("bounded")]
    assert owner.run(PARSED.spec, "/root") == cleanup.CleanupResult("RETRYABLE", "LOCAL_PROCESS_FAILURE")
    store.mark_raw_deleted.assert_not_called()
    failure = DBAPIError("commit acknowledgment", None, Exception("injected"))
    for ack_lost in (False, True):
        execute.side_effect = [ENCODED, json.dumps(REPORT).encode()]
        store.mark_raw_deleted.side_effect = failure
        with pytest.raises(DBAPIError) as caught:
            owner.run(PARSED.spec, "/root")
        assert caught.value is failure
        store.mark_raw_deleted.side_effect = None
        if ack_lost:
            store.recover_completed_snapshot.return_value = DELETED
            execute.reset_mock()
            assert owner.run(PARSED.spec, "/root").status == "RECOVERED_DONE"
            execute.assert_not_called()
        else:
            execute.side_effect = [ingest.AssemblyFailure("raw deleted"), json.dumps(REPORT).encode()]
            assert owner.run(PARSED.spec, "/root").status == "COMPLETE"


def test_result_matrix_and_path_validation(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, store, execute = setup(monkeypatch)
    allowed = {("COMPLETE", "DONE"), ("RECOVERED_DONE", "DONE"), ("UNAVAILABLE", "NOT_DONE"),
               ("RETRYABLE", "LOCAL_PROCESS_FAILURE")}
    for status in ("COMPLETE", "RECOVERED_DONE", "UNAVAILABLE", "RETRYABLE", True):
        for reason in ("DONE", "NOT_DONE", "LOCAL_PROCESS_FAILURE", True):
            if (status, reason) in allowed:
                cleanup.CleanupResult(status, reason)  # type: ignore[arg-type]
            else:
                with pytest.raises(ValueError):
                    cleanup.CleanupResult(status, reason)  # type: ignore[arg-type]
    for path in ("relative", "/bad\x00path"):
        with pytest.raises(ValueError):
            owner.run(PARSED.spec, path)
        with pytest.raises(ValueError):
            cleanup.ProfileRawCleanupProcess(store, path, "/cleanup")
    store.recover_completed_snapshot.assert_not_called()
    execute.assert_not_called()


@pytest.mark.parametrize("code", ["import time;time.sleep(5)", "import sys;sys.stdout.write('x'*65538)",
                                  "import sys;sys.stderr.write('SECRET'*1000)", "raise SystemExit(7)"])
def test_real_cleanup_child_is_bounded_devnull_no_shell_and_reaped(tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
                                                                  code: str) -> None:
    helper = tmp_path / "SECRET helper"
    helper.write_text(f"#!{sys.executable}\n{code}\n")
    helper.chmod(0o700)
    children = []
    popen = subprocess.Popen

    def spawn(*args, **kwargs):
        assert kwargs["shell"] is False and kwargs["stdin"] == subprocess.DEVNULL
        child = popen(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(ingest.subprocess, "Popen", spawn)
    store = MagicMock()
    store.recover_completed_snapshot.return_value = RECOVERED
    owner = cleanup.ProfileRawCleanupProcess(store, str(tmp_path / "missing"), str(helper),
                                             ingest.IngestPolicy(timeout_ms=100))
    assert owner.run(PARSED.spec, str(tmp_path)) == cleanup.CleanupResult("RETRYABLE", "LOCAL_PROCESS_FAILURE")
    assert len(children) == 1 and children[0].poll() is not None
    store.mark_raw_deleted.assert_not_called()
