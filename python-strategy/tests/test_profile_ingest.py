"""Offline process contracts; no PostgreSQL, network or trading integration."""
import json
import subprocess
import sys
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.core.market_data.profiles import ingest
from src.core.market_data.profiles.handoff import parse_handoff
from src.core.market_data.profiles.jobs import JobConflict, JobPublicationResult, JobState, LeaseLost
from src.core.market_data.profiles.publication import CanonicalJsonObject
from src.core.market_data.profiles.repository import PublishedProfile

wire = json.loads(json.loads((Path(__file__).parent / "fixtures/profile_handoff_v1.json").read_text())["wire_bytes"])
WIRE = json.dumps(wire).encode()
PARSED = parse_handoff(WIRE)
NOW = datetime(2026, 1, 1, tzinfo=timezone.utc)
DONE = JobState(PARSED.spec, "DONE", 1, CanonicalJsonObject({}), CanonicalJsonObject({}),
                None, None, None, None, None, PARSED.publication.content_sha256, NOW, NOW)
PROFILE = PublishedProfile(PARSED.publication.content_sha256, 1, PARSED.publication.content_sha256,
                           False, NOW, NOW, PARSED.publication)
PUBLISHED = JobPublicationResult(PROFILE, DONE, False)
RECOVERED = JobPublicationResult(replace(PROFILE, already_present=True), DONE, True)
RETRYABLE = replace(DONE, status="RETRYABLE", completed_snapshot_id=None, attempt=0)


def setup(monkeypatch: pytest.MonkeyPatch):
    store = MagicMock()
    store.get.return_value = None
    store.register.return_value = RETRYABLE
    store.claim.return_value = object()
    store.publish_and_complete.return_value = PUBLISHED
    store.recover_completed.return_value = RECOVERED
    assembler = MagicMock(return_value=WIRE)
    monkeypatch.setattr(ingest, "_assemble", assembler)
    owner = ingest.ProfileIngestProcess(store, "/trusted/assembler")
    return owner, store, assembler


def invoke(owner: ingest.ProfileIngestProcess):
    return owner.run(PARSED.spec, "/trusted/SECRET staging", "worker")


def test_exact_command_publication_and_done_restart(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, store, assembler = setup(monkeypatch)
    assert invoke(owner).status == "PUBLISHED"
    store.register.assert_called_once_with(PARSED.spec)
    argv, policy = assembler.call_args.args
    assert argv == ["/trusted/assembler", "--staging-root", "/trusted/SECRET staging", "--job-id", PARSED.spec.id,
                    "--start-ms", "0"]
    assert policy == ingest.IngestPolicy()
    store.claim.assert_called_once_with(PARSED.spec.id, "worker", policy.lease)
    store.publish_and_complete.assert_called_once_with(store.claim.return_value, PARSED.publication)
    failure = RuntimeError("commit acknowledgment lost")
    store.publish_and_complete.side_effect = failure
    with pytest.raises(RuntimeError) as caught:
        invoke(owner)
    assert caught.value is failure
    store.retry.assert_not_called()
    store.get.return_value = DONE
    store.reset_mock()
    result = invoke(owner)
    assert result.status == "RECOVERED"
    store.register.assert_not_called()
    store.retry.assert_not_called()
    store.claim.assert_not_called()
    store.publish_and_complete.assert_not_called()
    store.recover_completed.assert_called_once_with(PARSED.spec, PARSED.publication)


@pytest.mark.parametrize("status", ["RUNNING", "RETRYABLE", "FAILED"])
def test_unavailable_never_assembles_or_claims_another_job(monkeypatch: pytest.MonkeyPatch, status: str) -> None:
    owner, store, assembler = setup(monkeypatch)
    if status == "RUNNING":
        store.get.return_value = replace(RETRYABLE, status=status, attempt=1, lease_owner="worker", lease_expires_at=NOW)
    elif status == "FAILED":
        store.get.return_value = replace(RETRYABLE, status=status, attempt=1, last_error_code="FAILED", last_error_detail="bounded")
    store.claim.return_value = None
    assert invoke(owner).status == "UNAVAILABLE"
    assembler.assert_not_called()
    store.claim_next.assert_not_called()
    if status == "FAILED":
        store.claim.assert_not_called()
    else:
        assert store.claim.call_args.args[0] == PARSED.spec.id


@pytest.mark.parametrize("failure", ["subprocess", "parse", "spec"])
def test_failure_retries_only_live_claim_with_sanitized_details(monkeypatch: pytest.MonkeyPatch, failure: str) -> None:
    owner, store, assembler = setup(monkeypatch)
    if failure == "subprocess":
        assembler.side_effect = ingest.AssemblyFailure("SECRET stderr")
    elif failure == "parse":
        assembler.return_value = b"SECRET malformed"
    else:
        monkeypatch.setattr(ingest, "parse_handoff", lambda _: replace(PARSED, spec=replace(PARSED.spec, id="other")))
    assert invoke(owner).status == "RETRYABLE"
    assert store.retry.call_args.args[2:] == ("ASSEMBLY_FAILED", "offline assembly validation failed")
    store.publish_and_complete.assert_not_called()
    store.retry.side_effect = LeaseLost("job lease lost")
    with pytest.raises(LeaseLost):
        invoke(owner)
    store.get.return_value = DONE
    store.reset_mock()
    with pytest.raises(ingest.AssemblyFailure, match="^completed job assembly failed$"):
        invoke(owner)
    store.retry.assert_not_called()


def test_input_limits_reject_before_job_or_process(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, store, assembler = setup(monkeypatch)
    for root in ["relative", "/nul\x00root"]:
        with pytest.raises(ValueError):
            owner.run(PARSED.spec, root, "worker")
    store.register.assert_not_called()
    assembler.assert_not_called()
    for changes in ({"timeout_ms": 0}, {"stdout_bytes": 2097154}, {"stderr_bytes": True}):
        with pytest.raises(ValueError):
            replace(ingest.IngestPolicy(), **changes)


def test_removed_source_arguments_have_no_compatibility_path(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, store, assembler = setup(monkeypatch)
    with pytest.raises(TypeError):
        owner.run(PARSED.spec, "/trusted", b"raw", 86400000, "worker")  # type: ignore[call-arg]
    store.get.assert_not_called()
    assembler.assert_not_called()


def test_existing_spec_conflict_precedes_any_mutation(monkeypatch: pytest.MonkeyPatch) -> None:
    owner, store, assembler = setup(monkeypatch)
    store.get.return_value = replace(DONE, spec=replace(PARSED.spec, config_sha256="b" * 64))
    with pytest.raises(JobConflict):
        invoke(owner)
    store.register.assert_not_called()
    store.claim.assert_not_called()
    assembler.assert_not_called()


def test_result_state_matrix_requires_exact_frozen_publication() -> None:
    valid = {("PUBLISHED", "DONE", 0), ("ACK_RECOVERED", "DONE", 1), ("RECOVERED", "DONE", 1),
             ("UNAVAILABLE", "FAILED", 2), ("UNAVAILABLE", "BUSY_OR_NOT_DUE", 2),
             ("RETRYABLE", "ASSEMBLY_FAILED", 2)}
    for status in ("PUBLISHED", "ACK_RECOVERED", "RECOVERED", "UNAVAILABLE", "RETRYABLE", "BAD", True):
        for reason in ("DONE", "FAILED", "BUSY_OR_NOT_DUE", "ASSEMBLY_FAILED", True):
            for index, publication in enumerate((PUBLISHED, RECOVERED, None, MagicMock())):
                if (status, reason, index) in valid:
                    ingest.IngestResult(status, reason, publication)  # type: ignore[arg-type]
                else:
                    with pytest.raises(ValueError, match="^invalid ingest result$"):
                        ingest.IngestResult(status, reason, publication)  # type: ignore[arg-type]


def test_real_pipes_are_bounded_no_shell_and_child_is_reaped(monkeypatch: pytest.MonkeyPatch) -> None:
    real_popen = subprocess.Popen
    children = []

    def spawn(*args, **kwargs):
        assert kwargs["shell"] is False
        assert kwargs["stdin"] == subprocess.DEVNULL
        child = real_popen(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(ingest.subprocess, "Popen", spawn)
    policy = ingest.IngestPolicy(timeout_ms=100, stdout_bytes=8, stderr_bytes=8)
    assert ingest._assemble([sys.executable, "-c", "import sys;assert sys.stdin.buffer.read()==b'';sys.stdout.write('exact')"], policy) == b"exact"
    for code in ["import time;time.sleep(5)", "import sys;sys.stdout.write('x'*9)",
                 "import sys;sys.stderr.write('SECRET'*9)", "raise SystemExit(7)"]:
        with pytest.raises(ingest.AssemblyFailure) as caught:
            ingest._assemble([sys.executable, "-c", code], policy)
        assert "SECRET" not in str(caught.value)
        assert children[-1].poll() is not None


def test_real_stdout_accepts_full_framed_limit_and_reaps_one_over(monkeypatch: pytest.MonkeyPatch) -> None:
    real_popen = subprocess.Popen
    children = []

    def spawn(*args, **kwargs):
        child = real_popen(*args, **kwargs)
        children.append(child)
        return child

    monkeypatch.setattr(ingest.subprocess, "Popen", spawn)
    policy = ingest.IngestPolicy()
    assert policy.stdout_bytes == 2097153
    command = [sys.executable, "-c", "import sys;sys.stdout.buffer.write(b'x'*int(sys.argv[1]))"]
    assert ingest._assemble([*command, "2097153"], policy) == b"x" * 2097153
    assert children[-1].poll() is not None
    with pytest.raises(ingest.AssemblyFailure, match="^assembler output limit$"):
        ingest._assemble([*command, "2097154"], policy)
    assert children[-1].poll() is not None
