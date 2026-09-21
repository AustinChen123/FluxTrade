"""One-job offline publication. Source owner must persist/replay the exact response
bytes and first-observed timestamp pair. This owner neither fetches nor attests
provenance, schedules work, or deletes staging. Requires POSIX pipe selectors.
"""

import os
import hashlib
import selectors
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Literal

from .handoff import parse_handoff
from .jobs import JobConflict, JobPublicationResult, JobSpec, ProfileIngestJobStore, _safe


class AssemblyFailure(ValueError):
    """Stable local failure; never includes subprocess output or caller paths."""


@dataclass(frozen=True, slots=True)
class IngestPolicy:
    lease: timedelta = timedelta(minutes=5)
    retry_delay: timedelta = timedelta(minutes=1)
    timeout_ms: int = 30_000
    stdout_bytes: int = 65_536
    stderr_bytes: int = 4_096

    def __post_init__(self) -> None:
        for value in (self.lease, self.retry_delay):
            if type(value) is not timedelta or not timedelta(0) < value <= timedelta(days=1):
                raise ValueError("invalid ingest duration")
        for value, maximum in ((self.timeout_ms, 60_000), (self.stdout_bytes, 65_536), (self.stderr_bytes, 65_536)):
            if type(value) is not int or not 1 <= value <= maximum:
                raise ValueError("invalid ingest limit")


@dataclass(frozen=True, slots=True)
class IngestResult:
    status: Literal["PUBLISHED", "ACK_RECOVERED", "RECOVERED", "UNAVAILABLE", "RETRYABLE"]
    reason: Literal["DONE", "FAILED", "BUSY_OR_NOT_DUE", "ASSEMBLY_FAILED"]
    publication: JobPublicationResult | None = None

    def __post_init__(self) -> None:
        if type(self.status) is not str or type(self.reason) is not str:
            raise ValueError("invalid ingest result")
        if self.status in ("PUBLISHED", "ACK_RECOVERED", "RECOVERED"):
            if (self.reason != "DONE" or type(self.publication) is not JobPublicationResult
                    or self.publication.recovered_after_commit != (self.status != "PUBLISHED")):
                raise ValueError("invalid ingest result")
        elif (self.publication is not None
              or (self.status, self.reason) not in (("UNAVAILABLE", "FAILED"),
                  ("UNAVAILABLE", "BUSY_OR_NOT_DUE"), ("RETRYABLE", "ASSEMBLY_FAILED"))):
            raise ValueError("invalid ingest result")


def _assemble(argv: list[str], raw: bytes, policy: IngestPolicy) -> bytes:
    """Bound every pipe, including stdin; kill/reap the child on any failure."""
    try:
        with subprocess.Popen(argv, shell=False, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE) as child:
            assert child.stdin is not None and child.stdout is not None and child.stderr is not None
            try:
                deadline = time.monotonic() + policy.timeout_ms / 1000
                output, errors, sent = bytearray(), 0, 0
                with selectors.DefaultSelector() as selector:
                    for pipe, event in ((child.stdin, selectors.EVENT_WRITE),
                                        (child.stdout, selectors.EVENT_READ), (child.stderr, selectors.EVENT_READ)):
                        os.set_blocking(pipe.fileno(), False)
                        selector.register(pipe, event)
                    while selector.get_map():
                        remaining = deadline - time.monotonic()
                        if remaining <= 0:
                            raise AssemblyFailure("assembler timeout")
                        for key, _ in selector.select(remaining):
                            pipe = key.fileobj
                            if pipe is child.stdin:
                                if sent < len(raw):
                                    sent += os.write(key.fd, raw[sent:sent + 4096])
                                if sent == len(raw):
                                    selector.unregister(pipe)
                                    child.stdin.close()
                                continue
                            limit = policy.stdout_bytes - len(output) if pipe is child.stdout else policy.stderr_bytes - errors
                            chunk = os.read(key.fd, min(4096, limit + 1))
                            if len(chunk) > limit:
                                raise AssemblyFailure("assembler output limit")
                            if not chunk:
                                selector.unregister(pipe)
                            elif pipe is child.stdout:
                                output.extend(chunk)
                            else:
                                errors += len(chunk)
                    if child.wait(timeout=max(0, deadline - time.monotonic())) != 0:
                        raise AssemblyFailure("assembler failed")
                return bytes(output)
            finally:
                if child.poll() is None:
                    child.kill()
                child.wait()
    except (OSError, subprocess.TimeoutExpired):
        raise AssemblyFailure("assembler execution failed") from None


class ProfileIngestProcess:
    """Independent one-shot owner, not an Engine or scheduler integration."""

    def __init__(self, store: ProfileIngestJobStore, executable: str,
                 policy: IngestPolicy = IngestPolicy()) -> None:
        if type(executable) is not str or not os.path.isabs(executable) or "\x00" in executable:
            raise ValueError("invalid assembler executable")
        if type(policy) is not IngestPolicy:
            raise ValueError("invalid ingest policy")
        self._store, self._executable, self._policy = store, executable, policy

    def run(self, spec: JobSpec, staging_root: str, raw: bytes, source_available_at_ms: int,
            worker_id: str) -> IngestResult:
        if type(spec) is not JobSpec or type(raw) is not bytes or not 0 < len(raw) <= 65_536:
            raise ValueError("invalid ingest input")
        if type(staging_root) is not str or not os.path.isabs(staging_root) or "\x00" in staging_root:
            raise ValueError("invalid staging root")
        if type(source_available_at_ms) is not int or not spec.window_end_ms <= source_available_at_ms <= 253402300799999:
            raise ValueError("invalid source observation")
        _safe(worker_id, 128)
        state = self._store.get(spec.id)
        if state is None:
            state = self._store.register(spec)
        if state.spec != spec:
            raise JobConflict("job specification conflict")
        if state.status == "FAILED":
            return IngestResult("UNAVAILABLE", "FAILED")
        claim = None if state.status == "DONE" else self._store.claim(spec.id, worker_id, self._policy.lease)
        if claim is None and state.status != "DONE":
            return IngestResult("UNAVAILABLE", "BUSY_OR_NOT_DUE")
        argv = [self._executable, "--staging-root", staging_root, "--job-id", spec.id,
                "--start-ms", str(spec.window_start_ms), "--source-available-at-ms", str(source_available_at_ms)]
        try:
            parsed = parse_handoff(_assemble(argv, raw, self._policy))
            if parsed.spec != spec:
                raise AssemblyFailure("assembler identity mismatch")
            observed = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(milliseconds=source_available_at_ms)
            if (parsed.publication.source_available_at != observed
                    or parsed.publication.reconciliation.thaw()["response_sha256"] != hashlib.sha256(raw).hexdigest()):
                raise AssemblyFailure("assembler source mismatch")
        except (AssemblyFailure, ValueError):
            if claim is None:
                raise AssemblyFailure("completed job assembly failed") from None
            self._store.retry(claim, self._policy.retry_delay, "ASSEMBLY_FAILED", "offline assembly validation failed")
            return IngestResult("RETRYABLE", "ASSEMBLY_FAILED")
        if claim is None:
            recovered = self._store.recover_completed(spec, parsed.publication)
            if recovered is None:
                raise AssemblyFailure("completed job recovery unavailable")
            return IngestResult("RECOVERED", "DONE", recovered)
        published = self._store.publish_and_complete(claim, parsed.publication)
        return IngestResult("ACK_RECOVERED" if published.recovered_after_commit else "PUBLISHED", "DONE", published)
