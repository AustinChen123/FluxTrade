"""One-shot raw cleanup, never a scheduler or filesystem concurrency protocol.

Caller MUST exclusively own the entire staging root and daily job family across
collection, fetch, assembly and cleanup. DB locks alone do not provide this.
"""
import hashlib
import json
import os
from dataclasses import dataclass
from typing import Literal

from .handoff import _keys, _pairs, _reject_number, encode_handoff
from .ingest import AssemblyFailure, IngestPolicy, _assemble
from .jobs import JobSpec, ProfileIngestJobStore


class CleanupIntegrityError(ValueError):
    """Local evidence mismatch; never includes paths, raw output or secrets."""


@dataclass(frozen=True, slots=True)
class CleanupResult:
    status: Literal["COMPLETE", "RECOVERED_DONE", "UNAVAILABLE", "RETRYABLE"]
    reason: Literal["DONE", "NOT_DONE", "LOCAL_PROCESS_FAILURE"]

    def __post_init__(self) -> None:
        if type(self.status) is not str or type(self.reason) is not str or (self.status, self.reason) not in (
            ("COMPLETE", "DONE"), ("RECOVERED_DONE", "DONE"),
            ("UNAVAILABLE", "NOT_DONE"), ("RETRYABLE", "LOCAL_PROCESS_FAILURE"),
        ):
            raise ValueError("invalid cleanup result")


def _path(value: str) -> None:
    if type(value) is not str or not os.path.isabs(value) or "\x00" in value:
        raise ValueError("invalid cleanup path")


class ProfileRawCleanupProcess:
    """Requires exclusive root/family ownership; does not acquire or attest it."""

    def __init__(self, store: ProfileIngestJobStore, assembler: str, cleanup: str,
                 policy: IngestPolicy = IngestPolicy()) -> None:
        _path(assembler)
        _path(cleanup)
        if type(policy) is not IngestPolicy:
            raise ValueError("invalid cleanup policy")
        self._store, self._assembler, self._cleanup, self._policy = store, assembler, cleanup, policy

    def run(self, spec: JobSpec, staging_root: str) -> CleanupResult:
        if type(spec) is not JobSpec:
            raise ValueError("invalid cleanup input")
        _path(staging_root)
        completed = self._store.recover_completed_snapshot(spec)
        if completed is None:
            return CleanupResult("UNAVAILABLE", "NOT_DONE")
        publication = completed.profile.publication
        if publication.raw_retention_state == "DELETED":
            return CleanupResult("RECOVERED_DONE", "DONE")
        expected = encode_handoff(spec, publication)
        digest = hashlib.sha256(expected).hexdigest()
        hours = publication.source_manifest.thaw()["hours"]
        assert type(hours) is list  # encode_handoff validates every hour and page count.
        pages = sum(hour["page_count"] for hour in hours)
        args = ["--staging-root", staging_root, "--job-id", spec.id, "--start-ms", str(spec.window_start_ms)]
        try:
            assembled = _assemble([self._assembler, *args], self._policy)
        except AssemblyFailure:
            pass  # Raw may already be deleted; Rust still verifies the full DB-derived SHA.
        else:
            if assembled != expected:
                raise CleanupIntegrityError("cleanup evidence verification failed")
        try:
            report = _assemble([self._cleanup, *args, "--expected-content-sha256", publication.content_sha256,
                                "--expected-handoff-sha256", digest], self._policy)
        except AssemblyFailure:
            return CleanupResult("RETRYABLE", "LOCAL_PROCESS_FAILURE")
        try:
            value = _keys(json.loads(report.decode("utf-8"), object_pairs_hook=_pairs,
                                    parse_float=_reject_number, parse_constant=_reject_number),
                          "reason hours referenced_pages content_sha256 handoff_sha256")
            if (value["reason"] != "COMPLETE" or type(value["hours"]) is not int or value["hours"] != 24
                    or type(value["referenced_pages"]) is not int or value["referenced_pages"] != pages
                    or value["content_sha256"] != publication.content_sha256 or value["handoff_sha256"] != digest):
                raise ValueError
        except (ValueError, TypeError, UnicodeError, RecursionError):
            raise CleanupIntegrityError("cleanup report verification failed") from None
        marked = self._store.mark_raw_deleted(spec, publication.content_sha256)
        return CleanupResult("RECOVERED_DONE" if marked.already_deleted else "COMPLETE", "DONE")
