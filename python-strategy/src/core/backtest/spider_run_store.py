"""Single-writer Spider durability barriers; no runtime or financial owner."""

import os
import stat
from builtins import open as _open
from copy import deepcopy
from hashlib import sha256
from json import JSONDecodeError
from pathlib import Path
from typing import Any

from src.core.backtest.spider_run_artifacts import _HASHES, _P3_RUN_CONTRACT, _boundary, _configuration_context, _historical_context, _require, _text, canonical_bytes, configuration_context, decode_jsonl, historical_context, validate_artifact
from src.core.backtest.spider_run_completion_schema import _ARTIFACTS, completion, report as validate_report
from src.core.backtest.spider_run_envelope_schema import endpoint as validate_endpoint, journal as validate_journal, journal_record
from src.core.backtest.spider_run_evidence import ReconciliationProjectionError, _context_chain, build_reconciliation
from src.core.backtest.spider_run_reconciliation_schema import reconciliation as validate_reconciliation
from src.core.backtest.spider_historical_input import (
    HistoricalRunInput, decode_historical_run_input, decode_p2_configuration,
    encode_historical_run_input, historical_context_for_input, historical_planned_coverage,
    validate_historical_input,
)


class SpiderRunStoreError(Exception):
    def __init__(self, reason: str, requested_failure_reason: str | None = None, primary_failure: dict | None = None):
        super().__init__(reason)
        self.reason = reason
        self.requested_failure_reason = requested_failure_reason
        self.primary_failure = deepcopy(primary_failure)


def _report_validator(attempt: dict[str, Any]):
    def validate(value: list[dict[str, Any]]) -> None:
        context = None
        historical = None
        if attempt.get("run_contract_id") == "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1":
            _require(type(value) is list and bool(value) and type(value[0]) is dict)
            context = configuration_context(value[0].get("configuration_context"))
        elif attempt.get("run_contract_id") == "SPIDER_HISTORICAL_RESEARCH_RUN_V1":
            context = configuration_context(attempt["configuration_context"])
            historical = historical_context(attempt["historical_context"])
        validate_report(value, context=context, historical_context=historical)
    return validate


def _sync(directory: Path) -> None:
    try:
        descriptor = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    except OSError as error:
        raise SpiderRunStoreError("PUBLICATION_DURABILITY_UNKNOWN") from error


def _write(path: Path, raw: bytes, append: bool = False) -> None:
    try:
        flags = os.O_WRONLY | os.O_NOFOLLOW | os.O_NONBLOCK | (os.O_APPEND if append else os.O_CREAT | os.O_EXCL)
        descriptor = os.open(path, flags, 0o600)
        try:
            with _open(descriptor, "ab" if append else "wb", closefd=False) as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise OSError("not an exclusive regular file")
                if stream.write(raw) != len(raw):
                    raise OSError("short write")
                stream.flush()
                os.fsync(stream.fileno())
        finally:
            os.close(descriptor)
    except OSError as error:
        raise SpiderRunStoreError("ARTIFACT_WRITE_FAILED") from error


class SpiderRunStore:
    def __init__(self, path: Path, run_id: str):
        self._path, self._run_id, self._state = path, run_id, "CREATED"
        self._attempt: dict[str, Any] = {}
        self._processed: dict[str, Any] | None = None
        self._persisted: dict[str, Any] | None = None
        self._barrier_failed = False
        self._processed_kind: str | None = None
        self._historical_barrier_ids: set[str] = set()

    @classmethod
    def create(cls, output_root: str, run_id: str) -> "SpiderRunStore":
        _text(run_id, r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
        _text(output_root)
        root = Path(output_root)
        try:
            if root.is_symlink() or not root.is_dir():
                raise OSError("invalid output root")
            path = root / run_id
            path.mkdir()
        except OSError as error:
            raise SpiderRunStoreError("PUBLICATION_FAILED") from error
        _sync(root)
        return cls(path, run_id)

    def _allowed(self, *states: str) -> None:
        if self._state not in states:
            raise RuntimeError("INVALID_STORE_OPERATION")

    def _publish(self, name: str, raw: bytes) -> None:
        temporary, final = self._path / ("." + name + ".tmp"), self._path / name
        _write(temporary, raw)
        try:
            if name == "status.json":
                if final.is_symlink() or (final.exists() and (not final.is_file() or final.stat().st_nlink != 1)):
                    raise OSError("invalid status target")
                os.replace(temporary, final)
            else:
                os.link(temporary, final)
                if name == "completion.json":
                    self._state = "FROZEN"
                temporary.unlink()
        except OSError as error:
            raise SpiderRunStoreError("PUBLICATION_FAILED") from error
        _sync(self._path)

    def _status(self, state: str, reason: str | None = None, primary: dict | None = None) -> dict[str, Any]:
        value = dict(schema_version="spider_status_v1", run_id=self._run_id, state=state,
                     processed_boundary=deepcopy(self._processed), persisted_boundary=deepcopy(self._persisted),
                     failure_reason=reason, primary_failure=deepcopy(primary))
        if "configuration_context" in self._attempt:
            value["configuration_context"] = _configuration_context(self._attempt["configuration_context"])
        if "historical_context" in self._attempt:
            value["historical_context"] = _historical_context(self._attempt["historical_context"])
        validate_artifact(value)
        return value

    def _historical_input(self, attempt: dict[str, Any], raw: bytes | None) -> bytes | None:
        historical = attempt.get("run_contract_id") == _P3_RUN_CONTRACT
        if not historical:
            if raw is not None:
                raise ValueError("INVALID_ARTIFACT")
            return None
        if type(raw) is not bytes:
            raise ValueError("INVALID_ARTIFACT")
        try:
            run = decode_historical_run_input(raw)
            if type(run) is not HistoricalRunInput or encode_historical_run_input(run) != raw:
                raise ValueError("INVALID_ARTIFACT")
            if run.run_id != self._run_id or attempt["run_id"] != run.run_id:
                raise ValueError("INVALID_ARTIFACT")
            history = historical_context(attempt["historical_context"])
            expected_history = historical_context_for_input(run)
            configuration = decode_p2_configuration(
                run.configuration_bytes, run.configuration_sha256, run.ordered_products
            )
            expected_configuration = configuration_context(dict(
                schema_version="spider_configuration_context_v1",
                config_id=configuration["config_id"],
                configuration_sha256=run.configuration_sha256,
                products=list(run.ordered_products),
            ))
            semantic_hash = validate_historical_input(run)
            if (history != historical_context(expected_history)
                    or configuration_context(attempt["configuration_context"]) != expected_configuration
                    or attempt["historical_context"] != expected_history
                    or attempt["policy_source_sha256"] != run.policy_source_sha256
                    or any(entry["sha256"] != attempt[key]
                           for entry, key in zip(attempt["input_contract_hashes"], _HASHES, strict=True))
                    or attempt["scenario_plan_sha256"] != semantic_hash
                    or attempt["input_contract_hashes"][0]["sha256"] != semantic_hash
                    or attempt["account_key"].get("account") != run.account_key
                    or attempt["planned_coverage"] != historical_planned_coverage(run)):
                raise ValueError("INVALID_ARTIFACT")
        except ValueError as error:
            if error.args == ("INVALID_ARTIFACT",):
                raise
            raise ValueError("INVALID_ARTIFACT") from error
        return raw

    def register(self, attempt: dict[str, Any], *, historical_input: bytes | None = None) -> str:
        self._allowed("CREATED")
        validate_artifact(attempt)
        if attempt["schema_version"] != "spider_attempt_v1" or attempt["run_id"] != self._run_id:
            raise ValueError("INVALID_ARTIFACT")
        historical_input = self._historical_input(attempt, historical_input)
        if historical_input is not None:
            self._publish("historical_input.json", historical_input)
        self._publish("attempt.json", canonical_bytes(attempt) + b"\n")
        self._attempt = deepcopy(attempt)
        if attempt["registration_state"] == "REJECTED":
            self.publish_failure("UNSUPPORTED_CONFIGURATION", None)
            return "REGISTRATION_REJECTED"
        self._publish("status.json", canonical_bytes(self._status("RUNNING")) + b"\n")
        self._state = "RUNNING"
        try:
            _write(self._path / "journal.jsonl", b"")
            _sync(self._path)
        except SpiderRunStoreError as error:
            primary = dict(kind="PERSISTENCE", reason=error.reason)
            self.publish_failure("PERSISTENCE_FAILED", primary)
            raise SpiderRunStoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED", primary) from error
        return "NATIVE_CONSTRUCTION_ALLOWED"

    def mark_processed(self, boundary: dict[str, Any], *, record_kind: str | None = None) -> None:
        self._allowed("RUNNING")
        _boundary(boundary)
        ordinal = self._persisted["ordinal"] + 1 if self._persisted else 1
        historical = self._attempt["run_contract_id"] == _P3_RUN_CONTRACT
        coverage = self._attempt["planned_coverage"]
        if historical:
            valid_kind = record_kind in ("SOURCE_GROUP_RESULT", "SNAPSHOT_FACT", "DELIVERY_ATTEMPT",
                                         "CALLBACK_RESULT", "HISTORICAL_MARKET_RESULT")
            invalid = (not valid_kind or boundary["ordinal"] != ordinal or boundary["journal_seq"] != ordinal
                       or boundary["barrier_id"] in self._historical_barrier_ids)
        else:
            invalid = (record_kind is not None or ordinal > len(coverage)
                       or boundary != dict(ordinal=ordinal, journal_seq=ordinal,
                                           barrier_id=coverage[ordinal - 1]["barrier_id"]))
        if self._barrier_failed or self._processed != self._persisted or invalid:
            raise RuntimeError("INVALID_STORE_OPERATION")
        self._processed = deepcopy(boundary)
        self._processed_kind = record_kind if historical else None

    def append_journal(self, row: dict[str, Any]) -> None:
        self._allowed("RUNNING")
        journal_record(row)
        boundary = dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"], barrier_id=row["barrier_id"])
        historical = self._attempt["run_contract_id"] == _P3_RUN_CONTRACT
        expected_kind = self._processed_kind if historical else self._attempt["planned_coverage"][boundary["ordinal"] - 1]["record_kind"]
        if (self._barrier_failed or self._processed is None or self._processed == self._persisted or boundary != self._processed
                or row["run_id"] != self._run_id or row["record_kind"] != expected_kind):
            raise RuntimeError("INVALID_STORE_OPERATION")
        try:
            _context_chain(self._attempt, row)
            if (historical and row["record_kind"] == "HISTORICAL_MARKET_RESULT"
                    and row["payload"]["account_key"] != self._attempt["account_key"]):
                raise ReconciliationProjectionError()
        except ReconciliationProjectionError as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        try:
            _write(self._path / "journal.jsonl", canonical_bytes(row) + b"\n", append=True)
            if historical:
                self._historical_barrier_ids.add(boundary["barrier_id"])
            self._persisted = deepcopy(self._processed)
            self._publish("status.json", canonical_bytes(self._status("RUNNING")) + b"\n")
            self._processed_kind = None
        except SpiderRunStoreError as error:
            self._barrier_failed = True
            raise SpiderRunStoreError("PERSISTENCE_FAILED", "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason=error.reason)) from error

    def publish_failure(self, reason: str, primary_failure: dict | None) -> None:
        self._allowed("CREATED", "RUNNING")
        if not self._attempt:
            raise RuntimeError("INVALID_STORE_OPERATION")
        value = self._status("FAILED", reason, primary_failure)
        try:
            self._publish("status.json", canonical_bytes(value) + b"\n")
        except SpiderRunStoreError as error:
            self._barrier_failed = True
            raise SpiderRunStoreError(error.reason, reason, primary_failure) from error
        self._state = "FAILED"

    def _reread(self, name: str, validator: Any) -> tuple[Any, dict[str, Any]]:
        try:
            path = self._path / name
            if path.is_symlink() or not path.is_file():
                raise OSError("invalid artifact")
            raw = path.read_bytes()
        except OSError as error:
            raise SpiderRunStoreError("PUBLICATION_FAILED") from error
        try:
            rows = decode_jsonl(raw)
            if not name.endswith(".jsonl") and len(rows) != 1:
                raise ValueError("INVALID_ARTIFACT")
            value = rows if name.endswith(".jsonl") else rows[0]
            validator(value)
            if any(row["run_id"] != self._run_id for row in rows):
                raise ValueError("INVALID_ARTIFACT")
        except (JSONDecodeError, UnicodeDecodeError) as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        except ValueError as error:
            if error.args != ("INVALID_ARTIFACT",):
                raise
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        schema = dict(_ARTIFACTS)[name]
        if any(row["schema_version"] != schema for row in rows):
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        return value, dict(path=name, schema_version=schema, sha256=sha256(raw).hexdigest(), byte_count=len(raw), row_count=len(rows))

    def finalize(self, endpoint: dict, reconciliation: dict, report: list) -> str:
        self._allowed("RUNNING")
        if self._attempt.get("run_contract_id") == _P3_RUN_CONTRACT:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        if self._barrier_failed or self._processed != self._persisted or self._persisted is None or self._persisted["ordinal"] != len(self._attempt["planned_coverage"]):
            raise RuntimeError("INVALID_STORE_OPERATION")
        validate_endpoint(endpoint)
        validate_reconciliation(reconciliation)
        candidate_report_validator = _report_validator(self._attempt)
        candidate_report_validator(report)
        for name, value in (("endpoint.json", endpoint), ("reconciliation.json", reconciliation)):
            if any(row["run_id"] != self._run_id for row in [value]):
                raise ValueError("INVALID_ARTIFACT")
        if any(row["run_id"] != self._run_id for row in report):
            raise ValueError("INVALID_ARTIFACT")
        try:
            current = [self._reread(name, validator)[0] for name, validator in (
                ("attempt.json", validate_artifact), ("status.json", validate_artifact), ("journal.jsonl", validate_journal))]
            _context_chain(current[0], current[1], current[2], endpoint, reconciliation, report)
        except ReconciliationProjectionError as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        for name, value in [("endpoint.json", endpoint), ("reconciliation.json", reconciliation), ("report.jsonl", report)]:
            rows = value if name.endswith(".jsonl") else [value]
            self._publish(name, b"".join(canonical_bytes(row) + b"\n" for row in rows))
        validators = (validate_artifact, validate_artifact, validate_journal, validate_endpoint, validate_reconciliation)
        reread = [self._reread(name, validator) for (name, _), validator in zip(_ARTIFACTS[:-1], validators, strict=True)]
        reread.append(self._reread("report.jsonl", _report_validator(reread[0][0])))
        attempt, status, journal, final, supplied, rows = (value for value, _ in reread)
        try:
            _context_chain(attempt, status, journal, final, supplied, rows)
        except ReconciliationProjectionError as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        try:
            computed = build_reconciliation(self._run_id, attempt, status, journal, final, rows)
        except ReconciliationProjectionError as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        if computed["result"] != "OK" or canonical_bytes(computed) != canonical_bytes(supplied):
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        self._publish("status.json", canonical_bytes(self._status("COMPLETE")) + b"\n")
        complete_status, status_metadata = self._reread("status.json", validate_artifact)
        if complete_status != self._status("COMPLETE"):
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        metadata = [entry for _, entry in reread]
        metadata[1] = status_metadata
        manifest = dict(schema_version="spider_completion_v1", run_id=self._run_id, state="COMPLETE", terminal_reason=final["terminal_reason"],
                        input_contract_hashes=attempt["input_contract_hashes"], planned_coverage=attempt["planned_coverage"],
                        processed_boundary=self._processed, persisted_boundary=self._persisted,
                        endpoint_state_digest=sha256(canonical_bytes(final)).hexdigest(), reconciliation_digest=sha256(canonical_bytes(supplied)).hexdigest(), artifacts=metadata)
        if "configuration_context" in attempt:
            manifest["configuration_context"] = _configuration_context(attempt["configuration_context"])
        if "historical_context" in attempt:
            manifest["historical_context"] = _historical_context(attempt["historical_context"])
        completion(manifest)
        try:
            _context_chain(attempt, complete_status, journal, final, supplied, rows, manifest)
        except ReconciliationProjectionError as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        self._publish("completion.json", canonical_bytes(manifest) + b"\n")
        return "COMPLETE_PUBLISHED"
