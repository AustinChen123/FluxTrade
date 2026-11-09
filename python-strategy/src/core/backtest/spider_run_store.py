"""Single-writer Spider durability barriers; no runtime or financial owner."""

import os
import stat
from builtins import open as _open
from copy import deepcopy
from hashlib import sha256
from json import JSONDecodeError
from pathlib import Path
from typing import Any, cast

from src.core.backtest.spider_run_artifacts import _P3_RUN_CONTRACT, _boundary, _configuration_context, _historical_context, _require, _text, canonical_bytes, configuration_context, decode_jsonl, historical_context, validate_artifact
from src.core.backtest.spider_run_completion_schema import (
    _ARTIFACTS, _P3_V2_ARTIFACTS, completion, report as validate_report,
)
from src.core.backtest.spider_run_envelope_schema import endpoint as validate_endpoint, journal as validate_journal, journal_record
from src.core.backtest.spider_run_errors import ReconciliationProjectionError, SpiderRunStoreError
from src.core.backtest.spider_run_evidence import _context_chain, build_reconciliation
from src.core.backtest.spider_run_reconciliation_schema import reconciliation as validate_reconciliation
from src.core.backtest import spider_artifact_limits as artifact_limits
from src.core.backtest.spider_journal_reader_v2 import read_p3_v2_journal
from src.core.backtest.spider_journal_codec_v2 import encode_p3_v2_row_with_context_gate
from src.core.backtest.spider_projection_size import (
    canonical_jsonl_row_len,
    projected_jsonl_budget_ok,
)
from src.core.backtest.spider_historical_input import (
    ValidatedHistoricalInput,
    _historical_context_for_validated_input,
    _validate_historical_run_link,
    historical_context_for_input,
)
from src.core.backtest.spider_run_execution_schema import (
    _COMPLETION_ARTIFACTS as _EXECUTION_COMPLETION_ARTIFACTS,
    _GROSS_COMPLETION_ARTIFACTS,
    _FLOATING_COMPLETION_ARTIFACTS,
    _GROSS_SCHEMA as _GROSS_EXECUTION_SCHEMA,
    _SCHEMA as _EXECUTION_SCHEMA,
    canonical_gross_paid_bytes,
    canonical_paid_bytes,
    execution_context_from_input,
    _execution_context_for_input,
    gross_execution_context_from_input,
    _gross_execution_context_for_input,
    floating_exit_context_from_input,
    _floating_exit_context_for_input,
    validate_execution_artifact,
)
from src.core.backtest.spider_run_execution_evidence import build_execution_reconciliation
from src.core.backtest.spider_execution_input import (
    _decode_spider_execution_input_validated,
    decode_spider_execution_input,
)
from src.core.backtest.spider_floating_exit_input import (
    _decode_spider_floating_exit_input_validated,
    decode_spider_floating_exit_input,
)
from src.core.backtest.spider_gross_execution_input import (
    _decode_spider_gross_execution_input_validated,
    decode_spider_gross_execution_input,
)
from src.core.backtest.spider_run_artifacts import (
    _HASH_NAMES,
    configuration_context as validate_configuration_context,
    decode_canonical,
    execution_context as validate_execution_context,
    floating_exit_context as validate_floating_exit_context,
    gross_execution_context as validate_gross_execution_context,
    historical_context as validate_historical_context,
)


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


def _read_bounded(path: Path, limit: int) -> bytes:
    if type(limit) is not int or limit < 0:
        raise SpiderRunStoreError("INVALID_ARTIFACT")
    result = bytearray()
    try:
        with path.open("rb") as stream:
            while len(result) <= limit:
                remaining = limit + 1 - len(result)
                if remaining <= 0:
                    break
                chunk = stream.read(min(65_536, remaining))
                if not chunk:
                    break
                result.extend(chunk)
    except OSError as error:
        raise SpiderRunStoreError("PUBLICATION_FAILED") from error
    if len(result) > limit:
        raise SpiderRunStoreError("ARTIFACT_LIMIT_EXCEEDED")
    return bytes(result)


class SpiderRunStore:
    def __init__(self, path: Path, run_id: str):
        self._path, self._run_id, self._state = path, run_id, "CREATED"
        self._attempt: dict[str, Any] = {}
        self._processed: dict[str, Any] | None = None
        self._persisted: dict[str, Any] | None = None
        self._barrier_failed = False
        self._processed_kind: str | None = None
        self._historical_barrier_ids: set[str] = set()
        self._execution_context: dict[str, Any] | None = None
        self._execution_configuration: dict[str, Any] | None = None
        self._execution_historical: dict[str, Any] | None = None
        self._execution_input: bytes | None = None
        self._floating_exit = False
        self._gross_execution = False
        self._p3_v2_journal_metadata: dict[str, object] | None = None
        self._p3_v2_write_enabled = False
        self._p3_v2_codec_state: tuple[bytes, ...] = ()
        self._p3_v2_wire_bytes = 0
        self._p3_v2_kind_bytes: dict[str, int] = {}
        self._p3_v2_kind_rows: dict[str, int] = {}
        self._p3_v2_projected_bytes = 0
        self._p3_v2_definition_bytes = 0
        self._p3_v2_journal_hasher = sha256()

    def _is_execution(self) -> bool:
        return self._attempt.get("run_contract_id") in (
            "SPIDER_PAID_RESEARCH_RUN_V1", "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1",
            "SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1",
        )

    def _writes_p3_v2_journal(self) -> bool:
        return (
            self._p3_v2_write_enabled
            and self._attempt.get("run_contract_id") == _P3_RUN_CONTRACT
            and self._attempt.get("artifact_encoding") == "artifact_encoding_v2"
        )

    def _execution_schema(self) -> dict[str, str]:
        if self._gross_execution:
            return _GROSS_EXECUTION_SCHEMA
        if self._floating_exit:
            from src.core.backtest.spider_run_execution_schema import _FLOATING_SCHEMA
            return _FLOATING_SCHEMA
        return _EXECUTION_SCHEMA

    def _execution_completion_artifacts(self):
        if self._gross_execution:
            return _GROSS_COMPLETION_ARTIFACTS
        return _FLOATING_COMPLETION_ARTIFACTS if self._floating_exit else _EXECUTION_COMPLETION_ARTIFACTS

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
        if self._is_execution():
            assert self._execution_context is not None
            value = dict(
                schema_version=self._execution_schema()["status.json"], run_id=self._run_id, state=state,
                processed_boundary=deepcopy(self._processed), persisted_boundary=deepcopy(self._persisted),
                failure_reason=reason, primary_failure=deepcopy(primary),
                configuration_context=deepcopy(self._execution_configuration),
                historical_context=deepcopy(self._execution_historical),
                execution_context=deepcopy(self._execution_context),
            )
            validate_execution_artifact(
                "status.json", canonical_bytes(value) + b"\n", execution_context=self._execution_context,
            )
            return value
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
            _validate_historical_run_link(raw, attempt, self._run_id)
        except ValueError as error:
            if error.args == ("INVALID_ARTIFACT",):
                raise
            raise ValueError("INVALID_ARTIFACT") from error
        return raw

    def _execution_registration(self, attempt: dict[str, Any], raw: bytes, *,
                                floating_exit: bool = False,
                                gross_execution: bool = False) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        if type(raw) is not bytes:
            raise ValueError("INVALID_ARTIFACT")
        historical_token: ValidatedHistoricalInput | None = None
        if gross_execution:
            gross_request, historical_token, wrapper_hash, base_hash = (
                _decode_spider_gross_execution_input_validated(raw)
            )
            request = gross_request.execution_input
            execution = (
                _gross_execution_context_for_input(gross_request, wrapper_hash, base_hash)
                if historical_token is not None and base_hash is not None
                else gross_execution_context_from_input(raw)
            )
            context = validate_gross_execution_context(execution)
        elif floating_exit:
            floating_request, historical_token, wrapper_hash, base_hash = (
                _decode_spider_floating_exit_input_validated(raw)
            )
            request = floating_request.execution_input
            execution = (
                _floating_exit_context_for_input(floating_request, wrapper_hash, base_hash)
                if historical_token is not None and base_hash is not None
                else floating_exit_context_from_input(raw)
            )
            context = validate_floating_exit_context(execution)
        else:
            request, historical_token, wrapper_hash = _decode_spider_execution_input_validated(raw)
            execution = (
                _execution_context_for_input(request, wrapper_hash)
                if historical_token is not None else execution_context_from_input(raw)
            )
            context = validate_execution_context(execution)
        run = request.historical_input
        if historical_token is not None and historical_token.run is not run:
            raise ValueError("INVALID_ARTIFACT")
        if run.run_id != self._run_id:
            raise ValueError("INVALID_ARTIFACT")
        validate_execution_artifact("attempt.json", canonical_bytes(attempt) + b"\n", execution_context=execution)
        configuration_value = decode_canonical(run.configuration_bytes)
        if type(configuration_value) is not dict:
            raise ValueError("INVALID_ARTIFACT")
        configuration = validate_configuration_context(dict(
            schema_version="spider_configuration_context_v1",
            config_id=configuration_value["config_id"],
            configuration_sha256=run.configuration_sha256,
            products=list(run.ordered_products),
        ))._asdict() | {"products": list(run.ordered_products)}
        history_value = (
            _historical_context_for_validated_input(historical_token)
            if historical_token is not None else historical_context_for_input(run)
        )
        historical = validate_historical_context(history_value)._asdict()
        if (attempt["run_id"] != self._run_id
                or attempt["configuration_context"] != configuration
                or attempt["historical_context"] != historical
                or attempt["execution_context"] != (execution if gross_execution else context._asdict())
                or attempt["account_key"] != dict(
                    venue="SPIDER_HISTORICAL_RESEARCH", environment="RESEARCH_ONLY", account=run.account_key,
                )
                or attempt["profile_id"] != (
                    "SYNTHETIC_CONFIGURED_GROSS_ADMISSION_V1" if gross_execution
                    else "SYNTHETIC_CONFIGURED_MULTI_PRODUCT_V1"
                )
                or attempt["scenario_plan_sha256"] != wrapper_hash
                or attempt["policy_source_sha256"] != run.policy_source_sha256
                or [entry["name"] for entry in attempt["input_contract_hashes"]] != list(_HASH_NAMES)
                or [entry["sha256"] for entry in attempt["input_contract_hashes"]] != [
                    wrapper_hash, attempt["program_sha256"], attempt["native_artifact_sha256"],
                    attempt["policy_source_sha256"],
                ]):
            raise ValueError("INVALID_ARTIFACT")
        from src.core.backtest.spider_run_execution_evidence import (
            _expected_coverage,
            _expected_coverage_validated,
        )

        expected_coverage = (
            _expected_coverage_validated(request, wrapper_hash, historical_token)
            if historical_token is not None else _expected_coverage(request, wrapper_hash)
        )
        if attempt["planned_coverage"] != expected_coverage:
            raise ValueError("INVALID_ARTIFACT")
        return execution, configuration, historical

    def register(
        self, attempt: dict[str, Any], *, historical_input: bytes | None = None,
        execution_input: bytes | None = None, floating_exit_input: bytes | None = None,
        gross_execution_input: bytes | None = None,
    ) -> str:
        # Authoritative v2 guard is deliberately the first statement.
        if (type(attempt) is dict and attempt.get("artifact_encoding") == "artifact_encoding_v2"
                and historical_input is not None):
            try:
                artifact_limits.p3_v2_registration_preflight(attempt, historical_input)
            except ValueError as error:
                if error.args == ("ARTIFACT_LIMIT_EXCEEDED",):
                    raise SpiderRunStoreError("ARTIFACT_LIMIT_EXCEEDED") from error
                raise
        self._allowed("CREATED")
        floating_exit = floating_exit_input is not None
        gross_execution = gross_execution_input is not None
        if sum((floating_exit, gross_execution, execution_input is not None)) > 1:
            raise ValueError("INVALID_ARTIFACT")
        paid = execution_input is not None or floating_exit or gross_execution
        if paid:
            raw = (gross_execution_input if gross_execution else
                   floating_exit_input if floating_exit else execution_input)
            if historical_input is not None:
                raise ValueError("INVALID_ARTIFACT")
            if (type(attempt) is not dict
                    or attempt.get("schema_version") != (
                        _GROSS_EXECUTION_SCHEMA["attempt.json"] if gross_execution else
                        "spider_floating_exit_attempt_v1" if floating_exit else "spider_execution_attempt_v1"
                    )
                    or attempt.get("run_contract_id") != (
                        "SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1" if gross_execution else
                        "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1" if floating_exit else
                        "SPIDER_PAID_RESEARCH_RUN_V1"
                    )
                    or attempt.get("registration_state") != "VALIDATED"):
                raise ValueError("INVALID_ARTIFACT")
            try:
                execution_context, configuration_context, historical_context_value = self._execution_registration(
                    attempt, cast(bytes, raw), floating_exit=floating_exit,
                    gross_execution=gross_execution,
                )
            except (ValueError, KeyError, TypeError, IndexError) as error:
                raise ValueError("INVALID_ARTIFACT") from error
            self._execution_context = execution_context
            self._execution_configuration = configuration_context
            self._execution_historical = historical_context_value
            self._execution_input = cast(bytes, raw)
            self._floating_exit = floating_exit
            self._gross_execution = gross_execution
            self._publish("execution_input.json", cast(bytes, raw))
            self._publish("attempt.json", canonical_bytes(attempt) + b"\n")
            self._attempt = deepcopy(attempt)
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
        if historical_input is not None and attempt.get("run_contract_id") != _P3_RUN_CONTRACT:
            raise ValueError("INVALID_ARTIFACT")
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
        p3_v2_write = self._writes_p3_v2_journal()
        if p3_v2_write and record_kind == "DELIVERY_ATTEMPT":
            self._barrier_failed = True
            raise SpiderRunStoreError("CODEC_UNSUPPORTED_ROW")
        paid = self._is_execution()
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
        if paid:
            valid_kind = record_kind in (
                "SOURCE_GROUP_RESULT", "SNAPSHOT_FACT", "CALLBACK_RESULT", "HISTORICAL_MARKET_RESULT",
            )
            # Barrier identity is assigned by the actual journal producer, not
            # by the scheduler's planned-coverage row.
            invalid = (not valid_kind or boundary["ordinal"] != ordinal
                       or boundary["journal_seq"] != ordinal
                       or boundary["barrier_id"] in self._historical_barrier_ids)
        if self._barrier_failed or self._processed != self._persisted or invalid:
            raise RuntimeError("INVALID_STORE_OPERATION")
        self._processed = deepcopy(boundary)
        self._processed_kind = record_kind if historical or paid else None
        if historical or paid:
            try:
                self._publish("status.json", canonical_bytes(self._status("RUNNING")) + b"\n")
            except SpiderRunStoreError as error:
                self._barrier_failed = True
                reason = ("PUBLICATION_DURABILITY_UNKNOWN" if error.reason == "PUBLICATION_DURABILITY_UNKNOWN"
                          else "PERSISTENCE_FAILED")
                raise SpiderRunStoreError(
                    reason, "PERSISTENCE_FAILED", dict(kind="PERSISTENCE", reason=error.reason),
                ) from error

    def append_journal(self, row: dict[str, Any]) -> None:
        self._allowed("RUNNING")
        paid = self._is_execution()
        historical = self._attempt["run_contract_id"] == _P3_RUN_CONTRACT
        p3_v2_write = self._writes_p3_v2_journal()
        if p3_v2_write and type(row) is dict and row.get("record_kind") == "DELIVERY_ATTEMPT":
            self._barrier_failed = True
            raise SpiderRunStoreError("CODEC_UNSUPPORTED_ROW")
        try:
            journal_record(row, execution=self._execution_context if paid else None)
        except ValueError as error:
            if not p3_v2_write:
                raise
            self._barrier_failed = True
            raise SpiderRunStoreError(
                "PERSISTENCE_FAILED", "PERSISTENCE_FAILED",
                dict(kind="PERSISTENCE", reason="CODEC_UNSUPPORTED_ROW"),
            ) from error
        boundary = dict(ordinal=row["journal_seq"], journal_seq=row["journal_seq"], barrier_id=row["barrier_id"])
        expected_kind = (self._processed_kind if historical or paid
                         else self._attempt["planned_coverage"][boundary["ordinal"] - 1]["record_kind"])
        if (self._barrier_failed or self._processed is None or self._processed == self._persisted or boundary != self._processed
                or row["run_id"] != self._run_id or row["record_kind"] != expected_kind):
            raise RuntimeError("INVALID_STORE_OPERATION")
        try:
            if paid:
                if (row["record_kind"] == "DELIVERY_ATTEMPT" or row["execution_context"] != self._execution_context
                        or row["configuration_context"] != self._execution_configuration
                        or row["historical_context"] != self._execution_historical):
                    raise ReconciliationProjectionError()
                assert self._execution_context is not None
            else:
                _context_chain(self._attempt, row)
            if (historical and row["record_kind"] == "HISTORICAL_MARKET_RESULT"
                    and row["payload"]["account_key"] != self._attempt["account_key"]):
                raise ReconciliationProjectionError()
        except ReconciliationProjectionError as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        try:
            next_codec_state = self._p3_v2_codec_state
            next_kind_bytes = self._p3_v2_kind_bytes
            next_kind_rows = self._p3_v2_kind_rows
            next_projected_bytes = self._p3_v2_projected_bytes
            next_definition_bytes = self._p3_v2_definition_bytes
            if p3_v2_write:
                coverage: dict[str, int] = {}
                for item in self._attempt["planned_coverage"]:
                    kind = item["record_kind"]
                    coverage[kind] = coverage.get(kind, 0) + 1
                budget = cast(dict[str, Any], artifact_limits.p3_v2_scope_budget(coverage))
                if budget["refused"]:
                    raise SpiderRunStoreError("ARTIFACT_LIMIT_EXCEEDED")
                try:
                    encoded, next_codec_state = encode_p3_v2_row_with_context_gate(
                        row, self._p3_v2_codec_state,
                        validate_v1=lambda candidate: journal_record(candidate),
                        check_context_chain=lambda candidate: _context_chain(self._attempt, candidate),
                    )
                except ValueError as error:
                    raise SpiderRunStoreError("CODEC_UNSUPPORTED_ROW") from error
                raw = canonical_bytes(encoded) + b"\n"
                if len(raw) > artifact_limits.P3_V2_ROW_MAX:
                    raise SpiderRunStoreError("ARTIFACT_LIMIT_EXCEEDED")
                kind = row["record_kind"]
                next_kind_bytes = dict(self._p3_v2_kind_bytes)
                next_kind_rows = dict(self._p3_v2_kind_rows)
                next_kind_bytes[kind] = next_kind_bytes.get(kind, 0) + len(raw)
                next_kind_rows[kind] = next_kind_rows.get(kind, 0) + 1
                cap = budget["B_SG"] if kind == "SOURCE_GROUP_RESULT" else budget["kind_budgets"].get(kind)
                row_cap = budget["row_counts"].get(kind)
                previous_definition_count = len(self._p3_v2_codec_state)
                added_definitions = next_codec_state[previous_definition_count:]
                definition_bytes = self._p3_v2_definition_bytes + sum(
                    len(item) for item in added_definitions
                )
                next_row_projected_bytes = canonical_jsonl_row_len(row)
                next_projected_bytes += next_row_projected_bytes
                if (self._p3_v2_wire_bytes + len(raw) > budget["J2"]
                        or next_kind_bytes[kind] > cap
                        or (kind != "SOURCE_GROUP_RESULT" and next_kind_rows[kind] > row_cap)
                        or len(next_codec_state) > budget["table_max"]
                        or definition_bytes > budget["table_bytes_max"]
                        or not projected_jsonl_budget_ok(
                            wire_bytes=self._p3_v2_wire_bytes + len(raw),
                            projected_bytes=next_projected_bytes,
                            row_projected_bytes=next_row_projected_bytes,
                        )):
                    raise SpiderRunStoreError("ARTIFACT_LIMIT_EXCEEDED")
                if any(len(item) > artifact_limits.P3_V2_MAX_DEFINITION_BYTES for item in next_codec_state):
                    raise SpiderRunStoreError("ARTIFACT_LIMIT_EXCEEDED")
                next_definition_bytes = definition_bytes
            else:
                raw = canonical_bytes(row) + b"\n"
            _write(self._path / "journal.jsonl", raw, append=True)
            if p3_v2_write:
                self._p3_v2_codec_state = next_codec_state
                self._p3_v2_kind_bytes = next_kind_bytes
                self._p3_v2_kind_rows = next_kind_rows
                self._p3_v2_wire_bytes += len(raw)
                self._p3_v2_projected_bytes = next_projected_bytes
                self._p3_v2_definition_bytes = next_definition_bytes
                self._p3_v2_journal_hasher.update(raw)
                self._p3_v2_journal_metadata = dict(
                    schema_version="spider_journal_record_v2",
                    sha256=self._p3_v2_journal_hasher.hexdigest(),
                    byte_count=self._p3_v2_wire_bytes,
                    row_count=sum(self._p3_v2_kind_rows.values()),
                )
            if historical or paid:
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

    def publish_paid_diagnostic(self, endpoint: dict[str, Any]) -> None:
        """Persist a known paid DTO through the store's ordinary no-clobber path."""
        self._allowed("RUNNING", "FAILED")
        if not self._is_execution():
            raise RuntimeError("INVALID_STORE_OPERATION")
        assert self._execution_context is not None and self._execution_configuration is not None
        raw = (
            canonical_bytes(endpoint) + b"\n" if self._floating_exit else
            canonical_gross_paid_bytes(endpoint, self._execution_context, self._execution_configuration)
            if self._gross_execution else
            canonical_paid_bytes(endpoint, self._execution_context, self._execution_configuration)
        )
        if self._floating_exit:
            validate_execution_artifact("paid_endpoint.json", raw,
                                        execution_context=self._execution_context,
                                        configuration=self._execution_configuration)
        self._publish("paid_endpoint.json", raw)

    def _execution_reread(self, name: str, *, journal_rows: int | None = None) -> tuple[Any, dict[str, Any], bytes]:
        assert self._execution_context is not None
        try:
            path = self._path / name
            if path.is_symlink() or not path.is_file():
                raise OSError("invalid artifact")
            raw = path.read_bytes()
        except OSError as error:
            raise SpiderRunStoreError("PUBLICATION_FAILED") from error
        try:
            value = validate_execution_artifact(
                name, raw, execution_context=self._execution_context,
                configuration=self._execution_configuration,
                historical=self._execution_historical, journal_rows=journal_rows,
            )
            if name == "execution_input.json":
                request = (
                    decode_spider_gross_execution_input(raw).execution_input if self._gross_execution else
                    decode_spider_floating_exit_input(raw).execution_input if self._floating_exit else
                    decode_spider_execution_input(raw)
                )
                if request.historical_input.run_id != self._run_id:
                    raise ValueError("INVALID_ARTIFACT")
                rows = [value]
            elif name.endswith(".jsonl"):
                rows = decode_jsonl(raw)
            else:
                rows = decode_jsonl(raw)
                if len(rows) != 1:
                    raise ValueError("INVALID_ARTIFACT")
            if name not in ("execution_input.json", "paid_endpoint.json") and any(
                    cast(dict[str, Any], row).get("run_id") != self._run_id for row in rows):
                raise ValueError("INVALID_ARTIFACT")
        except (JSONDecodeError, UnicodeDecodeError, ValueError) as error:
            if isinstance(error, ValueError) and error.args != ("INVALID_ARTIFACT",):
                raise
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        metadata = dict(
            path=name, schema_version=self._execution_schema()[name], sha256=sha256(raw).hexdigest(),
            byte_count=len(raw), row_count=1 if name == "execution_input.json" else len(rows),
        )
        return value, metadata, raw

    def _finalize_execution(self, endpoint: dict[str, Any], reconciliation: dict[str, Any], report: list) -> str:
        self._allowed("RUNNING")
        if self._barrier_failed or self._processed != self._persisted:
            raise RuntimeError("INVALID_STORE_OPERATION")
        assert (self._execution_context is not None and self._execution_configuration is not None
                and self._execution_historical is not None and self._execution_input is not None)
        if type(report) is not list or len(report) != 1:
            raise ValueError("INVALID_ARTIFACT")
        report_raw = b"".join(canonical_bytes(row) + b"\n" for row in report)
        endpoint_raw = (
            canonical_bytes(endpoint) + b"\n" if self._floating_exit else
            canonical_gross_paid_bytes(endpoint, self._execution_context, self._execution_configuration)
            if self._gross_execution else
            canonical_paid_bytes(endpoint, self._execution_context, self._execution_configuration)
        )
        if self._floating_exit:
            validate_execution_artifact("paid_endpoint.json", endpoint_raw,
                                        execution_context=self._execution_context,
                                        configuration=self._execution_configuration)
        recon_raw = canonical_bytes(reconciliation) + b"\n"
        validate_execution_artifact(
            "reconciliation.json", recon_raw, execution_context=self._execution_context,
            journal_rows=self._persisted["journal_seq"] if self._persisted is not None else 0,
        )
        validate_execution_artifact(
            "report.jsonl", report_raw, execution_context=self._execution_context,
            configuration=self._execution_configuration,
        )
        for name, raw in (("paid_endpoint.json", endpoint_raw), ("reconciliation.json", recon_raw),
                          ("report.jsonl", report_raw)):
            self._publish(name, raw)

        completion_artifacts = self._execution_completion_artifacts()
        reread_names = [name for name, _, _ in completion_artifacts]
        reread: dict[str, Any] = {}
        metadata: list[dict[str, Any]] = []
        artifacts: dict[str, bytes] = {}
        journal_count = self._persisted["journal_seq"] if self._persisted is not None else 0
        for name in reread_names:
            value, entry, raw = self._execution_reread(name, journal_rows=journal_count)
            reread[name] = value
            metadata.append(entry)
            artifacts[name] = raw
        computed = build_execution_reconciliation(self._run_id, artifacts | {"reconciliation.json": recon_raw})
        if computed["result"] != "OK" or canonical_bytes(computed) != canonical_bytes(reconciliation):
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")

        self._publish("status.json", canonical_bytes(self._status("COMPLETE")) + b"\n")
        complete_status, status_metadata, status_raw = self._execution_reread("status.json", journal_rows=journal_count)
        artifacts["status.json"] = status_raw
        metadata[1] = status_metadata
        computed = build_execution_reconciliation(self._run_id, artifacts | {"reconciliation.json": recon_raw})
        if computed["result"] != "OK" or canonical_bytes(computed) != canonical_bytes(reconciliation):
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        assert self._execution_configuration is not None and self._execution_historical is not None
        manifest = dict(
            schema_version=self._execution_schema()["completion.json"], run_id=self._run_id, state="COMPLETE",
            terminal_reason="PAID_CLOSE_RESEARCH_V1",
            input_contract_hashes=self._attempt["input_contract_hashes"],
            planned_coverage=self._attempt["planned_coverage"],
            processed_boundary=self._processed, persisted_boundary=self._persisted,
            endpoint_state_digest=sha256(canonical_bytes(endpoint)).hexdigest(),
            reconciliation_digest=sha256(canonical_bytes(reconciliation)).hexdigest(),
            artifacts=metadata,
            configuration_context=deepcopy(self._execution_configuration),
            historical_context=deepcopy(self._execution_historical),
            execution_context=deepcopy(self._execution_context),
            endpoint_status=endpoint["status"], evidence_scope=reconciliation["evidence_scope"],
        )
        completion_raw = canonical_bytes(manifest) + b"\n"
        validate_execution_artifact("completion.json", completion_raw, execution_context=self._execution_context)
        artifacts["completion.json"] = completion_raw
        completed_reconciliation = build_execution_reconciliation(self._run_id, artifacts | {"reconciliation.json": recon_raw})
        if completed_reconciliation["result"] != "OK" or canonical_bytes(completed_reconciliation) != canonical_bytes(reconciliation):
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        self._publish("completion.json", completion_raw)
        return "COMPLETE_PUBLISHED"

    def _reread(self, name: str, validator: Any, *, raw: bytes | None = None) -> tuple[Any, dict[str, Any]]:
        try:
            path = self._path / name
            if path.is_symlink() or not path.is_file():
                raise OSError("invalid artifact")
            if raw is None:
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

    def _reread_artifact(
        self, name: str, validator: Any, *, attempt: dict[str, Any] | None = None,
    ) -> tuple[Any, dict[str, Any]]:
        """Dispatch one artifact read by the registered P3 wire encoding."""
        p3_v2 = (
            self._attempt.get("run_contract_id") == _P3_RUN_CONTRACT
            and self._attempt.get("artifact_encoding") == "artifact_encoding_v2"
        )
        if not p3_v2:
            return self._reread(name, validator)
        if name == "attempt.json":
            def validate_p3_v2_attempt(value: object) -> None:
                if type(value) is not dict or value.get("artifact_encoding") != "artifact_encoding_v2":
                    raise ValueError("INVALID_ARTIFACT")
                normalized = value.copy()
                normalized["artifact_encoding"] = "artifact_encoding_v1"
                validate_artifact(normalized)

            reread = self._reread(
                name, validate_p3_v2_attempt,
                raw=self._read_p3_v2_artifact(name),
            )
            if reread[0] != self._attempt:
                raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
            return reread
        if name != "journal.jsonl":
            return self._reread(
                name, validator,
                raw=self._read_p3_v2_artifact(name),
            )
        if attempt is None or attempt != self._attempt or self._p3_v2_journal_metadata is None:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        if attempt.get("artifact_encoding") != "artifact_encoding_v2":
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        journal_path = self._path / name
        if journal_path.is_symlink() or not journal_path.is_file():
            raise SpiderRunStoreError("PUBLICATION_FAILED")
        coverage: dict[str, int] = {}
        for entry in attempt["planned_coverage"]:
            kind = entry["record_kind"]
            coverage[kind] = coverage.get(kind, 0) + 1
        try:
            result = read_p3_v2_journal(
                self._path / name,
                expected_metadata=self._p3_v2_journal_metadata,
                contexts=(attempt["configuration_context"], attempt["historical_context"]),
                budget=artifact_limits.p3_v2_scope_budget(coverage),
            )
            try:
                validator(result.rows)
            except ValueError as error:
                if error.args == ("INVALID_ARTIFACT",):
                    raise SpiderRunStoreError("ARTIFACT_MISMATCH") from error
                raise
            if any(row["run_id"] != self._run_id for row in result.rows):
                raise SpiderRunStoreError("ARTIFACT_MISMATCH")
        except OSError as error:
            raise SpiderRunStoreError("PUBLICATION_FAILED") from error
        except ValueError as error:
            reason = (
                "ARTIFACT_LIMIT_EXCEEDED"
                if error.args == ("ARTIFACT_LIMIT_EXCEEDED",)
                else "ENDPOINT_RECONCILIATION_FAILED"
            )
            raise SpiderRunStoreError(reason) from error
        return result.rows, dict(path=name, **result.wire_metadata)

    def _historical_input_metadata(self, attempt: dict[str, Any]) -> dict[str, Any]:
        path = self._path / "historical_input.json"
        try:
            if path.is_symlink() or not path.is_file():
                raise OSError("invalid historical input target")
            if attempt.get("artifact_encoding") == "artifact_encoding_v2":
                raw = _read_bounded(path, self._p3_v2_artifact_limit("historical_input.json"))
            else:
                raw = path.read_bytes()
        except OSError as error:
            raise SpiderRunStoreError("PUBLICATION_FAILED") from error
        try:
            _validate_historical_run_link(raw, attempt, self._run_id)
        except ValueError as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        return dict(path="historical_input.json", schema_version="spider_historical_research_run_v1",
                    sha256=sha256(raw).hexdigest(), byte_count=len(raw), row_count=1)

    def _p3_v2_artifact_limit(self, name: str) -> int:
        if name == "endpoint.json" or name == "reconciliation.json":
            coverage: dict[str, int] = {}
            for entry in self._attempt["planned_coverage"]:
                kind = entry["record_kind"]
                coverage[kind] = coverage.get(kind, 0) + 1
            budget = artifact_limits.p3_v2_scope_budget(coverage)
            limit = budget["E2" if name == "endpoint.json" else "X2"]
        else:
            limit = artifact_limits.artifact_limit("P3", "v2", name)
        if type(limit) is not int or limit < 0:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        return limit

    def _p3_v2_value_metadata(self, name: str, value: Any) -> dict[str, Any]:
        limit = self._p3_v2_artifact_limit(name)
        rows = value if name.endswith(".jsonl") else (value,)
        digest = sha256()
        byte_count = 0
        row_count = 0
        for row in rows:
            raw = canonical_bytes(row) + b"\n"
            byte_count += len(raw)
            if byte_count > limit:
                raise SpiderRunStoreError("ARTIFACT_LIMIT_EXCEEDED")
            digest.update(raw)
            row_count += 1
        schema = dict(_P3_V2_ARTIFACTS)[name]
        return dict(path=name, schema_version=schema, sha256=digest.hexdigest(),
                    byte_count=byte_count, row_count=row_count)

    def _p3_v2_completion_bytes(
        self, attempt: dict[str, Any], endpoint: dict[str, Any], reconciliation: dict[str, Any],
        metadata: list[dict[str, Any]],
    ) -> bytes:
        manifest = dict(
            schema_version="spider_completion_v1", run_id=self._run_id, state="COMPLETE",
            terminal_reason=endpoint["terminal_reason"],
            input_contract_hashes=attempt["input_contract_hashes"],
            planned_coverage=attempt["planned_coverage"],
            processed_boundary=self._processed, persisted_boundary=self._persisted,
            endpoint_state_digest=sha256(canonical_bytes(endpoint)).hexdigest(),
            reconciliation_digest=sha256(canonical_bytes(reconciliation)).hexdigest(),
            artifacts=metadata,
        )
        if "configuration_context" in attempt:
            manifest["configuration_context"] = _configuration_context(attempt["configuration_context"])
        if "historical_context" in attempt:
            manifest["historical_context"] = _historical_context(attempt["historical_context"])
        completion(manifest)
        raw = canonical_bytes(manifest) + b"\n"
        if len(raw) > self._p3_v2_artifact_limit("completion.json"):
            raise SpiderRunStoreError("ARTIFACT_LIMIT_EXCEEDED")
        return raw

    def _read_p3_v2_artifact(self, name: str) -> bytes:
        path = self._path / name
        if path.is_symlink() or not path.is_file():
            raise SpiderRunStoreError("PUBLICATION_FAILED")
        return _read_bounded(path, self._p3_v2_artifact_limit(name))

    def finalize(self, endpoint: dict, reconciliation: dict, report: list) -> str:
        self._allowed("RUNNING")
        if self._is_execution():
            return self._finalize_execution(endpoint, reconciliation, report)
        historical = self._attempt.get("run_contract_id") == _P3_RUN_CONTRACT
        frontier_ready = (self._persisted is not None and self._processed == self._persisted
                          and self._persisted["ordinal"] == self._persisted["journal_seq"] > 0)
        if not historical:
            frontier_ready = (frontier_ready and self._persisted is not None
                              and self._persisted["ordinal"] == len(self._attempt["planned_coverage"]))
        if self._barrier_failed or not frontier_ready:
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
        p3_v2 = self._attempt.get("artifact_encoding") == "artifact_encoding_v2"
        preflight_completion: bytes | None = None
        complete_status_raw: bytes | None = None
        try:
            current_attempt, attempt_metadata = self._reread_artifact("attempt.json", validate_artifact)
            current_status = self._reread_artifact("status.json", validate_artifact)[0]
            if p3_v2:
                complete_status_raw = canonical_bytes(self._status("COMPLETE")) + b"\n"
                if len(complete_status_raw) > self._p3_v2_artifact_limit("status.json"):
                    raise SpiderRunStoreError("ARTIFACT_LIMIT_EXCEEDED")
            current_journal, first_journal_metadata = self._reread_artifact(
                "journal.jsonl", validate_journal, attempt=current_attempt,
            )
            _context_chain(current_attempt, current_status, current_journal, endpoint, reconciliation, report)
            historical_metadata = self._historical_input_metadata(current_attempt) if historical else None
            del current_journal
            if p3_v2:
                preflight_metadata = [
                    attempt_metadata,
                    self._p3_v2_value_metadata("status.json", self._status("COMPLETE")),
                    first_journal_metadata,
                    self._p3_v2_value_metadata("endpoint.json", endpoint),
                    self._p3_v2_value_metadata("reconciliation.json", reconciliation),
                    self._p3_v2_value_metadata("report.jsonl", report),
                ]
                if historical_metadata is not None:
                    preflight_metadata.append(historical_metadata)
                preflight_completion = self._p3_v2_completion_bytes(
                    current_attempt, endpoint, reconciliation, preflight_metadata,
                )
        except ReconciliationProjectionError as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        for name, value in [("endpoint.json", endpoint), ("reconciliation.json", reconciliation), ("report.jsonl", report)]:
            rows = value if name.endswith(".jsonl") else [value]
            self._publish(name, b"".join(canonical_bytes(row) + b"\n" for row in rows))
        validators = (validate_artifact, validate_artifact, validate_journal, validate_endpoint, validate_reconciliation)
        reread: list[tuple[Any, dict[str, Any]]] = []
        for (name, _), validator in zip(_ARTIFACTS[:-1], validators, strict=True):
            reread.append(
                self._reread_artifact(
                    name, validator, attempt=reread[0][0] if name == "journal.jsonl" else None,
                )
            )
        reread.append(self._reread_artifact("report.jsonl", _report_validator(reread[0][0])))
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
        if self._attempt.get("artifact_encoding") == "artifact_encoding_v2":
            expected_wire_metadata = dict(path="journal.jsonl", **(self._p3_v2_journal_metadata or {}))
            if reread[2][1] != expected_wire_metadata or first_journal_metadata != reread[2][1]:
                raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        del journal
        reread[2] = (None, reread[2][1])
        actual_complete_status_raw = canonical_bytes(self._status("COMPLETE")) + b"\n"
        if p3_v2 and actual_complete_status_raw != complete_status_raw:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        self._publish("status.json", actual_complete_status_raw)
        complete_status, status_metadata = self._reread_artifact("status.json", validate_artifact)
        if complete_status != self._status("COMPLETE"):
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        metadata = [entry for _, entry in reread]
        metadata[1] = status_metadata
        if historical_metadata is not None:
            metadata.append(historical_metadata)
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
            _context_chain(attempt, complete_status, final, supplied, rows, manifest)
        except ReconciliationProjectionError as error:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED") from error
        completion_raw = canonical_bytes(manifest) + b"\n"
        if p3_v2 and completion_raw != preflight_completion:
            raise SpiderRunStoreError("ENDPOINT_RECONCILIATION_FAILED")
        self._publish("completion.json", completion_raw)
        return "COMPLETE_PUBLISHED"
