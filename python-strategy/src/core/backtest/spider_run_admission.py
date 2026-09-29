"""Read-only, anchored admission of the fixed Spider artifact bundle."""

import os as _os
import re as _re
import stat as _stat
from hashlib import sha256 as _sha256
from json import JSONDecodeError as _JSONDecodeError
from typing import Any as _Any

from src.core.backtest.spider_run_artifacts import canonical_bytes as _bytes, configuration_context as _validate_context, decode_jsonl as _decode_jsonl, validate_artifact as _artifact
from src.core.backtest.spider_run_completion_schema import _ARTIFACTS, completion as _completion, report as _report
from src.core.backtest.spider_run_envelope_schema import endpoint as _endpoint, journal as _journal
from src.core.backtest.spider_run_evidence import ReconciliationProjectionError as _ProjectionError, _context_chain, build_reconciliation as _build
from src.core.backtest.spider_run_reconciliation_schema import reconciliation as _reconciliation
from src.core.backtest.spider_scenario_plans import plan_bundle as _plan_bundle

_NAMES = tuple(name for name, _ in _ARTIFACTS)
_ALL = (*_NAMES, "completion.json")
_LIMIT = 16_777_216
_POLICY = "eb6ab34d8685fb59e286f5ffda8af24cbecf3e5ab2c729c585ccdca797cac336"
_DIAGNOSTIC = ("completion.json", "status.json", "journal.jsonl")
_FRONTIER = ("attempt.json", "status.json", "journal.jsonl", "completion.json")


class _Unsafe(Exception):
    pass


class _Oversize(Exception):
    pass


class _Invalid(Exception):
    pass


def _reject(reason: str, evidence: tuple[str, ...]) -> dict[str, _Any]:
    return dict(decision="REJECT", reason=reason, evidence=evidence)


def _regular(directory: int, name: str) -> None:
    if not _stat.S_ISREG(_os.stat(name, dir_fd=directory, follow_symlinks=False).st_mode):
        raise _Unsafe()


def _capture(directory: int, name: str) -> bytes:
    _regular(directory, name)
    descriptor = _os.open(name, _os.O_RDONLY | _os.O_NOFOLLOW | _os.O_NONBLOCK, dir_fd=directory)
    try:
        if not _stat.S_ISREG(_os.fstat(descriptor).st_mode):
            raise _Unsafe()
        chunks, count = [], 0
        while count <= _LIMIT:
            chunk = _os.read(descriptor, min(65536, _LIMIT + 1 - count))
            if not chunk:
                return b"".join(chunks)
            chunks.append(chunk)
            count += len(chunk)
        raise _Oversize()
    finally:
        _os.close(descriptor)


def _decode(raw: bytes, name: str) -> _Any:
    try:
        rows = _decode_jsonl(raw)
    except (_JSONDecodeError, UnicodeDecodeError) as error:
        raise _Invalid() from error
    except ValueError as error:
        if error.args != ("INVALID_ARTIFACT",):
            raise
        raise _Invalid() from error
    if name.endswith(".jsonl"):
        return rows
    if len(rows) != 1:
        raise _Invalid()
    return rows[0]


def _validate(validator: _Any, value: _Any) -> None:
    try:
        validator(value)
    except ValueError as error:
        if error.args != ("INVALID_ARTIFACT",):
            raise
        raise _Invalid() from error


def _run_id(value: _Any) -> bool:
    return type(value) is str and _re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", value) is not None


def _diagnose(directory: int) -> dict[str, _Any]:
    try:
        status = _decode(_capture(directory, "status.json"), "status.json")
        journal = _decode(_capture(directory, "journal.jsonl"), "journal.jsonl")
        _validate(_artifact, status)
        _validate(_journal, journal)
        if status["schema_version"] != "spider_status_v1":
            raise _Invalid()
        persisted = dict(ordinal=len(journal), journal_seq=len(journal), barrier_id=journal[-1]["barrier_id"]) if journal else None
        processed = status["processed_boundary"]
        if (status["schema_version"] == "spider_status_v1" and status["state"] == "FAILED"
                and status["failure_reason"] == "PERSISTENCE_FAILED" and status["persisted_boundary"] == persisted
                and processed is not None and processed["ordinal"] == processed["journal_seq"] > len(journal)
                and all(row["run_id"] == status["run_id"] for row in journal)):
            return _reject("INCOMPLETE_PERSISTENCE", _DIAGNOSTIC)
    except (OSError, _Unsafe, _Oversize, _Invalid):
        pass
    return _reject("MISSING_COMPLETE_MANIFEST", _DIAGNOSTIC)


def _admit(directory: int) -> dict[str, _Any]:
    try:
        raw = _capture(directory, "completion.json")
    except FileNotFoundError:
        return _diagnose(directory)
    except _Unsafe:
        return _reject("UNSAFE_ARTIFACT_PATH", ("completion.json",))
    except _Oversize:
        return _reject("ARTIFACT_MISMATCH", ("completion.json",))
    except OSError:
        return _reject("INVALID_MANIFEST", ("completion.json",))
    try:
        manifest = _decode(raw, "completion.json")
        _validate(_completion, manifest)
    except _Invalid:
        return _reject("INVALID_MANIFEST", ("completion.json",))
    unsafe = []
    for name in _NAMES:
        try:
            _regular(directory, name)
        except _Unsafe:
            unsafe.append(name)
        except OSError:
            pass
    if unsafe:
        return _reject("UNSAFE_ARTIFACT_PATH", tuple(unsafe))
    captured, bad = {}, []
    for name in _NAMES:
        try:
            captured[name] = _capture(directory, name)
        except _Unsafe:
            unsafe.append(name)
        except (OSError, _Oversize):
            bad.append(name)
    if unsafe:
        return _reject("UNSAFE_ARTIFACT_PATH", tuple(unsafe))
    values: dict[str, _Any] = {}
    for entry in manifest["artifacts"]:
        name = entry["path"]
        if name not in captured:
            continue
        data = captured[name]
        try:
            value = _decode(data, name)
            count = len(value) if name.endswith(".jsonl") else 1
            if len(data) != entry["byte_count"] or _sha256(data).hexdigest() != entry["sha256"] or count != entry["row_count"]:
                raise _Invalid()
            digest_field = {"endpoint.json": "endpoint_state_digest", "reconciliation.json": "reconciliation_digest"}.get(name)
            if digest_field and _sha256(_bytes(value)).hexdigest() != manifest[digest_field]:
                raise _Invalid()
            values[name] = value
        except _Invalid:
            bad.append(name)
    if bad:
        return _reject("ARTIFACT_MISMATCH", tuple(name for name in _NAMES if name in bad))
    for name, value in values.items():
        rows = value if name.endswith(".jsonl") else [value]
        if any(not _run_id(row.get("run_id")) or row["run_id"] != manifest["run_id"] for row in rows):
            bad.append(name)
    if bad:
        return _reject("RUN_ID_MISMATCH", tuple(bad))
    validators = (_artifact, _artifact, _journal, _endpoint, _reconciliation, _report)
    for (name, schema), validator in zip(_ARTIFACTS, validators, strict=True):
        value = values[name]
        rows = value if name.endswith(".jsonl") else [value]
        try:
            if any(row.get("schema_version") != schema for row in rows):
                raise _Invalid()
            if name == "report.jsonl" and values["attempt.json"].get("run_contract_id") == "SPIDER_SYNTHETIC_P2_CONFIGURED_RUN_V1":
                if not rows:
                    raise _Invalid()
                try:
                    report_context = _validate_context(rows[0].get("configuration_context"))
                    _report(value, context=report_context)
                except ValueError as error:
                    if error.args != ("INVALID_ARTIFACT",):
                        raise
                    raise _Invalid() from error
            else:
                _validate(validator, value)
        except _Invalid:
            bad.append(name)
    if bad:
        return _reject("UNSUPPORTED_SCHEMA", tuple(bad))
    attempt, status, journal, endpoint, supplied, report = (values[name] for name in _NAMES)
    try:
        _context_chain(attempt, status, journal, endpoint, supplied, report, manifest)
    except _ProjectionError:
        return _reject("ENDPOINT_RECONCILIATION_FAILED", _ALL)
    coverage = attempt["planned_coverage"]
    frontier = dict(ordinal=len(journal), journal_seq=len(journal), barrier_id=journal[-1]["barrier_id"]) if journal else None
    if (status["state"] != "COMPLETE" or not coverage or not journal or manifest["planned_coverage"] != coverage
            or coverage[-1]["ordinal"] != len(journal) or coverage[-1]["barrier_id"] != journal[-1]["barrier_id"]
            or any(manifest[key] != status[key] or status[key] != frontier for key in ("processed_boundary", "persisted_boundary"))):
        return _reject("INCOMPLETE_PERSISTENCE", _FRONTIER)
    if attempt["registration_state"] != "VALIDATED":
        return _reject("ENDPOINT_RECONCILIATION_FAILED", _ALL)
    try:
        selected = _plan_bundle(attempt["scenario_plan_id"])
    except ValueError as error:
        if error.args != ("UNSUPPORTED_CONFIGURATION",):
            raise
        return _reject("ENDPOINT_RECONCILIATION_FAILED", _ALL)
    hash_fields = ("scenario_plan_sha256", "program_sha256", "native_artifact_sha256", "policy_source_sha256")
    if (attempt["requested_scenario_selector"] != attempt["scenario_plan_id"]
            or any(entry["sha256"] != attempt[key] for entry, key in zip(attempt["input_contract_hashes"], hash_fields, strict=True))
            or attempt["policy_source_sha256"] != _POLICY or attempt["scenario_plan_sha256"] != selected["plan_sha256"]
            or manifest["input_contract_hashes"] != attempt["input_contract_hashes"] or manifest["terminal_reason"] != endpoint["terminal_reason"]):
        return _reject("ENDPOINT_RECONCILIATION_FAILED", _ALL)
    try:
        computed = _build(manifest["run_id"], attempt, status, journal, endpoint, report)
    except _ProjectionError:
        return _reject("ENDPOINT_RECONCILIATION_FAILED", _ALL)
    if computed["result"] != "OK" or any(row["result"] != "OK" for row in computed["checks"]) or _bytes(computed) != _bytes(supplied):
        return _reject("ENDPOINT_RECONCILIATION_FAILED", _ALL)
    values["completion.json"] = manifest
    return dict(decision="ACCEPT", artifacts=values)


def admit_spider_run(path: str | _os.PathLike[str]) -> dict[str, _Any]:
    """Admit captured bytes, never grant authority over future filesystem state."""
    try:
        name = _os.fspath(path)
    except TypeError:
        return _reject("UNSAFE_ARTIFACT_PATH", ("run-directory",))
    if not isinstance(name, str) or not name or "\0" in name or not name.rstrip(_os.sep):
        return _reject("UNSAFE_ARTIFACT_PATH", ("run-directory",))
    name = name.rstrip(_os.sep)
    try:
        if not _stat.S_ISDIR(_os.stat(name, follow_symlinks=False).st_mode):
            return _reject("UNSAFE_ARTIFACT_PATH", ("run-directory",))
        directory = _os.open(name, _os.O_RDONLY | _os.O_DIRECTORY | _os.O_NOFOLLOW | _os.O_NONBLOCK)
    except OSError:
        return _reject("UNSAFE_ARTIFACT_PATH", ("run-directory",))
    try:
        if not _stat.S_ISDIR(_os.fstat(directory).st_mode):
            return _reject("UNSAFE_ARTIFACT_PATH", ("run-directory",))
        return _admit(directory)
    finally:
        _os.close(directory)
