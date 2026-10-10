"""Read-only canonical projections of admitted historical Spider runs."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from hashlib import sha256
from os import PathLike
from pathlib import Path
from typing import Any, Literal, cast

from src.core.backtest.spider_run_admission import admit_spider_run
from src.core.backtest.spider_run_artifacts import canonical_bytes
from src.core.backtest import spider_scenario_run

_PROJECTION_SCHEMA = "spider_p02_projection_v1"
_P3_CONTRACT = "SPIDER_HISTORICAL_RESEARCH_RUN_V1"
_REPORT_CHECK = "REPORT_PROJECTION_MATCH"
_EXECUTION_PROJECTION_SCHEMA = "spider_execution_projection_v1"
_FLOATING_EXIT_PROJECTION_SCHEMA = "spider_floating_exit_projection_v1"
_GROSS_EXECUTION_PROJECTION_SCHEMA = "spider_gross_execution_projection_v1"
_EXECUTION_RESULT_STATUSES = (
    "PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED",
)


class SpiderProjectionIntegrityError(ValueError):
    """An already-admitted artifact bundle could not be projected intact."""

    def __init__(self) -> None:
        super().__init__("FORMAL_PROJECTION_INTEGRITY_FAILED")


@dataclass(frozen=True, slots=True)
class SpiderP02Projection:
    """Immutable canonical P02 bytes and their content identity."""

    canonical_json: bytes
    sha256: str

    def __post_init__(self) -> None:
        if type(self.canonical_json) is not bytes or sha256(self.canonical_json).hexdigest() != self.sha256:
            raise SpiderProjectionIntegrityError

    @property
    def value(self) -> dict[str, object]:
        """Return a fresh decoded copy so callers cannot mutate the projection."""
        try:
            decoded = json.loads(self.canonical_json)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise SpiderProjectionIntegrityError from error
        if type(decoded) is not dict:
            raise SpiderProjectionIntegrityError
        return cast(dict[str, object], decoded)

    @property
    def report_rows(self) -> list[dict[str, object]]:
        """Return normalized report rows as detached mutable copies."""
        rows = self.value["report_rows"]
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise SpiderProjectionIntegrityError
        return cast(list[dict[str, object]], rows)


@dataclass(frozen=True, slots=True)
class SpiderFormalRunResult:
    """Typed immutable result contract shared by the future runner wrappers."""

    runner_kind: Literal["full", "research"]
    run_id: str
    outcome: Literal["ADMITTED", "REJECTED", "FAILED", "DURABILITY_UNKNOWN"]
    reason: str | None
    admission: Literal["ACCEPT"] | None
    projection: SpiderP02Projection | None
    _report_rows_json: bytes | None = field(default=None, repr=False)

    @property
    def schema_version(self) -> str:
        return "spider_formal_run_result_v1"

    @property
    def report_rows(self) -> list[dict[str, object]] | None:
        if self._report_rows_json is None:
            return None
        rows = json.loads(self._report_rows_json)
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise SpiderProjectionIntegrityError
        return cast(list[dict[str, object]], rows)


@dataclass(frozen=True, slots=True)
class SpiderExecutionProjection:
    """Immutable canonical paid execution projection and content identity."""

    canonical_json: bytes
    sha256: str

    def __post_init__(self) -> None:
        if type(self.canonical_json) is not bytes or sha256(self.canonical_json).hexdigest() != self.sha256:
            raise SpiderProjectionIntegrityError

    @property
    def value(self) -> dict[str, object]:
        try:
            decoded = json.loads(self.canonical_json)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise SpiderProjectionIntegrityError from error
        if type(decoded) is not dict:
            raise SpiderProjectionIntegrityError
        return cast(dict[str, object], decoded)

    @property
    def report_rows(self) -> list[dict[str, object]]:
        rows = self.value["report_rows"]
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise SpiderProjectionIntegrityError
        return cast(list[dict[str, object]], rows)


@dataclass(frozen=True, slots=True)
class SpiderExecutionFormalResult:
    """Typed, detached result for an admitted paid execution research run."""

    runner_kind: Literal["full", "research"]
    run_id: str
    outcome: Literal["ADMITTED", "REJECTED", "FAILED", "DURABILITY_UNKNOWN"]
    reason: str | None
    admission: Literal["ACCEPT"] | None
    endpoint_status: Literal[
        "PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED",
    ] | None
    projection: SpiderExecutionProjection | None
    _report_rows_json: bytes | None = field(default=None, repr=False)

    @property
    def schema_version(self) -> str:
        return "spider_execution_formal_result_v1"

    @property
    def report_rows(self) -> list[dict[str, object]] | None:
        if self._report_rows_json is None:
            return None
        rows = json.loads(self._report_rows_json)
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise SpiderProjectionIntegrityError
        return cast(list[dict[str, object]], rows)


@dataclass(frozen=True, slots=True)
class SpiderFloatingExitProjection:
    """Immutable canonical candidate projection and content identity."""

    canonical_json: bytes
    sha256: str

    def __post_init__(self) -> None:
        if type(self.canonical_json) is not bytes or sha256(self.canonical_json).hexdigest() != self.sha256:
            raise SpiderProjectionIntegrityError

    @property
    def value(self) -> dict[str, object]:
        try:
            decoded = json.loads(self.canonical_json)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise SpiderProjectionIntegrityError from error
        if type(decoded) is not dict:
            raise SpiderProjectionIntegrityError
        return cast(dict[str, object], decoded)

    @property
    def report_rows(self) -> list[dict[str, object]]:
        rows = self.value["report_rows"]
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise SpiderProjectionIntegrityError
        return cast(list[dict[str, object]], rows)


@dataclass(frozen=True, slots=True)
class SpiderFloatingExitFormalResult:
    """Typed, detached result for an admitted source-derived exit run."""

    runner_kind: Literal["full", "research"]
    run_id: str
    outcome: Literal["ADMITTED", "REJECTED", "FAILED", "DURABILITY_UNKNOWN"]
    reason: str | None
    admission: Literal["ACCEPT"] | None
    endpoint_status: Literal[
        "PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED",
    ] | None
    projection: SpiderFloatingExitProjection | None
    _report_rows_json: bytes | None = field(default=None, repr=False)

    @property
    def schema_version(self) -> str:
        return "spider_floating_exit_formal_result_v1"

    @property
    def report_rows(self) -> list[dict[str, object]] | None:
        if self._report_rows_json is None:
            return None
        rows = json.loads(self._report_rows_json)
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise SpiderProjectionIntegrityError
        return cast(list[dict[str, object]], rows)


@dataclass(frozen=True, slots=True)
class SpiderGrossExecutionProjection:
    """Immutable canonical admitted gross execution projection."""

    canonical_json: bytes
    sha256: str

    def __post_init__(self) -> None:
        if type(self.canonical_json) is not bytes or sha256(self.canonical_json).hexdigest() != self.sha256:
            raise SpiderProjectionIntegrityError

    @property
    def value(self) -> dict[str, object]:
        try:
            decoded = json.loads(self.canonical_json)
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise SpiderProjectionIntegrityError from error
        if type(decoded) is not dict:
            raise SpiderProjectionIntegrityError
        return cast(dict[str, object], decoded)

    @property
    def report_rows(self) -> list[dict[str, object]]:
        rows = self.value["report_rows"]
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise SpiderProjectionIntegrityError
        return cast(list[dict[str, object]], rows)


@dataclass(frozen=True, slots=True)
class SpiderGrossExecutionFormalResult:
    """Typed immutable result for the closed gross formal family."""

    runner_kind: Literal["full", "research"]
    run_id: str
    outcome: Literal["ADMITTED", "REJECTED", "FAILED", "DURABILITY_UNKNOWN"]
    reason: str | None
    admission: Literal["ACCEPT"] | None
    endpoint_status: Literal[
        "PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED",
    ] | None
    projection: SpiderGrossExecutionProjection | None
    _report_rows_json: bytes | None = field(default=None, repr=False)

    @property
    def schema_version(self) -> str:
        return "spider_gross_execution_formal_result_v1"

    @property
    def report_rows(self) -> list[dict[str, object]] | None:
        if self._report_rows_json is None:
            return None
        rows = json.loads(self._report_rows_json)
        if type(rows) is not list or any(type(row) is not dict for row in rows):
            raise SpiderProjectionIntegrityError
        return cast(list[dict[str, object]], rows)


def _without_run_id(value: object) -> object:
    if type(value) is dict:
        return {
            key: _without_run_id(item)
            for key, item in cast(dict[str, object], value).items()
            if key != "run_id"
        }
    if type(value) is list:
        return [_without_run_id(item) for item in cast(list[object], value)]
    return value


def _normalize_report_check(
    reconciliation: dict[str, object], report_digest: str,
) -> dict[str, object]:
    normalized = cast(dict[str, object], _without_run_id(reconciliation))
    checks = normalized["checks"]
    if type(checks) is not list:
        raise SpiderProjectionIntegrityError
    matches = 0
    for item in checks:
        if type(item) is not dict or item.get("name") != _REPORT_CHECK:
            continue
        check = cast(dict[str, object], item)
        for side in ("expected", "observed"):
            evidence = check.get(side)
            if type(evidence) is not dict:
                raise SpiderProjectionIntegrityError
            expected = cast(dict[str, object], evidence)
            if type(expected.get("report_sha256")) is not str:
                raise SpiderProjectionIntegrityError
            expected["report_sha256"] = report_digest
        matches += 1
    if matches != 1:
        raise SpiderProjectionIntegrityError
    return normalized


def _projection_value(artifacts: dict[str, Any]) -> dict[str, object]:
    attempt = artifacts["attempt.json"]
    status = artifacts["status.json"]
    journal = artifacts["journal.jsonl"]
    endpoint = artifacts["endpoint.json"]
    reconciliation = artifacts["reconciliation.json"]
    report_rows = artifacts["report.jsonl"]
    completion = artifacts["completion.json"]
    historical_input = artifacts["historical_input.json"]

    normalized_reports = cast(list[dict[str, object]], _without_run_id(report_rows))
    report_digest = sha256(canonical_bytes(normalized_reports)).hexdigest()
    normalized_endpoint = cast(dict[str, object], _without_run_id(endpoint))
    normalized_reconciliation = _normalize_report_check(reconciliation, report_digest)
    normalized_completion = cast(dict[str, object], _without_run_id(completion))
    normalized_artifacts = normalized_completion["artifacts"]
    if type(normalized_artifacts) is not list:
        raise SpiderProjectionIntegrityError
    for item in normalized_artifacts:
        if type(item) is not dict:
            raise SpiderProjectionIntegrityError
        entry = cast(dict[str, object], item)
        # These captured-file metadata values are run-ID-sensitive by contract.
        entry.pop("sha256", None)
        entry.pop("byte_count", None)
    normalized_completion["endpoint_state_digest"] = normalized_endpoint
    normalized_completion["reconciliation_digest"] = normalized_reconciliation

    attempt_row = cast(dict[str, object], _without_run_id(attempt))
    if attempt_row.get("run_contract_id") != _P3_CONTRACT:
        raise SpiderProjectionIntegrityError
    return {
        "schema_version": _PROJECTION_SCHEMA,
        "attempt": attempt_row,
        "status": _without_run_id(status),
        "journal": _without_run_id(journal),
        "endpoint": normalized_endpoint,
        "report_rows": normalized_reports,
        "reconciliation": normalized_reconciliation,
        "completion": normalized_completion,
        "historical_input": _without_run_id(historical_input),
    }


def _projection_from_admitted_artifacts(artifacts: object) -> SpiderP02Projection:
    if type(artifacts) is not dict:
        raise SpiderProjectionIntegrityError
    try:
        value = _projection_value(cast(dict[str, Any], artifacts))
        encoded = canonical_bytes(value)
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, SpiderProjectionIntegrityError):
            raise
        raise SpiderProjectionIntegrityError from error
    return SpiderP02Projection(encoded, sha256(encoded).hexdigest())


def _execution_projection_value(artifacts: dict[str, Any]) -> dict[str, object]:
    attempt = artifacts["attempt.json"]
    status = artifacts["status.json"]
    journal = artifacts["journal.jsonl"]
    execution_input = artifacts["execution_input.json"]
    paid_endpoint = artifacts["paid_endpoint.json"]
    report_rows = artifacts["report.jsonl"]
    reconciliation = artifacts["reconciliation.json"]
    completion = artifacts["completion.json"]

    normalized_endpoint = cast(dict[str, object], _without_run_id(paid_endpoint))
    normalized_reconciliation = cast(dict[str, object], _without_run_id(reconciliation))
    normalized_completion = cast(dict[str, object], _without_run_id(completion))
    completion_artifacts = normalized_completion["artifacts"]
    if type(completion_artifacts) is not list:
        raise SpiderProjectionIntegrityError
    for item in completion_artifacts:
        if type(item) is not dict:
            raise SpiderProjectionIntegrityError
        entry = cast(dict[str, object], item)
        entry.pop("sha256", None)
        entry.pop("byte_count", None)
    normalized_completion["endpoint_state_digest"] = normalized_endpoint
    normalized_completion["reconciliation_digest"] = normalized_reconciliation

    return {
        "schema_version": _EXECUTION_PROJECTION_SCHEMA,
        "attempt": _without_run_id(attempt),
        "status": _without_run_id(status),
        "journal": _without_run_id(journal),
        "execution_input": _without_run_id(execution_input),
        "paid_endpoint": normalized_endpoint,
        "report_rows": _without_run_id(report_rows),
        "reconciliation": normalized_reconciliation,
        "completion": normalized_completion,
    }


def _execution_projection_from_admitted_artifacts(artifacts: object) -> SpiderExecutionProjection:
    if type(artifacts) is not dict:
        raise SpiderProjectionIntegrityError
    try:
        value = _execution_projection_value(cast(dict[str, Any], artifacts))
        encoded = canonical_bytes(value)
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, SpiderProjectionIntegrityError):
            raise
        raise SpiderProjectionIntegrityError from error
    return SpiderExecutionProjection(encoded, sha256(encoded).hexdigest())


def _floating_exit_projection_value(artifacts: dict[str, Any]) -> dict[str, object]:
    attempt = artifacts["attempt.json"]
    status = artifacts["status.json"]
    journal = artifacts["journal.jsonl"]
    execution_input = artifacts["execution_input.json"]
    paid_endpoint = artifacts["paid_endpoint.json"]
    report_rows = artifacts["report.jsonl"]
    reconciliation = artifacts["reconciliation.json"]
    completion = artifacts["completion.json"]

    attempt_row = cast(dict[str, object], _without_run_id(attempt))
    if attempt_row.get("run_contract_id") != "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1":
        raise SpiderProjectionIntegrityError
    normalized_endpoint = cast(dict[str, object], _without_run_id(paid_endpoint))
    normalized_reconciliation = cast(dict[str, object], _without_run_id(reconciliation))
    normalized_completion = cast(dict[str, object], _without_run_id(completion))
    completion_artifacts = normalized_completion["artifacts"]
    if type(completion_artifacts) is not list:
        raise SpiderProjectionIntegrityError
    for item in completion_artifacts:
        if type(item) is not dict:
            raise SpiderProjectionIntegrityError
        entry = cast(dict[str, object], item)
        entry.pop("sha256", None)
        entry.pop("byte_count", None)
    normalized_completion["endpoint_state_digest"] = normalized_endpoint
    normalized_completion["reconciliation_digest"] = normalized_reconciliation
    return {
        "schema_version": _FLOATING_EXIT_PROJECTION_SCHEMA,
        "attempt": attempt_row,
        "status": _without_run_id(status),
        "journal": _without_run_id(journal),
        "execution_input": _without_run_id(execution_input),
        "paid_endpoint": normalized_endpoint,
        "report_rows": _without_run_id(report_rows),
        "reconciliation": normalized_reconciliation,
        "completion": normalized_completion,
    }


def _floating_exit_projection_from_admitted_artifacts(artifacts: object) -> SpiderFloatingExitProjection:
    if type(artifacts) is not dict:
        raise SpiderProjectionIntegrityError
    try:
        value = _floating_exit_projection_value(cast(dict[str, Any], artifacts))
        encoded = canonical_bytes(value)
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, SpiderProjectionIntegrityError):
            raise
        raise SpiderProjectionIntegrityError from error
    return SpiderFloatingExitProjection(encoded, sha256(encoded).hexdigest())


def _gross_execution_projection_value(artifacts: dict[str, Any]) -> dict[str, object]:
    attempt = artifacts["attempt.json"]
    if cast(dict[str, object], attempt).get("run_contract_id") != "SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1":
        raise SpiderProjectionIntegrityError
    status = artifacts["status.json"]
    journal = artifacts["journal.jsonl"]
    execution_input = artifacts["execution_input.json"]
    paid_endpoint = artifacts["paid_endpoint.json"]
    report_rows = artifacts["report.jsonl"]
    reconciliation = artifacts["reconciliation.json"]
    completion = artifacts["completion.json"]
    normalized_endpoint = cast(dict[str, object], _without_run_id(paid_endpoint))
    normalized_reconciliation = cast(dict[str, object], _without_run_id(reconciliation))
    normalized_completion = cast(dict[str, object], _without_run_id(completion))
    completion_artifacts = normalized_completion["artifacts"]
    if type(completion_artifacts) is not list:
        raise SpiderProjectionIntegrityError
    for item in completion_artifacts:
        if type(item) is not dict:
            raise SpiderProjectionIntegrityError
        entry = cast(dict[str, object], item)
        entry.pop("sha256", None)
        entry.pop("byte_count", None)
    normalized_completion["endpoint_state_digest"] = normalized_endpoint
    normalized_completion["reconciliation_digest"] = normalized_reconciliation
    return {
        "schema_version": _GROSS_EXECUTION_PROJECTION_SCHEMA,
        "attempt": _without_run_id(attempt),
        "status": _without_run_id(status),
        "journal": _without_run_id(journal),
        "execution_input": _without_run_id(execution_input),
        "paid_endpoint": normalized_endpoint,
        "report_rows": _without_run_id(report_rows),
        "reconciliation": normalized_reconciliation,
        "completion": normalized_completion,
    }


def _gross_execution_projection_from_admitted_artifacts(artifacts: object) -> SpiderGrossExecutionProjection:
    if type(artifacts) is not dict:
        raise SpiderProjectionIntegrityError
    try:
        encoded = canonical_bytes(_gross_execution_projection_value(cast(dict[str, Any], artifacts)))
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, SpiderProjectionIntegrityError):
            raise
        raise SpiderProjectionIntegrityError from error
    return SpiderGrossExecutionProjection(encoded, sha256(encoded).hexdigest())


def project_admitted_spider_run(
    directory: str | PathLike[str],
) -> SpiderP02Projection | None:
    """Project a P3 run only when the existing shared admission accepts it.

    Rejected/incomplete bundles return ``None``. Admission owns every acceptance
    decision and returns detached bytes that already passed artifact schemas.
    """
    admission = admit_spider_run(directory)
    if admission.get("decision") != "ACCEPT":
        return None
    return _projection_from_admitted_artifacts(admission.get("artifacts"))


def _invoke_spider_historical(
    *, output_root: str, run_id: str, historical_input: bytes,
    runner_kind: Literal["full", "research"],
) -> SpiderFormalRunResult:
    """Run the shared P3 lifecycle once, then expose only admitted evidence."""
    run_result = spider_scenario_run.run_spider_scenario(
        output_root, run_id, "SPIDER_HISTORICAL_RESEARCH_RUN_V1", historical_input,
    )
    outcome = cast(Literal["ADMITTED", "REJECTED", "FAILED", "DURABILITY_UNKNOWN"], run_result["outcome"])
    reason = cast(str | None, run_result["reason"])
    if outcome != "ADMITTED":
        return SpiderFormalRunResult(runner_kind, run_id, outcome, reason, None, None)

    admission = admit_spider_run(Path(output_root) / run_id)
    if admission.get("decision") != "ACCEPT":
        return SpiderFormalRunResult(
            runner_kind, run_id, "FAILED", cast(str | None, admission.get("reason")), None, None,
        )
    artifacts = admission.get("artifacts")
    try:
        projection = _projection_from_admitted_artifacts(artifacts)
        if type(artifacts) is not dict:
            raise SpiderProjectionIntegrityError
        report_rows_json = canonical_bytes(cast(dict[str, Any], artifacts)["report.jsonl"])
    except (KeyError, TypeError, ValueError) as error:
        if isinstance(error, SpiderProjectionIntegrityError):
            reason = str(error)
        else:
            reason = "FORMAL_PROJECTION_INTEGRITY_FAILED"
        return SpiderFormalRunResult(runner_kind, run_id, "FAILED", reason, None, None)
    return SpiderFormalRunResult(
        runner_kind, run_id, "ADMITTED", None, "ACCEPT", projection, report_rows_json,
    )


def _invoke_spider_execution(
    *, output_root: str, run_id: str, execution_input: bytes,
    runner_kind: Literal["full", "research"],
) -> SpiderExecutionFormalResult:
    """Run the shared paid lifecycle once and project only shared admission."""
    run_result = spider_scenario_run.run_spider_scenario(
        output_root, run_id, "SPIDER_PAID_RESEARCH_RUN_V1", execution_input=execution_input,
    )
    outcome = cast(Literal["ADMITTED", "REJECTED", "FAILED", "DURABILITY_UNKNOWN"], run_result["outcome"])
    reason = cast(str | None, run_result["reason"])
    endpoint_status = cast(
        Literal["PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED"] | None,
        run_result["endpoint_status"],
    )
    if outcome != "ADMITTED":
        return SpiderExecutionFormalResult(
            runner_kind, run_id, outcome, reason, None, endpoint_status, None,
        )

    admission = admit_spider_run(Path(output_root) / run_id)
    if admission.get("decision") != "ACCEPT":
        return SpiderExecutionFormalResult(
            runner_kind, run_id, "FAILED", cast(str | None, admission.get("reason")),
            None, endpoint_status, None,
        )
    artifacts = admission.get("artifacts")
    try:
        projection = _execution_projection_from_admitted_artifacts(artifacts)
        if type(artifacts) is not dict:
            raise SpiderProjectionIntegrityError
        report_rows_json = canonical_bytes(cast(dict[str, Any], artifacts)["report.jsonl"])
    except (KeyError, TypeError, ValueError) as error:
        reason = (str(error) if isinstance(error, SpiderProjectionIntegrityError)
                 else "FORMAL_PROJECTION_INTEGRITY_FAILED")
        return SpiderExecutionFormalResult(
            runner_kind, run_id, "FAILED", reason, None, endpoint_status, None,
        )
    if endpoint_status not in _EXECUTION_RESULT_STATUSES:
        return SpiderExecutionFormalResult(
            runner_kind, run_id, "FAILED", "FORMAL_PROJECTION_INTEGRITY_FAILED",
            None, None, None,
        )
    return SpiderExecutionFormalResult(
        runner_kind, run_id, "ADMITTED", reason, "ACCEPT", endpoint_status,
        projection, report_rows_json,
    )


def _invoke_spider_floating_exit(
    *, output_root: str, run_id: str, floating_exit_input: bytes,
    runner_kind: Literal["full", "research"],
) -> SpiderFloatingExitFormalResult:
    """Run one candidate invocation, then expose only shared admission."""
    run_result = spider_scenario_run.run_spider_scenario(
        output_root, run_id, "SPIDER_FLOATING_EXIT_RESEARCH_RUN_V1",
        floating_exit_input=floating_exit_input,
    )
    outcome = cast(Literal["ADMITTED", "REJECTED", "FAILED", "DURABILITY_UNKNOWN"], run_result["outcome"])
    reason = cast(str | None, run_result["reason"])
    endpoint_status = cast(
        Literal["PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED"] | None,
        run_result["endpoint_status"],
    )
    if outcome != "ADMITTED":
        return SpiderFloatingExitFormalResult(
            runner_kind, run_id, outcome, reason, None, endpoint_status, None,
        )

    admission = admit_spider_run(Path(output_root) / run_id)
    if admission.get("decision") != "ACCEPT":
        return SpiderFloatingExitFormalResult(
            runner_kind, run_id, "FAILED", cast(str | None, admission.get("reason")),
            None, endpoint_status, None,
        )
    artifacts = admission.get("artifacts")
    try:
        projection = _floating_exit_projection_from_admitted_artifacts(artifacts)
        if type(artifacts) is not dict:
            raise SpiderProjectionIntegrityError
        report_rows_json = canonical_bytes(cast(dict[str, Any], artifacts)["report.jsonl"])
    except (KeyError, TypeError, ValueError) as error:
        reason = (str(error) if isinstance(error, SpiderProjectionIntegrityError)
                 else "FORMAL_PROJECTION_INTEGRITY_FAILED")
        return SpiderFloatingExitFormalResult(
            runner_kind, run_id, "FAILED", reason, None, endpoint_status, None,
        )
    if endpoint_status not in _EXECUTION_RESULT_STATUSES:
        return SpiderFloatingExitFormalResult(
            runner_kind, run_id, "FAILED", "FORMAL_PROJECTION_INTEGRITY_FAILED",
            None, None, None,
        )
    return SpiderFloatingExitFormalResult(
        runner_kind, run_id, "ADMITTED", reason, "ACCEPT", endpoint_status,
        projection, report_rows_json,
    )


def _invoke_spider_gross_execution(
    *, output_root: str, run_id: str, gross_execution_input: bytes,
    runner_kind: Literal["full", "research"],
) -> SpiderGrossExecutionFormalResult:
    """Run the shared gross lifecycle and expose only accepted artifacts."""
    run_result = spider_scenario_run.run_spider_scenario(
        output_root, run_id, "SPIDER_GROSS_PROTECTED_RESEARCH_RUN_V1",
        gross_execution_input=gross_execution_input,
    )
    outcome = cast(Literal["ADMITTED", "REJECTED", "FAILED", "DURABILITY_UNKNOWN"], run_result["outcome"])
    reason = cast(str | None, run_result["reason"])
    endpoint_status = cast(
        Literal["PAID_FLAT", "INCOMPLETE", "LIQUIDATED_FLAT", "LIQUIDATED_INSOLVENT", "FAILED"] | None,
        run_result["endpoint_status"],
    )
    if outcome != "ADMITTED":
        return SpiderGrossExecutionFormalResult(
            runner_kind, run_id, outcome, reason, None, endpoint_status, None,
        )
    admission = admit_spider_run(Path(output_root) / run_id)
    if admission.get("decision") != "ACCEPT":
        return SpiderGrossExecutionFormalResult(
            runner_kind, run_id, "FAILED", cast(str | None, admission.get("reason")),
            None, endpoint_status, None,
        )
    artifacts = admission.get("artifacts")
    try:
        projection = _gross_execution_projection_from_admitted_artifacts(artifacts)
        if type(artifacts) is not dict:
            raise SpiderProjectionIntegrityError
        report_rows_json = canonical_bytes(cast(dict[str, Any], artifacts)["report.jsonl"])
    except (KeyError, TypeError, ValueError) as error:
        reason = (str(error) if isinstance(error, SpiderProjectionIntegrityError)
                 else "FORMAL_PROJECTION_INTEGRITY_FAILED")
        return SpiderGrossExecutionFormalResult(
            runner_kind, run_id, "FAILED", reason, None, endpoint_status, None,
        )
    if endpoint_status not in _EXECUTION_RESULT_STATUSES:
        return SpiderGrossExecutionFormalResult(
            runner_kind, run_id, "FAILED", "FORMAL_PROJECTION_INTEGRITY_FAILED",
            None, None, None,
        )
    return SpiderGrossExecutionFormalResult(
        runner_kind, run_id, "ADMITTED", reason, "ACCEPT", endpoint_status,
        projection, report_rows_json,
    )
