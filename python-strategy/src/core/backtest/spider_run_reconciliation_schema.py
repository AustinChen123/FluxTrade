"""Closed reconciliation structures, not evidence truth or run admission."""

from src.core.backtest import spider_run_envelope_schema as envelope
from src.core.backtest import spider_run_native_schema as native
from src.core.backtest.spider_run_artifacts import (
    _KINDS, _TERMINALS, _boundary, _enum, _integer, _list, _object, _require, _text,
)
from src.core.backtest.spider_run_completion_schema import (
    evidence_reference, evidence_references, report,
)

_CHECKS = (
    "PLANNED_COVERAGE_COMPLETE", "PROCESSED_EQUALS_PERSISTED", "JOURNAL_CONTIGUOUS",
    "OWNER_IDENTITY_MATCH", "OWNER_DIGEST_MATCH", "ENDPOINT_SNAPSHOT_VERSION_MATCH",
    "NO_UNRESOLVED_QUEUE", "NO_UNRESOLVED_POLL", "NO_UNSUBMITTED_CALLBACK_ACTION",
    "TERMINAL_POLICY_MATCH", "REPORT_PROJECTION_MATCH",
)


def _unique(values: list) -> None:
    _require(len(values) == len(set(values)))


def _barriers(value: object, *, journal: bool = False, ordered: bool = False) -> None:
    identities: list[tuple[int, str]] = []
    sequences: list[int] = []
    for item in _list(value):
        row = _object(item, "ordinal barrier_id record_kind" + (" journal_seq" if journal else ""))
        ordinal = _integer(row["ordinal"])
        _require(ordinal > 0)
        identities.append((ordinal, _text(row["barrier_id"])))
        _enum(row["record_kind"], _KINDS)
        if journal:
            sequence = _integer(row["journal_seq"])
            _require(sequence > 0)
            sequences.append(sequence)
    _unique([item[0] for item in identities])
    _unique([item[1] for item in identities])
    _unique(sequences)
    if ordered:
        envelope._ordered(identities)


def _actions(value: object) -> None:
    identities: list[tuple[str, int, int]] = []
    for item in _list(value):
        row = _object(item, "delivery_id event_index action_index group_id status group_result_ref cancel_effect_ref")
        identities.append((_text(row["delivery_id"]), _integer(row["event_index"]), _integer(row["action_index"])))
        _enum(row["status"], ["SUBMITTED", "UNSUBMITTED"])
        if row["group_id"] is not None:
            _text(row["group_id"])
        for key in ("group_result_ref", "cancel_effect_ref"):
            if row[key] is not None:
                evidence_reference(row[key])
    envelope._ordered(identities)


def _value(name: str, value: object) -> None:
    if name == "PLANNED_COVERAGE_COMPLETE":
        _barriers(value)
    elif name == "PROCESSED_EQUALS_PERSISTED":
        row = _object(value, "processed_boundary persisted_boundary last_planned")
        for key in ("processed_boundary", "persisted_boundary"):
            _require(row[key] is not None)
            _boundary(row[key])
        _barriers([row["last_planned"]])
    elif name == "JOURNAL_CONTIGUOUS":
        _barriers(value, journal=True)
    elif name == "OWNER_IDENTITY_MATCH":
        refs: list[str] = []
        for item in _list(value):
            row = _object(item, "evidence_ref account_key profile_id config_id")
            refs.append(evidence_reference(row["evidence_ref"]))
            native.account(row["account_key"])
            _enum(row["profile_id"], native._PROFILES)
            _enum(row["config_id"], ["scenario-v1"])
        _unique(refs)
    elif name == "OWNER_DIGEST_MATCH":
        barriers: list[str] = []
        for item in _list(value):
            row = _object(item, "barrier_id expected_owner_sha256 observed_owner_sha256")
            barriers.append(_text(row["barrier_id"]))
            native._hash(row["expected_owner_sha256"])
            native._hash(row["observed_owner_sha256"])
        _unique(barriers)
        _require(bool(barriers) and barriers[-1] == "FINAL")
    elif name == "ENDPOINT_SNAPSHOT_VERSION_MATCH":
        row = _object(value, "inspection_account_version trading positions open_orders endpoint_cutoff")
        _integer(row["inspection_account_version"])
        _integer(row["endpoint_cutoff"])
        for key in ("trading", "positions", "open_orders"):
            snapshot = _object(row[key], "captured_account_version snapshot_as_of")
            _integer(snapshot["captured_account_version"])
            _integer(snapshot["snapshot_as_of"])
    elif name == "NO_UNRESOLVED_QUEUE":
        row = _object(value, "pending_keys remaining_planned_barriers")
        envelope._ordered([envelope.scheduler_key(item) for item in _list(row["pending_keys"])])
        _barriers(row["remaining_planned_barriers"], ordered=True)
    elif name == "NO_UNRESOLVED_POLL":
        polls = [envelope.poll(item) for item in _list(value)]
        envelope._ordered(polls)
        _unique([item[0] for item in polls])
        _unique([item[1] for item in polls])
    elif name == "NO_UNSUBMITTED_CALLBACK_ACTION":
        _actions(value)
    elif name == "TERMINAL_POLICY_MATCH":
        row = _object(value, "terminal_policy terminal_reason scheduler_gate scheduler_terminal owner_gate owner_lifecycle remaining_planned_barriers")
        _enum(row["terminal_policy"], _TERMINALS)
        _enum(row["terminal_reason"], _TERMINALS)
        _enum(row["scheduler_gate"], ["RUNNING", "FAILED"])
        _enum(row["owner_gate"], ["RUNNING", "FAILED"])
        _enum(row["owner_lifecycle"], native._LIFECYCLES)
        envelope.scheduler_terminal(row["scheduler_terminal"])
        _barriers(row["remaining_planned_barriers"], ordered=True)
    else:
        _require(name == "REPORT_PROJECTION_MATCH")
        row = _object(value, "report_rows report_sha256")
        report(row["report_rows"])
        native._hash(row["report_sha256"])


def reconciliation(value: object) -> None:
    """Validate all eleven shapes without comparing or interpreting evidence."""
    row = _object(value, "schema_version run_id result checks")
    _enum(row["schema_version"], ["spider_reconciliation_v1"])
    _text(row["run_id"], "[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    _enum(row["result"], ["OK", "FAILED"])
    checks = _list(row["checks"])
    _require(len(checks) == len(_CHECKS))
    for item, name in zip(checks, _CHECKS, strict=True):
        check = _object(item, "name expected observed result evidence_refs")
        _enum(check["name"], [name])
        _enum(check["result"], ["OK", "FAILED"])
        evidence_references(check["evidence_refs"])
        _value(name, check["expected"])
        _value(name, check["observed"])
