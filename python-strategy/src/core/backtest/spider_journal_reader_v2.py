# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
import hashlib
import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import NoReturn, cast

from src.core.backtest import spider_artifact_limits as limits
from src.core.backtest import spider_journal_buffer_v2 as journal_buffer
from src.core.backtest.spider_projection_size import (
    canonical_jsonl_row_len,
    materialize_v1_row,
    projected_jsonl_budget_ok,
    projected_v1_jsonl_row_len,
)
from src.core.backtest.spider_run_artifacts import (
    canonical_bytes,
    decode_canonical,
    decode_jsonl,
)
from src.core.backtest.spider_journal_slots_v2 import SLOT_LIST
from src.core.backtest.spider_journal_slot_classifier_v2 import classify_p3_v2_slots

_V2_SCHEMA = "spider_journal_record_v2"


@dataclass(frozen=True, slots=True)
class P3V2JournalRead:
    rows: list[dict[str, object]]
    wire_metadata: dict[str, int | str]
    counters: dict[str, object]


def _raise(code: str) -> NoReturn:
    raise ValueError(code)


def _iter_owned_lines(raw: bytearray):
    if raw and raw[-1] != 10:
        _raise("ARTIFACT_MISMATCH")
    view = memoryview(raw)
    start = 0
    while start < len(view):
        end = raw.find(b"\n", start)
        if end < 0:
            _raise("ARTIFACT_MISMATCH")
        framed_size = end - start + 1
        if end == start or framed_size > limits.P3_V2_ROW_MAX:
            _raise(
                "ARTIFACT_LIMIT_EXCEEDED"
                if framed_size > limits.P3_V2_ROW_MAX
                else "ARTIFACT_MISMATCH"
            )
        yield view[start:end].tobytes()
        start = end + 1


def _decode_wire_row(line: bytes) -> dict[str, object]:
    try:
        row = decode_canonical(line)
    except (UnicodeError, ValueError, RecursionError):
        _raise("ARTIFACT_MISMATCH")
    if type(row) is not dict or row.get("schema_version") != _V2_SCHEMA:
        _raise("ARTIFACT_MISMATCH")
    if "configuration_context" in row or "historical_context" in row:
        _raise("ARTIFACT_MISMATCH")
    kind = row.get("record_kind")
    if type(kind) is not str or kind not in limits.P3_V2_RECORD_KINDS:
        _raise("ARTIFACT_MISMATCH")
    return cast(dict[str, object], row)


def _refs(row: dict[str, object]) -> list[list[object]]:
    slots = classify_p3_v2_slots(row)
    if slots is None:
        _raise("ARTIFACT_MISMATCH")
    return [
        cast(list[object], parent[leaf])
        for _, _, state, parent, leaf in slots
        if state == SLOT_LIST
    ]


def _projected_len(
    row: dict[str, object],
    definitions: list[object],
    contexts: tuple[object, object],
    prior: int,
) -> int:
    try:
        return projected_v1_jsonl_row_len(
            row, definitions, *contexts, prior_definition_count=prior
        )
    except (KeyError, IndexError, TypeError, ValueError, RecursionError):
        _raise("ARTIFACT_MISMATCH")


def _pass_one(
    raw: bytearray,
    *,
    contexts: tuple[object, object],
    budget: Mapping[str, object],
) -> dict[str, object]:
    journal_cap = budget.get("J2")
    if type(journal_cap) is not int or journal_cap < 0:
        _raise("INVALID_ARTIFACT")
    if len(raw) > journal_cap:
        _raise("ARTIFACT_LIMIT_EXCEEDED")
    table_max = budget.get("table_max")
    table_bytes_max = budget.get("table_bytes_max")
    kind_budgets = budget.get("kind_budgets")
    row_budgets = budget.get("row_counts")
    sg_budget = budget.get("B_SG")
    if (
        any(type(value) is not int for value in (table_max, table_bytes_max, sg_budget))
        or type(kind_budgets) is not dict
        or type(row_budgets) is not dict
    ):
        _raise("INVALID_ARTIFACT")
    table_max = cast(int, table_max)
    table_bytes_max = cast(int, table_bytes_max)
    kind_budgets = cast(dict[str, int], kind_budgets)
    row_budgets = cast(dict[str, int], row_budgets)
    sg_budget = cast(int, sg_budget)

    definitions: list[object] = []
    definition_keys: set[bytes] = set()
    definition_bytes = 0
    totals = {kind: 0 for kind in limits.P3_V2_RECORD_KINDS}
    kind_rows = dict.fromkeys(limits.P3_V2_RECORD_KINDS, 0)
    projected_total = 0
    rows = 0
    for line in _iter_owned_lines(raw):
        row = _decode_wire_row(line)
        refs = _refs(row)
        row_defs = row.get("order_defs", [])
        if type(row_defs) is not list:
            _raise("ARTIFACT_MISMATCH")
        if "order_defs" in row and not refs:
            _raise("ARTIFACT_MISMATCH")
        first_new = len(definitions)
        row_new_refs: set[int] = set()
        for item in row_defs:
            encoded = canonical_bytes(item)
            if len(encoded) > limits.P3_V2_MAX_DEFINITION_BYTES:
                _raise("ARTIFACT_LIMIT_EXCEEDED")
            if encoded in definition_keys:
                _raise("ARTIFACT_MISMATCH")
            definition_keys.add(encoded)
            definitions.append(item)
            definition_bytes += len(encoded)
        if len(definitions) > table_max or definition_bytes > table_bytes_max:
            _raise("ARTIFACT_LIMIT_EXCEEDED")
        for slot in refs:
            for value in slot:
                if type(value) is not int or value < 0 or value >= len(definitions):
                    _raise("ARTIFACT_MISMATCH")
                if value >= first_new:
                    row_new_refs.add(value)
        if len(row_new_refs) != len(row_defs):
            _raise("ARTIFACT_MISMATCH")
        expanded = _projected_len(row, definitions, contexts, first_new)
        if not projected_jsonl_budget_ok(
            wire_bytes=len(raw),
            projected_bytes=projected_total + expanded,
            row_projected_bytes=expanded,
        ):
            _raise("ARTIFACT_LIMIT_EXCEEDED")
        projected_total += expanded
        kind = cast(str, row["record_kind"])
        wire_row_bytes = len(line) + 1
        totals[kind] += wire_row_bytes
        kind_rows[kind] += 1
        cap = sg_budget if kind == "SOURCE_GROUP_RESULT" else kind_budgets.get(kind)
        row_cap = row_budgets.get(kind)
        if (
            type(cap) is not int
            or totals[kind] > cap
            or (
                kind != "SOURCE_GROUP_RESULT"
                and (type(row_cap) is not int or kind_rows[kind] > row_cap)
            )
        ):
            _raise("ARTIFACT_LIMIT_EXCEEDED")
        rows += 1
    return {
        "rows": rows,
        "projected_bytes": projected_total,
        "kind_bytes": totals,
        "kind_rows": kind_rows,
        "definition_count": len(definitions),
        "definition_bytes": definition_bytes,
    }


def _pass_two(
    owned: bytearray,
    *,
    contexts: tuple[object, object],
) -> list[dict[str, object]]:
    result: list[dict[str, object]] = []
    table: list[object] = []
    for line in _iter_owned_lines(owned):
        wire_row = _decode_wire_row(line)
        prior_count = len(table)
        projected_len = _projected_len(wire_row, table, contexts, prior_count)
        projected, new_definitions = materialize_v1_row(wire_row, table, contexts)
        table.extend(new_definitions)
        if canonical_jsonl_row_len(projected) != projected_len:
            _raise("ARTIFACT_MISMATCH")
        result.append(projected)
    return result


def _read_captured_p3_v2_journal(
    captured: journal_buffer.OwnedP3V2Buffer,
    *,
    contexts: tuple[object, object], budget: Mapping[str, object],
) -> P3V2JournalRead:
    owned, metadata = captured.data, captured.wire_metadata
    row_count = cast(int, metadata["row_count"])
    first = _pass_one(owned, contexts=contexts, budget=budget)
    if first["rows"] != row_count:
        _raise("ARTIFACT_MISMATCH")
    result = _pass_two(owned, contexts=contexts)
    return P3V2JournalRead(result, metadata, first)


def read_p3_v2_journal(
    path: str | os.PathLike[str],
    *,
    expected_metadata: Mapping[str, object],
    contexts: tuple[object, object],
    budget: Mapping[str, object],
) -> P3V2JournalRead:
    journal_cap = budget.get("J2")
    if type(journal_cap) is not int or journal_cap < 0:
        _raise("INVALID_ARTIFACT")
    captured = journal_buffer.capture_owned_p3_v2_buffer(
        path, expected_metadata=expected_metadata, journal_cap=journal_cap
    )
    return _read_captured_p3_v2_journal(captured, contexts=contexts, budget=budget)


def read_p3_v2_journal_at(
    directory_fd: int,
    name: str,
    *,
    expected_metadata: Mapping[str, object],
    contexts: tuple[object, object],
    budget: Mapping[str, object],
) -> P3V2JournalRead:
    """Read a v2 journal leaf relative to a previously validated directory fd."""
    journal_cap = budget.get("J2")
    if type(journal_cap) is not int or journal_cap < 0:
        _raise("INVALID_ARTIFACT")
    captured = journal_buffer.capture_owned_p3_v2_buffer_at(
        directory_fd, name, expected_metadata=expected_metadata, journal_cap=journal_cap,
    )
    return _read_captured_p3_v2_journal(captured, contexts=contexts, budget=budget)


def _contexts_from_retained_attempt_bytes(
    attempt_bytes: object, expected_sha256: object
) -> tuple[dict[str, object], dict[str, object]]:
    if (
        type(attempt_bytes) is not bytes
        or type(expected_sha256) is not str
        or hashlib.sha256(attempt_bytes).hexdigest() != expected_sha256
    ):
        _raise("INVALID_ARTIFACT")
    try:
        attempts = decode_jsonl(attempt_bytes)
    except (UnicodeError, ValueError, RecursionError):
        _raise("INVALID_ARTIFACT")
    if len(attempts) != 1:
        _raise("INVALID_ARTIFACT")
    attempt = attempts[0]
    configuration = attempt.get("configuration_context")
    historical = attempt.get("historical_context")
    if type(configuration) is not dict or type(historical) is not dict:
        _raise("INVALID_ARTIFACT")
    return cast(dict[str, object], configuration), cast(dict[str, object], historical)


def read_p3_v2_journal_from_attempt_bytes_at(
    directory_fd: int,
    name: str,
    *,
    expected_metadata: Mapping[str, object],
    retained_attempt_bytes: object,
    expected_attempt_sha256: object,
    budget: Mapping[str, object],
) -> P3V2JournalRead:
    contexts = _contexts_from_retained_attempt_bytes(
        retained_attempt_bytes, expected_attempt_sha256
    )
    return read_p3_v2_journal_at(
        directory_fd, name, expected_metadata=expected_metadata,
        contexts=contexts, budget=budget,
    )


def read_p3_v2_journal_for_diagnosis_at(
    directory_fd: int, name: str, *, retained_attempt_bytes: object,
    expected_attempt_sha256: object, budget: Mapping[str, object],
) -> P3V2JournalRead:
    """Run both reader passes on one owned journal buffer without a manifest."""
    journal_cap = budget.get("J2")
    if type(journal_cap) is not int or journal_cap < 0:
        _raise("INVALID_ARTIFACT")
    contexts = _contexts_from_retained_attempt_bytes(
        retained_attempt_bytes, expected_attempt_sha256
    )
    captured = journal_buffer.capture_owned_p3_v2_buffer_unmanifested_at(
        directory_fd, name, journal_cap=journal_cap,
    )
    return _read_captured_p3_v2_journal(captured, contexts=contexts, budget=budget)


def read_p3_v2_journal_from_attempt_bytes(
    path: str | os.PathLike[str],
    *,
    expected_metadata: Mapping[str, object],
    retained_attempt_bytes: object,
    expected_attempt_sha256: object,
    budget: Mapping[str, object],
) -> P3V2JournalRead:
    """Read v2 rows and reattach contexts from the verified retained attempt.

    The caller supplies the exact bytes captured and validated for this run;
    this function verifies their digest and never reopens an attempt path.
    """
    contexts = _contexts_from_retained_attempt_bytes(
        retained_attempt_bytes, expected_attempt_sha256
    )
    return read_p3_v2_journal(
        path,
        expected_metadata=expected_metadata,
        contexts=contexts,
        budget=budget,
    )
