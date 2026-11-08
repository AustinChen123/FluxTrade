# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""Wire, corpus and import-boundary fixtures for J4 tests."""

import hashlib
from pathlib import Path

from src.core.backtest import spider_artifact_limits as limits
from src.core.backtest import spider_journal_codec_v2 as codec
from src.core.backtest import spider_journal_reader_v2 as reader
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical
from spider_journal_codec_test_support import (
    CONFIGURATION,
    HISTORICAL,
    _market,
    _open,
    _row,
    _snapshot,
    _source_group,
    _working,
    copy,
)


def _without_context_members(rows):
    # Test-only composition of the later B2 step: J4 must not implement the
    # context gate/removal, while the J3 reader requires the final v2 shape.
    result = copy.deepcopy(rows)
    for row in result:
        row.pop("configuration_context", None)
        row.pop("historical_context", None)
    return result


def _wire_read_result(rows, tmp_path, contexts=(CONFIGURATION, HISTORICAL)):
    rows = _without_context_members(rows)
    raw = b"".join(canonical_bytes(row) + b"\n" for row in rows)
    path = tmp_path / "codec-v2.jsonl"
    path.write_bytes(raw)
    metadata = {
        "schema_version": "spider_journal_record_v2",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
        "row_count": raw.count(b"\n"),
    }
    result = reader.read_p3_v2_journal(
        path,
        expected_metadata=metadata,
        contexts=contexts,
        budget={
            "J2": 1_000_000,
            "kind_budgets": {kind: 1_000_000 for kind in limits.P3_V2_RECORD_KINDS},
            "row_counts": {kind: 1_000 for kind in limits.P3_V2_RECORD_KINDS},
            "B_SG": 1_000_000,
            "table_max": 10_000,
            "table_bytes_max": 10_000_000,
        },
    )
    return result


def _wire_read(rows, tmp_path, contexts=(CONFIGURATION, HISTORICAL)):
    return _wire_read_result(rows, tmp_path, contexts).rows


def _generated_rows():
    working_one = _working("w-one")
    working_two = _working("w-two", side="SHORT", remaining_quantity_contracts="1.25")
    open_one = _open("o-one")
    open_two = _open("o-two", side="sell", cumulative_filled_size_contracts="0.25")
    return [
        _market([working_one, working_two, working_one], [working_two, working_one]),
        _source_group([open_one], [open_one, open_two]),
        _snapshot([open_two]),
        _row("CALLBACK_RESULT", {"callback": "complete"}),
        _snapshot(..., outcome="FAILURE"),
        _snapshot(..., snapshot_kind="POSITIONS"),
    ]


def _encode_rows(rows):
    state = ()
    encoded_rows = []
    for source in rows:
        encoded, state = codec.encode_p3_v2_row(source, state)
        encoded_rows.append(encoded)
    return encoded_rows, state


def _reorder_nested(value, rng):
    if type(value) is dict:
        items = list(value.items())
        rng.shuffle(items)
        return {key: _reorder_nested(item, rng) for key, item in items}
    if type(value) is list:
        return [_reorder_nested(item, rng) for item in value]
    return value


def _assert_source_import_boundary(source_text):
    import ast

    tree = ast.parse(source_text)
    imports = []
    forbidden_names = {
        "open",
        "eval",
        "exec",
        "compile",
        "__import__",
        "getattr",
        "setattr",
        "delattr",
        "globals",
        "locals",
        "vars",
        "breakpoint",
        "input",
        "memoryview",
    }
    allowed_attributes = {"copy", "get", "append"}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            raise AssertionError("module imports are forbidden")
        if isinstance(node, ast.ImportFrom):
            imports.append(node)
        if isinstance(node, ast.Name) and isinstance(node.ctx, ast.Load):
            if node.id in forbidden_names:
                raise AssertionError(f"forbidden capability name: {node.id}")
        if isinstance(node, ast.Attribute):
            if node.attr.startswith("__") or node.attr not in allowed_attributes:
                raise AssertionError(f"attribute not in allowlist: {node.attr}")
    actual = {
        imported.module: [(alias.name, alias.asname) for alias in imported.names]
        for imported in imports
    }
    assert len(imports) == 3
    assert all(imported.level == 0 for imported in imports)
    assert actual == {
        "src.core.backtest.spider_run_artifacts": [
            ("canonical_bytes", None),
            ("decode_canonical", None),
        ],
        "src.core.backtest.spider_journal_slots_v2": [("SLOT_LIST", None)],
        "src.core.backtest.spider_journal_slot_classifier_v2": [
            ("classify_p3_v2_slots", None),
        ],
    }


ROUND_TRIP_SLOT_CASES = [
    (
        "HISTORICAL_MARKET_RESULT",
        ("payload", "request", "working_orders"),
        _market([], []),
    ),
    (
        "HISTORICAL_MARKET_RESULT",
        ("payload", "working_orders_snapshot"),
        _market([], []),
    ),
    (
        "SOURCE_GROUP_RESULT",
        (
            "payload",
            "owner_evidence_before",
            "open_orders_fact",
            "immutable_payload",
            "rows",
        ),
        _source_group([], []),
    ),
    (
        "SOURCE_GROUP_RESULT",
        (
            "payload",
            "owner_evidence_after",
            "open_orders_fact",
            "immutable_payload",
            "rows",
        ),
        _source_group([], []),
    ),
    (
        "SNAPSHOT_FACT",
        ("payload", "fact", "immutable_payload", "rows"),
        _snapshot([]),
    ),
]


def _slot_at(row, path):
    current = row
    for name in path:
        current = current[name]
    return current


__all__ = [
    "ROUND_TRIP_SLOT_CASES",
    "Path",
    "hashlib",
    "canonical_bytes",
    "decode_canonical",
    "limits",
    "codec",
    "reader",
    "_assert_source_import_boundary",
    "_encode_rows",
    "_generated_rows",
    "_reorder_nested",
    "_slot_at",
    "_wire_read",
    "_wire_read_result",
    "_without_context_members",
]
