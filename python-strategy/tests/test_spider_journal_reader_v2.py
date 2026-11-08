"""T3.3 tests for the owned-buffer P3-v2 journal reader."""

import hashlib

import pytest

from src.core.backtest import spider_artifact_limits as limits
from src.core.backtest import spider_journal_reader_v2 as reader
from src.core.backtest.spider_run_envelope_schema import journal_record
from src.core.backtest.spider_run_artifacts import canonical_bytes
from test_spider_run_envelope_schema import record as valid_v1_record

CONTEXTS = (
    {"schema_version": "spider_configuration_context_v1", "config_id": "cfg"},
    {
        "schema_version": "spider_historical_context_v1",
        "historical_input_sha256": "0" * 64,
    },
)


def _wire_row(
    payload: object | None = None, kind: str = "CALLBACK_RESULT"
) -> dict[str, object]:
    return {
        "schema_version": "spider_journal_record_v2",
        "record_kind": kind,
        "payload": {"result": "ok"} if payload is None else payload,
    }


def _raw(*rows: dict[str, object]) -> bytes:
    return b"".join(canonical_bytes(row) + b"\n" for row in rows)


def _metadata(raw: bytes) -> dict[str, object]:
    return {
        "schema_version": "spider_journal_record_v2",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
        "row_count": raw.count(b"\n"),
    }


def _budget(raw: bytes, *, cap: int | None = None) -> dict[str, object]:
    return {
        "J2": len(raw) if cap is None else cap,
        "kind_budgets": {
            kind: limits.P3_V2_JOURNAL_CAP for kind in limits.P3_V2_RECORD_KINDS
        },
        "row_counts": {kind: 100 for kind in limits.P3_V2_RECORD_KINDS},
        "B_SG": limits.P3_V2_JOURNAL_CAP,
        "table_max": 100,
        "table_bytes_max": 100_000,
    }


def _read(
    path,
    raw: bytes,
    *,
    cap: int | None = None,
    budget: dict[str, object] | None = None,
):
    return reader.read_p3_v2_journal(
        path,
        expected_metadata=_metadata(raw),
        contexts=CONTEXTS,
        budget=_budget(raw, cap=cap) if budget is None else budget,
    )


def test_reader_returns_projection_metadata_and_budget_counters(tmp_path):
    raw = _raw(_wire_row())
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)

    result = _read(path, raw)

    assert result.wire_metadata == {
        "schema_version": "spider_journal_record_v2",
        "sha256": hashlib.sha256(raw).hexdigest(),
        "byte_count": len(raw),
        "row_count": 1,
    }
    assert result.counters == {
        "rows": 1,
        "projected_bytes": len(
            canonical_bytes(
                {
                    **_wire_row(),
                    "schema_version": "spider_journal_record_v1",
                    "configuration_context": CONTEXTS[0],
                    "historical_context": CONTEXTS[1],
                }
            )
        )
        + 1,
        "kind_bytes": {
            **{kind: 0 for kind in limits.P3_V2_RECORD_KINDS},
            "CALLBACK_RESULT": len(raw),
        },
        "kind_rows": {
            **{kind: 0 for kind in limits.P3_V2_RECORD_KINDS},
            "CALLBACK_RESULT": 1,
        },
        "definition_count": 0,
        "definition_bytes": 0,
    }
    assert result.rows[0]["schema_version"] == "spider_journal_record_v1"
    assert result.rows[0]["configuration_context"] == CONTEXTS[0]


def test_all_kind_counter_maps_are_pinned_by_exact_equality(tmp_path):
    callback = _wire_row()
    snapshot = _wire_row({"fact": {"snapshot_kind": "POSITIONS"}}, "SNAPSHOT_FACT")
    raw = _raw(callback, snapshot)
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    callback_v1 = {
        **callback,
        "schema_version": "spider_journal_record_v1",
        "configuration_context": CONTEXTS[0],
        "historical_context": CONTEXTS[1],
    }
    snapshot_v1 = {
        **snapshot,
        "schema_version": "spider_journal_record_v1",
        "configuration_context": CONTEXTS[0],
        "historical_context": CONTEXTS[1],
    }
    callback_size = len(canonical_bytes(callback_v1)) + 1
    snapshot_size = len(canonical_bytes(snapshot_v1)) + 1
    result = _read(path, raw)
    assert result.counters == {
        "rows": 2,
        "projected_bytes": callback_size + snapshot_size,
        "kind_bytes": {
            "HISTORICAL_MARKET_RESULT": 0,
            "SNAPSHOT_FACT": len(canonical_bytes(snapshot)) + 1,
            "CALLBACK_RESULT": len(canonical_bytes(callback)) + 1,
            "SOURCE_GROUP_RESULT": 0,
        },
        "kind_rows": {
            "HISTORICAL_MARKET_RESULT": 0,
            "SNAPSHOT_FACT": 1,
            "CALLBACK_RESULT": 1,
            "SOURCE_GROUP_RESULT": 0,
        },
        "definition_count": 0,
        "definition_bytes": 0,
    }


@pytest.mark.parametrize(
    "field", ["schema_version", "sha256", "byte_count", "row_count"]
)
def test_reader_checks_each_manifest_wire_field_before_parsing(tmp_path, field):
    raw = _raw(_wire_row())
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    expected = _metadata(raw)
    expected[field] = (
        "bad" if field in {"schema_version", "sha256"} else expected[field] + 1
    )

    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        reader.read_p3_v2_journal(
            path, expected_metadata=expected, contexts=CONTEXTS, budget=_budget(raw)
        )


def test_metadata_mismatch_rejects_before_any_json_parse(monkeypatch, tmp_path):
    raw = _raw(_wire_row())
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    called = []

    def forbidden_parse(line):
        called.append(line)
        raise AssertionError("manifest mismatch must precede parsing")

    monkeypatch.setattr(reader, "decode_canonical", forbidden_parse)
    expected = _metadata(raw)
    expected["sha256"] = "f" * 64
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        reader.read_p3_v2_journal(
            path, expected_metadata=expected, contexts=CONTEXTS, budget=_budget(raw)
        )
    assert called == []


def test_reader_uses_one_open_and_reads_file_bytes_once(monkeypatch, tmp_path):
    raw = _raw(_wire_row(), _wire_row({"result": "second"}))
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    opens: list[object] = []
    read_bytes = 0
    open_fd, read_fd = reader.os.open, reader.os.read

    def observed_open(name, flags):
        opens.append(name)
        return open_fd(name, flags)

    def observed_read(fd, size):
        nonlocal read_bytes
        chunk = read_fd(fd, size)
        read_bytes += len(chunk)
        return chunk

    monkeypatch.setattr(reader.os, "open", observed_open)
    monkeypatch.setattr(reader.os, "read", observed_read)

    result = _read(path, raw)

    assert len(opens) == 1
    assert read_bytes == len(raw)
    assert len(result.rows) == 2


def test_both_parse_passes_reuse_the_same_owned_buffer(monkeypatch, tmp_path):
    raw = _raw(_wire_row())
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    iterator = reader._iter_owned_lines
    buffers = []

    def observed(value):
        buffers.append(id(value))
        yield from iterator(value)

    monkeypatch.setattr(reader, "_iter_owned_lines", observed)
    _read(path, raw)
    assert len(buffers) == 2
    assert buffers[0] == buffers[1]


def test_line_iteration_does_not_copy_or_split_the_whole_owned_buffer(
    monkeypatch, tmp_path
):
    raw = _raw(_wire_row())
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    capture = reader.journal_buffer.capture_owned_p3_v2_buffer

    class NoWholeBufferCopies(bytearray):
        def __bytes__(self):
            raise AssertionError("whole-buffer bytes copy")

        def split(self, *args, **kwargs):
            raise AssertionError("whole-buffer split copy")

    def owned_copy_guard(*args, **kwargs):
        captured = capture(*args, **kwargs)
        return reader.journal_buffer.OwnedP3V2Buffer(
            NoWholeBufferCopies(captured.data), captured.wire_metadata
        )

    monkeypatch.setattr(
        reader.journal_buffer, "capture_owned_p3_v2_buffer", owned_copy_guard
    )
    assert len(_read(path, raw).rows) == 1


@pytest.mark.parametrize("delta", [-1, 1])
@pytest.mark.parametrize("with_definitions", [False, True])
def test_pass_two_projection_length_must_match_the_materialized_row(
    monkeypatch, tmp_path, delta, with_definitions
):
    wire = _wire_row()
    if with_definitions:
        wire = _wire_row(
            {
                "request": {"working_orders": [0]},
                "working_orders_snapshot": [0],
            },
            "HISTORICAL_MARKET_RESULT",
        )
        wire["order_defs"] = [{"order_id": "order-1"}]
    raw = _raw(wire)
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    projected_len = reader._projected_len
    calls = 0

    def drift_only_on_pass_two(*args, **kwargs):
        nonlocal calls
        calls += 1
        expected = projected_len(*args, **kwargs)
        return expected + delta if calls == 2 else expected

    monkeypatch.setattr(reader, "_projected_len", drift_only_on_pass_two)
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        _read(path, raw)
    assert calls == 2


def test_real_policy_kind_budget_counts_wire_bytes_at_exact_and_plus_one(
    tmp_path,
):
    budget = limits.p3_v2_scope_budget({"HISTORICAL_MARKET_STEP": 1})
    cap = budget["kind_budgets"]["CALLBACK_RESULT"]
    v1_row = valid_v1_record("CALLBACK_RESULT")
    journal_record(v1_row)
    wire_row = dict(v1_row)
    wire_row["schema_version"] = "spider_journal_record_v2"
    wire_row["payload"] = dict(v1_row["payload"])
    wire_row["payload"]["delivery_id"] = ""
    base = len(canonical_bytes(wire_row)) + 1
    wire_row["payload"]["delivery_id"] = "x" * (cap - base)
    v1_row["payload"]["delivery_id"] = wire_row["payload"]["delivery_id"]
    journal_record(v1_row)
    raw = _raw(wire_row)
    assert len(raw) == cap
    exact_path = tmp_path / "exact.jsonl"
    exact_path.write_bytes(raw)
    result = _read(exact_path, raw, budget=budget)
    assert result.counters["kind_bytes"]["CALLBACK_RESULT"] == cap
    assert result.counters["projected_bytes"] > cap

    one_over = dict(budget)
    one_over["kind_budgets"] = dict(budget["kind_budgets"])
    one_over["kind_budgets"]["CALLBACK_RESULT"] = cap - 1
    over_path = tmp_path / "one-over.jsonl"
    over_path.write_bytes(raw)
    with pytest.raises(ValueError, match="ARTIFACT_LIMIT_EXCEEDED"):
        _read(over_path, raw, budget=one_over)


@pytest.mark.parametrize(
    "kind,path",
    [
        ("CALLBACK_RESULT", ("payload",)),
        ("SNAPSHOT_FACT", ("payload",)),
        ("SNAPSHOT_FACT", ("payload", "fact")),
        ("SNAPSHOT_FACT", ("payload", "fact", "immutable_payload")),
        (
            "SNAPSHOT_FACT",
            ("payload", "fact", "immutable_payload", "rows"),
        ),
        ("HISTORICAL_MARKET_RESULT", ("payload",)),
        ("HISTORICAL_MARKET_RESULT", ("payload", "request")),
        (
            "HISTORICAL_MARKET_RESULT",
            ("payload", "request", "working_orders"),
        ),
        ("HISTORICAL_MARKET_RESULT", ("payload", "working_orders_snapshot")),
        ("SOURCE_GROUP_RESULT", ("payload",)),
        ("SOURCE_GROUP_RESULT", ("payload", "owner_evidence_before")),
        (
            "SOURCE_GROUP_RESULT",
            ("payload", "owner_evidence_before", "open_orders_fact"),
        ),
        (
            "SOURCE_GROUP_RESULT",
            (
                "payload",
                "owner_evidence_before",
                "open_orders_fact",
                "immutable_payload",
            ),
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
        ),
        ("SOURCE_GROUP_RESULT", ("payload", "owner_evidence_after")),
        (
            "SOURCE_GROUP_RESULT",
            ("payload", "owner_evidence_after", "open_orders_fact"),
        ),
        (
            "SOURCE_GROUP_RESULT",
            (
                "payload",
                "owner_evidence_after",
                "open_orders_fact",
                "immutable_payload",
            ),
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
        ),
    ],
)
def test_every_reference_path_rejects_non_container_types(tmp_path, kind, path):
    valid_payloads = {
        "CALLBACK_RESULT": {"result": "ok"},
        "SNAPSHOT_FACT": {
            "fact": {
                "snapshot_kind": "OPEN_ORDERS",
                "immutable_payload": {"outcome": "SUCCESS", "rows": []},
            }
        },
        "HISTORICAL_MARKET_RESULT": {
            "request": {"working_orders": []},
            "working_orders_snapshot": [],
        },
        "SOURCE_GROUP_RESULT": {
            "owner_evidence_before": {
                "open_orders_fact": {"immutable_payload": {"rows": []}}
            },
            "owner_evidence_after": {
                "open_orders_fact": {"immutable_payload": {"rows": []}}
            },
        },
    }
    bad_types = (
        (None, {}, 0, "bad")
        if path[-1]
        in {
            "rows",
            "working_orders",
            "working_orders_snapshot",
        }
        else (None, [], 0, "bad")
    )
    for index, bad_type in enumerate(bad_types):
        row = {
            "schema_version": "spider_journal_record_v2",
            "record_kind": kind,
            "payload": valid_payloads[kind],
        }
        target = row
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = bad_type
        raw = _raw(row)
        journal_path = tmp_path / f"malformed-{index}.jsonl"
        journal_path.write_bytes(raw)
        with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
            _read(journal_path, raw)


def test_open_orders_failure_payload_does_not_require_success_rows(tmp_path):
    raw = _raw(
        _wire_row(
            {
                "fact": {
                    "snapshot_kind": "OPEN_ORDERS",
                    "immutable_payload": {"outcome": "FAILURE"},
                }
            },
            "SNAPSHOT_FACT",
        )
    )
    journal_path = tmp_path / "failure.jsonl"
    journal_path.write_bytes(raw)
    result = _read(journal_path, raw)
    assert result.rows[0]["payload"]["fact"]["immutable_payload"] == {
        "outcome": "FAILURE"
    }


def test_empty_journal_is_a_complete_two_pass_input(tmp_path):
    raw = b""
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    result = _read(path, raw, cap=0)
    assert result.rows == []
    assert result.counters == {
        "rows": 0,
        "projected_bytes": 0,
        "kind_bytes": {kind: 0 for kind in limits.P3_V2_RECORD_KINDS},
        "kind_rows": {kind: 0 for kind in limits.P3_V2_RECORD_KINDS},
        "definition_count": 0,
        "definition_bytes": 0,
    }


def test_row_with_order_defs_projects_same_row_references(tmp_path):
    definition = {"order_id": "order-1", "symbol": "BTC-USDT-SWAP"}
    wire = _wire_row(
        {"request": {"working_orders": [0]}, "working_orders_snapshot": [0]},
        "HISTORICAL_MARKET_RESULT",
    )
    wire["order_defs"] = [definition]
    raw = _raw(wire)
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)

    result = _read(path, raw)

    assert result.rows[0]["payload"]["request"]["working_orders"] == [definition]
    assert result.rows[0]["payload"]["working_orders_snapshot"] == [definition]
    assert "order_defs" not in result.rows[0]


@pytest.mark.parametrize("kind", ["DELIVERY_ATTEMPT", "UNKNOWN"])
def test_reader_rejects_kinds_outside_v2_closed_set(tmp_path, kind):
    raw = _raw(_wire_row(kind=kind))
    path = tmp_path / "journal.jsonl"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        _read(path, raw)


@pytest.mark.parametrize("kind", [None, [], {}, 0, True])
def test_reader_rejects_non_string_record_kind_as_named_mismatch(tmp_path, kind):
    wire = _wire_row()
    wire["record_kind"] = kind
    raw = _raw(wire)
    path = tmp_path / "record-kind.jsonl"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        _read(path, raw)


@pytest.mark.parametrize("schema", [None, [], {}, 0, True, "wrong"])
def test_reader_rejects_non_v2_schema_value_as_named_mismatch(tmp_path, schema):
    wire = _wire_row()
    wire["schema_version"] = schema
    raw = _raw(wire)
    path = tmp_path / "schema-version.jsonl"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        _read(path, raw)


@pytest.mark.parametrize("root", [None, [], {}, 0, "bad"])
def test_reader_rejects_non_object_wire_row_as_named_mismatch(tmp_path, root):
    raw = canonical_bytes(root) + b"\n"
    path = tmp_path / "root.jsonl"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        _read(path, raw)


@pytest.mark.parametrize("definitions", [None, True, 0, {}, "bad"])
def test_reader_rejects_non_list_order_defs_as_named_mismatch(
    tmp_path, definitions
):
    wire = _wire_row(
        {
            "request": {"working_orders": []},
            "working_orders_snapshot": [],
        },
        "HISTORICAL_MARKET_RESULT",
    )
    wire["order_defs"] = definitions
    raw = _raw(wire)
    path = tmp_path / "order-defs.jsonl"
    path.write_bytes(raw)
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        _read(path, raw)
