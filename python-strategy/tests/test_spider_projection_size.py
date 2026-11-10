"""T2 projection sizing exactness tests from JOURNAL_AB_DESIGN v3 T2.3."""

from copy import deepcopy
from collections.abc import Mapping, Sequence
from typing import cast

import pytest

from src.core.backtest import spider_projection_size as projection
from src.core.backtest.spider_run_artifacts import canonical_bytes

CONFIGURATION = {"schema_version": "spider_configuration_context_v1", "id": "cfg-é"}
HISTORICAL = {"schema_version": "spider_historical_context_v1", "source": "ü"}


def _wire_row(
    kind: str, definitions: Sequence[Mapping[str, object]], refs: list[int]
) -> dict[str, object]:
    row: dict[str, object] = {
        "schema_version": "spider_journal_record_v2",
        "record_kind": kind,
        "run_id": "run-1",
        "payload": {
            "request": {"working_orders": refs},
            "working_orders_snapshot": list(refs),
        },
    }
    if definitions:
        row["order_defs"] = list(definitions)
    return row


def _expanded(
    row: dict[str, object], definitions: Sequence[Mapping[str, object]]
) -> dict[str, object]:
    result = deepcopy(row)
    result.pop("order_defs", None)
    result["schema_version"] = "spider_journal_record_v1"
    payload = result["payload"]
    assert isinstance(payload, dict)
    request = payload["request"]
    assert isinstance(request, dict)
    request["working_orders"] = [
            dict(definitions[index]) for index in request["working_orders"]
    ]
    payload["working_orders_snapshot"] = [
        dict(definitions[index]) for index in payload["working_orders_snapshot"]
    ]
    result["configuration_context"] = CONFIGURATION
    result["historical_context"] = HISTORICAL
    return result


@pytest.mark.parametrize(
    ("definitions", "refs"),
    [
        ([], []),
        ([{"order_id": "μ"}], [0]),
        ([{"order_id": "same", "side": "buy"}], [0, 0, 0, 0]),
        ([{"order_id": str(index)} for index in range(100)], [9, 10, 99]),
    ],
)
def test_projected_row_length_equals_canonical_projection(definitions, refs):
    wire = _wire_row("HISTORICAL_MARKET_RESULT", definitions, refs)
    projected = _expanded(wire, definitions)
    expected = len(canonical_bytes(projected)) + 1
    assert (
        projection.projected_v1_jsonl_row_len(wire, (), CONFIGURATION, HISTORICAL)
        == expected
    )
    assert projection.canonical_jsonl_row_len(projected) == expected


def test_no_reference_row_still_accounts_for_both_context_member_frames():
    wire = {
        "schema_version": "spider_journal_record_v2",
        "record_kind": "CALLBACK_RESULT",
        "payload": {"result": "café"},
    }
    projected = {
        **wire,
        "schema_version": "spider_journal_record_v1",
        "configuration_context": CONFIGURATION,
        "historical_context": HISTORICAL,
    }
    assert (
        projection.projected_v1_jsonl_row_len(wire, (), CONFIGURATION, HISTORICAL)
        == len(canonical_bytes(projected)) + 1
    )


def test_prior_definition_reference_uses_global_index_and_definition_bytes():
    prior = [{"order_id": "old-雪"}]
    wire = _wire_row("HISTORICAL_MARKET_RESULT", [], [0])
    projected = _expanded(wire, prior)
    assert (
        projection.projected_v1_jsonl_row_len(wire, prior, CONFIGURATION, HISTORICAL)
        == len(canonical_bytes(projected)) + 1
    )


@pytest.mark.parametrize("kind", ["SOURCE_GROUP_RESULT", "SNAPSHOT_FACT"])
def test_open_order_slot_projection_matches_canonical(kind):
    definition = {"order_id": "open-雪", "venue": "sim"}
    if kind == "SOURCE_GROUP_RESULT":
        evidence = {"open_orders_fact": {"immutable_payload": {"rows": [0]}}}
        payload = {
            "owner_evidence_before": deepcopy(evidence),
            "owner_evidence_after": deepcopy(evidence),
        }
        projected_payload = deepcopy(payload)
        for side in ("owner_evidence_before", "owner_evidence_after"):
            owner = cast(dict[str, object], projected_payload[side])
            orders = cast(dict[str, object], owner["open_orders_fact"])
            immutable = cast(dict[str, object], orders["immutable_payload"])
            immutable["rows"] = [dict(definition)]
    else:
        fact = {
            "snapshot_kind": "OPEN_ORDERS",
            "immutable_payload": {"outcome": "SUCCESS", "rows": [0]},
        }
        payload = {"fact": fact}
        projected_fact = deepcopy(fact)
        projected_fact["immutable_payload"]["rows"] = [dict(definition)]
        projected_payload = {"fact": projected_fact}
    wire = _wire_row(kind, [definition], [])
    wire["payload"] = payload
    projected = {
        **wire,
        "schema_version": "spider_journal_record_v1",
        "payload": projected_payload,
        "configuration_context": CONFIGURATION,
        "historical_context": HISTORICAL,
    }
    projected.pop("order_defs")
    assert (
        projection.projected_v1_jsonl_row_len(wire, (), CONFIGURATION, HISTORICAL)
        == len(canonical_bytes(projected)) + 1
    )
    materialized, added = projection.materialize_v1_row(
        deepcopy(wire), (), (CONFIGURATION, HISTORICAL)
    )
    assert materialized == projected
    assert added == [definition]


def test_schema_names_have_equal_width():
    projection.assert_schema_width_identity()


@pytest.mark.parametrize(
    ("wire", "projected", "row_projected", "expected"),
    [
        (1, 6, 6, True),
        (1, 7, 6, False),
        (1, 6, 1_048_576, True),
        (1, 6, 1_048_577, False),
        (89_478_485, 536_870_910, 1, True),
        (89_478_485, 536_870_911, 1, False),
        (100_000_000, 536_870_912, 1, True),
        (100_000_000, 536_870_913, 1, False),
        (0, 0, 0, True),
    ],
)
def test_projection_budget_boundaries_are_inclusive(
    wire, projected, row_projected, expected
):
    assert (
        projection.projected_jsonl_budget_ok(
            wire_bytes=wire,
            projected_bytes=projected,
            row_projected_bytes=row_projected,
        )
        is expected
    )


@pytest.mark.parametrize(
    ("name", "value"), [("wire_bytes", -1), ("projected_bytes", True)]
)
def test_projection_budget_rejects_invalid_counters(name, value):
    counters = dict(wire_bytes=1, projected_bytes=1, row_projected_bytes=1)
    counters[name] = value
    with pytest.raises(ValueError, match="INVALID_ARTIFACT"):
        projection.projected_jsonl_budget_ok(**counters)


@pytest.mark.parametrize("immutable", [None, [], 0, "bad"])
def test_open_orders_projection_rejects_non_mapping_immutable_payload(immutable):
    row = {
        "schema_version": "spider_journal_record_v2",
        "record_kind": "SNAPSHOT_FACT",
        "payload": {
            "fact": {
                "snapshot_kind": "OPEN_ORDERS",
                "immutable_payload": immutable,
            }
        },
    }
    with pytest.raises(ValueError, match="ARTIFACT_MISMATCH"):
        projection.projected_v1_jsonl_row_len(row, (), {}, {})
