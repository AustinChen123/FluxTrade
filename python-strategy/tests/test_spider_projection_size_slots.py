# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""Projection size preserves nullable and absent D5.2 slots."""

import pytest

from spider_journal_codec_test_support import (
    CONFIGURATION,
    HISTORICAL,
    _market,
    canonical_bytes,
)
from src.core.backtest.spider_projection_size import (
    materialize_v1_row,
    projected_v1_jsonl_row_len,
)


@pytest.mark.parametrize("state", ["missing", "null"])
def test_projection_size_and_materialization_preserve_slot_state(state):
    row = _market([], [])
    parent = row["payload"]["request"]
    if state == "missing":
        parent.pop("working_orders")
    else:
        parent["working_orders"] = None
    expected = {
        **row,
        "configuration_context": CONFIGURATION,
        "historical_context": HISTORICAL,
    }
    wire = {**row, "schema_version": "spider_journal_record_v2"}
    wire.pop("configuration_context")
    wire.pop("historical_context")

    assert (
        projected_v1_jsonl_row_len(wire, (), CONFIGURATION, HISTORICAL)
        == len(canonical_bytes(expected)) + 1
    )
    projected, definitions = materialize_v1_row(wire, (), (CONFIGURATION, HISTORICAL))
    assert canonical_bytes(projected) == canonical_bytes(expected)
    assert definitions == []
