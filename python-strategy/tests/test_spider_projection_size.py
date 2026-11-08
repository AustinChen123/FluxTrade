"""T2 projection sizing exactness tests from JOURNAL_AB_DESIGN v3 T2.3."""

from copy import deepcopy

import pytest

from src.core.backtest import spider_projection_size as projection
from src.core.backtest.spider_run_artifacts import canonical_bytes

CONFIGURATION = {"schema_version": "spider_configuration_context_v1", "id": "cfg-é"}
HISTORICAL = {"schema_version": "spider_historical_context_v1", "source": "ü"}


def _wire_row(
    kind: str, definitions: list[dict[str, object]], refs: list[int]
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
        row["order_defs"] = definitions
    return row


def _expanded(
    row: dict[str, object], definitions: list[dict[str, object]]
) -> dict[str, object]:
    result = deepcopy(row)
    result.pop("order_defs", None)
    result["schema_version"] = "spider_journal_record_v1"
    payload = result["payload"]
    assert isinstance(payload, dict)
    request = payload["request"]
    assert isinstance(request, dict)
    request["working_orders"] = [
        definitions[index] for index in request["working_orders"]
    ]
    payload["working_orders_snapshot"] = [
        definitions[index] for index in payload["working_orders_snapshot"]
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
