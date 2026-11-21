# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""D5.2 row-slot classifier state matrix."""

import copy

import pytest

from src.core.backtest import spider_journal_slot_classifier_v2 as classifier
from src.core.backtest import spider_journal_slots_v2 as slots

OPEN = frozenset(
    {
        "order_id",
        "client_order_id",
        "product_id",
        "state",
        "side",
        "limit_price",
        "original_size_contracts",
        "cumulative_filled_size_contracts",
        "created_at",
    }
)
WORKING = frozenset(
    {
        "order_id",
        "product_id",
        "order_version",
        "status",
        "remaining_quantity_contracts",
        "accepted_at",
        "accepted_source_sequence",
        "order_kind",
        "side",
        "limit_price",
        "risk_cancel_pending",
    }
)
PATHS = {
    "HISTORICAL_MARKET_RESULT": (
        (("payload", "request", "working_orders"), WORKING),
        (("payload", "working_orders_snapshot"), WORKING),
    ),
    "SOURCE_GROUP_RESULT": (
        (
            (
                "payload",
                "owner_evidence_before",
                "open_orders_fact",
                "immutable_payload",
                "rows",
            ),
            OPEN,
        ),
        (
            (
                "payload",
                "owner_evidence_after",
                "open_orders_fact",
                "immutable_payload",
                "rows",
            ),
            OPEN,
        ),
    ),
    "SNAPSHOT_FACT": ((("payload", "fact", "immutable_payload", "rows"), OPEN),),
    "CALLBACK_RESULT": (),
}


def _row(kind):
    if kind == "HISTORICAL_MARKET_RESULT":
        payload = {"request": {"working_orders": []}, "working_orders_snapshot": []}
    elif kind == "SOURCE_GROUP_RESULT":
        side = {"open_orders_fact": {"immutable_payload": {"rows": []}}}
        payload = {
            "owner_evidence_before": copy.deepcopy(side),
            "owner_evidence_after": side,
        }
    elif kind == "SNAPSHOT_FACT":
        payload = {
            "fact": {
                "snapshot_kind": "OPEN_ORDERS",
                "immutable_payload": {"outcome": "SUCCESS", "rows": []},
            }
        }
    else:
        payload = {"callback": "done"}
    return {"record_kind": kind, "payload": payload}


def _at(root, path):
    for name in path:
        root = root[name]
    return root


@pytest.mark.parametrize(
    "kind,entries", [(kind, entries) for kind, entries in PATHS.items() if entries]
)
@pytest.mark.parametrize(
    "shape,state",
    [
        ("missing", slots.SLOT_MISSING),
        ("null", slots.SLOT_NULL),
        ("empty", slots.SLOT_LIST),
        ("nonempty", slots.SLOT_LIST),
    ],
)
def test_each_slot_has_exact_missing_null_and_list_states(kind, entries, shape, state):
    for path, expected_fields in entries:
        row = _row(kind)
        parent = _at(row, path[:-1])
        if shape == "missing":
            parent.pop(path[-1])
        elif shape == "null":
            parent[path[-1]] = None
        elif shape == "nonempty":
            parent[path[-1]] = [{"order_id": "one"}]
        result = classifier.classify_p3_v2_slots(row)
        assert result is not None
        selected = [entry for entry in result if entry[0] == path]
        assert len(selected) == 1
        got_path, fields, got_state, got_parent, leaf = selected[0]
        assert (got_path, fields, got_state) == (path, expected_fields, state)
        if shape == "missing":
            assert leaf not in got_parent
        elif shape == "null":
            assert got_parent[leaf] is None
        elif shape == "empty":
            assert got_parent[leaf] == []
        else:
            assert got_parent[leaf] == [{"order_id": "one"}]


@pytest.mark.parametrize("bad", [0, False, {}, "bad", ([],)])
def test_nonlist_nonnull_slot_is_malformed(bad):
    row = _row("HISTORICAL_MARKET_RESULT")
    row["payload"]["request"]["working_orders"] = bad
    assert classifier.classify_p3_v2_slots(row) is None


@pytest.mark.parametrize(
    "snapshot_kind,outcome,count",
    [
        ("OPEN_ORDERS", "SUCCESS", 1),
        ("OPEN_ORDERS", "FAILURE", 0),
        ("POSITIONS", "SUCCESS", 0),
    ],
)
def test_only_open_order_success_snapshot_has_a_slot(snapshot_kind, outcome, count):
    row = _row("SNAPSHOT_FACT")
    fact = row["payload"]["fact"]
    fact["snapshot_kind"] = snapshot_kind
    fact["immutable_payload"]["outcome"] = outcome
    assert len(classifier.classify_p3_v2_slots(row)) == count


def test_unknown_and_nonexact_record_kinds_are_rejected():
    class StringSubclass(str):
        pass

    for kind in (None, 1, True, "UNKNOWN", StringSubclass("CALLBACK_RESULT")):
        assert (
            classifier.classify_p3_v2_slots({"record_kind": kind, "payload": {}})
            is None
        )
