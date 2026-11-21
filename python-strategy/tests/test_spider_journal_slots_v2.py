# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""Closed D5.2 slot schema and value-state tests."""

import copy


from src.core.backtest import spider_artifact_limits as limits
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


def _payload(kind):
    if kind == "HISTORICAL_MARKET_RESULT":
        return {"request": {"working_orders": []}, "working_orders_snapshot": []}
    if kind == "SOURCE_GROUP_RESULT":
        side = {"open_orders_fact": {"immutable_payload": {"rows": []}}}
        return {
            "owner_evidence_before": copy.deepcopy(side),
            "owner_evidence_after": side,
        }
    if kind == "SNAPSHOT_FACT":
        return {
            "fact": {
                "snapshot_kind": "OPEN_ORDERS",
                "immutable_payload": {"outcome": "SUCCESS", "rows": []},
            }
        }
    return {"callback": "done"}


def _row(kind):
    return {"record_kind": kind, "payload": _payload(kind)}


def _at(root, path):
    for name in path:
        root = root[name]
    return root


def test_one_immutable_slot_table_matches_d52_and_closed_j3_kind_set():
    actual = {
        kind: tuple((path, fields) for path, fields in entries)
        for kind, entries, _ in slots.P3_V2_SLOT_TABLE
    }
    assert actual == PATHS
    assert slots.P3_V2_RECORD_KINDS == frozenset(PATHS)
    assert slots.P3_V2_RECORD_KINDS == limits.P3_V2_RECORD_KINDS
    assert type(slots.P3_V2_SLOT_TABLE) is tuple
    assert all(type(entries) is tuple for _, entries, _ in slots.P3_V2_SLOT_TABLE)
    assert all(
        type(path) is tuple and type(fields) is frozenset
        for entries in actual.values()
        for path, fields in entries
    )
