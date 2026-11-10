# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""The single immutable D5.2 slot definition."""

_OPEN_FIELDS = frozenset(
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
_WORKING_FIELDS = frozenset(
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


# Entry = (kind, ordered (path, exact order fields) slots, optional snapshot selector).
P3_V2_SLOT_TABLE = (
    (
        "HISTORICAL_MARKET_RESULT",
        (
            (("payload", "request", "working_orders"), _WORKING_FIELDS),
            (("payload", "working_orders_snapshot"), _WORKING_FIELDS),
        ),
        None,
    ),
    (
        "SOURCE_GROUP_RESULT",
        (
            (
                (
                    "payload",
                    "owner_evidence_before",
                    "open_orders_fact",
                    "immutable_payload",
                    "rows",
                ),
                _OPEN_FIELDS,
            ),
            (
                (
                    "payload",
                    "owner_evidence_after",
                    "open_orders_fact",
                    "immutable_payload",
                    "rows",
                ),
                _OPEN_FIELDS,
            ),
        ),
        None,
    ),
    (
        "SNAPSHOT_FACT",
        ((("payload", "fact", "immutable_payload", "rows"), _OPEN_FIELDS),),
        ("OPEN_ORDERS", "SUCCESS"),
    ),
    ("CALLBACK_RESULT", (), None),
)
SLOT_MISSING = "MISSING"
SLOT_NULL = "NULL"
SLOT_LIST = "LIST"
P3_V2_RECORD_KINDS = frozenset(kind for kind, _, _ in P3_V2_SLOT_TABLE)
