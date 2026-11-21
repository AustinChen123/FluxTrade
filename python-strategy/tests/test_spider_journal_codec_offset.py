# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""Named regression for dense indexes spanning prior rows."""

from src.core.backtest.spider_journal_codec_v2 import encode_p3_v2_row
from src.core.backtest.spider_run_artifacts import canonical_bytes


def _working():
    return {
        "order_id": "w",
        "product_id": "p",
        "order_version": 1,
        "status": "OPEN",
        "remaining_quantity_contracts": "1",
        "accepted_at": 1,
        "accepted_source_sequence": 0,
        "order_kind": "LIMIT",
        "side": "LONG",
        "limit_price": "1",
        "risk_cancel_pending": False,
    }


def _open():
    return {
        "order_id": "o",
        "client_order_id": "c",
        "product_id": "p",
        "state": "live",
        "side": "buy",
        "limit_price": "1",
        "original_size_contracts": "1",
        "cumulative_filled_size_contracts": "0",
        "created_at": 1,
    }


def test_dense_index_uses_prior_state_offset_for_first_new_definition():
    working, opened = _working(), _open()
    first = {
        "schema_version": "spider_journal_record_v1",
        "record_kind": "HISTORICAL_MARKET_RESULT",
        "payload": {
            "request": {"working_orders": [working]},
            "working_orders_snapshot": [],
        },
    }
    _, prior = encode_p3_v2_row(first)
    second = {
        "schema_version": "spider_journal_record_v1",
        "record_kind": "SOURCE_GROUP_RESULT",
        "payload": {
            "owner_evidence_before": {
                "open_orders_fact": {"immutable_payload": {"rows": []}}
            },
            "owner_evidence_after": {
                "open_orders_fact": {"immutable_payload": {"rows": [opened]}}
            },
        },
    }

    encoded, final = encode_p3_v2_row(second, prior)

    assert encoded["payload"]["owner_evidence_after"]["open_orders_fact"][
        "immutable_payload"
    ]["rows"] == [1]
    assert encoded["order_defs"] == [opened]
    assert final == (canonical_bytes(working), canonical_bytes(opened))
