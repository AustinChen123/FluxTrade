# pyright: reportArgumentType=false, reportIndexIssue=false, reportAttributeAccessIssue=false
"""Core row fixtures for J4 tests."""

import copy
import hashlib
import random
from pathlib import Path
import pytest
from src.core.backtest import spider_artifact_limits as limits
from src.core.backtest import spider_journal_codec_v2 as codec
from src.core.backtest import spider_journal_reader_v2 as reader
from src.core.backtest.spider_projection_size import projected_v1_jsonl_row_len
from src.core.backtest import spider_journal_slots_v2 as slots
from src.core.backtest.spider_journal_slot_classifier_v2 import classify_p3_v2_slots
from src.core.backtest.spider_run_artifacts import canonical_bytes, decode_canonical

CONFIGURATION = {
    "config_id": "codec-fixture",
    "schema_version": "spider_configuration_context_v1",
}
HISTORICAL = {
    "historical_input_sha256": "a" * 64,
    "schema_version": "spider_historical_context_v1",
}
WORKING_FIELDS = frozenset(
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
OPEN_FIELDS = frozenset(
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


def _working(order_id="w-1", **overrides):
    value = {
        "order_id": order_id,
        "product_id": "BTC-USDT-SWAP",
        "order_version": 1,
        "status": "OPEN",
        "remaining_quantity_contracts": "2.5",
        "accepted_at": 100,
        "accepted_source_sequence": 0,
        "order_kind": "LIMIT",
        "side": "LONG",
        "limit_price": "100.00",
        "risk_cancel_pending": False,
    }
    value.update(overrides)
    return value


def _open(order_id="o-1", **overrides):
    value = {
        "order_id": order_id,
        "client_order_id": "client-1",
        "product_id": "BTC-USDT-SWAP",
        "state": "live",
        "side": "buy",
        "limit_price": "100.00",
        "original_size_contracts": "2.5",
        "cumulative_filled_size_contracts": "0",
        "created_at": 100,
    }
    value.update(overrides)
    return value


def _row(kind, payload):
    return {
        "schema_version": "spider_journal_record_v1",
        "record_kind": kind,
        "configuration_context": CONFIGURATION.copy(),
        "historical_context": HISTORICAL.copy(),
        "payload": payload,
    }


def _market(working=None, snapshot=None):
    request = {}
    if working is not ...:
        request["working_orders"] = working
    payload = {"request": request}
    if snapshot is not ...:
        payload["working_orders_snapshot"] = snapshot
    return _row("HISTORICAL_MARKET_RESULT", payload)


def _source_group(before=..., after=...):
    def side(rows):
        return {"open_orders_fact": {"immutable_payload": {"rows": rows}}}

    payload = {
        "owner_evidence_before": side([] if before is ... else before),
        "owner_evidence_after": side([] if after is ... else after),
    }
    return _row("SOURCE_GROUP_RESULT", payload)


def _snapshot(rows, *, outcome="SUCCESS", snapshot_kind="OPEN_ORDERS"):
    immutable = {"outcome": outcome}
    if rows is not ...:
        immutable["rows"] = rows
    return _row(
        "SNAPSHOT_FACT",
        {"fact": {"snapshot_kind": snapshot_kind, "immutable_payload": immutable}},
    )


__all__ = [
    "CONFIGURATION",
    "HISTORICAL",
    "OPEN_FIELDS",
    "WORKING_FIELDS",
    "canonical_bytes",
    "decode_canonical",
    "codec",
    "limits",
    "reader",
    "projected_v1_jsonl_row_len",
    "slots",
    "classify_p3_v2_slots",
    "copy",
    "hashlib",
    "random",
    "Path",
    "pytest",
    "_market",
    "_open",
    "_row",
    "_snapshot",
    "_source_group",
    "_working",
]
