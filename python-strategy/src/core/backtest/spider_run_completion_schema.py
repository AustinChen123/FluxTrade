"""Pure report/completion structures; supplied metadata is never authority."""

import re
from typing import cast

from src.core.backtest.spider_run_artifacts import (
    _HASH_NAMES, _KINDS, _TERMINALS, _boundary, _decimal_text, _enum, _integer,
    _list, _object, _require, _text,
)
from src.core.backtest.spider_run_native_schema import open_order

_ARTIFACTS = (
    ("attempt.json", "spider_attempt_v1"),
    ("status.json", "spider_status_v1"),
    ("journal.jsonl", "spider_journal_record_v1"),
    ("endpoint.json", "spider_endpoint_v1"),
    ("reconciliation.json", "spider_reconciliation_v1"),
    ("report.jsonl", "spider_product_report_v1"),
)


def evidence_reference(value: object) -> str:
    """Validate literal pointer syntax without decoding or resolving a target."""
    _require(type(value) is str)
    text = cast(str, value)
    if text.startswith("journal:"):
        _require(re.fullmatch(r"journal:[1-9][0-9]*", text) is not None)
    else:
        target, separator, pointer = text.partition("#")
        _require(bool(separator) and target in ["artifact:" + name for name, _ in _ARTIFACTS] + ["artifact:completion.json"])
        _require(not pointer or pointer.startswith("/"))
        _require(re.search(r"~(?![01])", pointer) is None)
    return text


def evidence_references(value: object) -> None:
    refs = [evidence_reference(item) for item in _list(value)]
    _require(bool(refs) and len(refs) == len(set(refs)))


def _header(row: dict[str, object], schema: str) -> None:
    _enum(row["schema_version"], [schema])
    _text(row["run_id"], "[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
    _enum(row["terminal_reason"], _TERMINALS)


def report_row(value: object) -> None:
    row = _object(value, "schema_version run_id product_id terminal_reason position_contracts mark_price notional_usd open_orders committed_execution_refs account_cash account_equity account_available_equity account_gross_realized account_total_fees source_evidence_refs")
    _header(row, "spider_product_report_v1")
    _enum(row["product_id"], ["BTC-USDT-SWAP", "ETH-USDT-SWAP", "P_A"])
    for name in "position_contracts notional_usd account_cash account_equity account_available_equity account_gross_realized account_total_fees".split():
        _decimal_text(row[name])
    if row["mark_price"] is not None:
        _decimal_text(row["mark_price"])
    orders: list[tuple[str, str]] = []
    for item in _list(row["open_orders"]):
        open_order(item)
        order = cast(dict[str, object], item)
        orders.append((_text(order["product_id"]), _text(order["order_id"])))
    _require(orders == sorted(set(orders)))
    refs: list[str] = []
    for item in _list(row["committed_execution_refs"]):
        ref = _text(item)
        namespace, separator, fact_id = ref.partition(":")
        _enum(namespace, ["SOURCE", "LIQUIDATION"])
        _require(bool(separator))
        _text(fact_id)
        refs.append(ref)
    _require(len(refs) == len(set(refs)))
    evidence_references(row["source_evidence_refs"])


def report(value: object) -> None:
    rows = _list(value)
    for row in rows:
        report_row(row)
    products = [cast(dict[str, object], row)["product_id"] for row in rows]
    _require(products in (["P_A"], ["BTC-USDT-SWAP", "ETH-USDT-SWAP"]))


def completion(value: object) -> None:
    row = _object(value, "schema_version run_id state terminal_reason input_contract_hashes planned_coverage processed_boundary persisted_boundary endpoint_state_digest reconciliation_digest artifacts")
    _header(row, "spider_completion_v1")
    _enum(row["state"], ["COMPLETE"])
    for name in ("endpoint_state_digest", "reconciliation_digest"):
        _text(row[name], "[0-9a-f]{64}")
    for name in ("processed_boundary", "persisted_boundary"):
        _require(row[name] is not None)
        _boundary(row[name])
    hashes = _list(row["input_contract_hashes"])
    _require(len(hashes) == len(_HASH_NAMES))
    for item, name in zip(hashes, _HASH_NAMES, strict=True):
        entry = _object(item, "name sha256")
        _enum(entry["name"], [name])
        _text(entry["sha256"], "[0-9a-f]{64}")
    barriers: list[str] = []
    for ordinal, item in enumerate(_list(row["planned_coverage"]), 1):
        entry = _object(item, "ordinal barrier_id record_kind")
        _require(_integer(entry["ordinal"]) == ordinal)
        barriers.append(_text(entry["barrier_id"]))
        _enum(entry["record_kind"], _KINDS)
    _require(len(barriers) == len(set(barriers)))
    artifacts = _list(row["artifacts"])
    _require(len(artifacts) == len(_ARTIFACTS))
    for item, (path, schema) in zip(artifacts, _ARTIFACTS, strict=True):
        entry = _object(item, "path schema_version sha256 byte_count row_count")
        _enum(entry["path"], [path])
        _enum(entry["schema_version"], [schema])
        _text(entry["sha256"], "[0-9a-f]{64}")
        _integer(entry["byte_count"])
        _integer(entry["row_count"])
